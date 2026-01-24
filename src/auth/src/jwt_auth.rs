use anyhow::Result;
use bcrypt::{DEFAULT_COST, hash, verify};
use chrono::{Duration, Utc};
use jsonwebtoken::{
    DecodingKey, EncodingKey, Header, Validation, decode, encode, errors::ErrorKind,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{OnceLock, RwLock},
    time::SystemTimeError,
};
use totp_rs::{Algorithm, Secret, TOTP};

// JWT_SECRET is the JWT secret key, will be dynamically set from config
static JWT_SECRET: OnceLock<String> = OnceLock::new();

// TOKEN_BLACKLIST for logged out tokens (memory only, cleaned by expiration time)
static TOKEN_BLACKLIST: OnceLock<RwLock<HashMap<String, i64>>> = OnceLock::new();

// MAXBLACKLISTENTRIES is the maximum capacity threshold for blacklist
const MAXBLACKLISTENTRIES: usize = 100_000;

// OTPISSUER is the OTP issuer name
const OTPISSUER: &str = "matrix";

// 计算验证码时使用的加密数学公式
const TOTP_ALGORITHM: Algorithm = Algorithm::SHA1;

// 验证码位数
const TOTP_DIGITS: usize = 6;

// 允许 1 个时间步长的误差（30秒），防止客户端时间微小偏移导致验证失败
const TOTP_SKEW: u8 = 1;

// 一个验证码的有效期
const TOTP_STEP: u64 = 30;

/// Initialize the global token blacklist
fn get_blacklist() -> &'static RwLock<HashMap<String, i64>> {
    TOKEN_BLACKLIST.get_or_init(|| RwLock::new(HashMap::new()))
}

// set_jwt_secret sets the JWT secret key
pub fn set_jwt_secret(secret: &str) {
    let _ = JWT_SECRET.set(secret.to_string());
}

fn get_jwt_secret() -> &'static [u8] {
    JWT_SECRET
        .get()
        .expect("JWT SECRET NOT initialized")
        .as_bytes()
}

// blacklist_token adds token to blacklist until expiration
pub fn blacklist_token(token: String, exp: i64) {
    let blist = get_blacklist();
    let mut blist_write = blist.write().unwrap();

    blist_write.insert(token, exp);

    if blist_write.len() > MAXBLACKLISTENTRIES {
        let now = Utc::now().timestamp();
        blist_write.retain(|_, &mut expiration_time| expiration_time > now);
        if blist_write.len() > MAXBLACKLISTENTRIES {
            log::info!(
                "auth: token blacklist size ({}) exceeds limit ({}) after sweep; consider reducing JWT TTL or using a shared persistent store",
                blist_write.len(),
                MAXBLACKLISTENTRIES
            );
        }
    }
}

pub fn is_token_black_listed(token: &str) -> Result<bool> {
    let blist = get_blacklist();
    {
        let blist_read = blist.read().unwrap();
        if let Some(&exp) = blist_read.get(token) {
            if Utc::now().timestamp() <= exp {
                return Ok(true);
            }
        } else {
            return Ok(false);
        }
    }

    let mut blist_write = blist.write().unwrap();
    if let Some(&exp) = blist_write.get(token) {
        if Utc::now().timestamp() > exp {
            blist_write.remove(token);
        }
    }

    Ok(false)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub user_id: String,
    pub email: String,
    // Standard JWT claims (exp, iat, nbf, iss)
    // exp (Expiration): Defines when the token expires.
    pub exp: usize,
    // iat (Issued At): Timestamp when the token was created.
    pub iat: usize,
    // nbf (Not Before): Specifies when the token becomes valid.
    pub nbf: usize,
    // iss (Issuer): Identifies who issued the token.
    pub iss: String,
}

// hash_password hashes the password
pub fn hash_password(password: &str) -> Result<String, bcrypt::BcryptError> {
    hash(password, DEFAULT_COST)
}

// check_password verifies the password
pub fn check_password(password: &str, hash: &str) -> bcrypt::BcryptResult<bool> {
    verify(password, hash)
}

// generate_otp_secret generates OTP secret
pub fn generate_otp_secret() -> Result<String> {
    let secret = Secret::generate_secret().to_encoded().to_string();

    Ok(secret)
}

// verify_otp verifies OTP code
pub fn verify_otp(secret: &str, code: &str) -> Result<bool, SystemTimeError> {
    let secret = match Secret::Encoded(secret.to_string()).to_bytes() {
        Ok(s) => s,
        Err(_) => return Ok(false),
    };

    let totp = match TOTP::new(
        TOTP_ALGORITHM,
        TOTP_DIGITS,
        TOTP_SKEW,
        TOTP_STEP,
        secret,
        None,          // 验证时不需要 Issuer
        String::new(), // 验证时不需要 Account Name
    ) {
        Ok(t) => t,
        Err(_) => return Ok(false),
    };

    totp.check_current(code)
}

// generate_jwt generates JWT token
pub fn generate_jwt(user_id: &str, email: &str) -> Result<String, jsonwebtoken::errors::Error> {
    let now = Utc::now();
    let exp = now + Duration::hours(24);

    let claims = Claims {
        user_id: user_id.to_string(),
        email: email.to_string(),
        exp: exp.timestamp() as usize,
        iat: now.timestamp() as usize,
        nbf: now.timestamp() as usize,
        iss: OTPISSUER.to_string(),
    };

    let secret = get_jwt_secret();

    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret),
    )
}

// validate_jwt validates JWT token
pub fn validate_jwt(token: &str) -> Result<Claims, String> {
    if is_token_black_listed(token).unwrap() {
        return Err("Token is blacklisted".to_string());
    }

    let secret = get_jwt_secret();
    let mut validation = Validation::new(jsonwebtoken::Algorithm::HS256);
    validation.validate_exp = true;
    validation.validate_nbf = true;

    match decode::<Claims>(token, &DecodingKey::from_secret(secret), &validation) {
        Ok(token_data) => Ok(token_data.claims),
        Err(err) => match err.kind() {
            ErrorKind::ExpiredSignature => Err("Token has expired".to_string()),
            ErrorKind::InvalidToken => Err("Invalid token".to_string()),
            _ => Err(format!("Token validation error: {}", err)),
        },
    }
}

// get_otp_qrcode_url gets OTP QR code URL
pub fn get_otp_qrcode_url(secret: &str, email: &str) -> Result<String> {
    let secret = Secret::Encoded(secret.to_string())
        .to_bytes()
        .unwrap_or(vec![]);

    if let Ok(totp) = TOTP::new(
        TOTP_ALGORITHM,
        TOTP_DIGITS,
        TOTP_SKEW,
        TOTP_STEP,
        secret,
        Some(OTPISSUER.to_string()),
        email.to_string(),
    ) {
        Ok(totp.get_url())
    } else {
        Ok(String::new())
    }
}
