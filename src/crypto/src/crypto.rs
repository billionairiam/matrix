use std::env;

use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{Aead, KeyInit, Payload},
};
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use chrono::{DateTime, Duration, Utc};
use rand::{RngCore, rngs::OsRng};
use rsa::{
    Oaep, RsaPrivateKey, RsaPublicKey,
    pkcs1::{DecodeRsaPrivateKey, EncodeRsaPrivateKey},
    pkcs8::{DecodePrivateKey, EncodePublicKey},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const STORAGE_PREFIX: &str = "ENC:v1:";
const STORAGE_DELIMITER: &str = ":";

// AES data encryption key (Base64)
const ENV_DATA_ENCRYPTION_KEY: &str = "DATA_ENCRYPTION_KEY";
// RSA private key (PEM format, use \n for newlines)
const ENV_RSA_PRIVATE_KEY: &str = "RSA_PRIVATE_KEY";

#[derive(Debug, serde::Deserialize, serde::Serialize, Default)]
pub struct EncryptedPayload {
    #[serde(rename = "wrappedKey")]
    pub wrapped_key: String,
    pub iv: String,
    pub ciphertext: String,
    #[serde(default)]
    pub aad: Option<String>,
    #[serde(default)]
    pub kid: Option<String>,
    #[serde(default)]
    pub ts: Option<i64>,
}

#[derive(Debug, serde::Deserialize)]
pub struct AADData {
    #[serde(rename = "userId")]
    pub user_id: String,
    #[serde(rename = "sessionId")]
    pub session_id: String,
    pub ts: i64,
    pub purpose: String,
}

#[derive(Debug)]
pub struct CryptoService {
    private_key: RsaPrivateKey,
    public_key: RsaPublicKey,
    data_key: Vec<u8>,
}

#[derive(Debug, thiserror::Error, Serialize, Deserialize)]
pub enum CryptoError {
    #[error("Environment variable missing: {0}")]
    EnvMissing(String),
    #[error("Invalid key format: {0}")]
    KeyFormat(String),
    #[error("Decryption failed: {0}")]
    DecryptionFailed(String),
    #[error("Encryption failed: {0}")]
    EncryptionFailed(String),
    #[error("Invalid storage format")]
    InvalidStorageFormat,
    #[error("Timestamp invalid or expired")]
    TimestampExpired,
    #[error("Data key not configured")]
    DataKeyMissing,
    #[error("IO/System error: {0}")]
    SystemError(String),
}

impl CryptoService {
    /// Create crypto service (loads keys from environment variables)
    pub fn new() -> Result<Self, CryptoError> {
        // Load .env (optional, ignore failure)
        let _ = dotenvy::dotenv();

        let private_key = Self::load_rsa_private_key_from_env()?;
        let public_key = private_key.to_public_key();
        let data_key = Self::load_data_key_from_env()?;

        Ok(Self {
            private_key,
            public_key,
            data_key,
        })
    }

    fn load_rsa_private_key_from_env() -> Result<RsaPrivateKey, CryptoError> {
        let key_pem = env::var(ENV_RSA_PRIVATE_KEY)
            .map_err(|_| CryptoError::EnvMissing(ENV_RSA_PRIVATE_KEY.to_string()))?;

        // Handle newlines
        let key_pem = key_pem.replace("\\n", "\n");

        // Try PKCS#1 first, then PKCS#8
        if let Ok(key) = RsaPrivateKey::from_pkcs1_pem(&key_pem) {
            return Ok(key);
        }

        if let Ok(key) = RsaPrivateKey::from_pkcs8_pem(&key_pem) {
            return Ok(key);
        }

        Err(CryptoError::KeyFormat(
            "Invalid PEM format for RSA key".into(),
        ))
    }

    fn load_data_key_from_env() -> Result<Vec<u8>, CryptoError> {
        let key_str = env::var(ENV_DATA_ENCRYPTION_KEY)
            .map(|s| s.trim().to_string())
            .map_err(|_| CryptoError::EnvMissing(ENV_DATA_ENCRYPTION_KEY.to_string()))?;

        if let Some(key) = Self::decode_possible_key(&key_str) {
            return Ok(key);
        }

        // Fallback: SHA256 hash
        let mut hasher = Sha256::new();
        hasher.update(key_str.as_bytes());
        Ok(hasher.finalize().to_vec())
    }

    fn decode_possible_key(value: &str) -> Option<Vec<u8>> {
        // Try Base64 Standard
        if let Ok(decoded) = STANDARD.decode(value) {
            if let Some(normalized) = Self::normalize_aes_key(decoded) {
                return Some(normalized);
            }
        }
        // Try Base64 Raw Standard
        if let Ok(decoded) = base64::engine::general_purpose::STANDARD_NO_PAD.decode(value) {
            if let Some(normalized) = Self::normalize_aes_key(decoded) {
                return Some(normalized);
            }
        }
        // Try Hex
        if let Ok(decoded) = hex::decode(value) {
            if let Some(normalized) = Self::normalize_aes_key(decoded) {
                return Some(normalized);
            }
        }
        None
    }

    fn normalize_aes_key(raw: Vec<u8>) -> Option<Vec<u8>> {
        match raw.len() {
            16 | 24 | 32 => Some(raw),
            0 => None,
            _ => {
                let mut hasher = Sha256::new();
                hasher.update(&raw);
                Some(hasher.finalize().to_vec())
            }
        }
    }

    pub fn has_data_key(&self) -> bool {
        !self.data_key.is_empty()
    }

    pub fn get_public_key_pem(&self) -> Result<String, CryptoError> {
        self.public_key
            .to_public_key_pem(rsa::pkcs8::LineEnding::LF)
            .map_err(|e| CryptoError::SystemError(e.to_string()))
    }

    pub fn encrypt_for_storage(
        &self,
        plaintext: &str,
        aad_parts: &[&str],
    ) -> Result<String, CryptoError> {
        if plaintext.is_empty() {
            return Ok(String::new());
        }
        if !self.has_data_key() {
            return Err(CryptoError::DataKeyMissing);
        }
        if self.is_encrypted_storage_value(plaintext) {
            return Ok(plaintext.to_string());
        }

        // Initialize AES-GCM
        let key = Key::<Aes256Gcm>::from_slice(&self.data_key); // Assumes 32-byte key
        let cipher = Aes256Gcm::new(key);

        // Generate Nonce
        let mut nonce_bytes = [0u8; 12];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        // Compose AAD
        let aad_bytes = Self::compose_aad(aad_parts);
        let payload = Payload {
            msg: plaintext.as_bytes(),
            aad: &aad_bytes,
        };

        // Encrypt
        let ciphertext = cipher
            .encrypt(nonce, payload)
            .map_err(|e| CryptoError::EncryptionFailed(e.to_string()))?;

        // Format: ENC:v1:NONCE:CIPHERTEXT
        Ok(format!(
            "{}{}{}{}",
            STORAGE_PREFIX,
            STANDARD.encode(nonce),
            STORAGE_DELIMITER,
            STANDARD.encode(ciphertext)
        ))
    }

    pub fn decrypt_from_storage(
        &self,
        value: &str,
        aad_parts: &[&str],
    ) -> Result<String, CryptoError> {
        if value.is_empty() {
            return Ok(String::new());
        }
        if !self.has_data_key() {
            return Err(CryptoError::DataKeyMissing);
        }
        if !self.is_encrypted_storage_value(value) {
            return Err(CryptoError::InvalidStorageFormat);
        }

        let payload_str = value.strip_prefix(STORAGE_PREFIX).unwrap_or(value);
        let parts: Vec<&str> = payload_str.splitn(2, STORAGE_DELIMITER).collect();

        if parts.len() != 2 {
            return Err(CryptoError::InvalidStorageFormat);
        }

        let nonce_bytes = STANDARD
            .decode(parts[0])
            .map_err(|_| CryptoError::InvalidStorageFormat)?;
        let ciphertext = STANDARD
            .decode(parts[1])
            .map_err(|_| CryptoError::InvalidStorageFormat)?;

        if nonce_bytes.len() != 12 {
            return Err(CryptoError::InvalidStorageFormat);
        }

        let key = Key::<Aes256Gcm>::from_slice(&self.data_key);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let aad_bytes = Self::compose_aad(aad_parts);
        let payload = Payload {
            msg: &ciphertext,
            aad: &aad_bytes,
        };

        let plaintext = cipher
            .decrypt(nonce, payload)
            .map_err(|e| CryptoError::DecryptionFailed(e.to_string()))?;

        String::from_utf8(plaintext)
            .map_err(|e| CryptoError::DecryptionFailed(format!("Invalid UTF-8: {}", e)))
    }

    pub fn is_encrypted_storage_value(&self, value: &str) -> bool {
        value.starts_with(STORAGE_PREFIX)
    }

    fn compose_aad(parts: &[&str]) -> Vec<u8> {
        if parts.is_empty() {
            return Vec::new();
        }
        parts.join("|").into_bytes()
    }

    pub fn decrypt_payload(&self, payload: &EncryptedPayload) -> Result<Vec<u8>, CryptoError> {
        // 1. Validate Timestamp (Fixed API Deprecation)
        if let Some(ts) = payload.ts {
            if ts != 0 {
                // 使用 DateTime::from_timestamp 替代 NaiveDateTime::from_timestamp_opt
                // 注意：DateTime::from_timestamp 返回 Option<DateTime<Utc>>，我们直接使用它与 Utc::now() 比较
                let ts_time =
                    DateTime::from_timestamp(ts, 0).ok_or(CryptoError::TimestampExpired)?;

                let now = Utc::now();

                // Allow 5 minutes drift
                let diff = now.signed_duration_since(ts_time);
                if diff > Duration::minutes(5) || diff < Duration::minutes(-1) {
                    return Err(CryptoError::TimestampExpired);
                }
            }
        }

        // 2. Decode Base64URL fields
        let wrapped_key = URL_SAFE_NO_PAD
            .decode(&payload.wrapped_key)
            .map_err(|e| CryptoError::DecryptionFailed(format!("WrappedKey decode: {}", e)))?;

        let iv = URL_SAFE_NO_PAD
            .decode(&payload.iv)
            .map_err(|e| CryptoError::DecryptionFailed(format!("IV decode: {}", e)))?;

        let ciphertext = URL_SAFE_NO_PAD
            .decode(&payload.ciphertext)
            .map_err(|e| CryptoError::DecryptionFailed(format!("Ciphertext decode: {}", e)))?;

        let mut aad_bytes = Vec::new();
        if let Some(aad_str) = &payload.aad {
            if !aad_str.is_empty() {
                aad_bytes = URL_SAFE_NO_PAD
                    .decode(aad_str)
                    .map_err(|e| CryptoError::DecryptionFailed(format!("AAD decode: {}", e)))?;
            }
        }

        // 3. RSA Decrypt (OAEP SHA256)
        let padding = Oaep::new::<Sha256>();
        let aes_key_bytes = self
            .private_key
            .decrypt(padding, &wrapped_key)
            .map_err(|e| CryptoError::DecryptionFailed(format!("RSA decrypt failed: {}", e)))?;

        // 4. AES-GCM Decrypt
        // Normalize Key if needed (though RSA output should be exact size)
        let normalized_key = Self::normalize_aes_key(aes_key_bytes).ok_or_else(|| {
            CryptoError::DecryptionFailed("Invalid decrypted AES key size".into())
        })?;

        let key = Key::<Aes256Gcm>::from_slice(&normalized_key);
        let cipher = Aes256Gcm::new(key);

        if iv.len() != 12 {
            return Err(CryptoError::DecryptionFailed(format!(
                "Invalid IV length: {}",
                iv.len()
            )));
        }
        let nonce = Nonce::from_slice(&iv);

        let gcm_payload = Payload {
            msg: &ciphertext,
            aad: &aad_bytes,
        };

        let plaintext = cipher.decrypt(nonce, gcm_payload).map_err(|e| {
            CryptoError::DecryptionFailed(format!("AES verification failed: {}", e))
        })?;

        Ok(plaintext)
    }

    pub fn decrypt_sensitive_data(
        &self,
        payload: &EncryptedPayload,
    ) -> Result<String, CryptoError> {
        let bytes = self.decrypt_payload(payload)?;
        String::from_utf8(bytes)
            .map_err(|e| CryptoError::DecryptionFailed(format!("Invalid UTF-8: {}", e)))
    }
}

pub fn generate_key_pair() -> Result<(String, String), CryptoError> {
    let mut rng = OsRng;
    let private_key =
        RsaPrivateKey::new(&mut rng, 2048).map_err(|e| CryptoError::SystemError(e.to_string()))?;

    let priv_pem = private_key
        .to_pkcs1_pem(rsa::pkcs8::LineEnding::LF)
        .map_err(|e| CryptoError::SystemError(e.to_string()))?;

    let pub_pem = private_key
        .to_public_key()
        .to_public_key_pem(rsa::pkcs8::LineEnding::LF)
        .map_err(|e| CryptoError::SystemError(e.to_string()))?;

    Ok((priv_pem.to_string(), pub_pem))
}

pub fn generate_data_key() -> String {
    let mut key = [0u8; 32];
    OsRng.fill_bytes(&mut key);
    STANDARD.encode(key)
}
