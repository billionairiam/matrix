use sqlx::{ SqlitePool, FromRow};
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use anyhow::{Context, Result};
use rand::RngCore;
use data_encoding::BASE32;

/// User user entity
#[derive(Debug, Serialize, Deserialize, Clone, FromRow)]
pub struct User {
    pub id: String,
    pub email: String,
    
    #[serde(skip)] // json:"-"
    #[sqlx(rename = "password_hash")]
    pub password_hash: String,
    
    #[serde(skip)] // json:"-"
    #[sqlx(rename = "otp_secret")]
    pub otp_secret: String,
    
    #[sqlx(rename = "otp_verified")]
    pub otp_verified: bool,
    
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// UserStore user storage
#[derive(Clone)]
pub struct UserStore {
    db: SqlitePool,
}

impl UserStore {
    pub fn new(db: SqlitePool) -> Self {
        Self { db }
    }

    /// Generates OTP secret (20 random bytes encoded in Base32)
    pub fn generate_otp_secret() -> Result<String> {
        let mut secret = [0u8; 20];
        rand::thread_rng().fill_bytes(&mut secret);
        Ok(BASE32.encode(&secret))
    }

    /// Initializes user tables
    pub async fn init_tables(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                otp_secret TEXT,
                otp_verified BOOLEAN DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            "#,
        )
        .execute(&self.db)
        .await
        .context("Failed to create users table")?;

        // Trigger
        sqlx::query(
            r#"
            CREATE TRIGGER IF NOT EXISTS update_users_updated_at
            AFTER UPDATE ON users
            BEGIN
                UPDATE users SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
            END
            "#,
        )
        .execute(&self.db)
        .await
        .context("Failed to create trigger")?;

        Ok(())
    }

    /// Creates user
    pub async fn create(&self, user: &User) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO users (id, email, password_hash, otp_secret, otp_verified, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&user.id)
        .bind(&user.email)
        .bind(&user.password_hash)
        .bind(&user.otp_secret)
        .bind(user.otp_verified)
        // Note: We explicitly bind times here to ensure the structs match DB exactly on creation,
        // though DB defaults would work if we omitted them.
        .bind(user.created_at) 
        .bind(user.updated_at)
        .execute(&self.db)
        .await
        .context("Failed to create user")?;

        Ok(())
    }

    /// Gets user by email
    pub async fn get_by_email(&self, email: &str) -> Result<User> {
        // sqlx::query_as uses the FromRow implementation derived on User
        // It automatically handles standard SQLite datetime string parsing into DateTime<Utc>
        let user = sqlx::query_as::<_, User>(
            r#"
            SELECT id, email, password_hash, otp_secret, otp_verified, created_at, updated_at
            FROM users WHERE email = ?
            "#,
        )
        .bind(email)
        .fetch_one(&self.db)
        .await
        .context("Failed to get user by email")?;

        Ok(user)
    }

    /// Gets user by ID
    pub async fn get_by_id(&self, user_id: &str) -> Result<User> {
        let user = sqlx::query_as::<_, User>(
            r#"
            SELECT id, email, password_hash, otp_secret, otp_verified, created_at, updated_at
            FROM users WHERE id = ?
            "#,
        )
        .bind(user_id)
        .fetch_one(&self.db)
        .await
        .context("Failed to get user by ID")?;

        Ok(user)
    }

    /// Gets all user IDs
    pub async fn get_all_ids(&self) -> Result<Vec<String>> {
        let user_ids = sqlx::query_scalar::<_, String>(
            "SELECT id FROM users ORDER BY id"
        )
        .fetch_all(&self.db)
        .await
        .context("Failed to get all user IDs")?;

        Ok(user_ids)
    }

    /// Updates OTP verification status
    pub async fn update_otp_verified(&self, user_id: &str, verified: bool) -> Result<()> {
        sqlx::query("UPDATE users SET otp_verified = ? WHERE id = ?")
            .bind(verified)
            .bind(user_id)
            .execute(&self.db)
            .await
            .context("Failed to update OTP verified status")?;
        Ok(())
    }

    /// Updates password
    pub async fn update_password(&self, user_id: &str, password_hash: &str) -> Result<()> {
        sqlx::query(
            "UPDATE users SET password_hash = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?"
        )
        .bind(password_hash)
        .bind(user_id)
        .execute(&self.db)
        .await
        .context("Failed to update password")?;
        Ok(())
    }

    /// Ensures admin user exists
    pub async fn ensure_admin(&self) -> Result<()> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE id = 'admin'")
            .fetch_one(&self.db)
            .await
            .unwrap_or(0);

        if count > 0 {
            return Ok(());
        }

        let admin_user = User {
            id: "admin".to_string(),
            email: "admin@localhost".to_string(),
            password_hash: "".to_string(),
            otp_secret: "".to_string(),
            otp_verified: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        self.create(&admin_user).await?;
        Ok(())
    }
}