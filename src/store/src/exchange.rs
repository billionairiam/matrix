use std::sync::Arc;

use super::CryptoProvider;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{QueryBuilder, Row, SqlitePool, sqlite::SqliteRow};
use tracing::{debug, instrument};

/// Exchange exchange configuration
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Exchange {
    pub id: String,
    pub user_id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub type_: String, // "type" is a reserved keyword in Rust
    pub enabled: bool,
    #[serde(rename = "apiKey")]
    pub api_key: String,
    #[serde(rename = "secretKey")]
    pub secret_key: String,
    #[serde(rename = "passphrase")]
    pub passphrase: String, // OKX-specific
    pub testnet: bool,
    #[serde(rename = "hyperliquidWalletAddr")]
    pub hyperliquid_wallet_addr: String,
    #[serde(rename = "asterUser")]
    pub aster_user: String,
    #[serde(rename = "asterSigner")]
    pub aster_signer: String,
    #[serde(rename = "asterPrivateKey")]
    pub aster_private_key: String,
    #[serde(rename = "lighterWalletAddr")]
    pub lighter_wallet_addr: String,
    #[serde(rename = "lighterPrivateKey")]
    pub lighter_private_key: String,
    #[serde(rename = "lighterAPIKeyPrivateKey")]
    pub lighter_api_key_private_key: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// ExchangeStore exchange storage
#[derive(Clone)]
pub struct ExchangeStore {
    pool: SqlitePool,
    provider: Arc<dyn CryptoProvider>,
}

impl ExchangeStore {
    pub fn new(pool: SqlitePool, provider: Arc<dyn CryptoProvider>) -> Self {
        Self { pool, provider }
    }

    pub async fn init_tables(&self) -> Result<()> {
        // Main table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS exchanges (
                id TEXT NOT NULL,
                user_id TEXT NOT NULL DEFAULT 'default',
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                enabled BOOLEAN DEFAULT 0,
                api_key TEXT DEFAULT '',
                secret_key TEXT DEFAULT '',
                passphrase TEXT DEFAULT '',
                testnet BOOLEAN DEFAULT 0,
                hyperliquid_wallet_addr TEXT DEFAULT '',
                aster_user TEXT DEFAULT '',
                aster_signer TEXT DEFAULT '',
                aster_private_key TEXT DEFAULT '',
                lighter_wallet_addr TEXT DEFAULT '',
                lighter_private_key TEXT DEFAULT '',
                lighter_api_key_private_key TEXT DEFAULT '',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id, user_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("Failed to create exchanges table")?;

        // Migration: add passphrase column (if not exists)
        // SQLite doesn't support "ADD COLUMN IF NOT EXISTS" in older versions,
        // but sqlx usually handles duplicate column errors gracefully or we ignore the error.
        // A simple way is to check pragma or just try and ignore error.
        let _ = sqlx::query("ALTER TABLE exchanges ADD COLUMN passphrase TEXT DEFAULT ''")
            .execute(&self.pool)
            .await;

        // Trigger
        sqlx::query(
            r#"
            CREATE TRIGGER IF NOT EXISTS update_exchanges_updated_at
            AFTER UPDATE ON exchanges
            BEGIN
                UPDATE exchanges SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id AND user_id = NEW.user_id;
            END
            "#,
        )
        .execute(&self.pool)
        .await
        .context("Failed to create trigger")?;

        Ok(())
    }

    pub async fn init_default_data(&self) -> Result<()> {
        // No longer pre-populate exchanges - create on demand when user configures
        Ok(())
    }

    fn encrypt(&self, plaintext: &str) -> String {
        self.provider.encrypt(plaintext)
    }

    fn decrypt(&self, encrypted: &str) -> String {
        self.provider.decrypt(encrypted)
    }

    /// List gets user's exchange list
    pub async fn list(&self, user_id: &str) -> Result<Vec<Exchange>> {
        let rows = sqlx::query(
            r#"
            SELECT id, user_id, name, type, enabled, api_key, secret_key,
                   COALESCE(passphrase, '') as passphrase, testnet,
                   COALESCE(hyperliquid_wallet_addr, '') as hyperliquid_wallet_addr,
                   COALESCE(aster_user, '') as aster_user,
                   COALESCE(aster_signer, '') as aster_signer,
                   COALESCE(aster_private_key, '') as aster_private_key,
                   COALESCE(lighter_wallet_addr, '') as lighter_wallet_addr,
                   COALESCE(lighter_private_key, '') as lighter_private_key,
                   COALESCE(lighter_api_key_private_key, '') as lighter_api_key_private_key,
                   created_at, updated_at
            FROM exchanges WHERE user_id = ? ORDER BY id
            "#,
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await
        .context("Failed to list exchanges")?;

        let mut exchanges = Vec::new();
        for row in rows {
            let mut e = self.scan_exchange(&row)?;
            // Decrypt sensitive fields
            e.api_key = self.decrypt(&e.api_key);
            e.secret_key = self.decrypt(&e.secret_key);
            e.passphrase = self.decrypt(&e.passphrase);
            e.aster_private_key = self.decrypt(&e.aster_private_key);
            e.lighter_private_key = self.decrypt(&e.lighter_private_key);
            e.lighter_api_key_private_key = self.decrypt(&e.lighter_api_key_private_key);
            exchanges.push(e);
        }

        Ok(exchanges)
    }

    /// Update updates exchange configuration
    #[instrument(skip(
        self,
        api_key,
        secret_key,
        passphrase,
        aster_private_key,
        lighter_private_key,
        lighter_api_key_private_key
    ))]
    #[allow(clippy::too_many_arguments)]
    pub async fn update(
        &self,
        user_id: &str,
        id: &str,
        enabled: bool,
        api_key: &str,
        secret_key: &str,
        passphrase: &str,
        testnet: bool,
        hyperliquid_wallet_addr: &str,
        aster_user: &str,
        aster_signer: &str,
        aster_private_key: &str,
        lighter_wallet_addr: &str,
        lighter_private_key: &str,
        lighter_api_key_private_key: &str,
    ) -> Result<()> {
        debug!(
            "🔧 ExchangeStore.Update: userID={}, id={}, enabled={}",
            user_id, id, enabled
        );

        // Use QueryBuilder for dynamic update query
        let mut builder = QueryBuilder::new("UPDATE exchanges SET ");

        builder.push("enabled = ");
        builder.push_bind(enabled);

        builder.push(", testnet = ");
        builder.push_bind(testnet);

        builder.push(", hyperliquid_wallet_addr = ");
        builder.push_bind(hyperliquid_wallet_addr);

        builder.push(", aster_user = ");
        builder.push_bind(aster_user);

        builder.push(", aster_signer = ");
        builder.push_bind(aster_signer);

        builder.push(", lighter_wallet_addr = ");
        builder.push_bind(lighter_wallet_addr);

        builder.push(", updated_at = datetime('now')");

        // Conditional updates
        if !api_key.is_empty() {
            builder.push(", api_key = ");
            builder.push_bind(self.encrypt(api_key));
        }
        if !secret_key.is_empty() {
            builder.push(", secret_key = ");
            builder.push_bind(self.encrypt(secret_key));
        }
        if !passphrase.is_empty() {
            builder.push(", passphrase = ");
            builder.push_bind(self.encrypt(passphrase));
        }
        if !aster_private_key.is_empty() {
            builder.push(", aster_private_key = ");
            builder.push_bind(self.encrypt(aster_private_key));
        }
        if !lighter_private_key.is_empty() {
            builder.push(", lighter_private_key = ");
            builder.push_bind(self.encrypt(lighter_private_key));
        }
        if !lighter_api_key_private_key.is_empty() {
            builder.push(", lighter_api_key_private_key = ");
            builder.push_bind(self.encrypt(lighter_api_key_private_key));
        }

        builder.push(" WHERE id = ");
        builder.push_bind(id);
        builder.push(" AND user_id = ");
        builder.push_bind(user_id);

        let result = builder.build().execute(&self.pool).await?;

        if result.rows_affected() == 0 {
            // Create new record
            let (name, type_) = match id {
                "binance" => ("Binance Futures", "binance"),
                "bybit" => ("Bybit Futures", "bybit"),
                "okx" => ("OKX Futures", "okx"),
                "hyperliquid" => ("Hyperliquid", "hyperliquid"),
                "aster" => ("Aster DEX", "aster"),
                "lighter" => ("LIGHTER DEX", "lighter"),
                _ => (id, id), // simplified default case logic
            };

            // Note: In the default case, Go does `name = id + " Exchange"`.
            // Due to borrowing rules, we'll construct that if needed inside the query or prepare logic.
            let final_name = if name == id {
                format!("{} Exchange", id)
            } else {
                name.to_string()
            };

            sqlx::query(
                r#"
                INSERT INTO exchanges (id, user_id, name, type, enabled, api_key, secret_key, passphrase, testnet,
                                       hyperliquid_wallet_addr, aster_user, aster_signer, aster_private_key,
                                       lighter_wallet_addr, lighter_private_key, lighter_api_key_private_key, 
                                       created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
                "#
            )
            .bind(id)
            .bind(user_id)
            .bind(final_name)
            .bind(type_)
            .bind(enabled)
            .bind(self.encrypt(api_key))
            .bind(self.encrypt(secret_key))
            .bind(self.encrypt(passphrase))
            .bind(testnet)
            .bind(hyperliquid_wallet_addr)
            .bind(aster_user)
            .bind(aster_signer)
            .bind(self.encrypt(aster_private_key))
            .bind(lighter_wallet_addr)
            .bind(self.encrypt(lighter_private_key))
            .bind(self.encrypt(lighter_api_key_private_key))
            .execute(&self.pool)
            .await
            .context("Failed to insert new exchange on update")?;
        }

        Ok(())
    }

    /// Create creates exchange configuration
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        &self,
        user_id: &str,
        id: &str,
        name: &str,
        type_: &str,
        enabled: bool,
        api_key: &str,
        secret_key: &str,
        testnet: bool,
        hyperliquid_wallet_addr: &str,
        aster_user: &str,
        aster_signer: &str,
        aster_private_key: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT OR IGNORE INTO exchanges (id, user_id, name, type, enabled, api_key, secret_key, testnet,
                                             hyperliquid_wallet_addr, aster_user, aster_signer, aster_private_key,
                                             lighter_wallet_addr, lighter_private_key)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', '')
            "#
        )
        .bind(id)
        .bind(user_id)
        .bind(name)
        .bind(type_)
        .bind(enabled)
        .bind(self.encrypt(api_key))
        .bind(self.encrypt(secret_key))
        .bind(testnet)
        .bind(hyperliquid_wallet_addr)
        .bind(aster_user)
        .bind(aster_signer)
        .bind(self.encrypt(aster_private_key))
        .execute(&self.pool)
        .await
        .context("Failed to create exchange")?;

        Ok(())
    }

    // Helper to map row to struct
    fn scan_exchange(&self, row: &SqliteRow) -> Result<Exchange> {
        // Handle DateTime mapping from SQLite strings or standard Types
        let created_at: DateTime<Utc> = row.try_get("created_at").or_else(|_| {
            // Fallback for string parsing if SQLx type mapping doesn't auto-trigger
            let s: String = row.try_get("created_at")?;
            // Attempt simplistic parsing logic akin to Go's time.Parse
            // Ideally, SQLx with 'chrono' feature handles this.
            Ok::<DateTime<Utc>, sqlx::Error>(
                DateTime::parse_from_rfc3339(&s)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now()),
            )
        })?;

        let updated_at: DateTime<Utc> = row.try_get("updated_at").or_else(|_| {
            let s: String = row.try_get("updated_at")?;
            Ok::<DateTime<Utc>, sqlx::Error>(
                DateTime::parse_from_rfc3339(&s)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now()),
            )
        })?;

        Ok(Exchange {
            id: row.try_get("id")?,
            user_id: row.try_get("user_id")?,
            name: row.try_get("name")?,
            type_: row.try_get("type")?,
            enabled: row.try_get("enabled")?,
            api_key: row.try_get("api_key")?,
            secret_key: row.try_get("secret_key")?,
            passphrase: row.try_get("passphrase").unwrap_or_default(),
            testnet: row.try_get("testnet")?,
            hyperliquid_wallet_addr: row.try_get("hyperliquid_wallet_addr").unwrap_or_default(),
            aster_user: row.try_get("aster_user").unwrap_or_default(),
            aster_signer: row.try_get("aster_signer").unwrap_or_default(),
            aster_private_key: row.try_get("aster_private_key").unwrap_or_default(),
            lighter_wallet_addr: row.try_get("lighter_wallet_addr").unwrap_or_default(),
            lighter_private_key: row.try_get("lighter_private_key").unwrap_or_default(),
            lighter_api_key_private_key: row
                .try_get("lighter_api_key_private_key")
                .unwrap_or_default(),
            created_at,
            updated_at,
        })
    }
}
