use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};
use std::sync::Arc;

use super::CryptoFunc;
use super::ai_model::AIModel;
use super::exchange::Exchange;
use super::strategy::Strategy;

// ==========================================
// Struct Definitions
// ==========================================

/// Trader trader configuration
#[derive(Debug, Serialize, Deserialize, Clone, sqlx::FromRow)]
pub struct Trader {
    pub id: String,
    pub user_id: String,
    pub name: String,
    pub ai_model_id: String,
    pub exchange_id: String,
    #[serde(default)]
    pub strategy_id: String, // Associated strategy ID
    pub initial_balance: f64,
    pub scan_interval_minutes: i32,
    pub is_running: bool,
    #[serde(default = "default_true")]
    pub is_cross_margin: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    // Deprecated fields kept for compatibility
    #[serde(default = "default_leverage")]
    pub btc_eth_leverage: i32,
    #[serde(default = "default_leverage")]
    pub altcoin_leverage: i32,
    #[serde(default)]
    pub trading_symbols: String,
    #[serde(default)]
    pub use_coin_pool: bool,
    #[serde(default)]
    pub use_oi_top: bool,
    #[serde(default)]
    pub custom_prompt: String,
    #[serde(default)]
    pub override_base_prompt: bool,
    #[serde(default = "default_prompt_template")]
    pub system_prompt_template: String,
}

fn default_true() -> bool {
    true
}
fn default_leverage() -> i32 {
    5
}
fn default_prompt_template() -> String {
    "default".to_string()
}

/// TraderFullConfig trader full configuration (includes AI model, exchange and strategy)
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TraderFullConfig {
    pub trader: Trader,
    pub ai_model: AIModel,
    pub exchange: Exchange,
    pub strategy: Option<Strategy>,
}

/// TraderStore trader storage
#[derive(Clone)]
pub struct TraderStore {
    db: SqlitePool,
    decrypt_func: Option<Arc<CryptoFunc>>,
}

// ==========================================
// Implementation
// ==========================================

impl TraderStore {
    pub fn new(db: SqlitePool, decrypt_func: Option<CryptoFunc>) -> Self {
        Self {
            db,
            decrypt_func: decrypt_func.map(Arc::new),
        }
    }

    /// Initializes trader tables
    pub async fn init_tables(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS traders (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL DEFAULT 'default',
                name TEXT NOT NULL,
                ai_model_id TEXT NOT NULL,
                exchange_id TEXT NOT NULL,
                initial_balance REAL NOT NULL,
                scan_interval_minutes INTEGER DEFAULT 3,
                is_running BOOLEAN DEFAULT 0,
                btc_eth_leverage INTEGER DEFAULT 5,
                altcoin_leverage INTEGER DEFAULT 5,
                trading_symbols TEXT DEFAULT '',
                use_coin_pool BOOLEAN DEFAULT 0,
                use_oi_top BOOLEAN DEFAULT 0,
                custom_prompt TEXT DEFAULT '',
                override_base_prompt BOOLEAN DEFAULT 0,
                system_prompt_template TEXT DEFAULT 'default',
                is_cross_margin BOOLEAN DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            "#,
        )
        .execute(&self.db)
        .await
        .context("Failed to create traders table")?;

        // Trigger
        sqlx::query(
            r#"
            CREATE TRIGGER IF NOT EXISTS update_traders_updated_at
            AFTER UPDATE ON traders
            BEGIN
                UPDATE traders SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
            END
            "#,
        )
        .execute(&self.db)
        .await
        .context("Failed to create trigger")?;

        // Backward compatibility: add columns if not exist
        // Note: SQLx/SQLite doesn't support "ADD COLUMN IF NOT EXISTS" directly in all versions.
        // We attempt to add and ignore errors or check pragma.
        // A simple way is to run them sequentially and ignore errors.
        let alter_queries = vec![
            "ALTER TABLE traders ADD COLUMN custom_prompt TEXT DEFAULT ''",
            "ALTER TABLE traders ADD COLUMN override_base_prompt BOOLEAN DEFAULT 0",
            "ALTER TABLE traders ADD COLUMN is_cross_margin BOOLEAN DEFAULT 1",
            "ALTER TABLE traders ADD COLUMN btc_eth_leverage INTEGER DEFAULT 5",
            "ALTER TABLE traders ADD COLUMN altcoin_leverage INTEGER DEFAULT 5",
            "ALTER TABLE traders ADD COLUMN trading_symbols TEXT DEFAULT ''",
            "ALTER TABLE traders ADD COLUMN use_coin_pool BOOLEAN DEFAULT 0",
            "ALTER TABLE traders ADD COLUMN use_oi_top BOOLEAN DEFAULT 0",
            "ALTER TABLE traders ADD COLUMN system_prompt_template TEXT DEFAULT 'default'",
            "ALTER TABLE traders ADD COLUMN strategy_id TEXT DEFAULT ''",
        ];

        for query in alter_queries {
            let _ = sqlx::query(query).execute(&self.db).await;
        }

        Ok(())
    }

    fn decrypt(&self, encrypted: &str) -> String {
        if let Some(func) = &self.decrypt_func {
            func(encrypted)
        } else {
            encrypted.to_string()
        }
    }

    /// Creates trader
    pub async fn create(&self, trader: &Trader) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO traders (id, user_id, name, ai_model_id, exchange_id, strategy_id, initial_balance,
                                 scan_interval_minutes, is_running, is_cross_margin,
                                 btc_eth_leverage, altcoin_leverage, trading_symbols, use_coin_pool,
                                 use_oi_top, custom_prompt, override_base_prompt, system_prompt_template)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&trader.id)
        .bind(&trader.user_id)
        .bind(&trader.name)
        .bind(&trader.ai_model_id)
        .bind(&trader.exchange_id)
        .bind(&trader.strategy_id)
        .bind(trader.initial_balance)
        .bind(trader.scan_interval_minutes)
        .bind(trader.is_running)
        .bind(trader.is_cross_margin)
        .bind(trader.btc_eth_leverage)
        .bind(trader.altcoin_leverage)
        .bind(&trader.trading_symbols)
        .bind(trader.use_coin_pool)
        .bind(trader.use_oi_top)
        .bind(&trader.custom_prompt)
        .bind(trader.override_base_prompt)
        .bind(&trader.system_prompt_template)
        .execute(&self.db)
        .await
        .context("Failed to create trader")?;

        Ok(())
    }

    /// Lists user's trader list
    pub async fn list(&self, user_id: &str) -> Result<Vec<Trader>> {
        let traders = sqlx::query_as::<_, Trader>(
            r#"
            SELECT id, user_id, name, ai_model_id, exchange_id, COALESCE(strategy_id, '') as strategy_id,
                   initial_balance, scan_interval_minutes, is_running, COALESCE(is_cross_margin, 1) as is_cross_margin,
                   COALESCE(btc_eth_leverage, 5) as btc_eth_leverage, COALESCE(altcoin_leverage, 5) as altcoin_leverage, COALESCE(trading_symbols, '') as trading_symbols,
                   COALESCE(use_coin_pool, 0) as use_coin_pool, COALESCE(use_oi_top, 0) as use_oi_top, COALESCE(custom_prompt, '') as custom_prompt,
                   COALESCE(override_base_prompt, 0) as override_base_prompt, COALESCE(system_prompt_template, 'default') as system_prompt_template,
                   created_at, updated_at
            FROM traders WHERE user_id = ? ORDER BY created_at DESC
            "#,
        )
        .bind(user_id)
        .fetch_all(&self.db)
        .await
        .context("Failed to list traders")?;

        Ok(traders)
    }

    /// Updates trader running status
    pub async fn update_status(&self, user_id: &str, id: &str, is_running: bool) -> Result<()> {
        sqlx::query("UPDATE traders SET is_running = ? WHERE id = ? AND user_id = ?")
            .bind(is_running)
            .bind(id)
            .bind(user_id)
            .execute(&self.db)
            .await
            .context("Failed to update status")?;
        Ok(())
    }

    /// Updates trader configuration
    pub async fn update(&self, trader: &Trader) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE traders SET
                name = ?, ai_model_id = ?, exchange_id = ?, strategy_id = ?,
                scan_interval_minutes = ?, is_cross_margin = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND user_id = ?
            "#,
        )
        .bind(&trader.name)
        .bind(&trader.ai_model_id)
        .bind(&trader.exchange_id)
        .bind(&trader.strategy_id)
        .bind(trader.scan_interval_minutes)
        .bind(trader.is_cross_margin)
        .bind(&trader.id)
        .bind(&trader.user_id)
        .execute(&self.db)
        .await
        .context("Failed to update trader")?;

        Ok(())
    }

    /// Updates initial balance
    pub async fn update_initial_balance(
        &self,
        user_id: &str,
        id: &str,
        new_balance: f64,
    ) -> Result<()> {
        sqlx::query("UPDATE traders SET initial_balance = ? WHERE id = ? AND user_id = ?")
            .bind(new_balance)
            .bind(id)
            .bind(user_id)
            .execute(&self.db)
            .await
            .context("Failed to update initial balance")?;
        Ok(())
    }

    /// Updates custom prompt
    pub async fn update_custom_prompt(
        &self,
        user_id: &str,
        id: &str,
        custom_prompt: &str,
        override_base: bool,
    ) -> Result<()> {
        sqlx::query("UPDATE traders SET custom_prompt = ?, override_base_prompt = ? WHERE id = ? AND user_id = ?")
            .bind(custom_prompt)
            .bind(override_base)
            .bind(id)
            .bind(user_id)
            .execute(&self.db)
            .await
            .context("Failed to update custom prompt")?;
        Ok(())
    }

    /// Deletes trader
    pub async fn delete(&self, user_id: &str, id: &str) -> Result<()> {
        sqlx::query("DELETE FROM traders WHERE id = ? AND user_id = ?")
            .bind(id)
            .bind(user_id)
            .execute(&self.db)
            .await
            .context("Failed to delete trader")?;
        Ok(())
    }

    /// Gets trader full configuration
    pub async fn get_full_config(
        &self,
        user_id: &str,
        trader_id: &str,
    ) -> Result<TraderFullConfig> {
        // We use a join query and map manually because sqlx_from_row is tricky with joins and name collisions
        let row = sqlx::query(
            r#"
            SELECT
                t.id as t_id, t.user_id as t_user_id, t.name as t_name, t.ai_model_id, t.exchange_id, COALESCE(t.strategy_id, '') as t_strategy_id,
                t.initial_balance, t.scan_interval_minutes, t.is_running, COALESCE(t.is_cross_margin, 1) as is_cross_margin,
                COALESCE(t.btc_eth_leverage, 5) as btc_eth_leverage, COALESCE(t.altcoin_leverage, 5) as altcoin_leverage, COALESCE(t.trading_symbols, '') as trading_symbols,
                COALESCE(t.use_coin_pool, 0) as use_coin_pool, COALESCE(t.use_oi_top, 0) as use_oi_top, COALESCE(t.custom_prompt, '') as custom_prompt,
                COALESCE(t.override_base_prompt, 0) as override_base_prompt, COALESCE(t.system_prompt_template, 'default') as system_prompt_template,
                t.created_at as t_created_at, t.updated_at as t_updated_at,
                
                a.id as a_id, a.user_id as a_user_id, a.name as a_name, a.provider, a.enabled as a_enabled, a.api_key as a_api_key,
                COALESCE(a.custom_api_url, '') as custom_api_url, COALESCE(a.custom_model_name, '') as custom_model_name, a.created_at as a_created_at, a.updated_at as a_updated_at,
                
                e.id as e_id, e.user_id as e_user_id, e.name as e_name, e.type as e_type, e.enabled as e_enabled, e.api_key as e_api_key, 
                e.secret_key as e_secret_key, COALESCE(e.passphrase, '') as e_passphrase, e.testnet as e_testnet,
                COALESCE(e.hyperliquid_wallet_addr, '') as hyperliquid_wallet_addr, COALESCE(e.aster_user, '') as aster_user, 
                COALESCE(e.aster_signer, '') as aster_signer, COALESCE(e.aster_private_key, '') as aster_private_key, 
                COALESCE(e.lighter_wallet_addr, '') as lighter_wallet_addr, COALESCE(e.lighter_private_key, '') as lighter_private_key,
                COALESCE(e.lighter_api_key_private_key, '') as lighter_api_key_private_key, e.created_at as e_created_at, e.updated_at as e_updated_at
            FROM traders t
            JOIN ai_models a ON t.ai_model_id = a.id AND t.user_id = a.user_id
            JOIN exchanges e ON t.exchange_id = e.id AND t.user_id = e.user_id
            WHERE t.id = ? AND t.user_id = ?
            "#,
        )
        .bind(trader_id)
        .bind(user_id)
        .fetch_one(&self.db)
        .await
        .context("Failed to get full config")?;

        // Helper to parse DateTime safely
        let parse_time = |key: &str| -> DateTime<Utc> {
            row.try_get::<DateTime<Utc>, _>(key).unwrap_or_else(|_| {
                let s: String = row.get(key);
                DateTime::parse_from_rfc3339(&s)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now())
            })
        };

        let trader = Trader {
            id: row.try_get("t_id")?,
            user_id: row.try_get("t_user_id")?,
            name: row.try_get("t_name")?,
            ai_model_id: row.try_get("ai_model_id")?,
            exchange_id: row.try_get("exchange_id")?,
            strategy_id: row.try_get("t_strategy_id")?,
            initial_balance: row.try_get("initial_balance")?,
            scan_interval_minutes: row.try_get("scan_interval_minutes")?,
            is_running: row.try_get("is_running")?,
            is_cross_margin: row.try_get("is_cross_margin")?,
            created_at: parse_time("t_created_at"),
            updated_at: parse_time("t_updated_at"),
            btc_eth_leverage: row.try_get("btc_eth_leverage")?,
            altcoin_leverage: row.try_get("altcoin_leverage")?,
            trading_symbols: row.try_get("trading_symbols")?,
            use_coin_pool: row.try_get("use_coin_pool")?,
            use_oi_top: row.try_get("use_oi_top")?,
            custom_prompt: row.try_get("custom_prompt")?,
            override_base_prompt: row.try_get("override_base_prompt")?,
            system_prompt_template: row.try_get("system_prompt_template")?,
        };

        let mut ai_model = AIModel {
            id: row.try_get("a_id")?,
            user_id: row.try_get("a_user_id")?,
            name: row.try_get("a_name")?,
            provider: row.try_get("provider")?,
            enabled: row.try_get("a_enabled")?,
            api_key: row.try_get("a_api_key")?,
            custom_api_url: row.try_get("custom_api_url")?,
            custom_model_name: row.try_get("custom_model_name")?,
            created_at: parse_time("a_created_at"),
            updated_at: parse_time("a_updated_at"),
        };

        let mut exchange = Exchange {
            id: row.try_get("e_id")?,
            user_id: row.try_get("e_user_id")?,
            name: row.try_get("e_name")?,
            type_: row.try_get("e_type")?,
            enabled: row.try_get("e_enabled")?,
            api_key: row.try_get("e_api_key")?,
            secret_key: row.try_get("e_secret_key")?,
            passphrase: row.try_get("e_passphrase")?,
            testnet: row.try_get("e_testnet")?,
            hyperliquid_wallet_addr: row.try_get("hyperliquid_wallet_addr")?,
            aster_user: row.try_get("aster_user")?,
            aster_signer: row.try_get("aster_signer")?,
            aster_private_key: row.try_get("aster_private_key")?,
            lighter_wallet_addr: row.try_get("lighter_wallet_addr")?,
            lighter_private_key: row.try_get("lighter_private_key")?,
            lighter_api_key_private_key: row.try_get("lighter_api_key_private_key")?,
            created_at: parse_time("e_created_at"),
            updated_at: parse_time("e_updated_at"),
        };

        // Decrypt
        ai_model.api_key = self.decrypt(&ai_model.api_key);
        exchange.api_key = self.decrypt(&exchange.api_key);
        exchange.secret_key = self.decrypt(&exchange.secret_key);
        exchange.passphrase = self.decrypt(&exchange.passphrase);
        exchange.aster_private_key = self.decrypt(&exchange.aster_private_key);
        exchange.lighter_private_key = self.decrypt(&exchange.lighter_private_key);
        exchange.lighter_api_key_private_key = self.decrypt(&exchange.lighter_api_key_private_key);

        // Load associated strategy
        let mut strategy: Option<Strategy> = None;
        if !trader.strategy_id.is_empty() {
            strategy = self
                .get_strategy_by_id(user_id, &trader.strategy_id)
                .await
                .ok();
        }
        // If no associated strategy, get user's active strategy or default strategy
        if strategy.is_none() {
            strategy = self.get_active_or_default_strategy(user_id).await.ok();
        }

        Ok(TraderFullConfig {
            trader,
            ai_model,
            exchange,
            strategy,
        })
    }

    /// Internal method: gets strategy by ID
    async fn get_strategy_by_id(&self, user_id: &str, strategy_id: &str) -> Result<Strategy> {
        let strategy = sqlx::query_as::<_, Strategy>(
            r#"
            SELECT id, user_id, name, description, is_active, is_default, config, created_at, updated_at
            FROM strategies WHERE id = ? AND (user_id = ? OR is_default = 1)
            "#,
        )
        .bind(strategy_id)
        .bind(user_id)
        .fetch_one(&self.db)
        .await?;
        Ok(strategy)
    }

    /// Internal method: gets user's active strategy or system default strategy
    async fn get_active_or_default_strategy(&self, user_id: &str) -> Result<Strategy> {
        // First try to get user's active strategy
        let strategy = sqlx::query_as::<_, Strategy>(
            r#"
            SELECT id, user_id, name, description, is_active, is_default, config, created_at, updated_at
            FROM strategies WHERE user_id = ? AND is_active = 1
            "#,
        )
        .bind(user_id)
        .fetch_optional(&self.db)
        .await?;

        if let Some(s) = strategy {
            return Ok(s);
        }

        // Fallback to system default strategy
        let default_strategy = sqlx::query_as::<_, Strategy>(
            r#"
            SELECT id, user_id, name, description, is_active, is_default, config, created_at, updated_at
            FROM strategies WHERE is_default = 1 LIMIT 1
            "#,
        )
        .fetch_one(&self.db)
        .await?;

        Ok(default_strategy)
    }

    /// Lists all users' trader list
    pub async fn list_all(&self) -> Result<Vec<Trader>> {
        let traders = sqlx::query_as::<_, Trader>(
            r#"
            SELECT id, user_id, name, ai_model_id, exchange_id, COALESCE(strategy_id, '') as strategy_id,
                   initial_balance, scan_interval_minutes, is_running, COALESCE(is_cross_margin, 1) as is_cross_margin,
                   COALESCE(btc_eth_leverage, 5) as btc_eth_leverage, COALESCE(altcoin_leverage, 5) as altcoin_leverage, COALESCE(trading_symbols, '') as trading_symbols,
                   COALESCE(use_coin_pool, 0) as use_coin_pool, COALESCE(use_oi_top, 0) as use_oi_top, COALESCE(custom_prompt, '') as custom_prompt,
                   COALESCE(override_base_prompt, 0) as override_base_prompt, COALESCE(system_prompt_template, 'default') as system_prompt_template,
                   created_at, updated_at
            FROM traders ORDER BY created_at DESC
            "#,
        )
        .fetch_all(&self.db)
        .await
        .context("Failed to list all traders")?;

        Ok(traders)
    }
}
