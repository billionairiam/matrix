use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use std::collections::HashMap;

// ==========================================
// Struct Definitions
// ==========================================

/// Strategy strategy configuration
#[derive(Debug, Serialize, Deserialize, Clone, sqlx::FromRow)]
pub struct Strategy {
    pub id: String,
    pub user_id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub is_active: bool, // whether it is active (a user can only have one active strategy)
    #[serde(default)]
    pub is_default: bool, // whether it is a system default strategy
    #[serde(default)]
    pub config: String, // strategy configuration in JSON format
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// StrategyConfig strategy configuration details (JSON structure)
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StrategyConfig {
    #[serde(rename = "coin_source")]
    pub coin_source: CoinSourceConfig,
    #[serde(rename = "indicators")]
    pub indicators: IndicatorConfig,
    #[serde(rename = "custom_prompt", default)]
    pub custom_prompt: String,
    #[serde(rename = "risk_control")]
    pub risk_control: RiskControlConfig,
    #[serde(rename = "prompt_sections", default)]
    pub prompt_sections: PromptSectionsConfig,
}

// PromptSectionsConfig editable sections of System Prompt
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct PromptSectionsConfig {
    #[serde(rename = "role_definition", default)]
    pub role_definition: String,
    #[serde(rename = "trading_frequency", default)]
    pub trading_frequency: String,
    #[serde(rename = "entry_standards", default)]
    pub entry_standards: String,
    #[serde(rename = "decision_process", default)]
    pub decision_process: String,
}

// CoinSourceConfig coin source configuration
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CoinSourceConfig {
    #[serde(rename = "source_type")]
    pub source_type: String, // "static" | "coinpool" | "oi_top" | "mixed"
    #[serde(rename = "static_coins", default)]
    pub static_coins: Vec<String>,
    #[serde(rename = "use_coin_pool")]
    pub use_coin_pool: bool,
    #[serde(rename = "coin_pool_limit", default)]
    pub coin_pool_limit: i32,
    #[serde(rename = "coin_pool_api_url", default)]
    pub coin_pool_api_url: String,
    #[serde(rename = "use_oi_top")]
    pub use_oi_top: bool,
    #[serde(rename = "oi_top_limit", default)]
    pub oi_top_limit: i32,
    #[serde(rename = "oi_top_api_url", default)]
    pub oi_top_api_url: String,
}

// IndicatorConfig indicator configuration
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IndicatorConfig {
    pub klines: KlineConfig,
    #[serde(rename = "enable_ema")]
    pub enable_ema: bool,
    #[serde(rename = "enable_macd")]
    pub enable_macd: bool,
    #[serde(rename = "enable_rsi")]
    pub enable_rsi: bool,
    #[serde(rename = "enable_atr")]
    pub enable_atr: bool,
    #[serde(rename = "enable_volume")]
    pub enable_volume: bool,
    #[serde(rename = "enable_oi")]
    pub enable_oi: bool,
    #[serde(rename = "enable_funding_rate")]
    pub enable_funding_rate: bool,
    #[serde(rename = "ema_periods", default = "default_ema_periods")]
    pub ema_periods: Vec<i32>,
    #[serde(rename = "rsi_periods", default = "default_rsi_periods")]
    pub rsi_periods: Vec<i32>,
    #[serde(rename = "atr_periods", default = "default_atr_periods")]
    pub atr_periods: Vec<i32>,
    #[serde(rename = "external_data_sources", default)]
    pub external_data_sources: Vec<ExternalDataSource>,
    #[serde(rename = "enable_quant_data")]
    pub enable_quant_data: bool,
    #[serde(rename = "quant_data_api_url", default)]
    pub quant_data_api_url: String,
}

// KlineConfig K-line configuration
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct KlineConfig {
    #[serde(rename = "primary_timeframe")]
    pub primary_timeframe: String,
    #[serde(rename = "primary_count")]
    pub primary_count: i32,
    #[serde(rename = "longer_timeframe", default)]
    pub longer_timeframe: String,
    #[serde(rename = "longer_count", default)]
    pub longer_count: i32,
    #[serde(rename = "enable_multi_timeframe")]
    pub enable_multi_timeframe: bool,
    #[serde(rename = "selected_timeframes", default)]
    pub selected_timeframes: Vec<String>,
}

// ExternalDataSource external data source configuration
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExternalDataSource {
    pub name: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub url: String,
    pub method: String,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    #[serde(rename = "data_path", default)]
    pub data_path: String,
    #[serde(rename = "refresh_secs", default)]
    pub refresh_secs: i32,
}

// RiskControlConfig risk control configuration
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RiskControlConfig {
    #[serde(rename = "max_positions")]
    pub max_positions: i32,
    #[serde(rename = "btc_eth_max_leverage")]
    pub btc_eth_max_leverage: i32,
    #[serde(rename = "altcoin_max_leverage")]
    pub altcoin_max_leverage: i32,
    #[serde(rename = "min_risk_reward_ratio")]
    pub min_risk_reward_ratio: f64,
    #[serde(rename = "max_margin_usage")]
    pub max_margin_usage: f64,
    #[serde(rename = "max_position_ratio")]
    pub max_position_ratio: f64,
    #[serde(rename = "min_position_size")]
    pub min_position_size: f64,
    #[serde(rename = "min_confidence")]
    pub min_confidence: i32,
}

// Default values helpers
fn default_ema_periods() -> Vec<i32> {
    vec![20, 50]
}
fn default_rsi_periods() -> Vec<i32> {
    vec![7, 14]
}
fn default_atr_periods() -> Vec<i32> {
    vec![14]
}

// ==========================================
// StrategyStore
// ==========================================

#[derive(Clone)]
pub struct StrategyStore {
    db: SqlitePool,
}

impl StrategyStore {
    pub fn new(db: SqlitePool) -> Self {
        Self { db }
    }

    /// Initializes strategy tables
    pub async fn init_tables(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS strategies (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL DEFAULT '',
                name TEXT NOT NULL,
                description TEXT DEFAULT '',
                is_active BOOLEAN DEFAULT 0,
                is_default BOOLEAN DEFAULT 0,
                config TEXT NOT NULL DEFAULT '{}',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            "#,
        )
        .execute(&self.db)
        .await
        .context("Failed to create strategies table")?;

        // create indexes
        let _ =
            sqlx::query("CREATE INDEX IF NOT EXISTS idx_strategies_user_id ON strategies(user_id)")
                .execute(&self.db)
                .await;
        let _ = sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_strategies_is_active ON strategies(is_active)",
        )
        .execute(&self.db)
        .await;

        // trigger
        sqlx::query(
            r#"
            CREATE TRIGGER IF NOT EXISTS update_strategies_updated_at
            AFTER UPDATE ON strategies
            BEGIN
                UPDATE strategies SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
            END
            "#,
        )
        .execute(&self.db)
        .await
        .context("Failed to create trigger")?;

        Ok(())
    }

    pub async fn init_default_data(&self) -> Result<()> {
        Ok(())
    }

    /// Gets the default strategy configuration for the given language
    pub fn get_default_strategy_config(lang: &str) -> StrategyConfig {
        let mut config = StrategyConfig {
            coin_source: CoinSourceConfig {
                source_type: "coinpool".to_string(),
                static_coins: vec![],
                use_coin_pool: true,
                coin_pool_limit: 30,
                coin_pool_api_url: "http://nofxaios.com:30006/api/ai500/list?auth=cm_568c67eae410d912c54c".to_string(),
                use_oi_top: false,
                oi_top_limit: 0,
                oi_top_api_url: String::new(),
            },
            indicators: IndicatorConfig {
                klines: KlineConfig {
                    primary_timeframe: "5m".to_string(),
                    primary_count: 30,
                    longer_timeframe: "4h".to_string(),
                    longer_count: 10,
                    enable_multi_timeframe: true,
                    selected_timeframes: vec!["5m".to_string(), "15m".to_string(), "1h".to_string(), "4h".to_string()],
                },
                enable_ema: true,
                enable_macd: true,
                enable_rsi: true,
                enable_atr: true,
                enable_volume: true,
                enable_oi: true,
                enable_funding_rate: true,
                ema_periods: vec![20, 50],
                rsi_periods: vec![7, 14],
                atr_periods: vec![14],
                external_data_sources: vec![],
                enable_quant_data: true,
                quant_data_api_url: "http://nofxaios.com:30006/api/coin/{symbol}?include=netflow,oi,price&auth=cm_568c67eae410d912c54c".to_string(),
            },
            custom_prompt: String::new(),
            risk_control: RiskControlConfig {
                max_positions: 3,
                btc_eth_max_leverage: 5,
                altcoin_max_leverage: 5,
                min_risk_reward_ratio: 3.0,
                max_margin_usage: 0.9,
                max_position_ratio: 1.5,
                min_position_size: 12.0,
                min_confidence: 75,
            },
            prompt_sections: PromptSectionsConfig::default(),
        };

        if lang == "zh" {
            config.prompt_sections = PromptSectionsConfig {
                role_definition: "# 你是一个专业的加密货币交易AI\n\n你的任务是根据提供的市场数据做出交易决策。你是一个经验丰富的量化交易员，擅长技术分析和风险管理。".to_string(),
                trading_frequency: "# ⏱️ 交易频率意识\n\n- 优秀交易员：每天2-4笔 ≈ 每小时0.1-0.2笔\n- 每小时超过2笔 = 过度交易\n- 单笔持仓时间 ≥ 30-60分钟\n如果你发现自己每个周期都在交易 → 标准太低；如果持仓不到30分钟就平仓 → 太冲动。".to_string(),
                entry_standards: "# 🎯 入场标准（严格）\n\n只在多个信号共振时入场。自由使用任何有效的分析方法，避免单一指标、信号矛盾、横盘震荡、或平仓后立即重新开仓等低质量行为。".to_string(),
                decision_process: "# 📋 决策流程\n\n1. 检查持仓 → 是否止盈/止损\n2. 扫描候选币种 + 多时间框架 → 是否存在强信号\n3. 先写思维链，再输出结构化JSON".to_string(),
            };
        } else {
            config.prompt_sections = PromptSectionsConfig {
                role_definition: "# You are a professional cryptocurrency trading AI\n\nYour task is to make trading decisions based on the provided market data. You are an experienced quantitative trader skilled in technical analysis and risk management.".to_string(),
                trading_frequency: "# ⏱️ Trading Frequency Awareness\n\n- Excellent trader: 2-4 trades per day ≈ 0.1-0.2 trades per hour\n- >2 trades per hour = overtrading\n- Single position holding time ≥ 30-60 minutes\nIf you find yourself trading every cycle → standards are too low; if closing positions in <30 minutes → too impulsive.".to_string(),
                entry_standards: "# 🎯 Entry Standards (Strict)\n\nOnly enter positions when multiple signals resonate. Freely use any effective analysis methods, avoid low-quality behaviors such as single indicators, contradictory signals, sideways oscillation, or immediately restarting after closing positions.".to_string(),
                decision_process: "# 📋 Decision Process\n\n1. Check positions → whether to take profit/stop loss\n2. Scan candidate coins + multi-timeframe → whether strong signals exist\n3. Write chain of thought first, then output structured JSON".to_string(),
            };
        }

        config
    }

    /// Create create a strategy
    pub async fn create(&self, strategy: &Strategy) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO strategies (id, user_id, name, description, is_active, is_default, config)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&strategy.id)
        .bind(&strategy.user_id)
        .bind(&strategy.name)
        .bind(&strategy.description)
        .bind(strategy.is_active)
        .bind(strategy.is_default)
        .bind(&strategy.config)
        .execute(&self.db)
        .await
        .context("Failed to create strategy")?;

        Ok(())
    }

    /// Update update a strategy
    pub async fn update(&self, strategy: &Strategy) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE strategies SET
                name = ?, description = ?, config = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND user_id = ?
            "#,
        )
        .bind(&strategy.name)
        .bind(&strategy.description)
        .bind(&strategy.config)
        .bind(&strategy.id)
        .bind(&strategy.user_id)
        .execute(&self.db)
        .await
        .context("Failed to update strategy")?;

        Ok(())
    }

    /// Delete delete a strategy
    pub async fn delete(&self, user_id: &str, id: &str) -> Result<()> {
        let is_default: Option<bool> =
            sqlx::query_scalar("SELECT is_default FROM strategies WHERE id = ?")
                .bind(id)
                .fetch_optional(&self.db)
                .await?;

        if let Some(true) = is_default {
            return Err(anyhow::anyhow!("cannot delete system default strategy"));
        }

        sqlx::query("DELETE FROM strategies WHERE id = ? AND user_id = ?")
            .bind(id)
            .bind(user_id)
            .execute(&self.db)
            .await
            .context("Failed to delete strategy")?;

        Ok(())
    }

    /// List get user's strategy list
    pub async fn list(&self, user_id: &str) -> Result<Vec<Strategy>> {
        let strategies = sqlx::query_as::<_, Strategy>(
            r#"
            SELECT id, user_id, name, description, is_active, is_default, config, created_at, updated_at
            FROM strategies
            WHERE user_id = ? OR is_default = 1
            ORDER BY is_default DESC, created_at DESC
            "#,
        )
        .bind(user_id)
        .fetch_all(&self.db)
        .await
        .context("Failed to list strategies")?;

        Ok(strategies)
    }

    /// Get get a single strategy
    pub async fn get(&self, user_id: &str, id: &str) -> Result<Strategy> {
        let strategy = sqlx::query_as::<_, Strategy>(
            r#"
            SELECT id, user_id, name, description, is_active, is_default, config, created_at, updated_at
            FROM strategies
            WHERE id = ? AND (user_id = ? OR is_default = 1)
            "#,
        )
        .bind(id)
        .bind(user_id)
        .fetch_one(&self.db)
        .await
        .context("Failed to get strategy")?;

        Ok(strategy)
    }

    /// GetActive get user's currently active strategy
    pub async fn get_active(&self, user_id: &str) -> Result<Strategy> {
        let strategy_res = sqlx::query_as::<_, Strategy>(
            r#"
            SELECT id, user_id, name, description, is_active, is_default, config, created_at, updated_at
            FROM strategies
            WHERE user_id = ? AND is_active = 1
            "#,
        )
        .bind(user_id)
        .fetch_one(&self.db)
        .await;

        match strategy_res {
            Ok(s) => Ok(s),
            Err(sqlx::Error::RowNotFound) => self.get_default().await,
            Err(e) => Err(anyhow::Error::new(e)),
        }
    }

    /// GetDefault get system default strategy
    pub async fn get_default(&self) -> Result<Strategy> {
        let strategy = sqlx::query_as::<_, Strategy>(
            r#"
            SELECT id, user_id, name, description, is_active, is_default, config, created_at, updated_at
            FROM strategies
            WHERE is_default = 1
            LIMIT 1
            "#,
        )
        .fetch_one(&self.db)
        .await
        .context("Failed to get default strategy")?;

        Ok(strategy)
    }

    /// SetActive set active strategy (will first deactivate other strategies)
    pub async fn set_active(&self, user_id: &str, strategy_id: &str) -> Result<()> {
        let mut tx = self
            .db
            .begin()
            .await
            .context("Failed to begin transaction")?;

        // first deactivate all strategies for the user
        sqlx::query("UPDATE strategies SET is_active = 0 WHERE user_id = ?")
            .bind(user_id)
            .execute(&mut *tx)
            .await
            .context("Failed to deactivate strategies")?;

        // activate specified strategy
        sqlx::query(
            "UPDATE strategies SET is_active = 1 WHERE id = ? AND (user_id = ? OR is_default = 1)",
        )
        .bind(strategy_id)
        .bind(user_id)
        .execute(&mut *tx)
        .await
        .context("Failed to activate strategy")?;

        tx.commit().await.context("Failed to commit transaction")?;

        Ok(())
    }

    /// Duplicate duplicate a strategy
    pub async fn duplicate(
        &self,
        user_id: &str,
        source_id: &str,
        new_id: &str,
        new_name: &str,
    ) -> Result<()> {
        // get source strategy
        let source = self
            .get(user_id, source_id)
            .await
            .context("Failed to get source strategy")?;

        // create new strategy
        let new_strategy = Strategy {
            id: new_id.to_string(),
            user_id: user_id.to_string(),
            name: new_name.to_string(),
            description: format!("Created based on [{}]", source.name),
            is_active: false,
            is_default: false,
            config: source.config,
            created_at: Utc::now(), // Not really used in INSERT as DB uses DEFAULT CURRENT_TIMESTAMP, but useful for struct completeness
            updated_at: Utc::now(),
        };

        self.create(&new_strategy).await
    }
}

// ==========================================
// Struct Methods Helpers
// ==========================================

impl Strategy {
    pub fn parse_config(&self) -> Result<StrategyConfig> {
        let config: StrategyConfig =
            serde_json::from_str(&self.config).context("Failed to parse strategy configuration")?;
        Ok(config)
    }

    pub fn set_config(&mut self, config: &StrategyConfig) -> Result<()> {
        let data =
            serde_json::to_string(config).context("Failed to serialize strategy configuration")?;
        self.config = data;
        Ok(())
    }
}
