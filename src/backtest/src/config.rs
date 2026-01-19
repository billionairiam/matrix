use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use market::data::normalize;
use market::timeframe::normalize_timeframe;

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub enum FillPolicy {
    #[default]
    NextOpen,
    BarVWAP,
    MidPrice,
}

impl FillPolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            FillPolicy::NextOpen => "NextOpen",
            FillPolicy::BarVWAP => "BarVWAP",
            FillPolicy::MidPrice => "MidPrice",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AIConfig {
    #[serde(default)]
    pub provider: String,

    #[serde(default)]
    pub model: String,

    #[serde(rename = "key", default)]
    pub api_key: String,

    #[serde(
        rename = "secret_key",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub secret_key: String,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub base_url: String,

    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub temperature: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeverageConfig {
    #[serde(rename = "btc_eth_leverage", default)]
    pub btc_eth_leverage: i32,

    #[serde(rename = "altcoin_leverage", default)]
    pub altcoin_leverage: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestConfig {
    pub run_id: String,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub user_id: String,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub ai_model_id: String,

    #[serde(default)]
    pub symbols: Vec<String>,

    #[serde(default)]
    pub timeframes: Vec<String>,

    #[serde(default)]
    pub decision_timeframe: String,

    #[serde(default)]
    pub decision_cadence_nbars: i32,

    pub start_ts: i64,
    pub end_ts: i64,

    pub initial_balance: f64,
    pub fee_bps: f64,
    pub slippage_bps: f64,

    #[serde(default)]
    pub fill_policy: FillPolicy,

    #[serde(default)]
    pub prompt_variant: String,
    #[serde(default)]
    pub prompt_template: String,
    #[serde(default)]
    pub custom_prompt: String,

    #[serde(rename = "override_prompt", default)]
    pub override_base_prompt: bool,

    #[serde(default)]
    pub cache_ai: bool,
    #[serde(default)]
    pub replay_only: bool,

    #[serde(rename = "ai", default = "default_ai_config")]
    pub ai_cfg: AIConfig,

    #[serde(default = "default_leverage_config")]
    pub leverage: LeverageConfig,

    #[serde(
        rename = "ai_cache_path",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub shared_ai_cache_path: String,

    #[serde(default, skip_serializing_if = "is_zero_int")]
    pub checkpoint_interval_bars: i32,

    #[serde(default, skip_serializing_if = "is_zero_int")]
    pub checkpoint_interval_seconds: i32,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub replay_decision_dir: String,
}

// Helpers for Serde defaults and skips
fn is_zero_f64(v: &f64) -> bool {
    *v == 0.0
}
fn is_zero_int(v: &i32) -> bool {
    *v == 0
}
fn default_ai_config() -> AIConfig {
    AIConfig {
        provider: String::new(),
        model: String::new(),
        api_key: String::new(),
        secret_key: String::new(),
        base_url: String::new(),
        temperature: 0.0,
    }
}
fn default_leverage_config() -> LeverageConfig {
    LeverageConfig {
        btc_eth_leverage: 0,
        altcoin_leverage: 0,
    }
}

impl BacktestConfig {
    /// Validate performs validity checks on the configuration and fills in default values.
    /// In Rust, we use `&mut self` to allow modification during validation, matching the Go logic.
    pub fn validate(&mut self) -> Result<()> {
        self.run_id = self.run_id.trim().to_string();
        if self.run_id.is_empty() {
            return Err(anyhow!("run_id cannot be empty"));
        }

        self.user_id = self.user_id.trim().to_string();
        if self.user_id.is_empty() {
            self.user_id = "default".to_string();
        }

        self.ai_model_id = self.ai_model_id.trim().to_string();

        if self.symbols.is_empty() {
            return Err(anyhow!("at least one symbol is required"));
        }

        // Normalize symbols
        for sym in &mut self.symbols {
            *sym = normalize(sym);
        }

        // Handle timeframes
        if self.timeframes.is_empty() {
            self.timeframes = vec!["3m".to_string(), "15m".to_string(), "4h".to_string()];
        }

        let mut norm_tf = Vec::with_capacity(self.timeframes.len());
        for tf in &self.timeframes {
            let normalized = normalize_timeframe(tf)
                .map_err(|e| anyhow!("invalid timeframe '{}': {}", tf, e))?;
            norm_tf.push(normalized);
        }
        self.timeframes = norm_tf;

        // Handle decision timeframe
        if self.decision_timeframe.is_empty() {
            self.decision_timeframe = self.timeframes[0].clone();
        }
        self.decision_timeframe =
            normalize_timeframe(&self.decision_timeframe).context("invalid decision_timeframe")?;

        if self.decision_cadence_nbars <= 0 {
            self.decision_cadence_nbars = 20;
        }

        if self.start_ts <= 0 || self.end_ts <= 0 || self.end_ts <= self.start_ts {
            return Err(anyhow!("invalid start_ts/end_ts"));
        }

        if self.initial_balance <= 0.0 {
            self.initial_balance = 1000.0;
        }

        validate_fill_policy(&self.fill_policy)?;

        if self.checkpoint_interval_bars <= 0 {
            self.checkpoint_interval_bars = 20;
        }
        if self.checkpoint_interval_seconds <= 0 {
            self.checkpoint_interval_seconds = 2;
        }

        self.prompt_variant = self.prompt_variant.trim().to_string();
        if self.prompt_variant.is_empty() {
            self.prompt_variant = "baseline".to_string();
        }

        self.prompt_template = self.prompt_template.trim().to_string();
        if self.prompt_template.is_empty() {
            self.prompt_template = "default".to_string();
        }

        self.custom_prompt = self.custom_prompt.trim().to_string();

        if self.ai_cfg.provider.is_empty() {
            self.ai_cfg.provider = "inherit".to_string();
        }
        if self.ai_cfg.temperature == 0.0 {
            self.ai_cfg.temperature = 0.4;
        }

        if self.leverage.btc_eth_leverage <= 0 {
            self.leverage.btc_eth_leverage = 5;
        }
        if self.leverage.altcoin_leverage <= 0 {
            self.leverage.altcoin_leverage = 5;
        }

        Ok(())
    }

    /// Returns the backtest interval duration.
    pub fn duration(&self) -> Duration {
        let start = DateTime::from_timestamp(self.start_ts, 0).unwrap_or(DateTime::<Utc>::MIN_UTC);
        let end = DateTime::from_timestamp(self.end_ts, 0).unwrap_or(DateTime::<Utc>::MIN_UTC);
        end - start
    }
}

pub fn validate_fill_policy(policy: &FillPolicy) -> Result<()> {
    match policy {
        FillPolicy::NextOpen | FillPolicy::MidPrice | FillPolicy::BarVWAP => Ok(()),
    }
}
