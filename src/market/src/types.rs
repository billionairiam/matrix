use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Data market data structure
/// Uses `rename_all = "PascalCase"` to match Go's default JSON export for public fields,
/// unless overridden by a specific `rename` attribute.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Data {
    pub symbol: String,
    pub current_price: f64,
    #[serde(rename = "PriceChange1h")]
    pub price_change_1h: f64, // 1-hour price change percentage
    #[serde(rename = "PriceChange4h")]
    pub price_change_4h: f64, // 4-hour price change percentage
    #[serde(rename = "CurrentEMA20")]
    pub current_ema20: f64,
    #[serde(rename = "CurrentMACD")]
    pub current_macd: f64,
    #[serde(rename = "CurrentRSI7")]
    pub current_rsi7: f64,

    // Using Option to mimic Go's pointer (*OIData), allowing null/nil
    pub open_interest: Option<OIData>,
    pub funding_rate: f64,
    pub intraday_series: Option<IntradayData>,
    pub longer_term_context: Option<LongerTermData>,

    // Multi-timeframe data
    // Matches Go tag `json:"timeframe_data,omitempty"`
    #[serde(rename = "timeframe_data", skip_serializing_if = "Option::is_none")]
    pub timeframe_data: Option<HashMap<String, TimeframeSeriesData>>,
}

/// TimeframeSeriesData series data for a single timeframe
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeframeSeriesData {
    #[serde(rename = "timeframe")]
    pub timeframe: String,
    #[serde(rename = "mid_prices")]
    pub mid_prices: Vec<f64>,
    #[serde(rename = "ema20_values")]
    pub ema20_values: Vec<f64>,
    #[serde(rename = "ema50_values")]
    pub ema50_values: Vec<f64>,
    #[serde(rename = "macd_values")]
    pub macd_values: Vec<f64>,
    #[serde(rename = "rsi7_values")]
    pub rsi7_values: Vec<f64>,
    #[serde(rename = "rsi14_values")]
    pub rsi14_values: Vec<f64>,
    #[serde(rename = "volume")]
    pub volume: Vec<f64>,
    #[serde(rename = "atr14")]
    pub atr14: f64,
}

/// OIData Open Interest data
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct OIData {
    pub latest: f64,
    pub average: f64,
}

/// IntradayData intraday data (3-minute interval)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct IntradayData {
    pub mid_prices: Vec<f64>,
    #[serde(rename = "EMA20Values")]
    pub ema20_values: Vec<f64>,
    #[serde(rename = "MACDValues")]
    pub macd_values: Vec<f64>,
    #[serde(rename = "RSI7Values")]
    pub rsi7_values: Vec<f64>,
    #[serde(rename = "RSI14Values")]
    pub rsi14_values: Vec<f64>,
    pub volume: Vec<f64>,
    #[serde(rename = "ATR14")]
    pub atr14: f64,
}

/// LongerTermData longer-term data (4-hour timeframe)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct LongerTermData {
    #[serde(rename = "EMA20")]
    pub ema20: f64,
    #[serde(rename = "EMA50")]
    pub ema50: f64,
    #[serde(rename = "ATR3")]
    pub atr3: f64,
    #[serde(rename = "ATR14")]
    pub atr14: f64,
    pub current_volume: f64,
    pub average_volume: f64,
    #[serde(rename = "MACDValues")]
    pub macd_values: Vec<f64>,
    #[serde(rename = "RSI14Values")]
    pub rsi14_values: Vec<f64>,
}

// -----------------------------------------------------------------------------
// Binance API Structures
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeInfo {
    #[serde(rename = "symbols")]
    pub symbols: Vec<SymbolInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolInfo {
    #[serde(rename = "symbol")]
    pub symbol: String,
    #[serde(rename = "status")]
    pub status: String,
    #[serde(rename = "baseAsset")]
    pub base_asset: String,
    #[serde(rename = "quoteAsset")]
    pub quote_asset: String,
    #[serde(rename = "contractType")]
    pub contract_type: String,
    #[serde(rename = "pricePrecision")]
    pub price_precision: i32,
    #[serde(rename = "quantityPrecision")]
    pub quantity_precision: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Kline {
    #[serde(rename = "openTime")]
    pub open_time: i64,
    #[serde(rename = "open")]
    pub open: f64,
    #[serde(rename = "high")]
    pub high: f64,
    #[serde(rename = "low")]
    pub low: f64,
    #[serde(rename = "close")]
    pub close: f64,
    #[serde(rename = "volume")]
    pub volume: f64,
    #[serde(rename = "closeTime")]
    pub close_time: i64,
    #[serde(rename = "quoteVolume")]
    pub quote_volume: f64,
    #[serde(rename = "trades")]
    pub trades: i64,
    #[serde(rename = "takerBuyBaseVolume")]
    pub taker_buy_base_volume: f64,
    #[serde(rename = "takerBuyQuoteVolume")]
    pub taker_buy_quote_volume: f64,
}

// Go: type KlineResponse []interface{}
// In Rust, we represent this as a Vec of arbitrary JSON values,
// as the API likely returns mixed types (numbers and strings) in an array.
pub type KlineResponse = Vec<serde_json::Value>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceTicker {
    #[serde(rename = "symbol")]
    pub symbol: String,
    #[serde(rename = "price")]
    pub price: String, // Kept as String to match Go/API types
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ticker24hr {
    #[serde(rename = "symbol")]
    pub symbol: String,
    #[serde(rename = "priceChange")]
    pub price_change: String,
    #[serde(rename = "priceChangePercent")]
    pub price_change_percent: String,
    #[serde(rename = "volume")]
    pub volume: String,
    #[serde(rename = "quoteVolume")]
    pub quote_volume: String,
}

/// SymbolFeatures feature data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolFeatures {
    #[serde(rename = "symbol")]
    pub symbol: String,
    #[serde(rename = "timestamp")]
    pub timestamp: DateTime<Utc>,
    #[serde(rename = "price")]
    pub price: f64,
    #[serde(rename = "price_change_15min")]
    pub price_change_15min: f64,
    #[serde(rename = "price_change_1h")]
    pub price_change_1h: f64,
    #[serde(rename = "price_change_4h")]
    pub price_change_4h: f64,
    #[serde(rename = "volume")]
    pub volume: f64,
    #[serde(rename = "volume_ratio_5")]
    pub volume_ratio_5: f64,
    #[serde(rename = "volume_ratio_20")]
    pub volume_ratio_20: f64,
    #[serde(rename = "volume_trend")]
    pub volume_trend: f64,
    #[serde(rename = "rsi_14")]
    pub rsi14: f64,
    #[serde(rename = "sma_5")]
    pub sma5: f64,
    #[serde(rename = "sma_10")]
    pub sma10: f64,
    #[serde(rename = "sma_20")]
    pub sma20: f64,
    #[serde(rename = "high_low_ratio")]
    pub high_low_ratio: f64,
    #[serde(rename = "volatility_20")]
    pub volatility_20: f64,
    #[serde(rename = "position_in_range")]
    pub position_in_range: f64,
}

/// Alert alert data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    #[serde(rename = "type")]
    pub alert_type: String, // "type" is a reserved keyword in Rust
    #[serde(rename = "symbol")]
    pub symbol: String,
    #[serde(rename = "value")]
    pub value: f64,
    #[serde(rename = "threshold")]
    pub threshold: f64,
    #[serde(rename = "message")]
    pub message: String,
    #[serde(rename = "timestamp")]
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(rename = "alert_thresholds")]
    pub alert_thresholds: AlertThresholds,
    #[serde(rename = "update_interval")]
    pub update_interval: u64, // seconds
    #[serde(rename = "cleanup_config")]
    pub cleanup_config: CleanupConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertThresholds {
    #[serde(rename = "volume_spike")]
    pub volume_spike: f64,
    #[serde(rename = "price_change_15min")]
    pub price_change_15min: f64,
    #[serde(rename = "volume_trend")]
    pub volume_trend: f64,
    #[serde(rename = "rsi_overbought")]
    pub rsi_overbought: f64,
    #[serde(rename = "rsi_oversold")]
    pub rsi_oversold: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CleanupConfig {
    // We map Go's time.Duration to u64 (seconds) for JSON serialization,
    // or use a custom deserializer if string format "30m" is required.
    // Here we assume simple integers (seconds) or internal Duration usage.
    // To strictly match Go's logical use, we'll store them as Duration in Rust
    // but Serialize them as needed. Here, we'll use `std::time::Duration`.
    // Note: Standard Serde does not serialize Duration easily without crates like `humantime-serde`.
    // For simplicity in this translation, we will treat them as fields, but implementing `Default` is key.
    #[serde(skip, default = "default_inactive_timeout")]
    pub inactive_timeout: Duration,

    #[serde(rename = "min_score_threshold")]
    pub min_score_threshold: f64,

    #[serde(skip, default = "default_no_alert_timeout")]
    pub no_alert_timeout: Duration,

    #[serde(skip, default = "default_check_interval")]
    pub check_interval: Duration,
}

// Helper functions for defaults
fn default_inactive_timeout() -> Duration {
    Duration::from_secs(30 * 60)
}
fn default_no_alert_timeout() -> Duration {
    Duration::from_secs(20 * 60)
}
fn default_check_interval() -> Duration {
    Duration::from_secs(5 * 60)
}

impl Default for Config {
    fn default() -> Self {
        Config {
            alert_thresholds: AlertThresholds {
                volume_spike: 3.0,
                price_change_15min: 0.05,
                volume_trend: 2.0,
                rsi_overbought: 70.0,
                rsi_oversold: 30.0,
            },
            cleanup_config: CleanupConfig {
                inactive_timeout: default_inactive_timeout(), // 30 Minutes
                min_score_threshold: 15.0,
                no_alert_timeout: default_no_alert_timeout(), // 20 Minutes
                check_interval: default_check_interval(),     // 5 Minutes
            },
            update_interval: 60, // 1 minute
        }
    }
}

/// Helper to get the default configuration
pub fn get_default_config() -> Config {
    Config::default()
}
