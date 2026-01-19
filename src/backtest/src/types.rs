use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// RunState represents the current state of a backtest run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunState {
    Created,
    Running,
    Paused,
    Stopped,
    Completed,
    Failed,
    Liquidated,
}

impl RunState {
    /// Returns the string representation of the RunState.
    pub fn as_str(&self) -> &'static str {
        match self {
            RunState::Created => "created",
            RunState::Running => "running",
            RunState::Paused => "paused",
            RunState::Stopped => "stopped",
            RunState::Completed => "completed",
            RunState::Failed => "failed",
            RunState::Liquidated => "liquidated",
        }
    }
}

/// PositionSnapshot represents core position data for backtest state and persistence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionSnapshot {
    pub symbol: String,
    pub side: String,
    pub quantity: f64,
    pub avg_price: f64,
    pub leverage: i32,
    pub liquidation_price: f64,
    pub margin_used: f64,
    pub open_time: i64,
}

/// BacktestState represents the real-time state during execution (in-memory state).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestState {
    pub bar_index: usize,
    pub bar_timestamp: i64,
    pub decision_cycle: usize,

    pub cash: f64,
    pub equity: f64,
    pub unrealized_pnl: f64,
    pub realized_pnl: f64,
    pub max_equity: f64,
    pub min_equity: f64,
    pub max_drawdown_pct: f64,
    pub positions: HashMap<String, PositionSnapshot>,
    pub last_update: DateTime<Utc>,
    pub liquidated: bool,
    pub liquidation_note: String,
}

/// EquityPoint represents a single point on the equity curve.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EquityPoint {
    #[serde(rename = "ts")]
    pub timestamp: i64,
    pub equity: f64,
    pub available: f64,
    pub pnl: f64,
    pub pnl_pct: f64,
    #[serde(rename = "dd_pct")]
    pub drawdown_pct: f64,
    pub cycle: i32,
}

/// TradeEvent records a trade execution result or special event (such as liquidation).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeEvent {
    #[serde(rename = "ts")]
    pub timestamp: i64,
    pub symbol: String,
    pub action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub side: Option<String>,
    #[serde(rename = "qty")]
    pub quantity: f64,
    pub price: f64,
    pub fee: f64,
    pub slippage: f64,
    pub order_value: f64,
    pub realized_pnl: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub leverage: Option<i32>,
    pub cycle: i32,
    pub position_after: f64,
    #[serde(rename = "liquidation")]
    pub liquidation_flag: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// Metrics summarizes backtest performance metrics.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Metrics {
    pub total_return_pct: f64,
    pub max_drawdown_pct: f64,
    pub sharpe_ratio: f64,
    pub profit_factor: f64,
    pub win_rate: f64,
    pub trades: i32,
    pub avg_win: f64,
    pub avg_loss: f64,
    pub best_symbol: String,
    pub worst_symbol: String,
    pub symbol_stats: HashMap<String, SymbolMetrics>,
    pub liquidated: bool,
}

/// SymbolMetrics records performance for a single symbol.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SymbolMetrics {
    pub total_trades: i32,
    pub winning_trades: i32,
    pub losing_trades: i32,
    pub total_pnl: f64,
    pub avg_pnl: f64,
    pub win_rate: f64,
}

/// Checkpoint represents checkpoint information saved to disk for pause, resume, and crash recovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    pub bar_index: usize,
    #[serde(rename = "bar_ts")]
    pub bar_timestamp: i64,
    pub cash: f64,
    pub equity: f64,
    pub max_equity: f64,
    pub min_equity: f64,
    pub max_drawdown_pct: f64,
    pub unrealized_pnl: f64,
    pub realized_pnl: f64,
    pub positions: Vec<PositionSnapshot>,
    pub decision_cycle: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indicators_state: Option<HashMap<String, HashMap<String, Value>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rng_seed: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ai_cache_ref: Option<String>,
    pub liquidated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub liquidation_note: Option<String>,
}

/// RunMetadata records the summary required for run.json.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunMetadata {
    pub run_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub version: i32,
    pub state: RunState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub summary: RunSummary,
}

/// RunSummary represents the summary field in run.json.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSummary {
    pub symbol_count: usize,
    pub decision_tf: String,
    pub processed_bars: usize,
    pub progress_pct: f64,
    pub equity_last: f64,
    pub max_drawdown_pct: f64,
    pub liquidated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub liquidation_note: Option<String>,
}

/// StatusPayload is used for /status API responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusPayload {
    pub run_id: String,
    pub state: RunState,
    pub progress_pct: f64,
    pub processed_bars: i32,
    pub current_time: i64,
    pub decision_cycle: i32,
    pub equity: f64,
    pub unrealized_pnl: f64,
    pub realized_pnl: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub last_updated_iso: String,
}
