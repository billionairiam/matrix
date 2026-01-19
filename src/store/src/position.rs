use super::order::TraderStats;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};
use std::collections::HashMap;

// ==========================================
// Struct Definitions
// ==========================================

/// TraderPosition position record (complete open/close position tracking)
#[derive(Debug, Serialize, Deserialize, Clone, sqlx::FromRow)]
pub struct TraderPosition {
    #[serde(default)]
    pub id: i64,
    pub trader_id: String,
    #[serde(default)]
    pub exchange_id: String, // Exchange ID: binance/bybit/hyperliquid/aster/lighter
    pub symbol: String,
    pub side: String,     // LONG/SHORT
    pub quantity: f64,    // Opening quantity
    pub entry_price: f64, // Entry price
    #[serde(default)]
    pub entry_order_id: String, // Entry order ID
    pub entry_time: DateTime<Utc>, // Entry time
    #[serde(default)]
    pub exit_price: f64, // Exit price
    #[serde(default)]
    pub exit_order_id: String, // Exit order ID
    pub exit_time: Option<DateTime<Utc>>, // Exit time
    #[serde(default)]
    pub realized_pnl: f64, // Realized profit and loss
    #[serde(default)]
    pub fee: f64, // Fee
    #[serde(default = "default_leverage")]
    pub leverage: i32, // Leverage multiplier
    #[serde(default = "default_status")]
    pub status: String, // OPEN/CLOSED
    #[serde(default)]
    pub close_reason: String, // Close reason: ai_decision/manual/stop_loss/take_profit
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

fn default_leverage() -> i32 {
    1
}
fn default_status() -> String {
    "OPEN".to_string()
}

/// RecentTrade recent trade record (for AI input)
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RecentTrade {
    pub symbol: String,
    pub side: String, // long/short
    pub entry_price: f64,
    pub exit_price: f64,
    pub realized_pnl: f64,
    pub pnl_pct: f64,
    pub exit_time: String,
}

/// PositionStore position storage
#[derive(Clone)]
pub struct PositionStore {
    db: SqlitePool,
}

// ==========================================
// Implementation
// ==========================================

impl PositionStore {
    pub fn new(db: SqlitePool) -> Self {
        Self { db }
    }

    /// Initializes position tables
    pub async fn init_tables(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS trader_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trader_id TEXT NOT NULL,
                exchange_id TEXT NOT NULL DEFAULT '',
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                quantity REAL NOT NULL,
                entry_price REAL NOT NULL,
                entry_order_id TEXT DEFAULT '',
                entry_time DATETIME NOT NULL,
                exit_price REAL DEFAULT 0,
                exit_order_id TEXT DEFAULT '',
                exit_time DATETIME,
                realized_pnl REAL DEFAULT 0,
                fee REAL DEFAULT 0,
                leverage INTEGER DEFAULT 1,
                status TEXT DEFAULT 'OPEN',
                close_reason TEXT DEFAULT '',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            "#,
        )
        .execute(&self.db)
        .await
        .context("Failed to create trader_positions table")?;

        // Migration: add exchange_id column to existing table (if not exists)
        // Ignoring error if column exists (common pattern in simple migrations)
        let _ = sqlx::query(
            "ALTER TABLE trader_positions ADD COLUMN exchange_id TEXT NOT NULL DEFAULT ''",
        )
        .execute(&self.db)
        .await;

        let indices = vec![
            "CREATE INDEX IF NOT EXISTS idx_positions_trader ON trader_positions(trader_id)",
            "CREATE INDEX IF NOT EXISTS idx_positions_exchange ON trader_positions(exchange_id)",
            "CREATE INDEX IF NOT EXISTS idx_positions_status ON trader_positions(trader_id, status)",
            "CREATE INDEX IF NOT EXISTS idx_positions_symbol ON trader_positions(trader_id, symbol, side, status)",
            "CREATE INDEX IF NOT EXISTS idx_positions_entry ON trader_positions(trader_id, entry_time DESC)",
            "CREATE INDEX IF NOT EXISTS idx_positions_exit ON trader_positions(trader_id, exit_time DESC)",
        ];

        for idx in indices {
            sqlx::query(idx)
                .execute(&self.db)
                .await
                .context("Failed to create index")?;
        }

        Ok(())
    }

    /// Creates position record (called when opening position)
    pub async fn create(&self, pos: &mut TraderPosition) -> Result<()> {
        let now = Utc::now();
        pos.created_at = now;
        pos.updated_at = now;
        pos.status = "OPEN".to_string();

        let result = sqlx::query(
            r#"
            INSERT INTO trader_positions (
                trader_id, exchange_id, symbol, side, quantity, entry_price, entry_order_id,
                entry_time, leverage, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&pos.trader_id)
        .bind(&pos.exchange_id)
        .bind(&pos.symbol)
        .bind(&pos.side)
        .bind(pos.quantity)
        .bind(pos.entry_price)
        .bind(&pos.entry_order_id)
        .bind(pos.entry_time)
        .bind(pos.leverage)
        .bind(&pos.status)
        .bind(pos.created_at)
        .bind(pos.updated_at)
        .execute(&self.db)
        .await
        .context("Failed to create position record")?;

        pos.id = result.last_insert_rowid();

        Ok(())
    }

    /// Closes position (updates position record)
    pub async fn close_position(
        &self,
        id: i64,
        exit_price: f64,
        exit_order_id: &str,
        realized_pnl: f64,
        fee: f64,
        close_reason: &str,
    ) -> Result<()> {
        let now = Utc::now();

        sqlx::query(
            r#"
            UPDATE trader_positions SET
                exit_price = ?, exit_order_id = ?, exit_time = ?,
                realized_pnl = ?, fee = ?, status = 'CLOSED',
                close_reason = ?, updated_at = ?
            WHERE id = ?
            "#,
        )
        .bind(exit_price)
        .bind(exit_order_id)
        .bind(now)
        .bind(realized_pnl)
        .bind(fee)
        .bind(close_reason)
        .bind(now)
        .bind(id)
        .execute(&self.db)
        .await
        .context("Failed to update position record")?;

        Ok(())
    }

    /// Gets all open positions
    pub async fn get_open_positions(&self, trader_id: &str) -> Result<Vec<TraderPosition>> {
        let positions = sqlx::query_as::<_, TraderPosition>(
            r#"
            SELECT id, trader_id, exchange_id, symbol, side, quantity, entry_price, entry_order_id,
                entry_time, exit_price, exit_order_id, exit_time, realized_pnl, fee,
                leverage, status, close_reason, created_at, updated_at
            FROM trader_positions
            WHERE trader_id = ? AND status = 'OPEN'
            ORDER BY entry_time DESC
            "#,
        )
        .bind(trader_id)
        .fetch_all(&self.db)
        .await
        .context("Failed to query open positions")?;

        Ok(positions)
    }

    /// Gets open position for specified symbol and direction
    pub async fn get_open_position_by_symbol(
        &self,
        trader_id: &str,
        symbol: &str,
        side: &str,
    ) -> Result<Option<TraderPosition>> {
        let position = sqlx::query_as::<_, TraderPosition>(
            r#"
            SELECT id, trader_id, exchange_id, symbol, side, quantity, entry_price, entry_order_id,
                entry_time, exit_price, exit_order_id, exit_time, realized_pnl, fee,
                leverage, status, close_reason, created_at, updated_at
            FROM trader_positions
            WHERE trader_id = ? AND symbol = ? AND side = ? AND status = 'OPEN'
            ORDER BY entry_time DESC LIMIT 1
            "#,
        )
        .bind(trader_id)
        .bind(symbol)
        .bind(side)
        .fetch_optional(&self.db)
        .await?;

        Ok(position)
    }

    /// Gets closed positions (historical records)
    pub async fn get_closed_positions(
        &self,
        trader_id: &str,
        limit: i64,
    ) -> Result<Vec<TraderPosition>> {
        let positions = sqlx::query_as::<_, TraderPosition>(
            r#"
            SELECT id, trader_id, exchange_id, symbol, side, quantity, entry_price, entry_order_id,
                entry_time, exit_price, exit_order_id, exit_time, realized_pnl, fee,
                leverage, status, close_reason, created_at, updated_at
            FROM trader_positions
            WHERE trader_id = ? AND status = 'CLOSED'
            ORDER BY exit_time DESC
            LIMIT ?
            "#,
        )
        .bind(trader_id)
        .bind(limit)
        .fetch_all(&self.db)
        .await
        .context("Failed to query closed positions")?;

        Ok(positions)
    }

    /// Gets all traders' open positions (for global sync)
    pub async fn get_all_open_positions(&self) -> Result<Vec<TraderPosition>> {
        let positions = sqlx::query_as::<_, TraderPosition>(
            r#"
            SELECT id, trader_id, exchange_id, symbol, side, quantity, entry_price, entry_order_id,
                entry_time, exit_price, exit_order_id, exit_time, realized_pnl, fee,
                leverage, status, close_reason, created_at, updated_at
            FROM trader_positions
            WHERE status = 'OPEN'
            ORDER BY trader_id, entry_time DESC
            "#,
        )
        .fetch_all(&self.db)
        .await
        .context("Failed to query all open positions")?;

        Ok(positions)
    }

    /// Gets position statistics (simplified version)
    pub async fn get_position_stats(
        &self,
        trader_id: &str,
    ) -> Result<HashMap<String, serde_json::Value>> {
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as wins,
                COALESCE(SUM(realized_pnl), 0) as total_pnl,
                COALESCE(SUM(fee), 0) as total_fee
            FROM trader_positions
            WHERE trader_id = ? AND status = 'CLOSED'
            "#,
        )
        .bind(trader_id)
        .fetch_one(&self.db)
        .await?;

        let total_trades: i32 = row.try_get("total")?;
        let win_trades: i32 = row.try_get("wins").unwrap_or(0);
        let total_pnl: f64 = row.try_get("total_pnl").unwrap_or(0.0);
        let total_fee: f64 = row.try_get("total_fee").unwrap_or(0.0);

        let win_rate = if total_trades > 0 {
            (win_trades as f64 / total_trades as f64) * 100.0
        } else {
            0.0
        };

        let mut stats = HashMap::new();
        stats.insert("total_trades".to_string(), serde_json::json!(total_trades));
        stats.insert("win_trades".to_string(), serde_json::json!(win_trades));
        stats.insert("total_pnl".to_string(), serde_json::json!(total_pnl));
        stats.insert("total_fee".to_string(), serde_json::json!(total_fee));
        stats.insert("win_rate".to_string(), serde_json::json!(win_rate));

        Ok(stats)
    }

    /// Gets complete trading statistics (compatible with TraderStats)
    pub async fn get_full_stats(&self, trader_id: &str) -> Result<TraderStats> {
        let mut stats = TraderStats::default();

        let rows = sqlx::query(
            r#"
            SELECT realized_pnl, fee, exit_time
            FROM trader_positions
            WHERE trader_id = ? AND status = 'CLOSED'
            ORDER BY exit_time ASC
            "#,
        )
        .bind(trader_id)
        .fetch_all(&self.db)
        .await
        .context("Failed to query position statistics")?;

        let mut pnls = Vec::new();
        let mut total_win = 0.0;
        let mut total_loss = 0.0;

        for row in rows {
            let pnl: f64 = row.try_get("realized_pnl")?;
            let fee: f64 = row.try_get("fee")?;

            stats.total_trades += 1;
            stats.total_pnl += pnl;
            stats.total_fee += fee;
            pnls.push(pnl);

            if pnl > 0.0 {
                stats.win_trades += 1;
                total_win += pnl;
            } else if pnl < 0.0 {
                stats.loss_trades += 1;
                total_loss += pnl.abs();
            }
        }

        if stats.total_trades > 0 {
            stats.win_rate = (stats.win_trades as f64 / stats.total_trades as f64) * 100.0;
        }

        if total_loss > 0.0 {
            stats.profit_factor = total_win / total_loss;
        }

        if stats.win_trades > 0 {
            stats.avg_win = total_win / stats.win_trades as f64;
        }
        if stats.loss_trades > 0 {
            stats.avg_loss = total_loss / stats.loss_trades as f64;
        }

        if pnls.len() > 1 {
            stats.sharpe_ratio = Self::calculate_sharpe_ratio(&pnls);
        }

        if !pnls.is_empty() {
            stats.max_drawdown_pct = Self::calculate_max_drawdown(&pnls);
        }

        Ok(stats)
    }

    /// Gets recent closed trades
    pub async fn get_recent_trades(&self, trader_id: &str, limit: i64) -> Result<Vec<RecentTrade>> {
        let rows = sqlx::query(
            r#"
            SELECT symbol, side, entry_price, exit_price, realized_pnl, leverage, exit_time
            FROM trader_positions
            WHERE trader_id = ? AND status = 'CLOSED'
            ORDER BY exit_time DESC
            LIMIT ?
            "#,
        )
        .bind(trader_id)
        .bind(limit)
        .fetch_all(&self.db)
        .await
        .context("Failed to query recent trades")?;

        let mut trades = Vec::new();

        for row in rows {
            let symbol: String = row.try_get("symbol")?;
            let mut side: String = row.try_get("side")?;
            let entry_price: f64 = row.try_get("entry_price")?;
            let exit_price: f64 = row.try_get("exit_price")?;
            let realized_pnl: f64 = row.try_get("realized_pnl")?;
            let leverage: i32 = row.try_get("leverage")?;
            let exit_time_val: Option<DateTime<Utc>> = row.try_get("exit_time")?;

            if side == "LONG" {
                side = "long".to_string();
            } else if side == "SHORT" {
                side = "short".to_string();
            }

            let mut pnl_pct = 0.0;
            if entry_price > 0.0 {
                if side == "long" {
                    pnl_pct = (exit_price - entry_price) / entry_price * 100.0 * (leverage as f64);
                } else {
                    pnl_pct = (entry_price - exit_price) / entry_price * 100.0 * (leverage as f64);
                }
            }

            let exit_time_str = if let Some(dt) = exit_time_val {
                dt.format("%m-%d %H:%M").to_string()
            } else {
                "".to_string()
            };

            trades.push(RecentTrade {
                symbol,
                side,
                entry_price,
                exit_price,
                realized_pnl,
                pnl_pct,
                exit_time: exit_time_str,
            });
        }

        Ok(trades)
    }

    // ==========================================
    // Math Helpers
    // ==========================================

    fn calculate_sharpe_ratio(pnls: &[f64]) -> f64 {
        if pnls.len() < 2 {
            return 0.0;
        }

        let n = pnls.len() as f64;
        let sum: f64 = pnls.iter().sum();
        let mean = sum / n;

        let variance: f64 = pnls.iter().map(|&pnl| (pnl - mean).powi(2)).sum();
        let std_dev = (variance / (n - 1.0)).sqrt();

        if std_dev == 0.0 { 0.0 } else { mean / std_dev }
    }

    fn calculate_max_drawdown(pnls: &[f64]) -> f64 {
        if pnls.is_empty() {
            return 0.0;
        }

        let mut cumulative = 0.0;
        let mut peak = 0.0;
        let mut max_dd = 0.0;

        for &pnl in pnls {
            cumulative += pnl;
            if cumulative > peak {
                peak = cumulative;
            }
            if peak > 0.0 {
                let dd = (peak - cumulative) / peak * 100.0;
                if dd > max_dd {
                    max_dd = dd;
                }
            }
        }

        max_dd
    }
}
