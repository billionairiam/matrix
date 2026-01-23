use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};

/// TraderOrder trader order record
#[derive(Debug, Serialize, Deserialize, Clone, sqlx::FromRow, Default)]
pub struct TraderOrder {
    #[serde(default)]
    pub id: i64,
    pub trader_id: String,
    pub order_id: String,
    #[serde(default)]
    pub client_order_id: String,
    pub symbol: String,
    pub side: String,
    #[serde(default)]
    pub position_side: String,
    pub action: String, // open_long/close_long/open_short/close_short
    #[serde(default = "default_order_type")]
    pub order_type: String,
    pub quantity: f64,
    #[serde(default)]
    pub price: f64,
    #[serde(default)]
    pub avg_price: f64,
    #[serde(default)]
    pub executed_qty: f64,
    #[serde(default = "default_leverage")]
    pub leverage: i32,
    #[serde(default = "default_status")]
    pub status: String,
    #[serde(default)]
    pub fee: f64,
    #[serde(default = "default_fee_asset")]
    pub fee_asset: String,
    #[serde(default)]
    pub realized_pnl: f64,
    #[serde(default)]
    pub entry_price: f64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub filled_at: Option<DateTime<Utc>>,
}

fn default_order_type() -> String {
    "MARKET".to_string()
}
fn default_leverage() -> i32 {
    1
}
fn default_status() -> String {
    "NEW".to_string()
}
fn default_fee_asset() -> String {
    "USDT".to_string()
}

/// TraderStats trading statistics metrics
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TraderStats {
    pub total_trades: i32,
    pub win_trades: i32,
    pub loss_trades: i32,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub sharpe_ratio: f64,
    pub total_pnl: f64,
    pub total_fee: f64,
    pub avg_win: f64,
    pub avg_loss: f64,
    pub max_drawdown_pct: f64,
}

/// CompletedOrder completed order (for AI input)
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CompletedOrder {
    pub symbol: String,
    pub action: String,
    pub side: String,
    pub quantity: f64,
    pub entry_price: f64,
    pub exit_price: f64,
    pub realized_pnl: f64,
    pub pnl_pct: f64,
    pub fee: f64,
    pub leverage: i32,
    pub filled_at: DateTime<Utc>,
}

/// OrderStore order storage
#[derive(Clone)]
pub struct OrderStore {
    db: SqlitePool,
}

impl OrderStore {
    pub fn new(db: SqlitePool) -> Self {
        Self { db }
    }

    /// Initializes order tables
    pub async fn init_tables(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS trader_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trader_id TEXT NOT NULL,
                order_id TEXT NOT NULL,
                client_order_id TEXT DEFAULT '',
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                position_side TEXT DEFAULT '',
                action TEXT NOT NULL,
                order_type TEXT DEFAULT 'MARKET',
                quantity REAL NOT NULL,
                price REAL DEFAULT 0,
                avg_price REAL DEFAULT 0,
                executed_qty REAL DEFAULT 0,
                leverage INTEGER DEFAULT 1,
                status TEXT DEFAULT 'NEW',
                fee REAL DEFAULT 0,
                fee_asset TEXT DEFAULT 'USDT',
                realized_pnl REAL DEFAULT 0,
                entry_price REAL DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                filled_at DATETIME,
                UNIQUE(trader_id, order_id)
            )
            "#,
        )
        .execute(&self.db)
        .await
        .context("Failed to create trader_orders table")?;

        let indices = vec![
            "CREATE INDEX IF NOT EXISTS idx_trader_orders_trader ON trader_orders(trader_id)",
            "CREATE INDEX IF NOT EXISTS idx_trader_orders_status ON trader_orders(trader_id, status)",
            "CREATE INDEX IF NOT EXISTS idx_trader_orders_symbol ON trader_orders(trader_id, symbol)",
            "CREATE INDEX IF NOT EXISTS idx_trader_orders_filled ON trader_orders(trader_id, filled_at DESC)",
        ];

        for idx in indices {
            sqlx::query(idx)
                .execute(&self.db)
                .await
                .context("Failed to create index")?;
        }

        Ok(())
    }

    /// Creates order record
    pub async fn create(&self, order: &mut TraderOrder) -> Result<()> {
        // Rust's DateTime<Utc> serializes to RFC3339 string automatically in SQLx with sqlite
        if order.created_at.timestamp() == 0 {
            order.created_at = Utc::now();
        }
        order.updated_at = order.created_at;

        let result = sqlx::query(
            r#"
            INSERT INTO trader_orders (
                trader_id, order_id, client_order_id, symbol, side, position_side,
                action, order_type, quantity, price, avg_price, executed_qty,
                leverage, status, fee, fee_asset, realized_pnl, entry_price,
                created_at, updated_at, filled_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&order.trader_id)
        .bind(&order.order_id)
        .bind(&order.client_order_id)
        .bind(&order.symbol)
        .bind(&order.side)
        .bind(&order.position_side)
        .bind(&order.action)
        .bind(&order.order_type)
        .bind(order.quantity)
        .bind(order.price)
        .bind(order.avg_price)
        .bind(order.executed_qty)
        .bind(order.leverage)
        .bind(&order.status)
        .bind(order.fee)
        .bind(&order.fee_asset)
        .bind(order.realized_pnl)
        .bind(order.entry_price)
        .bind(order.created_at)
        .bind(order.updated_at)
        .bind(order.filled_at)
        .execute(&self.db)
        .await
        .context("Failed to create order record")?;

        order.id = result.last_insert_rowid();

        Ok(())
    }

    /// Updates order record
    pub async fn update(&self, order: &mut TraderOrder) -> Result<()> {
        order.updated_at = Utc::now();

        sqlx::query(
            r#"
            UPDATE trader_orders SET
                avg_price = ?, executed_qty = ?, status = ?, fee = ?,
                realized_pnl = ?, entry_price = ?, updated_at = ?, filled_at = ?
            WHERE trader_id = ? AND order_id = ?
            "#,
        )
        .bind(order.avg_price)
        .bind(order.executed_qty)
        .bind(&order.status)
        .bind(order.fee)
        .bind(order.realized_pnl)
        .bind(order.entry_price)
        .bind(order.updated_at)
        .bind(order.filled_at)
        .bind(&order.trader_id)
        .bind(&order.order_id)
        .execute(&self.db)
        .await
        .context("Failed to update order record")?;

        Ok(())
    }

    /// Gets order by order ID
    pub async fn get_by_order_id(&self, trader_id: &str, order_id: &str) -> Result<TraderOrder> {
        let order = sqlx::query_as::<_, TraderOrder>(
            r#"
            SELECT id, trader_id, order_id, client_order_id, symbol, side, position_side,
                action, order_type, quantity, price, avg_price, executed_qty,
                leverage, status, fee, fee_asset, realized_pnl, entry_price,
                created_at, updated_at, filled_at
            FROM trader_orders WHERE trader_id = ? AND order_id = ?
            "#,
        )
        .bind(trader_id)
        .bind(order_id)
        .fetch_one(&self.db)
        .await?;

        Ok(order)
    }

    /// Gets the latest open order for a symbol (for calculating close PnL)
    pub async fn get_latest_open_order(
        &self,
        trader_id: &str,
        symbol: &str,
        side: &str,
    ) -> Result<TraderOrder> {
        // side: long -> find open_long, short -> find open_short
        let action = if side == "short" {
            "open_short"
        } else {
            "open_long"
        };

        let order = sqlx::query_as::<_, TraderOrder>(
            r#"
            SELECT id, trader_id, order_id, client_order_id, symbol, side, position_side,
                action, order_type, quantity, price, avg_price, executed_qty,
                leverage, status, fee, fee_asset, realized_pnl, entry_price,
                created_at, updated_at, filled_at
            FROM trader_orders
            WHERE trader_id = ? AND symbol = ? AND action = ? AND status = 'FILLED'
            ORDER BY filled_at DESC LIMIT 1
            "#,
        )
        .bind(trader_id)
        .bind(symbol)
        .bind(action)
        .fetch_one(&self.db)
        .await?;

        Ok(order)
    }

    /// Gets recent completed close orders
    pub async fn get_recent_completed_orders(
        &self,
        trader_id: &str,
        limit: i64,
    ) -> Result<Vec<CompletedOrder>> {
        let rows = sqlx::query(
            r#"
            SELECT symbol, action, side, executed_qty, entry_price, avg_price,
                realized_pnl, fee, leverage, filled_at
            FROM trader_orders
            WHERE trader_id = ? AND status = 'FILLED'
                AND (action = 'close_long' OR action = 'close_short')
            ORDER BY filled_at DESC
            LIMIT ?
            "#,
        )
        .bind(trader_id)
        .bind(limit)
        .fetch_all(&self.db)
        .await
        .context("Failed to query completed orders")?;

        let mut orders = Vec::new();

        for row in rows {
            let symbol: String = row.try_get("symbol")?;
            let action: String = row.try_get("action")?;
            let side_db: Option<String> = row.try_get("side")?;
            let quantity: f64 = row.try_get("executed_qty")?;
            let entry_price: f64 = row.try_get("entry_price")?;
            let exit_price: f64 = row.try_get("avg_price")?;
            let realized_pnl: f64 = row.try_get("realized_pnl")?;
            let fee: f64 = row.try_get("fee")?;
            let leverage: i32 = row.try_get("leverage")?;
            let filled_at_opt: Option<DateTime<Utc>> = row.try_get("filled_at")?;

            // Skip invalid data
            let filled_at = match filled_at_opt {
                Some(dt) => dt,
                None => continue,
            };

            // Infer side from action or DB side
            let side = if action == "close_long" {
                "long".to_string()
            } else if action == "close_short" {
                "short".to_string()
            } else {
                side_db.unwrap_or_default()
            };

            // Calculate PnL percentage
            let mut pnl_pct = 0.0;
            if entry_price > 0.0 {
                if side == "long" {
                    pnl_pct = (exit_price - entry_price) / entry_price * 100.0 * (leverage as f64);
                } else {
                    pnl_pct = (entry_price - exit_price) / entry_price * 100.0 * (leverage as f64);
                }
            }

            orders.push(CompletedOrder {
                symbol,
                action,
                side,
                quantity,
                entry_price,
                exit_price,
                realized_pnl,
                pnl_pct,
                fee,
                leverage,
                filled_at,
            });
        }

        Ok(orders)
    }

    /// Gets trading statistics metrics
    pub async fn get_trader_stats(&self, trader_id: &str) -> Result<TraderStats> {
        let mut stats = TraderStats::default();

        // Query all completed close orders
        // Note: We only need realized_pnl and fee for stats, filled_at strictly for ordering if needed
        let rows = sqlx::query(
            r#"
            SELECT realized_pnl, fee
            FROM trader_orders
            WHERE trader_id = ? AND status = 'FILLED'
                AND (action = 'close_long' OR action = 'close_short')
            ORDER BY filled_at ASC
            "#,
        )
        .bind(trader_id)
        .fetch_all(&self.db)
        .await
        .context("Failed to query order statistics")?;

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

        // Calculate win rate
        if stats.total_trades > 0 {
            stats.win_rate = (stats.win_trades as f64) / (stats.total_trades as f64) * 100.0;
        }

        // Calculate profit factor
        if total_loss > 0.0 {
            stats.profit_factor = total_win / total_loss;
        }

        // Calculate average win/loss
        if stats.win_trades > 0 {
            stats.avg_win = total_win / (stats.win_trades as f64);
        }
        if stats.loss_trades > 0 {
            stats.avg_loss = total_loss / (stats.loss_trades as f64);
        }

        // Calculate Sharpe ratio
        if pnls.len() > 1 {
            stats.sharpe_ratio = Self::calculate_sharpe_ratio(&pnls);
        }

        // Calculate max drawdown
        if !pnls.is_empty() {
            stats.max_drawdown_pct = Self::calculate_max_drawdown(&pnls);
        }

        Ok(stats)
    }

    /// Gets pending orders (for polling)
    pub async fn get_pending_orders(&self, trader_id: &str) -> Result<Vec<TraderOrder>> {
        let orders = sqlx::query_as::<_, TraderOrder>(
            r#"
            SELECT id, trader_id, order_id, client_order_id, symbol, side, position_side,
                action, order_type, quantity, price, avg_price, executed_qty,
                leverage, status, fee, fee_asset, realized_pnl, entry_price,
                created_at, updated_at, filled_at
            FROM trader_orders
            WHERE trader_id = ? AND status = 'NEW'
            ORDER BY created_at ASC
            "#,
        )
        .bind(trader_id)
        .fetch_all(&self.db)
        .await
        .context("Failed to query pending orders")?;

        Ok(orders)
    }

    /// Gets all pending orders (for global sync)
    pub async fn get_all_pending_orders(&self) -> Result<Vec<TraderOrder>> {
        let orders = sqlx::query_as::<_, TraderOrder>(
            r#"
            SELECT id, trader_id, order_id, client_order_id, symbol, side, position_side,
                action, order_type, quantity, price, avg_price, executed_qty,
                leverage, status, fee, fee_asset, realized_pnl, entry_price,
                created_at, updated_at, filled_at
            FROM trader_orders
            WHERE status = 'NEW'
            ORDER BY trader_id, created_at ASC
            "#,
        )
        .fetch_all(&self.db)
        .await
        .context("Failed to query all pending orders")?;

        Ok(orders)
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
