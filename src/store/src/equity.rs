use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool, sqlite::SqliteRow};
use std::collections::HashMap;

// ==========================================
// Struct Definitions
// ==========================================

/// EquityStore account equity storage (for plotting return curves)
#[derive(Clone)]
pub struct EquityStore {
    pool: SqlitePool,
}

/// EquitySnapshot equity snapshot
#[derive(Debug, Serialize, Deserialize, Clone, sqlx::FromRow)]
pub struct EquitySnapshot {
    #[serde(default)]
    pub id: i64,
    pub trader_id: String,
    pub timestamp: DateTime<Utc>,
    pub total_equity: f64,    // Account equity (balance + unrealized PnL)
    pub balance: f64,         // Account balance
    pub unrealized_pnl: f64,  // Unrealized profit and loss
    pub position_count: i32,  // Position count
    pub margin_used_pct: f64, // Margin usage percentage
}

// ==========================================
// Implementation
// ==========================================

impl EquityStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Initializes equity tables
    pub async fn init_tables(&self) -> Result<()> {
        let queries = vec![
            // Equity snapshot table - specifically for return curves
            r#"CREATE TABLE IF NOT EXISTS trader_equity_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trader_id TEXT NOT NULL,
                timestamp DATETIME NOT NULL,
                total_equity REAL NOT NULL DEFAULT 0,
                balance REAL NOT NULL DEFAULT 0,
                unrealized_pnl REAL NOT NULL DEFAULT 0,
                position_count INTEGER DEFAULT 0,
                margin_used_pct REAL DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )"#,
            // Indexes
            r#"CREATE INDEX IF NOT EXISTS idx_equity_trader_time ON trader_equity_snapshots(trader_id, timestamp DESC)"#,
            r#"CREATE INDEX IF NOT EXISTS idx_equity_timestamp ON trader_equity_snapshots(timestamp DESC)"#,
        ];

        for query in queries {
            sqlx::query(query)
                .execute(&self.pool)
                .await
                .context("Failed to execute init table query")?;
        }

        Ok(())
    }

    /// Saves equity snapshot
    pub async fn save(&self, snapshot: &mut EquitySnapshot) -> Result<()> {
        // Handle zero timestamp (In Rust, default might be UNIX_EPOCH or we just check logic)
        // Here we assume if it looks like the epoch start, we update it, or just rely on caller.
        // To match Go's IsZero(), we update if it's strictly the default value or very old.
        if snapshot.timestamp.timestamp() == 0 {
            snapshot.timestamp = Utc::now();
        } else {
            // Ensure UTC
            // chrono::DateTime is already timezone aware, so we just ensure it's mapped correctly.
        }

        // SQLite stores DateTime as string usually, sqlx handles the conversion via RFC3339
        let timestamp_str = snapshot.timestamp.to_rfc3339();

        let result = sqlx::query(
            r#"
            INSERT INTO trader_equity_snapshots (
                trader_id, timestamp, total_equity, balance,
                unrealized_pnl, position_count, margin_used_pct
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&snapshot.trader_id)
        .bind(timestamp_str)
        .bind(snapshot.total_equity)
        .bind(snapshot.balance)
        .bind(snapshot.unrealized_pnl)
        .bind(snapshot.position_count)
        .bind(snapshot.margin_used_pct)
        .execute(&self.pool)
        .await
        .context("Failed to save equity snapshot")?;

        snapshot.id = result.last_insert_rowid();

        Ok(())
    }

    /// Gets the latest N equity records for specified trader
    /// (sorted in ascending chronological order: old to new)
    pub async fn get_latest(&self, trader_id: &str, limit: i64) -> Result<Vec<EquitySnapshot>> {
        let rows = sqlx::query(
            r#"
            SELECT id, trader_id, timestamp, total_equity, balance,
                   unrealized_pnl, position_count, margin_used_pct
            FROM trader_equity_snapshots
            WHERE trader_id = ?
            ORDER BY timestamp DESC
            LIMIT ?
            "#,
        )
        .bind(trader_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .context("Failed to query equity records")?;

        let mut snapshots = Vec::new();
        for row in rows {
            snapshots.push(self.scan_snapshot(&row)?);
        }

        // Reverse the array to sort time from old to new (suitable for plotting curves)
        snapshots.reverse();

        Ok(snapshots)
    }

    /// Gets equity records within specified time range
    pub async fn get_by_time_range(
        &self,
        trader_id: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<EquitySnapshot>> {
        let start_str = start.to_rfc3339();
        let end_str = end.to_rfc3339();

        let rows = sqlx::query(
            r#"
            SELECT id, trader_id, timestamp, total_equity, balance,
                   unrealized_pnl, position_count, margin_used_pct
            FROM trader_equity_snapshots
            WHERE trader_id = ? AND timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp ASC
            "#,
        )
        .bind(trader_id)
        .bind(start_str)
        .bind(end_str)
        .fetch_all(&self.pool)
        .await
        .context("Failed to query equity records by range")?;

        let mut snapshots = Vec::new();
        for row in rows {
            snapshots.push(self.scan_snapshot(&row)?);
        }

        Ok(snapshots)
    }

    /// Gets latest equity for all traders (for leaderboards)
    pub async fn get_all_traders_latest(&self) -> Result<HashMap<String, EquitySnapshot>> {
        let rows = sqlx::query(
            r#"
            SELECT e.id, e.trader_id, e.timestamp, e.total_equity, e.balance,
                   e.unrealized_pnl, e.position_count, e.margin_used_pct
            FROM trader_equity_snapshots e
            INNER JOIN (
                SELECT trader_id, MAX(timestamp) as max_ts
                FROM trader_equity_snapshots
                GROUP BY trader_id
            ) latest ON e.trader_id = latest.trader_id AND e.timestamp = latest.max_ts
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .context("Failed to query latest equity for all traders")?;

        let mut result = HashMap::new();
        for row in rows {
            let snap = self.scan_snapshot(&row)?;
            result.insert(snap.trader_id.clone(), snap);
        }

        Ok(result)
    }

    /// Cleans old records from N days ago
    pub async fn clean_old_records(&self, trader_id: &str, days: i64) -> Result<u64> {
        let cutoff_time = Utc::now() - chrono::Duration::days(days);
        let cutoff_str = cutoff_time.to_rfc3339();

        let result = sqlx::query(
            r#"
            DELETE FROM trader_equity_snapshots
            WHERE trader_id = ? AND timestamp < ?
            "#,
        )
        .bind(trader_id)
        .bind(cutoff_str)
        .execute(&self.pool)
        .await
        .context("Failed to clean old records")?;

        Ok(result.rows_affected())
    }

    /// Gets record count for specified trader
    pub async fn get_count(&self, trader_id: &str) -> Result<i32> {
        let count: Option<i32> =
            sqlx::query_scalar("SELECT COUNT(*) FROM trader_equity_snapshots WHERE trader_id = ?")
                .bind(trader_id)
                .fetch_one(&self.pool)
                .await
                .context("Failed to count equity records")?;

        Ok(count.unwrap_or(0))
    }

    /// Migrates data from old decision_account_snapshots table
    pub async fn migrate_from_decision(&self) -> Result<u64> {
        // Check if migration is needed (whether new table is empty)
        let count: i32 = sqlx::query_scalar("SELECT COUNT(*) FROM trader_equity_snapshots")
            .fetch_one(&self.pool)
            .await
            .unwrap_or(0);

        if count > 0 {
            return Ok(0); // Already has data, skip migration
        }

        // Check if old table exists
        let table_exists: Option<String> = sqlx::query_scalar(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='decision_account_snapshots'"
        )
        .fetch_optional(&self.pool)
        .await?;

        if table_exists.is_none() {
            return Ok(0); // Old table doesn't exist, skip
        }

        // Migrate data: join query from decision_records + decision_account_snapshots
        let result = sqlx::query(
            r#"
            INSERT INTO trader_equity_snapshots (
                trader_id, timestamp, total_equity, balance,
                unrealized_pnl, position_count, margin_used_pct
            )
            SELECT
                dr.trader_id,
                dr.timestamp,
                das.total_balance,
                das.available_balance,
                das.total_unrealized_profit,
                das.position_count,
                das.margin_used_pct
            FROM decision_records dr
            JOIN decision_account_snapshots das ON dr.id = das.decision_id
            ORDER BY dr.timestamp ASC
            "#,
        )
        .execute(&self.pool)
        .await
        .context("Failed to migrate data")?;

        Ok(result.rows_affected())
    }

    // Helper: Manual scanning to handle potential timestamp parsing nuances strictly
    fn scan_snapshot(&self, row: &SqliteRow) -> Result<EquitySnapshot> {
        let timestamp_str: String = row.try_get("timestamp")?;
        let timestamp = DateTime::parse_from_rfc3339(&timestamp_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());

        Ok(EquitySnapshot {
            id: row.try_get("id")?,
            trader_id: row.try_get("trader_id")?,
            timestamp,
            total_equity: row.try_get("total_equity")?,
            balance: row.try_get("balance")?,
            unrealized_pnl: row.try_get("unrealized_pnl")?,
            position_count: row.try_get("position_count")?,
            margin_used_pct: row.try_get("margin_used_pct")?,
        })
    }
}
