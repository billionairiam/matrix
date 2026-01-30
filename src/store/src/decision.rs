use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool, sqlite::SqliteRow};

/// DecisionStore decision log storage
#[derive(Clone)]
pub struct DecisionStore {
    pool: SqlitePool,
}

/// DecisionRecord decision record
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct DecisionRecord {
    #[serde(default)]
    pub id: i64,
    pub trader_id: String,
    pub cycle_number: i32,
    pub timestamp: DateTime<Utc>,
    pub system_prompt: String,
    pub input_prompt: String,
    pub cot_trace: String,
    pub decision_json: String,

    // Stored as JSON strings in DB, serialized to Vec<String> in struct
    pub candidate_coins: Vec<String>,
    pub execution_log: Vec<String>,

    pub success: bool,
    pub error_message: String,
    pub ai_request_duration_ms: i64,

    #[serde(default)]
    pub account_state: Option<AccountSnapshot>,
    #[serde(default)]
    pub positions: Vec<PositionSnapshot>,
    #[serde(default)]
    pub decisions: Vec<DecisionAction>,
}

/// AccountSnapshot account state snapshot
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct AccountSnapshot {
    pub total_balance: f64,
    pub available_balance: f64,
    pub total_unrealized_profit: f64,
    pub position_count: i32,
    pub margin_used_pct: f64,
    pub initial_balance: f64,
}

/// PositionSnapshot position snapshot
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PositionSnapshot {
    pub symbol: String,
    pub side: String,
    pub position_amt: f64,
    pub entry_price: f64,
    pub mark_price: f64,
    pub unrealized_profit: f64,
    pub leverage: f64,
    pub liquidation_price: f64,
}

/// DecisionAction decision action
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct DecisionAction {
    pub action: String,
    pub symbol: String,
    pub quantity: f64,
    pub leverage: i32,
    pub price: f64,
    pub order_id: i64,
    pub timestamp: DateTime<Utc>,
    pub success: bool,
    pub error: String,
}

/// Statistics information
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Statistics {
    pub total_cycles: i32,
    pub successful_cycles: i32,
    pub failed_cycles: i32,
    pub total_open_positions: i32,
    pub total_close_positions: i32,
}

// ==========================================
// Implementation
// ==========================================

impl DecisionStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Initializes AI decision log tables
    pub async fn init_tables(&self) -> Result<()> {
        let queries = vec![
            r#"CREATE TABLE IF NOT EXISTS decision_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trader_id TEXT NOT NULL,
                cycle_number INTEGER NOT NULL,
                timestamp DATETIME NOT NULL,
                system_prompt TEXT DEFAULT '',
                input_prompt TEXT DEFAULT '',
                cot_trace TEXT DEFAULT '',
                decision_json TEXT DEFAULT '',
                candidate_coins TEXT DEFAULT '',
                execution_log TEXT DEFAULT '',
                success BOOLEAN DEFAULT 0,
                error_message TEXT DEFAULT '',
                ai_request_duration_ms INTEGER DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )"#,
            r#"CREATE INDEX IF NOT EXISTS idx_decision_records_trader_time ON decision_records(trader_id, timestamp DESC)"#,
            r#"CREATE INDEX IF NOT EXISTS idx_decision_records_timestamp ON decision_records(timestamp DESC)"#,
        ];

        for query in queries {
            sqlx::query(query)
                .execute(&self.pool)
                .await
                .context("Failed to execute init table query")?;
        }

        Ok(())
    }

    /// Logs decision (only saves AI decision log)
    pub async fn log_decision(&self, record: &mut DecisionRecord) -> Result<()> {
        // Ensure timestamp is set
        if record.timestamp.timestamp() == 0 {
            record.timestamp = Utc::now();
        }

        // Serialize complex fields to JSON strings
        let candidate_coins_json =
            serde_json::to_string(&record.candidate_coins).unwrap_or_default();
        let execution_log_json = serde_json::to_string(&record.execution_log).unwrap_or_default();
        let timestamp_str = record.timestamp.to_rfc3339();

        let result = sqlx::query(
            r#"
            INSERT INTO decision_records (
                trader_id, cycle_number, timestamp, system_prompt, input_prompt,
                cot_trace, decision_json, candidate_coins, execution_log,
                success, error_message, ai_request_duration_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&record.trader_id)
        .bind(record.cycle_number)
        .bind(timestamp_str)
        .bind(&record.system_prompt)
        .bind(&record.input_prompt)
        .bind(&record.cot_trace)
        .bind(&record.decision_json)
        .bind(candidate_coins_json)
        .bind(execution_log_json)
        .bind(record.success)
        .bind(&record.error_message)
        .bind(record.ai_request_duration_ms)
        .execute(&self.pool)
        .await
        .context("Failed to insert decision record")?;

        record.id = result.last_insert_rowid();

        Ok(())
    }

    /// Gets the latest N records for specified trader (sorted by time in ascending order: old to new)
    pub async fn get_latest_records(&self, trader_id: &str, n: i64) -> Result<Vec<DecisionRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT id, trader_id, cycle_number, timestamp, system_prompt, input_prompt,
                   cot_trace, decision_json, candidate_coins, execution_log,
                   success, error_message, ai_request_duration_ms
            FROM decision_records
            WHERE trader_id = ?
            ORDER BY timestamp DESC
            LIMIT ?
            "#,
        )
        .bind(trader_id)
        .bind(n)
        .fetch_all(&self.pool)
        .await
        .context("Failed to query decision records")?;

        let mut records = Vec::new();
        for row in rows {
            records.push(self.scan_decision_record(&row)?);
        }

        // Fill associated data (empty implementation as per Go code)
        for record in &mut records {
            self.fill_record_details(record);
        }

        // Reverse to return Old -> New
        records.reverse();

        Ok(records)
    }

    /// Gets the latest N records for all traders
    pub async fn get_all_latest_records(&self, n: i64) -> Result<Vec<DecisionRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT id, trader_id, cycle_number, timestamp, system_prompt, input_prompt,
                   cot_trace, decision_json, candidate_coins, execution_log,
                   success, error_message, ai_request_duration_ms
            FROM decision_records
            ORDER BY timestamp DESC
            LIMIT ?
            "#,
        )
        .bind(n)
        .fetch_all(&self.pool)
        .await
        .context("Failed to query decision records")?;

        let mut records = Vec::new();
        for row in rows {
            records.push(self.scan_decision_record(&row)?);
        }

        // Reverse to return Old -> New
        records.reverse();

        Ok(records)
    }

    /// Gets all records for a specified trader on a specified date
    pub async fn get_records_by_date(
        &self,
        trader_id: &str,
        date: DateTime<Utc>,
    ) -> Result<Vec<DecisionRecord>> {
        let date_str = date.format("%Y-%m-%d").to_string();

        let rows = sqlx::query(
            r#"
            SELECT id, trader_id, cycle_number, timestamp, system_prompt, input_prompt,
                   cot_trace, decision_json, candidate_coins, execution_log,
                   success, error_message, ai_request_duration_ms
            FROM decision_records
            WHERE trader_id = ? AND DATE(timestamp) = ?
            ORDER BY timestamp ASC
            "#,
        )
        .bind(trader_id)
        .bind(date_str)
        .fetch_all(&self.pool)
        .await
        .context("Failed to query decision records by date")?;

        let mut records = Vec::new();
        for row in rows {
            records.push(self.scan_decision_record(&row)?);
        }

        Ok(records)
    }

    /// Cleans old records from N days ago
    pub async fn clean_old_records(&self, trader_id: &str, days: i64) -> Result<u64> {
        let cutoff_time = Utc::now() - chrono::Duration::days(days);
        let cutoff_str = cutoff_time.to_rfc3339();

        let result = sqlx::query(
            r#"
            DELETE FROM decision_records
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

    /// Gets statistics information for specified trader
    pub async fn get_statistics(&self, trader_id: &str) -> Result<Statistics> {
        let mut stats = Statistics::default();

        // Total Cycles
        stats.total_cycles =
            sqlx::query_scalar("SELECT COUNT(*) FROM decision_records WHERE trader_id = ?")
                .bind(trader_id)
                .fetch_one(&self.pool)
                .await
                .unwrap_or(0);

        // Successful Cycles
        stats.successful_cycles = sqlx::query_scalar(
            "SELECT COUNT(*) FROM decision_records WHERE trader_id = ? AND success = 1",
        )
        .bind(trader_id)
        .fetch_one(&self.pool)
        .await
        .unwrap_or(0);

        stats.failed_cycles = stats.total_cycles - stats.successful_cycles;

        // Total Open Positions (from trader_orders)
        stats.total_open_positions = sqlx::query_scalar(
            "SELECT COUNT(*) FROM trader_orders WHERE trader_id = ? AND status = 'FILLED' AND action IN ('open_long', 'open_short')"
        )
        .bind(trader_id)
        .fetch_one(&self.pool)
        .await
        .unwrap_or(0);

        // Total Close Positions (from trader_orders)
        stats.total_close_positions = sqlx::query_scalar(
            "SELECT COUNT(*) FROM trader_orders WHERE trader_id = ? AND status = 'FILLED' AND action IN ('close_long', 'close_short', 'auto_close_long', 'auto_close_short')"
        )
        .bind(trader_id)
        .fetch_one(&self.pool)
        .await
        .unwrap_or(0);

        Ok(stats)
    }

    /// Gets statistics information for all traders
    pub async fn get_all_statistics(&self) -> Result<Statistics> {
        let mut stats = Statistics::default();

        stats.total_cycles = sqlx::query_scalar("SELECT COUNT(*) FROM decision_records")
            .fetch_one(&self.pool)
            .await
            .unwrap_or(0);

        stats.successful_cycles =
            sqlx::query_scalar("SELECT COUNT(*) FROM decision_records WHERE success = 1")
                .fetch_one(&self.pool)
                .await
                .unwrap_or(0);

        stats.failed_cycles = stats.total_cycles - stats.successful_cycles;

        stats.total_open_positions = sqlx::query_scalar(
            "SELECT COUNT(*) FROM trader_orders WHERE status = 'FILLED' AND action IN ('open_long', 'open_short')"
        )
        .fetch_one(&self.pool)
        .await
        .unwrap_or(0);

        stats.total_close_positions = sqlx::query_scalar(
            "SELECT COUNT(*) FROM trader_orders WHERE status = 'FILLED' AND action IN ('close_long', 'close_short', 'auto_close_long', 'auto_close_short')"
        )
        .fetch_one(&self.pool)
        .await
        .unwrap_or(0);

        Ok(stats)
    }

    /// Gets the last cycle number for specified trader
    pub async fn get_last_cycle_number(&self, trader_id: &str) -> Result<i32> {
        let cycle_number: Option<i32> = sqlx::query_scalar(
            "SELECT MAX(cycle_number) FROM decision_records WHERE trader_id = ?",
        )
        .bind(trader_id)
        .fetch_optional(&self.pool)
        .await?
        .flatten();

        Ok(cycle_number.unwrap_or(0))
    }

    // Helper: Scan row into DecisionRecord
    fn scan_decision_record(&self, row: &SqliteRow) -> Result<DecisionRecord> {
        let timestamp_str: String = row.try_get("timestamp")?;
        let timestamp = DateTime::parse_from_rfc3339(&timestamp_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now()); // Fallback if parse fails, though DB should strictly adhere

        let candidate_coins_json: String = row.try_get("candidate_coins").unwrap_or_default();
        let execution_log_json: String = row.try_get("execution_log").unwrap_or_default();

        let candidate_coins: Vec<String> =
            serde_json::from_str(&candidate_coins_json).unwrap_or_default();
        let execution_log: Vec<String> =
            serde_json::from_str(&execution_log_json).unwrap_or_default();

        Ok(DecisionRecord {
            id: row.try_get("id")?,
            trader_id: row.try_get("trader_id")?,
            cycle_number: row.try_get("cycle_number")?,
            timestamp,
            system_prompt: row.try_get("system_prompt").unwrap_or_default(),
            input_prompt: row.try_get("input_prompt").unwrap_or_default(),
            cot_trace: row.try_get("cot_trace").unwrap_or_default(),
            decision_json: row.try_get("decision_json").unwrap_or_default(),
            candidate_coins,
            execution_log,
            success: row.try_get("success")?,
            error_message: row.try_get("error_message").unwrap_or_default(),
            ai_request_duration_ms: row.try_get("ai_request_duration_ms").unwrap_or_default(),
            // Fields not present in DB, defaulted
            account_state: None,
            positions: vec![],
            decisions: vec![],
        })
    }

    // Helper: Fill associated details (Kept for compatibility)
    fn fill_record_details(&self, _record: &mut DecisionRecord) {
        // As per Go code comments:
        // "Old associated tables removed, no longer need to fill"
    }
}
