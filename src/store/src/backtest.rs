use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(rename_all = "lowercase")]
pub enum RunState {
    Created,
    Running,
    Paused,
    Completed,
    Failed,
}

impl ToString for RunState {
    fn to_string(&self) -> String {
        match self {
            RunState::Created => "created".to_string(),
            RunState::Running => "running".to_string(),
            RunState::Paused => "paused".to_string(),
            RunState::Completed => "completed".to_string(),
            RunState::Failed => "failed".to_string(),
        }
    }
}

impl FromStr for RunState {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "created" => Ok(RunState::Created),
            "running" => Ok(RunState::Running),
            "paused" => Ok(RunState::Paused),
            "completed" => Ok(RunState::Completed),
            "failed" => Ok(RunState::Failed),
            _ => Err(anyhow!("Invalid RunState: {}", s)),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RunSummary {
    #[serde(rename = "symbol_count")]
    pub symbol_count: i32,
    #[serde(rename = "decision_tf")]
    pub decision_tf: String,
    #[serde(rename = "processed_bars")]
    pub processed_bars: i32,
    #[serde(rename = "progress_pct")]
    pub progress_pct: f64,
    #[serde(rename = "equity_last")]
    pub equity_last: f64,
    #[serde(rename = "max_drawdown_pct")]
    pub max_drawdown_pct: f64,
    pub liquidated: bool,
    #[serde(rename = "liquidation_note")]
    pub liquidation_note: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RunMetadata {
    #[serde(rename = "run_id")]
    pub run_id: String,
    #[serde(rename = "user_id")]
    pub user_id: String,
    pub version: i32,
    pub state: RunState,
    pub label: String,
    #[serde(rename = "last_error")]
    pub last_error: String,
    pub summary: RunSummary,
    #[serde(rename = "created_at")]
    pub created_at: DateTime<Utc>,
    #[serde(rename = "updated_at")]
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct EquityPoint {
    pub timestamp: i64,
    pub equity: f64,
    pub available: f64,
    pub pnl: f64,
    #[serde(rename = "pnl_pct")]
    pub pnl_pct: f64,
    #[serde(rename = "drawdown_pct")]
    #[sqlx(rename = "dd_pct")]
    pub drawdown_pct: f64,
    pub cycle: i32,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct TradeEvent {
    #[sqlx(rename = "ts")]
    pub timestamp: i64,
    pub symbol: String,
    pub action: String,
    pub side: String,
    #[sqlx(rename = "qty")]
    pub quantity: f64,
    pub price: f64,
    pub fee: f64,
    pub slippage: f64,
    #[serde(rename = "order_value")]
    pub order_value: f64,
    #[serde(rename = "realized_pnl")]
    pub realized_pnl: f64,
    pub leverage: i32,
    pub cycle: i32,
    #[serde(rename = "position_after")]
    pub position_after: f64,
    #[serde(rename = "liquidation_flag")]
    #[sqlx(rename = "liquidation")]
    pub liquidation_flag: bool,
    pub note: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RunIndexEntry {
    #[serde(rename = "run_id")]
    pub run_id: String,
    pub state: String,
    pub symbols: Vec<String>,
    #[serde(rename = "decision_tf")]
    pub decision_tf: String,
    #[serde(rename = "equity_last")]
    pub equity_last: f64,
    #[serde(rename = "max_drawdown_pct")]
    pub max_drawdown_pct: f64,
    #[serde(rename = "start_ts")]
    pub start_ts: i64,
    #[serde(rename = "end_ts")]
    pub end_ts: i64,
    #[serde(rename = "created_at")]
    pub created_at_iso: String,
    #[serde(rename = "updated_at")]
    pub updated_at_iso: String,
}

#[derive(Clone)]
pub struct BacktestStore {
    db: SqlitePool,
}

impl BacktestStore {
    pub fn new(db: SqlitePool) -> Self {
        Self { db }
    }

    pub async fn init_tables(&self) -> Result<()> {
        let queries = vec![
            // Backtest runs main table
            r#"CREATE TABLE IF NOT EXISTS backtest_runs (
                run_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL DEFAULT '',
                config_json TEXT NOT NULL DEFAULT '',
                state TEXT NOT NULL DEFAULT 'created',
                label TEXT DEFAULT '',
                symbol_count INTEGER DEFAULT 0,
                decision_tf TEXT DEFAULT '',
                processed_bars INTEGER DEFAULT 0,
                progress_pct REAL DEFAULT 0,
                equity_last REAL DEFAULT 0,
                max_drawdown_pct REAL DEFAULT 0,
                liquidated BOOLEAN DEFAULT 0,
                liquidation_note TEXT DEFAULT '',
                prompt_template TEXT DEFAULT '',
                custom_prompt TEXT DEFAULT '',
                override_prompt BOOLEAN DEFAULT 0,
                ai_provider TEXT DEFAULT '',
                ai_model TEXT DEFAULT '',
                last_error TEXT DEFAULT '',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )"#,
            // Backtest checkpoints
            r#"CREATE TABLE IF NOT EXISTS backtest_checkpoints (
                run_id TEXT PRIMARY KEY,
                payload BLOB NOT NULL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (run_id) REFERENCES backtest_runs(run_id) ON DELETE CASCADE
            )"#,
            // Backtest equity curve
            r#"CREATE TABLE IF NOT EXISTS backtest_equity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                ts INTEGER NOT NULL,
                equity REAL NOT NULL,
                available REAL NOT NULL,
                pnl REAL NOT NULL,
                pnl_pct REAL NOT NULL,
                dd_pct REAL NOT NULL,
                cycle INTEGER NOT NULL,
                FOREIGN KEY (run_id) REFERENCES backtest_runs(run_id) ON DELETE CASCADE
            )"#,
            // Backtest trade records
            r#"CREATE TABLE IF NOT EXISTS backtest_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                ts INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                action TEXT NOT NULL,
                side TEXT DEFAULT '',
                qty REAL DEFAULT 0,
                price REAL DEFAULT 0,
                fee REAL DEFAULT 0,
                slippage REAL DEFAULT 0,
                order_value REAL DEFAULT 0,
                realized_pnl REAL DEFAULT 0,
                leverage INTEGER DEFAULT 0,
                cycle INTEGER DEFAULT 0,
                position_after REAL DEFAULT 0,
                liquidation BOOLEAN DEFAULT 0,
                note TEXT DEFAULT '',
                FOREIGN KEY (run_id) REFERENCES backtest_runs(run_id) ON DELETE CASCADE
            )"#,
            // Backtest metrics
            r#"CREATE TABLE IF NOT EXISTS backtest_metrics (
                run_id TEXT PRIMARY KEY,
                payload BLOB NOT NULL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (run_id) REFERENCES backtest_runs(run_id) ON DELETE CASCADE
            )"#,
            // Backtest decision logs
            r#"CREATE TABLE IF NOT EXISTS backtest_decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                cycle INTEGER NOT NULL,
                payload BLOB NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (run_id) REFERENCES backtest_runs(run_id) ON DELETE CASCADE
            )"#,
            // Indexes
            r#"CREATE INDEX IF NOT EXISTS idx_backtest_runs_state ON backtest_runs(state, updated_at)"#,
            r#"CREATE INDEX IF NOT EXISTS idx_backtest_equity_run_ts ON backtest_equity(run_id, ts)"#,
            r#"CREATE INDEX IF NOT EXISTS idx_backtest_trades_run_ts ON backtest_trades(run_id, ts)"#,
            r#"CREATE INDEX IF NOT EXISTS idx_backtest_decisions_run_cycle ON backtest_decisions(run_id, cycle)"#,
        ];

        for query in queries {
            sqlx::query(query).execute(&self.db).await?;
        }

        // Add potentially missing columns (backward compatibility)
        self.add_column_if_not_exists("backtest_runs", "label", "TEXT DEFAULT ''")
            .await;
        self.add_column_if_not_exists("backtest_runs", "last_error", "TEXT DEFAULT ''")
            .await;
        self.add_column_if_not_exists("backtest_trades", "leverage", "INTEGER DEFAULT 0")
            .await;

        Ok(())
    }

    async fn add_column_if_not_exists(&self, table: &str, column: &str, definition: &str) {
        let check_query = format!("PRAGMA table_info({})", table);
        let rows = sqlx::query(&check_query).fetch_all(&self.db).await;

        if let Ok(rows) = rows {
            for row in rows {
                // table_info returns columns like: cid, name, type, notnull, dflt_value, pk
                let name: String = row.get("name");
                if name == column {
                    return; // Column already exists
                }
            }
        }

        let alter_query = format!("ALTER TABLE {} ADD COLUMN {} {}", table, column, definition);
        let _ = sqlx::query(&alter_query).execute(&self.db).await;
    }

    // SaveCheckpoint saves checkpoint
    pub async fn save_checkpoint(&self, run_id: &str, payload: &[u8]) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO backtest_checkpoints (run_id, payload, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(run_id) DO UPDATE SET payload=excluded.payload, updated_at=CURRENT_TIMESTAMP
            "#,
        )
        .bind(run_id)
        .bind(payload)
        .execute(&self.db)
        .await?;
        Ok(())
    }

    // LoadCheckpoint loads checkpoint
    pub async fn load_checkpoint(&self, run_id: &str) -> Result<Vec<u8>> {
        let payload: Vec<u8> =
            sqlx::query_scalar("SELECT payload FROM backtest_checkpoints WHERE run_id = ?")
                .bind(run_id)
                .fetch_one(&self.db)
                .await?;
        Ok(payload)
    }

    // SaveRunMetadata saves run metadata
    pub async fn save_run_metadata(&self, meta: &RunMetadata) -> Result<()> {
        // sqlx maps DateTime<Utc> automatically, but we can also treat them as strings to match Go code exactly
        // Go: created := meta.CreatedAt.UTC().Format(time.RFC3339)
        // Here we let sqlx handle parameter binding or convert explicitly if needed.
        // Assuming database is storing ISO8601 strings or compatible format.

        sqlx::query(
            r#"
            INSERT INTO backtest_runs (run_id, user_id, label, last_error, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO NOTHING
            "#,
        )
        .bind(&meta.run_id)
        .bind(&meta.user_id)
        .bind(&meta.label)
        .bind(&meta.last_error)
        .bind(meta.created_at)
        .bind(meta.updated_at)
        .execute(&self.db)
        .await?;

        sqlx::query(
            r#"
            UPDATE backtest_runs
            SET user_id = ?, state = ?, symbol_count = ?, decision_tf = ?, processed_bars = ?,
                progress_pct = ?, equity_last = ?, max_drawdown_pct = ?, liquidated = ?,
                liquidation_note = ?, label = ?, last_error = ?, updated_at = ?
            WHERE run_id = ?
            "#,
        )
        .bind(&meta.user_id)
        .bind(meta.state.to_string())
        .bind(meta.summary.symbol_count as i32)
        .bind(&meta.summary.decision_tf)
        .bind(meta.summary.processed_bars as i32)
        .bind(meta.summary.progress_pct)
        .bind(meta.summary.equity_last)
        .bind(meta.summary.max_drawdown_pct)
        .bind(meta.summary.liquidated)
        .bind(&meta.summary.liquidation_note)
        .bind(&meta.label)
        .bind(&meta.last_error)
        .bind(meta.updated_at)
        .bind(&meta.run_id)
        .execute(&self.db)
        .await?;

        Ok(())
    }

    // LoadRunMetadata loads run metadata
    pub async fn load_run_metadata(&self, run_id: &str) -> Result<RunMetadata> {
        let row = sqlx::query(
            r#"
            SELECT user_id, state, label, last_error, symbol_count, decision_tf, processed_bars,
                   progress_pct, equity_last, max_drawdown_pct, liquidated, liquidation_note,
                   created_at, updated_at
            FROM backtest_runs WHERE run_id = ?
            "#,
        )
        .bind(run_id)
        .fetch_one(&self.db)
        .await?;

        // sqlx matches SQLite DATETIME -> chrono::NaiveDateTime usually, or DateTime<Utc> if configured
        // We'll trust sqlx's FromRow/Get logic to convert to DateTime<Utc> if stored as standard ISO string.
        let created_at: DateTime<Utc> = row.try_get("created_at")?;
        let updated_at: DateTime<Utc> = row.try_get("updated_at")?;
        let state_str: String = row.try_get("state")?;

        let meta = RunMetadata {
            run_id: run_id.to_string(),
            user_id: row.try_get("user_id")?,
            version: 1,
            state: RunState::from_str(&state_str)?,
            label: row.try_get("label")?,
            last_error: row.try_get("last_error")?,
            summary: RunSummary {
                symbol_count: row.try_get("symbol_count")?,
                decision_tf: row.try_get("decision_tf")?,
                processed_bars: row.try_get("processed_bars")?,
                progress_pct: row.try_get("progress_pct")?,
                equity_last: row.try_get("equity_last")?,
                max_drawdown_pct: row.try_get("max_drawdown_pct")?,
                liquidated: row.try_get("liquidated")?,
                liquidation_note: row.try_get("liquidation_note")?,
            },
            created_at,
            updated_at,
        };

        Ok(meta)
    }

    // ListRunIDs lists all run IDs
    pub async fn list_run_ids(&self) -> Result<Vec<String>> {
        let rows =
            sqlx::query("SELECT run_id FROM backtest_runs ORDER BY datetime(updated_at) DESC")
                .fetch_all(&self.db)
                .await?;

        let ids = rows.iter().map(|r| r.get("run_id")).collect();
        Ok(ids)
    }

    // AppendEquityPoint appends equity point
    pub async fn append_equity_point(&self, run_id: &str, point: &EquityPoint) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO backtest_equity (run_id, ts, equity, available, pnl, pnl_pct, dd_pct, cycle)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(run_id)
        .bind(point.timestamp)
        .bind(point.equity)
        .bind(point.available)
        .bind(point.pnl)
        .bind(point.pnl_pct)
        .bind(point.drawdown_pct)
        .bind(point.cycle)
        .execute(&self.db)
        .await?;
        Ok(())
    }

    // LoadEquityPoints loads equity points
    pub async fn load_equity_points(&self, run_id: &str) -> Result<Vec<EquityPoint>> {
        let points = sqlx::query_as::<_, EquityPoint>(
            r#"
            SELECT ts, equity, available, pnl, pnl_pct, dd_pct, cycle
            FROM backtest_equity WHERE run_id = ? ORDER BY ts ASC
            "#,
        )
        .bind(run_id)
        .fetch_all(&self.db)
        .await?;

        Ok(points)
    }

    // AppendTradeEvent appends trade event
    pub async fn append_trade_event(&self, run_id: &str, event: &TradeEvent) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO backtest_trades (run_id, ts, symbol, action, side, qty, price, fee,
                                         slippage, order_value, realized_pnl, leverage, cycle,
                                         position_after, liquidation, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(run_id)
        .bind(event.timestamp)
        .bind(&event.symbol)
        .bind(&event.action)
        .bind(&event.side)
        .bind(event.quantity)
        .bind(event.price)
        .bind(event.fee)
        .bind(event.slippage)
        .bind(event.order_value)
        .bind(event.realized_pnl)
        .bind(event.leverage)
        .bind(event.cycle)
        .bind(event.position_after)
        .bind(event.liquidation_flag)
        .bind(&event.note)
        .execute(&self.db)
        .await?;
        Ok(())
    }

    // LoadTradeEvents loads trade events
    pub async fn load_trade_events(&self, run_id: &str) -> Result<Vec<TradeEvent>> {
        let events = sqlx::query_as::<_, TradeEvent>(
            r#"
            SELECT ts, symbol, action, side, qty, price, fee, slippage, order_value,
                   realized_pnl, leverage, cycle, position_after, liquidation, note
            FROM backtest_trades WHERE run_id = ? ORDER BY ts ASC
            "#,
        )
        .bind(run_id)
        .fetch_all(&self.db)
        .await?;

        Ok(events)
    }

    // SaveMetrics saves metrics
    pub async fn save_metrics(&self, run_id: &str, payload: &[u8]) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO backtest_metrics (run_id, payload, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(run_id) DO UPDATE SET payload=excluded.payload, updated_at=CURRENT_TIMESTAMP
            "#,
        )
        .bind(run_id)
        .bind(payload)
        .execute(&self.db)
        .await?;
        Ok(())
    }

    // LoadMetrics loads metrics
    pub async fn load_metrics(&self, run_id: &str) -> Result<Vec<u8>> {
        let payload: Vec<u8> =
            sqlx::query_scalar("SELECT payload FROM backtest_metrics WHERE run_id = ?")
                .bind(run_id)
                .fetch_one(&self.db)
                .await?;
        Ok(payload)
    }

    // SaveDecisionRecord saves decision record
    pub async fn save_decision_record(
        &self,
        run_id: &str,
        cycle: i32,
        payload: &[u8],
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO backtest_decisions (run_id, cycle, payload)
            VALUES (?, ?, ?)
            "#,
        )
        .bind(run_id)
        .bind(cycle)
        .bind(payload)
        .execute(&self.db)
        .await?;
        Ok(())
    }

    // LoadDecisionRecords loads decision records
    pub async fn load_decision_records(
        &self,
        run_id: &str,
        limit: i32,
        offset: i32,
    ) -> Result<Vec<serde_json::Value>> {
        let rows = sqlx::query(
            r#"
            SELECT payload FROM backtest_decisions
            WHERE run_id = ?
            ORDER BY id DESC
            LIMIT ? OFFSET ?
            "#,
        )
        .bind(run_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.db)
        .await?;

        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let payload: Vec<u8> = row.get("payload");
            let json_val: serde_json::Value = serde_json::from_slice(&payload)?;
            records.push(json_val);
        }
        Ok(records)
    }

    // LoadLatestDecision loads latest decision
    pub async fn load_latest_decision(&self, run_id: &str, cycle: i32) -> Result<Vec<u8>> {
        let result = if cycle > 0 {
            sqlx::query_scalar(
                "SELECT payload FROM backtest_decisions WHERE run_id = ? AND cycle = ? ORDER BY datetime(created_at) DESC LIMIT 1",
            )
            .bind(run_id)
            .bind(cycle)
            .fetch_one(&self.db)
            .await
        } else {
            sqlx::query_scalar(
                "SELECT payload FROM backtest_decisions WHERE run_id = ? ORDER BY datetime(created_at) DESC LIMIT 1",
            )
            .bind(run_id)
            .fetch_one(&self.db)
            .await
        };

        match result {
            Ok(payload) => Ok(payload),
            Err(e) => Err(anyhow::Error::new(e)),
        }
    }

    // UpdateProgress updates progress
    pub async fn update_progress(
        &self,
        run_id: &str,
        progress_pct: f64,
        equity: f64,
        bar_index: i32,
        liquidated: bool,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE backtest_runs
            SET progress_pct = ?, equity_last = ?, processed_bars = ?, liquidated = ?, updated_at = CURRENT_TIMESTAMP
            WHERE run_id = ?
            "#,
        )
        .bind(progress_pct)
        .bind(equity)
        .bind(bar_index)
        .bind(liquidated)
        .bind(run_id)
        .execute(&self.db)
        .await?;
        Ok(())
    }

    // ListIndexEntries lists index entries
    pub async fn list_index_entries(&self) -> Result<Vec<RunIndexEntry>> {
        let rows = sqlx::query(
            r#"
            SELECT run_id, state, symbol_count, decision_tf, equity_last, max_drawdown_pct,
                   created_at, updated_at, config_json
            FROM backtest_runs
            ORDER BY datetime(updated_at) DESC
            "#,
        )
        .fetch_all(&self.db)
        .await?;

        // Helper struct for parsing internal config JSON
        #[derive(Deserialize)]
        struct PartialConfig {
            #[serde(default)]
            symbols: Vec<String>,
            #[serde(default)]
            start_ts: i64,
            #[serde(default)]
            end_ts: i64,
        }

        let mut entries = Vec::new();

        for row in rows {
            let run_id: String = row.get("run_id");
            let state: String = row.get("state");
            let symbol_count: i32 = row.get("symbol_count");
            let decision_tf: String = row.get("decision_tf");
            let equity_last: f64 = row.get("equity_last");
            let max_drawdown_pct: f64 = row.get("max_drawdown_pct");

            // Format dates as ISO strings
            let created_at: DateTime<Utc> = row.get("created_at");
            let updated_at: DateTime<Utc> = row.get("updated_at");
            let created_iso = created_at.to_rfc3339();
            let updated_iso = updated_at.to_rfc3339();

            let config_json: String = row.get("config_json");

            let mut symbols = Vec::with_capacity(symbol_count as usize);
            let mut start_ts = 0;
            let mut end_ts = 0;

            if !config_json.is_empty() {
                if let Ok(cfg) = serde_json::from_str::<PartialConfig>(&config_json) {
                    symbols = cfg.symbols;
                    start_ts = cfg.start_ts;
                    end_ts = cfg.end_ts;
                }
            }

            entries.push(RunIndexEntry {
                run_id,
                state,
                symbols,
                decision_tf,
                equity_last,
                max_drawdown_pct,
                start_ts,
                end_ts,
                created_at_iso: created_iso,
                updated_at_iso: updated_iso,
            });
        }

        Ok(entries)
    }

    // DeleteRun deletes run
    pub async fn delete_run(&self, run_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM backtest_runs WHERE run_id = ?")
            .bind(run_id)
            .execute(&self.db)
            .await?;
        Ok(())
    }

    // SaveConfig saves config
    #[allow(clippy::too_many_arguments)]
    pub async fn save_config(
        &self,
        run_id: &str,
        user_id: &str,
        template: &str,
        custom_prompt: &str,
        provider: &str,
        model: &str,
        override_prompt: bool,
        config_json: &[u8],
    ) -> Result<()> {
        let now = Utc::now();
        let user_id = if user_id.is_empty() {
            "default"
        } else {
            user_id
        };
        let config_str = std::str::from_utf8(config_json).unwrap_or("");

        sqlx::query(
            r#"
            INSERT INTO backtest_runs (run_id, user_id, config_json, prompt_template, custom_prompt,
                                       override_prompt, ai_provider, ai_model, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO NOTHING
            "#,
        )
        .bind(run_id)
        .bind(user_id)
        .bind(config_str)
        .bind(template)
        .bind(custom_prompt)
        .bind(override_prompt)
        .bind(provider)
        .bind(model)
        .bind(now)
        .bind(now)
        .execute(&self.db)
        .await?;

        sqlx::query(
            r#"
            UPDATE backtest_runs
            SET user_id = ?, config_json = ?, prompt_template = ?, custom_prompt = ?,
                override_prompt = ?, ai_provider = ?, ai_model = ?, updated_at = CURRENT_TIMESTAMP
            WHERE run_id = ?
            "#,
        )
        .bind(user_id)
        .bind(config_str)
        .bind(template)
        .bind(custom_prompt)
        .bind(override_prompt)
        .bind(provider)
        .bind(model)
        .bind(run_id)
        .execute(&self.db)
        .await?;

        Ok(())
    }

    // LoadConfig loads config
    pub async fn load_config(&self, run_id: &str) -> Result<Vec<u8>> {
        let config_str: String =
            sqlx::query_scalar("SELECT config_json FROM backtest_runs WHERE run_id = ?")
                .bind(run_id)
                .fetch_one(&self.db)
                .await?;

        Ok(config_str.into_bytes())
    }
}
