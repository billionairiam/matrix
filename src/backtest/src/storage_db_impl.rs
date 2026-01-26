use std::fs::File;
use std::io::Write;

use crate::config::BacktestConfig;
use crate::registry::RunIndexEntry;
use crate::storage::ProgressPayload;
use crate::types::Checkpoint;
use crate::types::EquityPoint;
use crate::types::Metrics;
use crate::types::RunMetadata;
use crate::types::RunState;
use crate::types::RunSummary;
use crate::types::TradeEvent;
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{Pool, Row, Sqlite};
use store::decision::DecisionRecord;
use zip::CompressionMethod;
use zip::write::FileOptions;

pub async fn save_checkpoint_db(
    pool: &Pool<Sqlite>,
    run_id: &str,
    ckpt: &Checkpoint,
) -> Result<()> {
    let data = serde_json::to_vec(ckpt).map_err(|e| sqlx::Error::Protocol(e.to_string().into()))?;

    sqlx::query(
        r#"
        INSERT INTO backtest_checkpoints (run_id, payload, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(run_id) DO UPDATE SET payload=excluded.payload, updated_at=CURRENT_TIMESTAMP
        "#,
    )
    .bind(run_id)
    .bind(data)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn load_checkpoint_db(
    pool: &Pool<Sqlite>,
    run_id: &str,
) -> Result<Checkpoint, sqlx::Error> {
    let row = sqlx::query("SELECT payload FROM backtest_checkpoints WHERE run_id = ?")
        .bind(run_id)
        .fetch_optional(pool)
        .await?;

    match row {
        Some(row) => {
            let payload: Vec<u8> = row.try_get("payload")?;
            let ckpt = serde_json::from_slice(&payload)
                .map_err(|e| sqlx::Error::Protocol(e.to_string().into()))?;
            Ok(ckpt)
        }
        None => Err(sqlx::Error::RowNotFound),
    }
}

pub async fn save_config_db(pool: &Pool<Sqlite>, run_id: &str, cfg: &BacktestConfig) -> Result<()> {
    let mut persist = cfg.clone();
    persist.ai_cfg.api_key = String::new(); // Clear sensitive data

    let data =
        serde_json::to_vec(&persist).map_err(|e| sqlx::Error::Protocol(e.to_string().into()))?;

    let template = if cfg.prompt_template.is_empty() {
        "default"
    } else {
        &cfg.prompt_template
    };
    let user_id = if cfg.user_id.is_empty() {
        "default"
    } else {
        &cfg.user_id
    };

    let now = Utc::now().to_rfc3339();

    // First Insert/Ignore
    sqlx::query(
        r#"
        INSERT INTO backtest_runs (run_id, user_id, config_json, prompt_template, custom_prompt, override_prompt, ai_provider, ai_model, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id) DO NOTHING
        "#
    )
    .bind(run_id)
    .bind(user_id)
    .bind(&data)
    .bind(template)
    .bind(&cfg.custom_prompt)
    .bind(cfg.override_base_prompt)
    .bind(&cfg.ai_cfg.provider)
    .bind(&cfg.ai_cfg.model)
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await?;

    // Then Update
    sqlx::query(
        r#"
        UPDATE backtest_runs
        SET user_id = ?, config_json = ?, prompt_template = ?, custom_prompt = ?, override_prompt = ?, ai_provider = ?, ai_model = ?, updated_at = CURRENT_TIMESTAMP
        WHERE run_id = ?
        "#
    )
    .bind(user_id)
    .bind(&data)
    .bind(template)
    .bind(&cfg.custom_prompt)
    .bind(cfg.override_base_prompt)
    .bind(&cfg.ai_cfg.provider)
    .bind(&cfg.ai_cfg.model)
    .bind(run_id)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn load_config_db(pool: &Pool<Sqlite>, run_id: &str) -> Result<BacktestConfig> {
    let row = sqlx::query("SELECT config_json FROM backtest_runs WHERE run_id = ?")
        .bind(run_id)
        .fetch_one(pool)
        .await?;

    let payload: Vec<u8> = row.try_get("config_json")?;
    if payload.is_empty() {
        return Err(anyhow!("config missing for {}", run_id));
    }

    let cfg = serde_json::from_slice(&payload)
        .map_err(|e| sqlx::Error::Protocol(e.to_string().into()))?;
    Ok(cfg)
}

pub async fn save_run_metadata_db(pool: &Pool<Sqlite>, meta: &RunMetadata) -> Result<()> {
    let created = meta.created_at.to_rfc3339();
    let updated = meta.updated_at.to_rfc3339();
    let user_id = if meta.user_id.as_deref().unwrap_or("").is_empty() {
        "default"
    } else {
        meta.user_id.as_ref().unwrap()
    };

    // Using string representation for Enum, assuming `serde` serialization to string or manual impl
    let state_str = serde_json::to_string(&meta.state)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string();

    sqlx::query(
        r#"
        INSERT INTO backtest_runs (run_id, user_id, label, last_error, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id) DO NOTHING
        "#,
    )
    .bind(&meta.run_id)
    .bind(user_id)
    .bind(&meta.label)
    .bind(&meta.last_error)
    .bind(&created)
    .bind(&updated)
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        UPDATE backtest_runs
        SET user_id = ?, state = ?, symbol_count = ?, decision_tf = ?, processed_bars = ?, progress_pct = ?, equity_last = ?, max_drawdown_pct = ?, liquidated = ?, liquidation_note = ?, label = ?, last_error = ?, updated_at = ?
        WHERE run_id = ?
        "#
    )
    .bind(user_id)
    .bind(state_str)
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
    .bind(&updated)
    .bind(&meta.run_id)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn load_run_metadata_db(pool: &Pool<Sqlite>, run_id: &str) -> Result<RunMetadata> {
    let row = sqlx::query(
        r#"
        SELECT user_id, state, label, last_error, symbol_count, decision_tf, processed_bars, progress_pct, equity_last, max_drawdown_pct, liquidated, liquidation_note, created_at, updated_at
        FROM backtest_runs WHERE run_id = ?
        "#
    )
    .bind(run_id)
    .fetch_one(pool)
    .await?;

    let user_id: String = row.try_get("user_id")?;
    let state_str: String = row.try_get("state")?;
    let label: Option<String> = row.try_get("label")?;
    let last_error: Option<String> = row.try_get("last_error")?;
    let symbol_count: i32 = row.try_get("symbol_count")?;
    let decision_tf: String = row.try_get("decision_tf")?;
    let processed_bars: i32 = row.try_get("processed_bars")?;
    let progress_pct: f64 = row.try_get("progress_pct")?;
    let equity_last: f64 = row.try_get("equity_last")?;
    let max_dd: f64 = row.try_get("max_drawdown_pct")?;
    let liquidated: bool = row.try_get("liquidated")?;
    let liquidation_note: Option<String> = row.try_get("liquidation_note")?;
    let created_iso: String = row.try_get("created_at")?;
    let updated_iso: String = row.try_get("updated_at")?;

    let state: RunState = serde_json::from_str(&format!("\"{}\"", state_str))
        .map_err(|e| sqlx::Error::Protocol(e.to_string().into()))?;

    let created_at = DateTime::parse_from_rfc3339(&created_iso)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now());
    let updated_at = DateTime::parse_from_rfc3339(&updated_iso)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now());

    let meta = RunMetadata {
        run_id: run_id.to_string(),
        user_id: if user_id.is_empty() {
            Some("default".to_string())
        } else {
            Some(user_id)
        },
        version: 1,
        state,
        label,
        last_error,
        created_at,
        updated_at,
        summary: RunSummary {
            symbol_count: symbol_count as usize,
            decision_tf,
            processed_bars: processed_bars as usize,
            progress_pct,
            equity_last,
            max_drawdown_pct: max_dd,
            liquidated,
            liquidation_note,
        },
    };

    Ok(meta)
}

pub async fn load_run_ids_db(pool: &Pool<Sqlite>) -> Result<Vec<String>> {
    let rows = sqlx::query("SELECT run_id FROM backtest_runs ORDER BY datetime(updated_at) DESC")
        .fetch_all(pool)
        .await?;

    let mut ids = Vec::new();
    for row in rows {
        ids.push(row.try_get("run_id")?);
    }
    Ok(ids)
}

pub async fn query_retention_candidates(
    pool: &Pool<Sqlite>,
    query: &str,
    final_states: &Vec<RunState>,
    max_runs: i32,
) -> sqlx::Result<Vec<String>> {
    let rows = sqlx::query(query)
        .bind(final_states[0].as_str())
        .bind(final_states[1].as_str())
        .bind(final_states[2].as_str())
        .bind(final_states[3].as_str())
        .bind(max_runs)
        .fetch_all(pool)
        .await?;

    let mut ids = Vec::new();
    for row in rows {
        ids.push(row.try_get("run_id")?);
    }

    Ok(ids)
}

pub async fn append_equity_point_db(
    pool: &Pool<Sqlite>,
    run_id: &str,
    point: &EquityPoint,
) -> Result<()> {
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
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn load_equity_points_db(pool: &Pool<Sqlite>, run_id: &str) -> Result<Vec<EquityPoint>> {
    let rows = sqlx::query(
        r#"
        SELECT ts, equity, available, pnl, pnl_pct, dd_pct, cycle
        FROM backtest_equity WHERE run_id = ? ORDER BY ts ASC
        "#,
    )
    .bind(run_id)
    .fetch_all(pool)
    .await?;

    let mut points = Vec::new();
    for row in rows {
        points.push(EquityPoint {
            timestamp: row.try_get("ts")?,
            equity: row.try_get("equity")?,
            available: row.try_get("available")?,
            pnl: row.try_get("pnl")?,
            pnl_pct: row.try_get("pnl_pct")?,
            drawdown_pct: row.try_get("dd_pct")?,
            cycle: row.try_get("cycle")?,
        });
    }
    Ok(points)
}

pub async fn append_trade_event_db(
    pool: &Pool<Sqlite>,
    run_id: &str,
    event: &TradeEvent,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO backtest_trades (run_id, ts, symbol, action, side, qty, price, fee, slippage, order_value, realized_pnl, leverage, cycle, position_after, liquidation, note)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#
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
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn load_trade_events_db(pool: &Pool<Sqlite>, run_id: &str) -> Result<Vec<TradeEvent>> {
    let rows = sqlx::query(
        r#"
        SELECT ts, symbol, action, side, qty, price, fee, slippage, order_value, realized_pnl, leverage, cycle, position_after, liquidation, note
        FROM backtest_trades WHERE run_id = ? ORDER BY ts ASC
        "#
    )
    .bind(run_id)
    .fetch_all(pool)
    .await?;

    let mut events = Vec::new();
    for row in rows {
        events.push(TradeEvent {
            timestamp: row.try_get("ts")?,
            symbol: row.try_get("symbol")?,
            action: row.try_get("action")?,
            side: row.try_get("side")?,
            quantity: row.try_get("qty")?,
            price: row.try_get("price")?,
            fee: row.try_get("fee")?,
            slippage: row.try_get("slippage")?,
            order_value: row.try_get("order_value")?,
            realized_pnl: row.try_get("realized_pnl")?,
            leverage: row.try_get("leverage")?,
            cycle: row.try_get("cycle")?,
            position_after: row.try_get("position_after")?,
            liquidation_flag: row.try_get("liquidation")?,
            note: row.try_get("note")?,
        });
    }
    Ok(events)
}

pub async fn save_metrics_db(pool: &Pool<Sqlite>, run_id: &str, metrics: &Metrics) -> Result<()> {
    let data =
        serde_json::to_vec(metrics).map_err(|e| sqlx::Error::Protocol(e.to_string().into()))?;

    sqlx::query(
        r#"
        INSERT INTO backtest_metrics (run_id, payload, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(run_id) DO UPDATE SET payload=excluded.payload, updated_at=CURRENT_TIMESTAMP
        "#,
    )
    .bind(run_id)
    .bind(data)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn load_metrics_db(pool: &Pool<Sqlite>, run_id: &str) -> Result<Metrics> {
    let row = sqlx::query("SELECT payload FROM backtest_metrics WHERE run_id = ?")
        .bind(run_id)
        .fetch_one(pool)
        .await?;

    let payload: Vec<u8> = row.try_get("payload")?;
    let metrics = serde_json::from_slice(&payload).map_err(|e| anyhow!(e))?;
    Ok(metrics)
}

pub async fn save_progress_db(
    pool: &Pool<Sqlite>,
    run_id: &str,
    payload: &ProgressPayload,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE backtest_runs
        SET progress_pct = ?, equity_last = ?, processed_bars = ?, liquidated = ?, updated_at = ?
        WHERE run_id = ?
        "#,
    )
    .bind(payload.progress_pct)
    .bind(payload.equity)
    .bind(payload.bar_index)
    .bind(payload.liquidated)
    .bind(&payload.updated_at_iso)
    .bind(run_id)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn load_decision_trace_db(
    pool: &Pool<Sqlite>,
    run_id: &str,
    cycle: i32,
) -> Result<DecisionRecord> {
    let base_query = "SELECT payload FROM backtest_decisions WHERE run_id = ?";
    let row = if cycle > 0 {
        sqlx::query(&format!(
            "{} AND cycle = ? ORDER BY datetime(created_at) DESC LIMIT 1",
            base_query
        ))
        .bind(run_id)
        .bind(cycle)
        .fetch_optional(pool)
        .await?
    } else {
        sqlx::query(&format!(
            "{} ORDER BY datetime(created_at) DESC LIMIT 1",
            base_query
        ))
        .bind(run_id)
        .fetch_optional(pool)
        .await?
    };

    match row {
        Some(row) => {
            let payload: Vec<u8> = row.try_get("payload")?;
            let record = serde_json::from_slice(&payload)
                .map_err(|e| sqlx::Error::Protocol(e.to_string().into()))?;
            Ok(record)
        }
        None => Err(anyhow!("RowNotFound")),
    }
}

pub async fn save_decision_record_db(
    pool: &Pool<Sqlite>,
    run_id: &str,
    record: &DecisionRecord,
) -> Result<()> {
    let data =
        serde_json::to_vec(record).map_err(|e| sqlx::Error::Protocol(e.to_string().into()))?;

    sqlx::query(
        r#"
        INSERT INTO backtest_decisions (run_id, cycle, payload)
        VALUES (?, ?, ?)
        "#,
    )
    .bind(run_id)
    .bind(record.cycle_number)
    .bind(data)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn load_decision_records_db(
    pool: &Pool<Sqlite>,
    run_id: &str,
    limit: i32,
    offset: i32,
) -> Result<Vec<DecisionRecord>> {
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
    .fetch_all(pool)
    .await?;

    let mut records = Vec::new();
    for row in rows {
        let payload: Vec<u8> = row.try_get("payload")?;
        let record = serde_json::from_slice(&payload)
            .map_err(|e| sqlx::Error::Protocol(e.to_string().into()))?;
        records.push(record);
    }
    Ok(records)
}

pub async fn delete_run_db(pool: &Pool<Sqlite>, run_id: &str) -> sqlx::Result<()> {
    sqlx::query("DELETE FROM backtest_runs WHERE run_id = ?")
        .bind(run_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn list_index_entries_db(pool: &Pool<Sqlite>) -> Result<Vec<RunIndexEntry>> {
    let rows = sqlx::query(
        r#"
        SELECT run_id, state, symbol_count, decision_tf, equity_last, max_drawdown_pct, created_at, updated_at, config_json
        FROM backtest_runs
        ORDER BY datetime(updated_at) DESC
        "#
    )
    .fetch_all(pool)
    .await?;

    let mut entries = Vec::new();
    for row in rows {
        let run_id: String = row.try_get("run_id")?;
        let state_str: String = row.try_get("state")?;
        let symbol_count: i32 = row.try_get("symbol_count")?;
        let decision_tf: String = row.try_get("decision_tf")?;
        let equity_last: f64 = row.try_get("equity_last")?;
        let max_dd: f64 = row.try_get("max_drawdown_pct")?;
        let created_iso: String = row.try_get("created_at")?;
        let updated_iso: String = row.try_get("updated_at")?;
        let config_json: Vec<u8> = row.try_get("config_json")?;

        let state: RunState =
            serde_json::from_str(&format!("\"{}\"", state_str)).unwrap_or(RunState::Created); // Fallback

        let mut entry = RunIndexEntry {
            run_id,
            state,
            symbols: Vec::with_capacity(symbol_count as usize),
            decision_tf,
            start_ts: 0,
            end_ts: 0,
            equity_last,
            max_drawdown_pct: max_dd,
            created_at_iso: created_iso,
            updated_at_iso: updated_iso,
        };

        if !config_json.is_empty() {
            if let Ok(cfg) = serde_json::from_slice::<BacktestConfig>(&config_json) {
                entry.symbols = cfg.symbols;
                entry.start_ts = cfg.start_ts;
                entry.end_ts = cfg.end_ts;
            }
        }

        entries.push(entry);
    }
    Ok(entries)
}

pub async fn create_run_export_db(pool: &Pool<Sqlite>, run_id: &str) -> Result<String> {
    let run_id = run_id.to_string();
    let pool = pool.clone();

    // Fetch data async
    let meta = load_run_metadata_db(&pool, &run_id).await.ok();
    let cfg = load_config_db(&pool, &run_id).await.ok();
    let ckpt = load_checkpoint_db(&pool, &run_id).await.ok();
    let metrics = load_metrics_db(&pool, &run_id).await.ok();
    let points = load_equity_points_db(&pool, &run_id).await.ok();
    let trades = load_trade_events_db(&pool, &run_id).await.ok();

    // Decisions might be large, streaming them is better, but simplified here to match struct
    let decisions_rows = sqlx::query(
        "SELECT id, cycle, payload FROM backtest_decisions WHERE run_id = ? ORDER BY id ASC",
    )
    .bind(&run_id)
    .fetch_all(&pool)
    .await?;

    // Perform Zip writing in blocking thread
    let zip_path = tokio::task::spawn_blocking(
        move || -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
            let tmp_file = tempfile::Builder::new()
                .prefix(&format!("{}-", run_id))
                .suffix(".zip")
                .tempfile()?;

            let (file, path) = tmp_file.keep()?; // Persist file, return path
            let mut zip = zip::ZipWriter::new(file);

            if let Some(data) = meta {
                write_json_to_zip(&mut zip, "run.json", &data)?;
            }
            if let Some(data) = cfg {
                write_json_to_zip(&mut zip, "config.json", &data)?;
            }
            if let Some(data) = ckpt {
                write_json_to_zip(&mut zip, "checkpoint.json", &data)?;
            }
            if let Some(data) = metrics {
                write_json_to_zip(&mut zip, "metrics.json", &data)?;
            }
            if let Some(data) = points {
                if !data.is_empty() {
                    write_json_lines_to_zip(&mut zip, "equity.jsonl", &data)?;
                }
            }
            if let Some(data) = trades {
                if !data.is_empty() {
                    write_json_lines_to_zip(&mut zip, "trades.jsonl", &data)?;
                }
            }

            // Decisions
            for row in decisions_rows {
                let id: i64 = row.try_get("id")?;
                let cycle: i32 = row.try_get("cycle")?;
                let payload: Vec<u8> = row.try_get("payload")?;

                let name = format!("decision_logs/decision_{}_cycle{}.json", id, cycle);
                let options =
                    FileOptions::<()>::default().compression_method(CompressionMethod::Deflated);
                zip.start_file(name, options)?;
                zip.write_all(&payload)?;
            }

            zip.finish()?;

            Ok(path.to_string_lossy().to_string())
        },
    )
    .await?
    .map_err(|e| anyhow!("{}", e.to_string()));

    zip_path
}

fn write_json_to_zip<T: Serialize>(
    zip: &mut zip::ZipWriter<File>,
    name: &str,
    value: &T,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let data = serde_json::to_vec_pretty(value)?;
    let options = FileOptions::<()>::default().compression_method(CompressionMethod::Deflated);
    zip.start_file(name, options)?;
    zip.write_all(&data)?;
    Ok(())
}

fn write_json_lines_to_zip<T: Serialize>(
    zip: &mut zip::ZipWriter<File>,
    name: &str,
    items: &[T],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let options = FileOptions::<()>::default().compression_method(CompressionMethod::Deflated);
    zip.start_file(name, options)?;
    for item in items {
        let data = serde_json::to_vec(item)?;
        zip.write_all(&data)?;
        zip.write_all(b"\n")?;
    }
    Ok(())
}
