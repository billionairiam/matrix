use std::cmp::Ordering;
use std::fs as std_fs; // Synchronous fs for zip operations
use std::io::{self};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::config::BacktestConfig;
use crate::persistence_db::{get_db, using_db};
use crate::storage_db_impl::{
    append_equity_point_db, append_trade_event_db, create_run_export_db, load_checkpoint_db,
    load_config_db, load_decision_records_db, load_decision_trace_db, load_equity_points_db,
    load_metrics_db, load_run_ids_db, load_run_metadata_db, load_trade_events_db,
    save_checkpoint_db, save_config_db, save_decision_record_db, save_metrics_db, save_progress_db,
    save_run_metadata_db,
};
use crate::types::BacktestState;
use crate::types::Checkpoint;
use crate::types::EquityPoint;
use crate::types::Metrics;
use crate::types::RunMetadata;
use crate::types::TradeEvent;
use anyhow::{Result, anyhow};
use chrono::{TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use store::decision::DecisionRecord;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use uuid::Uuid;
use walkdir::WalkDir;
use zip::CompressionMethod;
use zip::write::FileOptions;

pub const BACKTESTS_ROOT_DIR: &str = "backtests";

#[derive(Debug, Serialize, Deserialize)]
pub struct ProgressPayload {
    #[serde(rename = "bar_index")]
    pub bar_index: i32,
    pub equity: f64,
    #[serde(rename = "progress_pct")]
    pub progress_pct: f64,
    pub liquidated: bool,
    #[serde(rename = "updated_at_iso")]
    pub updated_at_iso: String,
}

pub fn run_dir(run_id: &str) -> PathBuf {
    Path::new(BACKTESTS_ROOT_DIR).join(run_id)
}

pub async fn ensure_run_dir(run_id: &str) -> Result<()> {
    fs::create_dir_all(run_dir(run_id))
        .await
        .map_err(|e| anyhow!("{}", e))
}

fn checkpoint_path(run_id: &str) -> PathBuf {
    run_dir(run_id).join("checkpoint.json")
}

fn run_metadata_path(run_id: &str) -> PathBuf {
    run_dir(run_id).join("run.json")
}

fn equity_log_path(run_id: &str) -> PathBuf {
    run_dir(run_id).join("equity.jsonl")
}

fn trades_log_path(run_id: &str) -> PathBuf {
    run_dir(run_id).join("trades.jsonl")
}

fn metrics_path(run_id: &str) -> PathBuf {
    run_dir(run_id).join("metrics.json")
}

fn progress_path(run_id: &str) -> PathBuf {
    run_dir(run_id).join("progress.json")
}

pub fn decision_log_dir(run_id: &str) -> PathBuf {
    run_dir(run_id).join("decision_logs")
}

async fn write_json_atomic<T: Serialize>(path: &Path, v: &T) -> Result<()> {
    let data = serde_json::to_vec_pretty(v)?;
    write_file_atomic(path, &data).await
}

async fn write_file_atomic(path: &Path, data: &[u8]) -> Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "no parent dir"))?;
    fs::create_dir_all(dir).await?;

    let tmp_name = format!(".tmp-{}", Uuid::new_v4());
    let tmp_path = dir.join(tmp_name);

    {
        let mut tmp_file = File::create(&tmp_path).await?;
        tmp_file.write_all(data).await?;
        tmp_file.sync_all().await?;
    }

    // Rename is atomic on POSIX
    if let Err(e) = fs::rename(&tmp_path, path).await {
        let _ = fs::remove_file(&tmp_path).await;
        return Err(anyhow!("{}", e));
    }

    Ok(())
}

async fn append_json_line<T: Serialize>(path: &Path, payload: &T) -> Result<()> {
    let data = serde_json::to_vec(payload)?;

    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir).await?;
    }

    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .await?;

    file.write_all(&data).await?;
    file.write_all(b"\n").await?;
    file.flush().await?;
    file.sync_all().await?;

    Ok(())
}

pub async fn save_checkpoint(run_id: &str, ckpt: &Checkpoint) -> Result<()> {
    if using_db() {
        let pool = get_db().unwrap();
        return save_checkpoint_db(&pool, run_id, ckpt).await;
    }
    write_json_atomic(&checkpoint_path(run_id), ckpt).await
}

pub async fn load_checkpoint(run_id: &str) -> Result<Checkpoint> {
    if using_db() {
        let pool = get_db().unwrap();
        load_checkpoint_db(&pool, run_id)
            .await
            .map_err(|e| anyhow!("load_checkpoint_db failed {}", e))?;
    }
    let data = fs::read(checkpoint_path(run_id)).await?;
    let ckpt = serde_json::from_slice(&data)?;
    Ok(ckpt)
}

pub async fn save_run_metadata(meta: &mut RunMetadata) -> Result<()> {
    if meta.version == 0 {
        meta.version = 1;
    }
    meta.updated_at = Utc::now();

    if using_db() {
        let pool = get_db().unwrap();
        return save_run_metadata_db(&pool, meta).await;
    }
    write_json_atomic(&run_metadata_path(&meta.run_id), meta).await
}

pub async fn load_run_metadata(run_id: &str) -> Result<RunMetadata> {
    if using_db() {
        let pool = get_db().unwrap();
        return load_run_metadata_db(&pool, run_id).await;
    }

    let data = fs::read(run_metadata_path(run_id)).await?;
    let meta = serde_json::from_slice(&data)?;
    Ok(meta)
}

pub async fn append_equity_point(run_id: &str, point: &EquityPoint) -> Result<()> {
    if using_db() {
        let pool = get_db().unwrap();
        return append_equity_point_db(&pool, run_id, point).await;
    }
    append_json_line(&equity_log_path(run_id), point).await
}

pub async fn append_trade_event(run_id: &str, event: &TradeEvent) -> Result<()> {
    if using_db() {
        let pool = get_db().unwrap();
        return append_trade_event_db(&pool, run_id, event).await;
    }
    append_json_line(&trades_log_path(run_id), event).await
}

pub async fn save_metrics(run_id: &str, metrics: &Metrics) -> Result<()> {
    if using_db() {
        let pool = get_db().unwrap();
        return save_metrics_db(&pool, run_id, metrics).await;
    }
    write_json_atomic(&metrics_path(run_id), metrics).await
}

pub async fn save_progress(
    run_id: &str,
    state: &BacktestState,
    cfg: &BacktestConfig,
) -> Result<()> {
    let dur_ms = cfg.duration(); // Assuming cfg has a method returning i64/u64 duration
    let mut progress = 0.0;

    if dur_ms > TimeDelta::zero() {
        let current = state.bar_timestamp;
        let start_ms = cfg.start_ts * 1000;

        if current > start_ms {
            let elapsed = current - start_ms;
            progress = elapsed as f64 / dur_ms.as_seconds_f64();
        }
    }

    let payload = ProgressPayload {
        bar_index: state.bar_index as i32,
        equity: state.equity,
        progress_pct: progress * 100.0,
        liquidated: state.liquidated,
        updated_at_iso: Utc::now().to_rfc3339(),
    };

    if using_db() {
        let pool = get_db().unwrap();
        return save_progress_db(&pool, run_id, &payload).await;
    }
    write_json_atomic(&progress_path(run_id), &payload).await
}

pub async fn save_config(run_id: &str, cfg: &BacktestConfig) -> Result<()> {
    let mut persist = cfg.clone();
    persist.ai_cfg.api_key = String::new();

    if using_db() {
        let pool = get_db().unwrap();
        return save_config_db(&pool, run_id, cfg).await;
    }
    ensure_run_dir(run_id).await?;
    write_json_atomic(&run_dir(run_id).join("config.json"), &persist).await
}

pub async fn load_config(run_id: &str) -> Result<BacktestConfig> {
    if using_db() {
        let pool = get_db().unwrap();
        return load_config_db(&pool, run_id).await;
    }

    let data = fs::read(run_dir(run_id).join("config.json")).await?;
    let cfg = serde_json::from_slice(&data)?;
    Ok(cfg)
}

pub async fn load_equity_points(run_id: &str) -> Result<Vec<EquityPoint>> {
    if using_db() {
        let pool = get_db().unwrap();
        return load_equity_points_db(&pool, run_id).await;
    }

    let mut points: Vec<EquityPoint> = load_json_lines(&equity_log_path(run_id)).await?;
    points.sort_by_key(|p| p.timestamp);

    Ok(points)
}

pub async fn load_trade_events(run_id: &str) -> Result<Vec<TradeEvent>> {
    if using_db() {
        let pool = get_db().unwrap();
        return load_trade_events_db(&pool, run_id).await;
    }

    let mut events: Vec<TradeEvent> = load_json_lines(&trades_log_path(run_id)).await?;
    events.sort_by(|a, b| match a.timestamp.cmp(&b.timestamp) {
        Ordering::Equal => a.symbol.cmp(&b.symbol),
        other => other,
    });
    Ok(events)
}

pub async fn load_metrics(run_id: &str) -> Result<Metrics> {
    if using_db() {
        let pool = get_db().unwrap();
        return load_metrics_db(&pool, run_id).await;
    }

    let data = fs::read(metrics_path(run_id)).await?;
    let metrics = serde_json::from_slice(&data)?;
    Ok(metrics)
}

pub async fn load_run_ids() -> Result<Vec<String>> {
    if using_db() {
        let pool = get_db().unwrap();
        return load_run_ids_db(&pool).await;
    }

    let mut run_ids = Vec::new();
    let mut read_dir = match fs::read_dir(BACKTESTS_ROOT_DIR).await {
        Ok(rd) => rd,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(vec![]),
        Err(e) => return Err(e.into()),
    };

    while let Ok(Some(entry)) = read_dir.next_entry().await {
        if let Ok(ft) = entry.file_type().await {
            if ft.is_dir() {
                if let Ok(name) = entry.file_name().into_string() {
                    run_ids.push(name);
                }
            }
        }
    }
    run_ids.sort();
    Ok(run_ids)
}

// Generic JSONL loader
async fn load_json_lines<T: for<'a> Deserialize<'a>>(path: &Path) -> Result<Vec<T>> {
    let file = match File::open(path).await {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(vec![]),
        Err(e) => return Err(e.into()),
    };

    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    let mut result = Vec::new();

    while let Ok(Some(line)) = lines.next_line().await {
        if line.is_empty() {
            continue;
        }
        if let Ok(item) = serde_json::from_str(&line) {
            result.push(item);
        } else {
            return Err(anyhow!("malformed json line"));
        }
    }

    Ok(result)
}

pub async fn persist_metrics(run_id: &str, metrics: &Metrics) -> Result<()> {
    save_metrics(run_id, metrics).await
}

// --- Decision Record Logic ---

pub async fn load_decision_trace(run_id: &str, cycle: i32) -> Result<DecisionRecord> {
    if using_db() {
        let pool = get_db().unwrap();
        return load_decision_trace_db(&pool, run_id, cycle).await;
    }

    let dir = decision_log_dir(run_id);
    let candidates = scan_decision_files(&dir).await?;

    for cand in candidates {
        let data = match fs::read(&cand.path).await {
            Ok(d) => d,
            Err(_) => continue,
        };

        let record: DecisionRecord = match serde_json::from_slice(&data) {
            Ok(r) => r,
            Err(_) => continue,
        };

        if cycle <= 0 || record.cycle_number == cycle {
            return Ok(record);
        }
    }

    Err(anyhow!(
        "decision trace not found for run {} cycle {}",
        run_id,
        cycle
    ))
}

pub async fn load_decision_records(
    run_id: &str,
    limit: usize,
    offset: usize,
) -> Result<Vec<DecisionRecord>> {
    let limit = if limit == 0 { 20 } else { limit };

    if using_db() {
        let pool = get_db().unwrap();
        return load_decision_records_db(&pool, run_id, limit as i32, offset as i32).await;
    }

    let dir = decision_log_dir(run_id);
    // Note: Rust's read_dir doesn't return IsNotExist error automatically usually,
    // but helper might.
    let files = match scan_decision_files(&dir).await {
        Ok(f) => f,
        Err(e) => return Err(e),
    };

    if offset >= files.len() {
        return Ok(vec![]);
    }

    let end = (offset + limit).min(files.len());
    let mut records = Vec::with_capacity(end - offset);

    for file in &files[offset..end] {
        let data = match fs::read(&file.path).await {
            Ok(d) => d,
            Err(_) => continue,
        };
        if let Ok(record) = serde_json::from_slice::<DecisionRecord>(&data) {
            records.push(record);
        }
    }

    Ok(records)
}

struct DecisionFile {
    path: PathBuf,
    mod_time: SystemTime,
}

// Helper to scan and sort decision files
async fn scan_decision_files(dir: &Path) -> Result<Vec<DecisionFile>> {
    let mut read_dir = fs::read_dir(dir).await?;
    let mut files = Vec::new();

    while let Ok(Some(entry)) = read_dir.next_entry().await {
        let path = entry.path();
        if path.is_dir() {
            continue;
        }

        let name = entry.file_name().to_string_lossy().to_string();
        if !name.starts_with("decision_") || !name.ends_with(".json") {
            continue;
        }

        let meta = entry.metadata().await?;
        files.push(DecisionFile {
            path,
            mod_time: meta.modified()?,
        });
    }

    // Sort descending by mod_time
    files.sort_by(|a, b| b.mod_time.cmp(&a.mod_time));

    Ok(files)
}

pub async fn create_run_export(run_id: &str) -> Result<String> {
    if using_db() {
        let pool = get_db().unwrap();
        return create_run_export_db(&pool, run_id).await;
    }

    let root = run_dir(run_id);
    if !root.exists() {
        return Err(anyhow!("run dir not found"));
    }

    // Zip operations are synchronous (CPU bound + blocking IO).
    // Offload to a blocking thread to avoid blocking the async executor.
    let run_id_owned = run_id.to_string();
    let root_owned = root.clone();

    let zip_path = tokio::task::spawn_blocking(move || -> Result<String> {
        let tmp_dir = std::env::temp_dir();
        let tmp_file_name = format!("{}-{}.zip", run_id_owned, Uuid::new_v4());
        let tmp_path = tmp_dir.join(tmp_file_name);

        let file = std_fs::File::create(&tmp_path)?;
        let mut zip_writer = zip::ZipWriter::new(file);
        let options = FileOptions::<()>::default().compression_method(CompressionMethod::Deflated);

        for entry in WalkDir::new(&root_owned) {
            let entry = entry.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            let path = entry.path();

            if path.is_dir() {
                continue;
            }

            let rel_path = path
                .strip_prefix(&root_owned)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                .to_str()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid path"))?;

            zip_writer.start_file(rel_path, options)?;

            let mut f = std_fs::File::open(path)?;
            io::copy(&mut f, &mut zip_writer)?;
        }

        zip_writer.finish()?;

        Ok(tmp_path.to_string_lossy().into_owned())
    })
    .await??;

    Ok(zip_path)
}

pub async fn persist_decision_record(run_id: &str, record: Option<&DecisionRecord>) -> Result<()> {
    if !using_db() || record.is_none() {
        return Ok(());
    }

    let pool = get_db().unwrap();
    let _ = save_decision_record_db(&pool, run_id, record.unwrap()).await?;

    Ok(())
}
