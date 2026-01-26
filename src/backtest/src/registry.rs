use std::collections::HashMap;
use std::path::PathBuf;

use crate::config::BacktestConfig;
use crate::lock::write_json_atomic;
use crate::persistence_db::get_db;
use crate::persistence_db::using_db;
use crate::retention::MAX_COMPLETED_RUNS;
use crate::retention::enforce_retention;
use crate::storage::BACKTESTS_ROOT_DIR;
use crate::storage::load_config;
use crate::storage::run_dir;
use crate::storage_db_impl::delete_run_db;
use crate::storage_db_impl::list_index_entries_db;
use crate::types::RunMetadata;
use crate::types::RunState;
use anyhow::{Result, anyhow};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::fs;

const RUN_INDEX_FILE: &str = "index.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunIndexEntry {
    #[serde(rename = "run_id")]
    pub run_id: String,
    pub state: RunState,
    pub symbols: Vec<String>,
    #[serde(rename = "decision_tf")]
    pub decision_tf: String,
    #[serde(rename = "start_ts")]
    pub start_ts: i64,
    #[serde(rename = "end_ts")]
    pub end_ts: i64,
    #[serde(rename = "equity_last")]
    pub equity_last: f64,
    #[serde(rename = "max_dd_pct")]
    pub max_drawdown_pct: f64,
    #[serde(rename = "created_at")]
    pub created_at_iso: String,
    #[serde(rename = "updated_at")]
    pub updated_at_iso: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunIndex {
    #[serde(rename = "runs")]
    pub runs: HashMap<String, RunIndexEntry>,
    #[serde(rename = "updated_at")]
    pub updated_at: String,
}

fn run_index_path() -> PathBuf {
    PathBuf::new().join(BACKTESTS_ROOT_DIR).join(RUN_INDEX_FILE)
}

pub async fn load_run_index() -> Result<RunIndex> {
    if using_db() {
        let pool = get_db().unwrap();
        let entries = list_index_entries_db(&pool).await?;
        let mut idx = RunIndex {
            runs: HashMap::new(),
            updated_at: Utc::now().to_rfc3339(),
        };
        for entry in entries {
            idx.runs.insert(entry.run_id.clone(), entry);
        }
        return Ok(idx);
    }

    let path = run_index_path();
    match fs::read(&path).await {
        Ok(data) => {
            let idx: RunIndex = serde_json::from_slice(&data)?;
            Ok(idx)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(RunIndex {
            runs: HashMap::new(),
            updated_at: Utc::now().to_rfc3339(),
        }),
        Err(e) => Err(anyhow!("{}", e)),
    }
}

pub async fn save_run_index(idx: &mut RunIndex) -> Result<()> {
    if using_db() {
        return Ok(());
    }

    idx.updated_at = Utc::now().to_rfc3339();
    write_json_atomic(&run_index_path(), idx).await
}

pub async fn update_run_index(
    meta: &RunMetadata,
    cfg: Option<&BacktestConfig>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if using_db() {
        enforce_retention(MAX_COMPLETED_RUNS).await;
        return Ok(());
    }

    // Rust handles optional config loading logic
    let loaded_cfg; // extending lifetime scope
    let cfg_ref = match cfg {
        Some(c) => c,
        None => {
            loaded_cfg = load_config(&meta.run_id).await?;
            &loaded_cfg
        }
    };

    let mut idx = load_run_index().await?;

    let entry = RunIndexEntry {
        run_id: meta.run_id.clone(),
        state: meta.state.clone(),
        symbols: cfg_ref.symbols.clone(),
        decision_tf: meta.summary.decision_tf.clone(),
        start_ts: cfg_ref.start_ts,
        end_ts: cfg_ref.end_ts,
        equity_last: meta.summary.equity_last,
        max_drawdown_pct: meta.summary.max_drawdown_pct,
        created_at_iso: meta.created_at.to_rfc3339(),
        updated_at_iso: meta.updated_at.to_rfc3339(),
    };

    idx.runs.insert(meta.run_id.clone(), entry);

    save_run_index(&mut idx).await?;
    enforce_retention(MAX_COMPLETED_RUNS).await;

    Ok(())
}

pub async fn remove_from_run_index(run_id: &str) -> Result<()> {
    if using_db() {
        let pool = get_db().unwrap();
        delete_run_db(&pool, run_id).await?;
        // fs::remove_dir_all handles "os.RemoveAll"
        // Note: In Rust, remove_dir_all returns error if path doesn't exist,
        // Go's RemoveAll returns nil. We might want to suppress NotFound here.
        let dir = run_dir(run_id);
        if let Err(e) = fs::remove_dir_all(dir).await {
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(anyhow!("{}", e));
            }
        }
        return Ok(());
    }

    let mut idx = load_run_index().await?;
    if idx.runs.remove(run_id).is_some() {
        save_run_index(&mut idx).await?;
    }

    Ok(())
}

pub async fn list_index_entries() -> Result<Vec<RunIndexEntry>> {
    if using_db() {
        let pool = get_db().unwrap();
        return list_index_entries_db(&pool).await;
    }

    let idx = load_run_index().await?;
    let mut entries: Vec<RunIndexEntry> = idx.runs.into_values().collect();

    // Sort descending by UpdatedAtISO
    entries.sort_by(|a, b| b.updated_at_iso.cmp(&a.updated_at_iso));

    Ok(entries)
}
