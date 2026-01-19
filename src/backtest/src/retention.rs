use chrono::{DateTime, Utc};
use tokio::fs;

use crate::persistence_db::get_db;
use crate::persistence_db::using_db;
use crate::registry::load_run_index;
use crate::registry::save_run_index;
use crate::storage::run_dir;
use crate::storage_db_impl::delete_run_db;
use crate::storage_db_impl::query_retention_candidates;
use crate::types::RunState;

use logger::info;

pub const MAX_COMPLETED_RUNS: usize = 100;

/// Enforces the retention policy for backtest runs.
pub async fn enforce_retention(max_runs: usize) {
    if max_runs == 0 {
        return;
    }

    if using_db() {
        enforce_retention_db(max_runs).await;
        return;
    }

    let mut idx = match load_run_index().await {
        Ok(idx) => idx,
        Err(_) => return, // Fail silently/log internally as per Go original
    };

    struct WrappedEntry {
        id: String,
        updated: DateTime<Utc>,
    }

    let mut candidates: Vec<WrappedEntry> = Vec::new();

    // Iterate over the map of runs in the index
    // Assuming idx.runs is HashMap<String, RunIndexEntry>
    for (run_id, entry) in &idx.runs {
        let is_final = matches!(
            entry.state,
            RunState::Completed | RunState::Stopped | RunState::Failed | RunState::Liquidated
        );

        if !is_final {
            continue;
        }

        // Parse ISO string to DateTime, default to Now if failed
        let ts = DateTime::parse_from_rfc3339(&entry.updated_at_iso)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());

        candidates.push(WrappedEntry {
            id: run_id.clone(),
            updated: ts,
        });
    }

    if candidates.len() <= max_runs {
        return;
    }

    candidates.sort_by(|a, b| a.updated.cmp(&b.updated));

    let to_remove_count = candidates.len() - max_runs;
    let to_remove_ids: Vec<String> = candidates
        .into_iter()
        .take(to_remove_count)
        .map(|w| w.id)
        .collect();

    for run_id in &to_remove_ids {
        let dir = run_dir(run_id);
        if let Err(e) = fs::remove_dir_all(&dir).await {
            info!("failed to prune run {}: {:?}", run_id, e);
            continue;
        }
        idx.runs.remove(run_id);
    }

    if let Err(e) = save_run_index(&mut idx).await {
        info!("failed to save index after pruning: {:?}", e);
    }
}

async fn enforce_retention_db(max_runs: usize) {
    let final_states = vec![
        RunState::Completed,
        RunState::Stopped,
        RunState::Failed,
        RunState::Liquidated,
    ];

    let query = "
        SELECT run_id FROM backtest_runs
        WHERE state IN (?, ?, ?, ?)
        ORDER BY datetime(updated_at) DESC
        LIMIT -1 OFFSET ?
    ";

    if using_db() {
        let pool = get_db().unwrap();
        let runs_to_delete =
            match query_retention_candidates(&pool, query, &final_states, max_runs as i32).await {
                Ok(ids) => ids,
                Err(_) => return,
            };

        for run_id in runs_to_delete {
            if let Err(e) = delete_run_db(&pool, &run_id).await {
                info!("failed to remove run {}: {:?}", run_id, e);
                continue;
            }

            let dir = run_dir(&run_id);
            // We attempt to remove the directory, logging if it fails
            if let Err(e) = fs::remove_dir_all(&dir).await {
                info!("failed to remove run dir {}: {:?}", run_id, e);
            }
        }
    }
}
