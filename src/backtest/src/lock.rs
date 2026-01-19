use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::process;
use std::time::Duration;

use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::storage::ensure_run_dir;

const LOCK_FILE_NAME: &str = "lock";
pub const LOCK_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);
const LOCK_STALE_AFTER: Duration = Duration::from_secs(10);

/// RunLockInfo represents the lock file structure for a backtest run.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct RunLockInfo {
    pub run_id: String,
    pub pid: u32,
    pub host: String,
    pub started_at: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,
}

/// Returns the directory path for a specific run ID.
fn run_dir(run_id: &str) -> PathBuf {
    Path::new("data").join("runs").join(run_id)
}

/// Writes JSON to a file atomically (write to temp, then rename).
pub async fn write_json_atomic<T: Serialize>(path: &Path, data: &T) -> Result<()> {
    // 1. Create a temporary file in the same directory
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Invalid file path"))?;

    // Create a temp file like "lock.tmp"
    let mut temp_path = PathBuf::from(dir);
    temp_path.push(format!("{}.tmp", file_name.to_string_lossy()));

    let file = File::create(&temp_path)?;

    // 2. Write JSON
    serde_json::to_writer(&file, data)?;

    // 3. Sync to disk to ensure data is written
    file.sync_all()?;

    // 4. Atomic rename (mv temp lock)
    fs::rename(temp_path, path)?;

    Ok(())
}

fn lock_file_path(run_id: &str) -> PathBuf {
    run_dir(run_id).join(LOCK_FILE_NAME)
}

pub fn load_run_lock(run_id: &str) -> Result<RunLockInfo> {
    let path = lock_file_path(run_id);

    // Read file
    let data = fs::read_to_string(path)?;

    // Parse JSON
    let info: RunLockInfo = serde_json::from_str(&data)?;

    Ok(info)
}

async fn save_run_lock(info: &RunLockInfo) -> Result<()> {
    let path = lock_file_path(&info.run_id);
    write_json_atomic(&path, info).await
}

pub fn delete_run_lock(run_id: &str) -> Result<()> {
    let path = lock_file_path(run_id);
    match fs::remove_file(path) {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(anyhow!("{}", e)),
    }
}

pub fn lock_is_stale(info: &RunLockInfo) -> bool {
    let now = Utc::now();
    now.signed_duration_since(info.last_heartbeat)
        .to_std()
        .map(|duration| duration > LOCK_STALE_AFTER)
        .unwrap_or(true)
}

/// Attempts to acquire a lock for the given run ID.
pub async fn acquire_run_lock(run_id: &str) -> Result<RunLockInfo> {
    ensure_run_dir(run_id).await?;

    // Check existing lock
    if let Ok(existing) = load_run_lock(run_id) {
        if !lock_is_stale(&existing) {
            return Err(anyhow!("run {} is locked by pid {}", run_id, existing.pid));
        }
    }

    // Get Hostname
    let host = hostname::get()
        .map(|h| h.to_string_lossy().into_owned())
        .unwrap_or_else(|_| "unknown".to_string());

    let now = Utc::now();
    let info = RunLockInfo {
        run_id: run_id.to_string(),
        pid: process::id(),
        host,
        started_at: now,
        last_heartbeat: now,
    };

    save_run_lock(&info).await?;

    Ok(info)
}

pub async fn update_run_lock_heartbeat(info: &mut RunLockInfo) -> Result<()> {
    info.last_heartbeat = Utc::now();
    save_run_lock(info).await
}
