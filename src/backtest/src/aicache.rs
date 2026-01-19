use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use decision::engine::Context as EngineContext;
use decision::engine::FullDecision;
use decision::engine::{AccountInfo, CandidateCoin, PositionInfo};
use market::types::Data;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedDecision {
    pub key: String,
    pub prompt_variant: String,
    #[serde(rename = "ts")]
    pub timestamp: i64,
    pub decision: FullDecision,
}

// AICache persists AI decisions for repeated backtesting or replay.
#[derive(Clone, Default)]
pub struct AICache {
    path: Arc<PathBuf>,
    entries: Arc<RwLock<HashMap<String, CachedDecision>>>,
}

/// Helper struct for serialization to match the Go JSON structure exactly.
#[derive(Serialize, Deserialize)]
struct DiskCache {
    entries: HashMap<String, CachedDecision>,
}

impl AICache {
    /// LoadAICache loads the cache from disk.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if path.as_os_str().is_empty() {
            return Err(anyhow!("ai cache path is empty"));
        }

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Initialize empty map
        let mut entries = HashMap::new();

        // Try reading file
        match fs::read(&path) {
            Ok(data) => {
                if !data.is_empty() {
                    let disk_cache: DiskCache =
                        serde_json::from_slice(&data).context("failed to unmarshal cache")?;
                    entries = disk_cache.entries;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // It's okay if file doesn't exist yet
            }
            Err(e) => return Err(e.into()),
        }

        Ok(Self {
            path: path.into(),
            entries: RwLock::new(entries).into(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get retrieves a decision from the cache.
    /// Returns (Decision, found_bool).
    pub fn get(&self, key: &str) -> Option<(FullDecision)> {
        if key.is_empty() {
            return None;
        }

        let map = self.entries.read().unwrap();
        if let Some(entry) = map.get(key) {
            // Rust Clone is deep by default for these structs, equivalent to the JSON marshal/unmarshal cycle
            return Some(entry.decision.clone());
        }

        None
    }

    /// Put adds a decision to the cache and saves it to disk.
    pub fn put(&self, key: &str, variant: &str, ts: i64, decision: &FullDecision) -> Result<()> {
        if key.is_empty() {
            return Ok(());
        }

        let entry = CachedDecision {
            key: key.to_string(),
            prompt_variant: variant.to_string(),
            timestamp: ts,
            decision: decision.clone(),
        };

        {
            let mut map = self.entries.write().unwrap();
            map.insert(key.to_string(), entry);
        } // Lock released here

        self.save()
    }

    fn save(&self) -> Result<()> {
        let map = self.entries.read().unwrap();

        // Wrap for serialization to match Go's `Entries map...` JSON output
        let disk_data = DiskCache {
            entries: map.clone(),
        };

        let data = serde_json::to_vec_pretty(&disk_data)?;
        write_file_atomic(&self.path, &data)
    }
}

/// Simulates atomic file writing (write to temp, then rename).
fn write_file_atomic(path: &Path, data: &[u8]) -> Result<()> {
    let mut tmp_path = path.to_path_buf();
    // Append a generic extension or random string
    let tmp_ext = format!(
        "{}.tmp",
        path.extension().and_then(|e| e.to_str()).unwrap_or("")
    );
    tmp_path.set_extension(tmp_ext);

    fs::write(&tmp_path, data)?;
    fs::rename(tmp_path, path)?;
    Ok(())
}

/// Computes a deterministic cache key based on the context.
pub fn compute_cache_key(ctx: &EngineContext, variant: &str, ts: i64) -> Result<String> {
    // We define a struct that matches the Go anonymous struct layout exactly
    // to ensure the SHA256 hash is identical (assuming field sorting is handled consistently).
    #[derive(Serialize)]
    struct Payload<'a> {
        variant: &'a str,
        ts: i64,
        current_time: &'a str,
        account: &'a AccountInfo,
        positions: &'a [PositionInfo],
        candidate_coins: &'a [CandidateCoin],
        market: HashMap<String, Data>, // Using Map to match Go's map[string]
        margin_used_pct: f64,
        runtime_minutes: i32,
        call_count: i32,
    }

    let mut market_data_clean = HashMap::new();
    for (sym, data) in &ctx.market_data_map {
        market_data_clean.insert(sym.clone(), data.clone());
    }

    let payload = Payload {
        variant,
        ts,
        current_time: &ctx.current_time,
        account: &ctx.account,
        positions: &ctx.positions,
        candidate_coins: &ctx.candidate_coins,
        market: market_data_clean,
        margin_used_pct: ctx.account.margin_used_pct,
        runtime_minutes: ctx.runtime_minutes,
        call_count: ctx.call_count,
    };

    // Serialize to JSON bytes
    let bytes = serde_json::to_vec(&payload)?;

    // Hash
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let result = hasher.finalize();

    Ok(hex::encode(result))
}
