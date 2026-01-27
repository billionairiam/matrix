use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{error, info, instrument, warn};

const DEFAULT_MAINSTREAM_COINS: &[&str] = &[
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT", "HYPEUSDT",
];

#[derive(Clone, Debug)]
pub struct CoinPoolConfig {
    pub api_url: Arc<RwLock<String>>,
    pub timeout: Arc<StdDuration>,
    pub cache_dir: Arc<String>,
    pub use_default_coins: Arc<RwLock<bool>>,
}

impl Default for CoinPoolConfig {
    fn default() -> Self {
        Self {
            api_url: Arc::new(RwLock::new("".to_string())),
            timeout: StdDuration::from_secs(30).into(),
            cache_dir: "coin_pool_cache".to_string().into(),
            use_default_coins: RwLock::new(false).into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct OITopConfig {
    pub api_url: Arc<RwLock<String>>,
    pub timeout: Arc<StdDuration>,
    pub cache_dir: Arc<String>,
}

impl Default for OITopConfig {
    fn default() -> Self {
        Self {
            api_url: RwLock::new("".to_string()).into(),
            timeout: StdDuration::from_secs(30).into(),
            cache_dir: "coin_pool_cache".to_string().into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoinInfo {
    pub pair: String,
    pub score: f64,
    pub start_time: i64,
    pub start_price: f64,
    pub last_score: f64,
    pub max_score: f64,
    pub max_price: f64,
    pub increase_percent: f64,
    #[serde(skip)]
    pub is_available: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct CoinPoolCache {
    coins: Vec<CoinInfo>,
    fetched_at: DateTime<Utc>,
    source_type: String,
}

#[derive(Debug, Deserialize)]
struct CoinPoolApiResponse {
    success: bool,
    data: CoinPoolApiData,
}

#[derive(Debug, Deserialize)]
struct CoinPoolApiData {
    #[serde(default)]
    coins: Option<Vec<CoinInfo>>,
    #[serde(default)]
    _count: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OIPosition {
    pub symbol: String,
    pub rank: i32,
    pub current_oi: f64,
    pub oi_delta: f64,
    pub oi_delta_percent: f64,
    pub oi_delta_value: f64,
    pub price_delta_percent: f64,
    pub net_long: f64,
    pub net_short: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct OITopCache {
    positions: Vec<OIPosition>,
    fetched_at: DateTime<Utc>,
    source_type: String,
}

#[derive(Debug, Deserialize)]
struct OITopApiResponse {
    success: bool,
    data: OITopApiData,
}

#[derive(Debug, Deserialize)]
struct OITopApiData {
    positions: Vec<OIPosition>,
    #[allow(dead_code)]
    count: i32,
    #[allow(dead_code)]
    exchange: String,
    time_range: String,
}

#[derive(Debug)]
pub struct MergedCoinPool {
    pub ai500_coins: Vec<CoinInfo>,
    pub oi_top_coins: Vec<OIPosition>,
    pub all_symbols: Vec<String>,
    pub symbol_sources: HashMap<String, Vec<String>>,
}

#[derive(Clone)]
pub struct CoinPoolClient {
    pub config: Arc<CoinPoolConfig>,
    pub oi_config: Arc<OITopConfig>,
    http_client: Arc<reqwest::Client>,
    default_coins: Arc<RwLock<Vec<String>>>,
}

impl CoinPoolClient {
    pub fn new() -> Self {
        Self {
            config: CoinPoolConfig::default().into(),
            oi_config: OITopConfig::default().into(),
            http_client: reqwest::Client::new().into(),
            default_coins: Arc::new(RwLock::new(
                DEFAULT_MAINSTREAM_COINS
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            )),
        }
    }

    pub async fn set_coin_pool_api(&self, url: String) {
        let mut coin_pool_url_write = self.config.api_url.write().await;
        *coin_pool_url_write = url;
    }

    pub async fn set_oi_top_api(&self, url: String) {
        let mut oi_top_url_write = self.oi_config.api_url.write().await;
        *oi_top_url_write = url;
    }

    pub async fn set_use_default_coins(&mut self, use_default: bool) {
        let mut use_default_coins_write = self.config.use_default_coins.write().await;
        *use_default_coins_write = use_default;
    }

    #[instrument(skip(self))]
    pub async fn set_default_coins(&mut self, coins: Vec<String>) {
        if !coins.is_empty() {
            info!(
                "✓ Default coin pool set ({} coins): {:?}",
                coins.len(),
                coins
            );

            let mut default_coins_write = self.default_coins.write().await;
            *default_coins_write = coins;
        }
    }

    #[instrument(skip(self))]
    pub async fn get_coin_pool(&self) -> Result<Vec<CoinInfo>> {
        let (use_default, api_url) = {
            let use_default_lock = self.config.use_default_coins.read().await;
            let api_url_lock = self.config.api_url.read().await;
            (*use_default_lock, api_url_lock.clone())
        };

        if use_default {
            info!("✓ Default mainstream coin list enabled");
            let default_coins = self.default_coins.read().await;
            return Ok(self.convert_symbols_to_coins(&default_coins));
        }

        if api_url.is_empty() {
            warn!("⚠️  Coin pool API URL not configured, using default mainstream coin list");
            let default_coins = self.default_coins.read().await;
            return Ok(self.convert_symbols_to_coins(&default_coins));
        }

        let max_retries = 3;
        let mut last_err = None;

        for attempt in 1..=max_retries {
            if attempt > 1 {
                warn!(
                    "⚠️  Retry attempt {} of {} to fetch coin pool...",
                    attempt, max_retries
                );
                sleep(StdDuration::from_secs(2)).await;
            }

            match self.fetch_coin_pool().await {
                Ok(coins) => {
                    if attempt > 1 {
                        info!("✓ Retry attempt {} succeeded", attempt);
                    }
                    if let Err(e) = self.save_coin_pool_cache(&coins).await {
                        warn!("⚠️  Failed to save coin pool cache: {:?}", e);
                    }
                    return Ok(coins);
                }
                Err(e) => {
                    error!("❌ Request attempt {} failed: {:?}", attempt, e);
                    last_err = Some(e);
                }
            }
        }

        warn!("⚠️  All API requests failed, trying to use historical cache data...");
        if let Ok(cached_coins) = self.load_coin_pool_cache().await {
            info!(
                "✓ Using historical cache data ({} coins)",
                cached_coins.len()
            );
            return Ok(cached_coins);
        }

        warn!(
            "⚠️  Unable to load cache data (last error: {:?}), using default mainstream coin list",
            last_err
        );
        let default_coins = self.default_coins.read().await;
        Ok(self.convert_symbols_to_coins(&default_coins))
    }

    #[instrument(skip(self))]
    async fn fetch_coin_pool(&self) -> Result<Vec<CoinInfo>> {
        info!("🔄 Requesting AI500 coin pool...");

        let api_url = {
            let api_url_lock = self.config.api_url.read().await;
            api_url_lock.clone()
        };
        let resp = self
            .http_client
            .get(api_url)
            .timeout(*self.config.timeout)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "API returned error (status {}): {}",
                status,
                text
            ));
        }

        let body: CoinPoolApiResponse = resp.json().await.context("JSON parsing failed")?;

        if !body.success {
            return Err(anyhow::anyhow!("API returned failure status"));
        }

        let mut coins = body.data.coins.unwrap_or_default();

        if coins.is_empty() {
            return Err(anyhow::anyhow!("Coin list is empty"));
        }
        // Set IsAvailable flag (internal use)
        for coin in &mut coins {
            coin.is_available = true;
        }

        info!("✓ Successfully fetched {} coins", coins.len());
        Ok(coins)
    }

    #[instrument(skip(self))]
    async fn save_coin_pool_cache(&self, coins: &[CoinInfo]) -> Result<()> {
        fs::create_dir_all(self.config.cache_dir.as_ref()).await?;

        let cache = CoinPoolCache {
            coins: coins.to_vec(),
            fetched_at: Utc::now(),
            source_type: "api".to_string(),
        };

        let data = serde_json::to_string_pretty(&cache)?;
        let path = Path::new(self.config.cache_dir.as_ref()).join("latest.json");
        fs::write(path, data).await?;

        info!("💾 Coin pool cache saved ({} coins)", coins.len());
        Ok(())
    }

    #[instrument(skip(self))]
    async fn load_coin_pool_cache(&self) -> Result<Vec<CoinInfo>> {
        let path = Path::new(self.config.cache_dir.as_ref()).join("latest.json");
        if !path.exists() {
            return Err(anyhow::anyhow!("Cache file does not exist"));
        }

        let data = fs::read_to_string(path).await?;
        let cache: CoinPoolCache = serde_json::from_str(&data)?;

        let cache_age = Utc::now().signed_duration_since(cache.fetched_at);
        if cache_age > Duration::hours(24) {
            warn!(
                "⚠️  Cache data is old ({:.1} hours ago), but still usable",
                cache_age.num_hours()
            );
        } else {
            info!(
                "📂 Cache data timestamp: {} ({:.1} minutes ago)",
                cache.fetched_at.format("%Y-%m-%d %H:%M:%S"),
                cache_age.num_minutes()
            );
        }

        Ok(cache.coins)
    }

    pub async fn get_available_coins(&self) -> Result<Vec<String>> {
        let coins = self.get_coin_pool().await?;
        let mut symbols = Vec::new();

        for coin in coins {
            if coin.is_available {
                symbols.push(normalize_symbol(&coin.pair));
            }
        }

        if symbols.is_empty() {
            return Err(anyhow::anyhow!("No available coins"));
        }

        Ok(symbols)
    }

    pub async fn get_top_rated_coins(&self, limit: usize) -> Result<Vec<String>> {
        let coins = self.get_coin_pool().await?;
        let mut available_coins: Vec<CoinInfo> =
            coins.into_iter().filter(|c| c.is_available).collect();

        if available_coins.is_empty() {
            return Err(anyhow::anyhow!("No available coins"));
        }

        // Sort descending by score
        available_coins.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let max_count = std::cmp::min(limit, available_coins.len());
        let symbols: Vec<String> = available_coins
            .into_iter()
            .take(max_count)
            .map(|c| normalize_symbol(&c.pair))
            .collect();

        Ok(symbols)
    }

    // ==========================================
    // OI Top Logic
    // ==========================================
    #[instrument(skip(self))]
    pub async fn get_oi_top_positions(&self) -> Result<Vec<OIPosition>> {
        let api_url = {
            let api_url_lock = self.oi_config.api_url.read().await;
            api_url_lock.clone()
        };
        if api_url.trim().is_empty() {
            warn!("⚠️  OI Top API URL not configured, skipping OI Top data fetch");
            return Ok(Vec::new());
        }

        let max_retries = 3;
        let mut last_err = None;

        for attempt in 1..=max_retries {
            if attempt > 1 {
                warn!(
                    "⚠️  Retry attempt {} of {} to fetch OI Top data...",
                    attempt, max_retries
                );
                sleep(StdDuration::from_secs(2)).await;
            }

            match self.fetch_oi_top().await {
                Ok(positions) => {
                    if attempt > 1 {
                        info!("✓ Retry attempt {} succeeded", attempt);
                    }
                    if let Err(e) = self.save_oi_top_cache(&positions).await {
                        warn!("⚠️  Failed to save OI Top cache: {:?}", e);
                    }
                    return Ok(positions);
                }
                Err(e) => {
                    info!("❌ OI Top request attempt {} failed: {:?}", attempt, e);
                    last_err = Some(e);
                }
            }
        }

        warn!("⚠️  All OI Top API requests failed, trying to use historical cache data...");
        if let Ok(cached) = self.load_oi_top_cache().await {
            info!(
                "✓ Using historical OI Top cache data ({} coins)",
                cached.len()
            );
            return Ok(cached);
        }

        warn!(
            "⚠️  Unable to load OI Top cache data (last error: {:?}), skipping OI Top data",
            last_err
        );
        Ok(Vec::new())
    }

    async fn fetch_oi_top(&self) -> Result<Vec<OIPosition>> {
        info!("🔄 Requesting OI Top data...");
        let api_url = {
            let api_url_lock = self.oi_config.api_url.read().await;
            api_url_lock.clone()
        };

        let resp = self
            .http_client
            .get(api_url)
            .timeout(*self.oi_config.timeout)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "OI Top API returned error (status {}): {}",
                status,
                text
            ));
        }

        let body: OITopApiResponse = resp.json().await.context("OI Top JSON parsing failed")?;

        if !body.success {
            return Err(anyhow::anyhow!("OI Top API returned failure status"));
        }

        if body.data.positions.is_empty() {
            return Err(anyhow::anyhow!("OI Top position list is empty"));
        }

        info!(
            "✓ Successfully fetched {} OI Top coins (time range: {})",
            body.data.positions.len(),
            body.data.time_range
        );

        Ok(body.data.positions)
    }

    #[instrument(skip(self))]
    async fn save_oi_top_cache(&self, positions: &[OIPosition]) -> Result<()> {
        fs::create_dir_all(self.oi_config.cache_dir.as_ref()).await?;

        let cache = OITopCache {
            positions: positions.to_vec(),
            fetched_at: Utc::now(),
            source_type: "api".to_string(),
        };

        let data = serde_json::to_string_pretty(&cache)?;
        let path = Path::new(self.oi_config.cache_dir.as_ref()).join("oi_top_latest.json");
        fs::write(path, data).await?;

        info!("💾 OI Top cache saved ({} coins)", positions.len());
        Ok(())
    }

    #[instrument(skip(self))]
    async fn load_oi_top_cache(&self) -> Result<Vec<OIPosition>> {
        let path = Path::new(self.oi_config.cache_dir.as_ref()).join("oi_top_latest.json");
        if !path.exists() {
            return Err(anyhow::anyhow!("OI Top cache file does not exist"));
        }

        let data = fs::read_to_string(path).await?;
        let cache: OITopCache = serde_json::from_str(&data)?;

        let cache_age = Utc::now().signed_duration_since(cache.fetched_at);
        if cache_age > Duration::hours(24) {
            warn!(
                "⚠️  OI Top cache data is old ({:.1} hours ago), but still usable",
                cache_age.num_hours()
            );
        } else {
            info!(
                "📂 OI Top cache data timestamp: {} ({:.1} minutes ago)",
                cache.fetched_at.format("%Y-%m-%d %H:%M:%S"),
                cache_age.num_minutes()
            );
        }

        Ok(cache.positions)
    }

    pub async fn get_oi_top_symbols(&self) -> Result<Vec<String>> {
        let positions = self.get_oi_top_positions().await?;
        let symbols = positions
            .into_iter()
            .map(|pos| normalize_symbol(&pos.symbol))
            .collect();
        Ok(symbols)
    }

    // ==========================================
    // Merged Pool Logic
    // ==========================================
    #[instrument(skip(self))]
    pub async fn get_merged_coin_pool(&self, ai500_limit: usize) -> Result<MergedCoinPool> {
        // 1. Get AI500 data
        let ai500_top_symbols = match self.get_top_rated_coins(ai500_limit).await {
            Ok(syms) => syms,
            Err(e) => {
                warn!("⚠️  Failed to get AI500 data: {:?}", e);
                Vec::new()
            }
        };

        // 2. Get OI Top data
        let oi_top_symbols = match self.get_oi_top_symbols().await {
            Ok(syms) => syms,
            Err(e) => {
                warn!("⚠️  Failed to get OI Top data: {:?}", e);
                Vec::new()
            }
        };

        // 3. Merge and deduplicate
        let mut symbol_set = HashSet::new();
        let mut symbol_sources: HashMap<String, Vec<String>> = HashMap::new();

        // Add AI500 coins
        for symbol in &ai500_top_symbols {
            symbol_set.insert(symbol.clone());
            symbol_sources
                .entry(symbol.clone())
                .or_default()
                .push("ai500".to_string());
        }

        // Add OI Top coins
        for symbol in &oi_top_symbols {
            symbol_set.insert(symbol.clone());
            symbol_sources
                .entry(symbol.clone())
                .or_default()
                .push("oi_top".to_string());
        }

        let all_symbols: Vec<String> = symbol_set.into_iter().collect();

        // Get complete raw data for the struct return
        // Note: In a real app, we might want to optimize not to fetch again if we just fetched above,
        // but the caching mechanism helps here.
        let ai500_coins = self.get_coin_pool().await.unwrap_or_default();
        let oi_top_positions = self.get_oi_top_positions().await.unwrap_or_default();

        let merged = MergedCoinPool {
            ai500_coins,
            oi_top_coins: oi_top_positions,
            all_symbols,
            symbol_sources,
        };

        info!(
            "📊 Coin pool merge complete: AI500={}, OI_Top={}, Total(deduplicated)={}",
            ai500_top_symbols.len(),
            oi_top_symbols.len(),
            merged.all_symbols.len()
        );

        Ok(merged)
    }

    // Helper
    fn convert_symbols_to_coins(&self, symbols: &Vec<String>) -> Vec<CoinInfo> {
        symbols
            .iter()
            .map(|s| CoinInfo {
                pair: s.clone(),
                score: 0.0,
                start_time: 0,
                start_price: 0.0,
                last_score: 0.0,
                max_score: 0.0,
                max_price: 0.0,
                increase_percent: 0.0,
                is_available: true,
            })
            .collect()
    }
}

fn normalize_symbol(symbol: &str) -> String {
    // Remove spaces
    let mut s = symbol.replace(' ', "");

    // Convert to uppercase
    s = s.to_uppercase();

    // Ensure ends with USDT
    if !s.ends_with("USDT") {
        s.push_str("USDT");
    }

    s
}
