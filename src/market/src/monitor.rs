use std::collections::HashMap;
use std::sync::Arc;

use crate::api_client::APIClient;
use crate::types::Kline;
use anyhow::{Context, Result, anyhow};
use serde_json::Value;
use tokio::sync::{RwLock, Semaphore};
use tracing::{error, info, instrument, warn};

use super::combined_streams::CombinedStreamsClient;
use super::websocket_client::{KlineWSData, WSClient};

const SUB_KLINE_TIME: [&str; 2] = ["3m", "4h"];

#[derive(Debug, Clone)]
pub struct SymbolStats {
    pub last_active_time: i64,
    pub alert_count: i32,
    pub volume_spike_count: i32,
    pub last_alert_time: i64,
    pub score: f64,
}

#[derive(Debug)]
pub struct WSMonitor {
    ws_client: WSClient,
    combined_client: CombinedStreamsClient,

    symbols: RwLock<Vec<String>>,
    filter_symbols: RwLock<HashMap<String, bool>>,

    kline_data_3m: RwLock<HashMap<String, Vec<Kline>>>,
    kline_data_4h: RwLock<HashMap<String, Vec<Kline>>>,
}

impl WSMonitor {
    pub fn new(batch_size: usize) -> Arc<Self> {
        Arc::new(Self {
            ws_client: WSClient::new(),
            combined_client: CombinedStreamsClient::new(batch_size),
            symbols: RwLock::new(Vec::new()),
            filter_symbols: RwLock::new(HashMap::new()),
            kline_data_3m: RwLock::new(HashMap::new()),
            kline_data_4h: RwLock::new(HashMap::new()),
        })
    }

    #[instrument]
    pub async fn initialize(self: &Arc<Self>, coins: Vec<String>) -> Result<()> {
        info!("Initializing WebSocket monitor...");
        let api_client = APIClient::new();
        let mut symbols_to_monitor = Vec::new();

        if coins.is_empty() {
            let exchange_info = api_client.get_exchange_info().await?;
            let mut filters = self.filter_symbols.write().await;

            for symbol in exchange_info.symbols {
                // Filter logic: TRADING status, PERPETUAL contract, ends with USDT
                if symbol.status == "TRADING"
                    && symbol.contract_type == "PERPETUAL"
                    && symbol.symbol.ends_with("USDT")
                {
                    symbols_to_monitor.push(symbol.symbol.clone());
                    filters.insert(symbol.symbol, true);
                }
            }
        } else {
            symbols_to_monitor = coins;
        }

        info!("Found {} trading pairs", symbols_to_monitor.len());

        {
            let mut s = self.symbols.write().await;
            *s = symbols_to_monitor;
        }

        // Initialize historical data
        if let Err(e) = self.initialize_historical_data().await {
            error!("Failed to initialize historical data: {}", e);
        }

        Ok(())
    }

    #[instrument]
    async fn initialize_historical_data(self: &Arc<Self>) -> Result<()> {
        info!("Fetching historical data...");
        let symbols = self.symbols.read().await.clone();

        // Semaphore to limit concurrency (size 5, matching Go code)
        let semaphore = Arc::new(Semaphore::new(5));
        let mut handles = Vec::new();

        for symbol in symbols {
            let sem_clone = semaphore.clone();
            let monitor_clone = self.clone();

            let handle = tokio::spawn(async move {
                // Acquire permit
                let _permit = sem_clone.acquire().await.unwrap();
                let api_client = APIClient::new(); // Create new client per task or reuse if thread-safe

                // Fetch 3m
                match api_client.get_klines(&symbol, "3m", 100).await {
                    Ok(klines) if !klines.is_empty() => {
                        monitor_clone
                            .kline_data_3m
                            .write()
                            .await
                            .insert(symbol.clone(), klines);
                        info!("Loaded {} historical K-line data-3m", symbol);
                    }
                    Err(e) => error!("Failed to get {} 3m data: {}", symbol, e),
                    _ => {}
                }

                // Fetch 4h
                match api_client.get_klines(&symbol, "4h", 100).await {
                    Ok(klines) if !klines.is_empty() => {
                        monitor_clone
                            .kline_data_4h
                            .write()
                            .await
                            .insert(symbol.clone(), klines);
                        info!("Loaded {} historical K-line data-4h", symbol);
                    }
                    Err(e) => error!("Failed to get {} 4h data: {}", symbol, e),
                    _ => {}
                }
            });
            handles.push(handle);
        }

        // Wait for all tasks
        for h in handles {
            let _ = h.await;
        }

        Ok(())
    }

    #[instrument]
    pub async fn start(self: &Arc<Self>, coins: Vec<String>) {
        info!("Starting WebSocket real-time monitoring...");

        if let Err(e) = self.initialize(coins).await {
            error!("❌ Failed to initialize coins: {}", e);
            return;
        }

        if let Err(e) = self.combined_client.connect().await {
            error!("❌ Failed to connect combined client: {}", e);
            return;
        }

        if let Err(e) = self.subscribe_all().await {
            error!("❌ Failed to subscribe to pairs: {}", e);
            return;
        }
    }

    #[instrument]
    async fn subscribe_all(self: &Arc<Self>) -> Result<()> {
        info!("Starting to subscribe to all trading pairs...");
        let symbols = self.symbols.read().await.clone();

        for symbol in &symbols {
            for interval in SUB_KLINE_TIME.iter() {
                self.subscribe_symbol(symbol, interval).await;
            }
        }

        // Perform the batch subscription at the socket level
        for interval in SUB_KLINE_TIME.iter() {
            self.combined_client
                .batch_subscribe_klines(&symbols, interval)
                .await
                .context(format!("Failed to batch subscribe {}", interval))?;
        }

        info!("All trading pair subscriptions completed");
        Ok(())
    }

    // Returns the stream name subscribed to
    async fn subscribe_symbol(self: &Arc<Self>, symbol: &str, interval: &str) -> String {
        let stream = format!("{}@kline_{}", symbol.to_lowercase(), interval);

        // Add subscriber returns a channel receiver
        let mut rx = self
            .combined_client
            .add_subscriber(stream.clone(), 100)
            .await;

        let monitor_clone = self.clone();
        let sym_owned = symbol.to_string();
        let interval_owned = interval.to_string();

        // Spawn a listener for this specific stream
        tokio::spawn(async move {
            while let Some(data) = rx.recv().await {
                monitor_clone
                    .handle_kline_data(&sym_owned, &interval_owned, data)
                    .await;
            }
        });

        stream
    }

    async fn handle_kline_data(&self, symbol: &str, interval: &str, data: Value) {
        match serde_json::from_value::<KlineWSData>(data) {
            Ok(parsed) => {
                self.process_kline_update(symbol, &parsed, interval).await;
            }
            Err(e) => {
                error!("Failed to parse Kline data for {}: {}", symbol, e);
            }
        }
    }

    async fn process_kline_update(&self, symbol: &str, ws_kline: &KlineWSData, interval: &str) {
        // Convert WS data to internal Kline struct
        // Uses unwrap_or_default() or handles errors gracefully for parsing
        let kline = Kline {
            open_time: ws_kline.kline.start_time,
            close_time: ws_kline.kline.close_time,
            trades: ws_kline.kline.number_of_trades,
            open: ws_kline.kline.open_price.parse().unwrap_or_default(),
            high: ws_kline.kline.high_price.parse().unwrap_or_default(),
            low: ws_kline.kline.low_price.parse().unwrap_or_default(),
            close: ws_kline.kline.close_price.parse().unwrap_or_default(),
            volume: ws_kline.kline.volume.parse().unwrap_or_default(),
            quote_volume: ws_kline.kline.quote_volume.parse().unwrap_or_default(),
            taker_buy_base_volume: ws_kline
                .kline
                .taker_buy_base_volume
                .parse()
                .unwrap_or_default(),
            taker_buy_quote_volume: ws_kline
                .kline
                .taker_buy_quote_volume
                .parse()
                .unwrap_or_default(),
        };

        let map_lock = match interval {
            "3m" => &self.kline_data_3m,
            "4h" => &self.kline_data_4h,
            _ => return,
        };

        let mut map = map_lock.write().await;

        map.entry(symbol.to_string())
            .and_modify(|klines| {
                if let Some(last) = klines.last_mut() {
                    if last.open_time == kline.open_time {
                        // Update current candle
                        *last = kline.clone();
                    } else {
                        // New candle
                        klines.push(kline.clone());
                        if klines.len() > 100 {
                            klines.remove(0);
                        }
                    }
                } else {
                    klines.push(kline.clone());
                }
            })
            .or_insert_with(|| vec![kline]);
    }

    #[instrument]
    pub async fn get_current_klines(
        self: &Arc<Self>,
        symbol: &str,
        interval: &str,
    ) -> Result<Vec<Kline>> {
        // Select the correct map
        let map_lock = match interval {
            "3m" => &self.kline_data_3m,
            "4h" => &self.kline_data_4h,
            _ => return Err(anyhow!("Invalid interval provided")),
        };

        // Check cache first (Read Lock)
        {
            let map = map_lock.read().await;
            if let Some(klines) = map.get(symbol) {
                // Return deep copy
                return Ok(klines.clone());
            }
        } // Drop read lock

        // Cache miss: Fetch via API
        info!("Cache miss for {} {}, fetching via API", symbol, interval);
        let api_client = APIClient::new();
        let klines = api_client
            .get_klines(symbol, interval, 100)
            .await
            .context(format!("Failed to get {}-minute K-line", interval))?;

        // Write to cache (Write Lock)
        {
            let mut map = map_lock.write().await;
            // Store uppercase symbol as per original code logic
            map.insert(symbol.to_uppercase(), klines.clone());
        }

        // Dynamic Subscription
        let stream_str = self.subscribe_symbol(symbol, interval).await;
        if let Err(e) = self
            .combined_client
            .subscribe_streams(vec![stream_str.clone()])
            .await
        {
            warn!(
                "Warning: Failed to dynamically subscribe to stream {}: {} (using API data)",
                stream_str, e
            );
        } else {
            info!("Dynamic subscription to stream: {}", stream_str);
        }

        Ok(klines)
    }

    pub async fn close(&mut self) {
        self.ws_client.close().await;
        self.combined_client.close().await;
    }
}
