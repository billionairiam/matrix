use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc};
use tokio::time::sleep;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use url::Url;

// Type alias for the Write half of the WebSocket stream
type WSWriter = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;

#[derive(Debug, Deserialize)]
pub struct WSMessage {
    pub stream: String,
    pub data: Value,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct KlineWSData {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "k")]
    pub kline: KlineDetail,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct KlineDetail {
    #[serde(rename = "t")]
    pub start_time: i64,
    #[serde(rename = "T")]
    pub close_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "i")]
    pub interval: String,
    #[serde(rename = "f")]
    pub first_trade_id: i64,
    #[serde(rename = "L")]
    pub last_trade_id: i64,
    #[serde(rename = "o")]
    pub open_price: String,
    #[serde(rename = "c")]
    pub close_price: String,
    #[serde(rename = "h")]
    pub high_price: String,
    #[serde(rename = "l")]
    pub low_price: String,
    #[serde(rename = "v")]
    pub volume: String,
    #[serde(rename = "n")]
    pub number_of_trades: i64,
    #[serde(rename = "x")]
    pub is_final: bool,
    #[serde(rename = "q")]
    pub quote_volume: String,
    #[serde(rename = "V")]
    pub taker_buy_base_volume: String,
    #[serde(rename = "Q")]
    pub taker_buy_quote_volume: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TickerWSData {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "p")]
    pub price_change: String,
    #[serde(rename = "P")]
    pub price_change_percent: String,
    #[serde(rename = "w")]
    pub weighted_avg_price: String,
    #[serde(rename = "c")]
    pub last_price: String,
    #[serde(rename = "Q")]
    pub last_qty: String,
    #[serde(rename = "o")]
    pub open_price: String,
    #[serde(rename = "h")]
    pub high_price: String,
    #[serde(rename = "l")]
    pub low_price: String,
    #[serde(rename = "v")]
    pub volume: String,
    #[serde(rename = "q")]
    pub quote_volume: String,
    #[serde(rename = "O")]
    pub open_time: i64,
    #[serde(rename = "C")]
    pub close_time: i64,
    #[serde(rename = "F")]
    pub first_id: i64,
    #[serde(rename = "L")]
    pub last_id: i64,
    #[serde(rename = "n")]
    pub count: i64,
}

#[derive(Clone, Debug)]
pub struct WSClient {
    // Protected Writer to allow sending messages from any thread
    writer: Arc<Mutex<Option<WSWriter>>>,
    // Map of stream names to channel senders
    subscribers: Arc<Mutex<HashMap<String, mpsc::Sender<Value>>>>,
    reconnect: bool,
    // Notify to stop the connection logic
    shutdown_tx: mpsc::Sender<()>,
    // Receiver retained only to keep channel open until close()
    _shutdown_rx: Arc<Mutex<mpsc::Receiver<()>>>,
}

impl WSClient {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(1);
        WSClient {
            writer: Arc::new(Mutex::new(None)),
            subscribers: Arc::new(Mutex::new(HashMap::new())),
            reconnect: true,
            shutdown_tx: tx,
            _shutdown_rx: Arc::new(Mutex::new(rx)),
        }
    }

    // Connects to the WebSocket.
    // Note: This is an async fn.
    pub async fn connect(&self) -> Result<(), String> {
        let url = Url::parse("wss://ws-fapi.binance.com/ws-fapi/v1")
            .map_err(|e| format!("Invalid URL: {}", e))?;

        match connect_async(url).await {
            Ok((ws_stream, _)) => {
                info!("WebSocket connected successfully");
                let (write, read) = ws_stream.split();

                // Store the writer
                let mut writer_guard = self.writer.lock().await;
                *writer_guard = Some(write);
                drop(writer_guard);

                // Spawn the read loop
                let client_clone = self.clone();
                tokio::spawn(async move {
                    client_clone.read_messages(read).await;
                });

                Ok(())
            }
            Err(e) => Err(format!("WebSocket connection failed: {}", e)),
        }
    }

    pub async fn subscribe_kline(&self, symbol: &str, interval: &str) -> Result<(), String> {
        let stream = format!("{}@kline_{}", symbol, interval);
        self.subscribe(&stream).await
    }

    pub async fn subscribe_ticker(&self, symbol: &str) -> Result<(), String> {
        let stream = format!("{}@ticker", symbol);
        self.subscribe(&stream).await
    }

    pub async fn subscribe_mini_ticker(&self, symbol: &str) -> Result<(), String> {
        let stream = format!("{}@miniTicker", symbol);
        self.subscribe(&stream).await
    }

    async fn subscribe(&self, stream: &str) -> Result<(), String> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let payload = serde_json::json!({
            "method": "SUBSCRIBE",
            "params": [stream],
            "id": timestamp
        });

        let msg = Message::Text(payload.to_string());

        let mut writer_guard = self.writer.lock().await;
        if let Some(writer) = writer_guard.as_mut() {
            writer
                .send(msg)
                .await
                .map_err(|e| format!("Failed to send subscribe message: {}", e))?;
            info!("Subscribing to stream: {}", stream);
            Ok(())
        } else {
            Err("WebSocket not connected".to_string())
        }
    }

    async fn read_messages(
        &self,
        mut read: futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    ) {
        loop {
            // Check if shutdown was signalled
            if self.shutdown_tx.is_closed() {
                break;
            }

            match read.next().await {
                Some(Ok(message)) => {
                    if let Message::Text(text) = message {
                        self.handle_message(&text).await;
                    } else if let Message::Close(_) = message {
                        warn!("WebSocket closed by server");
                        break;
                    }
                }
                Some(Err(e)) => {
                    error!("Failed to read WebSocket message: {}", e);
                    break;
                }
                None => {
                    // Stream ended
                    break;
                }
            }
        }

        // Connection lost, trigger reconnect logic
        // NOTE: We call this synchronously to break the async type cycle
        self.handle_reconnect();
    }

    async fn handle_message(&self, text: &str) {
        // Parse the generic structure to get the stream name
        let parsed: Result<WSMessage, _> = serde_json::from_str(text);

        if let Ok(ws_msg) = parsed {
            let subscribers = self.subscribers.lock().await;
            if let Some(tx) = subscribers.get(&ws_msg.stream) {
                // Try to send the data payload to the subscriber
                match tx.try_send(ws_msg.data) {
                    Ok(_) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        warn!("Subscriber channel is full: {}", ws_msg.stream);
                    }
                    Err(e) => {
                        warn!("Failed to send to subscriber: {}", e);
                    }
                }
            }
        }
    }

    // Changed to a synchronous function that spawns the async task.
    // This breaks the recursive opaque type cycle:
    // connect -> read_messages -> handle_reconnect -> spawn(connect)
    fn handle_reconnect(&self) {
        if !self.reconnect || self.shutdown_tx.is_closed() {
            return;
        }

        info!("Attempting to reconnect...");

        // Reset writer to None immediately
        // (Blocking mutex lock here is fine as it's short, or we can do it in the spawn)
        // We'll do it in the spawn to keep this fn fast.

        let client_clone = self.clone();
        tokio::spawn(async move {
            // Clear the writer
            {
                let mut writer_guard = client_clone.writer.lock().await;
                *writer_guard = None;
            }

            sleep(Duration::from_secs(3)).await;

            loop {
                if client_clone.shutdown_tx.is_closed() {
                    break;
                }

                // Call the async connect
                match client_clone.connect().await {
                    Ok(_) => {
                        info!("Reconnected successfully");
                        // Resubscribe logic could go here if you track active subscriptions
                        break;
                    }
                    Err(e) => {
                        error!("Reconnection failed: {}. Retrying in 3s...", e);
                        sleep(Duration::from_secs(3)).await;
                    }
                }
            }
        });
    }

    pub async fn add_subscriber(
        &self,
        stream: String,
        buffer_size: usize,
    ) -> mpsc::Receiver<Value> {
        let (tx, rx) = mpsc::channel(buffer_size);
        let mut sub_guard = self.subscribers.lock().await;
        sub_guard.insert(stream, tx);
        rx
    }

    pub async fn remove_subscriber(&self, stream: &str) {
        let mut sub_guard = self.subscribers.lock().await;
        sub_guard.remove(stream);
    }

    pub async fn close(&mut self) {
        self.reconnect = false;

        // Signal shutdown
        // Dropping the only Sender will close the channel.
        // We drop our handle, and any cloned handles in spawned tasks should also handle the closed signal.
        // However, explicitly closing internal things helps.

        let mut writer_guard = self.writer.lock().await;
        if let Some(mut writer) = writer_guard.take() {
            let _ = writer.close().await;
        }

        let mut sub_guard = self.subscribers.lock().await;
        sub_guard.clear();
    }
}
