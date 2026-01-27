use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow};
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc};
use tokio::time::sleep;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tracing::{debug, error, info, instrument, warn};
use url::Url;

// Type alias for the Write half of the WebSocket stream
type WSWriter = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;

// Structure to parse the specific format of combined streams
// e.g. {"stream": "btcusdt@kline_1m", "data": { ... }}
#[derive(Debug, Deserialize)]
struct CombinedStreamMessage {
    stream: String,
    data: Value,
}

#[derive(Clone, Debug)]
pub struct CombinedStreamsClient {
    // Write handle protected by Mutex
    writer: Arc<Mutex<Option<WSWriter>>>,
    // Map of "stream_name" -> "channel_sender"
    subscribers: Arc<Mutex<HashMap<String, mpsc::Sender<Value>>>>,
    // Configuration
    batch_size: usize,
    reconnect: bool,
    // Shutdown signal
    shutdown_tx: mpsc::Sender<()>,
    // Keep receiver alive in the struct
    _shutdown_rx: Arc<Mutex<mpsc::Receiver<()>>>,
}

impl CombinedStreamsClient {
    pub fn new(batch_size: usize) -> Self {
        let (tx, rx) = mpsc::channel(1);
        CombinedStreamsClient {
            writer: Arc::new(Mutex::new(None)),
            subscribers: Arc::new(Mutex::new(HashMap::new())),
            batch_size,
            reconnect: true,
            shutdown_tx: tx,
            _shutdown_rx: Arc::new(Mutex::new(rx)),
        }
    }

    /// Establishes the WebSocket connection to the Combined Streams endpoint
    pub async fn connect(&self) -> Result<()> {
        let url = Url::parse("wss://fstream.binance.com/stream")
            .map_err(|e| anyhow!("Invalid URL: {}", e))?;

        match connect_async(url).await {
            Ok((ws_stream, _)) => {
                info!("Combined stream WebSocket connected successfully");
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
            Err(e) => Err(anyhow!(
                "Combined stream WebSocket connection failed: {}",
                e
            )),
        }
    }

    /// Subscribes to K-lines in batches
    pub async fn batch_subscribe_klines(
        &self,
        symbols: &Vec<String>,
        interval: &str,
    ) -> Result<()> {
        let batches: Vec<&[String]> = symbols.chunks(self.batch_size).collect();

        for (i, batch) in batches.iter().enumerate() {
            info!("Subscribing batch {}, count: {}", i + 1, batch.len());

            // Format streams: "symbol@kline_interval"
            let streams: Vec<String> = batch
                .iter()
                .map(|s| format!("{}@kline_{}", s.to_lowercase(), interval))
                .collect();

            if let Err(e) = self.subscribe_streams(streams).await {
                return Err(anyhow!("Batch {} subscription failed: {}", i + 1, e));
            }

            // Delay between batches to avoid rate limiting
            if i < batches.len() - 1 {
                sleep(Duration::from_millis(100)).await;
            }
        }

        Ok(())
    }

    /// Sends the subscription message for a list of streams
    #[instrument(skip(self, streams))]
    pub async fn subscribe_streams(&self, streams: Vec<String>) -> Result<(), String> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let payload = json!({
            "method": "SUBSCRIBE",
            "params": streams,
            "id": timestamp as u64 // JSON numbers are usually i64/u64
        });

        let mut writer_guard = self.writer.lock().await;
        if let Some(writer) = writer_guard.as_mut() {
            info!("Subscribing to streams: {:?}", streams);
            writer
                .send(Message::Text(payload.to_string()))
                .await
                .map_err(|e| format!("Failed to write subscription: {}", e))
        } else {
            Err("WebSocket not connected".to_string())
        }
    }

    /// Main loop for reading messages from the WebSocket
    async fn read_messages(
        &self,
        mut read: futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    ) {
        loop {
            if self.shutdown_tx.is_closed() {
                break;
            }

            match read.next().await {
                Some(Ok(message)) => {
                    if let Message::Text(text) = message {
                        self.handle_combined_message(&text).await;
                    } else if let Message::Close(_) = message {
                        warn!("Combined stream WebSocket closed by server");
                        break;
                    }
                }
                Some(Err(e)) => {
                    error!("Failed to read combined stream message: {}", e);
                    break;
                }
                None => break, // Stream ended
            }
        }

        self.handle_reconnect();
    }

    /// Parses the combined message wrapper and dispatches data to subscribers
    #[instrument(skip(self))]
    async fn handle_combined_message(&self, text: &str) {
        // Parse: {"stream":"...", "data": ...}
        match serde_json::from_str::<CombinedStreamMessage>(text) {
            Ok(msg) => {
                let subscribers = self.subscribers.lock().await;
                if let Some(tx) = subscribers.get(&msg.stream) {
                    match tx.try_send(msg.data) {
                        Ok(_) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            warn!("Subscriber channel is full: {}", msg.stream);
                        }
                        Err(e) => {
                            warn!("Failed to send to subscriber: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                // This might happen for response messages (e.g. {"result":null,"id":...})
                // which don't match CombinedStreamMessage structure. We can ignore or log debug.
                debug!(
                    "Ignored message (not a stream event): {} | Error: {}",
                    text, e
                );
            }
        }
    }

    /// Registers a new subscriber channel for a specific stream name
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

    /// Handles reconnection logic using a fire-and-forget task
    #[instrument(skip(self))]
    fn handle_reconnect(&self) {
        if !self.reconnect || self.shutdown_tx.is_closed() {
            return;
        }

        info!("Combined stream attempting to reconnect...");

        let client_clone = self.clone();
        tokio::spawn(async move {
            // Reset writer
            {
                let mut writer_guard = client_clone.writer.lock().await;
                *writer_guard = None;
            }

            sleep(Duration::from_secs(3)).await;

            loop {
                if client_clone.shutdown_tx.is_closed() {
                    break;
                }

                match client_clone.connect().await {
                    Ok(_) => {
                        info!("Combined stream reconnected successfully");
                        // NOTE: In a real app, you might want to re-subscribe to topics here.
                        break;
                    }
                    Err(e) => {
                        error!("Combined stream reconnection failed: {}. Retrying...", e);
                        sleep(Duration::from_secs(3)).await;
                    }
                }
            }
        });
    }

    /// Closes the connection and cleans up
    pub async fn close(&mut self) {
        self.reconnect = false;

        // Close writer connection
        let mut writer_guard = self.writer.lock().await;
        if let Some(mut writer) = writer_guard.take() {
            let _ = writer.close().await;
        }

        // Close all subscriber channels by dropping the map
        let mut sub_guard = self.subscribers.lock().await;
        sub_guard.clear();
    }
}
