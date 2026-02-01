use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow};
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc,watch};
use tokio::time::sleep;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tracing::{error, info, instrument, warn};
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
    // Record currently active subscription streams for automatic recovery after reconnection.
    active_streams: Arc<Mutex<HashSet<String>>>, 
    // Configuration
    batch_size: usize,
    reconnect: bool,
    // Shutdown signal
    shutdown_tx: watch::Sender<bool>,
}

impl CombinedStreamsClient {
    pub fn new(batch_size: usize) -> Self {
        let (tx, _rx) = watch::channel(false);
        CombinedStreamsClient {
            writer: Arc::new(Mutex::new(None)),
            subscribers: Arc::new(Mutex::new(HashMap::new())),
            active_streams: Arc::new(Mutex::new(HashSet::new())),
            batch_size,
            reconnect: true,
            shutdown_tx: tx,
        }
    }

    /// Establishes the WebSocket connection to the Combined Streams endpoint
    pub async fn connect(&self) -> Result<()> {
        let url = Url::parse("wss://fstream.binance.com/stream")
            .map_err(|e| anyhow!("Invalid URL: {}", e))?;

        let (ws_stream, _) = connect_async(url).await
            .map_err(|e| anyhow!("Connection failed: {}", e))?;

        info!("Combined stream WebSocket connected successfully");
        let (write, read) = ws_stream.split();

        {
            let mut writer_guard = self.writer.lock().await;
            *writer_guard = Some(write);
        }

        // Automatically resume previous subscriptions after each successful connection.
        self.restore_subscriptions().await?;

        let client_clone = self.clone();
        tokio::spawn(async move {
            client_clone.read_messages(read).await;
        });

        Ok(())
    }

     async fn restore_subscriptions(&self) -> Result<()> {
        let streams: Vec<String>;
        {
            let active = self.active_streams.lock().await;
            streams = active.iter().cloned().collect();
        }

        if !streams.is_empty() {
            info!("Restoring {} active subscriptions...", streams.len());
            // Binance's bundled streaming subscriptions have frequency limits,
            // so we'll resubscribe in batches here.
            for chunk in streams.chunks(self.batch_size) {
                self.send_subscribe_payload(chunk.to_vec()).await
                    .map_err(|e| anyhow!(e))?;
                sleep(Duration::from_millis(100)).await;
            }
        }
        Ok(())
    }

    /// Subscribes to K-lines in batches
    pub async fn batch_subscribe_klines(&self, symbols: &[String], interval: &str) -> Result<()> {
        for batch in symbols.chunks(self.batch_size) {
            let streams: Vec<String> = batch
                .iter()
                .map(|s| format!("{}@kline_{}", s.to_lowercase(), interval))
                .collect();

            // store it in the active_streams list.
            {
                let mut active = self.active_streams.lock().await;
                for s in &streams { active.insert(s.clone()); }
            }

            // Send subscription request
            self.send_subscribe_payload(streams).await
                .map_err(|e| anyhow!(e))?;
            
            sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    }

    /// Sends the subscription message for a list of streams
    #[instrument(skip_all)]
    pub async fn send_subscribe_payload(&self, streams: Vec<String>) -> Result<(), String> {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let payload = json!({
            "method": "SUBSCRIBE",
            "params": streams,
            "id": timestamp as u64
        });

        let mut writer_guard = self.writer.lock().await;
        if let Some(writer) = writer_guard.as_mut() {
            writer.send(Message::Text(payload.to_string())).await
                .map_err(|e| format!("Write error: {}", e))
        } else {
            Err("Not connected".into())
        }
    }

    /// Main loop for reading messages from the WebSocket
    async fn read_messages(
        &self,
        mut read: futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    ) {
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        loop {
           tokio::select! {
                // Monitor shutdown signal
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {}
                    info!("receive shutdown signal");
                    return;
                }

                // Monitor websocket message
                res = read.next() => {
                    match res {
                    Some(Ok(message)) => {
                        if let Message::Text(text) = message {
                            self.handle_combined_message(&text).await;
                        } else if let Message::Close(_) = message {
                            warn!("WebSocket was closed by server");
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        error!("read error : {}", e);
                        break;
                    }
                    None => break,
                }
                }
           }
        }

        // Only attempt to reconnect if the connection is not actively closed.
    if !*shutdown_rx.borrow() {
        self.handle_reconnect();
    }
    }

    /// Parses the combined message wrapper and dispatches data to subscribers
    #[instrument(skip_all)]
    async fn handle_combined_message(&self, text: &str) {
        // Parse: {"stream":"...", "data": ...}
        if let Ok(msg) = serde_json::from_str::<CombinedStreamMessage>(text) {
            let subs = self.subscribers.lock().await;
            if let Some(tx) = subs.get(&msg.stream) {
                let _ = tx.try_send(msg.data);
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
        self.subscribers.lock().await.insert(stream, tx);
        rx
    }

    /// Handles reconnection logic using a fire-and-forget task
    #[instrument(skip_all)]
    fn handle_reconnect(&self) {
        let client = self.clone();
        tokio::spawn(async move {
            info!("Starting reconnection task...");
            loop {
                // Check if it has been closed before each reconnection.
                if *client.shutdown_tx.borrow() { return; }

                match client.connect().await {
                    Ok(_) => {
                        info!("Reconnected and re-subscribed successfully");
                        break;
                    }
                    Err(e) => {
                        error!("Reconnect failed: {}. Retrying in 5s...", e);
                        sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        });
    }

    /// Closes the connection and cleans up
    pub async fn close(&mut self) {
        self.reconnect = false;

        let _ = self.shutdown_tx.send(true);

        // Close writer connection
        let mut writer_guard = self.writer.lock().await;
        if let Some(mut writer) = writer_guard.take() {
            let _ = writer.close().await;
        }

        self.subscribers.lock().await.clear();
    }
}
