use std::{env, sync::Arc};

use anyhow::{Result, anyhow};
use api::crypto_handler::CryptoHandler;
use api::server::Server;
use auth::jwt_auth::set_jwt_secret;
use backtest::manager::Manager as BacktestManager;
use backtest::persistence_db::use_database;
use config_lib::config;
use crypto::crypto::CryptoService;
use dotenvy::dotenv;
use logger::logger;
use manager::trader_manager::TraderManager;
use market::monitor::WSMonitor;
use mcp::{Provider, deepseek_client::DeepseekProvider};
use store::CryptoProvider;
use store::store::Store;
use tracing::{error, info, warn};

pub struct AppCrypto {
    service: Arc<CryptoService>,
}

impl AppCrypto {
    pub fn new(service: Arc<CryptoService>) -> Self {
        Self { service }
    }
}

impl CryptoProvider for AppCrypto {
    fn encrypt(&self, plaintext: &str) -> String {
        if plaintext.is_empty() {
            return plaintext.to_string();
        }

        match self.service.encrypt_for_storage(plaintext, &[]) {
            Ok(encrypted) => encrypted,
            Err(e) => {
                warn!("⚠️ Encryption failed: {:?}", e);
                plaintext.to_string()
            }
        }
    }

    fn decrypt(&self, encrypted: &str) -> String {
        if encrypted.is_empty() {
            return encrypted.to_string();
        }

        if !self.service.is_encrypted_storage_value(encrypted) {
            return encrypted.to_string();
        }

        match self.service.decrypt_from_storage(encrypted, &[]) {
            Ok(decrypted) => decrypted,
            Err(e) => {
                warn!("⚠️ Decryption failed: {:?}", e);
                encrypted.to_string()
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    logger::init("MATRIX");

    info!("╔════════════════════════════════════════════════════════════╗");
    info!("║    🤖 AI Multi-Model Trading System - DeepSeek & Qwen      ║");
    info!("╚════════════════════════════════════════════════════════════╝");

    let config = config::get();
    info!("✅ Configuration loaded");

    let db_path = env::args().nth(1).unwrap_or_else(|| "data.db".to_string());
    info!("📋 Initializing database: {}", db_path);

    let st = Arc::new(
        Store::new(&db_path)
            .await
            .map_err(|e| anyhow!("❌ Failed to initialize database: {:?}", e))?,
    );

    use_database(st.pool.clone());
    info!("🔐 Initializing encryption service...");

    let crypto_service = Arc::new(
        CryptoService::new()
            .map_err(|e| anyhow!("❌ Failed to initialize encryption service: {:?}", e))?,
    );

    let app_crypto = AppCrypto::new(crypto_service.clone());
    st.set_provider(app_crypto).await;

    info!("✅ Crypto service initialized successfully");

    // Set JWT secret
    set_jwt_secret(&config.jwt_secret);
    info!("🔑 JWT secret configured");

    // Start WebSocket market monitor FIRST (before loading traders that may need market data)
    // This ensures WSMonitorCli is initialized before any trader tries to access it
    tokio::spawn(async move {
        WSMonitor::new(100).start(vec![]);
    });
    info!("📊 WebSocket market monitor started");

    // Give WebSocket monitor time to initialize
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Create TraderManager and BacktestManager
    let trader_manager = Arc::new(TraderManager::new());
    let mcp_client = new_shared_mcp_client();
    let backtest_manager = BacktestManager::new(mcp_client);
    backtest_manager.restore_runs().await?;

    // Start position sync manager (detects manual closures, TP/SL triggers)
    let position_sync_manager = TraderManager::new(); // 0 = use default 10s interval
    position_sync_manager.start_all().await;

    // Load traders from database
    trader_manager.load_traders_from_store(&st).await?;

    // Display loaded trader information
    let traders = st.trader().await.list("default").await?;
    info!("🤖 AI Trader Configurations in Database:");
    if traders.is_empty() {
        info!("  (No trader configurations, please create via Web interface)");
    } else {
        for trader in traders {
            let status = if trader.is_running {
                "✅ Running"
            } else {
                "❌ Stopped"
            };
            info!(
                "  • {} [{}] {} - AI Model: {}, Exchange: {}",
                trader.name, trader.id, status, trader.ai_model_id, trader.exchange_id
            );
        }
    }

    // Start API server
    let crypto_handler = Arc::new(CryptoHandler::new(crypto_service));
    let server = Server::new(
        trader_manager.clone(),
        st.clone(),
        crypto_handler.clone(),
        Some(backtest_manager),
        config.api_server_port,
    );
    tokio::spawn(async move {
        if let Err(e) = server.run().await {
            error!("❌ Failed to start API server: {:?}", e);
        }
    });

    // Wait for interrupt signal
    tokio::signal::ctrl_c().await.unwrap();
    info!("🛑 Received interrupt signal, shutting down...");

    // Stop all traders
    trader_manager.stop_all().await;
    info!("✅ All traders stopped");

    Ok(())
}

fn new_shared_mcp_client() -> Option<Arc<dyn Provider>> {
    let api_key = env::var("DEEPSEEK_API_KEY").ok();
    if let Some(_api_key) = api_key {
        return Some(Arc::new(DeepseekProvider));
    }
    warn!("❌ DEEPSEEK_API_KEY not set, AI features will be unavailable");
    None
}
