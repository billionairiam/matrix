use std::env;

use anyhow::{Result, anyhow};
use backtest::persistence_db::use_database;
use config_lib::config;
use crypto::crypto::CryptoService;
use dotenvy::dotenv;
use logger::logger;
use store::CryptoProvider;
use store::store::Store;
use tracing::{info, warn};

pub struct AppCrypto {
    service: CryptoService,
}

impl AppCrypto {
    pub fn new(service: CryptoService) -> Self {
        Self { service }
    }
}

impl CryptoProvider for AppCrypto {
    fn encrypt(&self, plaintext: &str) -> String {
        if plaintext.is_empty() {
            return plaintext.to_string();
        }

        // Go: encrypted, err := cryptoService.EncryptForStorage(plaintext)
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
                // Go: logger.Warnf(...) return encrypted
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

    let st = Store::new(&db_path)
        .await
        .map_err(|e| anyhow!("❌ Failed to initialize database: {:?}", e))?;

    use_database(st.pool.clone());
    info!("🔐 Initializing encryption service...");

    let crypto_service = CryptoService::new()
        .map_err(|e| anyhow!("❌ Failed to initialize encryption service: {:?}", e))?;

    let app_crypto = AppCrypto::new(crypto_service);
    st.set_provider(app_crypto).await;

    info!("✅ Crypto service initialized successfully");
    Ok(())
}
