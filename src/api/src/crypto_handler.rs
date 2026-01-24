use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
};
use serde_json::json;
use std::sync::Arc;
use tracing::error;

use crypto::crypto::{CryptoService, EncryptedPayload};

/// CryptoHandler Encryption API handler state
#[derive(Debug)]
pub struct CryptoHandler {
    // In Rust, services are often wrapped in Arc, but here we will wrap
    // the whole Handler in Arc when passing to the Router.
    pub crypto_service: Arc<CryptoService>,
}

impl CryptoHandler {
    /// Creates encryption handler
    pub fn new(crypto_service: Arc<CryptoService>) -> Self {
        Self { crypto_service }
    }

    // ==================== Public Key Endpoint ====================

    /// HandleGetPublicKey Get server public key
    /// Usage: .route("/public-key", get(CryptoHandler::get_public_key))
    pub async fn get_public_key(State(state): State<Arc<Self>>) -> impl IntoResponse {
        let public_key = state.crypto_service.get_public_key_pem().unwrap();

        Json(json!({
            "public_key": public_key,
            "algorithm":  "RSA-OAEP-2048",
        }))
    }

    // ==================== Encrypted Data Decryption Endpoint ====================

    /// HandleDecryptSensitiveData Decrypt encrypted data sent from client
    pub async fn decrypt_sensitive_data(
        State(state): State<Arc<Self>>,
        // Axum automatically binds JSON to the struct
        Json(payload): Json<EncryptedPayload>,
    ) -> impl IntoResponse {
        // Decrypt
        match state.crypto_service.decrypt_sensitive_data(&payload) {
            Ok(decrypted) => Json(json!({
                "plaintext": decrypted,
            }))
            .into_response(),
            Err(err) => {
                error!("❌ Decryption failed: {:?}", err);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "Decryption failed" })),
                )
                    .into_response()
            }
        }
    }
}

/// Validate private key format
fn is_valid_private_key(key: &str) -> bool {
    // EVM private key: 64 hex characters (optional 0x prefix)
    if key.len() == 64 {
        return true;
    }
    if key.len() == 66 && key.starts_with("0x") {
        return true;
    }
    // TODO: Add validation for other chains
    false
}
