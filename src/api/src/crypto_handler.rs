use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
};
use crypto::crypto::{CryptoService, EncryptedPayload};
use serde_json::json;
use tracing::error;

/// CryptoHandler Encryption API handler state
#[derive(Debug)]
pub struct CryptoHandler {
    pub crypto_service: Arc<CryptoService>,
}

impl CryptoHandler {
    /// Creates encryption handler
    pub fn new(crypto_service: Arc<CryptoService>) -> Self {
        Self { crypto_service }
    }

    /// HandleGetPublicKey Get server public key
    /// Usage: .route("/public-key", get(CryptoHandler::get_public_key))
    pub async fn get_public_key(State(state): State<Arc<Self>>) -> impl IntoResponse {
        let public_key = state.crypto_service.get_public_key_pem().unwrap();

        Json(json!({
            "public_key": public_key,
            "algorithm":  "RSA-OAEP-2048",
        }))
    }

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
