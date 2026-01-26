use std::collections::HashMap;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::backtest::{
    handle_backtest_label, handle_backtest_pause, handle_backtest_resume, handle_backtest_start,
    handle_backtest_stop,handle_backtest_delete,handle_backtest_status,handle_backtest_runs,
    handle_backtest_equity,handle_backtest_trades,handle_backtest_metrics,handle_backtest_trace,
    handle_backtest_decisions,handle_backtest_export
};
use crate::crypto_handler::CryptoHandler;
use crate::strategy::{
    handle_activate_strategy, handle_create_strategy, handle_delete_strategy,
    handle_duplicate_strategy, handle_get_active_strategy, handle_get_default_strategy_config,
    handle_get_strategies, handle_get_strategy, handle_preview_prompt, handle_strategy_test_run,
    handle_update_strategy,
};
use anyhow::Result;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Extension, Path, Query, Request, State},
    http::{HeaderMap, Method, StatusCode},
    middleware,
    middleware::Next,
    response::IntoResponse,
    routing::{any, get, post, put},
};
use backtest::manager::Manager;
use chrono::Utc;
use crypto::crypto::EncryptedPayload;
use futures::future::select_ok;
use if_addrs::get_if_addrs;
use tracing::{error, info, warn, instrument, debug};
use manager::trader_manager::TraderManager;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use store::exchange::Exchange;
use store::store::Store;
use store::user::User;
use tower_http::cors::{Any, CorsLayer};
use trader::Trader;
use uuid::Uuid;
use validator::Validate;

#[derive(Deserialize, Debug)]
pub struct UpdateExchangeConfigRequest {
    pub exchanges: HashMap<String, ExchangeUpdateData>,
}

// 3. The Specific Exchange Data
#[derive(Deserialize, Debug)]
pub struct ExchangeUpdateData {
    pub enabled: bool,

    // Handle potential casing differences
    #[serde(alias = "apiKey", alias = "api_key")]
    pub api_key: String,

    #[serde(alias = "secretKey", alias = "secret_key")]
    pub secret_key: String,

    pub passphrase: Option<String>,
    pub testnet: bool,

    // Specific Exchange Fields
    #[serde(alias = "hyperliquidWalletAddr", alias = "hyperliquid_wallet_addr")]
    pub hyperliquid_wallet_addr: Option<String>,

    #[serde(alias = "asterUser", alias = "aster_user")]
    pub aster_user: Option<String>,

    #[serde(alias = "asterSigner", alias = "aster_signer")]
    pub aster_signer: Option<String>,

    #[serde(alias = "asterPrivateKey", alias = "aster_private_key")]
    pub aster_private_key: Option<String>,

    #[serde(alias = "lighterWalletAddr", alias = "lighter_wallet_addr")]
    pub lighter_wallet_addr: Option<String>,

    #[serde(alias = "lighterPrivateKey", alias = "lighter_private_key")]
    pub lighter_private_key: Option<String>,

    #[serde(
        alias = "lighterAPIKeyPrivateKey",
        alias = "lighter_api_key_private_key"
    )]
    pub lighter_api_key_private_key: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct ModelUpdateData {
    pub enabled: bool,
    #[serde(alias = "api_key", alias = "apiKey")]
    pub api_key: String,
    #[serde(alias = "custom_api_url", alias = "customAPIURL")]
    pub custom_api_url: String,
    #[serde(alias = "custom_model_name", alias = "customModelName")]
    pub custom_model_name: String,
}

#[derive(Deserialize, Debug)]
pub struct UpdateModelConfigRequest {
    #[serde(rename = "models")]
    pub models: HashMap<String, ModelUpdateData>,
}

#[derive(Serialize)]
pub struct SafeModelConfig {
    pub id: String,
    pub name: String,
    pub provider: String,
    pub enabled: bool,
    pub custom_api_url: String,
    pub custom_model_name: String,
}

#[derive(Deserialize, Debug)]
pub struct ClosePositionRequest {
    pub symbol: String,
    pub side: String, // Expecting "LONG" or "SHORT"
}

#[derive(Deserialize, Debug)]
pub struct UpdateTraderPromptRequest {
    pub custom_prompt: String,
    pub override_base_prompt: bool,
}

#[derive(Deserialize, Validate, Debug)]
pub struct CreateTraderRequest {
    #[validate(length(min = 1, message = "Name is required"))]
    pub name: String,

    #[serde(rename = "ai_model")] // Map JSON "ai_model" to field "ai_model_id"
    #[validate(length(min = 1, message = "AI Model is required"))]
    pub ai_model_id: String,

    #[validate(length(min = 1, message = "Exchange ID is required"))]
    pub exchange_id: String,

    pub strategy_id: Option<String>,
    pub initial_balance: f64,

    pub btc_eth_leverage: Option<i32>,
    pub altcoin_leverage: Option<i32>,
    pub trading_symbols: Option<String>,

    pub use_coin_pool: bool,
    pub use_oi_top: bool,
    pub custom_prompt: Option<String>,
    pub override_base_prompt: bool,
    pub system_prompt_template: Option<String>,
    pub is_cross_margin: Option<bool>,
    pub scan_interval_minutes: i32,
}

// UpdateTraderRequest
// Using Option<T> allows us to distinguish between "provided" and "missing" fields
#[derive(Deserialize, Validate, Debug)]
pub struct UpdateTraderRequest {
    #[validate(length(min = 1, message = "Name is required"))]
    pub name: String,

    #[serde(rename = "ai_model_id")]
    #[validate(length(min = 1, message = "AI Model ID is required"))]
    pub ai_model_id: String,

    #[serde(rename = "exchange_id")]
    #[validate(length(min = 1, message = "Exchange ID is required"))]
    pub exchange_id: String,

    #[serde(rename = "strategy_id")]
    pub strategy_id: Option<String>,

    pub initial_balance: Option<f64>,

    pub scan_interval_minutes: Option<i32>,

    pub is_cross_margin: Option<bool>,

    // Fields kept for backward compatibility
    pub btc_eth_leverage: Option<i32>,
    pub altcoin_leverage: Option<i32>,
    pub trading_symbols: Option<String>,
    pub custom_prompt: Option<String>,
    pub override_base_prompt: Option<bool>,
    pub system_prompt_template: Option<String>,
}

#[derive(Clone, Debug)]
pub struct AuthUser {
    pub user_id: String,
    pub email: String,
}

#[derive(Deserialize)]
struct HistoryBatchBody {
    trader_ids: Vec<String>,
}

#[derive(Deserialize)]
struct HistoryBatchQuery {
    trader_ids: Option<String>,
}

#[derive(Deserialize, Validate)]
struct ClientRequest {
    #[validate(email(message = "Invalid email format"))]
    email: String,

    #[validate(length(min = 6, message = "Password must be at least 6 characters"))]
    password: String,
}

#[derive(Deserialize, Validate)]
pub struct VerifyOtpRequest {
    #[validate(length(min = 1, message = "User ID cannot be empty"))]
    pub user_id: String,

    #[validate(length(min = 1, message = "OTP code cannot be empty"))]
    pub otp_code: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SafeExchangeConfig {
    pub id: String,

    pub name: String,

    #[serde(rename = "type")]
    pub exchange_type: String, // "cex" or "dex"

    pub enabled: bool,

    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub testnet: bool,

    pub hyperliquid_wallet_addr: String,

    pub aster_user: String,

    pub aster_signer: String,
}

#[derive(Clone)]
pub struct AppState {
    pub trader_manager: Arc<TraderManager>,
    pub store: Arc<Store>,
    pub crypto_handler: Arc<CryptoHandler>,
    pub backtest_manager: Option<Arc<Manager>>,
}

pub struct Server {
    port: u16,
    state: AppState,
}

impl Server {
    pub fn new(
        trader_manager: Arc<TraderManager>,
        store: Arc<Store>,
        crypto_handler: Arc<CryptoHandler>,
        backtest_manager: Option<Arc<Manager>>,
        port: u16,
    ) -> Self {
        let state = AppState {
            trader_manager,
            store,
            crypto_handler,
            backtest_manager,
        };

        Self { port, state }
    }

    #[instrument(skip(self))]
    pub async fn run(self) -> Result<()> {
        let cors = CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(vec![
                Method::GET,
                Method::POST,
                Method::PUT,
                Method::DELETE,
                Method::OPTIONS,
            ])
            .allow_headers(Any);

        let app = self.setup_routes().layer(cors);

        let addr = SocketAddr::from(([0, 0, 0, 0], self.port));

        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app).await?;
        info!("Server listening on {}", addr);

        Ok(())
    }

    fn setup_routes(&self) -> Router {
        let public_routes = Router::new()
            // Health check
            .route("/health", any(handle_health))
            // System supported models and exchanges
            .route("/supported-models", get(handle_get_supported_models))
            .route("/supported-exchanges", get(handle_get_supported_exchanges))
            // System config
            .route("/config", get(handle_get_system_config))
            // Crypto related
            .route("/crypto/public-key", get(handle_crypto_public_key))
            .route("/crypto/decrypt", post(handle_crypto_decrypt))
            // System prompt template management
            .route("/prompt-templates", get(handle_get_prompt_templates))
            .route("/prompt-templates/{name}", get(handle_get_prompt_template))
            // Public competition data
            .route("/traders", get(handle_public_trader_list))
            .route("/competition", get(handle_public_competition))
            .route("/top-traders", get(handle_top_traders))
            .route("/equity-history", get(handle_equity_history))
            .route("/equity-history-batch", post(handle_equity_history_batch))
            .route(
                "/traders/{id}/public-config",
                get(handle_get_public_trader_config),
            )
            // Authentication related
            .route("/register", post(handle_register))
            .route("/login", post(handle_login))
            .route("/verify-otp", post(handle_verify_otp))
            .route("/complete-registration", post(handle_complete_registration))
            // Backtest routes
            .route("/backtest/start", post(handle_backtest_start))
            .route("/backtest/pause", post(handle_backtest_pause))
            .route("/backtest/resume", post(handle_backtest_resume))
            .route("/backtest/stop", post(handle_backtest_stop))
            .route("/backtest/label", post(handle_backtest_label))
            .route("/backtest/delete", post(handle_backtest_delete))
            .route("/backtest/status", get(handle_backtest_status))
            .route("/backtest/runs", get(handle_backtest_runs))
            .route("/backtest/equity", get(handle_backtest_equity))
            .route("/backtest/trades", get(handle_backtest_trades))
            .route("/backtest/metrics", get(handle_backtest_metrics))
            .route("/backtest/trace", get(handle_backtest_trace))
            .route("/backtest/decisions", get(handle_backtest_decisions))
            .route("/backtest/export", get(handle_backtest_export));

        // Protected routes
        let protected_routes = Router::new()
            .route("/logout", post(handle_logout))
            .route("/server-ip", get(handle_get_server_ip))
            // AI trader management
            .route("/my-traders", get(handle_trader_list))
            .route("/traders", post(handle_create_trader))
            .route("/traders/{id}/config", get(handle_get_trader_config))
            .route(
                "/traders/{id}",
                put(handle_update_trader).delete(handle_delete_trader),
            )
            .route("/traders/{id}/start", post(handle_start_trader))
            .route("/traders/{id}/stop", post(handle_stop_trader))
            .route("/traders/{id}/prompt", put(handle_update_trader_prompt))
            .route("/traders/{id}/sync-balance", post(handle_sync_balance))
            .route("/traders/{id}/close-position", post(handle_close_position))
            // AI model configuration
            .route(
                "/models",
                get(handle_get_model_configs).put(handle_update_model_configs),
            )
            // Exchange configuration
            .route(
                "/exchanges",
                get(handle_get_exchange_configs).put(handle_update_exchange_configs),
            )
            // Strategy management
            .route(
                "/strategies",
                get(handle_get_strategies).post(handle_create_strategy),
            )
            .route("/strategies/active", get(handle_get_active_strategy))
            .route(
                "/strategies/default-config",
                get(handle_get_default_strategy_config),
            )
            .route("/strategies/templates", get(handle_get_prompt_templates)) // 重复路由，复用 handler
            .route("/strategies/preview-prompt", post(handle_preview_prompt))
            .route("/strategies/test-run", post(handle_strategy_test_run))
            .route(
                "/strategies/{id}",
                get(handle_get_strategy)
                    .put(handle_update_strategy)
                    .delete(handle_delete_strategy),
            )
            .route("/strategies/{id}/activate", post(handle_activate_strategy))
            .route("/strategies/{id}/duplicate", post(handle_duplicate_strategy))
            // Data for specified trader
            .route("/status", get(handle_status))
            .route("/account", get(handle_account))
            .route("/positions", get(handle_positions))
            .route("/decisions", get(handle_decisions))
            .route("/decisions/latest", get(handle_latest_decisions))
            .route("/statistics", get(handle_statistics))
            .layer(middleware::from_fn(auth_middleware));

        Router::new()
            .nest("/api", public_routes.merge(protected_routes))
            .with_state(self.state.clone())
    }
}

async fn auth_middleware(
    mut req: Request,
    next: Next,
) -> Result<impl IntoResponse, (StatusCode, Json<Value>)> {
    let auth_header = req.headers().get("AUTHORIZATION").ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "Missing Authorization header" })),
        )
    })?;

    let auth_str = auth_header.to_str().map_err(|_| {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "Invalid Authorization header encoding" })),
        )
    })?;

    let token = match auth_str.strip_prefix("Bearer ") {
        Some(t) => t.trim(),
        None => {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": "Invalid Authorization format" })),
            ));
        }
    };

    if token.is_empty() {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "Invalid Authorization format" })),
        ));
    }

    if auth::jwt_auth::is_token_black_listed(token).unwrap() {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "Token expired, please login again" })),
        ));
    }

    let claims = auth::jwt_auth::validate_jwt(token).map_err(|e| {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": format!("Invalid token: {}", e) })),
        )
    })?;

    req.extensions_mut().insert(AuthUser {
        user_id: claims.user_id,
        email: claims.email,
    });

    Ok(next.run(req).await)
}

async fn handle_health(Extension(time): Extension<String>) -> impl IntoResponse {
    Json(json!({
        "status": "ok",
        "time":   time
    }))
}

async fn handle_get_supported_models() -> impl IntoResponse {
    let supported_models = json!([
        {
            "id": "deepseek",
            "name": "DeepSeek",
            "provider": "deepseek"
        },
        {
            "id": "qwen",
            "name": "Qwen",
            "provider": "qwen"
        }
    ]);

    Json(supported_models)
}

async fn handle_get_supported_exchanges(State(_state): State<AppState>) -> impl IntoResponse {
    let supported_exchanges = vec![
        SafeExchangeConfig {
            id: "binance".to_string(),
            name: "Binance Futures".to_string(),
            exchange_type: "binance".to_string(),
            ..Default::default()
        },
        SafeExchangeConfig {
            id: "bybit".to_string(),
            name: "Bybit Futures".to_string(),
            exchange_type: "bybit".to_string(),
            ..Default::default()
        },
        SafeExchangeConfig {
            id: "okx".to_string(),
            name: "OKX Futures".to_string(),
            exchange_type: "okx".to_string(),
            ..Default::default()
        },
        SafeExchangeConfig {
            id: "hyperliquid".to_string(),
            name: "Hyperliquid".to_string(),
            exchange_type: "hyperliquid".to_string(),
            ..Default::default()
        },
        SafeExchangeConfig {
            id: "aster".to_string(),
            name: "Aster DEX".to_string(),
            exchange_type: "aster".to_string(),
            ..Default::default()
        },
        SafeExchangeConfig {
            id: "lighter".to_string(),
            name: "LIGHTER DEX".to_string(),
            exchange_type: "lighter".to_string(),
            ..Default::default()
        },
    ];

    Json(supported_exchanges)
}

async fn handle_get_system_config() -> impl IntoResponse {
    let cfg = config::config::get();

    Json(json!({
        "registration_enabled": cfg.registration_enabled,
        "btc_eth_leverage": 10,
        "altcoin_leverage": 5,
    }))
}

// Crypto wrappers
async fn handle_crypto_public_key(State(state): State<AppState>) -> impl IntoResponse {
    let public_key = match state.crypto_handler.crypto_service.get_public_key_pem() {
        Ok(key) => key,
        Err(_) => String::new(),
    };

    Json(json!({
        "public_key": public_key,
        "algorithm": "RSA-OAEP-2048",
    }))
}

#[instrument(skip(state, payload))]
async fn handle_crypto_decrypt(
    State(state): State<AppState>,
    Json(payload): Json<crypto::crypto::EncryptedPayload>,
) -> impl IntoResponse {
    match state
        .crypto_handler
        .crypto_service
        .decrypt_sensitive_data(&payload)
    {
        Ok(decrypted) => Json(json!({
            "plaintext": decrypted
        }))
        .into_response(),
        Err(e) => {
            info!("❌ Decryption failed: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Decryption failed" })),
            )
                .into_response()
        }
    }
}

// Templates
async fn handle_get_prompt_templates() -> impl IntoResponse {
    let templates = decision::prompt_manager::get_all_prompt_templates();
    let templates_vec: Vec<_> = templates
        .iter()
        .map(|tmpl| {
            json!({
                "name": tmpl.name
            })
        })
        .collect();

    Json(json!({
        "templates": templates_vec
    }))
}

async fn handle_get_prompt_template(Path(name): Path<String>) -> impl IntoResponse {
    match decision::prompt_manager::get_prompt_template(&name) {
        Ok(temp) => Json(json!({
            "name":    temp.name,
            "content": temp.content,
        }))
        .into_response(),
        Err(_e) => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": format!("Template does not exist: {}", name)
            })),
        )
            .into_response(),
    }
}

// Public Trader Data
async fn handle_public_trader_list(State(state): State<AppState>) -> impl IntoResponse {
    let competition = match state.trader_manager.get_competition_data().await {
        Ok(compe) => compe,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": format!("Failed to get trader list: {:?}", e)
                })),
            )
                .into_response();
        }
    };

    let traders_value = match competition.get("traders") {
        Some(v) => v,
        None => {
            return Json(json!([])).into_response();
        }
    };
    let traders_arr = match traders_value.as_array() {
        Some(arr) => arr,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "Trader data format error"
                })),
            )
                .into_response();
        }
    };

    let result: Vec<Value> = traders_arr
        .iter()
        .map(|trader| {
            json!({
                "trader_id":       trader.get("trader_id"),
                "trader_name":     trader.get("trader_name"),
                "ai_model":        trader.get("ai_model"),
                "exchange":        trader.get("exchange"),
                "is_running":      trader.get("is_running"),
                "total_equity":    trader.get("total_equity"),
                "total_pnl":       trader.get("total_pnl"),
                "total_pnl_pct":   trader.get("total_pnl_pct"),
                "position_count":  trader.get("position_count"),
                "margin_used_pct": trader.get("margin_used_pct"),
            })
        })
        .collect();

    Json(result).into_response()
}

async fn handle_public_competition(State(state): State<AppState>) -> impl IntoResponse {
    let competition = match state.trader_manager.get_competition_data().await {
        Ok(compe) => compe,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": format!("Failed to get trader list: {:?}", e)
                })),
            )
                .into_response();
        }
    };

    Json(competition).into_response()
}

async fn handle_top_traders(State(state): State<AppState>) -> impl IntoResponse {
    let top_traders = match state.trader_manager.get_top_traders_data().await {
        Ok(data) => data,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": format!("Failed to get top 10 trader data: {:?}", e)
                })),
            )
                .into_response();
        }
    };

    Json(top_traders).into_response()
}

// get_trader_from_query
// Helper function to resolve trader_id from query or fallback to default
#[instrument(skip(state))]
pub async fn get_trader_from_query(
    state: &AppState,
    user_id: &str,
    query_trader_id: Option<String>,
) -> Result<String, (StatusCode, String)> {
    if let Err(e) = state
        .trader_manager
        .load_user_traders_from_store(&state.store, user_id)
        .await
    {
        warn!("⚠️ Failed to load traders for user {}: {:?}", user_id, e);
    }

    if let Some(id) = query_trader_id {
        if !id.trim().is_empty() {
            return Ok(id);
        }
    }

    let memory_ids = state.trader_manager.get_trader_ids().await;
    if memory_ids.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "No available traders".to_string()));
    }

    match state.store.trader().await.list(user_id).await {
        Ok(user_traders) if !user_traders.is_empty() => {
            return Ok(user_traders[0].id.clone());
        }
        Ok(_) => {}
        Err(e) => {
            error!("Failed to list user traders: {:?}", e);
        }
    }

    Ok(memory_ids[0].clone())
}

async fn handle_equity_history(
    State(state): State<AppState>,
    Query(query_params): Query<HistoryBatchQuery>,
    body: Option<Json<HistoryBatchBody>>,
) -> impl IntoResponse {
    let mut target_trader_ids: Vec<String> = match body {
        Some(Json(payload)) => payload.trader_ids,
        None => Vec::new(),
    };

    if target_trader_ids.is_empty() {
        if let Some(ids_str) = query_params.trader_ids {
            if !ids_str.trim().is_empty() {
                target_trader_ids = ids_str
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
            }
        }
    }

    if target_trader_ids.is_empty() {
        let top_traders = match state.trader_manager.get_top_traders_data().await {
            Ok(data) => data,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({
                        "error": format!("Failed to get top 5 traders: {:?}", e)
                    })),
                )
                    .into_response();
            }
        };

        if let Some(traders) = top_traders.get("traders").and_then(|t| t.as_array()) {
            target_trader_ids = traders
                .iter()
                .filter_map(|trader| {
                    trader
                        .get("trader_id")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                })
                .collect();
        } else {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Trader data format error" })),
            )
                .into_response();
        }
    }

    if target_trader_ids.len() > 20 {
        target_trader_ids.truncate(20);
    }

    let result = get_equity_history_for_traders(&target_trader_ids, &state).await;

    Json(result).into_response()
}

async fn get_equity_history_for_traders(trader_ids: &Vec<String>, state: &AppState) -> Value {
    let mut histories = Map::new();
    let mut errors = Map::new();

    for trader_id in trader_ids {
        if trader_id.is_empty() {
            continue;
        }

        match state.store.equity().get_latest(trader_id, 500).await {
            Ok(snapshots) => {
                if snapshots.is_empty() {
                    histories.insert(trader_id.clone(), json!([]));
                    continue;
                }

                let history_list: Vec<Value> = snapshots
                    .iter()
                    .map(|snap| {
                        json!({
                            "timestamp":    snap.timestamp,
                            "total_equity": snap.total_equity,
                            "total_pnl":    snap.unrealized_pnl,
                            "balance":      snap.balance,
                        })
                    })
                    .collect();

                histories.insert(trader_id.clone(), Value::Array(history_list));
            }
            Err(e) => {
                errors.insert(
                    trader_id.clone(),
                    json!(format!("Failed to get historical data: {:?}", e)),
                );
            }
        }
    }

    let mut result = Map::new();
    result.insert("histories".to_string(), Value::Object(histories));

    let count = result["histories"].as_object().unwrap().len();
    result.insert("count".to_string(), json!(count));

    if !errors.is_empty() {
        result.insert("errors".to_string(), Value::Object(errors));
    }

    Value::Object(result)
}

async fn handle_equity_history_batch() -> impl IntoResponse {
    Json("Equity History Batch")
}

async fn handle_get_public_trader_config(
    State(state): State<AppState>,
    Path(trader_id): Path<String>,
) -> impl IntoResponse {
    if trader_id.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Trader ID cannot be empty" })),
        )
            .into_response();
    }

    let trader = match state.trader_manager.get_trader(&trader_id).await {
        Ok(t) => t,
        Err(_) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Trader does not exist" })),
            )
                .into_response();
        }
    };

    let status = trader.get_status().await.unwrap();
    let result = json!({
        "trader_id":   trader.get_id(),
        "trader_name": trader.get_name(),
        "ai_model":    trader.get_ai_model(),
        "exchange":    trader.get_exchange(),

        "is_running":  status.get("is_running"),
        "ai_provider": status.get("ai_provider"),
        "start_time":  status.get("start_time"),
    });

    Json(result).into_response()
}

// Auth Handlers
async fn handle_register(
    State(state): State<AppState>,
    Json(payload): Json<ClientRequest>,
) -> impl IntoResponse {
    if !config::config::get().registration_enabled {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "Registration is disabled" })),
        )
            .into_response();
    }

    if let Err(e) = payload.validate() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": format!("Validation failed: {}", e) })),
        )
            .into_response();
    }

    if let Ok(_) = state.store.user().get_by_email(&payload.email).await {
        return (
            StatusCode::CONFLICT,
            Json(json!({ "error": format!("Email already registered") })),
        )
            .into_response();
    };

    let password_hash = match auth::jwt_auth::hash_password(&payload.password) {
        Ok(hash) => hash,
        Err(_error) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "{error}": "Password processing failed" })),
            )
                .into_response();
        }
    };

    let otp_secret = match auth::jwt_auth::generate_otp_secret() {
        Ok(secret) => secret,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "OTP secret generation failed" })),
            )
                .into_response();
        }
    };

    let user_id = Uuid::new_v4().to_string();
    let new_user = User {
        id: user_id.clone(),
        email: payload.email.clone(),
        password_hash: password_hash,
        otp_secret: otp_secret.clone(),
        otp_verified: false,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    if let Err(e) = state.store.user().create(&new_user).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("Failed to create user: {:?}", e) })),
        )
            .into_response();
    }

    let qr_code_url = auth::jwt_auth::get_otp_qrcode_url(&otp_secret, &payload.email).unwrap();

    (
        StatusCode::OK,
        Json(json!({
            "user_id":     user_id,
            "email":       payload.email,
            "otp_secret":  otp_secret,
            "qr_code_url": qr_code_url,
            "message":     "Please scan the QR code with Google Authenticator and verify OTP",
        })),
    )
        .into_response()
}

async fn handle_login(
    State(state): State<AppState>,
    Json(payload): Json<ClientRequest>,
) -> impl IntoResponse {
    if let Err(e) = payload.validate() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": format!("Validation failed: {}", e) })),
        )
            .into_response();
    };

    let user = match state.store.user().get_by_email(&payload.email).await {
        Ok(u) => u,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("Database error: {:?}", e) })),
            )
                .into_response();
        }
    };

    if !auth::jwt_auth::check_password(&payload.password, &user.password_hash).unwrap() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "Email or password incorrect" })),
        )
            .into_response();
    }

    if !user.otp_verified {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "error":              "Account has not completed OTP setup",
                "user_id":            user.id,
                "requires_otp_setup": true,
            })),
        )
            .into_response();
    }

    (
        StatusCode::OK,
        Json(json!({
            "user_id":      user.id,
            "email":        user.email,
            "message":      "Please enter Google Authenticator code",
            "requires_otp": true,
        })),
    )
        .into_response()
}

async fn handle_verify_otp(
    State(state): State<AppState>,
    Json(payload): Json<VerifyOtpRequest>,
) -> impl IntoResponse {
    if let Err(e) = payload.validate() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": format!("Validation failed: {}", e) })),
        )
            .into_response();
    }

    let user = match state.store.user().get_by_id(&payload.user_id).await {
        Ok(u) => u,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("Database error: {:?}", e) })),
            )
                .into_response();
        }
    };

    if !auth::jwt_auth::verify_otp(&user.otp_secret, &payload.otp_code).unwrap() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Verification code error" })),
        )
            .into_response();
    }

    let token = match auth::jwt_auth::generate_jwt(&user.id, &user.email) {
        Ok(t) => t,
        Err(_e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to generate token" })),
            )
                .into_response();
        }
    };

    (
        StatusCode::OK,
        Json(json!({
            "token":   token,
            "user_id": user.id,
            "email":   user.email,
            "message": "Login successful",
        })),
    )
        .into_response()
}

async fn handle_complete_registration(
    State(state): State<AppState>,
    Json(payload): Json<VerifyOtpRequest>,
) -> impl IntoResponse {
    if let Err(e) = payload.validate() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": format!("Validation failed: {}", e) })),
        )
            .into_response();
    }

    let user = match state.store.user().get_by_id(&payload.user_id).await {
        Ok(u) => u,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("Database error: {:?}", e) })),
            )
                .into_response();
        }
    };

    if !auth::jwt_auth::verify_otp(&user.otp_secret, &payload.otp_code).unwrap() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "OTP code error" })),
        )
            .into_response();
    }

    if let Err(_) = state
        .store
        .user()
        .update_otp_verified(&payload.user_id, true)
        .await
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Failed to update user status" })),
        )
            .into_response();
    }

    let token = match auth::jwt_auth::generate_jwt(&user.id, &user.email) {
        Ok(t) => t,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to generate token" })),
            )
                .into_response();
        }
    };

    (
        StatusCode::OK,
        Json(json!({
            "token":   token,
            "user_id": user.id,
            "email":   user.email,
            "message": "Registration completed",
        })),
    )
        .into_response()
}

pub async fn handle_logout(headers: HeaderMap) -> impl IntoResponse {
    let auth_header = match headers.get("Authorization") {
        Some(h) => h,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": "Missing Authorization header" })),
            )
                .into_response();
        }
    };

    let auth_str = match auth_header.to_str() {
        Ok(s) => s,
        Err(_) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": "Invalid Authorization header encoding" })),
            )
                .into_response();
        }
    };

    let token = match auth_str.strip_prefix("Bearer ") {
        Some(t) => t.trim(),
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": "Invalid Authorization format" })),
            )
                .into_response();
        }
    };

    if token.is_empty() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "Invalid Authorization format" })),
        )
            .into_response();
    }

    let claims = match auth::jwt_auth::validate_jwt(token) {
        Ok(c) => c,
        Err(_) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": "Invalid token" })),
            )
                .into_response();
        }
    };

    let exp_timestamp = claims.exp;

    auth::jwt_auth::blacklist_token(token.to_string(), exp_timestamp as i64);

    (StatusCode::OK, Json(json!({ "message": "Logged out" }))).into_response()
}

async fn handle_get_server_ip() -> impl IntoResponse {
    let mut public_ip = get_public_ip_fast().await;
    if public_ip.is_empty() {
        public_ip = get_public_ip_from_interface();
    }

    if public_ip.is_empty() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Unable to get public IP address" })),
        )
            .into_response();
    }

    Json(json!({
        "public_ip": public_ip,
        "message": "Please add this IP address to the whitelist",
    }))
    .into_response()
}

async fn get_public_ip_fast() -> String {
    let services = vec![
        "https://api.ipify.org?format=text",
        "https://icanhazip.com",
        "https://ifconfig.me",
    ];

    let client = Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap_or_default();

    let futures: Vec<_> = services
        .into_iter()
        .map(|url| {
            let c = client.clone();
            Box::pin(async move {
                let resp = c.get(url).send().await.map_err(|_| ())?;
                let text = resp.text().await.map_err(|_| ())?;
                let ip = text.trim().to_string();
                if ip.parse::<IpAddr>().is_ok() {
                    Ok(ip)
                } else {
                    Err(())
                }
            })
        })
        .collect();

    match select_ok(futures).await {
        Ok((ip, _remaining_futures)) => ip,
        Err(_) => String::new(),
    }
}

// get_public_ip_from_interface Get first public IP from network interface
pub fn get_public_ip_from_interface() -> String {
    let ifaces = match get_if_addrs() {
        Ok(interfaces) => interfaces,
        Err(e) => {
            tracing::error!("Failed to get network interfaces: {}", e);
            return String::new();
        }
    };

    for iface in ifaces {
        if iface.is_loopback() {
            continue;
        }

        let ip = match iface.addr {
            if_addrs::IfAddr::V4(v4) => v4.ip,
            if_addrs::IfAddr::V6(_) => continue,
        };

        if ip.is_loopback() {
            continue;
        }

        if !is_private_ip(&ip) {
            return ip.to_string();
        }
    }

    String::new()
}

// is_private_ip Determine if it's a private IP address
pub fn is_private_ip(ip: &Ipv4Addr) -> bool {
    let octets = ip.octets();
    match octets {
        [10, ..] => true,
        [172, b, ..] if (16..=31).contains(&b) => true,
        [192, 168, ..] => true,
        _ => false,
    }
}

// Trader Management
async fn handle_trader_list(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
) -> impl IntoResponse {
    let db_traders = match state.store.trader().await.list(&user.user_id).await {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("Failed to get trader list: {:?}", e) })),
            )
                .into_response();
        }
    };

    let mut result = Vec::with_capacity(db_traders.len());
    for trader in db_traders {
        let mut is_running = trader.is_running;
        if let Ok(active_trader) = state.trader_manager.get_trader(&trader.id).await {
            let status = active_trader.get_status().await.unwrap();

            if let Some(running) = status.get("is_running").and_then(|v| v.as_bool()) {
                is_running = running;
            }
        }

        result.push(json!({
            "trader_id":       trader.id,
            "trader_name":     trader.name,
            "ai_model":        trader.ai_model_id, // Return complete AIModelID
            "exchange_id":     trader.exchange_id,
            "is_running":      is_running,
            "initial_balance": trader.initial_balance,
        }));
    }

    Json(result).into_response()
}

// handle_get_trader_config Get trader detailed configuration
pub async fn handle_get_trader_config(
    State(state): State<AppState>,
    // Extract user_id from the middleware context
    Extension(user): Extension<AuthUser>,
    // Extract trader_id from the URL path parameter
    Path(trader_id): Path<String>,
) -> impl IntoResponse {
    if trader_id.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Trader ID cannot be empty" })),
        )
            .into_response();
    }

    let full_cfg = match state
        .store
        .trader()
        .await
        .get_full_config(&user.user_id, &trader_id)
        .await
    {
        Ok(cfg) => cfg,
        Err(e) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": format!("Failed to get trader config: {:?}", e) })),
            )
                .into_response();
        }
    };

    let trader_config = full_cfg.trader;

    // Determine Real-time Running Status
    // Default to database status
    let mut is_running = trader_config.is_running;

    // Check memory manager for override
    if let Ok(active_trader) = state.trader_manager.get_trader(&trader_id).await {
        // Get status map/json
        let status = active_trader.get_status().await.unwrap();

        if let Some(running) = status.get("is_running").and_then(|v| v.as_bool()) {
            is_running = running;
        }
    }

    // 4. Construct Response
    let result = json!({
        "trader_id":             trader_config.id,
        "trader_name":           trader_config.name,
        "ai_model":              trader_config.ai_model_id, // Complete ID consistent with Go
        "exchange_id":           trader_config.exchange_id,
        "initial_balance":       trader_config.initial_balance,
        "scan_interval_minutes": trader_config.scan_interval_minutes,
        "btc_eth_leverage":      trader_config.btc_eth_leverage,
        "altcoin_leverage":      trader_config.altcoin_leverage,
        "trading_symbols":       trader_config.trading_symbols,
        "custom_prompt":         trader_config.custom_prompt,
        "override_base_prompt":  trader_config.override_base_prompt,
        "is_cross_margin":       trader_config.is_cross_margin,
        "use_coin_pool":         trader_config.use_coin_pool,
        "use_oi_top":            trader_config.use_oi_top,
        "is_running":            is_running, // The calculated real-time status
    });

    Json(result).into_response()
}

#[instrument(skip(state))]
async fn handle_create_trader(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Json(req): Json<CreateTraderRequest>,
) -> impl IntoResponse {
    if let Err(e) = req.validate() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": format!("Validation failed: {}", e) })),
        )
            .into_response();
    }

    let btc_eth_leverage = req.btc_eth_leverage.unwrap_or(10);
    if !(0..=50).contains(&btc_eth_leverage) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "BTC/ETH leverage must be between 1-50x" })),
        )
            .into_response();
    }

    let altcoin_leverage = req.altcoin_leverage.unwrap_or(5);
    if !(0..=20).contains(&altcoin_leverage) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Altcoin leverage must be between 1-20x" })),
        )
            .into_response();
    }

    if let Some(ref symbols_str) = req.trading_symbols {
        for s in symbols_str.split(',') {
            let s = s.trim();
            if !s.is_empty() && !s.to_uppercase().ends_with("USDT") {
                return (
                    StatusCode::BAD_REQUEST, 
                    Json(json!({ "error": format!("Invalid symbol format: {}, must end with USDT", s) }))
                ).into_response();
            }
        }
    }

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let trader_id = format!("{}_{}_{}", req.exchange_id, req.ai_model_id, timestamp);

    let is_cross_margin = req.is_cross_margin.unwrap_or(true);
    let system_prompt_template = req
        .system_prompt_template
        .clone()
        .unwrap_or_else(|| "default".to_string());
    let scan_interval_minutes = if req.scan_interval_minutes < 3 {
        3
    } else {
        req.scan_interval_minutes
    };

    let mut actual_balance = req.initial_balance;

    // Get Exchange Configs
    match state.store.exchange().await.list(&user.user_id).await {
        Err(e) => warn!(
            "⚠️ Failed to get exchange config, using user input: {:?}",
            e
        ),
        Ok(exchanges) => {
            // Find matching exchange
            if let Some(exchange_cfg) = exchanges.iter().find(|ex| ex.id == req.exchange_id) {
                if !exchange_cfg.enabled {
                    warn!(
                        "⚠️ Exchange {} not enabled, using user input",
                        req.exchange_id
                    );
                } else {
                    // Create Temporary Trader Client
                    // Note: This logic depends on your `trader` module implementation.
                    // Assuming create_temp_trader returns Box<dyn TraderTrait>
                    let temp_trader_result =
                        create_temp_trader_client(&req.exchange_id, exchange_cfg).await;

                    match temp_trader_result {
                        Err(e) => warn!("⚠️ Failed to create temp trader: {:?}", e),
                        Ok(client) => {
                            // Query Balance
                            match client.get_balance().await {
                                Err(e) => warn!("⚠️ Failed to query balance: {:?}", e),
                                Ok(balance_info) => {
                                    // Extract Available Balance
                                    // Try different keys: availableBalance, available_balance, totalWalletBalance, balance
                                    // Using serde_json::Value helper
                                    let found_balance = balance_info
                                        .get("availableBalance")
                                        .or(balance_info.get("available_balance"))
                                        .or(balance_info.get("totalWalletBalance"))
                                        .or(balance_info.get("balance"))
                                        .and_then(|v| v.as_f64());

                                    if let Some(bal) = found_balance {
                                        if bal > 0.0 {
                                            actual_balance = bal;
                                            info!(
                                                "✓ Queried exchange actual balance: {:.2} USDT (user input: {:.2})",
                                                actual_balance, req.initial_balance
                                            );
                                        }
                                    } else {
                                        warn!(
                                            "⚠️ Unable to extract available balance from {:?}",
                                            balance_info
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                warn!("⚠️ Exchange {} configuration not found", req.exchange_id);
            }
        }
    }

    // Create Trader Entity (DB Record)
    debug!("🔧: Creating trader config ID={}", trader_id);
    let trader_record = store::trader::Trader {
        id: trader_id.clone(),
        user_id: user.user_id.clone(),
        name: req.name.clone(),
        ai_model_id: req.ai_model_id.clone(),
        exchange_id: req.exchange_id.clone(),
        strategy_id: req.strategy_id.clone().unwrap_or_default(),
        initial_balance: actual_balance,
        btc_eth_leverage,
        altcoin_leverage,
        trading_symbols: req.trading_symbols.clone().unwrap_or_default(),
        use_coin_pool: req.use_coin_pool,
        use_oi_top: req.use_oi_top,
        custom_prompt: req.custom_prompt.clone().unwrap_or_default(),
        override_base_prompt: req.override_base_prompt,
        system_prompt_template,
        is_cross_margin,
        scan_interval_minutes,
        is_running: false,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    // Save to DB
    if let Err(e) = state.store.trader().await.create(&trader_record).await {
        error!("❌ Failed to create trader: {:?}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("Failed to create trader: {:?}", e) })),
        )
            .into_response();
    }

    // Sync with TraderManager
    info!("🔧 DEBUG: Syncing with TraderManager");
    if let Err(e) = state
        .trader_manager
        .load_user_traders_from_store(&state.store, &user.user_id)
        .await
    {
        warn!("⚠️ Failed to load user traders into memory: {:?}", e);
    }

    info!("✓ Trader created successfully: {}", req.name);

    (
        StatusCode::CREATED,
        Json(json!({
            "trader_id":   trader_id,
            "trader_name": req.name,
            "ai_model":    req.ai_model_id,
            "is_running":  false,
        })),
    )
        .into_response()
}

async fn create_temp_trader_client(
    exchange_type: &str,
    cfg: &Exchange,
) -> Result<Box<dyn Trader + Send + Sync>> {
    let trader: Box<dyn Trader + Send + Sync> = match exchange_type {
        "binance" => {
            let trader = trader::binance_futures::FuturesTrader::new(&cfg.api_key, &cfg.secret_key);
            Box::new(trader)
        }
        "aster" => {
            let trader = trader::aster_trader::AsterTrader::new(
                &cfg.aster_user,
                &cfg.aster_signer,
                &cfg.aster_private_key,
            )?;
            Box::new(trader)
        }
        "hyperliquid" => {
            let trader = trader::hyperliquid_trader::HyperliquidTrader::new(
                &cfg.api_key,
                &cfg.hyperliquid_wallet_addr,
                cfg.testnet,
            )
            .await?;
            Box::new(trader)
        }
        _ => return Err(anyhow::anyhow!("Unsupported exchange type")),
    };

    Ok(trader)
}

// handle_update_trader Update trader configuration
#[instrument(skip(state))]
pub async fn handle_update_trader(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Path(trader_id): Path<String>,
    Json(req): Json<UpdateTraderRequest>,
) -> impl IntoResponse {
    // Validation
    if let Err(e) = req.validate() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": format!("Validation failed: {}", e) })),
        )
            .into_response();
    }

    // Check if trader exists and belongs to current user
    let traders = match state.store.trader().await.list(&user.user_id).await {
        Ok(t) => t,
        Err(_e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to get trader list" })),
            )
                .into_response();
        }
    };

    // Find the specific trader
    let existing_trader = match traders.iter().find(|t| t.id == trader_id) {
        Some(t) => t,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Trader does not exist" })),
            )
                .into_response();
        }
    };

    // Is Cross Margin: Use request if present, else keep original
    let is_cross_margin = req
        .is_cross_margin
        .unwrap_or(existing_trader.is_cross_margin);

    let btc_eth_leverage = match req.btc_eth_leverage {
        Some(v) if v > 0 => v,
        _ => existing_trader.btc_eth_leverage,
    };

    let altcoin_leverage = match req.altcoin_leverage {
        Some(v) if v > 0 => v,
        _ => existing_trader.altcoin_leverage,
    };

    // Scan Interval: Go logic "if <= 0 { keep original } else if < 3 { 3 }"
    let scan_interval_minutes = match req.scan_interval_minutes {
        Some(v) if v <= 0 => existing_trader.scan_interval_minutes,
        Some(v) if v < 3 => 3,
        Some(v) => v,
        None => existing_trader.scan_interval_minutes,
    };

    // System Prompt Template
    let system_prompt_template = match req.system_prompt_template {
        Some(ref s) if !s.is_empty() => s.clone(),
        _ => existing_trader.system_prompt_template.clone(),
    };

    // Strategy ID
    let strategy_id = match req.strategy_id {
        Some(ref s) if !s.is_empty() => s.clone(),
        _ => existing_trader.strategy_id.clone(),
    };

    // Construct the updated record
    let trader_record = store::trader::Trader {
        id: trader_id.clone(),
        user_id: user.user_id.clone(),
        name: req.name.clone(),
        ai_model_id: req.ai_model_id.clone(),
        exchange_id: req.exchange_id.clone(),
        strategy_id: strategy_id,
        // For simple fields, use unwrap_or(existing) or the request value directly
        initial_balance: req
            .initial_balance
            .unwrap_or(existing_trader.initial_balance),
        btc_eth_leverage,
        altcoin_leverage,
        trading_symbols: req
            .trading_symbols
            .clone()
            .unwrap_or_else(|| existing_trader.trading_symbols.clone()),
        custom_prompt: req
            .custom_prompt
            .clone()
            .unwrap_or(existing_trader.custom_prompt.clone()),
        override_base_prompt: req
            .override_base_prompt
            .unwrap_or(existing_trader.override_base_prompt),
        system_prompt_template,
        is_cross_margin,
        scan_interval_minutes,
        is_running: existing_trader.is_running,
        use_coin_pool: false,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        use_oi_top: false,
    };

    // Update Database
    if let Err(e) = state.store.trader().await.update(&trader_record).await {
        error!("Failed to update trader: {:?}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("Failed to update trader: {:?}", e) })),
        )
            .into_response();
    }

    // Reload into Memory
    if let Err(e) = state
        .trader_manager
        .load_user_traders_from_store(&state.store, &user.user_id)
        .await
    {
        info!("⚠️ Failed to reload user traders into memory: {:?}", e);
    }

    info!(
        "✓ Trader updated successfully: {} (model: {}, exchange: {})",
        req.name, req.ai_model_id, req.exchange_id
    );

    // 7. Return Response
    (
        StatusCode::OK,
        Json(json!({
            "trader_id":   trader_id,
            "trader_name": req.name,
            "ai_model":    req.ai_model_id,
            "message":     "Trader updated successfully",
        })),
    )
        .into_response()
}

// handle_delete_trader Delete trader
#[instrument(skip(state))]
pub async fn handle_delete_trader(
    State(state): State<AppState>,
    // Extract user_id from middleware
    Extension(user): Extension<AuthUser>,
    // Extract trader_id from URL path
    Path(trader_id): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = state.store.trader().await.delete(&user.user_id, &trader_id).await {
        error!("Failed to delete trader: {:?}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("Failed to delete trader: {:?}", e) })),
        )
            .into_response();
    }

    // If trader is running, stop it first
    if let Ok(active_trader) = state.trader_manager.get_trader(&trader_id).await {
        // Get real-time status
        let status = active_trader.get_status().await.unwrap_or_default();

        // Check if running
        let is_running = status
            .get("is_running")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        if is_running {
            // Stop the trader (assuming stop() is an async method on your Trader struct)
            active_trader.stop().await;
            info!("⏹  Stopped running trader: {}", trader_id);
        }

        // Optional: Explicitly remove from manager memory if your architecture requires it
        state.trader_manager.remove_trader(&trader_id).await;
    }

    info!("✓ Trader deleted: {}", trader_id);

    (StatusCode::OK, Json(json!({ "message": "Trader deleted" }))).into_response()
}

// handle_start_trader Start trader
#[instrument(skip(state))]
pub async fn handle_start_trader(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Path(trader_id): Path<String>,
) -> impl IntoResponse {
    // Verify trader belongs to current user
    let full_config_result = state
        .store
        .trader()
        .await
        .get_full_config(&user.user_id, &trader_id)
        .await;

    if full_config_result.is_err() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "Trader does not exist or no access permission" })),
        )
            .into_response();
    }

    // Try to get Trader from Memory
    let trader = match state.trader_manager.get_trader(&trader_id).await {
        Ok(t) => t,
        Err(_) => {
            // Trader not in memory, try loading from database
            info!("🔄 Trader {} not in memory, trying to load...", trader_id);

            if let Err(e) = state
                .trader_manager
                .load_user_traders_from_store(&state.store, &user.user_id)
                .await
            {
                error!("❌ Failed to load user traders: {:?}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": format!("Failed to load trader: {:?}", e) })),
                )
                    .into_response();
            }

            // Try to get trader again
            match state.trader_manager.get_trader(&trader_id).await {
                Ok(t) => t,
                Err(_) => {
                    // Detailed Error Diagnostics
                    let full_cfg = full_config_result.unwrap();

                    // Check Strategy
                    if full_cfg.strategy.is_none() {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(json!({ "error": "Trader has no strategy configured, please create a strategy in Strategy Studio and associate it with the trader" }))
                        ).into_response();
                    }

                    // Check AI Model
                    match &full_cfg.ai_model {
                        None => {
                            return (
                                StatusCode::BAD_REQUEST,
                                Json(json!({ "error": "Trader's AI model does not exist, please check AI model configuration" }))
                            ).into_response();
                        }
                        Some(model) if !model.enabled => {
                            return (
                                StatusCode::BAD_REQUEST,
                                Json(json!({ "error": "Trader's AI model is not enabled, please enable the AI model first" }))
                            ).into_response();
                        }
                        _ => {}
                    }

                    // Check Exchange
                    match &full_cfg.exchange {
                        None => {
                            return (
                                StatusCode::BAD_REQUEST,
                                Json(json!({ "error": "Trader's exchange does not exist, please check exchange configuration" }))
                            ).into_response();
                        }
                        Some(ex) if !ex.enabled => {
                            return (
                                StatusCode::BAD_REQUEST,
                                Json(json!({ "error": "Trader's exchange is not enabled, please enable the exchange first" }))
                            ).into_response();
                        }
                        _ => {}
                    }

                    // Generic error if specific reason not found
                    return (
                        StatusCode::NOT_FOUND,
                        Json(json!({ "error": "Failed to load trader, please check AI model, exchange and strategy configuration" }))
                    ).into_response();
                }
            }
        }
    };

    // Check if trader is already running
    let status = trader.get_status().await.unwrap_or_default();
    let is_running = status
        .get("is_running")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    if is_running {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Trader is already running" })),
        )
            .into_response();
    }

    // Start trader (Background Task)
    // We clone the Arc<Trader> to move it into the spawned task
    let trader_clone = trader.clone();
    let trader_name = trader.get_name(); // Assuming get_name returns String or &str (needs to be owned for logging inside spawn if using String)
    let trader_id_log = trader_id.clone();

    tokio::spawn(async move {
        info!("▶️  Starting trader {} ({})", trader_id_log, trader_name);

        // Corresponding Go: if err := trader.Run(); err != nil
        if let Err(e) = trader_clone.run().await {
            info!("❌ Trader {} runtime error: {:?}", trader_name, e);
        }
    });

    // Update running status in database
    if let Err(e) = state
        .store
        .trader()
        .await
        .update_status(&user.user_id, &trader_id, true)
        .await
    {
        info!("⚠️  Failed to update trader status: {:?}", e);
    }

    info!("✓ Trader {} started", trader.get_name());

    (StatusCode::OK, Json(json!({ "message": "Trader started" }))).into_response()
}

// handle_stop_trader Stop trader
#[instrument(skip(state))]
pub async fn handle_stop_trader(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Path(trader_id): Path<String>,
) -> impl IntoResponse {
    // Verify trader belongs to current user
    if let Err(_) = state
        .store
        .trader()
        .await
        .get_full_config(&user.user_id, &trader_id)
        .await
    {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "Trader does not exist or no access permission" })),
        )
            .into_response();
    }

    // Get active trader instance from memory
    let trader = match state.trader_manager.get_trader(&trader_id).await {
        Ok(t) => t,
        Err(_) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Trader does not exist" })),
            )
                .into_response();
        }
    };

    // Check if trader is running
    let status = trader.get_status().await.unwrap_or_default();
    let is_running = status
        .get("is_running")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    if !is_running {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Trader is already stopped" })),
        )
            .into_response();
    }

    // Stop trader
    trader.stop().await;

    // Update running status in database
    if let Err(e) = state
        .store
        .trader()
        .await
        .update_status(&user.user_id, &trader_id, false)
        .await
    {
        warn!("⚠️  Failed to update trader status: {:?}", e);
    }

    info!("⏹  Trader {} stopped", trader.get_name()); // Assuming get_name() returns &str or String

    (StatusCode::OK, Json(json!({ "message": "Trader stopped" }))).into_response()
}

// handle_update_trader_prompt Update trader custom prompt
#[instrument(skip(state))]
pub async fn handle_update_trader_prompt(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Path(trader_id): Path<String>,
    Json(req): Json<UpdateTraderPromptRequest>,
) -> impl IntoResponse {
    // Update database
    if let Err(e) = state
        .store
        .trader()
        .await
        .update_custom_prompt(
            &user.user_id,
            &trader_id,
            &req.custom_prompt,
            req.override_base_prompt,
        )
        .await
    {
        error!("Failed to update custom prompt: {:?}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("Failed to update custom prompt: {:?}", e) })),
        )
            .into_response();
    }

    // If trader is in memory, update its custom prompt and override settings
    if let Ok(active_trader) = state.trader_manager.get_trader(&trader_id).await {
        // Update memory state
        // Assuming your Trader struct has thread-safe setters (e.g., using RwLock internally)
        // If these methods involve locks, they might be async, so we use .await just in case.
        active_trader.set_custom_prompt(&req.custom_prompt).await;
        active_trader
            .set_override_base_prompt(req.override_base_prompt)
            .await;

        info!(
            "✓ Updated trader {} custom prompt (override base={})",
            active_trader.get_name(),
            req.override_base_prompt
        );
    }

    // Return success
    (
        StatusCode::OK,
        Json(json!({ "message": "Custom prompt updated" })),
    )
        .into_response()
}

// handle_sync_balance Sync exchange balance to initial_balance
#[instrument(skip(state))]
pub async fn handle_sync_balance(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Path(trader_id): Path<String>,
) -> impl IntoResponse {
    info!(
        "🔄 User {} requested balance sync for trader {}",
        user.user_id, trader_id
    );

    // Get trader configuration from database
    let full_config = match state
        .store
        .trader()
        .await
        .get_full_config(&user.user_id, &trader_id)
        .await
    {
        Ok(cfg) => cfg,
        Err(_) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Trader does not exist" })),
            )
                .into_response();
        }
    };

    let trader_config = full_config.trader;

    // Validate Exchange
    let exchange_cfg = match full_config.exchange {
        Some(ex) if ex.enabled => ex,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Exchange not configured or not enabled" })),
            )
                .into_response();
        }
    };

    // Create temporary trader client
    // We use a helper function to abstract the matching logic
    let temp_trader_result =
        create_temp_trader_client(&trader_config.exchange_id, &exchange_cfg).await;

    let client = match temp_trader_result {
        Ok(c) => c,
        Err(e) => {
            info!("⚠️ Failed to create temporary trader: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("Failed to connect to exchange: {:?}", e) })),
            )
                .into_response();
        }
    };

    // Query actual balance
    let balance_info = match client.get_balance().await {
        Ok(info) => info,
        Err(e) => {
            info!("⚠️ Failed to query exchange balance: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("Failed to query balance: {:?}", e) })),
            )
                .into_response();
        }
    };

    // Extract available balance
    let actual_balance_opt = balance_info
        .get("available_balance")
        .or(balance_info.get("availableBalance"))
        .or(balance_info.get("balance"))
        .or(balance_info.get("totalWalletBalance")) // Added from previous context
        .and_then(|v| v.as_f64())
        .filter(|&v| v > 0.0);

    let actual_balance = match actual_balance_opt {
        Some(bal) => bal,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Unable to get available balance" })),
            )
                .into_response();
        }
    };

    let old_balance = trader_config.initial_balance;

    let change_percent = if old_balance != 0.0 {
        ((actual_balance - old_balance) / old_balance) * 100.0
    } else {
        0.0 // Or 100.0 depending on how you view 0 -> positive
    };

    let change_type = if change_percent < 0.0 {
        "decrease"
    } else {
        "increase"
    };

    info!(
        "✓ Queried actual exchange balance: {:.2} USDT (current config: {:.2} USDT, change: {:.2}%)",
        actual_balance, old_balance, change_percent
    );

    // Update initial_balance in database
    if let Err(e) = state
        .store
        .trader()
        .await
        .update_initial_balance(&user.user_id, &trader_id, actual_balance)
        .await
    {
        info!("❌ Failed to update initial_balance: {:?}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Failed to update balance" })),
        )
            .into_response();
    }

    // Reload traders into memory
    if let Err(e) = state
        .trader_manager
        .load_user_traders_from_store(&state.store, &user.user_id)
        .await
    {
        info!("⚠️ Failed to reload user traders into memory: {:?}", e);
    }

    info!(
        "✅ Synced balance: {:.2} → {:.2} USDT ({} {:.2}%)",
        old_balance, actual_balance, change_type, change_percent
    );

    // 9. Return Response
    (
        StatusCode::OK,
        Json(json!({
            "message":        "Balance synced successfully",
            "old_balance":    old_balance,
            "new_balance":    actual_balance,
            "change_percent": change_percent,
            "change_type":    change_type,
        })),
    )
        .into_response()
}

// handle_close_position One-click close position
#[instrument(skip(state))]
pub async fn handle_close_position(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Path(trader_id): Path<String>,
    Json(req): Json<ClosePositionRequest>,
) -> impl IntoResponse {
    // Validate Input
    if req.symbol.trim().is_empty() || req.side.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Parameter error: symbol and side are required" })),
        )
            .into_response();
    }

    // Validate Side Enum
    let side_upper = req.side.to_uppercase();
    if side_upper != "LONG" && side_upper != "SHORT" {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "side must be LONG or SHORT" })),
        )
            .into_response();
    }

    info!(
        "🔻 User {} requested position close: trader={}, symbol={}, side={}",
        user.user_id, trader_id, req.symbol, side_upper
    );

    // Get configuration from database
    let full_config = match state
        .store
        .trader()
        .await
        .get_full_config(&user.user_id, &trader_id)
        .await
    {
        Ok(cfg) => cfg,
        Err(_) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Trader does not exist" })),
            )
                .into_response();
        }
    };

    let trader_config = full_config.trader;

    // Validate Exchange
    let exchange_cfg = match full_config.exchange {
        Some(ex) if ex.enabled => ex,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Exchange not configured or not enabled" })),
            )
                .into_response();
        }
    };

    // Create temporary trader client
    // We instantiate the specific client based on the exchange ID
    let trader_client_result =
        create_temp_trader_client(&trader_config.exchange_id, &exchange_cfg).await;

    let client = match trader_client_result {
        Ok(c) => c,
        Err(e) => {
            info!("⚠️ Failed to create temporary trader: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("Failed to connect to exchange: {:?}", e) })),
            )
                .into_response();
        }
    };

    // Execute close position operation
    let result_output: anyhow::Result<Value>;

    // 0.0 usually means "close all" in these API wrappers, matching the Go code's `0`
    if side_upper == "LONG" {
        result_output = client.close_long(&req.symbol, 0.0).await;
    } else {
        // We already validated side is either LONG or SHORT
        result_output = client.close_short(&req.symbol, 0.0).await;
    }

    match result_output {
        Ok(result) => {
            info!(
                "✅ Position closed successfully: symbol={}, side={}, result={:?}",
                req.symbol, side_upper, result
            );
            (
                StatusCode::OK,
                Json(json!({
                    "message": "Position closed successfully",
                    "symbol":  req.symbol,
                    "side":    side_upper,
                    "result":  result,
                })),
            )
                .into_response()
        }
        Err(e) => {
            info!(
                "❌ Close position failed: symbol={}, side={}, error={:?}",
                req.symbol, side_upper, e
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("Failed to close position: {:?}", e) })),
            )
                .into_response()
        }
    }
}

// handle_get_model_configs Get AI model configurations
#[instrument(skip(state))]
pub async fn handle_get_model_configs(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
) -> impl IntoResponse {
    info!("🔍 Querying AI model configs for user {}", user.user_id);

    // Get models from database
    let models = match state.store.ai_model().await.list(&user.user_id).await {
        Ok(m) => m,
        Err(e) => {
            error!("❌ Failed to get AI model configs: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("Failed to get AI model configs: {:?}", e) })),
            )
                .into_response();
        }
    };

    info!("✅ Found {} AI model configs", models.len());

    // Convert to safe response structure
    // Rust's functional style map() is often cleaner than a for-loop for simple transformations
    let safe_models: Vec<SafeModelConfig> = models
        .into_iter()
        .map(|model| {
            SafeModelConfig {
                id: model.id,
                name: model.name,
                provider: model.provider,
                enabled: model.enabled,
                // Assuming the DB model fields are Option<String> or String.
                // If they are String in DB but you want them mapped, clone() or simple assignment works.
                custom_api_url: model.custom_api_url,
                custom_model_name: model.custom_model_name,
            }
        })
        .collect();

    // Return JSON response
    Json(safe_models).into_response()
}

// handle_update_model_configs Update AI model configurations (encrypted data only)
#[instrument(skip(state))]
pub async fn handle_update_model_configs(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    // Use Bytes to read raw body first
    body: Bytes,
) -> impl IntoResponse {
    // Parse encrypted payload
    let encrypted_payload: EncryptedPayload = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => {
            error!("❌ Failed to parse encrypted payload: {:?}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid request format, encrypted transmission required" })),
            )
                .into_response();
        }
    };

    // Verify encrypted data (Check WrappedKey)
    if encrypted_payload.wrapped_key.is_empty() {
        error!("❌ Detected unencrypted request (UserID: {})", user.user_id);
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error":   "This endpoint only supports encrypted transmission, please use encrypted client",
                "code":    "ENCRYPTION_REQUIRED",
                "message": "Encrypted transmission is required for security reasons",
            })),
        ).into_response();
    }

    // Decrypt data
    let decrypted_json_str = match state
        .crypto_handler
        .crypto_service
        .decrypt_sensitive_data(&encrypted_payload)
    {
        Ok(s) => s,
        Err(e) => {
            error!(
                "❌ Failed to decrypt model config (UserID: {}): {:?}",
                user.user_id, e
            );
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Failed to decrypt data" })),
            )
                .into_response();
        }
    };

    // Parse decrypted data
    let req: UpdateModelConfigRequest = match serde_json::from_str(&decrypted_json_str) {
        Ok(r) => r,
        Err(e) => {
            error!("❌ Failed to parse decrypted data: {:?}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Failed to parse decrypted data" })),
            )
                .into_response();
        }
    };

    info!("🔓 Decrypted model config data (UserID: {})", user.user_id);

    // Update each model's configuration
    for (model_id, model_data) in &req.models {
        // Corresponding Go: s.store.AIModel().Update(...)
        if let Err(e) = state
            .store
            .ai_model()
            .await
            .update(
                &user.user_id,
                &model_id,
                model_data.enabled,
                &model_data.api_key,
                &model_data.custom_api_url,
                &model_data.custom_model_name,
            )
            .await
        {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("Failed to update model {}: {:?}", model_id, e) })),
            )
                .into_response();
        }
    }

    // Reload traders
    if let Err(e) = state
        .trader_manager
        .load_user_traders_from_store(&state.store, &user.user_id)
        .await
    {
        info!("⚠️ Failed to reload user traders into memory: {:?}", e);
    }

    // but we can assume req derives Debug.
    info!("✓ AI model config updated: {:?}", &req.models.keys());

    (
        StatusCode::OK,
        Json(json!({ "message": "Model configuration updated" })),
    )
        .into_response()
}

// handle_get_exchange_configs Get exchange configurations
#[instrument(skip(state))]
pub async fn handle_get_exchange_configs(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
) -> impl IntoResponse {
    info!("🔍 Querying exchange configs for user {}", user.user_id);

    // Get exchanges from database
    let exchanges = match state.store.exchange().await.list(&user.user_id).await {
        Ok(exs) => exs,
        Err(e) => {
            error!("❌ Failed to get exchange configs: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("Failed to get exchange configs: {:?}", e) })),
            )
                .into_response();
        }
    };

    info!("✅ Found {} exchange configs", exchanges.len());

    // Convert to safe response structure
    let safe_exchanges: Vec<SafeExchangeConfig> = exchanges
        .into_iter()
        .map(|ex| {
            SafeExchangeConfig {
                id: ex.id,
                name: ex.name,
                exchange_type: ex.type_, // Assuming the DB field is named type_ or similar
                enabled: ex.enabled,
                testnet: ex.testnet,
                hyperliquid_wallet_addr: ex.hyperliquid_wallet_addr,
                aster_user: ex.aster_user,
                aster_signer: ex.aster_signer,
            }
        })
        .collect();

    // Return JSON response
    Json(safe_exchanges).into_response()
}

// handle_update_exchange_configs Update exchange configurations (encrypted data only)
#[instrument(skip(state))]
pub async fn handle_update_exchange_configs(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    // Use Bytes to get raw data for manual parsing/error handling
    body: Bytes,
) -> impl IntoResponse {
    let user_id = &user.user_id;

    // Parse encrypted payload
    let encrypted_payload: EncryptedPayload = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => {
            error!("❌ Failed to parse encrypted payload: {:?}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Invalid request format, encrypted transmission required" })),
            )
                .into_response();
        }
    };

    // Verify encrypted data (Check WrappedKey)
    if encrypted_payload.wrapped_key.is_empty() {
        error!("❌ Detected unencrypted request (UserID: {})", user_id);
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error":   "This endpoint only supports encrypted transmission, please use encrypted client",
                "code":    "ENCRYPTION_REQUIRED",
                "message": "Encrypted transmission is required for security reasons",
            })),
        ).into_response();
    }

    // Decrypt data
    let decrypted_json = match state
        .crypto_handler
        .crypto_service
        .decrypt_sensitive_data(&encrypted_payload)
    {
        Ok(s) => s,
        Err(e) => {
            error!(
                "❌ Failed to decrypt exchange config (UserID: {}): {:?}",
                user_id, e
            );
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Failed to decrypt data" })),
            )
                .into_response();
        }
    };

    // Parse decrypted data
    let req: UpdateExchangeConfigRequest = match serde_json::from_str(&decrypted_json) {
        Ok(r) => r,
        Err(e) => {
            error!("❌ Failed to parse decrypted data: {:?}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "Failed to parse decrypted data" })),
            )
                .into_response();
        }
    };

    info!("🔓 Decrypted exchange config data (UserID: {})", user_id);

    // Update each exchange's configuration
    for (exchange_id, data) in &req.exchanges {
        let update_result = state
            .store
            .exchange()
            .await
            .update(
                user_id,
                &exchange_id,
                data.enabled,
                &data.api_key,
                &data.secret_key,
                &data.passphrase.clone().unwrap_or_default(),
                data.testnet,
                &data.hyperliquid_wallet_addr.clone().unwrap_or_default(),
                &data.aster_user.clone().unwrap_or_default(),
                &data.aster_signer.clone().unwrap_or_default(),
                &data.aster_private_key.clone().unwrap_or_default(),
                &data.lighter_wallet_addr.clone().unwrap_or_default(),
                &data.lighter_private_key.clone().unwrap_or_default(),
                &data.lighter_api_key_private_key.clone().unwrap_or_default(),
            )
            .await;

        if let Err(e) = update_result {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("Failed to update exchange {}: {:?}", exchange_id, e) })),
            ).into_response();
        }
    }

    // Reload traders
    if let Err(e) = state
        .trader_manager
        .load_user_traders_from_store(&state.store, user_id)
        .await
    {
        info!("⚠️ Failed to reload user traders into memory: {:?}", e);
    }

    // Log success (masking sensitive data in logs is handled by not logging `req` directly or implementing custom Debug)
    info!("✓ Exchange config updated successfully");

    (
        StatusCode::OK,
        Json(json!({ "message": "Exchange configuration updated" })),
    )
        .into_response()
}

// handle_status System status
pub async fn handle_status(
    State(state): State<AppState>,
    // Extract authenticated user
    Extension(user): Extension<AuthUser>,
    // Extract query parameters (?trader_id=...)
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let query_trader_id = params.get("trader_id").cloned();

    let trader_id = match get_trader_from_query(&state, &user.user_id, query_trader_id).await {
        Ok(id) => id,
        Err((code, msg)) => {
            // Map the helper's error directly to a JSON response
            return (code, Json(json!({ "error": msg }))).into_response();
        }
    };

    let trader = match state.trader_manager.get_trader(&trader_id).await {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": format!("Trader not found: {:?}", e) })),
            )
                .into_response();
        }
    };

    let status = trader.get_status().await.unwrap();

    // Return Response
    Json(status).into_response()
}

// handle_account Account information
#[instrument(skip(state))]
pub async fn handle_account(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    // Get Trader ID from query (or default)
    let query_trader_id = params.get("trader_id").cloned();

    // We reuse the helper function defined in previous steps
    let trader_id = match get_trader_from_query(&state, &user.user_id, query_trader_id).await {
        Ok(id) => id,
        Err((code, msg)) => {
            return (code, Json(json!({ "error": msg }))).into_response();
        }
    };

    // Get Trader Instance from Manager
    let trader = match state.trader_manager.get_trader(&trader_id).await {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response();
        }
    };

    // Log Request
    info!("📊 Received account info request [{}]", trader.get_name());

    // Get Account Info
    // We assume this returns Result<serde_json::Value> or Result<HashMap<String, Value>>
    let account = match trader.get_account_info().await {
        Ok(acc) => acc,
        Err(e) => {
            // Log error before returning
            info!(
                "❌ Failed to get account info [{}]: {:?}",
                trader.get_name(),
                e
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("Failed to get account info: {:?}", e) })),
            )
                .into_response();
        }
    };

    // Log Success (Extracting values for logging)
    let get_val = |key: &str| -> f64 { account.get(key).and_then(|v| v.as_f64()).unwrap_or(0.0) };

    info!(
        "✓ Returning account info [{}]: equity={:.2}, available={:.2}, pnl={:.2} ({:.2}%)",
        trader.get_name(),
        get_val("total_equity"),
        get_val("available_balance"),
        get_val("total_pnl"),
        get_val("total_pnl_pct")
    );

    // Return Response
    Json(account).into_response()
}

// handle_positions Position list
#[instrument(skip(state))]
pub async fn handle_positions(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    // Resolve Trader ID
    let query_trader_id = params.get("trader_id").cloned();

    // Use the helper function defined previously
    let trader_id = match get_trader_from_query(&state, &user.user_id, query_trader_id).await {
        Ok(id) => id,
        Err((code, msg)) => {
            return (code, Json(json!({ "error": msg }))).into_response();
        }
    };

    // Get Trader Instance
    let trader = match state.trader_manager.get_trader(&trader_id).await {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response();
        }
    };

    // Get Positions
    match trader.get_positions().await {
        Ok(positions) => {
            // Return JSON response
            // Corresponding Go: c.JSON(http.StatusOK, positions)
            Json(positions).into_response()
        }
        Err(e) => {
            // Corresponding Go: c.JSON(http.StatusInternalServerError, ...)
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("Failed to get position list: {:?}", e) })),
            )
                .into_response()
        }
    }
}

// handle_decisions Decision log list
pub async fn handle_decisions(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let query_trader_id = params.get("trader_id").cloned();

    let trader_id = match get_trader_from_query(&state, &user.user_id, query_trader_id).await {
        Ok(id) => id,
        Err((code, msg)) => {
            return (code, Json(json!({ "error": msg }))).into_response();
        }
    };

    if let Err(e) = state.trader_manager.get_trader(&trader_id).await {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response();
    }

    // Get all historical decision records (unlimited)
    match state
        .store
        .decision()
        .get_latest_records(&trader_id, 10000)
        .await
    {
        Ok(records) => {
            Json(records).into_response()
        }
        Err(e) => {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("Failed to get decision log: {:?}", e) })),
            )
                .into_response()
        }
    }
}

// handle_latest_decisions Latest decision logs (most recent 5, newest first)
pub async fn handle_latest_decisions(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let query_trader_id = params.get("trader_id").cloned();

    let trader_id = match get_trader_from_query(&state, &user.user_id, query_trader_id).await {
        Ok(id) => id,
        Err((code, msg)) => {
            return (code, Json(json!({ "error": msg }))).into_response();
        }
    };

    // Verify Trader exists in Manager
    if let Err(e) = state.trader_manager.get_trader(&trader_id).await {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response();
    }

    // Get decision records (Limit 5)
    match state
        .store
        .decision()
        .get_latest_records(&trader_id, 5)
        .await
    {
        Ok(mut records) => {
            records.reverse();

            // Return JSON
            Json(records).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("Failed to get decision log: {:?}", e) })),
        )
            .into_response(),
    }
}

// handle_statistics Statistics information
pub async fn handle_statistics(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let query_trader_id = params.get("trader_id").cloned();

    let trader_id = match get_trader_from_query(&state, &user.user_id, query_trader_id).await {
        Ok(id) => id,
        Err((code, msg)) => {
            return (code, Json(json!({ "error": msg }))).into_response();
        }
    };

    // Verify Trader exists in Manager
    if let Err(e) = state.trader_manager.get_trader(&trader_id).await {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response();
    }

    // Get Statistics
    match state.store.decision().get_statistics(&trader_id).await {
        Ok(stats) => {
            Json(stats).into_response()
        }
        Err(e) => {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("Failed to get statistics: {:?}", e) })),
            )
                .into_response()
        }
    }
}
