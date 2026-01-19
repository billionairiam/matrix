use std::collections::HashMap;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH,Duration};

use crate::crypto_handler::CryptoHandler;
use anyhow::Result;
use axum::{
    Json, Router,
    extract::{Extension, Path, Query, Request, State},
    http::{HeaderMap, Method, StatusCode},
    middleware,
    middleware::Next,
    response::IntoResponse,
    routing::{any, get, post, put},
};
use backtest::manager::Manager;
use chrono::Utc;
use futures::future::select_ok;
use if_addrs::get_if_addrs;
use logger::warn;
use logger::{error, info};
use manager::trader_manager::TraderManager;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use store::store::Store;
use store::user::User;
use tower_http::cors::{Any, CorsLayer};
use uuid::Uuid;
use validator::Validate;

#[derive(Deserialize, Validate)]
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
    pub backtest_manager: Arc<Manager>,
}

pub struct Server {
    port: u16,
    state: AppState,
}

impl Server {
    pub fn new(
        trader_manager: TraderManager,
        store: Store,
        crypto_handler: CryptoHandler,
        backtest_manager: Manager,
        port: u16,
    ) -> Self {
        let state = AppState {
            trader_manager: Arc::new(trader_manager),
            store: Arc::new(store),
            crypto_handler: Arc::new(crypto_handler),
            backtest_manager: Arc::new(backtest_manager),
        };

        Self { port, state }
    }

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
        println!("Server listening on {}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app).await?;

        Ok(())
    }

    fn setup_routes(&self) -> Router {
        // 公共路由组
        let public_routes = Router::new()
            // Health check
            .route("/health", any(handle_health))
            // System supported models and exchanges
            .route("/supported-models", get(handle_get_supported_models))
            .route("/supported-exchanges", get(handle_get_supported_exchanges))
            // System config
            .route("/config", get(handle_get_system_config))
            // Crypto related (调用 handler 中的方法)
            .route("/crypto/public-key", get(handle_crypto_public_key))
            .route("/crypto/decrypt", post(handle_crypto_decrypt))
            // System prompt template management
            .route("/prompt-templates", get(handle_get_prompt_templates))
            .route("/prompt-templates/:name", get(handle_get_prompt_template))
            // Public competition data
            .route("/traders", get(handle_public_trader_list))
            .route("/competition", get(handle_public_competition))
            .route("/top-traders", get(handle_top_traders))
            .route("/equity-history", get(handle_equity_history))
            .route("/equity-history-batch", post(handle_equity_history_batch))
            .route(
                "/traders/:id/public-config",
                get(handle_get_public_trader_config),
            )
            // Authentication related
            .route("/register", post(handle_register))
            .route("/login", post(handle_login))
            .route("/verify-otp", post(handle_verify_otp))
            .route("/complete-registration", post(handle_complete_registration));

        // 受保护路由组 (需要 Auth 中间件)
        let protected_routes = Router::new()
            .route("/logout", post(handle_logout))
            .route("/server-ip", get(handle_get_server_ip))
            // AI trader management
            .route("/my-traders", get(handle_trader_list))
            .route("/traders", post(handle_create_trader))
            .route("/traders/:id/config", get(handle_get_trader_config))
            .route(
                "/traders/:id",
                put(handle_update_trader).delete(handle_delete_trader),
            )
            .route("/traders/:id/start", post(handle_start_trader))
            .route("/traders/:id/stop", post(handle_stop_trader))
            .route("/traders/:id/prompt", put(handle_update_trader_prompt))
            .route("/traders/:id/sync-balance", post(handle_sync_balance))
            .route("/traders/:id/close-position", post(handle_close_position))
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
                "/strategies/:id",
                get(handle_get_strategy)
                    .put(handle_update_strategy)
                    .delete(handle_delete_strategy),
            )
            .route("/strategies/:id/activate", post(handle_activate_strategy))
            .route("/strategies/:id/duplicate", post(handle_duplicate_strategy))
            // Data for specified trader
            .route("/status", get(handle_status))
            .route("/account", get(handle_account))
            .route("/positions", get(handle_positions))
            .route("/decisions", get(handle_decisions))
            .route("/decisions/latest", get(handle_latest_decisions))
            .route("/statistics", get(handle_statistics))
            // 应用 Auth 中间件
            .layer(middleware::from_fn(auth_middleware));

        // 组合 API
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

    match state.store.trader().list(user_id).await {
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
                            // 注意 Go 代码映射关系: total_pnl -> snap.UnrealizedPnL
                            "total_pnl":    snap.unrealized_pnl,
                            "balance":      snap.balance,
                        })
                    })
                    .collect();

                histories.insert(trader_id.clone(), Value::Array(history_list));
            }
            Err(e) => {
                // 4. 记录错误但不中断循环
                // 对应 Go: errors[traderID] = fmt.Sprintf(...)
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
            // 对应 Go: c.JSON(http.StatusNotFound, ...)
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Trader does not exist" })),
            )
                .into_response();
        }
    };

    let status = trader.get_status().await.unwrap();
    let result = json!({
        "trader_id":   trader.get_id(),       // 对应 trader.GetID()
        "trader_name": trader.get_name(),     // 对应 trader.GetName()
        "ai_model":    trader.get_ai_model(), // 对应 trader.GetAIModel()
        "exchange":    trader.get_exchange(), // 对应 trader.GetExchange()

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
        Err(e) => {
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
    let db_traders = match state.store.trader().list(&user.user_id).await {
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

    // 3. Determine Real-time Running Status
    // Default to database status
    let mut is_running = trader_config.is_running;

    // Check memory manager for override
    // Corresponding Go: if at, err := s.traderManager.GetTrader(traderID); err == nil
    if let Ok(active_trader) = state.trader_manager.get_trader(&trader_id).await {
        // Get status map/json
        let status = active_trader.get_status().await.unwrap();

        // Corresponding Go: if running, ok := status["is_running"].(bool); ok
        if let Some(running) = status.get("is_running").and_then(|v| v.as_bool()) {
            is_running = running;
        }
    }

    // 4. Construct Response
    // Using json! macro to map fields exactly as in the Go version
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

    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let trader_id = format!("{}_{}_{}", req.exchange_id, req.ai_model_id, timestamp);

    let is_cross_margin = req.is_cross_margin.unwrap_or(true);
    let system_prompt_template = req.system_prompt_template.clone().unwrap_or_else(|| "default".to_string());
    let scan_interval_minutes = if req.scan_interval_minutes < 3 { 3 } else { req.scan_interval_minutes };

    let mut actual_balance = req.initial_balance;

     // 6.1 Get Exchange Configs
    match state.store.exchange().list(&user.user_id).await {
        Err(e) => warn!("⚠️ Failed to get exchange config, using user input: {:?}", e),
        Ok(exchanges) => {
            // 6.2 Find matching exchange
            if let Some(exchange_cfg) = exchanges.iter().find(|ex| ex.id == req.exchange_id) {
                if !exchange_cfg.enabled {
                     warn!("⚠️ Exchange {} not enabled, using user input", req.exchange_id);
                } else {
                    // 6.3 Create Temporary Trader Client
                    // Note: This logic depends on your `trader` module implementation.
                    // Assuming create_temp_trader returns Box<dyn TraderTrait>
                    let temp_trader_result = create_temp_trader_client(&req.exchange_id, exchange_cfg, &user.user_id).await;

                    match temp_trader_result {
                        Err(e) => warn!("⚠️ Failed to create temp trader: {:?}", e),
                        Ok(client) => {
                            // 6.4 Query Balance
                            match client.get_balance().await {
                                Err(e) => warn!("⚠️ Failed to query balance: {:?}", e),
                                Ok(balance_info) => {
                                    // 6.5 Extract Available Balance
                                    // Try different keys: availableBalance, available_balance, totalWalletBalance, balance
                                    // Using serde_json::Value helper
                                    let found_balance = balance_info.get("availableBalance")
                                        .or(balance_info.get("available_balance"))
                                        .or(balance_info.get("totalWalletBalance"))
                                        .or(balance_info.get("balance"))
                                        .and_then(|v| v.as_f64());
                                    
                                    if let Some(bal) = found_balance {
                                        if bal > 0.0 {
                                            actual_balance = bal;
                                            info!("✓ Queried exchange actual balance: {:.2} USDT (user input: {:.2})", actual_balance, req.initial_balance);
                                        }
                                    } else {
                                        warn!("⚠️ Unable to extract available balance from {:?}", balance_info);
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

    Json("Create Trader").into_response()
}

async fn handle_update_trader(Path(id): Path<String>) -> impl IntoResponse {
    Json(format!("Update {}", id))
}

async fn handle_delete_trader(Path(id): Path<String>) -> impl IntoResponse {
    Json(format!("Delete {}", id))
}

async fn handle_start_trader(
    State(_state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    // state.trader_manager.start_trader(id)...
    Json(format!("Start {}", id))
}

async fn handle_stop_trader(
    State(_state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    Json(format!("Stop {}", id))
}

async fn handle_update_trader_prompt(Path(id): Path<String>) -> impl IntoResponse {
    Json(format!("Update Prompt {}", id))
}

async fn handle_sync_balance(Path(id): Path<String>) -> impl IntoResponse {
    Json(format!("Sync Balance {}", id))
}

async fn handle_close_position(Path(id): Path<String>) -> impl IntoResponse {
    Json(format!("Close Position {}", id))
}

// Model & Exchange Config
async fn handle_get_model_configs() -> impl IntoResponse {
    Json("Model Configs")
}

async fn handle_update_model_configs() -> impl IntoResponse {
    Json("Update Model Configs")
}

async fn handle_get_exchange_configs() -> impl IntoResponse {
    Json("Exchange Configs")
}

async fn handle_update_exchange_configs() -> impl IntoResponse {
    Json("Update Exchange Configs")
}

// Strategies
async fn handle_get_strategies() -> impl IntoResponse {
    Json("Strategies")
}

async fn handle_get_active_strategy() -> impl IntoResponse {
    Json("Active Strategy")
}

async fn handle_get_default_strategy_config() -> impl IntoResponse {
    Json("Default Strategy Config")
}

async fn handle_preview_prompt() -> impl IntoResponse {
    Json("Preview Prompt")
}

async fn handle_strategy_test_run() -> impl IntoResponse {
    Json("Strategy Test Run")
}

async fn handle_get_strategy(Path(id): Path<String>) -> impl IntoResponse {
    Json(format!("Strategy {}", id))
}

async fn handle_create_strategy() -> impl IntoResponse {
    Json("Create Strategy")
}

async fn handle_update_strategy(Path(id): Path<String>) -> impl IntoResponse {
    Json(format!("Update Strategy {}", id))
}

async fn handle_delete_strategy(Path(id): Path<String>) -> impl IntoResponse {
    Json(format!("Delete Strategy {}", id))
}

async fn handle_activate_strategy(Path(id): Path<String>) -> impl IntoResponse {
    Json(format!("Activate Strategy {}", id))
}

async fn handle_duplicate_strategy(Path(id): Path<String>) -> impl IntoResponse {
    Json(format!("Duplicate Strategy {}", id))
}

// Statistics & Data (Query params example)
#[derive(Deserialize)]
struct TraderIdParam {
    trader_id: Option<String>,
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

    // 4. Return Response
    Json(status).into_response()
}

async fn handle_account(Query(params): Query<TraderIdParam>) -> impl IntoResponse {
    Json(format!("Account for {:?}", params.trader_id))
}

async fn handle_positions(Query(params): Query<TraderIdParam>) -> impl IntoResponse {
    Json(format!("Positions for {:?}", params.trader_id))
}

async fn handle_decisions(Query(params): Query<TraderIdParam>) -> impl IntoResponse {
    Json(format!("Decisions for {:?}", params.trader_id))
}

async fn handle_latest_decisions(Query(params): Query<TraderIdParam>) -> impl IntoResponse {
    Json(format!("Latest Decisions for {:?}", params.trader_id))
}

async fn handle_statistics(Query(params): Query<TraderIdParam>) -> impl IntoResponse {
    Json(format!("Statistics for {:?}", params.trader_id))
}
