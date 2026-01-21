use std::collections::HashMap;

use crate::server::AppState;
use crate::server::AuthUser;
use anyhow::{Context, Result, anyhow};
use axum::Extension;
use axum::Json;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use chrono::Local;
use chrono::Utc;
use decision::engine::AccountInfo;
use decision::engine::Context as EngineContext;
use decision::strategy_engine::StrategyEngine;
use futures::future::join_all;
use logger::error;
use logger::info;
use market::data::get_with_timeframes;
use market::types::Data;
use mcp::custom_client::new_custom_client;
use mcp::deepseek_client::new_deepseek_client;
use mcp::qwen_client::new_qwen_client;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;
use store::strategy::{Strategy, StrategyConfig};
use uuid::Uuid;

#[derive(Deserialize)]
pub struct PreviewPromptRequest {
    pub config: StrategyConfig,

    #[serde(default)] // Defaults to 0.0 if missing in JSON
    pub account_equity: f64,

    #[serde(default)] // Defaults to "" if missing in JSON
    pub prompt_variant: String,
}

#[derive(Deserialize)]
pub struct DuplicateStrategyRequest {
    pub name: String,
}

#[derive(Deserialize)]
pub struct UpdateStrategyRequest {
    pub name: String,
    // Use Option<String> to handle potential nulls, or String to match Go's default empty string behavior
    #[serde(default)]
    pub description: String,
    pub config: StrategyConfig,
}

#[derive(Deserialize)]
pub struct CreateStrategyRequest {
    pub name: String,
    pub description: Option<String>,
    pub config: StrategyConfig, // Expecting a JSON object that matches StrategyConfig
}

#[derive(Deserialize)]
pub struct StrategyTestRunRequest {
    pub config: StrategyConfig,

    #[serde(default)]
    pub prompt_variant: String,

    pub ai_model_id: Option<String>,

    #[serde(default)]
    pub run_real_ai: bool,
}

// handle_strategy_test_run AI test run
pub async fn handle_strategy_test_run(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Json(mut req): Json<StrategyTestRunRequest>,
) -> impl IntoResponse {
    // 1. Validate Request
    // Axum handles basic JSON binding errors.
    // Manual check for "balanced" default logic matches Go.
    if req.prompt_variant.is_empty() {
        req.prompt_variant = "balanced".to_string();
    }

    // 2. Create Strategy Engine
    // Assuming StrategyEngine::new takes a reference to config
    let engine = StrategyEngine::new(req.config.clone());

    // 3. Get Candidate Coins
    // Assuming this returns Result<Vec<CoinInfo>>
    let candidates = match engine.get_candidate_coins().await {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": format!("Failed to get candidate coins: {}", e),
                    "ai_response": ""
                })),
            )
                .into_response();
        }
    };

    // 4. Determine Timeframes configuration
    // Accessing nested fields from config
    let klines_config = &req.config.indicators.klines;
    let mut timeframes = klines_config.selected_timeframes.clone();
    let mut primary_timeframe = klines_config.primary_timeframe.clone();
    let mut kline_count = klines_config.primary_count;

    // Backward compatibility logic matching Go
    if timeframes.is_empty() {
        if !primary_timeframe.is_empty() {
            timeframes.push(primary_timeframe.clone());
        } else {
            timeframes.push("3m".to_string());
        }

        if !klines_config.longer_timeframe.is_empty() {
            timeframes.push(klines_config.longer_timeframe.clone());
        }
    }

    if primary_timeframe.is_empty() && !timeframes.is_empty() {
        primary_timeframe = timeframes[0].clone();
    }

    if kline_count <= 0 {
        kline_count = 30;
    }

    info!(
        "📊 Using timeframes: {:?}, primary: {}, kline count: {}",
        timeframes, primary_timeframe, kline_count
    );

    // 5. Get Real Market Data (Concurrent Fetching)
    // Go loop was sequential, here we make it concurrent for performance
    let fetch_tasks: Vec<_> = candidates
        .iter()
        .map(|coin| {
            let symbol = coin.symbol.clone();
            let mut tfs = timeframes.clone();
            let primary_tf = primary_timeframe.clone();

            async move {
                match get_with_timeframes(&symbol, &mut tfs, Some(primary_tf)).await {
                    Ok(data) => Some((symbol, data)),
                    Err(e) => {
                        info!("⚠️  Failed to get market data for {}: {:?}", symbol, e);
                        None
                    }
                }
            }
        })
        .collect();

    // Wait for all requests to finish
    let results = join_all(fetch_tasks).await;

    // Collect into HashMap, filtering out failures (Nones)
    let market_data_map: HashMap<String, Data> = results
        .into_iter()
        .flatten() // Removes None
        .collect();

    // 6. Build Context
    let current_time = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();

    let test_context = EngineContext {
        current_time,
        runtime_minutes: 0,
        call_count: 1,
        account: AccountInfo {
            total_equity: 1000.0,
            available_balance: 1000.0,
            unrealized_pnl: 0.0,
            total_pnl: 0.0,
            total_pnl_pct: 0.0,
            margin_used: 0.0,
            margin_used_pct: 0.0,
            position_count: 0,
        },
        positions: vec![], // Empty Vec<PositionInfo>
        candidate_coins: candidates.clone(),
        prompt_variant: Some(req.prompt_variant.clone()),
        market_data_map,
        trading_stats: None,
        recent_orders: Vec::new(),
        multi_tf_market: HashMap::new(),
        oi_top_data_map: None,
        quant_data_map: None,
        btc_eth_leverage: 10,
        altcoin_leverage: 5,
    };

    // 7. Build Prompts
    let system_prompt = engine.build_system_prompt(1000.0, &req.prompt_variant);
    let user_prompt = engine.build_user_prompt(&test_context);

    // 8. Handle Real AI Run
    if req.run_real_ai {
        if let Some(model_id) = &req.ai_model_id {
            if !model_id.is_empty() {
                // Call the helper function to run real AI test
                match run_real_ai_test(
                    &state,
                    &user.user_id,
                    model_id,
                    &system_prompt,
                    &user_prompt,
                )
                .await
                {
                    Ok(ai_response) => {
                        return (
                            StatusCode::OK,
                            Json(json!({
                                "system_prompt":   system_prompt,
                                "user_prompt":     user_prompt,
                                "candidate_count": candidates.len(),
                                "candidates":      candidates,
                                "prompt_variant":  req.prompt_variant,
                                "ai_response":     ai_response,
                                "note":            "✅ Real AI test run successful",
                            })),
                        )
                            .into_response();
                    }
                    Err(e) => {
                        return (
                            StatusCode::OK, // Return 200 even on AI failure to show the partial data, consistent with Go
                            Json(json!({
                                "system_prompt":   system_prompt,
                                "user_prompt":     user_prompt,
                                "candidate_count": candidates.len(),
                                "candidates":      candidates,
                                "prompt_variant":  req.prompt_variant,
                                "ai_response":     format!("❌ AI call failed: {}", e),
                                "ai_error":        e.to_string(),
                                "note":            "AI call error",
                            })),
                        )
                            .into_response();
                    }
                }
            }
        }
    }

    // 9. Return result (Dry Run)
    (
        StatusCode::OK,
        Json(json!({
            "system_prompt":   system_prompt,
            "user_prompt":     user_prompt,
            "candidate_count": candidates.len(),
            "candidates":      candidates,
            "prompt_variant":  req.prompt_variant,
            "ai_response":     "Please select an AI model and click 'Run Test' to perform real AI analysis.",
            "note":            "AI model not selected or real AI call not enabled",
        })),
    ).into_response()
}

// Helper: Run Real AI Test
// Assuming it exists as a method on AppState or a service within state.
async fn run_real_ai_test(
    state: &AppState,
    user_id: &str,
    model_id: &str,
    sys_prompt: &str,
    user_prompt: &str,
) -> Result<String> {
    let model = state
        .store
        .ai_model()
        .get(user_id, model_id)
        .await
        .context("failed to get AI model")?;

    // 2. Validate Model State
    if !model.enabled {
        return Err(anyhow!("AI model {} is not enabled", model.name));
    }

    if model.api_key.is_empty() {
        return Err(anyhow!("AI model {} is missing API Key", model.name));
    }

    let ai_client = match model.provider.as_str() {
        "qwen" => new_qwen_client(&model.api_key).unwrap(),
        "deepseek" => new_deepseek_client(&model.api_key).unwrap(),
        _ => new_custom_client(&model.api_key, &model.custom_api_url).unwrap(),
    };

    let response = ai_client
        .call_with_messages(sys_prompt, user_prompt)
        .await
        .map_err(|e| anyhow!("AI API call failed: {:?}", e))?;

    Ok(response)
}

pub async fn handle_get_strategy(
    State(state): State<AppState>,
    // Extract user_id from middleware
    Extension(user): Extension<AuthUser>,
    // Extract strategy_id from URL path
    Path(strategy_id): Path<String>,
) -> impl IntoResponse {
    // 1. Get strategy from database
    // Corresponding Go: s.store.Strategy().Get(userID, strategyID)
    let strategy = match state
        .store
        .strategy()
        .get(&user.user_id, &strategy_id)
        .await
    {
        Ok(s) => s,
        Err(_) => {
            // Corresponding Go: c.JSON(http.StatusNotFound, ...)
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Strategy not found" })),
            )
                .into_response();
        }
    };

    // 2. Parse config JSON
    // The config is stored as a string in DB, but we want to return it as a nested JSON object.
    // Corresponding Go: json.Unmarshal([]byte(strategy.Config), &config)
    let config_obj: Value = serde_json::from_str(&strategy.config).unwrap_or(json!({}));

    // 3. Return JSON response
    // Corresponding Go: c.JSON(http.StatusOK, gin.H{...})
    Json(json!({
        "id":          strategy.id,
        "name":        strategy.name,
        "description": strategy.description,
        "is_active":   strategy.is_active,
        "is_default":  strategy.is_default,
        "config":      config_obj,
        "created_at":  strategy.created_at,
        "updated_at":  strategy.updated_at,
    }))
    .into_response()
}

// handle_create_strategy Create strategy
pub async fn handle_create_strategy(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Json(req): Json<CreateStrategyRequest>,
) -> impl IntoResponse {
    // 1. Validate Input
    // Axum handles type mismatch errors automatically.
    // We add a manual check for empty name (corresponding to binding:"required")
    if req.name.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Invalid request parameters: Name is required" })),
        )
            .into_response();
    }

    // 2. Serialize configuration
    // Corresponding Go: json.Marshal(req.Config)
    let config_json = match serde_json::to_string(&req.config) {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to serialize configuration: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to serialize configuration" })),
            )
                .into_response();
        }
    };

    // 3. Create Strategy Entity
    // Corresponding Go: uuid.New().String()
    let strategy_id = Uuid::new_v4().to_string();

    let strategy = Strategy {
        id: strategy_id.clone(),
        user_id: user.user_id.clone(),
        name: req.name,
        description: req.description.unwrap_or_default(), // Handle Option -> String ("")
        is_active: false,
        is_default: false,
        config: config_json,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    // 4. Save to Database
    // Corresponding Go: s.store.Strategy().Create(strategy)
    if let Err(e) = state.store.strategy().create(&strategy).await {
        error!("Failed to create strategy: {:?}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("Failed to create strategy: {:?}", e) })),
        )
            .into_response();
    }

    // 5. Return Success Response
    (
        StatusCode::OK,
        Json(json!({
            "id":      strategy_id,
            "message": "Strategy created successfully",
        })),
    )
        .into_response()
}

// handle_update_strategy Update strategy
pub async fn handle_update_strategy(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Path(strategy_id): Path<String>,
    Json(req): Json<UpdateStrategyRequest>,
) -> impl IntoResponse {
    // 1. Check if strategy exists and belongs to user
    let mut strategy = match state
        .store
        .strategy()
        .get(&user.user_id, &strategy_id)
        .await
    {
        Ok(s) => s,
        Err(_) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "Strategy not found" })),
            )
                .into_response();
        }
    };

    // 2. Check if it's a system default strategy
    if strategy.is_default {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "Cannot modify system default strategy" })),
        )
            .into_response();
    }

    // 3. Serialize configuration
    let config_json = match serde_json::to_string(&req.config) {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to serialize configuration: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to serialize configuration" })),
            )
                .into_response();
        }
    };

    // 4. Update the strategy object
    strategy.name = req.name;
    strategy.description = req.description;
    strategy.config = config_json;
    strategy.updated_at = chrono::Utc::now();

    // 5. Update database
    if let Err(e) = state.store.strategy().update(&strategy).await {
        error!("Failed to update strategy: {:?}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("Failed to update strategy: {:?}", e) })),
        )
            .into_response();
    }

    // 6. Return Success
    (
        StatusCode::OK,
        Json(json!({ "message": "Strategy updated successfully" })),
    )
        .into_response()
}

// handle_delete_strategy Delete strategy
pub async fn handle_delete_strategy(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Path(strategy_id): Path<String>,
) -> impl IntoResponse {
    // 1. Delete from database
    if let Err(e) = state
        .store
        .strategy()
        .delete(&user.user_id, &strategy_id)
        .await
    {
        error!("Failed to delete strategy: {:?}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("Failed to delete strategy: {:?}", e) })),
        )
            .into_response();
    }

    // 2. Return Success
    (
        StatusCode::OK,
        Json(json!({ "message": "Strategy deleted successfully" })),
    )
        .into_response()
}

// handle_activate_strategy Activate strategy
pub async fn handle_activate_strategy(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Path(strategy_id): Path<String>,
) -> impl IntoResponse {
    // 1. Activate strategy in database
    if let Err(e) = state
        .store
        .strategy()
        .set_active(&user.user_id, &strategy_id)
        .await
    {
        error!("Failed to activate strategy: {:?}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("Failed to activate strategy: {:?}", e) })),
        )
            .into_response();
    }

    // 2. Return Success
    (
        StatusCode::OK,
        Json(json!({ "message": "Strategy activated successfully" })),
    )
        .into_response()
}

// handle_duplicate_strategy Duplicate strategy
pub async fn handle_duplicate_strategy(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Path(source_id): Path<String>,
    Json(req): Json<DuplicateStrategyRequest>,
) -> impl IntoResponse {
    // 1. Validate Input
    // Check if name is empty (corresponding to binding:"required")
    if req.name.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Invalid request parameters: name is required" })),
        )
            .into_response();
    }

    // 2. Generate new ID
    let new_id = Uuid::new_v4().to_string();

    // 3. Duplicate in database
    if let Err(e) = state
        .store
        .strategy()
        .duplicate(&user.user_id, &source_id, &new_id, &req.name)
        .await
    {
        error!("Failed to duplicate strategy: {:?}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("Failed to duplicate strategy: {:?}", e) })),
        )
            .into_response();
    }

    // 4. Return Success
    (
        StatusCode::OK,
        Json(json!({
            "id":      new_id,
            "message": "Strategy duplicated successfully",
        })),
    )
        .into_response()
}

// handle_get_active_strategy Get currently active strategy
pub async fn handle_get_active_strategy(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
) -> impl IntoResponse {
    // 1. Get active strategy from database
    // Note: Axum's Extension guarantees user exists if middleware passed,
    // so we don't strictly need to check for empty user_id here.
    let strategy = match state.store.strategy().get_active(&user.user_id).await {
        Ok(s) => s,
        Err(_) => {
            // Corresponding Go: c.JSON(http.StatusNotFound, ...)
            // We interpret any DB error here as "No active strategy" to match Go logic
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": "No active strategy" })),
            )
                .into_response();
        }
    };

    // 2. Parse config JSON
    // The config is stored as a string in the DB, but the API client expects a JSON object.
    // Corresponding Go: json.Unmarshal([]byte(strategy.Config), &config)
    let config_obj: Value = serde_json::from_str(&strategy.config).unwrap_or(json!({}));

    // 3. Return JSON response
    // Corresponding Go: c.JSON(http.StatusOK, gin.H{...})
    Json(json!({
        "id":          strategy.id,
        "name":        strategy.name,
        "description": strategy.description,
        "is_active":   strategy.is_active,
        "is_default":  strategy.is_default,
        "config":      config_obj, // Embeds as object, not string
        "created_at":  strategy.created_at,
        "updated_at":  strategy.updated_at,
    }))
    .into_response()
}

// handle_get_default_strategy_config Get default strategy configuration template
pub async fn handle_get_default_strategy_config(
    // Extract query parameters into a HashMap (e.g., ?lang=zh)
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    // 1. Get language from query parameter
    // Corresponding Go: lang := c.Query("lang")
    let raw_lang = params.get("lang").map(|s| s.as_str()).unwrap_or("");

    // 2. Logic: Default to "en" if not "zh"
    // Corresponding Go: if lang != "zh" { lang = "en" }
    let lang = if raw_lang == "zh" { "zh" } else { "en" };

    // 3. Get default configuration
    // Corresponding Go: defaultConfig := store.GetDefaultStrategyConfig(lang)
    let default_config = store::strategy::StrategyStore::get_default_strategy_config(lang);

    // 4. Return JSON
    Json(default_config)
}

// handle_preview_prompt Preview prompt generated by strategy
pub async fn handle_preview_prompt(
    Extension(user): Extension<AuthUser>,
    // Axum automatically handles JSON binding and validation errors (400 Bad Request)
    Json(req): Json<PreviewPromptRequest>,
) -> impl IntoResponse {
    // In Rust with Extension, if the middleware passed, the user exists.
    // However, if you strictly need to check for empty ID (though unlikely with auth middleware):
    if user.user_id.is_empty() {
        // This usually handled by middleware returning 401
        return (
            axum::http::StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "Unauthorized" })),
        )
            .into_response();
    }

    // 1. Apply Default Values
    // Corresponding Go: if req.AccountEquity <= 0 { ... }
    let account_equity = if req.account_equity <= 0.0 {
        1000.0
    } else {
        req.account_equity
    };

    // Corresponding Go: if req.PromptVariant == "" { ... }
    let prompt_variant = if req.prompt_variant.is_empty() {
        "balanced".to_string()
    } else {
        req.prompt_variant
    };

    // 2. Create strategy engine
    // Corresponding Go: engine := decision.NewStrategyEngine(&req.Config)
    // We assume StrategyEngine::new takes a reference to config
    let engine = decision::strategy_engine::StrategyEngine::new(req.config.clone());

    // 3. Build system prompt
    // Corresponding Go: engine.BuildSystemPrompt(...)
    let system_prompt = engine.build_system_prompt(account_equity, &prompt_variant);

    // 4. Get available templates
    // Corresponding Go: decision.GetAllPromptTemplateNames()
    let template_names = decision::prompt_manager::get_all_prompt_template_names();

    // 5. Build Config Summary for response
    // Accessing nested fields of StrategyConfig
    let config_summary = json!({
        "coin_source":      req.config.coin_source.source_type,
        "primary_tf":       req.config.indicators.klines.primary_timeframe,
        "btc_eth_leverage": req.config.risk_control.btc_eth_max_leverage,
        "altcoin_leverage": req.config.risk_control.altcoin_max_leverage,
        "max_positions":    req.config.risk_control.max_positions,
    });

    // 6. Return Response
    Json(json!({
        "system_prompt":       system_prompt,
        "prompt_variant":      prompt_variant,
        "available_templates": template_names,
        "config_summary":      config_summary,
    }))
    .into_response()
}

// handle_get_strategies Get strategy list
pub async fn handle_get_strategies(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
) -> impl IntoResponse {
    // 1. Authorization Check
    // Axum's Extension guarantees 'user' exists if middleware passed.
    // However, if you want to strictly match Go's empty check logic:
    if user.user_id.is_empty() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "Unauthorized" })),
        )
            .into_response();
    }

    // 2. Get strategies from database
    // Corresponding Go: s.store.Strategy().List(userID)
    let strategies = match state.store.strategy().list(&user.user_id).await {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to get strategy list: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("Failed to get strategy list: {:?}", e) })),
            )
                .into_response();
        }
    };

    // 3. Convert to frontend format
    // Corresponding Go: Loop + json.Unmarshal(st.Config)
    let result: Vec<Value> = strategies
        .into_iter()
        .map(|st| {
            // Parse the config JSON string into a serde_json::Value
            // If parsing fails, default to an empty object to prevent the whole request from failing
            // (This mimics Go's behavior where unmarshal errors might result in empty structs)
            let config_obj: Value = serde_json::from_str(&st.config).unwrap_or(json!({}));

            json!({
                "id":          st.id,
                "name":        st.name,
                "description": st.description,
                "is_active":   st.is_active,
                "is_default":  st.is_default,
                "config":      config_obj, // Nested JSON object
                "created_at":  st.created_at,
                "updated_at":  st.updated_at,
            })
        })
        .collect();

    // 4. Return response
    // Corresponding Go: c.JSON(http.StatusOK, gin.H{"strategies": result})
    Json(json!({
        "strategies": result
    }))
    .into_response()
}
