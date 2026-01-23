use std::collections::HashMap;
use std::path::PathBuf;

use crate::server::AppState;
use crate::server::AuthUser;
use anyhow::{Context, Result, anyhow, bail};
use axum::Extension;
use axum::Json;
use axum::body::Body;
use axum::extract::Query;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::header;
use axum::response::IntoResponse;
use axum::response::Response;
use backtest::config::BacktestConfig;
use backtest::storage::load_decision_records;
use backtest::types::RunMetadata;
use backtest::types::StatusPayload;
use chrono::Utc;
use decision::prompt_manager::get_prompt_template;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::json;
use tokio::fs::File;
use tokio_util::io::ReaderStream;

struct DeleteGuard(PathBuf);

impl Drop for DeleteGuard {
    fn drop(&mut self) {
        // Attempt to remove the file. We use std::fs here because Drop is synchronous.
        // For small metadata operations, this is acceptable.
        if let Err(e) = std::fs::remove_file(&self.0) {
            tracing::warn!("Failed to delete temporary export file {:?}: {}", self.0, e);
        } else {
            tracing::info!("Deleted temporary export file {:?}", self.0);
        }
    }
}

#[derive(Deserialize)]
pub struct LabelRequest {
    pub run_id: String,
    pub label: String,
}

#[derive(Deserialize)]
pub struct RunIdRequest {
    pub run_id: String,
}

#[derive(Deserialize)]
pub struct BacktestStartRequest {
    pub config: BacktestConfig,
}

// handle_backtest_start
pub async fn handle_backtest_start(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Json(req): Json<BacktestStartRequest>,
) -> impl IntoResponse {
    // 1. Check Backtest Manager Availability
    if state.backtest_manager.is_none() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "backtest manager unavailable" })),
        )
            .into_response();
    }
    let manager = state.backtest_manager.clone().unwrap();

    let mut cfg = req.config;

    // 2. RunID Logic
    if cfg.run_id.trim().is_empty() {
        cfg.run_id = format!("bt_{}", Utc::now().format("%Y%m%d_%H%M%S"));
    }

    // 3. Prompt Template Logic
    let trimmed_tmpl = cfg.prompt_template.trim();
    cfg.prompt_template = if trimmed_tmpl.is_empty() {
        "default".to_string()
    } else {
        trimmed_tmpl.to_string()
    };

    // Validate Template
    if get_prompt_template(&cfg.prompt_template).is_err() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": format!("Prompt template does not exist: {}", cfg.prompt_template) })),
        ).into_response();
    }

    // 5. Set UserID
    // Corresponding Go: normalizeUserID(...)
    // We assume the middleware provides a normalized ID
    cfg.user_id = user.user_id.clone();

    // 6. Hydrate AI Config
    // Corresponding Go: s.hydrateBacktestAIConfig(&cfg)
    if let Err(e) = hydrate_backtest_ai_config(&state, &mut cfg).await {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response();
    }

    // 7. Start Backtest
    // Corresponding Go: s.backtestManager.Start(context.Background(), cfg)
    // Note: In Rust, async context is implicit.
    match manager.start(cfg).await {
        Ok(runner) => {
            // Get metadata from the runner instance
            let meta = runner.current_metadata().await.unwrap();
            (StatusCode::OK, Json(meta)).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

// hydrate_backtest_ai_config
// Populates the BacktestConfig with sensitive AI credentials from the database.
pub async fn hydrate_backtest_ai_config(state: &AppState, cfg: &mut BacktestConfig) -> Result<()> {
    // 1. Basic Nil/Ready Checks
    // In Rust, 'cfg' cannot be nil if passed by reference.
    // 'state.store' availability depends on how you structure AppState.
    // If it's wrapped in Option, check it here. Assuming standard setup:
    // if state.store_is_not_ready() { return Err(...) }

    // 2. Normalize Inputs
    // cfg.user_id = normalize_user_id(&cfg.user_id); // Assuming helper exists, or just trim:
    cfg.user_id = cfg.user_id.trim().to_string();
    let model_id_input = cfg.ai_model_id.trim().to_string();

    // 3. Retrieve Model from Store
    let model = if !model_id_input.is_empty() {
        // Case A: Model ID provided
        state
            .store
            .ai_model()
            .await
            .get(&cfg.user_id, &model_id_input)
            .await
            .context(format!("Failed to load AI model: {}", model_id_input))?
    } else {
        // Case B: No ID provided, fetch default
        let default_model = state
            .store
            .ai_model()
            .await
            .get_default(&cfg.user_id)
            .await
            .context("No available AI model found")?;

        // Update the config with the found ID
        cfg.ai_model_id = default_model.id.clone();
        default_model
    };

    // 4. Validate Model Status
    if !model.enabled {
        return Err(anyhow!("AI model {} is not enabled yet", model.name));
    }

    let api_key = model.api_key.trim();
    if api_key.is_empty() {
        return Err(anyhow!(
            "AI model {} is missing API Key, please configure it in the system first",
            model.name
        ));
    }

    // 5. Populate AI Config
    cfg.ai_cfg.provider = model.provider.trim().to_lowercase();
    cfg.ai_cfg.api_key = api_key.to_string();

    // Handle Option<String> for nullable DB fields
    cfg.ai_cfg.base_url = model.custom_api_url.trim().to_string();

    let db_custom_model_name = model.custom_model_name.trim();

    // If config doesn't specify a model, use the one from DB
    if cfg.ai_cfg.model.trim().is_empty() {
        cfg.ai_cfg.model = db_custom_model_name.to_string();
    }

    // Final trim ensure
    cfg.ai_cfg.model = cfg.ai_cfg.model.trim().to_string();

    // 6. Provider-Specific Validation (e.g., "custom")
    if cfg.ai_cfg.provider == "custom" {
        if cfg.ai_cfg.base_url.is_empty() {
            return Err(anyhow!("Custom AI model requires API URL configuration"));
        }
        if cfg.ai_cfg.model.is_empty() {
            return Err(anyhow!("Custom AI model requires model name configuration"));
        }
    }

    Ok(())
}

pub async fn ensure_backtest_run_ownership(
    state: &AppState,
    run_id: &str,
    user_id: &str,
) -> Result<RunMetadata> {
    // 1. Check Manager Availability
    let manager = state
        .backtest_manager
        .as_ref()
        .ok_or_else(|| anyhow!("backtest manager unavailable"))?;

    // 2. Load Metadata
    let meta = manager.load_metadata(run_id).await?;

    // 3. Check Admin/System Bypass
    // Corresponding Go: if userID == "" || userID == "admin"
    if user_id.is_empty() || user_id == "admin" {
        return Ok(meta);
    }

    // 4. Check Owner
    if meta.user_id.is_none() {
        return Ok(meta);
    }

    let owner = meta.user_id.clone().unwrap();
    if owner.is_empty() {
        return Ok(meta);
    }

    // Verify ownership
    if owner != user_id {
        // We return a specific error string so the caller can map it to 403 Forbidden if needed
        bail!("Forbidden: You do not own this backtest run");
    }

    Ok(meta)
}

fn normalize_user_id(id: &str) -> String {
    let id = id.trim();
    if id == "" {
        return "default".to_string();
    }

    id.to_string()
}

pub async fn handle_backtest_control<F, Fut>(
    state: AppState,
    user: AuthUser,
    req: RunIdRequest,
    action: F,
) -> impl IntoResponse
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    // 1. Check Manager Availability
    let manager = match state.backtest_manager.as_ref() {
        Some(m) => m,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "backtest manager unavailable" })),
            )
                .into_response();
        }
    };

    // 2. Validate Input
    if req.run_id.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "run_id is required" })),
        )
            .into_response();
    }

    let user_id = user.user_id.trim();

    // 3. Verify Ownership
    if let Err(e) = ensure_backtest_run_ownership(&state, &req.run_id, user_id).await {
        // Map access errors (Forbidden vs generic error)
        let err_msg = e.to_string();
        let status = if err_msg.contains("Forbidden") {
            StatusCode::FORBIDDEN
        } else {
            StatusCode::BAD_REQUEST
        };
        return (status, Json(json!({ "error": err_msg }))).into_response();
    }

    // 4. Execute the specific action (Stop, Pause, etc.)
    // Corresponding Go: if err := fn(req.RunID); err != nil
    if let Err(e) = action(req.run_id.clone()).await {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response();
    }

    // 5. Load Metadata for response
    // Corresponding Go: meta, err := s.backtestManager.LoadMetadata(req.RunID)
    match manager.load_metadata(&req.run_id).await {
        Ok(meta) => {
            // Success with metadata
            Json(meta).into_response()
        }
        Err(_) => (StatusCode::OK, Json(json!({ "message": "ok" }))).into_response(),
    }
}

// handle_backtest_pause
pub async fn handle_backtest_pause(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Json(req): Json<RunIdRequest>,
) -> impl IntoResponse {
    // We capture a clone of the manager reference to use inside the closure
    let manager_opt = state.backtest_manager.clone();

    // Call the generic helper function
    handle_backtest_control(state, user, req, move |run_id| async move {
        // This closure corresponds to 's.backtestManager.Pause'
        if let Some(manager) = manager_opt {
            // Assuming manager.pause(&str) returns anyhow::Result<()>
            manager.pause(&run_id).await
        } else {
            // This case is theoretically handled inside handle_backtest_control checks,
            // but required here for type safety.
            Err(anyhow!("backtest manager unavailable"))
        }
    })
    .await
}

pub async fn handle_backtest_resume(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Json(req): Json<RunIdRequest>,
) -> impl IntoResponse {
    let manager_opt = state.backtest_manager.clone();

    // Call the generic helper function
    handle_backtest_control(state, user, req, move |run_id| async move {
        // This closure corresponds to 's.backtestManager.Pause'
        if let Some(manager) = manager_opt {
            // Assuming manager.pause(&str) returns anyhow::Result<()>
            manager.resume(&run_id).await
        } else {
            // This case is theoretically handled inside handle_backtest_control checks,
            // but required here for type safety.
            Err(anyhow!("backtest manager unavailable"))
        }
    })
    .await
}

pub async fn handle_backtest_stop(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Json(req): Json<RunIdRequest>,
) -> impl IntoResponse {
    let manager_opt = state.backtest_manager.clone();

    // Call the generic helper function
    handle_backtest_control(state, user, req, move |run_id| async move {
        // This closure corresponds to 's.backtestManager.Pause'
        if let Some(manager) = manager_opt {
            // Assuming manager.pause(&str) returns anyhow::Result<()>
            manager.stop(&run_id).await
        } else {
            // This case is theoretically handled inside handle_backtest_control checks,
            // but required here for type safety.
            Err(anyhow!("backtest manager unavailable"))
        }
    })
    .await
}

fn query_int(params: &HashMap<String, String>, name: &str, fallback: i32) -> i32 {
    params
        .get(name) // 1. Try to get the string value
        .map(|s| s.trim()) // 2. Trim whitespace (optional but recommended)
        .and_then(|s| s.parse::<i32>().ok()) // 3. Try to parse to i32, convert Result to Option
        .unwrap_or(fallback) // 4. Return value or fallback if None
}

// handle_backtest_label
pub async fn handle_backtest_label(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Json(req): Json<LabelRequest>,
) -> impl IntoResponse {
    // 1. Check Manager Availability
    let manager = match state.backtest_manager.as_ref() {
        Some(m) => m,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "backtest manager unavailable" })),
            )
                .into_response();
        }
    };

    // 2. Validate Inputs
    let run_id = req.run_id.trim();
    if run_id.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "run_id is required" })),
        )
            .into_response();
    }

    let user_id = user.user_id.trim();

    // 3. Verify Ownership
    if let Err(e) = ensure_backtest_run_ownership(&state, run_id, user_id).await {
        // Map access errors (Forbidden vs generic error)
        let err_msg = e.to_string();
        let status = if err_msg.contains("Forbidden") {
            StatusCode::FORBIDDEN
        } else {
            StatusCode::BAD_REQUEST
        };
        return (status, Json(json!({ "error": err_msg }))).into_response();
    }

    // 4. Update Label
    match manager.update_label(run_id, &req.label).await {
        Ok(meta) => {
            // Success
            Json(meta).into_response()
        }
        Err(e) => {
            // Corresponding Go: c.JSON(http.StatusInternalServerError, ...)
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    }
}

// handle_backtest_delete
pub async fn handle_backtest_delete(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Json(req): Json<RunIdRequest>,
) -> impl IntoResponse {
    // 1. Check Manager Availability
    let manager = match state.backtest_manager.as_ref() {
        Some(m) => m,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "backtest manager unavailable" })),
            )
                .into_response();
        }
    };

    // 2. Validate Inputs
    let run_id = req.run_id.trim();
    if run_id.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "run_id is required" })),
        )
            .into_response();
    }

    let user_id = user.user_id.trim();

    // 3. Verify Ownership
    if let Err(e) = ensure_backtest_run_ownership(&state, run_id, user_id).await {
        // Map access errors (Forbidden vs generic error)
        let err_msg = e.to_string();
        let status = if err_msg.contains("Forbidden") {
            StatusCode::FORBIDDEN
        } else {
            StatusCode::BAD_REQUEST
        };
        return (status, Json(json!({ "error": err_msg }))).into_response();
    }

    // 4. Delete Backtest Run
    if let Err(e) = manager.delete(run_id).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response();
    }

    // 5. Return Success
    (StatusCode::OK, Json(json!({ "message": "deleted" }))).into_response()
}

// handle_backtest_status
pub async fn handle_backtest_status(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    // 1. Check Manager Availability
    let manager = match state.backtest_manager.as_ref() {
        Some(m) => m,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "backtest manager unavailable" })),
            )
                .into_response();
        }
    };

    // 2. Validate Inputs
    let user_id = user.user_id.trim();

    let run_id = match params.get("run_id") {
        Some(id) if !id.trim().is_empty() => id,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "run_id is required" })),
            )
                .into_response();
        }
    };

    // 3. Verify Ownership & Get Metadata
    let meta = match ensure_backtest_run_ownership(&state, run_id, user_id).await {
        Ok(m) => m,
        Err(e) => {
            // Corresponding Go: writeBacktestAccessError(c, err)
            // Map "Forbidden" to 403, others to 400/404
            let err_msg = e.to_string();
            let status = if err_msg.contains("Forbidden") {
                StatusCode::FORBIDDEN
            } else {
                StatusCode::BAD_REQUEST
            };
            return (status, Json(json!({ "error": err_msg }))).into_response();
        }
    };

    // 4. Check for Active Status (In-Memory)
    if let Some(active_status) = manager.status(run_id).await {
        return Json(active_status).into_response();
    }

    // 5. Construct Fallback Payload from Metadata
    let payload = StatusPayload {
        run_id: meta.run_id.clone(),
        state: meta.state.clone(),
        progress_pct: meta.summary.progress_pct,
        processed_bars: meta.summary.processed_bars as i32,
        current_time: 0,
        decision_cycle: meta.summary.processed_bars as i32,
        equity: meta.summary.equity_last,
        unrealized_pnl: 0.0,
        realized_pnl: 0.0,
        note: meta.summary.liquidation_note.clone(),
        // Rust chrono to RFC3339 string
        last_updated_iso: meta.updated_at.to_rfc3339(),
        last_error: None,
    };

    Json(payload).into_response()
}

// handle_backtest_runs
pub async fn handle_backtest_runs(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    // 1. Check Manager Availability
    let manager = match state.backtest_manager.as_ref() {
        Some(m) => m,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "backtest manager unavailable" })),
            )
                .into_response();
        }
    };

    // 2. User Context & Permissions
    let raw_user_id = user.user_id.trim();

    // Logic: If user is not empty and not admin, we restrict results to their own runs
    let filter_by_user = !raw_user_id.is_empty() && raw_user_id != "admin";

    // 3. List Runs
    let metas = match manager.list_runs().await {
        Ok(m) => m,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response();
        }
    };

    // 4. Parse Query Parameters
    // Corresponding Go: stateFilter := strings.ToLower(...)
    let state_filter = params
        .get("state")
        .map(|s| s.trim().to_lowercase())
        .unwrap_or_default();

    // Corresponding Go: search := strings.ToLower(...)
    let search = params
        .get("search")
        .map(|s| s.trim().to_lowercase())
        .unwrap_or_default();

    // Helper for int parsing (similar to Go's queryInt)
    let parse_usize = |key: &str, default: usize| -> usize {
        params
            .get(key)
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(default)
    };

    // Corresponding Go: limit := queryInt(c, "limit", 50) ...
    let mut limit = parse_usize("limit", 50);
    if limit == 0 {
        limit = 50;
    }

    // Corresponding Go: offset := queryInt(c, "offset", 0)
    let offset = parse_usize("offset", 0);

    // 5. Apply Filters
    // We collect references to the original metadata to avoid cloning strings unnecessarily
    let filtered: Vec<_> = metas
        .iter()
        .filter(|meta| {
            // Filter by State
            // Corresponding Go: if stateFilter != "" && !strings.EqualFold(...)
            if !state_filter.is_empty() && meta.state.as_str().to_lowercase() != state_filter {
                return false;
            }

            // Filter by Search String
            if !search.is_empty() {
                let target = format!(
                    "{} {} {} {}",
                    meta.run_id,
                    meta.summary.decision_tf, // Assuming this field exists
                    meta.label.clone().unwrap_or_default(),
                    meta.last_error.clone().unwrap_or_default()
                )
                .to_lowercase();

                if !target.contains(&search) {
                    return false;
                }
            }

            // Filter by User Ownership
            // Corresponding Go: if filterByUser ...
            if filter_by_user {
                let owner = meta.user_id.clone().unwrap_or_default().trim().to_string();
                // Go: if owner != "" && owner != userID { continue }
                // Logic: If run has an owner, and that owner isn't me, skip it.
                if !owner.is_empty() && owner != raw_user_id {
                    return false;
                }
            }

            true
        })
        .collect();

    // 6. Pagination Logic
    let total = filtered.len();

    // Calculate safe slice bounds (Go: filtered[start:end])
    let start = offset.min(total);
    let end = (offset + limit).min(total);

    // Slice the vector. Axum/Serde will serialize the slice as a JSON array.
    let page = &filtered[start..end];

    // 7. Return Response
    Json(json!({
        "total": total,
        "items": page,
    }))
    .into_response()
}

// handle_backtest_equity
pub async fn handle_backtest_equity(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    // 1. Check Manager Availability
    let manager = match state.backtest_manager.as_ref() {
        Some(m) => m,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "backtest manager unavailable" })),
            )
                .into_response();
        }
    };

    // 2. Validate Run ID
    let run_id = match params.get("run_id") {
        Some(id) if !id.trim().is_empty() => id,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "run_id is required" })),
            )
                .into_response();
        }
    };

    let user_id = user.user_id.trim();

    // 3. Verify Ownership
    if let Err(e) = ensure_backtest_run_ownership(&state, run_id, user_id).await {
        // Map access errors (Forbidden vs generic error)
        let err_msg = e.to_string();
        let status = if err_msg.contains("Forbidden") {
            StatusCode::FORBIDDEN
        } else {
            StatusCode::BAD_REQUEST
        };
        return (status, Json(json!({ "error": err_msg }))).into_response();
    }

    // 4. Parse Query Parameters
    let timeframe = params.get("tf").map(|s| s.as_str()).unwrap_or("");

    // Corresponding Go: limit := queryInt(c, "limit", 1000)
    let limit = params
        .get("limit")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(1000);

    // 5. Load Equity Data
    // Corresponding Go: s.backtestManager.LoadEquity(runID, timeframe, limit)
    match manager.load_equity(run_id, timeframe, limit as usize).await {
        Ok(points) => {
            // Return JSON response
            Json(points).into_response()
        }
        Err(e) => {
            // Corresponding Go: c.JSON(http.StatusBadRequest, ...)
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    }
}

// handle_backtest_trades
pub async fn handle_backtest_trades(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    // 1. Check Manager Availability
    let manager = match state.backtest_manager.as_ref() {
        Some(m) => m,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "backtest manager unavailable" })),
            )
                .into_response();
        }
    };

    // 2. Validate Run ID
    let run_id = match params.get("run_id") {
        Some(id) if !id.trim().is_empty() => id,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "run_id is required" })),
            )
                .into_response();
        }
    };

    let user_id = user.user_id.trim();

    // 3. Verify Ownership
    if let Err(e) = ensure_backtest_run_ownership(&state, run_id, user_id).await {
        // Map access errors (Forbidden vs generic error)
        let err_msg = e.to_string();
        let status = if err_msg.contains("Forbidden") {
            StatusCode::FORBIDDEN
        } else {
            StatusCode::BAD_REQUEST
        };
        return (status, Json(json!({ "error": err_msg }))).into_response();
    }

    // 4. Parse Query Parameters (Limit)
    // Corresponding Go: limit := queryInt(c, "limit", 1000)
    let limit = params
        .get("limit")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(1000);

    // 5. Load Trades
    match manager.load_trades(run_id, limit as usize).await {
        Ok(events) => {
            // Return JSON response
            Json(events).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

// handle_backtest_metrics
pub async fn handle_backtest_metrics(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    // 1. Check Manager Availability
    let manager = match state.backtest_manager.as_ref() {
        Some(m) => m,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "backtest manager unavailable" })),
            )
                .into_response();
        }
    };

    // 2. Normalize User ID
    // Corresponding Go: userID := normalizeUserID(...)
    // Assuming simple trim, or use a helper function if you have complex normalization logic
    let user_id = user.user_id.trim();

    // 3. Get Run ID
    // Corresponding Go: runID := c.Query("run_id")
    let run_id = match params.get("run_id") {
        Some(id) if !id.trim().is_empty() => id.trim(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "run_id is required" })),
            )
                .into_response();
        }
    };

    // 4. Verify Ownership
    // Corresponding Go: s.ensureBacktestRunOwnership(runID, userID)
    if let Err(e) = ensure_backtest_run_ownership(&state, run_id, user_id).await {
        // Map access errors (Forbidden vs generic error) to match writeBacktestAccessError logic
        let err_msg = e.to_string();
        let status = if err_msg.contains("Forbidden") {
            StatusCode::FORBIDDEN
        } else {
            StatusCode::BAD_REQUEST
        };
        return (status, Json(json!({ "error": err_msg }))).into_response();
    }

    // 5. Get Metrics
    // Corresponding Go: s.backtestManager.GetMetrics(runID)
    match manager.get_metrics(run_id).await {
        Ok(metrics) => {
            // Success
            Json(metrics).into_response()
        }
        Err(e) => {
            // 6. Handle "Not Ready" Logic
            // Corresponding Go: if errors.Is(err, sql.ErrNoRows) || errors.Is(err, os.ErrNotExist)

            // In Rust (assuming 'e' is an anyhow::Error or Box<dyn Error>),
            // we check if the underlying error is a "Not Found" type.
            let is_not_found = {
                // Check if it's a SQLx RowNotFound error
                if let Some(sqlx_err) = e.downcast_ref::<sqlx::Error>() {
                    matches!(sqlx_err, sqlx::Error::RowNotFound)
                }
                // Check if it's an IO NotFound error
                else if let Some(io_err) = e.downcast_ref::<std::io::Error>() {
                    io_err.kind() == std::io::ErrorKind::NotFound
                }
                // Fallback: Check string representation
                else {
                    let msg = e.to_string().to_lowercase();
                    msg.contains("not found") || msg.contains("no rows")
                }
            };

            if is_not_found {
                // Return 202 Accepted to indicate calculation is pending or data not yet persisted
                return (
                    StatusCode::ACCEPTED,
                    Json(json!({ "error": "metrics not ready yet" })),
                )
                    .into_response();
            }

            // General Error
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    }
}

// handle_backtest_trace
pub async fn handle_backtest_trace(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    // 1. Check Manager Availability
    let manager = match state.backtest_manager.as_ref() {
        Some(m) => m,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "backtest manager unavailable" })),
            )
                .into_response();
        }
    };

    // 2. Validate Run ID
    let run_id = match params.get("run_id") {
        Some(id) if !id.trim().is_empty() => id.trim(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "run_id is required" })),
            )
                .into_response();
        }
    };

    let user_id = user.user_id.trim();

    // 3. Verify Ownership
    if let Err(e) = ensure_backtest_run_ownership(&state, run_id, user_id).await {
        let err_msg = e.to_string();
        let status = if err_msg.contains("Forbidden") {
            StatusCode::FORBIDDEN
        } else {
            StatusCode::BAD_REQUEST
        };
        return (status, Json(json!({ "error": err_msg }))).into_response();
    }

    // 4. Parse Cycle Parameter
    let cycle = params
        .get("cycle")
        .and_then(|s| s.parse::<i32>().ok())
        .unwrap_or(0);

    // 5. Get Trace Record
    // Corresponding Go: s.backtestManager.GetTrace(runID, cycle)
    match manager.get_trace(run_id, cycle).await {
        Ok(record) => Json(record).into_response(),
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

// handle_backtest_decisions
pub async fn handle_backtest_decisions(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    // 1. Check Manager Availability
    // Corresponding Go: if s.backtestManager == nil
    if state.backtest_manager.is_none() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "backtest manager unavailable" })),
        )
            .into_response();
    }

    // 2. Validate Inputs
    // Corresponding Go: runID := c.Query("run_id")
    let run_id = match params.get("run_id") {
        Some(id) if !id.trim().is_empty() => id.trim(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "run_id is required" })),
            )
                .into_response();
        }
    };

    let user_id = user.user_id.trim();

    // 3. Verify Ownership
    // Corresponding Go: s.ensureBacktestRunOwnership(runID, userID)
    if let Err(e) = ensure_backtest_run_ownership(&state, run_id, user_id).await {
        // Map access errors
        let err_msg = e.to_string();
        let status = if err_msg.contains("Forbidden") {
            StatusCode::FORBIDDEN
        } else {
            StatusCode::BAD_REQUEST
        };
        return (status, Json(json!({ "error": err_msg }))).into_response();
    }

    // 4. Pagination Logic
    // Corresponding Go: limit := queryInt(c, "limit", 20)
    let mut limit = params
        .get("limit")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(20);

    // Corresponding Go: offset := queryInt(c, "offset", 0)
    let mut offset = params
        .get("offset")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);

    // Clamping logic
    if limit <= 0 {
        limit = 20;
    }
    if limit > 200 {
        limit = 200;
    }
    if offset < 0 {
        offset = 0;
    }

    // 5. Load Decision Records
    // Corresponding Go: backtest.LoadDecisionRecords(runID, limit, offset)
    match load_decision_records(run_id, limit, offset).await {
        Ok(records) => {
            // Success
            Json(records).into_response()
        }
        Err(e) => {
            // Corresponding Go: c.JSON(http.StatusInternalServerError, ...)
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    }
}

// handle_backtest_export
pub async fn handle_backtest_export(
    State(state): State<AppState>,
    Extension(user): Extension<AuthUser>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    // 1. Check Manager Availability
    let manager = match state.backtest_manager.as_ref() {
        Some(m) => m,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "backtest manager unavailable" })),
            )
                .into_response();
        }
    };

    // 2. Normalize User
    let user_id = user.user_id.trim();

    // 3. Get Run ID
    let run_id = match params.get("run_id") {
        Some(id) if !id.trim().is_empty() => id.trim(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "run_id is required" })),
            )
                .into_response();
        }
    };

    // 4. Verify Ownership
    if let Err(e) = ensure_backtest_run_ownership(&state, run_id, user_id).await {
        let err_msg = e.to_string();
        let status = if err_msg.contains("Forbidden") {
            StatusCode::FORBIDDEN
        } else {
            StatusCode::BAD_REQUEST
        };
        return (status, Json(json!({ "error": err_msg }))).into_response();
    }

    // 5. Generate Export
    let path_buf = match manager.export_run(run_id).await {
        Ok(p) => PathBuf::from(p), // Ensure it's a PathBuf
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response();
        }
    };

    // 6. Open File for Streaming
    let file = match File::open(&path_buf).await {
        Ok(f) => f,
        Err(e) => {
            // If we can't open the file, try to clean it up and return error
            let _ = std::fs::remove_file(&path_buf);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("Failed to open export file: {}", e) })),
            )
                .into_response();
        }
    };

    // 7. Create Stream with Cleanup Guard
    // Convert file to a stream
    let stream = ReaderStream::new(file);

    // Create the guard. When 'guard' is dropped, the file is deleted.
    let guard = DeleteGuard(path_buf);

    // We map the stream to move the 'guard' into the closure.
    // The guard will stay alive as long as the stream is being polled.
    let stream_with_cleanup = stream.map(move |chunk| {
        // We just reference guard to ensure it's captured by the closure
        let _ = &guard;
        chunk
    });

    // 8. Build Response
    // Corresponding Go: c.FileAttachment(path, filename)
    let filename = format!("{}_export.zip", run_id);
    let body = Body::from_stream(stream_with_cleanup);

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/zip")
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}\"", filename),
        )
        .body(body)
        .unwrap()
}
