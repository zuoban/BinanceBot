use crate::config::{AppConfig, ServerConfig};
use crate::runtime::RuntimeHandle;
use crate::server::state::AppState;
use crate::types::{
    AuthLoginPayload, AuthSetupPayload, AuthStatusResponse, AuthTokenResponse, BotControlAction,
    BotSnapshot, ChangePasswordPayload, GridOrder, LogEntry, OverviewPairView, OverviewSnapshot,
    TradeRecord, TradingMode, TradingPairView, UpdateConfigPayload, UpdateGlobalConfigPayload,
    WebConfigView, WebGlobalConfigView,
};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Extension, Json, Router};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::sync::Arc;
use tower::ServiceExt;
use tracing::{error, info, warn};

#[derive(Debug, Deserialize)]
pub struct ControlRequest {
    pub action: String,
}

#[derive(Debug, Deserialize)]
pub struct ClearTradeHistoryRequest {
    #[serde(default)]
    pub confirm: bool,
}

#[derive(Debug, serde::Serialize)]
pub struct ClearTradeHistoryResponse {
    pub deleted_trades: usize,
    pub snapshot: BotSnapshot,
}

#[derive(Debug, serde::Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub message: Option<String>,
}

fn extract_token(headers: &HeaderMap) -> Option<String> {
    if let Some(auth_val) = headers.get(axum::http::header::AUTHORIZATION) {
        if let Ok(auth_str) = auth_val.to_str() {
            if let Some(token) = auth_str
                .strip_prefix("Bearer ")
                .or_else(|| auth_str.strip_prefix("bearer "))
            {
                let t = token.trim();
                if !t.is_empty() {
                    return Some(t.to_string());
                }
            }
        }
    }

    if let Some(token_val) = headers.get("X-Auth-Token") {
        if let Ok(token_str) = token_val.to_str() {
            let t = token_str.trim();
            if !t.is_empty() {
                return Some(t.to_string());
            }
        }
    }

    None
}

async fn require_auth_middleware(
    State(state): State<Arc<AppState>>,
    req: axum::extract::Request,
    next: Next,
) -> Result<Response, (StatusCode, Json<ApiResponse<()>>)> {
    let token_opt = extract_token(req.headers());
    let is_valid = match token_opt {
        Some(ref t) => state.is_token_valid(t).await,
        None => false,
    };

    if !is_valid {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ApiResponse {
                success: false,
                data: None,
                message: Some("未授权访问：请先输入管理员密码进行身份认证".to_string()),
            }),
        ));
    }

    Ok(next.run(req).await)
}

pub fn create_router(state: Arc<AppState>) -> Router {
    let pairs = Router::new()
        .route("/api/pairs", get(get_single_pair_handler))
        .route("/api/overview", get(get_single_overview_handler))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_auth_middleware,
        ))
        .route("/api/accounts/ws", get(single_accounts_ws_handler))
        .with_state(state.clone());
    create_base_router(state, true).merge(pairs)
}

async fn get_single_pair_handler(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<Vec<TradingPairView>>> {
    get_pairs_handler(State(Arc::new(vec![state]))).await
}

async fn get_single_overview_handler(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<OverviewSnapshot>> {
    get_overview_handler(State(Arc::new(vec![state]))).await
}

fn pair_routes() -> Router<Arc<AppState>> {
    pair_resource_routes().route("/config", get(get_config_handler).post(post_config_handler))
}

fn pair_resource_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/status", get(get_status_handler))
        .route("/orders", get(get_orders_handler))
        .route("/trades", get(get_trades_handler))
        .route("/logs", get(get_logs_handler))
        .route("/control", post(post_control_handler))
}

fn create_base_router(state: Arc<AppState>, history_clear: bool) -> Router {
    let mut api_protected = Router::new()
        .route("/api/status", get(get_status_handler))
        .route("/api/orders", get(get_orders_handler))
        .route("/api/trades", get(get_trades_handler))
        .route("/api/logs", get(get_logs_handler))
        .route("/api/database/export", get(export_database_handler))
        .route(
            "/api/config",
            get(get_config_handler).post(post_config_handler),
        )
        .route("/api/control", post(post_control_handler))
        .route(
            "/api/auth/change_password",
            post(post_change_password_handler),
        );
    if history_clear {
        api_protected = api_protected.route("/api/trades/clear", post(clear_trade_history_handler));
    }
    let api_protected = api_protected.route_layer(middleware::from_fn_with_state(
        state.clone(),
        require_auth_middleware,
    ));

    public_routes()
        .route("/ws", get(ws_handler))
        .merge(api_protected)
        .with_state(state)
}

fn public_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/auth/status", get(get_auth_status_handler))
        .route("/api/auth/setup", post(post_auth_setup_handler))
        .route("/api/auth/login", post(post_auth_login_handler))
        .route("/api/auth/logout", post(post_auth_logout_handler))
}

/// Existing unscoped APIs address the first pair; scoped APIs never change another pair.
pub fn create_multi_router(states: Vec<Arc<AppState>>) -> Router {
    let primary = states
        .first()
        .expect("At least one trading pair is required")
        .clone();
    let mut router = create_base_router(primary.clone(), false);
    for state in &states {
        let symbol = state
            .pair_scope()
            .expect("Multi-pair routes require a stable symbol");
        let scoped = pair_routes()
            .route_layer(middleware::from_fn_with_state(
                state.clone(),
                require_auth_middleware,
            ))
            .route("/ws", get(ws_handler))
            .with_state(state.clone());
        router = router.nest(&format!("/api/pairs/{}", symbol), scoped);
    }
    let global = Router::new()
        .route("/api/pairs", get(get_pairs_handler))
        .route("/api/overview", get(get_overview_handler))
        .route("/api/trades/clear", post(clear_multi_trade_history_handler))
        .route_layer(middleware::from_fn_with_state(
            primary,
            require_auth_middleware,
        ))
        .route("/api/accounts/ws", get(accounts_ws_handler))
        .with_state(Arc::new(states));
    router.merge(global)
}

async fn get_pairs_handler(
    State(states): State<Arc<Vec<Arc<AppState>>>>,
) -> Json<ApiResponse<Vec<TradingPairView>>> {
    let mut pairs = Vec::with_capacity(states.len());
    for state in states.iter() {
        let config = state.config.read().await;
        pairs.push(TradingPairView {
            symbol: config.exchange.symbol.clone(),
            scoped: state.pair_scope().is_some(),
            status: *state.status.read().await,
            mode: TradingMode::from_exchange(config.exchange.dry_run, config.exchange.is_testnet),
            grid_config: config.summary(),
        });
    }
    Json(ApiResponse {
        success: true,
        data: Some(pairs),
        message: None,
    })
}

async fn get_overview_handler(
    State(states): State<Arc<Vec<Arc<AppState>>>>,
) -> Json<ApiResponse<OverviewSnapshot>> {
    let generated_at = chrono::Utc::now();
    let pairs = futures_util::future::join_all(states.iter().map(|state| async move {
        // Reuse the per-pair chart cache and in-memory live fields used by WebSockets.
        let snapshot = state.snapshot().await;
        OverviewPairView {
            scoped: state.pair_scope().is_some(),
            mode: TradingMode::from_exchange(snapshot.dry_run, snapshot.grid_config.is_testnet),
            snapshot,
        }
    }))
    .await;
    Json(ApiResponse {
        success: true,
        data: Some(OverviewSnapshot {
            generated_at,
            pairs,
        }),
        message: None,
    })
}

/// Dispatch through the current registry so newly added pairs are immediately addressable.
pub async fn create_managed_router(handle: RuntimeHandle) -> Router {
    let primary = handle.states.read().await[0].clone();
    let protected = Router::new()
        .route(
            "/api/pairs",
            get(get_managed_pairs_handler).post(add_pair_handler),
        )
        .route("/api/overview", get(get_managed_overview_handler))
        .route("/api/overview/history", get(get_overview_history_handler))
        .route("/api/pairs/:symbol", delete(remove_pair_handler))
        .route(
            "/api/pairs/:symbol/:resource",
            get(dispatch_pair).post(dispatch_pair),
        )
        .route("/api/status", get(dispatch_pair))
        .route("/api/orders", get(dispatch_pair))
        .route("/api/trades", get(dispatch_pair))
        .route("/api/logs", get(dispatch_pair))
        .route("/api/config", get(dispatch_pair).post(dispatch_pair))
        .route("/api/control", post(dispatch_pair))
        .route("/api/trades/clear", post(clear_managed_history_handler))
        .route(
            "/api/global/config",
            get(get_global_config_handler).post(post_global_config_handler),
        )
        .route(
            "/api/server/config",
            get(get_server_config_handler).post(post_server_config_handler),
        )
        .route_layer(middleware::from_fn_with_state(
            primary.clone(),
            require_auth_middleware,
        ))
        .route("/ws", get(dispatch_pair))
        .route("/api/pairs/:symbol/ws", get(dispatch_pair))
        .route("/api/accounts/ws", get(managed_accounts_ws_handler))
        .with_state(handle);
    let account = Router::new()
        .route("/api/database/export", get(export_database_handler))
        .route(
            "/api/auth/change_password",
            post(post_change_password_handler),
        )
        .route_layer(middleware::from_fn_with_state(
            primary.clone(),
            require_auth_middleware,
        ));
    public_routes()
        .merge(account)
        .with_state(primary)
        .merge(protected)
}

async fn dispatch_pair(
    State(handle): State<RuntimeHandle>,
    mut req: axum::extract::Request,
) -> Response {
    let path = req.uri().path().to_string();
    let (symbol, resource) = if let Some(scoped) = path.strip_prefix("/api/pairs/") {
        let Some((symbol, resource)) = scoped.split_once('/') else {
            return StatusCode::NOT_FOUND.into_response();
        };
        (Some(symbol), resource)
    } else {
        (None, path.strip_prefix("/api/").unwrap_or("ws"))
    };
    let states = handle.states.read().await;
    let state = match symbol {
        Some(symbol) => states
            .iter()
            .find(|state| state.pair_scope() == Some(symbol)),
        None => states.first(),
    }
    .cloned();
    drop(states);
    let Some(state) = state else {
        return StatusCode::NOT_FOUND.into_response();
    };
    // The selected router enforces each resource's HTTP methods and WebSocket authentication.
    *req.uri_mut() = format!("/{}", resource).parse().unwrap();
    pair_resource_routes()
        .route(
            "/config",
            get(get_config_handler).post(post_managed_config_handler),
        )
        .route("/ws", get(ws_handler))
        .layer(Extension(handle))
        .with_state(state)
        .oneshot(req)
        .await
        .unwrap()
}

async fn get_managed_pairs_handler(
    State(handle): State<RuntimeHandle>,
) -> Json<ApiResponse<Vec<TradingPairView>>> {
    get_pairs_handler(State(Arc::new(handle.states.read().await.clone()))).await
}

async fn get_managed_overview_handler(
    State(handle): State<RuntimeHandle>,
) -> Json<ApiResponse<OverviewSnapshot>> {
    // Release the registry lock before collecting snapshots so dashboard reads
    // do not hold up pair additions or removals during database work.
    let states = handle.states.read().await.clone();
    get_overview_handler(State(Arc::new(states))).await
}

async fn get_overview_history_handler(
    State(handle): State<RuntimeHandle>,
) -> Json<ApiResponse<Vec<crate::snapshots::HourlySnapshot>>> {
    let since = chrono::Utc::now().timestamp() - 24 * 3600;
    match handle
        .db
        .run_blocking(move |db| db.hourly_account_snapshots(since))
        .await
    {
        Ok(data) => Json(ApiResponse {
            success: true,
            data: Some(data),
            message: None,
        }),
        Err(error) => {
            tracing::warn!(%error, "Could not read account snapshots");
            Json(ApiResponse {
                success: false,
                data: None,
                message: Some("无法读取历史快照".into()),
            })
        }
    }
}

async fn add_pair_handler(
    State(handle): State<RuntimeHandle>,
    Json(payload): Json<UpdateConfigPayload>,
) -> Json<ApiResponse<String>> {
    let result = async {
        anyhow::ensure!(
            !payload.has_global_updates(),
            "连接和 Telegram 通知请在全局设置中保存"
        );
        let saved = handle
            .db
            .run_blocking(|db| db.load_config())
            .await?
            .ok_or_else(|| anyhow::anyhow!("无法读取全局配置"))?;
        let mut defaults = AppConfig::default();
        crate::config::GlobalConfig::from_app(&saved).apply_to(&mut defaults);
        let config = apply_config_payload(defaults, payload)?;
        let symbol = config.exchange.symbol.clone();
        handle.add_pair(config).await?;
        Ok::<_, anyhow::Error>(symbol)
    }
    .await;
    match result {
        Ok(symbol) => Json(ApiResponse {
            success: true,
            data: Some(symbol),
            message: Some("交易对已添加并保存，策略保持暂停；检查配置后手动恢复".into()),
        }),
        Err(error) => Json(ApiResponse {
            success: false,
            data: None,
            message: Some(error.to_string()),
        }),
    }
}

async fn post_managed_config_handler(
    State(state): State<Arc<AppState>>,
    Extension(handle): Extension<RuntimeHandle>,
    Json(payload): Json<UpdateConfigPayload>,
) -> Json<ApiResponse<WebConfigView>> {
    let _membership = handle.membership_lock.lock().await;
    post_config_handler(State(state), Json(payload)).await
}

async fn get_global_config_handler(
    State(handle): State<RuntimeHandle>,
) -> Json<ApiResponse<WebGlobalConfigView>> {
    match handle.db.run_blocking(|db| db.load_config()).await {
        Ok(Some(config)) => {
            let view = web_config_view(&config);
            Json(ApiResponse {
                success: true,
                data: Some(WebGlobalConfigView {
                    recv_window: view.recv_window,
                    sync_interval_secs: view.sync_interval_secs,
                    has_api_key: view.has_api_key,
                    has_api_secret: view.has_api_secret,
                    api_key_preview: view.api_key_preview,
                    telegram_enabled: view.telegram_enabled,
                    has_telegram_bot_token: view.has_telegram_bot_token,
                    telegram_chat_id: view.telegram_chat_id,
                }),
                message: None,
            })
        }
        _ => Json(ApiResponse {
            success: false,
            data: None,
            message: Some("无法读取全局设置".into()),
        }),
    }
}

async fn post_global_config_handler(
    State(handle): State<RuntimeHandle>,
    Json(payload): Json<UpdateGlobalConfigPayload>,
) -> Json<ApiResponse<WebGlobalConfigView>> {
    if let Err(error) = handle.update_global_config(payload).await {
        return Json(ApiResponse {
            success: false,
            data: None,
            message: Some(error.to_string()),
        });
    }
    let mut response = get_global_config_handler(State(handle)).await;
    response.0.message = Some("全局设置已保存，所有交易对立即生效".into());
    response
}

async fn remove_pair_handler(
    State(handle): State<RuntimeHandle>,
    Path(symbol): Path<String>,
    Json(payload): Json<ClearTradeHistoryRequest>,
) -> Json<ApiResponse<String>> {
    if !payload.confirm {
        return Json(ApiResponse {
            success: false,
            data: None,
            message: Some("请确认移除交易对；已有持仓和历史记录会保留".into()),
        });
    }
    match handle.remove_pair(symbol.clone()).await {
        Ok(()) => Json(ApiResponse {
            success: true,
            data: Some(symbol),
            message: Some("交易对已移除，已有持仓和历史记录保留".into()),
        }),
        Err(error) => Json(ApiResponse {
            success: false,
            data: None,
            message: Some(error.to_string()),
        }),
    }
}

async fn clear_managed_history_handler(
    State(handle): State<RuntimeHandle>,
    Json(payload): Json<ClearTradeHistoryRequest>,
) -> (StatusCode, Json<ApiResponse<ClearTradeHistoryResponse>>) {
    // Keep the set of caches stable while resetting shared history and statistics.
    let _membership = handle.membership_lock.lock().await;
    clear_multi_trade_history_handler(
        State(Arc::new(handle.states.read().await.clone())),
        Json(payload),
    )
    .await
}

#[derive(serde::Serialize)]
struct ServerConfigView {
    saved: ServerConfig,
    active: ServerConfig,
}

async fn get_server_config_handler(
    State(handle): State<RuntimeHandle>,
) -> Json<ApiResponse<ServerConfigView>> {
    match handle.db.run_blocking(|db| db.load_config()).await {
        Ok(Some(config)) => Json(ApiResponse {
            success: true,
            data: Some(ServerConfigView {
                saved: config.server,
                active: handle.active_server,
            }),
            message: None,
        }),
        _ => Json(ApiResponse {
            success: false,
            data: None,
            message: Some("无法读取服务设置".into()),
        }),
    }
}

async fn post_server_config_handler(
    State(handle): State<RuntimeHandle>,
    Json(server): Json<ServerConfig>,
) -> Json<ApiResponse<ServerConfigView>> {
    let result = handle
        .db
        .run_blocking(move |db| db.save_server_config(&server))
        .await;
    match result {
        Ok(()) => {
            let mut response = get_server_config_handler(State(handle)).await;
            response.0.message = Some("服务设置已保存，重启程序后生效".into());
            response
        }
        Err(error) => Json(ApiResponse {
            success: false,
            data: None,
            message: Some(error.to_string()),
        }),
    }
}

async fn clear_multi_trade_history_handler(
    State(states): State<Arc<Vec<Arc<AppState>>>>,
    Json(payload): Json<ClearTradeHistoryRequest>,
) -> (StatusCode, Json<ApiResponse<ClearTradeHistoryResponse>>) {
    if !payload.confirm {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse {
                success: false,
                data: None,
                message: Some("请先确认清理全部交易对和运行模式的历史成交及累计统计".into()),
            }),
        );
    }
    match AppState::clear_trade_history_for(&states).await {
        Ok(deleted_trades) => {
            // Invalidate every client's cached statistics after the shared database cleanup.
            let mut primary_snapshot = None;
            for state in states.iter() {
                state
                    .add_log(
                        "WARN",
                        format!("已清理全部历史成交记录，共 {} 笔", deleted_trades),
                    )
                    .await;
                let snapshot = state.broadcast_snapshot().await;
                if primary_snapshot.is_none() {
                    primary_snapshot = Some(snapshot);
                }
            }
            (
                StatusCode::OK,
                Json(ApiResponse {
                    success: true,
                    data: Some(ClearTradeHistoryResponse {
                        deleted_trades,
                        snapshot: primary_snapshot.unwrap(),
                    }),
                    message: Some(format!(
                        "已清理 {} 笔历史成交记录，全部交易对累计统计已重置",
                        deleted_trades
                    )),
                }),
            )
        }
        Err(error) => {
            error!("Trade history cleanup failed: {}", error);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    message: Some("历史记录清理失败，请稍后重试或查看服务器日志".into()),
                }),
            )
        }
    }
}

async fn get_auth_status_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Json<ApiResponse<AuthStatusResponse>> {
    let initialized = state
        .db
        .run_blocking(|db| db.is_admin_password_set())
        .await
        .unwrap_or(false);
    let token = extract_token(&headers);
    let authenticated = match token {
        Some(ref t) => state.is_token_valid(t).await,
        None => false,
    };

    Json(ApiResponse {
        success: true,
        data: Some(AuthStatusResponse {
            initialized,
            authenticated,
        }),
        message: None,
    })
}

async fn post_auth_setup_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<AuthSetupPayload>,
) -> (StatusCode, Json<ApiResponse<AuthTokenResponse>>) {
    let is_set = state
        .db
        .run_blocking(|db| db.is_admin_password_set())
        .await
        .unwrap_or(false);
    if is_set {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse {
                success: false,
                data: None,
                message: Some(
                    "管理员密码已初始化，不能重复设置。如需修改请在登录后操作。".to_string(),
                ),
            }),
        );
    }

    let password = payload.password.trim();
    if password.len() < 6 {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse {
                success: false,
                data: None,
                message: Some("管理员密码长度至少需为 6 位字符".to_string()),
            }),
        );
    }

    if !state.begin_login_attempt().await {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(ApiResponse {
                success: false,
                data: None,
                message: Some("初始化尝试过于频繁，请一分钟后重试".to_string()),
            }),
        );
    }
    if !state.verify_setup_code(&payload.setup_code) {
        return (
            StatusCode::FORBIDDEN,
            Json(ApiResponse {
                success: false,
                data: None,
                message: Some("初始化码不正确，请从当前应用日志中获取".to_string()),
            }),
        );
    }

    let password = password.to_string();
    match state
        .db
        .run_blocking(move |db| db.initialize_admin_password(&password))
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    message: Some("管理员密码已由其他请求初始化".to_string()),
                }),
            )
        }
        Err(e) => {
            error!("Failed to set admin password: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    message: Some(format!("保存管理员密码失败: {}", e)),
                }),
            );
        }
    }

    let token = crate::auth::generate_session_token();
    state.clear_login_attempts().await;
    let _ = state.create_session(&token).await;
    info!("Admin password successfully initialized and initial session created");

    (
        StatusCode::OK,
        Json(ApiResponse {
            success: true,
            data: Some(AuthTokenResponse { token }),
            message: Some("管理员密码设置成功并已自动登录！".to_string()),
        }),
    )
}

async fn post_auth_login_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<AuthLoginPayload>,
) -> (StatusCode, Json<ApiResponse<AuthTokenResponse>>) {
    if !state.begin_login_attempt().await {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(ApiResponse {
                success: false,
                data: None,
                message: Some("登录尝试过于频繁，请一分钟后重试".to_string()),
            }),
        );
    }
    let is_set = state
        .db
        .run_blocking(|db| db.is_admin_password_set())
        .await
        .unwrap_or(false);
    if !is_set {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse {
                success: false,
                data: None,
                message: Some("系统尚未设置管理员密码，请先完成初始密码设置".to_string()),
            }),
        );
    }

    let password = payload.password;
    match state
        .db
        .run_blocking(move |db| db.verify_admin_password(&password))
        .await
    {
        Ok(true) => {
            state.clear_login_attempts().await;
            let token = crate::auth::generate_session_token();
            let _ = state.create_session(&token).await;
            info!("Admin authentication successful");
            (
                StatusCode::OK,
                Json(ApiResponse {
                    success: true,
                    data: Some(AuthTokenResponse { token }),
                    message: Some("管理员身份认证成功！".to_string()),
                }),
            )
        }
        Ok(false) => {
            warn!("Failed admin authentication attempt: incorrect password");
            (
                StatusCode::UNAUTHORIZED,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    message: Some("管理员密码错误，请重新输入".to_string()),
                }),
            )
        }
        Err(e) => {
            error!("Database error during password verification: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    message: Some(format!("认证服务异常: {}", e)),
                }),
            )
        }
    }
}

async fn post_auth_logout_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Json<ApiResponse<()>> {
    if let Some(token) = extract_token(&headers) {
        let _ = state.delete_session(&token).await;
    }
    Json(ApiResponse {
        success: true,
        data: None,
        message: Some("已安全退出登录".to_string()),
    })
}

async fn post_change_password_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ChangePasswordPayload>,
) -> (StatusCode, Json<ApiResponse<()>>) {
    let new_password = payload.new_password.trim();
    if new_password.len() < 6 {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse {
                success: false,
                data: None,
                message: Some("新密码长度至少需为 6 位字符".to_string()),
            }),
        );
    }

    let old_password = payload.old_password;
    match state
        .db
        .run_blocking(move |db| db.verify_admin_password(&old_password))
        .await
    {
        Ok(true) => {
            let new_password = new_password.to_string();
            if let Err(e) = state
                .db
                .run_blocking(move |db| db.set_admin_password(&new_password))
                .await
            {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiResponse {
                        success: false,
                        data: None,
                        message: Some(format!("修改密码失败: {}", e)),
                    }),
                );
            }
            // Clear other sessions to enforce re-login
            let _ = state.clear_all_sessions().await;
            info!("Admin password changed successfully, all sessions cleared");
            (
                StatusCode::OK,
                Json(ApiResponse {
                    success: true,
                    data: None,
                    message: Some("管理员密码已成功修改，所有会话已重置，请重新登录".to_string()),
                }),
            )
        }
        Ok(false) => (
            StatusCode::UNAUTHORIZED,
            Json(ApiResponse {
                success: false,
                data: None,
                message: Some("原密码校验失败，请重新输入".to_string()),
            }),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse {
                success: false,
                data: None,
                message: Some(format!("系统异常: {}", e)),
            }),
        ),
    }
}

async fn get_status_handler(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<crate::types::BotSnapshot>> {
    let snapshot = state.snapshot().await;
    Json(ApiResponse {
        success: true,
        data: Some(snapshot),
        message: None,
    })
}

async fn get_orders_handler(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<Vec<GridOrder>>> {
    let orders: Vec<GridOrder> = state.active_orders.read().await.values().cloned().collect();
    Json(ApiResponse {
        success: true,
        data: Some(orders),
        message: None,
    })
}

async fn get_trades_handler(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<Vec<TradeRecord>>> {
    let config = state.config.read().await;
    let symbol = config.exchange.symbol.clone();
    let mode = TradingMode::from_exchange(config.exchange.dry_run, config.exchange.is_testnet);
    drop(config);
    let trades = match state
        .db
        .run_blocking(move |db| db.get_recent_trades_for(&symbol, mode, 100))
        .await
    {
        Ok(t) => t,
        Err(e) => {
            error!("Failed to fetch trades from SQLite: {}", e);
            state.recent_trades.read().await.iter().cloned().collect()
        }
    };
    Json(ApiResponse {
        success: true,
        data: Some(trades),
        message: None,
    })
}

async fn get_logs_handler(State(state): State<Arc<AppState>>) -> Json<ApiResponse<Vec<LogEntry>>> {
    let logs: Vec<LogEntry> = state.recent_logs.read().await.iter().cloned().collect();
    Json(ApiResponse {
        success: true,
        data: Some(logs),
        message: None,
    })
}

async fn clear_trade_history_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ClearTradeHistoryRequest>,
) -> (StatusCode, Json<ApiResponse<ClearTradeHistoryResponse>>) {
    if !payload.confirm {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse {
                success: false,
                data: None,
                message: Some("请先确认清理全部交易对和运行模式的历史成交及累计统计".into()),
            }),
        );
    }
    match state.clear_trade_history().await {
        Ok(deleted_trades) => {
            state
                .add_log(
                    "WARN",
                    format!(
                        "已清理全部历史成交记录，共 {} 笔，累计统计已重置",
                        deleted_trades
                    ),
                )
                .await;
            let snapshot = state.broadcast_snapshot().await;
            (
                StatusCode::OK,
                Json(ApiResponse {
                    success: true,
                    data: Some(ClearTradeHistoryResponse {
                        deleted_trades,
                        snapshot,
                    }),
                    message: Some(format!(
                        "已清理 {} 笔历史成交记录，累计统计已重置",
                        deleted_trades
                    )),
                }),
            )
        }
        Err(error) => {
            error!("Trade history cleanup failed: {}", error);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    message: Some("历史记录清理失败，请稍后重试或查看服务器日志".into()),
                }),
            )
        }
    }
}

async fn export_database_handler(State(state): State<Arc<AppState>>) -> Response {
    match state
        .db
        .run_blocking(|db| db.export_analysis_snapshot())
        .await
    {
        Ok(bytes) => {
            let filename = format!(
                "binancebot-analysis-{}.db",
                chrono::Utc::now()
                    .with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).unwrap())
                    .format("%Y%m%d-%H%M%S")
            );
            (
                [
                    (
                        axum::http::header::CONTENT_TYPE,
                        "application/vnd.sqlite3".to_string(),
                    ),
                    (
                        axum::http::header::CONTENT_DISPOSITION,
                        format!("attachment; filename=\"{}\"", filename),
                    ),
                    (axum::http::header::CACHE_CONTROL, "no-store".to_string()),
                    (
                        axum::http::header::X_CONTENT_TYPE_OPTIONS,
                        "nosniff".to_string(),
                    ),
                ],
                bytes,
            )
                .into_response()
        }
        Err(error) => {
            error!("Database analysis export failed: {}", error);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::<()> {
                    success: false,
                    data: None,
                    message: Some("数据库导出失败，请稍后重试或查看服务器日志".into()),
                }),
            )
                .into_response()
        }
    }
}

async fn get_config_handler(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<WebConfigView>> {
    let config = state.config.read().await;
    Json(ApiResponse {
        success: true,
        data: Some(web_config_view(&config)),
        message: None,
    })
}

fn web_config_view(config: &AppConfig) -> WebConfigView {
    let has_key = !config.exchange.api_key.trim().is_empty();
    let has_secret = !config.exchange.api_secret.trim().is_empty();

    let key_preview = if has_key {
        let k = &config.exchange.api_key;
        if k.len() > 8 {
            format!(
                "{}...{}",
                k.chars().take(4).collect::<String>(),
                k.chars()
                    .rev()
                    .take(4)
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect::<String>()
            )
        } else {
            "****".to_string()
        }
    } else {
        "未配置".to_string()
    };

    let view = WebConfigView {
        symbol: config.exchange.symbol.clone(),
        direction: config.grid.direction,
        grid_interval: config.grid.grid_interval,
        order_amount_usdc: config.grid.order_amount_usdc,
        buy_window: config.grid.buy_window,
        sell_window: config.grid.sell_window,
        post_only: config.grid.post_only,
        dry_run: config.exchange.dry_run,
        is_testnet: config.exchange.is_testnet,
        recv_window: config.exchange.recv_window,
        sync_interval_secs: config.exchange.sync_interval_secs,
        has_api_key: has_key,
        has_api_secret: has_secret,
        api_key_preview: key_preview,
        telegram_enabled: config.telegram.enabled,
        has_telegram_bot_token: !config.telegram.bot_token.trim().is_empty(),
        telegram_chat_id: config.telegram.chat_id.clone(),
        min_price: config.grid.min_price,
        max_price: config.grid.max_price,
        max_position_usdc: config.grid.max_position_usdc,
        pause_on_max_position: config.grid.pause_on_max_position,
    };

    view
}

fn apply_config_payload(
    mut current_config: AppConfig,
    payload: UpdateConfigPayload,
) -> anyhow::Result<AppConfig> {
    current_config.exchange.symbol = payload.symbol.trim().to_uppercase();
    if let Some(direction) = payload.direction {
        current_config.grid.direction = direction;
    }
    current_config.grid.grid_interval = payload.grid_interval;
    current_config.grid.order_amount_usdc = payload.order_amount_usdc;
    current_config.grid.buy_window = payload.buy_window;
    current_config.grid.sell_window = payload.sell_window;
    if let Some(post_only) = payload.post_only {
        current_config.grid.post_only = post_only;
    }
    if let Some(dry_run) = payload.dry_run {
        current_config.exchange.dry_run = dry_run;
    }
    if let Some(is_testnet) = payload.is_testnet {
        current_config.exchange.is_testnet = is_testnet;
    }
    if let Some(value) = payload.recv_window {
        current_config.exchange.recv_window = value;
    }
    if let Some(value) = payload.sync_interval_secs {
        current_config.exchange.sync_interval_secs = value;
    }
    if payload.clear_api_credentials {
        current_config.exchange.api_key.clear();
        current_config.exchange.api_secret.clear();
    }
    if payload.clear_telegram_credentials {
        current_config.telegram.bot_token.clear();
        current_config.telegram.chat_id.clear();
    }
    current_config.grid.min_price = payload.min_price;
    current_config.grid.max_price = payload.max_price;
    current_config.grid.max_position_usdc = payload.max_position_usdc;
    if let Some(enabled) = payload.pause_on_max_position {
        current_config.grid.pause_on_max_position = enabled;
    }

    if let Some(key) = payload.api_key.filter(|_| !payload.clear_api_credentials) {
        let key_trimmed = key.trim().to_string();
        if !key_trimmed.is_empty() {
            current_config.exchange.api_key = key_trimmed;
        }
    }

    if let Some(secret) = payload
        .api_secret
        .filter(|_| !payload.clear_api_credentials)
    {
        let secret_trimmed = secret.trim().to_string();
        if !secret_trimmed.is_empty() {
            current_config.exchange.api_secret = secret_trimmed;
        }
    }

    if let Some(enabled) = payload.telegram_enabled {
        current_config.telegram.enabled = enabled;
    }
    if let Some(token) = payload
        .telegram_bot_token
        .filter(|_| !payload.clear_telegram_credentials)
    {
        let token = token.trim();
        if !token.is_empty() {
            current_config.telegram.bot_token = token.to_string();
        }
    }
    if let Some(chat_id) = payload
        .telegram_chat_id
        .filter(|_| !payload.clear_telegram_credentials)
    {
        let chat_id = chat_id.trim();
        current_config.telegram.chat_id = chat_id.to_string();
    }
    current_config.validate()?;
    Ok(current_config)
}

async fn post_config_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<UpdateConfigPayload>,
) -> Json<ApiResponse<WebConfigView>> {
    if state.pair_scope().is_some() && payload.has_global_updates() {
        return Json(ApiResponse {
            success: false,
            data: None,
            message: Some("连接和 Telegram 通知请在全局设置中保存".into()),
        });
    }
    let current_config = match apply_config_payload(state.config.read().await.clone(), payload) {
        Ok(config) => config,
        Err(error) => {
            return Json(ApiResponse {
                success: false,
                data: None,
                message: Some(error.to_string()),
            })
        }
    };
    if let Some(symbol) = state.pair_scope() {
        if symbol != current_config.exchange.symbol {
            return Json(ApiResponse {
                success: false,
                data: None,
                message: Some("交易对不能直接重命名，请在页面中添加新交易对".into()),
            });
        }
    }

    // The engine applies the change and persists it only after the exchange
    // transition succeeds. Wait for its result before reporting success.
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    if let Err(e) = state
        .action_tx
        .send(BotControlAction::UpdateConfig {
            config: Box::new(current_config.clone()),
            reply: reply_tx,
        })
        .await
    {
        error!("Failed to notify strategy engine of config update: {}", e);
        return Json(ApiResponse {
            success: false,
            data: None,
            message: Some(format!("策略更新通知失败: {}", e)),
        });
    }
    match reply_rx.await {
        Ok(Ok(())) => info!("Configuration applied and saved to {}", state.db.path()),
        Ok(Err(error)) => {
            return Json(ApiResponse {
                success: false,
                data: None,
                message: Some(error),
            })
        }
        Err(error) => {
            return Json(ApiResponse {
                success: false,
                data: None,
                message: Some(format!("策略未确认配置变更: {}", error)),
            })
        }
    }

    let mut response = get_config_handler(State(state)).await;
    response.0.message = Some("配置已保存至 SQLite 数据库并立即生效".into());
    response
}

async fn post_control_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ControlRequest>,
) -> Json<ApiResponse<String>> {
    let action_str = payload.action.to_lowercase();
    let action = match action_str.as_str() {
        "pause" => BotControlAction::Pause,
        "resume" => BotControlAction::Resume,
        "cancel_all" => BotControlAction::CancelAll,
        "rebalance" => BotControlAction::Rebalance,
        other => {
            return Json(ApiResponse {
                success: false,
                data: None,
                message: Some(format!("未知操作指令: {}", other)),
            });
        }
    };

    let action_name = format!("{:?}", action);
    if matches!(
        action,
        BotControlAction::Pause | BotControlAction::CancelAll
    ) {
        let reason = if matches!(action, BotControlAction::CancelAll) {
            "手动撤销全部策略挂单并暂停策略"
        } else {
            "手动暂停策略"
        };
        if let Err(error) = state.pause_trading_with_reason(reason).await {
            error!("Failed to persist immediate pause: {}", error);
            return Json(ApiResponse {
                success: false,
                data: None,
                message: Some(format!("暂停交易失败: {}", error)),
            });
        }
    }
    if let Err(e) = state.action_tx.send(action).await {
        error!("Failed to dispatch action: {}", e);
        return Json(ApiResponse {
            success: false,
            data: None,
            message: Some(format!("Failed to send action: {}", e)),
        });
    }

    Json(ApiResponse {
        success: true,
        data: Some(format!("Action {} triggered successfully", action_name)),
        message: None,
    })
}

fn websocket_token(headers: &HeaderMap) -> &str {
    headers
        .get(axum::http::header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        .and_then(|protocols| {
            protocols
                .split(',')
                .find_map(|protocol| protocol.trim().strip_prefix("token."))
        })
        .unwrap_or_default()
}

#[derive(Clone)]
enum AccountSource {
    Fixed(Arc<Vec<Arc<AppState>>>),
    Managed(RuntimeHandle),
}

impl AccountSource {
    async fn states(&self) -> Vec<Arc<AppState>> {
        match self {
            Self::Fixed(states) => states.as_ref().clone(),
            Self::Managed(handle) => handle.states.read().await.clone(),
        }
    }
}

async fn single_accounts_ws_handler(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Response {
    upgrade_accounts_socket(ws, headers, AccountSource::Fixed(Arc::new(vec![state]))).await
}

async fn accounts_ws_handler(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    State(states): State<Arc<Vec<Arc<AppState>>>>,
) -> Response {
    upgrade_accounts_socket(ws, headers, AccountSource::Fixed(states)).await
}

async fn managed_accounts_ws_handler(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    State(handle): State<RuntimeHandle>,
) -> Response {
    upgrade_accounts_socket(ws, headers, AccountSource::Managed(handle)).await
}

async fn upgrade_accounts_socket(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    source: AccountSource,
) -> Response {
    let token = websocket_token(&headers).to_string();
    let states = source.states().await;
    let Some(primary) = states.first().cloned() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    if !primary.is_token_valid(&token).await {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    ws.protocols(["auth"]).on_upgrade(move |socket| async move {
        let (mut sender, mut receiver) = socket.split();
        let mut ticks = tokio::time::interval(std::time::Duration::from_secs(1));
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticks.tick() => {
                    if !primary.is_token_valid(&token).await { break; }
                    // Resolve current membership without keeping the registry locked.
                    // Reuse coherent pair snapshots, but send only account fields.
                    let states = source.states().await;
                    let pairs = futures_util::future::join_all(states.iter().map(|state| async move {
                        let snapshot = state.snapshot().await;
                        serde_json::json!({
                            "mode": TradingMode::from_exchange(snapshot.dry_run, snapshot.grid_config.is_testnet),
                            "snapshot": {
                                "symbol": snapshot.symbol,
                                "dry_run": snapshot.dry_run,
                                "grid_config": { "is_testnet": snapshot.grid_config.is_testnet },
                                "position": { "unrealized_pnl": snapshot.position.unrealized_pnl },
                                "account": snapshot.account,
                            }
                        })
                    })).await;
                    let message = Message::Text(serde_json::json!({
                        "type": "accounts", "data": { "pairs": pairs }
                    }).to_string());
                    if !matches!(tokio::time::timeout(std::time::Duration::from_secs(5), sender.send(message)).await, Ok(Ok(()))) { break; }
                }
                message = receiver.next() => {
                    match message {
                        Some(Ok(Message::Ping(payload))) => {
                            if sender.send(Message::Pong(payload)).await.is_err() { break; }
                        }
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                        _ => {}
                    }
                }
            }
        }
    })
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Response {
    let token = websocket_token(&headers);
    if !state.is_token_valid(token).await {
        return (
            StatusCode::UNAUTHORIZED,
            "Unauthorized: Missing or invalid authentication token",
        )
            .into_response();
    }

    let token = token.to_string();
    ws.protocols(["auth"])
        .on_upgrade(move |socket| handle_socket(socket, state, token))
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>, token: String) {
    let (mut sender, mut receiver) = socket.split();
    let mut rx = state.ws_broadcast_tx.subscribe();

    // Send initial snapshot immediately upon connection
    let initial_snapshot = state.snapshot().await;
    if let Ok(json) = serde_json::to_string(&serde_json::json!({
        "type": "snapshot",
        "data": initial_snapshot
    })) {
        let _ = sender.send(Message::Text(json)).await;
    }

    let send_state = state.clone();
    let send_token = token.clone();
    let mut send_task = tokio::spawn(async move {
        let mut check_auth = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            tokio::select! {
                _ = check_auth.tick() => {
                    if !send_state.is_token_valid(&send_token).await { break; }
                }
                message = rx.recv() => {
                    match message {
                        Ok(msg) => {
                            if sender.send(Message::Text(msg)).await.is_err() { break; }
                        }
                        Err(_) => break,
                    }
                }
            }
        }
    });

    let state_clone = state.clone();
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            if let Message::Text(text) = msg {
                if let Ok(req) = serde_json::from_str::<ControlRequest>(&text) {
                    if !state_clone.is_token_valid(&token).await {
                        break;
                    }
                    let action = match req.action.to_lowercase().as_str() {
                        "pause" => Some(BotControlAction::Pause),
                        "resume" => Some(BotControlAction::Resume),
                        "cancel_all" => Some(BotControlAction::CancelAll),
                        "rebalance" => Some(BotControlAction::Rebalance),
                        _ => None,
                    };
                    if let Some(act) = action {
                        if matches!(act, BotControlAction::Pause | BotControlAction::CancelAll) {
                            let reason = if matches!(act, BotControlAction::CancelAll) {
                                "手动撤销全部策略挂单并暂停策略"
                            } else {
                                "手动暂停策略"
                            };
                            if state_clone.pause_trading_with_reason(reason).await.is_err() {
                                break;
                            }
                        }
                        let _ = state_clone.action_tx.send(act).await;
                    }
                }
            }
        }
    });

    // If either task exits, abort the other
    tokio::select! {
        _ = (&mut send_task) => recv_task.abort(),
        _ = (&mut recv_task) => send_task.abort(),
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use crate::db::Database;
    use crate::types::BotStatus;

    async fn overview_response(
        router: &Router,
        authenticated: bool,
    ) -> (StatusCode, serde_json::Value) {
        let mut request = axum::http::Request::builder().uri("/api/overview");
        if authenticated {
            request = request.header("Authorization", "Bearer overview-test-token");
        }
        let response = router
            .clone()
            .oneshot(request.body(axum::body::Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1_000_000)
            .await
            .unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn account_stream_authenticates_pushes_live_balances_and_rejects_revoked_sessions() {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (tx, _) = tokio::sync::mpsc::channel(1);
        let mut config = AppConfig::default();
        config.exchange.api_key = "account-stream-private-key".into();
        let state = AppState::new(config.clone(), db.clone(), tx.clone());
        let scoped = AppState::new_for_pair(
            config,
            db.clone(),
            tx,
            Arc::new(tokio::sync::Mutex::new(())),
        );
        for (router, state) in [
            (create_router(state.clone()), state),
            (create_multi_router(vec![scoped.clone()]), scoped),
        ] {
            db.create_session("account-stream-token").unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
            let url = format!("ws://{address}/api/accounts/ws");
            assert!(tokio_tungstenite::connect_async(&url).await.is_err());
            let mut request = url.into_client_request().unwrap();
            request.headers_mut().insert(
                "Sec-WebSocket-Protocol",
                "auth, token.account-stream-token".parse().unwrap(),
            );
            let (mut socket, response) = tokio_tungstenite::connect_async(request).await.unwrap();
            assert_eq!(response.headers()["Sec-WebSocket-Protocol"], "auth");
            let first = tokio::time::timeout(std::time::Duration::from_secs(3), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(!first
                .to_text()
                .unwrap()
                .contains("account-stream-private-key"));
            let first: serde_json::Value = serde_json::from_str(first.to_text().unwrap()).unwrap();
            assert_eq!(first["type"], "accounts");
            assert_eq!(first["data"]["pairs"].as_array().unwrap().len(), 1);
            assert!(first["data"]["pairs"][0]["snapshot"]["grid_config"]
                .get("api_key")
                .is_none());
            state.account.write().await.available_balance = rust_decimal_macros::dec!(1234);
            let updated = tokio::time::timeout(std::time::Duration::from_secs(3), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let updated: serde_json::Value =
                serde_json::from_str(updated.to_text().unwrap()).unwrap();
            assert_eq!(
                updated["data"]["pairs"][0]["snapshot"]["account"]["available_balance"],
                "1234"
            );
            db.delete_session("account-stream-token").unwrap();
            let closed = tokio::time::timeout(std::time::Duration::from_secs(3), socket.next())
                .await
                .unwrap();
            assert!(!matches!(
                closed,
                Some(Ok(tokio_tungstenite::tungstenite::Message::Text(_)))
            ));
            server.abort();
            let _ = server.await;
        }
    }

    #[tokio::test]
    async fn overview_authenticates_single_and_multi_pair_routes() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        db.create_session("overview-test-token").unwrap();
        let (tx, _) = tokio::sync::mpsc::channel(1);
        let mut config = AppConfig::default();
        config.exchange.api_key = "overview-private-api-key".into();
        config.exchange.api_secret = "overview-private-api-secret".into();
        config.telegram.bot_token = "overview-private-telegram-token".into();
        config.telegram.chat_id = "overview-private-chat-id".into();
        let state = AppState::new(config, db, tx);
        let router = create_router(state);
        assert_eq!(
            overview_response(&router, false).await.0,
            StatusCode::UNAUTHORIZED
        );
        let (status, response) = overview_response(&router, true).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(response["success"], true);
        assert_eq!(response["data"]["pairs"].as_array().unwrap().len(), 1);
        assert_eq!(response["data"]["pairs"][0]["scoped"], false);
        assert_eq!(
            response["data"]["pairs"][0]["snapshot"]["symbol"],
            "SOLUSDC"
        );
        let serialized = response.to_string();
        for private_value in [
            "overview-private-api-key",
            "overview-private-api-secret",
            "overview-private-telegram-token",
            "overview-private-chat-id",
        ] {
            assert!(!serialized.contains(private_value));
        }
        let summary = &response["data"]["pairs"][0]["snapshot"]["grid_config"];
        for private_field in ["api_key", "api_secret", "telegram", "bot_token", "chat_id"] {
            assert!(summary.get(private_field).is_none());
        }

        let db = Arc::new(Database::open(":memory:").unwrap());
        db.create_session("overview-test-token").unwrap();
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        let mut states = Vec::new();
        for (symbol, dry_run, is_testnet) in [("SOLUSDC", true, false), ("BTCUSDT", false, true)] {
            let mut config = AppConfig::default();
            config.exchange.symbol = symbol.into();
            config.exchange.dry_run = dry_run;
            config.exchange.is_testnet = is_testnet;
            let (tx, _) = tokio::sync::mpsc::channel(1);
            states.push(AppState::new_for_pair(config, db.clone(), tx, lock.clone()));
        }
        let account_time = chrono::Utc::now() - chrono::Duration::minutes(2);
        states[0].ticker.write().await.last_price = rust_decimal_macros::dec!(123);
        states[1].ticker.write().await.last_price = rust_decimal_macros::dec!(67890);
        states[1].account.write().await.update_time = account_time;
        states[0].add_log("INFO", "SOL scoped log").await;
        states[1].add_log("WARN", "BTC scoped log").await;
        let router = create_multi_router(states.clone());
        assert_eq!(
            overview_response(&router, false).await.0,
            StatusCode::UNAUTHORIZED
        );
        let (status, response) = overview_response(&router, true).await;
        assert_eq!(status, StatusCode::OK);
        let overview: OverviewSnapshot = serde_json::from_value(response["data"].clone()).unwrap();
        assert_eq!(overview.pairs.len(), 2);
        assert!(overview.pairs.iter().all(|pair| pair.scoped));
        assert_eq!(overview.pairs[0].mode, TradingMode::Paper);
        assert_eq!(overview.pairs[1].mode, TradingMode::Testnet);
        assert_eq!(overview.pairs[0].snapshot.symbol, "SOLUSDC");
        assert_eq!(overview.pairs[1].snapshot.symbol, "BTCUSDT");
        assert_eq!(
            overview.pairs[0].snapshot.ticker.last_price,
            rust_decimal_macros::dec!(123)
        );
        assert_eq!(
            overview.pairs[1].snapshot.ticker.last_price,
            rust_decimal_macros::dec!(67890)
        );
        assert_eq!(overview.pairs[1].snapshot.account.update_time, account_time);
        assert_eq!(
            overview.pairs[0].snapshot.recent_logs[0].message,
            "SOL scoped log"
        );
        assert_eq!(
            overview.pairs[1].snapshot.recent_logs[0].message,
            "BTC scoped log"
        );

        // No WebSocket subscribers are required for live fields to refresh.
        // Only chart queries are cached between overview polls.
        assert_eq!(db.hourly_stats_query_count(), 2);
        states[1].ticker.write().await.last_price = rust_decimal_macros::dec!(67900);
        states[1].account.write().await.update_time = chrono::Utc::now();
        let (_, response) = overview_response(&router, true).await;
        let refreshed: OverviewSnapshot = serde_json::from_value(response["data"].clone()).unwrap();
        assert_eq!(
            refreshed.pairs[1].snapshot.ticker.last_price,
            rust_decimal_macros::dec!(67900)
        );
        assert!(refreshed.pairs[1].snapshot.account.update_time > account_time);
        assert_eq!(db.hourly_stats_query_count(), 2);
    }

    #[tokio::test]
    async fn short_direction_is_saved_returned_and_preserved_for_legacy_clients() {
        use crate::types::GridDirection;
        let mut config = AppConfig::default();
        let payload = serde_json::json!({
            "symbol": "SOLUSDC", "grid_interval": "0.1", "order_amount_usdc": "100",
            "buy_window": 5, "sell_window": 5, "direction": "SHORT"
        });
        config =
            apply_config_payload(config, serde_json::from_value(payload.clone()).unwrap()).unwrap();
        assert_eq!(config.grid.direction, GridDirection::Short);
        let mut legacy = payload.clone();
        legacy.as_object_mut().unwrap().remove("direction");
        config = apply_config_payload(config, serde_json::from_value(legacy).unwrap()).unwrap();
        assert_eq!(config.grid.direction, GridDirection::Short);
        let db = Arc::new(Database::open(":memory:").unwrap());
        db.save_config(&config).unwrap();
        let saved = db.load_config().unwrap().unwrap();
        assert_eq!(saved.grid.direction, GridDirection::Short);
        let (tx, _) = tokio::sync::mpsc::channel(1);
        let state = AppState::new(saved, db, tx);
        assert_eq!(
            get_config_handler(State(state))
                .await
                .0
                .data
                .unwrap()
                .direction,
            GridDirection::Short
        );
        let mut invalid = payload;
        invalid["direction"] = serde_json::json!("BOTH");
        assert!(serde_json::from_value::<UpdateConfigPayload>(invalid).is_err());
    }

    #[tokio::test]
    async fn max_position_pause_is_saved_returned_and_preserved_for_legacy_clients() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (action_tx, mut action_rx) = tokio::sync::mpsc::channel(1);
        let state = AppState::new(AppConfig::default(), db.clone(), action_tx);
        let engine_state = state.clone();
        let engine = tokio::spawn(async move {
            while let Some(BotControlAction::UpdateConfig { config, reply }) =
                action_rx.recv().await
            {
                engine_state.db.save_config(&config).unwrap();
                *engine_state.config.write().await = *config;
                reply.send(Ok(())).unwrap();
            }
        });
        let mut payload = serde_json::json!({
            "symbol": "SOLUSDC",
            "grid_interval": "0.1",
            "order_amount_usdc": "100",
            "buy_window": 5,
            "sell_window": 5,
            "pause_on_max_position": true
        });
        let response = post_config_handler(
            State(state.clone()),
            Json(serde_json::from_value(payload.clone()).unwrap()),
        )
        .await;
        assert!(!response.0.success);
        assert!(db.load_config().unwrap().is_none());

        payload["max_position_usdc"] = serde_json::json!("1000");
        for (field, expected) in [(Some(true), true), (None, true), (Some(false), false)] {
            if let Some(enabled) = field {
                payload["pause_on_max_position"] = serde_json::json!(enabled);
            } else {
                payload
                    .as_object_mut()
                    .unwrap()
                    .remove("pause_on_max_position");
            }
            let response = post_config_handler(
                State(state.clone()),
                Json(serde_json::from_value(payload.clone()).unwrap()),
            )
            .await;
            assert!(response.0.success);
            assert_eq!(response.0.data.unwrap().pause_on_max_position, expected);
            let response = get_config_handler(State(state.clone())).await;
            assert_eq!(response.0.data.unwrap().pause_on_max_position, expected);
            assert_eq!(
                db.load_config()
                    .unwrap()
                    .unwrap()
                    .grid
                    .pause_on_max_position,
                expected
            );
        }
        engine.abort();
    }

    #[tokio::test]
    async fn pause_updates_status_before_engine_accepts_action() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (action_tx, action_rx) = tokio::sync::mpsc::channel(1);
        action_tx.send(BotControlAction::Rebalance).await.unwrap();
        let state = AppState::new(AppConfig::default(), db.clone(), action_tx);
        let request_state = state.clone();
        let request = tokio::spawn(async move {
            post_control_handler(
                State(request_state),
                Json(ControlRequest {
                    action: "pause".into(),
                }),
            )
            .await
        });

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while *state.status.read().await != BotStatus::Paused {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while db.load_bot_status().unwrap() != Some(BotStatus::Paused) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!request.is_finished());
        drop(action_rx);
        assert!(!request.await.unwrap().0.success);
    }
}
