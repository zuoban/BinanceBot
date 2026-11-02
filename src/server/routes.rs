use crate::config::{AppConfig, ServerConfig};
use crate::exchange::rate_limit::RateLimiter;
use crate::exchange::BinanceFuturesClient;
use crate::runtime::RuntimeHandle;
use crate::server::state::snapshot::AccountMessage;
use crate::server::state::AppState;
use crate::types::{
    AuthLoginPayload, AuthSetupPayload, AuthStatusResponse, AuthTokenResponse, BotControlAction,
    BotSnapshot, ChangePasswordPayload, GridOrder, LogEntry, OverviewPairView, OverviewSnapshot,
    TradeRecord, TradingMode, TradingPairView, UpdateConfigPayload, UpdateGlobalConfigPayload,
    WebConfigView, WebGlobalConfigView,
};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Extension, Json, Router};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tower::ServiceExt;
use tracing::{error, info, warn};

#[derive(Debug, Deserialize)]
pub struct ControlRequest {
    pub action: String,
    pub expected_mode: Option<TradingMode>,
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

#[derive(Debug, Deserialize)]
struct DatabaseTargetsRequest {
    targets: Vec<String>,
    #[serde(default)]
    confirm: bool,
}

#[derive(serde::Serialize)]
struct DatabaseClearResponse {
    deleted_trades: usize,
    deleted_account_hourly_snapshots: usize,
}

#[derive(Debug, serde::Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub message: Option<String>,
}

#[derive(Deserialize)]
struct SymbolsQuery {
    environment: String,
}

async fn exchange_symbols_handler(
    Extension(limiter): Extension<Arc<RateLimiter>>,
    query: Result<Query<SymbolsQuery>, axum::extract::rejection::QueryRejection>,
) -> (StatusCode, Json<ApiResponse<Vec<String>>>) {
    let is_testnet = match query {
        Ok(Query(SymbolsQuery { environment })) if environment == "mainnet" => false,
        Ok(Query(SymbolsQuery { environment })) if environment == "testnet" => true,
        _ => {
            return symbols_error(
                StatusCode::BAD_REQUEST,
                "无效的交易所环境，请使用 mainnet 或 testnet",
            )
        }
    };
    let mut config = AppConfig::default().exchange;
    config.is_testnet = is_testnet;
    fetch_exchange_symbols(&BinanceFuturesClient::new_with_limiter(&config, limiter)).await
}

fn symbols_error(
    status: StatusCode,
    message: &str,
) -> (StatusCode, Json<ApiResponse<Vec<String>>>) {
    (
        status,
        Json(ApiResponse {
            success: false,
            data: None,
            message: Some(message.to_string()),
        }),
    )
}

async fn fetch_exchange_symbols(
    client: &BinanceFuturesClient,
) -> (StatusCode, Json<ApiResponse<Vec<String>>>) {
    match client.get_exchange_info(None).await {
        Ok(info) => {
            let mut symbols: Vec<String> = info
                .symbols
                .into_iter()
                .filter(|symbol| symbol.status == "TRADING")
                .map(|symbol| symbol.symbol)
                .collect();
            symbols.sort_unstable();
            (
                StatusCode::OK,
                Json(ApiResponse {
                    success: true,
                    data: Some(symbols),
                    message: None,
                }),
            )
        }
        Err(error) => {
            warn!(%error, "获取交易所交易对失败");
            symbols_error(StatusCode::BAD_GATEWAY, "获取交易所交易对失败，请稍后重试")
        }
    }
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

fn auth_service_error<T>() -> (StatusCode, Json<ApiResponse<T>>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ApiResponse {
            success: false,
            data: None,
            message: Some("认证服务异常，请稍后重试".into()),
        }),
    )
}

async fn require_auth_middleware(
    State(state): State<Arc<AppState>>,
    req: axum::extract::Request,
    next: Next,
) -> Result<Response, (StatusCode, Json<ApiResponse<()>>)> {
    let is_valid = match extract_token(req.headers()) {
        Some(token) => match state.check_token_valid(&token).await {
            Ok(valid) => valid,
            Err(error) => {
                error!("Failed to check session: {}", error);
                return Err(auth_service_error());
            }
        },
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
        .route(
            "/api/pairs/:symbol/database/export",
            get(single_pair_export_handler),
        )
        .route(
            "/api/pairs/:symbol/trades/clear",
            post(single_pair_clear_handler),
        )
        .route("/api/database/clear", post(single_database_clear_handler))
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
    collect_pairs(std::slice::from_ref(&state)).await
}

async fn get_single_overview_handler(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<OverviewSnapshot>> {
    collect_overview(std::slice::from_ref(&state)).await
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
        .route("/database/export", get(scoped_export_handler))
        .route("/trades/clear", post(scoped_clear_handler))
}

fn create_base_router(state: Arc<AppState>, history_clear: bool) -> Router {
    let mut api_protected = Router::new()
        .route(
            "/api/exchange/symbols",
            get(exchange_symbols_handler).layer(Extension(Arc::new(RateLimiter::default()))),
        )
        .route("/api/status", get(get_status_handler))
        .route("/api/orders", get(get_orders_handler))
        .route("/api/trades", get(get_trades_handler))
        .route("/api/logs", get(get_logs_handler))
        .route(
            "/api/database/export",
            get(export_database_handler).post(export_selected_database_handler),
        )
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
        .route("/api/database/clear", post(multi_database_clear_handler))
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
    collect_pairs(&states).await
}

async fn collect_pairs(states: &[Arc<AppState>]) -> Json<ApiResponse<Vec<TradingPairView>>> {
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
    collect_overview(&states).await
}

async fn collect_overview(states: &[Arc<AppState>]) -> Json<ApiResponse<OverviewSnapshot>> {
    let generated_at = chrono::Utc::now();
    let today_pnl = match states.first() {
        Some(state) => state.db.cached_today_pnl(generated_at).await,
        None => None,
    };
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
            today_pnl,
        }),
        message: None,
    })
}

/// Dispatch through the current registry so newly added pairs are immediately addressable.
pub async fn create_managed_router(handle: RuntimeHandle) -> Router {
    let primary = handle.states.read().await[0].clone();
    let protected = Router::new()
        .route(
            "/api/exchange/symbols",
            get(exchange_symbols_handler).layer(Extension(handle.rate_limiter.clone())),
        )
        .route(
            "/api/pairs",
            get(get_managed_pairs_handler).post(add_pair_handler),
        )
        .route("/api/overview", get(get_managed_overview_handler))
        .route("/api/overview/history", get(get_overview_history_handler))
        .route("/api/pairs/:symbol", delete(remove_pair_handler))
        .route(
            "/api/pairs/:symbol/database/export",
            get(dispatch_managed_database_pair),
        )
        .route(
            "/api/pairs/:symbol/trades/clear",
            post(dispatch_managed_database_pair),
        )
        .route("/api/database/clear", post(managed_database_clear_handler))
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
        .route(
            "/api/database/export",
            get(export_database_handler).post(export_selected_database_handler),
        )
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

async fn single_pair_export_handler(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
) -> Response {
    if state.config.read().await.exchange.symbol != symbol {
        return StatusCode::NOT_FOUND.into_response();
    }
    export_symbol(&state, symbol).await
}

async fn single_pair_clear_handler(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
    payload: Json<ClearTradeHistoryRequest>,
) -> Response {
    if state.config.read().await.exchange.symbol != symbol {
        return StatusCode::NOT_FOUND.into_response();
    }
    clear_symbol(&state, symbol, payload.0)
        .await
        .into_response()
}

async fn dispatch_managed_database_pair(
    State(handle): State<RuntimeHandle>,
    Path(symbol): Path<String>,
    req: axum::extract::Request,
) -> Response {
    let _membership = handle.membership_lock.lock().await;
    let state = handle
        .states
        .read()
        .await
        .iter()
        .find(|state| state.pair_scope() == Some(symbol.as_str()))
        .cloned();
    let Some(state) = state else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let resource = if req.uri().path().ends_with("/database/export") {
        "/database/export"
    } else {
        "/trades/clear"
    };
    let mut req = req;
    *req.uri_mut() = resource.parse().unwrap();
    pair_resource_routes()
        .with_state(state)
        .oneshot(req)
        .await
        .unwrap()
}

async fn scoped_export_handler(State(state): State<Arc<AppState>>) -> Response {
    let symbol = state.config.read().await.exchange.symbol.clone();
    export_symbol(&state, symbol).await
}

async fn export_symbol(state: &AppState, symbol: String) -> Response {
    database_export_response(
        state
            .db
            .run_blocking(move |db| db.export_analysis_snapshot_for_symbol(&symbol))
            .await,
    )
}

async fn scoped_clear_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ClearTradeHistoryRequest>,
) -> (StatusCode, Json<ApiResponse<ClearTradeHistoryResponse>>) {
    let symbol = state.config.read().await.exchange.symbol.clone();
    clear_symbol(&state, symbol, payload).await
}

async fn clear_symbol(
    state: &Arc<AppState>,
    symbol: String,
    payload: ClearTradeHistoryRequest,
) -> (StatusCode, Json<ApiResponse<ClearTradeHistoryResponse>>) {
    if !payload.confirm {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse {
                success: false,
                data: None,
                message: Some("请确认清理当前币种所有模式的成交历史".into()),
            }),
        );
    }
    match state.clear_symbol_trade_history(symbol).await {
        Ok((deleted_trades, snapshot)) => (
            StatusCode::OK,
            Json(ApiResponse {
                success: true,
                data: Some(ClearTradeHistoryResponse {
                    deleted_trades,
                    snapshot,
                }),
                message: None,
            }),
        ),
        Err(error) => {
            error!("Scoped trade cleanup failed: {}", error);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    message: Some("历史记录清理失败".into()),
                }),
            )
        }
    }
}

fn selected_targets(payload: &DatabaseTargetsRequest) -> Option<(bool, bool)> {
    if payload.targets.is_empty()
        || payload
            .targets
            .iter()
            .any(|target| target != "trade_history" && target != "account_hourly_snapshots")
    {
        return None;
    }
    Some((
        payload
            .targets
            .iter()
            .any(|target| target == "trade_history"),
        payload
            .targets
            .iter()
            .any(|target| target == "account_hourly_snapshots"),
    ))
}

async fn export_selected_database_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<DatabaseTargetsRequest>,
) -> Response {
    let Some((trades, snapshots)) = selected_targets(&payload) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    database_export_response(
        state
            .db
            .run_blocking(move |db| db.export_selected_analysis_snapshot(trades, snapshots))
            .await,
    )
}

async fn single_database_clear_handler(
    State(state): State<Arc<AppState>>,
    payload: Json<DatabaseTargetsRequest>,
) -> Response {
    database_clear(&[state], payload.0).await
}

async fn multi_database_clear_handler(
    State(states): State<Arc<Vec<Arc<AppState>>>>,
    payload: Json<DatabaseTargetsRequest>,
) -> Response {
    database_clear(&states, payload.0).await
}

async fn managed_database_clear_handler(
    State(handle): State<RuntimeHandle>,
    payload: Json<DatabaseTargetsRequest>,
) -> Response {
    let membership = handle.membership_lock.clone().lock_owned().await;
    let states = handle.states.read().await.clone();
    database_clear_with_guard(&states, payload.0, Some(membership)).await
}

async fn database_clear(states: &[Arc<AppState>], payload: DatabaseTargetsRequest) -> Response {
    database_clear_with_guard(states, payload, None).await
}

async fn database_clear_with_guard(
    states: &[Arc<AppState>],
    payload: DatabaseTargetsRequest,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
) -> Response {
    let Some((trades, snapshots)) = selected_targets(&payload) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if !payload.confirm {
        return StatusCode::BAD_REQUEST.into_response();
    }
    if states.is_empty() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let (deleted_trades, deleted_account_hourly_snapshots) =
        match AppState::clear_selected_database_for(states, trades, snapshots, guard).await {
            Ok(counts) => counts,
            Err(error) => {
                error!("Database cleanup failed: {}", error);
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        };
    Json(ApiResponse {
        success: true,
        data: Some(DatabaseClearResponse {
            deleted_trades,
            deleted_account_hourly_snapshots,
        }),
        message: None,
    })
    .into_response()
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
    if states.is_empty() {
        let generated_at = chrono::Utc::now();
        return Json(ApiResponse {
            success: true,
            data: Some(OverviewSnapshot {
                generated_at,
                pairs: Vec::new(),
                today_pnl: handle.db.cached_today_pnl(generated_at).await,
            }),
            message: None,
        });
    }
    collect_overview(&states).await
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
) -> (StatusCode, Json<ApiResponse<WebConfigView>>) {
    tokio::spawn(async move {
        let _membership = handle.membership_lock.lock().await;
        if !handle
            .states
            .read()
            .await
            .iter()
            .any(|current| Arc::ptr_eq(current, &state))
        {
            return (
                StatusCode::CONFLICT,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    message: Some("交易对已被删除或替换，请刷新后重试".into()),
                }),
            );
        }
        apply_pair_config(state, payload).await
    })
    .await
    .expect("配置更新任务异常退出")
}

async fn get_global_config_handler(
    State(handle): State<RuntimeHandle>,
) -> Json<ApiResponse<WebGlobalConfigView>> {
    match handle.db.run_blocking(|db| db.load_config()).await {
        Ok(Some(config)) => {
            let global = crate::config::GlobalConfig::from_app(&config);
            Json(ApiResponse {
                success: true,
                data: Some(web_global_config_view(&config, &global)),
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
    if let Err(error) = crate::config::validate_global_position_limit(
        payload.global_max_position_notional.flatten(),
    ) {
        return Json(ApiResponse {
            success: false,
            data: None,
            message: Some(error.to_string()),
        });
    }
    match handle.update_global_config(payload).await {
        Ok(global) => {
            let mut config = AppConfig::default();
            global.apply_to(&mut config);
            let view = web_global_config_view(&config, &global);
            Json(ApiResponse {
                success: true,
                data: Some(view),
                message: Some("全局设置已保存，所有交易对立即生效".into()),
            })
        }
        Err(error) => Json(ApiResponse {
            success: false,
            data: None,
            message: Some(error.to_string()),
        }),
    }
}

fn web_global_config_view(
    config: &AppConfig,
    global: &crate::config::GlobalConfig,
) -> WebGlobalConfigView {
    let view = web_config_view(config);
    WebGlobalConfigView {
        version: crate::db::config_version(global),
        global_max_position_notional: global.global_max_position_notional,
        pause_on_zero_available_margin: global.pause_on_zero_available_margin,
        recv_window: view.recv_window,
        sync_interval_secs: view.sync_interval_secs,
        has_api_key: view.has_api_key,
        has_api_secret: view.has_api_secret,
        api_key_preview: view.api_key_preview,
        telegram_enabled: view.telegram_enabled,
        has_telegram_bot_token: view.has_telegram_bot_token,
        telegram_chat_id: view.telegram_chat_id,
    }
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
            message: Some("请确认移除交易对；移除前须平仓，历史记录会保留".into()),
        });
    }
    match handle.remove_pair(symbol.clone()).await {
        Ok(()) => Json(ApiResponse {
            success: true,
            data: Some(symbol),
            message: Some("交易对已移除，历史记录保留".into()),
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
    version: String,
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
                version: crate::db::config_version(&config.server),
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

#[derive(serde::Deserialize)]
struct UpdateServerConfigPayload {
    #[serde(flatten)]
    server: ServerConfig,
    baseline_version: Option<String>,
}

async fn post_server_config_handler(
    State(handle): State<RuntimeHandle>,
    Json(payload): Json<UpdateServerConfigPayload>,
) -> Json<ApiResponse<ServerConfigView>> {
    let server = payload.server;
    let submitted = server.clone();
    let result = handle
        .db
        .run_blocking(move |db| {
            db.save_server_config_checked(&server, payload.baseline_version.as_deref())
        })
        .await;
    match result {
        Ok(()) => Json(ApiResponse {
            success: true,
            data: Some(ServerConfigView {
                version: crate::db::config_version(&submitted),
                saved: submitted,
                active: handle.active_server,
            }),
            message: Some("服务设置已保存，重启程序后生效".into()),
        }),
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
) -> (StatusCode, Json<ApiResponse<AuthStatusResponse>>) {
    let token = extract_token(&headers);
    let result = state
        .db
        .run_blocking(move |db| {
            let initialized = db.is_admin_password_set()?;
            let authenticated = match token {
                Some(token) => db.is_session_valid(&token)?,
                None => false,
            };
            Ok(AuthStatusResponse {
                initialized,
                authenticated,
            })
        })
        .await;

    match result {
        Ok(status) => (
            StatusCode::OK,
            Json(ApiResponse {
                success: true,
                data: Some(status),
                message: None,
            }),
        ),
        Err(error) => {
            error!("Failed to check authentication status: {}", error);
            auth_service_error()
        }
    }
}

fn peer_ip(address: SocketAddr) -> IpAddr {
    match address.ip() {
        IpAddr::V6(ip) => ip
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(ip)),
        ip => ip,
    }
}
fn auth_busy<T>() -> (StatusCode, Json<ApiResponse<T>>) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ApiResponse {
            success: false,
            data: None,
            message: Some("认证服务繁忙，请稍后重试".to_string()),
        }),
    )
}

async fn post_auth_setup_handler(
    State(state): State<Arc<AppState>>,
    ConnectInfo(address): ConnectInfo<SocketAddr>,
    Json(payload): Json<AuthSetupPayload>,
) -> (StatusCode, Json<ApiResponse<AuthTokenResponse>>) {
    let Some(permit) = state.try_begin_auth_work() else {
        return auth_busy();
    };
    let is_set = match state.db.run_blocking(|db| db.is_admin_password_set()).await {
        Ok(is_set) => is_set,
        Err(error) => {
            error!("Failed to check admin initialization: {}", error);
            return auth_service_error();
        }
    };
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

    if !state.begin_login_attempt(peer_ip(address)).await {
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
    let token = crate::auth::generate_session_token();
    let session_token = token.clone();
    match state
        .db
        .run_blocking(move |db| {
            let _permit = permit;
            db.initialize_admin_session(&password, &session_token)
        })
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
                    message: Some("初始化失败，请稍后重试或查看服务器日志".into()),
                }),
            );
        }
    }

    state.clear_login_attempts(peer_ip(address)).await;
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
    ConnectInfo(address): ConnectInfo<SocketAddr>,
    Json(payload): Json<AuthLoginPayload>,
) -> (StatusCode, Json<ApiResponse<AuthTokenResponse>>) {
    let Some(permit) = state.try_begin_auth_work() else {
        return auth_busy();
    };
    if !state.begin_login_attempt(peer_ip(address)).await {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(ApiResponse {
                success: false,
                data: None,
                message: Some("登录尝试过于频繁，请一分钟后重试".to_string()),
            }),
        );
    }
    let is_set = match state.db.run_blocking(|db| db.is_admin_password_set()).await {
        Ok(is_set) => is_set,
        Err(error) => {
            error!("Failed to check admin initialization: {}", error);
            return auth_service_error();
        }
    };
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
    let token = crate::auth::generate_session_token();
    let session_token = token.clone();
    match state
        .db
        .run_blocking(move |db| {
            let _permit = permit;
            db.authenticate_and_create_session(&password, &session_token)
        })
        .await
    {
        Ok(true) => {
            state.clear_login_attempts(peer_ip(address)).await;
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
            auth_service_error()
        }
    }
}

async fn post_auth_logout_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> (StatusCode, Json<ApiResponse<()>>) {
    if let Some(token) = extract_token(&headers) {
        if let Err(error) = state.delete_session(&token).await {
            error!("Failed to delete session during logout: {}", error);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    message: Some("退出登录失败，请重试或查看服务器日志".to_string()),
                }),
            );
        }
    }
    (
        StatusCode::OK,
        Json(ApiResponse {
            success: true,
            data: None,
            message: Some("已安全退出登录".to_string()),
        }),
    )
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
    let Some(permit) = state.try_begin_auth_work() else {
        return auth_busy();
    };

    let old_password = payload.old_password;
    let new_password = new_password.to_string();
    match state
        .db
        .run_blocking(move |db| {
            let _permit = permit;
            db.change_admin_password(&old_password, &new_password)
        })
        .await
    {
        Ok(true) => {
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
            StatusCode::BAD_REQUEST,
            Json(ApiResponse {
                success: false,
                data: None,
                message: Some("原密码校验失败，请重新输入".to_string()),
            }),
        ),
        Err(error) => {
            error!("Failed to change admin password: {}", error);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    message: Some("修改密码失败，请稍后重试或查看服务器日志".into()),
                }),
            )
        }
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
    database_export_response(
        state
            .db
            .run_blocking(|db| db.export_analysis_snapshot())
            .await,
    )
}

fn database_export_response(result: anyhow::Result<Vec<u8>>) -> Response {
    match result {
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
    let mut view = web_config_view(&config);
    view.config_version = pair_config_version(&config, state.config_instance);
    Json(ApiResponse {
        success: true,
        data: Some(view),
        message: None,
    })
}

fn pair_config_version(config: &AppConfig, instance: u128) -> String {
    crate::db::config_version(&(
        instance,
        &config.exchange,
        &config.grid,
        &config.telegram,
        config.global_max_position_notional,
        config.pause_on_zero_available_margin,
    ))
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
        config_version: String::new(),
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
) -> (StatusCode, Json<ApiResponse<WebConfigView>>) {
    tokio::spawn(async move { apply_pair_config(state, payload).await })
        .await
        .expect("配置更新任务异常退出")
}

async fn apply_pair_config(
    state: Arc<AppState>,
    payload: UpdateConfigPayload,
) -> (StatusCode, Json<ApiResponse<WebConfigView>>) {
    let _update = state.config_update_lock.lock().await;
    let current = state.config.read().await.clone();
    let Some(version) = payload
        .baseline_version
        .as_deref()
        .filter(|version| !version.trim().is_empty())
    else {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse {
                success: false,
                data: None,
                message: Some("更新交易对设置时必须提供 baseline_version".into()),
            }),
        );
    };
    if version != pair_config_version(&current, state.config_instance) {
        return (
            StatusCode::CONFLICT,
            Json(ApiResponse {
                success: false,
                data: None,
                message: Some("交易对设置已被其他请求修改，请刷新后重试".into()),
            }),
        );
    }
    (
        StatusCode::OK,
        apply_pair_config_inner(state.clone(), current, payload).await,
    )
}

async fn apply_pair_config_inner(
    state: Arc<AppState>,
    current: AppConfig,
    payload: UpdateConfigPayload,
) -> Json<ApiResponse<WebConfigView>> {
    if state.pair_scope().is_some() && payload.has_global_updates() {
        return Json(ApiResponse {
            success: false,
            data: None,
            message: Some("连接和 Telegram 通知请在全局设置中保存".into()),
        });
    }
    let current_config = match apply_config_payload(current, payload) {
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

type ControlReply = tokio::sync::oneshot::Receiver<Result<(), String>>;

// A persisted pause must reach the engine before a later resume can use its
// epoch. Keep this guard alive until the pause is queued or dispatch fails.
struct PendingControlPause(Arc<AppState>);

impl PendingControlPause {
    fn new(state: &Arc<AppState>, action: &BotControlAction) -> Option<Self> {
        if matches!(
            action,
            BotControlAction::Pause | BotControlAction::CancelAll
        ) {
            state
                .pending_control_pauses
                .send_modify(|count| *count += 1);
            Some(Self(state.clone()))
        } else {
            None
        }
    }
}

impl Drop for PendingControlPause {
    fn drop(&mut self) {
        self.0
            .pending_control_pauses
            .send_modify(|count| *count -= 1);
    }
}

fn control_action(
    payload: ControlRequest,
    expected_epoch: u64,
) -> Result<(BotControlAction, Option<ControlReply>), String> {
    match payload.action.to_lowercase().as_str() {
        "pause" => Ok((BotControlAction::Pause, None)),
        "cancel_all" => Ok((BotControlAction::CancelAll, None)),
        "resume" => {
            let expected_mode = payload
                .expected_mode
                .filter(|mode| *mode != TradingMode::Unknown)
                .ok_or_else(|| "恢复策略需要提供确认时的运行模式".to_string())?;
            let (reply, receiver) = tokio::sync::oneshot::channel();
            Ok((
                BotControlAction::Resume {
                    expected_mode,
                    expected_epoch,
                    reply,
                },
                Some(receiver),
            ))
        }
        "rebalance" => {
            let expected_mode = payload
                .expected_mode
                .filter(|mode| *mode != TradingMode::Unknown)
                .ok_or_else(|| "重建网格需要提供确认时的运行模式".to_string())?;
            let (reply, receiver) = tokio::sync::oneshot::channel();
            Ok((
                BotControlAction::Rebalance {
                    expected_mode,
                    reply,
                },
                Some(receiver),
            ))
        }
        other => Err(format!("未知操作指令: {}", other)),
    }
}

async fn post_control_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ControlRequest>,
) -> Json<ApiResponse<String>> {
    let action = match control_action(payload, state.pause_epoch()) {
        Ok(action) => action,
        Err(message) => {
            return Json(ApiResponse {
                success: false,
                data: None,
                message: Some(message),
            })
        }
    };
    let result = tokio::spawn(dispatch_control(state, action)).await;
    match result {
        Ok(response) => response,
        Err(error) => Json(ApiResponse {
            success: false,
            data: None,
            message: Some(format!("控制任务失败: {}", error)),
        }),
    }
}

async fn prepare_control(state: &Arc<AppState>, action: &BotControlAction) -> Result<(), String> {
    let reason = match action {
        BotControlAction::Pause => Some("手动暂停策略"),
        BotControlAction::CancelAll => Some("手动撤销全部策略挂单并暂停策略"),
        _ => None,
    };
    let state = state.clone();
    tokio::spawn(async move {
        let _control = state.socket_control_lock.read().await;
        if state.is_retired() {
            return Err("交易对已移除，无法执行操作".into());
        }
        if let Some(reason) = reason {
            state
                .pause_trading_with_reason(reason)
                .await
                .map_err(|error| format!("暂停交易失败: {}", error))?;
        }
        Ok(())
    })
    .await
    .map_err(|error| format!("控制任务失败: {}", error))?
}

async fn dispatch_control(
    state: Arc<AppState>,
    action: (BotControlAction, Option<ControlReply>),
) -> Json<ApiResponse<String>> {
    let pending_pause = PendingControlPause::new(&state, &action.0);
    if let Err(message) = prepare_control(&state, &action.0).await {
        return Json(ApiResponse {
            success: false,
            data: None,
            message: Some(message),
        });
    }
    dispatch_prepared_control(state, action, pending_pause, None, None, None).await
}

async fn dispatch_prepared_control(
    state: Arc<AppState>,
    (action, reply): (BotControlAction, Option<ControlReply>),
    pending_pause: Option<PendingControlPause>,
    submitted: Option<tokio::sync::oneshot::Sender<()>>,
    socket_results: Option<tokio::sync::mpsc::UnboundedSender<String>>,
    socket_token: Option<String>,
) -> Json<ApiResponse<String>> {
    let action_name = format!("{:?}", action);
    let action = match action {
        BotControlAction::Pause => BotControlAction::PauseAlreadyPersisted,
        BotControlAction::CancelAll => BotControlAction::CancelAllAlreadyPersisted,
        action => action,
    };
    let mut retired = state.subscribe_retirement();
    if *retired.borrow() {
        return Json(ApiResponse {
            success: false,
            data: None,
            message: Some("交易对已移除，无法执行操作".into()),
        });
    }
    let reservation = tokio::select! {
        biased;
        _ = retired.changed() => return Json(ApiResponse { success: false, data: None, message: Some("交易对已移除，无法执行操作".into()) }),
        result = state.action_tx.reserve() => result,
    };
    let permit = match reservation {
        Ok(permit) => permit,
        Err(e) => {
            error!("Failed to dispatch action: {}", e);
            return Json(ApiResponse {
                success: false,
                data: None,
                message: Some(format!("Failed to send action: {}", e)),
            });
        }
    };
    let control = state.socket_control_lock.read().await;
    if state.is_retired() {
        return Json(ApiResponse {
            success: false,
            data: None,
            message: Some("交易对已移除，无法执行操作".into()),
        });
    }
    // Checking immediately before submission closes the channel-wait window.
    // Revocation can still race with this check after it completes; actions
    // already submitted to the engine cannot be recalled here.
    if pending_pause.is_none() {
        if let Some(token) = &socket_token {
            if !state.is_token_valid(token).await {
                return Json(ApiResponse {
                    success: false,
                    data: None,
                    message: Some("会话已失效".into()),
                });
            }
        }
        if *state.pending_control_pauses.borrow() != 0 {
            return Json(ApiResponse {
                success: false,
                data: None,
                message: Some("暂停或撤单正在提交，请稍后重试".into()),
            });
        }
    }
    permit.send(action);
    drop(pending_pause);
    drop(control);
    if let Some(submitted) = submitted {
        let _ = submitted.send(());
    }
    if let Some(reply) = reply {
        let response = tokio::select! {
            biased;
            _ = retired.changed() => return Json(ApiResponse { success: false, data: None, message: Some("交易对已移除，无法执行操作".into()) }),
            _ = async {
                if let Some(results) = &socket_results { results.closed().await; }
                else { std::future::pending::<()>().await; }
            } => return Json(ApiResponse { success: false, data: None, message: Some("控制连接已关闭".into()) }),
            response = reply => response,
        };
        match response {
            Ok(Ok(())) => {}
            Ok(Err(message)) => {
                return Json(ApiResponse {
                    success: false,
                    data: None,
                    message: Some(message),
                })
            }
            Err(error) => {
                return Json(ApiResponse {
                    success: false,
                    data: None,
                    message: Some(format!("策略未确认控制操作: {}", error)),
                })
            }
        }
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
                    // Collect only account fields under the pair's snapshot locks.
                    let states = source.states().await;
                    let pairs = futures_util::future::join_all(
                        states.iter().map(|state| state.account_snapshot())
                    ).await;
                    let Ok(json) = serde_json::to_string(&AccountMessage::Accounts { pairs }) else {
                        break;
                    };
                    if !primary.is_token_valid(&token).await { break; }
                    let message = Message::Text(json);
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

async fn send_until_retired<S>(
    sender: &mut S,
    message: Message,
    retired: &mut tokio::sync::watch::Receiver<bool>,
) -> Option<Result<(), S::Error>>
where
    S: futures_util::Sink<Message> + Unpin,
{
    if *retired.borrow() {
        return None;
    }
    tokio::select! {
        biased;
        _ = retired.changed() => None,
        result = sender.send(message) => Some(result),
    }
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>, token: String) {
    let mut retired = state.subscribe_retirement();
    if *retired.borrow() {
        return;
    }
    let (mut sender, mut receiver) = socket.split();
    let mut rx = state.ws_broadcast_tx.subscribe();

    // Send initial snapshot immediately upon connection
    let initial_snapshot = state.snapshot().await;
    if !state.is_token_valid(&token).await {
        return;
    }
    if let Ok(json) = serde_json::to_string(&serde_json::json!({
        "type": "snapshot",
        "data": initial_snapshot
    })) {
        if !matches!(
            send_until_retired(&mut sender, Message::Text(json), &mut retired).await,
            Some(Ok(()))
        ) {
            return;
        }
    }

    let (result_tx, mut result_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let (control_tx, mut control_rx) = tokio::sync::mpsc::unbounded_channel::<(
        String,
        (BotControlAction, Option<ControlReply>),
        Option<PendingControlPause>,
    )>();
    let worker_state = state.clone();
    let worker_results = result_tx.clone();
    let worker_token = token.clone();
    tokio::spawn(async move {
        while let Some((name, action, pending_pause)) = control_rx.recv().await {
            let (submitted, ready) = tokio::sync::oneshot::channel();
            let state = worker_state.clone();
            let results = worker_results.clone();
            let token = worker_token.clone();
            tokio::spawn(async move {
                let response = dispatch_prepared_control(
                    state,
                    action,
                    pending_pause,
                    Some(submitted),
                    Some(results.clone()),
                    Some(token),
                )
                .await
                .0;
                let _ = results.send(serde_json::json!({"type":"control", "action":name, "success":response.success, "data":response.data, "message":response.message}).to_string());
            });
            let _ = ready.await;
        }
    });
    let send_state = state.clone();
    let send_token = token.clone();
    let mut send_task = tokio::spawn(async move {
        let mut check_auth = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = retired.changed() => break,
                _ = check_auth.tick() => {
                    if !send_state.is_token_valid(&send_token).await { break; }
                }
                Some(msg) = result_rx.recv() => {
                    if !send_state.is_token_valid(&send_token).await { break; }
                    if !matches!(send_until_retired(&mut sender, Message::Text(msg), &mut retired).await, Some(Ok(()))) { break; }
                }
                message = rx.recv() => {
                    match message {
                        Ok(msg) => {
                            if !send_state.is_token_valid(&send_token).await { break; }
                            if !matches!(
                                send_until_retired(&mut sender, Message::Text(msg), &mut retired).await,
                                Some(Ok(()))
                            ) { break; }
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
                if !state_clone.is_token_valid(&token).await {
                    break;
                }
                let parsed = serde_json::from_str::<ControlRequest>(&text);
                let (name, action) = match parsed {
                    Ok(req) => {
                        let name = req.action.clone();
                        (Some(name), control_action(req, state_clone.pause_epoch()))
                    }
                    Err(error) => (None, Err(format!("无效的控制请求: {}", error))),
                };
                let action = match action {
                    Ok(action) => action,
                    Err(message) => {
                        let _ = result_tx.send(serde_json::json!({"type":"control", "action":name, "success":false, "data":null, "message":message}).to_string());
                        continue;
                    }
                };
                let pending_pause = PendingControlPause::new(&state_clone, &action.0);
                let prepare_state = state_clone.clone();
                let prepare_results = result_tx.clone();
                let prepare_queue = control_tx.clone();
                let preparation = tokio::spawn(async move {
                    if let Err(message) = prepare_control(&prepare_state, &action.0).await {
                        let _ = prepare_results.send(serde_json::json!({"type":"control", "action":name, "success":false, "data":null, "message":message}).to_string());
                    } else {
                        let _ = prepare_queue.send((name.unwrap(), action, pending_pause));
                    }
                });
                if preparation.await.is_err() {
                    break;
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

    #[tokio::test]
    async fn control_requires_confirmation_mode_and_propagates_engine_rejections() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (action_tx, mut action_rx) = tokio::sync::mpsc::channel(1);
        let state = AppState::new(AppConfig::default(), db, action_tx);
        for action in ["resume", "rebalance"] {
            for expected_mode in [None, Some(TradingMode::Unknown)] {
                let response = post_control_handler(
                    State(state.clone()),
                    Json(ControlRequest {
                        action: action.into(),
                        expected_mode,
                    }),
                )
                .await;
                assert!(!response.0.success);
                assert!(action_rx.try_recv().is_err());
            }
        }
        for action in ["resume", "rebalance"] {
            let request_state = state.clone();
            let request = tokio::spawn(async move {
                post_control_handler(
                    State(request_state),
                    Json(ControlRequest {
                        action: action.into(),
                        expected_mode: Some(TradingMode::Paper),
                    }),
                )
                .await
            });
            let reply = match action_rx.recv().await.unwrap() {
                BotControlAction::Resume {
                    expected_mode,
                    reply,
                    ..
                } => {
                    assert_eq!(expected_mode, TradingMode::Paper);
                    reply
                }
                BotControlAction::Rebalance {
                    expected_mode,
                    reply,
                } => {
                    assert_eq!(expected_mode, TradingMode::Paper);
                    reply
                }
                other => panic!("unexpected action: {:?}", other),
            };
            reply.send(Err("操作被拒绝".into())).unwrap();
            let response = request.await.unwrap().0;
            assert!(!response.success);
            assert_eq!(response.message.as_deref(), Some("操作被拒绝"));
        }
    }

    #[tokio::test]
    async fn symbols_consumer_filters_sorts_and_reports_upstream_errors() {
        use axum::{routing::get, Json, Router};
        use serde_json::json;

        let app = Router::new().route(
            "/fapi/v1/exchangeInfo",
            get(|| async {
                Json(json!({"symbols": [
                    {"symbol":"ZUSDT", "status":"TRADING", "pricePrecision":2, "quantityPrecision":2, "filters":[]},
                    {"symbol":"BUSDT", "status":"BREAK", "pricePrecision":2, "quantityPrecision":2, "filters":[]},
                    {"symbol":"AUSDT", "status":"TRADING", "pricePrecision":2, "quantityPrecision":2, "filters":[]}
                ]}))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = BinanceFuturesClient::with_base_url(&AppConfig::default().exchange, url);
        let (status, Json(response)) = fetch_exchange_symbols(&client).await;
        assert_eq!(status, StatusCode::OK);
        assert!(response.success);
        assert_eq!(response.data.unwrap(), vec!["AUSDT", "ZUSDT"]);
        assert!(response.message.is_none());
        server.abort();

        let app = Router::new().route(
            "/fapi/v1/exchangeInfo",
            get(|| async { StatusCode::SERVICE_UNAVAILABLE }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = BinanceFuturesClient::with_base_url(&AppConfig::default().exchange, url);
        let (status, Json(response)) = fetch_exchange_symbols(&client).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert!(!response.success);
        assert!(response.data.is_none());
        assert!(response.message.is_some());
        server.abort();
    }

    #[tokio::test]
    async fn symbols_route_requires_auth_and_rejects_invalid_environment() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        db.create_session("symbols-test-token").unwrap();
        let (tx, _) = tokio::sync::mpsc::channel(1);
        let router = create_router(AppState::new(AppConfig::default(), db, tx));
        let response = router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/exchange/symbols?environment=mainnet")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        for uri in [
            "/api/exchange/symbols",
            "/api/exchange/symbols?environment=invalid",
            "/api/exchange/symbols?environment=mainnet&environment=testnet",
        ] {
            let response = router
                .clone()
                .oneshot(
                    axum::http::Request::builder()
                        .uri(uri)
                        .header("Authorization", "Bearer symbols-test-token")
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let bytes = axum::body::to_bytes(response.into_body(), 1_000_000)
                .await
                .unwrap();
            let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(body["success"], false);
            assert!(body["data"].is_null());
            assert!(body["message"].is_string());
        }
    }

    struct BlockedSocketSink(Option<tokio::sync::oneshot::Sender<()>>);

    impl futures_util::Sink<Message> for BlockedSocketSink {
        type Error = ();

        fn poll_ready(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            if let Some(started) = self.0.take() {
                let _ = started.send(());
            }
            std::task::Poll::Pending
        }

        fn start_send(self: std::pin::Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
            unreachable!("blocked sink cannot accept a message")
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Pending
        }

        fn poll_close(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Pending
        }
    }

    #[tokio::test]
    async fn retirement_interrupts_blocked_snapshot_and_broadcast_sends() {
        for message in ["snapshot", "broadcast"] {
            let (retire, mut retired) = tokio::sync::watch::channel(false);
            let (started, waiting) = tokio::sync::oneshot::channel();
            let mut sender = BlockedSocketSink(Some(started));
            let send = tokio::spawn(async move {
                send_until_retired(&mut sender, Message::Text(message.into()), &mut retired)
                    .await
                    .is_none()
            });
            waiting.await.expect("send must reach the blocked sink");
            retire.send_replace(true);
            assert!(
                tokio::time::timeout(std::time::Duration::from_secs(3), send)
                    .await
                    .expect("retirement must interrupt a blocked send")
                    .unwrap()
            );
        }
    }

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
        config.exchange.api_secret = "account-stream-private-secret".into();
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
            assert!(!first
                .to_text()
                .unwrap()
                .contains("account-stream-private-secret"));
            assert!(!first.to_text().unwrap().contains("account-stream-token"));
            let first: serde_json::Value = serde_json::from_str(first.to_text().unwrap()).unwrap();
            assert_eq!(first["type"], "accounts");
            assert_eq!(first["data"]["pairs"].as_array().unwrap().len(), 1);
            let snapshot = &first["data"]["pairs"][0]["snapshot"];
            assert_eq!(snapshot["grid_config"].as_object().unwrap().len(), 1);
            assert!(snapshot["grid_config"]["is_testnet"].is_boolean());
            assert_eq!(snapshot.as_object().unwrap().len(), 6);
            assert!(snapshot["ticker"]["mark_update_time"].is_string());
            assert!(snapshot["ticker"]["mark_price"].is_string());
            assert!(snapshot["position"]["size"].is_string());
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
    async fn revoked_socket_commands_waiting_for_capacity_are_not_submitted() {
        for name in ["resume", "rebalance"] {
            let db = Arc::new(Database::open(":memory:").unwrap());
            db.create_session("revoked-control-token").unwrap();
            let (tx, mut rx) = tokio::sync::mpsc::channel(1);
            tx.send(BotControlAction::GlobalRiskCancel).await.unwrap();
            let state = AppState::new(AppConfig::default(), db.clone(), tx);
            let action = control_action(
                ControlRequest {
                    action: name.into(),
                    expected_mode: Some(TradingMode::Paper),
                },
                state.pause_epoch(),
            )
            .unwrap();
            let request = tokio::spawn(dispatch_prepared_control(
                state,
                action,
                None,
                None,
                None,
                Some("revoked-control-token".into()),
            ));
            tokio::task::yield_now().await;
            db.delete_session("revoked-control-token").unwrap();
            assert!(matches!(
                rx.recv().await,
                Some(BotControlAction::GlobalRiskCancel)
            ));
            let response = tokio::time::timeout(std::time::Duration::from_secs(3), request)
                .await
                .unwrap()
                .unwrap();
            assert!(!response.0.success);
            assert!(rx.try_recv().is_err());
        }
    }

    #[tokio::test]
    async fn pending_persisted_pause_blocks_other_clients_until_queued() {
        for name in ["pause", "cancel_all"] {
            let db = Arc::new(Database::open(":memory:").unwrap());
            let (tx, mut rx) = tokio::sync::mpsc::channel(1);
            let state = AppState::new(AppConfig::default(), db, tx);
            let pause = control_action(
                ControlRequest {
                    action: name.into(),
                    expected_mode: None,
                },
                state.pause_epoch(),
            )
            .unwrap();
            let pending = PendingControlPause::new(&state, &pause.0);
            prepare_control(&state, &pause.0).await.unwrap();
            let resume = control_action(
                ControlRequest {
                    action: "resume".into(),
                    expected_mode: Some(TradingMode::Paper),
                },
                state.pause_epoch(),
            )
            .unwrap();
            let response =
                dispatch_prepared_control(state.clone(), resume, None, None, None, None).await;
            assert!(!response.0.success);
            assert!(rx.try_recv().is_err());
            let response =
                dispatch_prepared_control(state.clone(), pause, pending, None, None, None).await;
            assert!(response.0.success);
            assert!(matches!(
                rx.recv().await,
                Some(
                    BotControlAction::PauseAlreadyPersisted
                        | BotControlAction::CancelAllAlreadyPersisted
                )
            ));
            assert_eq!(*state.status.read().await, BotStatus::Paused);
        }
    }

    #[tokio::test]
    async fn socket_pause_is_processed_while_engine_reply_is_pending() {
        use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};
        for action in ["resume", "rebalance"] {
            let db = Arc::new(Database::open(":memory:").unwrap());
            db.create_session("pause-test-token").unwrap();
            let (tx, mut rx) = tokio::sync::mpsc::channel(2);
            let state = AppState::new(AppConfig::default(), db, tx);
            state.resume_trading().await.unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let router = create_router(state.clone());
            let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
            let mut request = format!("ws://{address}/ws").into_client_request().unwrap();
            request.headers_mut().insert(
                "Sec-WebSocket-Protocol",
                "auth, token.pause-test-token".parse().unwrap(),
            );
            let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
            socket.next().await.unwrap().unwrap();
            socket
                .send(Message::Text(format!(
                    r#"{{"action":"{action}","expected_mode":"PAPER"}}"#
                )))
                .await
                .unwrap();
            let first = tokio::time::timeout(std::time::Duration::from_secs(3), rx.recv())
                .await
                .unwrap()
                .unwrap();
            let reply = match first {
                BotControlAction::Resume { reply, .. }
                | BotControlAction::Rebalance { reply, .. } => reply,
                other => panic!("unexpected action: {:?}", other),
            };
            assert!(state.socket_control_lock.try_write().is_ok());
            socket
                .send(Message::Text(r#"{"action":"pause"}"#.into()))
                .await
                .unwrap();
            let paused = tokio::time::timeout(std::time::Duration::from_secs(3), rx.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(matches!(paused, BotControlAction::PauseAlreadyPersisted));
            assert_eq!(*state.status.read().await, BotStatus::Paused);
            drop(reply);
            server.abort();
        }
    }

    #[tokio::test]
    async fn socket_disconnect_and_revocation_keep_accepted_pause_and_cancel_queued() {
        use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};
        for action in ["pause", "cancel_all"] {
            let db = Arc::new(Database::open(":memory:").unwrap());
            db.create_session("disconnect-test-token").unwrap();
            let (tx, mut rx) = tokio::sync::mpsc::channel(1);
            tx.send(BotControlAction::GlobalRiskCancel).await.unwrap();
            let state = AppState::new(AppConfig::default(), db.clone(), tx);
            state.resume_trading().await.unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let router = create_router(state.clone());
            let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
            let mut request = format!("ws://{address}/ws").into_client_request().unwrap();
            request.headers_mut().insert(
                "Sec-WebSocket-Protocol",
                "auth, token.disconnect-test-token".parse().unwrap(),
            );
            let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
            socket.next().await.unwrap().unwrap();
            socket
                .send(Message::Text(format!(r#"{{"action":"{action}"}}"#)))
                .await
                .unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(3), async {
                while *state.status.read().await != BotStatus::Paused {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            socket.close(None).await.unwrap();
            db.delete_session("disconnect-test-token").unwrap();
            drop(socket);
            tokio::task::yield_now().await;
            assert!(matches!(
                rx.recv().await,
                Some(BotControlAction::GlobalRiskCancel)
            ));
            let pending = tokio::time::timeout(std::time::Duration::from_secs(3), rx.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(matches!(
                (action, pending),
                ("pause", BotControlAction::PauseAlreadyPersisted)
                    | ("cancel_all", BotControlAction::CancelAllAlreadyPersisted)
            ));
            server.abort();
        }
    }

    #[tokio::test]
    async fn retirement_releases_control_waiting_for_queue_capacity() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        tx.send(BotControlAction::GlobalRiskCancel).await.unwrap();
        let state = AppState::new(AppConfig::default(), db, tx);
        let request_state = state.clone();
        let request = tokio::spawn(async move {
            post_control_handler(
                State(request_state),
                Json(ControlRequest {
                    action: "rebalance".into(),
                    expected_mode: Some(TradingMode::Paper),
                }),
            )
            .await
        });
        tokio::task::yield_now().await;
        state.retire();
        let response = tokio::time::timeout(std::time::Duration::from_secs(3), request)
            .await
            .unwrap()
            .unwrap();
        assert!(!response.0.success);
    }

    #[tokio::test]
    async fn socket_orders_pause_before_resume_and_reports_failures() {
        use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};
        let db = Arc::new(Database::open(":memory:").unwrap());
        db.create_session("ordering-test-token").unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        let state = AppState::new(AppConfig::default(), db, tx);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = create_router(state.clone());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let mut request = format!("ws://{address}/ws").into_client_request().unwrap();
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            "auth, token.ordering-test-token".parse().unwrap(),
        );
        let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        socket.next().await.unwrap().unwrap();
        let original_epoch = state.pause_epoch();
        socket
            .send(Message::Text(r#"{"action":"pause"}"#.into()))
            .await
            .unwrap();
        socket
            .send(Message::Text(
                r#"{"action":"resume","expected_mode":"PAPER"}"#.into(),
            ))
            .await
            .unwrap();
        socket.send(Message::Text("not json".into())).await.unwrap();
        let first = tokio::time::timeout(std::time::Duration::from_secs(3), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(first, BotControlAction::PauseAlreadyPersisted));
        let second = tokio::time::timeout(std::time::Duration::from_secs(3), rx.recv())
            .await
            .unwrap()
            .unwrap();
        let reply = match second {
            BotControlAction::Resume {
                expected_epoch,
                reply,
                ..
            } => {
                assert!(expected_epoch > original_epoch);
                assert_eq!(expected_epoch, state.pause_epoch());
                reply
            }
            other => panic!("unexpected action: {:?}", other),
        };
        reply.send(Err("恢复被拒绝".into())).unwrap();
        let mut parse_error = false;
        let mut resume_error = false;
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while !parse_error || !resume_error {
                let frame = socket.next().await.unwrap().unwrap();
                if let Ok(value) =
                    serde_json::from_str::<serde_json::Value>(frame.to_text().unwrap())
                {
                    if value["type"] == "control" && value["success"] == false {
                        if value["action"].is_null() {
                            parse_error = true;
                        }
                        if value["action"] == "resume" && value["message"] == "恢复被拒绝" {
                            resume_error = true;
                        }
                    }
                }
            }
        })
        .await
        .unwrap();
        server.abort();
    }

    #[tokio::test]
    async fn retired_state_closes_connected_pair_socket() {
        use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};

        let db = Arc::new(Database::open(":memory:").unwrap());
        db.create_session("retirement-test-token").unwrap();
        let (tx, _) = tokio::sync::mpsc::channel(1);
        let state = AppState::new(AppConfig::default(), db, tx);
        let router = create_router(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let mut request = format!("ws://{address}/ws").into_client_request().unwrap();
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            "auth, token.retirement-test-token".parse().unwrap(),
        );
        let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let initial = tokio::time::timeout(std::time::Duration::from_secs(3), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let snapshot: serde_json::Value = serde_json::from_str(initial.to_text().unwrap()).unwrap();
        assert_eq!(snapshot["type"], "snapshot");

        state.retire();
        let closed = tokio::time::timeout(std::time::Duration::from_secs(3), socket.next())
            .await
            .expect("retired state must close its socket");
        assert!(matches!(
            closed,
            None | Some(Ok(Message::Close(_))) | Some(Err(_))
        ));
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn already_retired_state_does_not_send_socket_snapshot() {
        use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};

        let db = Arc::new(Database::open(":memory:").unwrap());
        db.create_session("retirement-test-token").unwrap();
        let (tx, _) = tokio::sync::mpsc::channel(1);
        let state = AppState::new(AppConfig::default(), db, tx);
        state.retire();
        let router = create_router(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let mut request = format!("ws://{address}/ws").into_client_request().unwrap();
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            "auth, token.retirement-test-token".parse().unwrap(),
        );
        let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let closed = tokio::time::timeout(std::time::Duration::from_secs(3), socket.next())
            .await
            .expect("retired state must close its socket");
        assert!(matches!(
            closed,
            None | Some(Ok(Message::Close(_))) | Some(Err(_))
        ));
        server.abort();
        let _ = server.await;
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
        assert!(response["data"]["today_pnl"]["window_start"].is_string());
        assert!(response["data"]["today_pnl"]["window_end"].is_string());
        assert!(response["data"]["today_pnl"]["groups"]
            .as_array()
            .unwrap()
            .is_empty());
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

    #[test]
    fn pair_update_preserves_optional_settings_and_handles_explicit_clears() {
        use rust_decimal_macros::dec;

        let mut config = AppConfig::default();
        config.exchange.api_key = "key".into();
        config.exchange.api_secret = "secret".into();
        config.telegram.bot_token = "token".into();
        config.telegram.chat_id = "chat".into();
        config.grid.min_price = Some(dec!(1));
        config.grid.max_price = Some(dec!(100));
        let mut payload = serde_json::json!({
            "symbol": "SOLUSDC", "grid_interval": "0.1", "order_amount_usdc": "100",
            "buy_window": 5, "sell_window": 5,
            "api_key": " ", "api_secret": " ", "telegram_bot_token": " "
        });
        let updated = apply_config_payload(
            config.clone(),
            serde_json::from_value(payload.clone()).unwrap(),
        )
        .unwrap();
        assert_eq!(updated.exchange.api_key, "key");
        assert_eq!(updated.exchange.api_secret, "secret");
        assert_eq!(updated.telegram.bot_token, "token");
        assert_eq!(updated.telegram.chat_id, "chat");
        assert_eq!(updated.grid.min_price, None);
        assert_eq!(updated.grid.max_price, None);
        assert_eq!(updated.grid.max_position_usdc, None);

        payload["min_price"] = serde_json::Value::Null;
        payload["max_price"] = serde_json::Value::Null;
        payload["max_position_usdc"] = serde_json::Value::Null;
        payload["clear_api_credentials"] = serde_json::json!(true);
        payload["clear_telegram_credentials"] = serde_json::json!(true);
        payload["api_key"] = serde_json::json!("replacement");
        payload["telegram_bot_token"] = serde_json::json!("replacement");
        let cleared =
            apply_config_payload(config, serde_json::from_value(payload).unwrap()).unwrap();
        assert!(cleared.exchange.api_key.is_empty() && cleared.exchange.api_secret.is_empty());
        assert!(cleared.telegram.bot_token.is_empty() && cleared.telegram.chat_id.is_empty());
        assert_eq!(cleared.grid.min_price, None);
        assert_eq!(cleared.grid.max_price, None);
        assert_eq!(cleared.grid.max_position_usdc, None);
    }

    #[tokio::test]
    async fn pair_update_rejects_missing_and_stale_versions_before_dispatch() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (action_tx, mut action_rx) = tokio::sync::mpsc::channel(1);
        let state = AppState::new(AppConfig::default(), db.clone(), action_tx);
        let mut payload = serde_json::json!({
            "symbol": "SOLUSDC", "grid_interval": "0.1", "order_amount_usdc": "100",
            "buy_window": 5, "sell_window": 5
        });
        for version in [None, Some(""), Some("   "), Some("stale")] {
            if let Some(version) = version {
                payload["baseline_version"] = serde_json::json!(version);
            }
            let (status, response) = post_config_handler(
                State(state.clone()),
                Json(serde_json::from_value(payload.clone()).unwrap()),
            )
            .await;
            assert_eq!(
                status,
                if version == Some("stale") {
                    StatusCode::CONFLICT
                } else {
                    StatusCode::BAD_REQUEST
                }
            );
            assert!(!response.0.success);
            assert!(action_rx.try_recv().is_err());
            assert!(db.load_config().unwrap().is_none());
        }
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
        payload["baseline_version"] = serde_json::json!(pair_config_version(
            &*state.config.read().await,
            state.config_instance
        ));
        let response = post_config_handler(
            State(state.clone()),
            Json(serde_json::from_value(payload.clone()).unwrap()),
        )
        .await
        .1;
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
            payload["baseline_version"] = serde_json::json!(pair_config_version(
                &*state.config.read().await,
                state.config_instance
            ));
            let response = post_config_handler(
                State(state.clone()),
                Json(serde_json::from_value(payload.clone()).unwrap()),
            )
            .await
            .1;
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
    async fn retired_control_cannot_change_recreated_pair() {
        for action in ["pause", "cancel_all"] {
            let db = Arc::new(Database::open(":memory:").unwrap());
            let config = AppConfig::default();
            let trade_lock = Arc::new(tokio::sync::Mutex::new(()));
            let (old_tx, mut old_rx) = tokio::sync::mpsc::channel(1);
            let old =
                AppState::new_for_pair(config.clone(), db.clone(), old_tx, trade_lock.clone());
            let retirement = old.socket_control_lock.write().await;
            let request_state = old.clone();
            let request = tokio::spawn(async move {
                post_control_handler(
                    State(request_state),
                    Json(ControlRequest {
                        action: action.into(),
                        expected_mode: None,
                    }),
                )
                .await
            });
            tokio::task::yield_now().await;
            old.retire();
            drop(retirement);

            let (new_tx, _new_rx) = tokio::sync::mpsc::channel(1);
            let new = AppState::new_for_pair(config.clone(), db.clone(), new_tx, trade_lock);
            new.resume_trading().await.unwrap();
            assert!(!request.await.unwrap().0.success);
            assert!(old_rx.try_recv().is_err());
            assert_eq!(*new.status.read().await, BotStatus::Running);
            assert_eq!(
                db.load_pair_status(&config.exchange.symbol, TradingMode::Paper)
                    .unwrap(),
                Some(BotStatus::Running)
            );

            let response = post_control_handler(
                State(old),
                Json(ControlRequest {
                    action: action.into(),
                    expected_mode: None,
                }),
            )
            .await;
            assert!(!response.0.success);
            assert!(old_rx.try_recv().is_err());
            assert_eq!(
                db.load_pair_status(&config.exchange.symbol, TradingMode::Paper)
                    .unwrap(),
                Some(BotStatus::Running)
            );
        }
    }

    #[tokio::test]
    async fn cancelled_control_finishes_before_retirement_and_recreation() {
        for action in ["pause", "cancel_all"] {
            let db = Arc::new(Database::open(":memory:").unwrap());
            let config = AppConfig::default();
            let trade_lock = Arc::new(tokio::sync::Mutex::new(()));
            let (old_tx, mut old_rx) = tokio::sync::mpsc::channel(1);
            let old =
                AppState::new_for_pair(config.clone(), db.clone(), old_tx, trade_lock.clone());
            let status = old.status.write().await;
            let request_state = old.clone();
            let request = tokio::spawn(async move {
                post_control_handler(
                    State(request_state),
                    Json(ControlRequest {
                        action: action.into(),
                        expected_mode: None,
                    }),
                )
                .await
            });
            tokio::time::timeout(std::time::Duration::from_secs(3), async {
                while old.socket_control_lock.try_write().is_ok() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            request.abort();
            assert!(matches!(request.await, Err(error) if error.is_cancelled()));
            let retiring = old.clone();
            let retirement = tokio::spawn(async move {
                let _guard = retiring.socket_control_lock.write().await;
                retiring.retire();
            });
            tokio::task::yield_now().await;
            assert!(!retirement.is_finished());
            drop(status);
            retirement.await.unwrap();
            assert!(matches!(
                old_rx.try_recv(),
                Ok(BotControlAction::PauseAlreadyPersisted
                    | BotControlAction::CancelAllAlreadyPersisted)
                    | Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ));

            let (new_tx, _new_rx) = tokio::sync::mpsc::channel(1);
            let new = AppState::new_for_pair(config.clone(), db.clone(), new_tx, trade_lock);
            new.resume_trading().await.unwrap();
            assert_eq!(*new.status.read().await, BotStatus::Running);
            assert_eq!(
                db.load_pair_status(&config.exchange.symbol, TradingMode::Paper)
                    .unwrap(),
                Some(BotStatus::Running)
            );
        }
    }

    #[tokio::test]
    async fn pause_updates_status_before_engine_accepts_action() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (action_tx, action_rx) = tokio::sync::mpsc::channel(1);
        action_tx
            .send(BotControlAction::GlobalRiskCancel)
            .await
            .unwrap();
        let state = AppState::new(AppConfig::default(), db.clone(), action_tx);
        let request_state = state.clone();
        let request = tokio::spawn(async move {
            post_control_handler(
                State(request_state),
                Json(ControlRequest {
                    action: "pause".into(),
                    expected_mode: None,
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
