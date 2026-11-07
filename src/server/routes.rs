use crate::config::{AppConfig, ServerConfig};
use crate::exchange::rate_limit::RateLimiter;
use crate::exchange::BinanceFuturesClient;
use crate::runtime::RuntimeHandle;
use crate::server::state::snapshot::{AccountFrame, AccountMessage};
use crate::server::state::AppState;
use crate::types::{
    AuthLoginPayload, AuthStatusResponse, AuthTokenResponse, BotControlAction, BotSnapshot,
    GridOrder, LogEntry, OverviewSnapshot, TradeRecord, TradingMode, TradingPairView,
    UpdateConfigPayload, UpdateGlobalConfigPayload, WebConfigView, WebGlobalConfigView,
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
use std::future::Future;
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

impl DatabaseTargetsRequest {
    fn selected_targets(&self) -> Option<(bool, bool)> {
        let mut trades = false;
        let mut snapshots = false;
        for target in &self.targets {
            match target.as_str() {
                "trade_history" => trades = true,
                "account_hourly_snapshots" => snapshots = true,
                _ => return None,
            }
        }
        (trades || snapshots).then_some((trades, snapshots))
    }

    fn confirmed_targets(&self) -> Option<(bool, bool)> {
        self.confirm.then(|| self.selected_targets()).flatten()
    }
}

#[derive(serde::Serialize)]
struct DatabaseClearResponse {
    deleted_trades: usize,
    deleted_account_hourly_snapshots: usize,
}

pub use crate::types::ApiResponse;

fn json_ok<T>(data: T) -> Json<ApiResponse<T>> {
    Json(ApiResponse::ok(data))
}

fn json_ok_msg<T>(data: T, message: impl Into<String>) -> Json<ApiResponse<T>> {
    Json(ApiResponse::ok_with_message(data, message))
}

fn json_err<T>(message: impl Into<String>) -> Json<ApiResponse<T>> {
    Json(ApiResponse::err(message))
}

fn status_err<T>(
    status: StatusCode,
    message: impl Into<String>,
) -> (StatusCode, Json<ApiResponse<T>>) {
    (status, json_err(message))
}

#[derive(Deserialize)]
struct SymbolsQuery {
    environment: String,
}

#[derive(Deserialize)]
struct SymbolPreviewQuery {
    environment: String,
    symbol: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ExchangeSymbolPreview {
    pub symbol: String,
    pub last_price: Option<rust_decimal::Decimal>,
    pub mark_price: Option<rust_decimal::Decimal>,
    pub tick_size: rust_decimal::Decimal,
    pub step_size: rust_decimal::Decimal,
    pub min_qty: rust_decimal::Decimal,
    pub min_notional: rust_decimal::Decimal,
}

async fn exchange_symbols_handler(
    Extension(limiter): Extension<Arc<RateLimiter>>,
    query: Result<Query<SymbolsQuery>, axum::extract::rejection::QueryRejection>,
) -> (StatusCode, Json<ApiResponse<Vec<String>>>) {
    match query {
        Ok(Query(SymbolsQuery { environment })) if environment == "mainnet" => {}
        _ => return status_err(StatusCode::BAD_REQUEST, "无效的交易所环境，请使用 mainnet"),
    };
    let config = AppConfig::default().exchange;
    fetch_exchange_symbols(&BinanceFuturesClient::new_with_limiter(&config, limiter)).await
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
            (StatusCode::OK, json_ok(symbols))
        }
        Err(error) => {
            warn!(%error, "获取交易所交易对失败");
            status_err(StatusCode::BAD_GATEWAY, "获取交易所交易对失败，请稍后重试")
        }
    }
}

async fn exchange_symbol_preview_handler(
    Extension(limiter): Extension<Arc<RateLimiter>>,
    query: Result<Query<SymbolPreviewQuery>, axum::extract::rejection::QueryRejection>,
) -> (StatusCode, Json<ApiResponse<ExchangeSymbolPreview>>) {
    let SymbolPreviewQuery {
        environment: _,
        symbol,
    } = match query {
        Ok(Query(q)) if q.environment == "mainnet" => q,
        _ => return status_err(StatusCode::BAD_REQUEST, "无效的交易所环境或交易对参数"),
    };
    let symbol = symbol.trim().to_ascii_uppercase();
    if crate::config::validate_symbol(&symbol).is_err() {
        return status_err(StatusCode::BAD_REQUEST, "无效的交易对代码");
    }
    let config = AppConfig::default().exchange;
    let client = BinanceFuturesClient::new_with_limiter(&config, limiter);
    fetch_exchange_symbol_preview(&client, &symbol).await
}

async fn fetch_exchange_symbol_preview(
    client: &BinanceFuturesClient,
    symbol: &str,
) -> (StatusCode, Json<ApiResponse<ExchangeSymbolPreview>>) {
    let (info_res, last_res, mark_res) = tokio::join!(
        client.get_exchange_info(Some(symbol)),
        client.get_ticker_price(symbol),
        client.get_mark_price(symbol)
    );
    let info = match info_res {
        Ok(info) => info,
        Err(error) => {
            warn!(%error, symbol, "获取交易对规则失败");
            return status_err(StatusCode::BAD_GATEWAY, "获取交易对规则失败，请稍后重试");
        }
    };
    let rules =
        match crate::strategy::precision::SymbolRules::from_trading_exchange_info(&info, symbol) {
            Ok(rules) => rules,
            Err(crate::strategy::precision::SymbolRulesError::NotFound) => {
                return status_err(StatusCode::NOT_FOUND, "交易对不存在");
            }
            Err(crate::strategy::precision::SymbolRulesError::NotTrading) => {
                return status_err(StatusCode::BAD_REQUEST, "该交易对当前未处于交易状态");
            }
            Err(crate::strategy::precision::SymbolRulesError::MissingFilters) => {
                return status_err(StatusCode::BAD_GATEWAY, "交易对精度规则缺失");
            }
        };
    let last_price = last_res
        .ok()
        .filter(|price| *price > rust_decimal::Decimal::ZERO);
    let mark_price = mark_res
        .ok()
        .filter(|price| *price > rust_decimal::Decimal::ZERO);
    (
        StatusCode::OK,
        json_ok(ExchangeSymbolPreview {
            symbol: rules.symbol,
            last_price,
            mark_price,
            tick_size: rules.tick_size,
            step_size: rules.step_size,
            min_qty: rules.min_qty,
            min_notional: rules.min_notional,
        }),
    )
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
    status_err(
        StatusCode::INTERNAL_SERVER_ERROR,
        "认证服务异常，请稍后重试",
    )
}

async fn require_auth_middleware(
    State(state): State<Arc<AppState>>,
    req: axum::extract::Request,
    next: Next,
) -> Result<Response, (StatusCode, Json<ApiResponse<()>>)> {
    if !state.is_auth_configured() {
        return Ok(next.run(req).await);
    }
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
        return Err(status_err(
            StatusCode::UNAUTHORIZED,
            "未授权访问：请先输入管理员密码进行身份认证",
        ));
    }

    Ok(next.run(req).await)
}

pub fn create_router(state: Arc<AppState>) -> Router {
    let accounts = AccountHub::new(AccountSource::Fixed(Arc::new(vec![state.clone()])));
    let pairs = Router::new()
        .route("/api/pairs", get(get_single_pair_handler))
        .route("/api/overview", get(get_single_overview_handler))
        .route("/api/accounts", get(get_accounts_handler))
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
        .route("/api/accounts/ws", get(accounts_ws_handler))
        .layer(Extension(accounts))
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

fn exchange_routes<S: Clone + Send + Sync + 'static>(limiter: Arc<RateLimiter>) -> Router<S> {
    Router::new()
        .route(
            "/api/exchange/symbols",
            get(exchange_symbols_handler).layer(Extension(limiter.clone())),
        )
        .route(
            "/api/exchange/symbol-preview",
            get(exchange_symbol_preview_handler).layer(Extension(limiter)),
        )
}

fn create_base_router(state: Arc<AppState>, history_clear: bool) -> Router {
    let mut api_protected = exchange_routes(Arc::new(RateLimiter::default()))
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
        .route("/api/control", post(post_control_handler));
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
        .route("/api/auth/login", post(post_auth_login_handler))
        .route("/api/auth/logout", post(post_auth_logout_handler))
}

/// Existing unscoped APIs address the first pair; scoped APIs never change another pair.
pub fn create_multi_router(states: Vec<Arc<AppState>>) -> Router {
    let Some(primary) = states.first().cloned() else {
        return Router::new();
    };
    let mut router = create_base_router(primary.clone(), false);
    let states = Arc::new(states);
    let accounts = AccountHub::new(AccountSource::Fixed(states.clone()));
    for state in states.iter() {
        let Some(symbol) = state.pair_scope() else {
            continue;
        };
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
        .route("/api/accounts", get(get_accounts_handler))
        .route("/api/trades/clear", post(clear_multi_trade_history_handler))
        .route("/api/database/clear", post(multi_database_clear_handler))
        .route_layer(middleware::from_fn_with_state(
            primary,
            require_auth_middleware,
        ))
        .route("/api/accounts/ws", get(accounts_ws_handler))
        .layer(Extension(accounts))
        .with_state(states);
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
            mode: config.exchange.trading_mode(),
            grid_config: config.summary(),
        });
    }
    json_ok(pairs)
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
    let summaries = futures_util::future::join_all(
        states
            .iter()
            .map(|state| state.overview_snapshot(generated_at)),
    )
    .await;
    let mut pairs = Vec::with_capacity(summaries.len());
    let mut recent_trades = Vec::new();
    for (pair, trades) in summaries {
        pairs.push(pair);
        recent_trades.extend(trades);
    }
    recent_trades.sort_unstable_by(|a, b| {
        b.timestamp
            .cmp(&a.timestamp)
            .then_with(|| b.trade_id.cmp(&a.trade_id))
    });
    recent_trades.truncate(20);
    json_ok(OverviewSnapshot {
        generated_at,
        pairs,
        today_pnl,
        recent_trades,
    })
}

/// Dispatch through the current registry so newly added pairs are immediately addressable.
pub async fn create_managed_router(handle: RuntimeHandle) -> Router {
    let primary = handle.states.read().await[0].clone();
    let protected = exchange_routes(handle.rate_limiter.clone())
        .route(
            "/api/pairs",
            get(get_managed_pairs_handler).post(add_pair_handler),
        )
        .route("/api/overview", get(get_managed_overview_handler))
        .route("/api/accounts", get(get_accounts_handler))
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
        .route("/api/global/test-exchange", post(test_exchange_handler))
        .route("/api/global/test-telegram", post(test_telegram_handler))
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
        .route("/api/accounts/ws", get(accounts_ws_handler))
        .layer(Extension(AccountHub::new(AccountSource::Managed(
            handle.clone(),
        ))))
        .with_state(handle);
    let account = Router::new()
        .route(
            "/api/database/export",
            get(export_database_handler).post(export_selected_database_handler),
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
    let Ok(uri) = resource.parse() else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let mut req = req;
    *req.uri_mut() = uri;
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
        return status_err(
            StatusCode::BAD_REQUEST,
            "请确认清理当前币种所有模式的成交历史",
        );
    }
    match state.clear_symbol_trade_history(symbol).await {
        Ok((deleted_trades, snapshot)) => (
            StatusCode::OK,
            json_ok(ClearTradeHistoryResponse {
                deleted_trades,
                snapshot,
            }),
        ),
        Err(error) => {
            error!("Scoped trade cleanup failed: {}", error);
            status_err(StatusCode::INTERNAL_SERVER_ERROR, "历史记录清理失败")
        }
    }
}

async fn export_selected_database_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<DatabaseTargetsRequest>,
) -> Response {
    let Some((trades, snapshots)) = payload.selected_targets() else {
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
    let Some((trades, snapshots)) = payload.confirmed_targets() else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let membership = handle.membership_lock.clone().lock_owned().await;
    let states = handle.states.read().await.clone();
    database_clear_with_guard(&states, trades, snapshots, Some(membership)).await
}

async fn database_clear(states: &[Arc<AppState>], payload: DatabaseTargetsRequest) -> Response {
    let Some((trades, snapshots)) = payload.confirmed_targets() else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    database_clear_with_guard(states, trades, snapshots, None).await
}

async fn database_clear_with_guard(
    states: &[Arc<AppState>],
    trades: bool,
    snapshots: bool,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
) -> Response {
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
    json_ok(DatabaseClearResponse {
        deleted_trades,
        deleted_account_hourly_snapshots,
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
    let Ok(uri) = format!("/{}", resource).parse() else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    *req.uri_mut() = uri;
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
    let states = handle.states.read().await.clone();
    collect_pairs(&states).await
}

async fn get_managed_overview_handler(
    State(handle): State<RuntimeHandle>,
) -> Json<ApiResponse<OverviewSnapshot>> {
    // Release the registry lock before collecting snapshots so dashboard reads
    // do not hold up pair additions or removals during database work.
    let states = handle.states.read().await.clone();
    if states.is_empty() {
        let generated_at = chrono::Utc::now();
        return json_ok(OverviewSnapshot {
            generated_at,
            pairs: Vec::new(),
            today_pnl: handle.db.cached_today_pnl(generated_at).await,
            recent_trades: Vec::new(),
        });
    }
    collect_overview(&states).await
}

async fn get_overview_history_handler(
    State(handle): State<RuntimeHandle>,
) -> Json<ApiResponse<crate::snapshots::OverviewHistory>> {
    let since = chrono::Utc::now().timestamp() - 7 * 24 * 3600;
    let result = async {
        let current_accounts = crate::snapshots::current_accounts(&handle).await?;
        let snapshots = handle
            .db
            .run_blocking(move |db| db.hourly_account_snapshots(since))
            .await?;
        Ok::<_, anyhow::Error>(crate::snapshots::OverviewHistory {
            snapshots,
            current_accounts,
        })
    }
    .await;
    match result {
        Ok(data) => json_ok(data),
        Err(error) => {
            tracing::warn!(%error, "Could not read account snapshots");
            json_err("无法读取历史快照")
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
        let saved = handle.db.load_saved_config().await?;
        let mut defaults = AppConfig::default();
        crate::config::GlobalConfig::from_app(&saved).apply_to(&mut defaults);
        let config = apply_config_payload(defaults, payload)?;
        let symbol = config.exchange.symbol.clone();
        handle.add_pair(config).await?;
        Ok::<_, anyhow::Error>(symbol)
    }
    .await;
    match result {
        Ok(symbol) => json_ok_msg(
            symbol,
            "交易对已添加并保存，策略保持暂停；检查配置后手动恢复",
        ),
        Err(error) => json_err(error.to_string()),
    }
}

async fn post_managed_config_handler(
    State(state): State<Arc<AppState>>,
    Extension(handle): Extension<RuntimeHandle>,
    Json(payload): Json<UpdateConfigPayload>,
) -> (StatusCode, Json<ApiResponse<WebConfigView>>) {
    match tokio::spawn(async move {
        let _membership = handle.membership_lock.lock().await;
        if !handle
            .states
            .read()
            .await
            .iter()
            .any(|current| Arc::ptr_eq(current, &state))
        {
            return status_err(StatusCode::CONFLICT, "交易对已被删除或替换，请刷新后重试");
        }
        apply_pair_config(state, payload).await
    })
    .await
    {
        Ok(result) => result,
        Err(error) => status_err(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("配置更新任务异常退出: {}", error),
        ),
    }
}

async fn map_saved_config<T>(
    handle: &RuntimeHandle,
    missing_msg: &'static str,
    map: impl FnOnce(AppConfig) -> T,
) -> Json<ApiResponse<T>> {
    match handle.db.load_saved_config().await {
        Ok(config) => json_ok(map(config)),
        Err(_) => json_err(missing_msg),
    }
}

async fn get_global_config_handler(
    State(handle): State<RuntimeHandle>,
) -> Json<ApiResponse<WebGlobalConfigView>> {
    map_saved_config(&handle, "无法读取全局设置", |config| {
        let global = crate::config::GlobalConfig::from_app(&config);
        web_global_config_view(&global)
    })
    .await
}

async fn post_global_config_handler(
    State(handle): State<RuntimeHandle>,
    Json(payload): Json<UpdateGlobalConfigPayload>,
) -> Json<ApiResponse<WebGlobalConfigView>> {
    if let Err(error) = crate::config::validate_global_position_limit(
        payload.global_max_position_notional.flatten(),
    ) {
        return json_err(error.to_string());
    }
    if let Err(error) =
        crate::config::validate_min_available_margin_pct(payload.min_available_margin_pct.flatten())
    {
        return json_err(error.to_string());
    }
    match handle.update_global_config(payload).await {
        Ok(global) => {
            let view = web_global_config_view(&global);
            json_ok_msg(view, "全局设置已保存，所有交易对立即生效")
        }
        Err(error) => json_err(error.to_string()),
    }
}

fn web_global_config_view(global: &crate::config::GlobalConfig) -> WebGlobalConfigView {
    WebGlobalConfigView {
        version: crate::db::config_version(global),
        global_max_position_notional: global.global_max_position_notional,
        pause_on_zero_available_margin: global.pause_on_zero_available_margin,
        min_available_margin_pct: global.min_available_margin_pct,
        recv_window: global.recv_window,
        sync_interval_secs: global.sync_interval_secs,
        has_api_key: !global.api_key.trim().is_empty(),
        has_api_secret: !global.api_secret.trim().is_empty(),
        api_key_preview: mask_api_key_preview(&global.api_key),
        telegram_enabled: global.telegram.enabled,
        has_telegram_bot_token: !global.telegram.bot_token.trim().is_empty(),
        telegram_chat_id: global.telegram.chat_id.clone(),
    }
}

#[derive(Debug, Deserialize)]
struct TestExchangePayload {
    api_key: Option<String>,
    api_secret: Option<String>,
    recv_window: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct TestTelegramPayload {
    bot_token: Option<String>,
    chat_id: Option<String>,
}

fn non_blank(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Resolve probe credentials: a non-blank override wins, otherwise the saved value is used.
fn resolve_exchange_probe(
    saved: &crate::config::ExchangeConfig,
    payload: TestExchangePayload,
) -> Result<crate::config::ExchangeConfig, String> {
    let mut config = saved.clone();
    if let Some(key) = non_blank(payload.api_key) {
        config.api_key = key;
    }
    if let Some(secret) = non_blank(payload.api_secret) {
        config.api_secret = secret;
    }
    if let Some(window) = payload.recv_window {
        config.recv_window = window;
    }
    if config.api_key.trim().is_empty() || config.api_secret.trim().is_empty() {
        return Err("请同时填写 API Key 和 Secret（或先保存后再测试）".into());
    }
    if !(1..=60_000).contains(&config.recv_window) {
        return Err("请求有效窗口必须在 1-60000 毫秒之间".into());
    }
    Ok(config)
}

fn resolve_telegram_probe(
    saved: &crate::config::TelegramConfig,
    payload: TestTelegramPayload,
) -> Result<crate::config::TelegramConfig, String> {
    let mut config = saved.clone();
    if let Some(token) = non_blank(payload.bot_token) {
        config.bot_token = token;
    }
    if let Some(chat_id) = non_blank(payload.chat_id) {
        config.chat_id = chat_id;
    }
    if config.bot_token.trim().is_empty() || config.chat_id.trim().is_empty() {
        return Err("请填写 Bot Token 和 Chat ID（或先保存后再测试）".into());
    }
    Ok(config)
}

/// Probes talk to the exchange with credentials the engine does not use, so they get their own
/// request budget and a short cooldown: repeated clicks must never consume the engine's weight.
const PROBE_COOLDOWN: std::time::Duration = std::time::Duration::from_secs(3);

#[derive(Default)]
pub(crate) struct ProbeGate(std::sync::Mutex<Option<std::time::Instant>>);

impl ProbeGate {
    #[cfg(test)]
    fn new() -> Self {
        Self::default()
    }

    /// Err carries the whole seconds left to wait (at least one).
    fn enter(&self, now: std::time::Instant) -> Result<(), u64> {
        let mut last = self.0.lock().unwrap();
        if let Some(wait) = last
            .and_then(|at| PROBE_COOLDOWN.checked_sub(now.saturating_duration_since(at)))
            .filter(|wait| !wait.is_zero())
        {
            return Err(wait.as_secs() + 1);
        }
        *last = Some(now);
        Ok(())
    }
}

/// One gate per probe kind, owned by the runtime so tests and instances never share cooldowns.
#[derive(Default)]
pub(crate) struct ProbeGates {
    pub(crate) exchange: ProbeGate,
    pub(crate) telegram: ProbeGate,
}

fn probe_throttled(gate: &ProbeGate) -> Option<Json<ApiResponse<String>>> {
    gate.enter(std::time::Instant::now())
        .err()
        .map(|wait| json_err(format!("测试过于频繁，请 {wait} 秒后再试")))
}

async fn load_probe_config<T, P>(
    handle: &RuntimeHandle,
    gate: &ProbeGate,
    payload: P,
    resolve: impl FnOnce(&AppConfig, P) -> Result<T, String>,
) -> Result<T, Json<ApiResponse<String>>> {
    if let Some(throttled) = probe_throttled(gate) {
        return Err(throttled);
    }
    let Ok(saved) = handle.db.load_saved_config().await else {
        return Err(json_err("无法读取全局设置"));
    };
    resolve(&saved, payload).map_err(json_err)
}

/// Verify that the (possibly unsaved) API credentials can read the futures account.
/// Nothing is persisted, and failures never echo signed URLs or credentials.
async fn test_exchange_handler(
    State(handle): State<RuntimeHandle>,
    Json(payload): Json<TestExchangePayload>,
) -> Json<ApiResponse<String>> {
    let config = match load_probe_config(
        &handle,
        &handle.probes.exchange,
        payload,
        |saved, payload| resolve_exchange_probe(&saved.exchange, payload),
    )
    .await
    {
        Ok(config) => config,
        Err(response) => return response,
    };
    let client = BinanceFuturesClient::new_with_limiter(&config, Arc::new(RateLimiter::default()));
    if let Err(error) = client.sync_server_time().await {
        return json_err(sanitized_probe_error("无法连接币安", &error));
    }
    match client.get_account().await {
        Ok(_) => json_ok("连接成功：API Key 有效，可读取合约账户".into()),
        Err(error) => json_err(sanitized_probe_error("账户验证失败", &error)),
    }
}

fn sanitized_probe_error(prefix: &str, error: &anyhow::Error) -> String {
    if error.downcast_ref::<reqwest::Error>().is_some() {
        return format!("{prefix}：网络请求失败，请检查网络后重试");
    }
    format!("{prefix}：{error}")
}

/// Send one fixed probe message with the (possibly unsaved) Telegram credentials.
async fn test_telegram_handler(
    State(handle): State<RuntimeHandle>,
    Json(payload): Json<TestTelegramPayload>,
) -> Json<ApiResponse<String>> {
    let config = match load_probe_config(
        &handle,
        &handle.probes.telegram,
        payload,
        |saved, payload| resolve_telegram_probe(&saved.telegram, payload),
    )
    .await
    {
        Ok(config) => config,
        Err(response) => return response,
    };
    let Some(notifier) = handle
        .states
        .read()
        .await
        .first()
        .map(|state| state.telegram_notifier().clone())
    else {
        return json_err("没有活跃的交易对实例");
    };
    match notifier.send_test_message(&config).await {
        Ok(()) => json_ok("测试消息已发送，请在 Telegram 中查看".into()),
        Err(error) => json_err(format!("测试消息发送失败：{error}")),
    }
}

async fn remove_pair_handler(
    State(handle): State<RuntimeHandle>,
    Path(symbol): Path<String>,
    Json(payload): Json<ClearTradeHistoryRequest>,
) -> Json<ApiResponse<String>> {
    if !payload.confirm {
        return json_err("请确认移除交易对；移除前须平仓，历史记录会保留");
    }
    match handle.remove_pair(symbol.clone()).await {
        Ok(()) => json_ok_msg(symbol, "交易对已移除，历史记录保留"),
        Err(error) => json_err(error.to_string()),
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
    let active = handle.active_server.clone();
    map_saved_config(&handle, "无法读取服务设置", |config| {
        ServerConfigView {
            version: crate::db::config_version(&config.server),
            saved: config.server,
            active,
        }
    })
    .await
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
        Ok(()) => json_ok_msg(
            ServerConfigView {
                version: crate::db::config_version(&submitted),
                saved: submitted,
                active: handle.active_server,
            },
            "服务设置已保存，重启程序后生效",
        ),
        Err(error) => json_err(error.to_string()),
    }
}

async fn run_clear_trade_history(
    states: &[Arc<AppState>],
    payload: ClearTradeHistoryRequest,
    clear: impl Future<Output = anyhow::Result<usize>>,
    log_msg: impl FnOnce(usize) -> String,
    response_msg: impl FnOnce(usize) -> String,
) -> (StatusCode, Json<ApiResponse<ClearTradeHistoryResponse>>) {
    if !payload.confirm {
        return status_err(
            StatusCode::BAD_REQUEST,
            "请先确认清理全部交易对和运行模式的历史成交及累计统计",
        );
    }
    match clear.await {
        Ok(deleted_trades) => {
            // Invalidate every client's cached statistics after the shared database cleanup.
            let log_message = log_msg(deleted_trades);
            let mut primary_snapshot = None;
            for state in states {
                state.add_log("WARN", log_message.clone()).await;
                let snapshot = state.broadcast_snapshot().await;
                if primary_snapshot.is_none() {
                    primary_snapshot = Some(snapshot);
                }
            }
            let Some(snapshot) = primary_snapshot else {
                return status_err(StatusCode::INTERNAL_SERVER_ERROR, "没有可用的交易对状态");
            };
            (
                StatusCode::OK,
                json_ok_msg(
                    ClearTradeHistoryResponse {
                        deleted_trades,
                        snapshot,
                    },
                    response_msg(deleted_trades),
                ),
            )
        }
        Err(error) => {
            error!("Trade history cleanup failed: {}", error);
            status_err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "历史记录清理失败，请稍后重试或查看服务器日志",
            )
        }
    }
}

async fn clear_multi_trade_history_handler(
    State(states): State<Arc<Vec<Arc<AppState>>>>,
    Json(payload): Json<ClearTradeHistoryRequest>,
) -> (StatusCode, Json<ApiResponse<ClearTradeHistoryResponse>>) {
    run_clear_trade_history(
        &states,
        payload,
        AppState::clear_trade_history_for(&states),
        |deleted_trades| format!("已清理全部历史成交记录，共 {} 笔", deleted_trades),
        |deleted_trades| {
            format!(
                "已清理 {} 笔历史成交记录，全部交易对累计统计已重置",
                deleted_trades
            )
        },
    )
    .await
}

async fn get_auth_status_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> (StatusCode, Json<ApiResponse<AuthStatusResponse>>) {
    let token = extract_token(&headers);
    let initialized = state.is_auth_configured();
    let result = state
        .db
        .run_blocking(move |db| {
            let authenticated = !initialized
                || match token {
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
        Ok(status) => (StatusCode::OK, json_ok(status)),
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
    status_err(StatusCode::SERVICE_UNAVAILABLE, "认证服务繁忙，请稍后重试")
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
        return status_err(
            StatusCode::TOO_MANY_REQUESTS,
            "登录尝试过于频繁，请一分钟后重试",
        );
    }
    let Some(admin_auth) = state.admin_auth() else {
        warn!("Admin login rejected: no password configured for this process");
        return status_err(StatusCode::BAD_REQUEST, "未启用管理员密码认证，无需登录");
    };

    let password = payload.password;
    let verified = match tokio::task::spawn_blocking(move || {
        let _permit = permit;
        admin_auth.verify(&password)
    })
    .await
    {
        Ok(verified) => verified,
        Err(error) => {
            error!("Admin password verification task failed: {}", error);
            return auth_service_error();
        }
    };
    if !verified {
        warn!("Failed admin authentication attempt: incorrect password");
        return status_err(StatusCode::UNAUTHORIZED, "管理员密码错误，请重新输入");
    }

    let token = crate::auth::generate_session_token();
    match state.create_session(&token).await {
        Ok(()) => {
            state.clear_login_attempts(peer_ip(address)).await;
            info!("Admin authentication successful");
            (
                StatusCode::OK,
                json_ok_msg(AuthTokenResponse { token }, "管理员身份认证成功！"),
            )
        }
        Err(error) => {
            error!("Failed to create admin session: {}", error);
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
            return status_err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "退出登录失败，请重试或查看服务器日志",
            );
        }
    }
    (StatusCode::OK, json_ok_msg((), "已安全退出登录"))
}

async fn get_status_handler(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<crate::types::BotSnapshot>> {
    json_ok(state.snapshot().await)
}

async fn get_orders_handler(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<Vec<GridOrder>>> {
    json_ok(state.sorted_active_orders().await)
}

async fn get_trades_handler(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<Vec<TradeRecord>>> {
    let config = state.config.read().await;
    let symbol = config.exchange.symbol.clone();
    let mode = config.exchange.trading_mode();
    drop(config);
    let query_symbol = symbol.clone();
    let trades = match state
        .db
        .run_blocking(move |db| db.get_recent_trades_for(&query_symbol, mode, 100))
        .await
    {
        Ok(t) => t,
        Err(e) => {
            error!("Failed to fetch trades from SQLite: {}", e);
            state
                .recent_trades
                .read()
                .await
                .iter()
                .filter(|t| t.symbol == symbol && t.mode == mode)
                .rev()
                .take(100)
                .cloned()
                .collect()
        }
    };
    json_ok(trades)
}

async fn get_logs_handler(State(state): State<Arc<AppState>>) -> Json<ApiResponse<Vec<LogEntry>>> {
    let logs: Vec<LogEntry> = state.recent_logs.read().await.iter().cloned().collect();
    json_ok(logs)
}

async fn clear_trade_history_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ClearTradeHistoryRequest>,
) -> (StatusCode, Json<ApiResponse<ClearTradeHistoryResponse>>) {
    run_clear_trade_history(
        std::slice::from_ref(&state),
        payload,
        state.clear_trade_history(),
        |deleted_trades| {
            format!(
                "已清理全部历史成交记录，共 {} 笔，累计统计已重置",
                deleted_trades
            )
        },
        |deleted_trades| format!("已清理 {} 笔历史成交记录，累计统计已重置", deleted_trades),
    )
    .await
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
            status_err::<()>(
                StatusCode::INTERNAL_SERVER_ERROR,
                "数据库导出失败，请稍后重试或查看服务器日志",
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
    let rules = state.rules.read().await;
    if rules.symbol.eq_ignore_ascii_case(&config.exchange.symbol) {
        view.tick_size = (rules.tick_size > rust_decimal::Decimal::ZERO).then_some(rules.tick_size);
        view.step_size = (rules.step_size > rust_decimal::Decimal::ZERO).then_some(rules.step_size);
        view.min_qty = (rules.min_qty > rust_decimal::Decimal::ZERO).then_some(rules.min_qty);
        view.min_notional =
            (rules.min_notional > rust_decimal::Decimal::ZERO).then_some(rules.min_notional);
    }
    json_ok(view)
}

fn pair_config_version(config: &AppConfig, instance: u128) -> String {
    crate::db::config_version(&(
        instance,
        &config.exchange,
        &config.grid,
        &config.telegram,
        config.global_max_position_notional,
        config.pause_on_zero_available_margin,
        config.min_available_margin_pct,
    ))
}

fn mask_api_key_preview(api_key: &str) -> String {
    let k = api_key.trim();
    if k.is_empty() {
        return "未配置".to_string();
    }
    let mut prefix_end = 0;
    let mut tail = [0usize; 4];
    let mut count = 0usize;
    for (idx, _) in k.char_indices() {
        if count == 4 {
            prefix_end = idx;
        }
        tail[count % 4] = idx;
        count += 1;
    }
    if count > 8 {
        let suffix_start = tail[count % 4];
        format!("{}...{}", &k[..prefix_end], &k[suffix_start..])
    } else {
        "****".to_string()
    }
}

fn web_config_view(config: &AppConfig) -> WebConfigView {
    let global = web_global_config_view(&crate::config::GlobalConfig::from_app(config));

    WebConfigView {
        config_version: String::new(),
        symbol: config.exchange.symbol.clone(),
        direction: config.grid.direction,
        grid_interval: config.grid.grid_interval,
        order_amount_usdc: config.grid.order_amount_usdc,
        buy_window: config.grid.buy_window,
        sell_window: config.grid.sell_window,
        post_only: config.grid.post_only,
        recv_window: global.recv_window,
        sync_interval_secs: global.sync_interval_secs,
        has_api_key: global.has_api_key,
        has_api_secret: global.has_api_secret,
        api_key_preview: global.api_key_preview,
        telegram_enabled: global.telegram_enabled,
        has_telegram_bot_token: global.has_telegram_bot_token,
        telegram_chat_id: global.telegram_chat_id,
        min_price: config.grid.min_price,
        max_price: config.grid.max_price,
        max_position_usdc: config.grid.max_position_usdc,
        pause_on_max_position: config.grid.pause_on_max_position,
        cancel_opening_on_max_position: config.grid.cancel_opening_on_max_position,
        min_liquidation_distance_pct: config.grid.min_liquidation_distance_pct,
        tick_size: None,
        step_size: None,
        min_qty: None,
        min_notional: None,
    }
}

fn apply_config_payload(
    mut current_config: AppConfig,
    payload: UpdateConfigPayload,
) -> anyhow::Result<AppConfig> {
    anyhow::ensure!(
        payload.dry_run.is_none(),
        "运行模式已由 API Key 决定，请刷新页面后重试"
    );
    anyhow::ensure!(
        !payload.is_testnet.unwrap_or(false),
        "测试网模式已移除，运行模式由 API Key 决定"
    );
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
    current_config.grid.min_price = payload.min_price;
    current_config.grid.max_price = payload.max_price;
    current_config.grid.max_position_usdc = payload.max_position_usdc;
    if let Some(enabled) = payload.pause_on_max_position {
        current_config.grid.pause_on_max_position = enabled;
    }
    if let Some(enabled) = payload.cancel_opening_on_max_position {
        current_config.grid.cancel_opening_on_max_position = enabled;
    }
    current_config.grid.min_liquidation_distance_pct = payload.min_liquidation_distance_pct;

    let mut global = crate::config::GlobalConfig::from_app(&current_config);
    UpdateGlobalConfigPayload {
        recv_window: payload.recv_window,
        sync_interval_secs: payload.sync_interval_secs,
        api_key: payload.api_key,
        api_secret: payload.api_secret,
        clear_api_credentials: payload.clear_api_credentials,
        clear_telegram_credentials: payload.clear_telegram_credentials,
        telegram_enabled: payload.telegram_enabled,
        telegram_bot_token: payload.telegram_bot_token,
        telegram_chat_id: payload.telegram_chat_id,
        ..Default::default()
    }
    .apply_to(&mut global);
    global.apply_to(&mut current_config);
    current_config.validate()?;
    Ok(current_config)
}

async fn post_config_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<UpdateConfigPayload>,
) -> (StatusCode, Json<ApiResponse<WebConfigView>>) {
    match tokio::spawn(async move { apply_pair_config(state, payload).await }).await {
        Ok(result) => result,
        Err(error) => status_err(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("配置更新任务异常退出: {}", error),
        ),
    }
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
        return status_err(
            StatusCode::BAD_REQUEST,
            "更新交易对设置时必须提供 baseline_version",
        );
    };
    if version != pair_config_version(&current, state.config_instance) {
        return status_err(
            StatusCode::CONFLICT,
            "交易对设置已被其他请求修改，请刷新后重试",
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
        return json_err("连接和 Telegram 通知请在全局设置中保存");
    }
    let current_config = match apply_config_payload(current, payload) {
        Ok(config) => config,
        Err(error) => return json_err(error.to_string()),
    };
    if let Some(symbol) = state.pair_scope() {
        if symbol != current_config.exchange.symbol {
            return json_err("交易对不能直接重命名，请在页面中添加新交易对");
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
        return json_err(format!("策略更新通知失败: {}", e));
    }
    match reply_rx.await {
        Ok(Ok(())) => info!("Configuration applied and saved to {}", state.db.path()),
        Ok(Err(error)) => return json_err(error),
        Err(error) => return json_err(format!("策略未确认配置变更: {}", error)),
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
        Err(message) => return json_err(message),
    };
    let result = tokio::spawn(dispatch_control(state, action)).await;
    match result {
        Ok(response) => response,
        Err(error) => json_err(format!("控制任务失败: {}", error)),
    }
}

async fn prepare_control(state: &Arc<AppState>, action: &BotControlAction) -> Result<(), String> {
    let pause = matches!(
        action,
        BotControlAction::Pause | BotControlAction::CancelAll
    );
    let state = state.clone();
    tokio::spawn(async move {
        let _control = state.socket_control_lock.read().await;
        if state.is_retired() {
            return Err("交易对已移除，无法执行操作".into());
        }
        if pause {
            state
                .pause_trading()
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
        return json_err(message);
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
        return json_err("交易对已移除，无法执行操作");
    }
    let reservation = tokio::select! {
        biased;
        _ = retired.changed() => return json_err("交易对已移除，无法执行操作"),
        result = state.action_tx.reserve() => result,
    };
    let permit = match reservation {
        Ok(permit) => permit,
        Err(e) => {
            error!("Failed to dispatch action: {}", e);
            return json_err(format!("Failed to send action: {}", e));
        }
    };
    let control = state.socket_control_lock.read().await;
    if state.is_retired() {
        return json_err("交易对已移除，无法执行操作");
    }
    // Checking immediately before submission closes the channel-wait window.
    // Revocation can still race with this check after it completes; actions
    // already submitted to the engine cannot be recalled here.
    if pending_pause.is_none() {
        if let Some(token) = &socket_token {
            if !state.is_token_valid(token).await {
                return json_err("会话已失效");
            }
        }
        if *state.pending_control_pauses.borrow() != 0 {
            return json_err("暂停或撤单正在提交，请稍后重试");
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
            _ = retired.changed() => return json_err("交易对已移除，无法执行操作"),
            _ = async {
                if let Some(results) = &socket_results { results.closed().await; }
                else { std::future::pending::<()>().await; }
            } => return json_err("控制连接已关闭"),
            response = reply => response,
        };
        match response {
            Ok(Ok(())) => {}
            Ok(Err(message)) => return json_err(message),
            Err(error) => return json_err(format!("策略未确认控制操作: {}", error)),
        }
    }

    json_ok(format!("Action {} triggered successfully", action_name))
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

    async fn frame(&self) -> AccountFrame {
        let generated_at = chrono::Utc::now();
        let states = self.states().await;
        let pairs =
            futures_util::future::join_all(states.iter().map(|state| state.account_snapshot()))
                .await;
        AccountFrame {
            generated_at,
            pairs,
        }
    }
}

type AccountFrameSender = tokio::sync::watch::Sender<Option<Arc<String>>>;

// Router-owned: clients of this registry share collection and JSON encoding.
// The sender retains no receiver, so the sampler exits when the last client leaves.
struct AccountHub {
    source: AccountSource,
    sampler: Arc<tokio::sync::Mutex<Option<AccountFrameSender>>>,
}

impl AccountHub {
    fn new(source: AccountSource) -> Arc<Self> {
        Arc::new(Self {
            source,
            sampler: Arc::new(tokio::sync::Mutex::new(None)),
        })
    }

    async fn subscribe(&self) -> tokio::sync::watch::Receiver<Option<Arc<String>>> {
        let mut sampler = self.sampler.lock().await;
        if let Some(sender) = sampler
            .as_ref()
            .filter(|sender| sender.receiver_count() > 0)
        {
            return sender.subscribe();
        }
        let (sender, receiver) = tokio::sync::watch::channel(None);
        *sampler = Some(sender.clone());
        let source = self.source.clone();
        let lifecycle = self.sampler.clone();
        tokio::spawn(async move {
            let mut ticks = tokio::time::interval(std::time::Duration::from_secs(1));
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    _ = sender.closed() => {
                        // A subscriber may arrive after closed() becomes ready.
                        // Serialize retirement with subscribe before exiting.
                        let mut current = lifecycle.lock().await;
                        if sender.receiver_count() > 0 { continue; }
                        if current.as_ref().is_some_and(|active| active.same_channel(&sender)) {
                            *current = None;
                        }
                        break;
                    }
                    message = async {
                        ticks.tick().await;
                        serde_json::to_string(&AccountMessage::Accounts(source.frame().await))
                    } => {
                        let Ok(message) = message else {
                            let mut current = lifecycle.lock().await;
                            if current.as_ref().is_some_and(|active| active.same_channel(&sender)) {
                                *current = None;
                            }
                            break;
                        };
                        // If the last subscriber left during collection, the
                        // closed branch performs the synchronized shutdown.
                        let _ = sender.send(Some(Arc::new(message)));
                    }
                }
            }
        });
        receiver
    }
}

async fn get_accounts_handler(
    Extension(hub): Extension<Arc<AccountHub>>,
) -> Json<ApiResponse<AccountFrame>> {
    json_ok(hub.source.frame().await)
}

async fn accounts_ws_handler(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    Extension(hub): Extension<Arc<AccountHub>>,
) -> Response {
    upgrade_accounts_socket(ws, headers, hub).await
}

async fn upgrade_accounts_socket(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    hub: Arc<AccountHub>,
) -> Response {
    let token = websocket_token(&headers).to_string();
    let states = hub.source.states().await;
    let Some(primary) = states.first().cloned() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    if !primary.is_token_valid(&token).await {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    ws.protocols(["auth"]).on_upgrade(move |socket| async move {
        let mut retired = primary.subscribe_retirement();
        if *retired.borrow() { return; }
        let mut frames = hub.subscribe().await;
        let (mut sender, mut receiver) = socket.split();
        let mut check_auth = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            let frame = frames.borrow_and_update().clone();
            if let Some(frame) = frame {
                if !primary.is_token_valid(&token).await { break; }
                if !matches!(tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    send_until_retired(&mut sender, Message::Text((*frame).clone()), &mut retired),
                ).await, Ok(Some(Ok(())))) { break; }
            }
            loop {
                tokio::select! {
                    biased;
                    _ = retired.changed() => return,
                    _ = check_auth.tick() => {
                        if !primary.is_token_valid(&token).await { return; }
                    }
                    changed = frames.changed() => {
                        if changed.is_err() { return; }
                        break;
                    }
                    message = receiver.next() => {
                        match message {
                            Some(Ok(Message::Ping(payload))) => {
                                if !matches!(tokio::time::timeout(
                                    std::time::Duration::from_secs(5),
                                    send_until_retired(&mut sender, Message::Pong(payload), &mut retired),
                                ).await, Ok(Some(Ok(())))) { return; }
                            }
                            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return,
                            _ => {}
                        }
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

fn control_ws_message(action: Option<&str>, response: ApiResponse<String>) -> String {
    serde_json::json!({
        "type": "control",
        "action": action,
        "success": response.success,
        "data": response.data,
        "message": response.message,
    })
    .to_string()
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
                let _ = results.send(control_ws_message(Some(&name), response));
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
                        let _ = result_tx.send(control_ws_message(
                            name.as_deref(),
                            ApiResponse::err(message),
                        ));
                        continue;
                    }
                };
                let pending_pause = PendingControlPause::new(&state_clone, &action.0);
                let prepare_state = state_clone.clone();
                let prepare_results = result_tx.clone();
                let prepare_queue = control_tx.clone();
                let preparation = tokio::spawn(async move {
                    if let Err(message) = prepare_control(&prepare_state, &action.0).await {
                        let _ = prepare_results.send(control_ws_message(
                            name.as_deref(),
                            ApiResponse::err(message),
                        ));
                    } else if let Some(action_name) = name {
                        let _ = prepare_queue.send((action_name, action, pending_pause));
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

    #[test]
    fn probe_gate_allows_one_call_per_cooldown_and_reports_the_wait() {
        let gate = ProbeGate::new();
        let start = std::time::Instant::now();
        assert_eq!(gate.enter(start), Ok(()));
        assert_eq!(
            gate.enter(start + std::time::Duration::from_millis(400)),
            Err(3)
        );
        assert_eq!(
            gate.enter(start + PROBE_COOLDOWN - std::time::Duration::from_millis(10)),
            Err(1)
        );
        assert_eq!(gate.enter(start + PROBE_COOLDOWN), Ok(()));
    }
    use crate::config::AppConfig;
    use crate::db::Database;
    use crate::types::BotStatus;

    fn test_admin_auth() -> Option<Arc<crate::auth::AdminAuth>> {
        Some(Arc::new(
            crate::auth::AdminAuth::from_password("test-password").unwrap(),
        ))
    }

    fn auth_state(
        config: AppConfig,
        db: Arc<Database>,
        action_tx: tokio::sync::mpsc::Sender<BotControlAction>,
    ) -> Arc<AppState> {
        AppState::new_with_admin_auth(config, db, action_tx, test_admin_auth())
    }

    fn auth_pair_state(
        config: AppConfig,
        db: Arc<Database>,
        action_tx: tokio::sync::mpsc::Sender<BotControlAction>,
        trade_lock: Arc<tokio::sync::Mutex<()>>,
    ) -> Arc<AppState> {
        AppState::new_for_pair_with_admin_auth(config, db, action_tx, trade_lock, test_admin_auth())
    }

    #[tokio::test]
    async fn config_view_omits_tick_size_until_symbol_rules_match_pair_symbol() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (tx, _) = tokio::sync::mpsc::channel(1);
        let state = AppState::new(AppConfig::default(), db, tx);
        let initial = get_config_handler(State(state.clone())).await;
        assert_eq!(initial.0.data.unwrap().tick_size, None);
        {
            let mut rules = state.rules.write().await;
            rules.symbol = "SOLUSDC".into();
            rules.tick_size = rust_decimal_macros::dec!(0.05);
        }
        let loaded = get_config_handler(State(state)).await.0.data.unwrap();
        assert_eq!(loaded.tick_size, Some(rust_decimal_macros::dec!(0.05)));
        assert_eq!(loaded.step_size, Some(rust_decimal_macros::dec!(0.01)));
        assert_eq!(loaded.min_qty, Some(rust_decimal_macros::dec!(0.01)));
        assert_eq!(loaded.min_notional, Some(rust_decimal_macros::dec!(5.0)));
    }
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
    async fn symbol_preview_returns_mark_price_and_rules_and_validates_inputs() {
        let app = Router::new()
            .route(
                "/fapi/v1/exchangeInfo",
                get(|| async {
                    Json(serde_json::json!({
                        "symbols": [{
                            "symbol": "SOLUSDC",
                            "status": "TRADING",
                            "baseAsset": "SOL",
                            "quoteAsset": "USDC",
                            "pricePrecision": 2,
                            "quantityPrecision": 2,
                            "filters": [
                                {"filterType": "PRICE_FILTER", "minPrice": "0.10", "maxPrice": "100000.00", "tickSize": "0.01"},
                                {"filterType": "LOT_SIZE", "minQty": "0.01", "maxQty": "100000.00", "stepSize": "0.01"},
                                {"filterType": "MIN_NOTIONAL", "notional": "5.0"}
                            ]
                        }]
                    }))
                }),
            )
            .route(
                "/fapi/v1/ticker/price",
                get(|| async {
                    Json(serde_json::json!({
                        "symbol": "SOLUSDC",
                        "price": "142.40",
                        "time": 1700000000000i64
                    }))
                }),
            )
            .route(
                "/fapi/v1/premiumIndex",
                get(|| async {
                    Json(serde_json::json!({
                        "symbol": "SOLUSDC",
                        "markPrice": "142.35",
                        "indexPrice": "142.30",
                        "lastFundingRate": "0.0001",
                        "nextFundingTime": 1700000000000i64,
                        "time": 1700000000000i64
                    }))
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = BinanceFuturesClient::with_base_url(&AppConfig::default().exchange, url);
        let (status, Json(response)) = fetch_exchange_symbol_preview(&client, "SOLUSDC").await;
        assert_eq!(status, StatusCode::OK);
        let preview = response.data.expect("preview data");
        assert_eq!(preview.symbol, "SOLUSDC");
        assert_eq!(preview.last_price, Some(rust_decimal_macros::dec!(142.40)));
        assert_eq!(preview.mark_price, Some(rust_decimal_macros::dec!(142.35)));
        assert_eq!(preview.tick_size, rust_decimal_macros::dec!(0.01));
        assert_eq!(preview.step_size, rust_decimal_macros::dec!(0.01));
        assert_eq!(preview.min_qty, rust_decimal_macros::dec!(0.01));
        assert_eq!(preview.min_notional, rust_decimal_macros::dec!(5.0));
        server.abort();

        let db = Arc::new(Database::open(":memory:").unwrap());
        db.create_session("symbols-test-token").unwrap();
        let (tx, _) = tokio::sync::mpsc::channel(1);
        let router = create_router(auth_state(AppConfig::default(), db, tx));
        let unauth = router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/exchange/symbol-preview?environment=mainnet&symbol=SOLUSDC")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);

        for uri in [
            "/api/exchange/symbol-preview?environment=testnet&symbol=SOLUSDC",
            "/api/exchange/symbol-preview?environment=mainnet&symbol=",
            "/api/exchange/symbol-preview?environment=mainnet&symbol=SOL-USDC",
            "/api/exchange/symbol-preview?environment=mainnet&symbol=AB",
            "/api/exchange/symbol-preview?environment=mainnet&symbol=ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567",
        ] {
            let bad = router
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
            assert_eq!(bad.status(), StatusCode::BAD_REQUEST, "{uri}");
        }
    }

    #[tokio::test]
    async fn symbols_route_requires_auth_and_rejects_invalid_environment() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        db.create_session("symbols-test-token").unwrap();
        let (tx, _) = tokio::sync::mpsc::channel(1);
        let router = create_router(auth_state(AppConfig::default(), db, tx));
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
            "/api/exchange/symbols?environment=testnet",
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

    #[test]
    fn account_routes_can_be_constructed_without_a_tokio_runtime() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (tx, _) = tokio::sync::mpsc::channel(1);
        let state = AppState::new(AppConfig::default(), db, tx);
        let _single = create_router(state.clone());
        let _fixed = create_multi_router(vec![state]);
    }

    #[tokio::test]
    async fn account_sampler_shares_frames_cancels_idle_collection_and_restarts() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (tx, _) = tokio::sync::mpsc::channel(1);
        let trade_lock = Arc::new(tokio::sync::Mutex::new(()));
        let state = AppState::new_for_pair(AppConfig::default(), db, tx, trade_lock.clone());
        let hub = AccountHub::new(AccountSource::Fixed(Arc::new(vec![state.clone()])));
        assert!(hub.sampler.lock().await.is_none());
        let mut first = hub.subscribe().await;
        let second = hub.subscribe().await;
        tokio::time::timeout(std::time::Duration::from_secs(2), first.changed())
            .await
            .unwrap()
            .unwrap();
        let frame = first.borrow_and_update().clone().unwrap();
        assert!(Arc::ptr_eq(&frame, second.borrow().as_ref().unwrap()));
        let decoded: serde_json::Value = serde_json::from_str(&frame).unwrap();
        assert!(decoded["data"]["generated_at"].is_string());
        drop(first);
        drop(second);

        // Force collection to wait after taking the commit lock, then remove
        // its only subscriber. Cancellation must release that lock promptly.
        let config = state.config.write().await;
        let pending = hub.subscribe().await;
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if trade_lock.try_lock().is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(pending);
        let commit = tokio::time::timeout(std::time::Duration::from_secs(2), trade_lock.lock())
            .await
            .unwrap();
        drop(commit);
        drop(config);

        state.position.write().await.unrealized_pnl = rust_decimal_macros::dec!(42);
        let mut restarted = hub.subscribe().await;
        tokio::time::timeout(std::time::Duration::from_secs(2), restarted.changed())
            .await
            .unwrap()
            .unwrap();
        let decoded: serde_json::Value =
            serde_json::from_str(restarted.borrow().as_ref().unwrap()).unwrap();
        assert_eq!(
            decoded["data"]["pairs"][0]["snapshot"]["position"]["unrealized_pnl"],
            "42"
        );
    }

    #[tokio::test]
    async fn account_stream_authenticates_pushes_live_balances_and_rejects_revoked_sessions() {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (tx, _) = tokio::sync::mpsc::channel(1);
        let mut config = AppConfig::default();
        config.exchange.api_key = "account-stream-private-key".into();
        config.exchange.api_secret = "account-stream-private-secret".into();
        let state = auth_state(config.clone(), db.clone(), tx.clone());
        let scoped = auth_pair_state(
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
            let state = auth_state(AppConfig::default(), db.clone(), tx);
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
            let state = auth_state(AppConfig::default(), db.clone(), tx);
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
        let (mut accounts, _) =
            tokio_tungstenite::connect_async(format!("ws://{address}/api/accounts/ws"))
                .await
                .unwrap();
        let account = tokio::time::timeout(std::time::Duration::from_secs(3), accounts.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let account: serde_json::Value = serde_json::from_str(account.to_text().unwrap()).unwrap();
        assert_eq!(account["type"], "accounts");

        state.retire();
        let closed = tokio::time::timeout(std::time::Duration::from_secs(3), socket.next())
            .await
            .expect("retired state must close its socket");
        assert!(matches!(
            closed,
            None | Some(Ok(Message::Close(_))) | Some(Err(_))
        ));
        let closed = tokio::time::timeout(std::time::Duration::from_secs(3), accounts.next())
            .await
            .expect("retirement must also close the account stream");
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
        let state = auth_state(config, db, tx);
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
        let summary = &response["data"]["pairs"][0]["snapshot"];
        for field in [
            "active_orders",
            "recent_trades",
            "recent_logs",
            "hourly_trade_stats",
        ] {
            assert!(
                summary.get(field).is_none(),
                "overview retained heavy field {field}"
            );
        }
        assert!(summary["today_trade_counts"]["buy"].is_u64());
        assert!(response["data"]["recent_trades"]
            .as_array()
            .unwrap()
            .is_empty());
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
        for (symbol, paper) in [("SOLUSDC", true), ("BTCUSDT", false)] {
            let mut config = AppConfig::default();
            config.exchange.symbol = symbol.into();
            if !paper {
                config.exchange.api_key = "live-key".into();
                config.exchange.api_secret = "live-secret".into();
            }
            let (tx, _) = tokio::sync::mpsc::channel(1);
            states.push(auth_pair_state(config, db.clone(), tx, lock.clone()));
        }
        let account_time = chrono::Utc::now() - chrono::Duration::minutes(2);
        states[0].ticker.write().await.mark_price = rust_decimal_macros::dec!(123);
        states[1].ticker.write().await.mark_price = rust_decimal_macros::dec!(67890);
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
        assert_eq!(overview.pairs[1].mode, TradingMode::Live);
        assert_eq!(overview.pairs[0].snapshot.symbol, "SOLUSDC");
        assert_eq!(overview.pairs[1].snapshot.symbol, "BTCUSDT");
        assert_eq!(
            overview.pairs[0].snapshot.ticker.mark_price,
            rust_decimal_macros::dec!(123)
        );
        assert_eq!(
            overview.pairs[1].snapshot.ticker.mark_price,
            rust_decimal_macros::dec!(67890)
        );
        assert_eq!(overview.pairs[1].snapshot.account.update_time, account_time);
        for pair in response["data"]["pairs"].as_array().unwrap() {
            assert!(pair["snapshot"].get("recent_logs").is_none());
            assert!(pair["snapshot"].get("active_orders").is_none());
        }

        // No WebSocket subscribers are required for live fields to refresh.
        // Only chart queries are cached between overview polls.
        assert_eq!(db.hourly_stats_query_count(), 2);
        states[1].ticker.write().await.mark_price = rust_decimal_macros::dec!(67900);
        states[1].account.write().await.update_time = chrono::Utc::now();
        let (_, response) = overview_response(&router, true).await;
        let refreshed: OverviewSnapshot = serde_json::from_value(response["data"].clone()).unwrap();
        assert_eq!(
            refreshed.pairs[1].snapshot.ticker.mark_price,
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

    #[test]
    fn retired_testnet_request_is_rejected_instead_of_trading_live() {
        let payload: UpdateConfigPayload = serde_json::from_value(serde_json::json!({
            "symbol": "SOLUSDC", "grid_interval": "0.1", "order_amount_usdc": "100",
            "buy_window": 5, "sell_window": 5, "is_testnet": true
        }))
        .unwrap();
        let error = apply_config_payload(AppConfig::default(), payload).unwrap_err();
        assert!(error.to_string().contains("测试网模式已移除"));
    }

    #[test]
    fn retired_dry_run_request_is_rejected() {
        let payload: UpdateConfigPayload = serde_json::from_value(serde_json::json!({
            "symbol": "SOLUSDC", "grid_interval": "0.1", "order_amount_usdc": "100",
            "buy_window": 5, "sell_window": 5, "dry_run": true
        }))
        .unwrap();
        let error = apply_config_payload(AppConfig::default(), payload).unwrap_err();
        assert!(error.to_string().contains("API Key"));
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

    #[test]
    fn mask_api_key_preview_counts_characters_instead_of_utf8_bytes() {
        assert_eq!(mask_api_key_preview(""), "未配置");
        assert_eq!(mask_api_key_preview("   "), "未配置");
        assert_eq!(mask_api_key_preview("abcd1234"), "****");
        // 4 CJK characters = 12 UTF-8 bytes (> 8 bytes, <= 8 chars): must stay masked.
        assert_eq!(mask_api_key_preview("密钥测试"), "****");
        assert_eq!(mask_api_key_preview("一二三四五六七八"), "****");
        assert_eq!(
            mask_api_key_preview("一二三四五六七八九"),
            "一二三四...六七八九"
        );
        assert_eq!(mask_api_key_preview("abcdefghijkl"), "abcd...ijkl");
    }

    #[tokio::test]
    async fn get_orders_handler_returns_orders_sorted_by_price_desc() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (action_tx, _action_rx) = tokio::sync::mpsc::channel(1);
        let state = AppState::new(AppConfig::default(), db, action_tx);
        {
            let mut orders = state.active_orders.write().await;
            for (id, price) in [
                ("b", rust_decimal_macros::dec!(100)),
                ("a", rust_decimal_macros::dec!(200)),
                ("c", rust_decimal_macros::dec!(100)),
            ] {
                orders.insert(
                    id.into(),
                    crate::types::GridOrder {
                        client_order_id: id.into(),
                        order_id: None,
                        symbol: "SOLUSDC".into(),
                        side: crate::types::OrderSide::Buy,
                        price,
                        quantity: rust_decimal_macros::dec!(1),
                        amount_usdc: price,
                        status: crate::types::OrderStatus::New,
                        created_at: chrono::Utc::now(),
                        updated_at: chrono::Utc::now(),
                        grid_level: 0,
                        paired_client_order_id: None,
                        is_take_profit: false,
                        purpose: crate::types::OrderPurpose::Grid,
                        merge_sources: Vec::new(),
                    },
                );
            }
        }
        let response = get_orders_handler(State(state)).await.0;
        let ids: Vec<_> = response
            .data
            .unwrap()
            .into_iter()
            .map(|o| o.client_order_id)
            .collect();
        assert_eq!(ids, vec!["a", "b", "c"]);
    }
}
