use crate::exchange::rate_limit::RateLimiter;
use crate::runtime::RuntimeHandle;
use crate::server::state::{AppState, AuthContext};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::StatusCode;
use axum::middleware;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Extension, Json, Router};
use std::sync::Arc;
use tower::ServiceExt;

mod accounts_ws;
mod auth;
mod config_probes;
mod control;
mod database;
mod exchange_preview;
mod probes;
mod queries;
mod ws;

#[cfg(test)]
mod tests;

pub(crate) use crate::types::ApiResponse;
#[cfg(test)]
pub(crate) use control::ControlRequest;
pub(crate) use database::ClearTradeHistoryRequest;
#[cfg(test)]
pub(crate) use probes::ProbeGate;
pub(crate) use probes::{sanitized_probe_error, ProbeGates};

use accounts_ws::{accounts_ws_handler, get_accounts_handler, AccountHub, AccountSource};
use auth::{
    get_auth_status_handler, post_auth_login_handler, post_auth_logout_handler,
    require_auth_middleware,
};
use config_probes::{
    add_pair_handler, get_config_handler, get_global_config_handler, get_server_config_handler,
    post_config_handler, post_global_config_handler, post_managed_config_handler,
    post_server_config_handler, remove_pair_handler,
};
use control::post_control_handler;
use database::{
    clear_managed_history_handler, clear_multi_trade_history_handler, clear_trade_history_handler,
    dispatch_managed_database_pair, export_database_handler, export_selected_database_handler,
    managed_database_clear_handler, multi_database_clear_handler, scoped_clear_handler,
    scoped_export_handler, single_database_clear_handler, single_pair_clear_handler,
    single_pair_export_handler,
};
use exchange_preview::{
    exchange_symbol_preview_handler, exchange_symbols_handler, ExchangePreviewService,
};
use probes::{test_exchange_handler, test_telegram_handler};
use queries::{
    get_logs_handler, get_managed_overview_handler, get_managed_pairs_handler, get_orders_handler,
    get_overview_handler, get_overview_history_handler, get_pairs_handler,
    get_single_overview_handler, get_single_pair_handler, get_status_handler, get_trades_handler,
};
use ws::ws_handler;

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

async fn apply_security_headers(mut response: Response) -> Response {
    crate::metrics::record_http_status(response.status().as_u16());
    if response.status() != StatusCode::SWITCHING_PROTOCOLS {
        let headers = response.headers_mut();
        headers
            .entry(axum::http::header::CACHE_CONTROL)
            .or_insert(axum::http::HeaderValue::from_static("no-store"));
        headers
            .entry(axum::http::header::X_CONTENT_TYPE_OPTIONS)
            .or_insert(axum::http::HeaderValue::from_static("nosniff"));
        headers
            .entry(axum::http::header::X_FRAME_OPTIONS)
            .or_insert(axum::http::HeaderValue::from_static("DENY"));
        headers
            .entry(axum::http::header::REFERRER_POLICY)
            .or_insert(axum::http::HeaderValue::from_static("same-origin"));
        headers
            .entry(axum::http::header::HeaderName::from_static(
                "permissions-policy",
            ))
            .or_insert(axum::http::HeaderValue::from_static(
                "accelerometer=(), camera=(), geolocation=(), gyroscope=(), magnetometer=(), microphone=(), payment=(), usb=()",
            ));
    }
    response
}

async fn get_metrics_handler() -> Response {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        crate::metrics::render(),
    )
        .into_response()
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
            state.auth.clone(),
            require_auth_middleware,
        ))
        .route("/api/accounts/ws", get(accounts_ws_handler))
        .layer(Extension(accounts))
        .with_state(state.clone());
    create_base_router(state, true)
        .merge(pairs)
        .layer(DefaultBodyLimit::max(64 * 1024))
        .layer(middleware::map_response(apply_security_headers))
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
    let service = ExchangePreviewService::new(limiter);
    Router::new()
        .route(
            "/api/exchange/symbols",
            get(exchange_symbols_handler).layer(Extension(service.clone())),
        )
        .route(
            "/api/exchange/symbol-preview",
            get(exchange_symbol_preview_handler).layer(Extension(service)),
        )
}

fn create_base_router(state: Arc<AppState>, history_clear: bool) -> Router {
    let mut api_protected = exchange_routes(Arc::new(RateLimiter::default()))
        .route("/api/status", get(get_status_handler))
        .route("/api/orders", get(get_orders_handler))
        .route("/api/trades", get(get_trades_handler))
        .route("/api/logs", get(get_logs_handler))
        .route(
            "/api/config",
            get(get_config_handler).post(post_config_handler),
        )
        .route("/api/control", post(post_control_handler));
    if history_clear {
        api_protected = api_protected.route("/api/trades/clear", post(clear_trade_history_handler));
    }
    let api_protected = api_protected
        .route_layer(middleware::from_fn_with_state(
            state.auth.clone(),
            require_auth_middleware,
        ))
        .with_state(state.clone());
    let account = database_export_routes(state.auth.clone());
    let auth_routes = public_routes().with_state(state.auth.clone());

    Router::new()
        .route("/ws", get(ws_handler))
        .with_state(state)
        .merge(auth_routes)
        .merge(account)
        .merge(api_protected)
}

fn database_export_routes(auth: Arc<AuthContext>) -> Router {
    Router::new()
        .route(
            "/api/database/export",
            get(export_database_handler).post(export_selected_database_handler),
        )
        .route_layer(middleware::from_fn_with_state(
            auth.clone(),
            require_auth_middleware,
        ))
        .with_state(auth)
}

fn public_routes() -> Router<Arc<AuthContext>> {
    Router::new()
        .route("/api/auth/status", get(get_auth_status_handler))
        .route("/api/auth/login", post(post_auth_login_handler))
        .route("/api/auth/logout", post(post_auth_logout_handler))
        .layer(DefaultBodyLimit::max(4 * 1024))
}

/// Existing unscoped APIs address the first pair; scoped APIs never change another pair.
pub fn create_multi_router(states: Vec<Arc<AppState>>) -> Router {
    let Some(primary) = states.first().cloned() else {
        return Router::new()
            .layer(DefaultBodyLimit::max(64 * 1024))
            .layer(middleware::map_response(apply_security_headers));
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
                state.auth.clone(),
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
            primary.auth.clone(),
            require_auth_middleware,
        ))
        .route("/api/accounts/ws", get(accounts_ws_handler))
        .layer(Extension(accounts))
        .with_state(states);
    router
        .merge(global)
        .layer(DefaultBodyLimit::max(64 * 1024))
        .layer(middleware::map_response(apply_security_headers))
}

/// Dispatch through the current registry so newly added pairs are immediately addressable.
pub fn create_managed_router(handle: RuntimeHandle) -> Router {
    let auth = handle.auth.clone();
    let protected = exchange_routes(handle.rate_limiter.clone())
        .route(
            "/api/pairs",
            get(get_managed_pairs_handler).post(add_pair_handler),
        )
        .route("/api/overview", get(get_managed_overview_handler))
        .route("/api/accounts", get(get_accounts_handler))
        .route("/api/overview/history", get(get_overview_history_handler))
        .route("/api/metrics", get(get_metrics_handler))
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
            auth.clone(),
            require_auth_middleware,
        ))
        .route("/ws", get(dispatch_pair))
        .route("/api/pairs/:symbol/ws", get(dispatch_pair))
        .route("/api/accounts/ws", get(accounts_ws_handler))
        .layer(Extension(AccountHub::new(AccountSource::Managed(
            handle.clone(),
        ))))
        .with_state(handle);
    let account = database_export_routes(auth.clone());
    public_routes()
        .with_state(auth)
        .merge(account)
        .merge(protected)
        .layer(DefaultBodyLimit::max(64 * 1024))
        .layer(middleware::map_response(apply_security_headers))
}

async fn dispatch_pair(
    State(handle): State<RuntimeHandle>,
    mut req: axum::extract::Request,
) -> Response {
    let (symbol, uri) = {
        let path = req.uri().path();
        let (symbol, resource) = if let Some(scoped) = path.strip_prefix("/api/pairs/") {
            let Some((symbol, resource)) = scoped.split_once('/') else {
                return StatusCode::NOT_FOUND.into_response();
            };
            (Some(symbol), resource)
        } else {
            (None, path.strip_prefix("/api/").unwrap_or("ws"))
        };
        let target = format!("/{resource}");
        let uri_str = if let Some(q) = req.uri().query() {
            format!("{target}?{q}")
        } else {
            target
        };
        let Ok(uri) = uri_str.parse() else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        (symbol, uri)
    };
    let state = {
        let states = handle.states.read().await;
        match symbol {
            Some(symbol) => states
                .iter()
                .find(|state| state.pair_scope() == Some(symbol)),
            None => states
                .iter()
                .find(|state| !state.is_retired())
                .or_else(|| states.first()),
        }
        .cloned()
    };
    let Some(state) = state else {
        return StatusCode::NOT_FOUND.into_response();
    };
    // The selected router enforces each resource's HTTP methods and WebSocket authentication.
    let router = state
        .managed_router
        .get_or_init(|| {
            pair_resource_routes()
                .route(
                    "/config",
                    get(get_config_handler).post(post_managed_config_handler),
                )
                .route("/ws", get(ws_handler))
        })
        .clone()
        .with_state(state.clone());
    *req.uri_mut() = uri;
    req.extensions_mut().insert(handle);
    match router.oneshot(req).await {
        Ok(response) => response,
        Err(never) => match never {},
    }
}
