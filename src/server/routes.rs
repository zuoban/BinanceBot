use crate::server::state::AppState;
use crate::server::ui::INDEX_HTML;
use crate::types::{
    AuthLoginPayload, AuthSetupPayload, AuthStatusResponse, AuthTokenResponse,
    BotControlAction, ChangePasswordPayload, GridOrder, LogEntry, TradeRecord,
    UpdateConfigPayload, WebConfigView,
};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tracing::{error, info, warn};

#[derive(Debug, Deserialize)]
pub struct ControlRequest {
    pub action: String,
}

#[derive(Debug, serde::Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub message: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct WsQuery {
    pub token: Option<String>,
}

fn extract_token(headers: &HeaderMap, query_token: Option<&str>) -> Option<String> {
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

    if let Some(token) = query_token {
        let t = token.trim();
        if !t.is_empty() {
            return Some(t.to_string());
        }
    }

    None
}

async fn require_auth_middleware(
    State(state): State<Arc<AppState>>,
    req: axum::extract::Request,
    next: Next,
) -> Result<Response, (StatusCode, Json<ApiResponse<()>>)> {
    let query_token = req.uri().query().and_then(|q| {
        q.split('&').find_map(|pair| {
            let mut parts = pair.split('=');
            if parts.next()? == "token" {
                parts.next().map(|v| v.to_string())
            } else {
                None
            }
        })
    });

    let token_opt = extract_token(req.headers(), query_token.as_deref());
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
    let api_protected = Router::new()
        .route("/api/status", get(get_status_handler))
        .route("/api/orders", get(get_orders_handler))
        .route("/api/trades", get(get_trades_handler))
        .route("/api/logs", get(get_logs_handler))
        .route("/api/config", get(get_config_handler).post(post_config_handler))
        .route("/api/control", post(post_control_handler))
        .route("/api/auth/change_password", post(post_change_password_handler))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_auth_middleware,
        ));

    Router::new()
        .route("/", get(dashboard_handler))
        .route("/api/auth/status", get(get_auth_status_handler))
        .route("/api/auth/setup", post(post_auth_setup_handler))
        .route("/api/auth/login", post(post_auth_login_handler))
        .route("/api/auth/logout", post(post_auth_logout_handler))
        .route("/ws", get(ws_handler))
        .merge(api_protected)
        .layer(CorsLayer::permissive())
        .with_state(state)
}

async fn dashboard_handler() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn get_auth_status_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Json<ApiResponse<AuthStatusResponse>> {
    let initialized = state.db.is_admin_password_set().unwrap_or(false);
    let token = extract_token(&headers, None);
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
    let is_set = state.db.is_admin_password_set().unwrap_or(false);
    if is_set {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse {
                success: false,
                data: None,
                message: Some("管理员密码已初始化，不能重复设置。如需修改请在登录后操作。".to_string()),
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

    if let Err(e) = state.db.set_admin_password(password) {
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

    let token = crate::auth::generate_session_token();
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
    let is_set = state.db.is_admin_password_set().unwrap_or(false);
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

    match state.db.verify_admin_password(&payload.password) {
        Ok(true) => {
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
    if let Some(token) = extract_token(&headers, None) {
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

    match state.db.verify_admin_password(&payload.old_password) {
        Ok(true) => {
            if let Err(e) = state.db.set_admin_password(new_password) {
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
    let trades = match state.db.get_recent_trades(100) {
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

async fn get_config_handler(State(state): State<Arc<AppState>>) -> Json<ApiResponse<WebConfigView>> {
    let config = state.config.read().await;
    let has_key = !config.exchange.api_key.trim().is_empty();
    let has_secret = !config.exchange.api_secret.trim().is_empty();

    let key_preview = if has_key {
        let k = &config.exchange.api_key;
        if k.len() > 8 {
            format!("{}...{}", &k[..4], &k[k.len() - 4..])
        } else {
            "****".to_string()
        }
    } else {
        "未配置".to_string()
    };

    let view = WebConfigView {
        symbol: config.exchange.symbol.clone(),
        grid_interval: config.grid.grid_interval,
        order_amount_usdc: config.grid.order_amount_usdc,
        buy_window: config.grid.buy_window,
        sell_window: config.grid.sell_window,
        post_only: config.grid.post_only,
        dry_run: config.exchange.dry_run,
        is_testnet: config.exchange.is_testnet,
        has_api_key: has_key,
        has_api_secret: has_secret,
        api_key_preview: key_preview,
        min_price: config.grid.min_price,
        max_price: config.grid.max_price,
        max_position_usdc: config.grid.max_position_usdc,
    };

    Json(ApiResponse {
        success: true,
        data: Some(view),
        message: None,
    })
}

async fn post_config_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<UpdateConfigPayload>,
) -> Json<ApiResponse<WebConfigView>> {
    // Validation
    let symbol = payload.symbol.trim().to_uppercase();
    if symbol.is_empty() {
        return Json(ApiResponse {
            success: false,
            data: None,
            message: Some("交易对 (Symbol) 不能为空".to_string()),
        });
    }

    if payload.grid_interval <= rust_decimal::Decimal::ZERO {
        return Json(ApiResponse {
            success: false,
            data: None,
            message: Some("网格间距必须大于 0".to_string()),
        });
    }

    if payload.order_amount_usdc <= rust_decimal::Decimal::ZERO {
        return Json(ApiResponse {
            success: false,
            data: None,
            message: Some("每个网格下单金额必须大于 0".to_string()),
        });
    }

    if payload.buy_window == 0 || payload.sell_window == 0 {
        return Json(ApiResponse {
            success: false,
            data: None,
            message: Some("买入/卖出窗口挂单数必须大于 0".to_string()),
        });
    }

    let mut current_config = state.config.read().await.clone();
    current_config.exchange.symbol = symbol;
    current_config.grid.grid_interval = payload.grid_interval;
    current_config.grid.order_amount_usdc = payload.order_amount_usdc;
    current_config.grid.buy_window = payload.buy_window;
    current_config.grid.sell_window = payload.sell_window;
    current_config.grid.post_only = payload.post_only;
    current_config.exchange.dry_run = payload.dry_run;
    current_config.exchange.is_testnet = payload.is_testnet;
    current_config.grid.min_price = payload.min_price;
    current_config.grid.max_price = payload.max_price;
    current_config.grid.max_position_usdc = payload.max_position_usdc;

    if let Some(key) = payload.api_key {
        let key_trimmed = key.trim().to_string();
        if !key_trimmed.is_empty() {
            current_config.exchange.api_key = key_trimmed;
        }
    }

    if let Some(secret) = payload.api_secret {
        let secret_trimmed = secret.trim().to_string();
        if !secret_trimmed.is_empty() {
            current_config.exchange.api_secret = secret_trimmed;
        }
    }

    // Persist to SQLite database
    if let Err(e) = state.db.save_config(&current_config) {
        error!("Failed to save configuration to SQLite database: {}", e);
        return Json(ApiResponse {
            success: false,
            data: None,
            message: Some(format!("保存到 SQLite 数据库失败: {}", e)),
        });
    } else {
        info!(
            "Configuration successfully saved to SQLite database ({})",
            state.db.path()
        );
    }

    // Notify strategy engine
    if let Err(e) = state
        .action_tx
        .send(BotControlAction::UpdateConfig(Box::new(
            current_config.clone(),
        )))
        .await
    {
        error!("Failed to notify strategy engine of config update: {}", e);
        return Json(ApiResponse {
            success: false,
            data: None,
            message: Some(format!("策略更新通知失败: {}", e)),
        });
    }

    let has_key = !current_config.exchange.api_key.trim().is_empty();
    let has_secret = !current_config.exchange.api_secret.trim().is_empty();
    let key_preview = if has_key {
        let k = &current_config.exchange.api_key;
        if k.len() > 8 {
            format!("{}...{}", &k[..4], &k[k.len() - 4..])
        } else {
            "****".to_string()
        }
    } else {
        "未配置".to_string()
    };

    let view = WebConfigView {
        symbol: current_config.exchange.symbol.clone(),
        grid_interval: current_config.grid.grid_interval,
        order_amount_usdc: current_config.grid.order_amount_usdc,
        buy_window: current_config.grid.buy_window,
        sell_window: current_config.grid.sell_window,
        post_only: current_config.grid.post_only,
        dry_run: current_config.exchange.dry_run,
        is_testnet: current_config.exchange.is_testnet,
        has_api_key: has_key,
        has_api_secret: has_secret,
        api_key_preview: key_preview,
        min_price: current_config.grid.min_price,
        max_price: current_config.grid.max_price,
        max_position_usdc: current_config.grid.max_position_usdc,
    };

    Json(ApiResponse {
        success: true,
        data: Some(view),
        message: Some("配置已成功更新并保存至 SQLite 数据库，策略引擎已实时生效".to_string()),
    })
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

async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(query): Query<WsQuery>,
    State(state): State<Arc<AppState>>,
) -> Response {
    let token = query.token.as_deref().unwrap_or_default();
    if !state.is_token_valid(token).await {
        return (
            StatusCode::UNAUTHORIZED,
            "Unauthorized: Missing or invalid authentication token",
        )
            .into_response();
    }

    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
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

    let mut send_task = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            if sender.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
    });

    let state_clone = state.clone();
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            if let Message::Text(text) = msg {
                if let Ok(req) = serde_json::from_str::<ControlRequest>(&text) {
                    let action = match req.action.to_lowercase().as_str() {
                        "pause" => Some(BotControlAction::Pause),
                        "resume" => Some(BotControlAction::Resume),
                        "cancel_all" => Some(BotControlAction::CancelAll),
                        "rebalance" => Some(BotControlAction::Rebalance),
                        _ => None,
                    };
                    if let Some(act) = action {
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
