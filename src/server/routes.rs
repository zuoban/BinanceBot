use crate::server::state::AppState;
use crate::server::ui::INDEX_HTML;
use crate::types::{
    BotControlAction, GridOrder, LogEntry, TradeRecord, UpdateConfigPayload, WebConfigView,
};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::{Html, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tracing::{error, info};

#[derive(Debug, Deserialize)]
pub struct ControlRequest {
    pub action: String,
}

#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub message: Option<String>,
}

pub fn create_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(dashboard_handler))
        .route("/api/status", get(get_status_handler))
        .route("/api/orders", get(get_orders_handler))
        .route("/api/trades", get(get_trades_handler))
        .route("/api/logs", get(get_logs_handler))
        .route("/api/config", get(get_config_handler))
        .route("/api/config", post(post_config_handler))
        .route("/api/control", post(post_control_handler))
        .route("/ws", get(ws_handler))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

async fn dashboard_handler() -> Html<&'static str> {
    Html(INDEX_HTML)
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
    let trades: Vec<TradeRecord> = state.recent_trades.read().await.iter().cloned().collect();
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
            message: Some("买卖窗口订单数量必须至少为 1".to_string()),
        });
    }

    let mut current_config = state.config.read().await.clone();

    // Update fields
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

    // Persist to file if requested
    if payload.save_to_file {
        if let Err(e) = current_config.save_to_file("config.toml") {
            error!("Failed to save configuration to config.toml: {}", e);
        } else {
            info!("Configuration successfully saved to config.toml");
        }
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
        message: Some("配置已成功更新并实时应用至交易引擎".to_string()),
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

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> Response {
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
