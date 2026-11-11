use super::control::{
    control_action, dispatch_prepared_control, prepare_control, ControlReply, ControlRequest,
    PendingControlPause,
};
use super::ApiResponse;
use crate::server::state::{sanitize_session_token, AppState, AuthContext, SessionCheckError};
use crate::types::BotControlAction;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tracing::error;

const WS_MAX_FRAME_BYTES: usize = 64 * 1024;
const WS_RESULT_QUEUE_CAPACITY: usize = 32;
const WS_CONTROL_QUEUE_CAPACITY: usize = 16;
const WS_SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

fn websocket_token(headers: &HeaderMap) -> &str {
    headers
        .get(axum::http::header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        .and_then(|protocols| {
            protocols
                .split(',')
                .find_map(|protocol| protocol.trim().strip_prefix("token."))
        })
        .filter(|raw| *raw == raw.trim())
        .and_then(sanitize_session_token)
        .unwrap_or_default()
}

pub(super) fn ws_origin_allowed(headers: &HeaderMap) -> bool {
    let Some(origin_header) = headers.get(axum::http::header::ORIGIN) else {
        return true;
    };
    let Ok(origin_str) = origin_header.to_str() else {
        return false;
    };
    let Ok(origin) = url::Url::parse(origin_str) else {
        return false;
    };
    if !matches!(origin.scheme(), "http" | "https") || origin.host().is_none() {
        return false;
    }
    let Some(host_str) = headers
        .get(axum::http::header::HOST)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    if host_str.is_empty()
        || host_str.chars().any(|ch| {
            matches!(ch, '@' | '/' | '?' | '#' | '\\')
                || ch.is_ascii_whitespace()
                || ch.is_ascii_control()
        })
    {
        return false;
    }
    let effective_scheme = if let Some(proto_header) = headers.get("x-forwarded-proto") {
        let Ok(raw_proto) = proto_header.to_str() else {
            return false;
        };
        let first = raw_proto.split(',').next().unwrap_or("").trim();
        if first.eq_ignore_ascii_case("https") {
            if origin.scheme() != "https" {
                return false;
            }
            "https"
        } else if first.eq_ignore_ascii_case("http") {
            if origin.scheme() != "http" {
                return false;
            }
            "http"
        } else {
            return false;
        }
    } else {
        origin.scheme()
    };
    let Ok(req_url) = url::Url::parse(&format!("{effective_scheme}://{host_str}")) else {
        return false;
    };
    origin.host() == req_url.host()
        && origin.port_or_known_default() == req_url.port_or_known_default()
}

pub(super) async fn authorize_and_configure_ws(
    ws: WebSocketUpgrade,
    headers: &HeaderMap,
    auth: &AuthContext,
) -> Result<(WebSocketUpgrade, String), Response> {
    if !ws_origin_allowed(headers) {
        return Err((
            StatusCode::FORBIDDEN,
            "Forbidden: Cross-origin WebSocket connection rejected",
        )
            .into_response());
    }
    let token = websocket_token(headers);
    match auth.check_token_valid(token).await {
        Ok(true) => {}
        Ok(false) => {
            return Err((
                StatusCode::UNAUTHORIZED,
                "Unauthorized: Missing or invalid authentication token",
            )
                .into_response());
        }
        Err(SessionCheckError::Busy) => {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "Service Unavailable: Session validation busy, retry shortly",
            )
                .into_response());
        }
        Err(SessionCheckError::Database(err)) => {
            error!("Failed to validate WebSocket session: {}", err);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal Server Error: Authentication service unavailable",
            )
                .into_response());
        }
    }
    let configured = ws
        .protocols(["auth"])
        .max_message_size(WS_MAX_FRAME_BYTES)
        .max_frame_size(WS_MAX_FRAME_BYTES);
    Ok((configured, token.to_string()))
}
pub(super) async fn ws_handler(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Response {
    let (ws, token) = match authorize_and_configure_ws(ws, &headers, &state.auth).await {
        Ok(ready) => ready,
        Err(response) => return response,
    };
    ws.on_upgrade(move |socket| handle_socket(socket, state, token))
}

pub(super) async fn send_until_retired<S>(
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

pub(super) async fn send_with_timeout<S>(
    sender: &mut S,
    message: Message,
    retired: &mut tokio::sync::watch::Receiver<bool>,
) -> bool
where
    S: futures_util::Sink<Message> + Unpin,
{
    matches!(
        tokio::time::timeout(
            WS_SEND_TIMEOUT,
            send_until_retired(sender, message, retired)
        )
        .await,
        Ok(Some(Ok(())))
    )
}

fn control_ws_message(action: Option<&str>, response: &ApiResponse<String>) -> String {
    serde_json::json!({
        "type": "control",
        "action": action,
        "success": response.success,
        "data": &response.data,
        "message": &response.message,
    })
    .to_string()
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>, token: String) {
    let mut retired = state.subscribe_retirement();
    if *retired.borrow() {
        return;
    }
    let (mut sender, receiver) = socket.split();
    let rx = state.ws_broadcast_tx.subscribe();

    // Send initial snapshot immediately upon connection
    let initial_snapshot = state.snapshot().await;
    if !state.is_token_valid(&token).await {
        return;
    }
    if let Some(json) = crate::types::ws_frame_json("snapshot", &initial_snapshot) {
        if !send_with_timeout(
            &mut sender,
            Message::Text(json.as_ref().to_owned()),
            &mut retired,
        )
        .await
        {
            return;
        }
    }

    let (result_tx, result_rx) = tokio::sync::mpsc::channel::<String>(WS_RESULT_QUEUE_CAPACITY);
    let (control_tx, control_rx) = tokio::sync::mpsc::channel(WS_CONTROL_QUEUE_CAPACITY);
    spawn_socket_control_worker(state.clone(), result_tx.clone(), token.clone(), control_rx);
    let mut send_task =
        spawn_socket_send_task(sender, state.clone(), token.clone(), retired, result_rx, rx);
    let mut recv_task = spawn_socket_recv_task(receiver, state, token, result_tx, control_tx);

    // If either task exits, abort the other
    tokio::select! {
        _ = (&mut send_task) => recv_task.abort(),
        _ = (&mut recv_task) => send_task.abort(),
    };
}

type SocketControlEntry = (
    String,
    (BotControlAction, Option<ControlReply>),
    Option<PendingControlPause>,
);

fn spawn_socket_control_worker(
    worker_state: Arc<AppState>,
    worker_results: tokio::sync::mpsc::Sender<String>,
    worker_token: String,
    mut control_rx: tokio::sync::mpsc::Receiver<SocketControlEntry>,
) {
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
                let _ = results
                    .send(control_ws_message(Some(&name), &response))
                    .await;
            });
            let _ = ready.await;
        }
    });
}

fn spawn_socket_send_task(
    mut sender: futures_util::stream::SplitSink<WebSocket, Message>,
    send_state: Arc<AppState>,
    send_token: String,
    mut retired: tokio::sync::watch::Receiver<bool>,
    mut result_rx: tokio::sync::mpsc::Receiver<String>,
    mut rx: tokio::sync::broadcast::Receiver<Arc<str>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut check_auth = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = retired.changed() => break,
                _ = check_auth.tick() => {
                    if !send_state.is_token_valid(&send_token).await { break; }
                }
                Some(msg) = result_rx.recv() => {
                    if !send_state.is_token_valid(&send_token).await { break; }
                    if !send_with_timeout(&mut sender, Message::Text(msg), &mut retired).await { break; }
                }
                message = rx.recv() => {
                    match message {
                        Ok(msg) => {
                            if !send_state.is_token_valid(&send_token).await { break; }
                            if !send_with_timeout(
                                &mut sender,
                                Message::Text(msg.as_ref().to_owned()),
                                &mut retired,
                            )
                            .await
                            {
                                break;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            if !send_state.is_token_valid(&send_token).await { break; }
                            if let Some(json) =
                                crate::types::ws_frame_json("snapshot", &send_state.snapshot().await)
                            {
                                if !send_with_timeout(
                                    &mut sender,
                                    Message::Text(json.as_ref().to_owned()),
                                    &mut retired,
                                )
                                .await
                                {
                                    break;
                                }
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }
    })
}

fn spawn_socket_recv_task(
    mut receiver: futures_util::stream::SplitStream<WebSocket>,
    state_clone: Arc<AppState>,
    token: String,
    result_tx: tokio::sync::mpsc::Sender<String>,
    control_tx: tokio::sync::mpsc::Sender<SocketControlEntry>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            if let Message::Text(text) = msg {
                let keep_going = handle_socket_control_text(
                    &text,
                    &state_clone,
                    &token,
                    &result_tx,
                    &control_tx,
                )
                .await;
                if !keep_going {
                    break;
                }
            }
        }
    })
}

async fn handle_socket_control_text(
    text: &str,
    state: &Arc<AppState>,
    token: &str,
    result_tx: &tokio::sync::mpsc::Sender<String>,
    control_tx: &tokio::sync::mpsc::Sender<SocketControlEntry>,
) -> bool {
    if !state.is_token_valid(token).await {
        return false;
    }
    let (name, action) = match serde_json::from_str::<ControlRequest>(text) {
        Ok(req) => {
            let action = control_action(&req, state.pause_epoch());
            (Some(req.action), action)
        }
        Err(error) => (None, Err(format!("无效的控制请求: {}", error))),
    };
    let action = match action {
        Ok(action) => action,
        Err(message) => {
            return result_tx
                .send(control_ws_message(
                    name.as_deref(),
                    &ApiResponse::err(message),
                ))
                .await
                .is_ok();
        }
    };
    let pending_pause = PendingControlPause::new(state, &action.0);
    let prepare_state = state.clone();
    let prepare_results = result_tx.clone();
    let prepare_queue = control_tx.clone();
    tokio::spawn(async move {
        if let Err(message) = prepare_control(&prepare_state, &action.0).await {
            let _ = prepare_results
                .send(control_ws_message(
                    name.as_deref(),
                    &ApiResponse::err(message),
                ))
                .await;
        } else if let Some(action_name) = name {
            let _ = prepare_queue
                .send((action_name, action, pending_pause))
                .await;
        }
    })
    .await
    .is_ok()
}
