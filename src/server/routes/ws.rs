use super::control::{
    control_action, dispatch_prepared_control, prepare_control, ControlReply, ControlRequest,
    PendingControlPause,
};
use super::{json_ok, ApiResponse};
use crate::runtime::RuntimeHandle;
use crate::server::state::snapshot::{AccountFrame, AccountMessage};
use crate::server::state::{sanitize_session_token, AppState, AuthContext};
use crate::types::BotControlAction;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;

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

fn ws_origin_allowed(headers: &HeaderMap) -> bool {
    let Some(origin_header) = headers.get(axum::http::header::ORIGIN) else {
        return true;
    };
    let Ok(origin_str) = origin_header.to_str() else {
        return false;
    };
    let Ok(origin) = url::Url::parse(origin_str) else {
        return false;
    };
    if !matches!(origin.scheme(), "http" | "https") {
        return false;
    }
    let Some(origin_host) = origin.host_str() else {
        return false;
    };
    let Some(host_str) = headers
        .get(axum::http::header::HOST)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    let Ok(req_url) = url::Url::parse(&format!("http://{host_str}")) else {
        return false;
    };
    if req_url.host_str() != Some(origin_host) {
        return false;
    }
    match req_url.port() {
        Some(req_port) => origin.port_or_known_default() == Some(req_port),
        None => origin.port().is_none(),
    }
}

#[derive(Clone)]
pub(super) enum AccountSource {
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

    fn auth(&self) -> Option<Arc<AuthContext>> {
        match self {
            Self::Fixed(states) => states.first().map(|state| state.auth.clone()),
            Self::Managed(handle) => Some(handle.auth.clone()),
        }
    }

    fn subscribe_retirement(&self) -> Option<tokio::sync::watch::Receiver<bool>> {
        match self {
            Self::Fixed(states) => states.first().map(|state| state.subscribe_retirement()),
            Self::Managed(handle) => Some(handle.stop.subscribe()),
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

pub(super) type AccountFrameSender = tokio::sync::watch::Sender<Option<Arc<String>>>;

// Router-owned: clients of this registry share collection and JSON encoding.
// The sender retains no receiver, so the sampler exits when the last client leaves.
pub(super) struct AccountHub {
    source: AccountSource,
    pub(super) sampler: Arc<tokio::sync::Mutex<Option<AccountFrameSender>>>,
}

impl AccountHub {
    pub(super) fn new(source: AccountSource) -> Arc<Self> {
        Arc::new(Self {
            source,
            sampler: Arc::new(tokio::sync::Mutex::new(None)),
        })
    }

    pub(super) async fn subscribe(&self) -> tokio::sync::watch::Receiver<Option<Arc<String>>> {
        let mut sampler = self.sampler.lock().await;
        if let Some(sender) = sampler.as_ref() {
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
                        let mut guard = lifecycle.lock().await;
                        if sender.receiver_count() > 0 {
                            if sender.borrow().is_none() {
                                ticks.reset_immediately();
                            }
                            continue;
                        }
                        if guard.as_ref().is_some_and(|active| active.same_channel(&sender)) {
                            *guard = None;
                        }
                        break;
                    }
                    message = async {
                        ticks.tick().await;
                        serde_json::to_string(&AccountMessage::Accounts(source.frame().await))
                    } => {
                        let Ok(message) = message else {
                            let mut guard = lifecycle.lock().await;
                            if guard.as_ref().is_some_and(|active| active.same_channel(&sender)) {
                                *guard = None;
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

pub(super) async fn get_accounts_handler(
    Extension(hub): Extension<Arc<AccountHub>>,
) -> Json<ApiResponse<AccountFrame>> {
    json_ok(hub.source.frame().await)
}

pub(super) async fn accounts_ws_handler(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    Extension(hub): Extension<Arc<AccountHub>>,
) -> Response {
    upgrade_accounts_socket(ws, headers, hub).await
}

async fn authorize_and_configure_ws(
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
    if !auth.is_token_valid(token).await {
        return Err((
            StatusCode::UNAUTHORIZED,
            "Unauthorized: Missing or invalid authentication token",
        )
            .into_response());
    }
    let configured = ws
        .protocols(["auth"])
        .max_message_size(WS_MAX_FRAME_BYTES)
        .max_frame_size(WS_MAX_FRAME_BYTES);
    Ok((configured, token.to_string()))
}

async fn upgrade_accounts_socket(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    hub: Arc<AccountHub>,
) -> Response {
    if !ws_origin_allowed(&headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let (Some(auth), Some(retired)) = (hub.source.auth(), hub.source.subscribe_retirement()) else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let (ws, token) = match authorize_and_configure_ws(ws, &headers, &auth).await {
        Ok(ready) => ready,
        Err(response) => return response,
    };
    ws.on_upgrade(move |socket| run_accounts_socket(socket, hub, auth, token, retired))
}

async fn run_accounts_socket(
    socket: WebSocket,
    hub: Arc<AccountHub>,
    auth: Arc<AuthContext>,
    token: String,
    mut retired: tokio::sync::watch::Receiver<bool>,
) {
    if *retired.borrow() {
        return;
    }
    let mut frames = hub.subscribe().await;
    let (mut sender, mut receiver) = socket.split();
    let mut check_auth = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        let frame = frames.borrow_and_update().clone();
        if let Some(frame) = frame {
            if !auth.is_token_valid(&token).await {
                break;
            }
            if !send_with_timeout(&mut sender, Message::Text((*frame).clone()), &mut retired).await
            {
                break;
            }
        }
        loop {
            tokio::select! {
                biased;
                _ = retired.changed() => return,
                _ = check_auth.tick() => {
                    if !auth.is_token_valid(&token).await { return; }
                }
                changed = frames.changed() => {
                    if changed.is_err() { return; }
                    break;
                }
                message = receiver.next() => {
                    match message {
                        Some(Ok(Message::Ping(payload))) => {
                            if !send_with_timeout(&mut sender, Message::Pong(payload), &mut retired).await { return; }
                        }
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return,
                        _ => {}
                    }
                }
            }
        }
    }
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

async fn send_with_timeout<S>(
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
    let (mut sender, receiver) = socket.split();
    let rx = state.ws_broadcast_tx.subscribe();

    // Send initial snapshot immediately upon connection
    let initial_snapshot = state.snapshot().await;
    if !state.is_token_valid(&token).await {
        return;
    }
    if let Some(json) = crate::types::ws_frame_json("snapshot", &initial_snapshot) {
        if !send_with_timeout(&mut sender, Message::Text(json), &mut retired).await {
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
                    .send(control_ws_message(Some(&name), response))
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
    mut rx: tokio::sync::broadcast::Receiver<String>,
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
                            if !send_with_timeout(&mut sender, Message::Text(msg), &mut retired).await {
                                break;
                            }
                        }
                        Err(_) => break,
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
                        if result_tx
                            .send(control_ws_message(
                                name.as_deref(),
                                ApiResponse::err(message),
                            ))
                            .await
                            .is_err()
                        {
                            break;
                        }
                        continue;
                    }
                };
                let pending_pause = PendingControlPause::new(&state_clone, &action.0);
                let prepare_state = state_clone.clone();
                let prepare_results = result_tx.clone();
                let prepare_queue = control_tx.clone();
                let preparation = tokio::spawn(async move {
                    if let Err(message) = prepare_control(&prepare_state, &action.0).await {
                        let _ = prepare_results
                            .send(control_ws_message(
                                name.as_deref(),
                                ApiResponse::err(message),
                            ))
                            .await;
                    } else if let Some(action_name) = name {
                        let _ = prepare_queue
                            .send((action_name, action, pending_pause))
                            .await;
                    }
                });
                if preparation.await.is_err() {
                    break;
                }
            }
        }
    })
}
