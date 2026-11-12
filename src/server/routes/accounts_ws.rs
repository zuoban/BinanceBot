use super::ws::{authorize_and_configure_ws, send_with_timeout};
use super::{json_ok, ApiResponse};
use crate::runtime::RuntimeHandle;
use crate::server::state::snapshot::{AccountFrame, AccountMessage};
use crate::server::state::{AppState, AuthContext};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use futures_util::StreamExt;
use std::sync::Arc;

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

pub(super) type AccountFrameSender = tokio::sync::watch::Sender<Option<Arc<str>>>;

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

    pub(super) async fn subscribe(&self) -> tokio::sync::watch::Receiver<Option<Arc<str>>> {
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
                        let _ = sender.send(Some(Arc::<str>::from(message)));
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
    let (Some(auth), Some(retired)) = (hub.source.auth(), hub.source.subscribe_retirement()) else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let (ws, token) = match authorize_and_configure_ws(ws, &headers, &auth).await {
        Ok(ready) => ready,
        Err(rejection) => return rejection.into_response(),
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
            if !send_with_timeout(
                &mut sender,
                Message::Text(frame.as_ref().to_owned()),
                &mut retired,
            )
            .await
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
                        Some(Ok(Message::Close(_)) | Err(_)) | None => return,
                        _ => {}
                    }
                }
            }
        }
    }
}
