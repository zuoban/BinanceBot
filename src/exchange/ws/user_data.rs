use super::{send_ws_pong, upstream_ws_config, WS_CONNECT_TIMEOUT, WS_PONG_WRITE_TIMEOUT};
use crate::exchange::account_cache::AccountCache;
use crate::exchange::position_cache::PositionCache;
use crate::exchange::BinanceFuturesClient;
use crate::sync_ext::MutexUnpoisonedExt;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{interval, sleep, MissedTickBehavior};
use tokio_tungstenite::connect_async_with_config;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

const DEFAULT_WS_BASE_URL: &str = "wss://fstream.binance.com";
const DEFAULT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(25 * 60);
const DEFAULT_PING_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(90);

#[derive(Clone)]
pub(crate) struct UserDataScope {
    pub(crate) client: Arc<BinanceFuturesClient>,
    pub(crate) symbol: String,
    pub(crate) is_paper: bool,
}

impl UserDataScope {
    #[must_use]
    fn is_live_with_credentials(&self) -> bool {
        !self.is_paper && self.client.has_api_key() && !self.symbol.is_empty()
    }
}

struct UserSlotEntry {
    scope: UserDataScope,
    wakeup_tx: watch::Sender<u64>,
}

#[derive(Default)]
struct UserDataState {
    slots: HashMap<u64, UserSlotEntry>,
    next_slot_id: u64,
    active_workers: HashMap<[u8; 32], watch::Sender<u64>>,
}

struct UserDataInner {
    state: Mutex<UserDataState>,
    account_cache: Arc<AccountCache>,
    position_cache: Arc<PositionCache>,
    ws_base_url: String,
    keepalive_interval: Duration,
    ping_interval: Duration,
    read_timeout: Duration,
}

impl UserDataInner {
    fn sync_workers(self: &Arc<Self>) {
        let mut state = self.state.lock_unpoisoned();
        let mut active_accounts = std::collections::HashSet::new();
        for slot in state.slots.values() {
            if slot.scope.is_live_with_credentials() {
                active_accounts.insert(slot.scope.client.account_identity_key());
            }
        }
        for account_key in active_accounts {
            if let Some(tx) = state.active_workers.get(&account_key) {
                tx.send_modify(|seq| *seq = seq.wrapping_add(1));
            } else {
                let (tx, rx) = watch::channel(0u64);
                state.active_workers.insert(account_key, tx);
                tokio::spawn(run_account_user_stream(Arc::clone(self), account_key, rx));
            }
        }
        for (account_key, tx) in &state.active_workers {
            if !state.slots.values().any(|slot| {
                slot.scope.is_live_with_credentials()
                    && slot.scope.client.account_identity_key() == *account_key
            }) {
                tx.send_modify(|seq| *seq = seq.wrapping_add(1));
            }
        }
    }

    fn active_client_for_account(
        &self,
        account_key: [u8; 32],
    ) -> Option<Arc<BinanceFuturesClient>> {
        let state = self.state.lock_unpoisoned();
        state
            .slots
            .values()
            .find(|slot| {
                slot.scope.is_live_with_credentials()
                    && slot.scope.client.account_identity_key() == account_key
            })
            .map(|slot| Arc::clone(&slot.scope.client))
    }

    fn notify_account_symbols(
        &self,
        account_key: [u8; 32],
        target_symbol: Option<&str>,
        invalidate_caches: bool,
    ) {
        let mut invalidated_client: Option<Arc<BinanceFuturesClient>> = None;
        let senders: Vec<watch::Sender<u64>> = {
            let state = self.state.lock_unpoisoned();
            state
                .slots
                .values()
                .filter(|slot| {
                    slot.scope.is_live_with_credentials()
                        && slot.scope.client.account_identity_key() == account_key
                        && target_symbol
                            .is_none_or(|sym| slot.scope.symbol.eq_ignore_ascii_case(sym))
                })
                .map(|slot| {
                    if invalidate_caches && invalidated_client.is_none() {
                        invalidated_client = Some(Arc::clone(&slot.scope.client));
                    }
                    slot.wakeup_tx.clone()
                })
                .collect()
        };
        if let Some(client) = invalidated_client {
            self.account_cache.invalidate(&client);
            self.position_cache.invalidate(&client);
        }
        for tx in senders {
            tx.send_modify(|seq| *seq = seq.wrapping_add(1));
        }
    }
}

struct UserSlotGuard {
    inner: Arc<UserDataInner>,
    slot_id: u64,
}

impl Drop for UserSlotGuard {
    fn drop(&mut self) {
        let removed = {
            let mut state = self.inner.state.lock_unpoisoned();
            state.slots.remove(&self.slot_id).is_some()
        };
        if removed {
            self.inner.sync_workers();
        }
    }
}

#[derive(Deserialize)]
struct OrderTradeFields<'a> {
    #[serde(borrow, rename = "s")]
    symbol: Cow<'a, str>,
    #[serde(borrow, default, rename = "x")]
    execution_type: Cow<'a, str>,
    #[serde(borrow, default, rename = "X")]
    order_status: Cow<'a, str>,
}

#[derive(Deserialize)]
struct AccountPositionItem<'a> {
    #[serde(borrow, rename = "s")]
    symbol: Cow<'a, str>,
}

#[derive(Deserialize)]
struct AccountUpdatePayload<'a> {
    #[serde(borrow, default, rename = "P")]
    positions: Vec<AccountPositionItem<'a>>,
}

#[derive(Deserialize)]
struct UserDataFrame<'a> {
    #[serde(borrow, rename = "e")]
    event_type: Cow<'a, str>,
    #[serde(borrow, default, rename = "o")]
    order: Option<OrderTradeFields<'a>>,
    #[serde(borrow, default, rename = "a")]
    account: Option<AccountUpdatePayload<'a>>,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum ParsedUserDataAction {
    ListenKeyExpired,
    OrderUpdate {
        symbol: String,
        invalidate_caches: bool,
    },
    AccountUpdate {
        symbols: Vec<String>,
    },
    Ignore,
}

pub(super) fn parse_user_data_frame(text: &str) -> ParsedUserDataAction {
    let Ok(frame) = serde_json::from_str::<UserDataFrame<'_>>(text) else {
        return ParsedUserDataAction::Ignore;
    };
    match frame.event_type.as_ref() {
        "listenKeyExpired" => ParsedUserDataAction::ListenKeyExpired,
        "ORDER_TRADE_UPDATE" => {
            let Some(order) = frame.order else {
                return ParsedUserDataAction::Ignore;
            };
            let symbol = order.symbol.trim().to_ascii_uppercase();
            if symbol.is_empty() {
                return ParsedUserDataAction::Ignore;
            }
            let invalidate_caches = order.execution_type == "TRADE"
                || matches!(order.order_status.as_ref(), "PARTIALLY_FILLED" | "FILLED");
            ParsedUserDataAction::OrderUpdate {
                symbol,
                invalidate_caches,
            }
        }
        "ACCOUNT_UPDATE" => {
            let symbols = frame
                .account
                .map(|acc| {
                    acc.positions
                        .into_iter()
                        .map(|pos| pos.symbol.trim().to_ascii_uppercase())
                        .filter(|sym| !sym.is_empty())
                        .collect()
                })
                .unwrap_or_default();
            ParsedUserDataAction::AccountUpdate { symbols }
        }
        _ => ParsedUserDataAction::Ignore,
    }
}

/// Manages private Binance User Data Streams (`listenKey`) per live account.
pub(crate) struct UserDataHub {
    inner: Arc<UserDataInner>,
}

impl UserDataHub {
    pub(crate) fn new(
        account_cache: Arc<AccountCache>,
        position_cache: Arc<PositionCache>,
    ) -> Self {
        Self::with_options(
            account_cache,
            position_cache,
            DEFAULT_WS_BASE_URL.to_string(),
            DEFAULT_KEEPALIVE_INTERVAL,
            DEFAULT_PING_INTERVAL,
            DEFAULT_READ_TIMEOUT,
        )
    }

    pub(crate) fn with_options(
        account_cache: Arc<AccountCache>,
        position_cache: Arc<PositionCache>,
        ws_base_url: String,
        keepalive_interval: Duration,
        ping_interval: Duration,
        read_timeout: Duration,
    ) -> Self {
        Self {
            inner: Arc::new(UserDataInner {
                state: Mutex::new(UserDataState::default()),
                account_cache,
                position_cache,
                ws_base_url,
                keepalive_interval,
                ping_interval,
                read_timeout,
            }),
        }
    }

    /// Register a pair's scope receiver with `UserDataHub`.
    /// Returns a `(watch::Receiver<u64>, JoinHandle<()>)` where the receiver signals
    /// user-data wakeups for this pair and the handle unregisters the slot when aborted/dropped.
    pub(crate) fn register(
        &self,
        mut scope_rx: watch::Receiver<UserDataScope>,
    ) -> (watch::Receiver<u64>, JoinHandle<()>) {
        let initial_scope = scope_rx.borrow_and_update().clone();
        let (wakeup_tx, wakeup_rx) = watch::channel(0u64);
        let slot_id = {
            let mut state = self.inner.state.lock_unpoisoned();
            state.next_slot_id = state.next_slot_id.wrapping_add(1);
            let id = state.next_slot_id;
            state.slots.insert(
                id,
                UserSlotEntry {
                    scope: initial_scope,
                    wakeup_tx,
                },
            );
            id
        };
        self.inner.sync_workers();

        let watcher_inner = Arc::clone(&self.inner);
        let handle = tokio::spawn(async move {
            let _guard = UserSlotGuard {
                inner: Arc::clone(&watcher_inner),
                slot_id,
            };
            while scope_rx.changed().await.is_ok() {
                let next_scope = scope_rx.borrow_and_update().clone();
                {
                    let mut state = watcher_inner.state.lock_unpoisoned();
                    if let Some(slot) = state.slots.get_mut(&slot_id) {
                        slot.scope = next_scope;
                    }
                }
                watcher_inner.sync_workers();
            }
        });
        (wakeup_rx, handle)
    }
}

enum UserStreamExit {
    Reconnect,
    RotateListenKey,
    Shutdown,
}

async fn read_user_data_session<S>(
    inner: &UserDataInner,
    account_key: [u8; 32],
    client: &BinanceFuturesClient,
    listen_key: &str,
    ws_stream: S,
    ws_url: &str,
    notify_rx: &mut watch::Receiver<u64>,
) -> UserStreamExit
where
    S: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
        + futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error>
        + Unpin,
{
    let (mut write, mut read) = ws_stream.split();
    let mut keepalive_timer = interval(inner.keepalive_interval);
    keepalive_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    keepalive_timer.tick().await;
    let mut ping_timer = interval(inner.ping_interval);
    ping_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ping_timer.tick().await;

    loop {
        let next_msg = tokio::select! {
            msg = tokio::time::timeout(inner.read_timeout, read.next()) => msg,
            _ = keepalive_timer.tick() => {
                if let Err(error) = client.keepalive_listen_key(listen_key).await {
                    warn!("Failed to keepalive Binance listenKey; rotating: {error}");
                    return UserStreamExit::RotateListenKey;
                }
                debug!("Refreshed Binance listenKey keepalive");
                continue;
            }
            _ = ping_timer.tick() => {
                match tokio::time::timeout(WS_PONG_WRITE_TIMEOUT, write.send(Message::Ping(Vec::new()))).await {
                    Ok(Ok(())) => continue,
                    _ => return UserStreamExit::Reconnect,
                }
            }
            changed = notify_rx.changed() => {
                if changed.is_err() || inner.active_client_for_account(account_key).is_none() {
                    return UserStreamExit::Shutdown;
                }
                continue;
            }
        };
        let msg_res = match next_msg {
            Ok(Some(res)) => res,
            Ok(None) => return UserStreamExit::Reconnect,
            Err(_) => {
                warn!("User data WebSocket read timed out for {ws_url}; reconnecting");
                return UserStreamExit::Reconnect;
            }
        };
        match msg_res {
            Ok(Message::Ping(payload)) => {
                if !send_ws_pong(&mut write, payload, ws_url).await {
                    return UserStreamExit::Reconnect;
                }
            }
            Ok(Message::Close(_)) => return UserStreamExit::Reconnect,
            Ok(msg) if msg.is_text() => {
                if let Ok(text) = msg.to_text() {
                    match parse_user_data_frame(text) {
                        ParsedUserDataAction::ListenKeyExpired => {
                            warn!("Binance listenKey expired; rotating immediately");
                            return UserStreamExit::RotateListenKey;
                        }
                        ParsedUserDataAction::OrderUpdate {
                            symbol,
                            invalidate_caches,
                        } => {
                            inner.notify_account_symbols(
                                account_key,
                                Some(&symbol),
                                invalidate_caches,
                            );
                        }
                        ParsedUserDataAction::AccountUpdate { symbols } => {
                            if symbols.is_empty() {
                                inner.notify_account_symbols(account_key, None, true);
                            } else {
                                for symbol in &symbols {
                                    inner.notify_account_symbols(account_key, Some(symbol), true);
                                }
                            }
                        }
                        ParsedUserDataAction::Ignore => {}
                    }
                }
            }
            Ok(_) => {}
            Err(error) => {
                warn!("User data WebSocket read error: {error}");
                return UserStreamExit::Reconnect;
            }
        }
    }
}

async fn run_account_user_stream(
    inner: Arc<UserDataInner>,
    account_key: [u8; 32],
    mut notify_rx: watch::Receiver<u64>,
) {
    let mut backoff_secs = 1u64;
    'connect: loop {
        let _ = *notify_rx.borrow_and_update();
        let Some(client) = inner.active_client_for_account(account_key) else {
            let mut state = inner.state.lock_unpoisoned();
            let still_none = !state.slots.values().any(|slot| {
                slot.scope.is_live_with_credentials()
                    && slot.scope.client.account_identity_key() == account_key
            });
            if still_none {
                state.active_workers.remove(&account_key);
                return;
            }
            drop(state);
            continue 'connect;
        };

        let listen_key = tokio::select! {
            res = client.create_listen_key() => {
                match res {
                    Ok(key) => key,
                    Err(error) => {
                        warn!("Failed to create Binance listenKey: {error}");
                        String::new()
                    }
                }
            }
            changed = notify_rx.changed() => {
                if changed.is_err() {
                    inner.state.lock_unpoisoned().active_workers.remove(&account_key);
                    return;
                }
                continue 'connect;
            }
        };

        if !listen_key.is_empty() {
            let base = inner.ws_base_url.trim_end_matches('/');
            let ws_url = format!("{base}/ws/{listen_key}");
            let redacted_url = format!("{base}/ws/[listenKey]");
            info!("Connecting to Binance User Data Stream: {redacted_url}");

            let connection = tokio::select! {
                result = tokio::time::timeout(
                    WS_CONNECT_TIMEOUT,
                    connect_async_with_config(&ws_url, Some(upstream_ws_config()), false),
                ) => result,
                changed = notify_rx.changed() => {
                    if changed.is_err() {
                        inner.state.lock_unpoisoned().active_workers.remove(&account_key);
                        return;
                    }
                    continue 'connect;
                }
            };

            match connection {
                Ok(Ok((ws_stream, _))) => {
                    info!("Connected to Binance User Data Stream: {redacted_url}");
                    backoff_secs = 1;
                    // Trigger a reconciliation pass upon stream (re)connection so any
                    // fills that occurred while disconnected are caught immediately.
                    inner.notify_account_symbols(account_key, None, true);
                    match read_user_data_session(
                        &inner,
                        account_key,
                        &client,
                        &listen_key,
                        ws_stream,
                        &redacted_url,
                        &mut notify_rx,
                    )
                    .await
                    {
                        UserStreamExit::RotateListenKey => continue 'connect,
                        UserStreamExit::Shutdown => {
                            let _ = client.close_listen_key(&listen_key).await;
                            inner
                                .state
                                .lock_unpoisoned()
                                .active_workers
                                .remove(&account_key);
                            return;
                        }
                        UserStreamExit::Reconnect => {}
                    }
                }
                Ok(Err(error)) => {
                    warn!("Failed to connect to Binance User Data Stream: {error}");
                }
                Err(_) => {
                    warn!("Timed out connecting to Binance User Data Stream: {redacted_url}");
                }
            }
        }

        if inner.active_client_for_account(account_key).is_none() {
            continue 'connect;
        }
        crate::metrics::USER_DATA_WS_RECONNECTS.inc();
        tokio::select! {
            _ = sleep(Duration::from_millis(
                rand::random::<u64>() % (backoff_secs * 500 + 1) + backoff_secs * 500,
            )) => {}
            changed = notify_rx.changed() => {
                if changed.is_err() {
                    inner.state.lock_unpoisoned().active_workers.remove(&account_key);
                    return;
                }
                continue 'connect;
            }
        }
        backoff_secs = (backoff_secs * 2).min(30);
    }
}
