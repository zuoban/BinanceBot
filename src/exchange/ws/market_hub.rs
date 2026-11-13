use super::{
    apply_local_observed_times, current_local_and_exchange_now, parse_multiplexed_market_update,
    send_ws_pong, upstream_ws_config, EventTimes, WS_CONNECT_TIMEOUT, WS_PONG_WRITE_TIMEOUT,
    WS_READ_IDLE_TIMEOUT,
};
use crate::sync_ext::MutexUnpoisonedExt;
use crate::types::TickerInfo;
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_tungstenite::connect_async_with_config;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

const DEFAULT_WS_BASE_URL: &str = "wss://fstream.binance.com";
const MAX_COMBINED_SYMBOLS: usize = 100;

struct SlotEntry {
    symbol: String,
    ticker_tx: broadcast::Sender<TickerInfo>,
}

#[derive(Default)]
struct HubState {
    slots: HashMap<u64, SlotEntry>,
    by_symbol: BTreeMap<String, Vec<(u64, broadcast::Sender<TickerInfo>)>>,
    next_slot_id: u64,
    worker_running: bool,
}

impl HubState {
    fn detach_symbol(&mut self, slot_id: u64, symbol: &str) {
        if symbol.is_empty() {
            return;
        }
        if let Some(entries) = self.by_symbol.get_mut(symbol) {
            entries.retain(|(id, _)| *id != slot_id);
            if entries.is_empty() {
                self.by_symbol.remove(symbol);
            }
        }
    }

    fn insert_slot(
        &mut self,
        slot_id: u64,
        symbol: String,
        ticker_tx: broadcast::Sender<TickerInfo>,
    ) {
        if !symbol.is_empty() {
            self.by_symbol
                .entry(symbol.clone())
                .or_default()
                .push((slot_id, ticker_tx.clone()));
        }
        self.slots.insert(slot_id, SlotEntry { symbol, ticker_tx });
    }

    fn update_slot_symbol(&mut self, slot_id: u64, updated: String) -> bool {
        let Some(old_symbol) = self.slots.get(&slot_id).map(|slot| slot.symbol.clone()) else {
            return false;
        };
        if old_symbol == updated {
            return false;
        }
        self.detach_symbol(slot_id, &old_symbol);
        let slot = self.slots.get_mut(&slot_id).expect("checked slot");
        if !updated.is_empty() {
            self.by_symbol
                .entry(updated.clone())
                .or_default()
                .push((slot_id, slot.ticker_tx.clone()));
        }
        slot.symbol = updated;
        true
    }

    fn remove_slot(&mut self, slot_id: u64) -> bool {
        let Some(removed) = self.slots.remove(&slot_id) else {
            return false;
        };
        self.detach_symbol(slot_id, &removed.symbol);
        true
    }
}

struct HubInner {
    state: Mutex<HubState>,
    notify_tx: watch::Sender<u64>,
    ws_base_url: String,
    read_timeout: Duration,
}

impl HubInner {
    fn bump_notify(&self) {
        self.notify_tx.send_modify(|seq| *seq = seq.wrapping_add(1));
    }

    fn desired_symbols(&self) -> BTreeSet<String> {
        let state = self.state.lock_unpoisoned();
        state
            .by_symbol
            .keys()
            .take(MAX_COMBINED_SYMBOLS)
            .cloned()
            .collect()
    }

    fn dispatch_ticker(&self, symbol: &str, update: TickerInfo) {
        let senders: Vec<broadcast::Sender<TickerInfo>> = {
            let state = self.state.lock_unpoisoned();
            let Some(subscribers) = state.by_symbol.get(symbol) else {
                return;
            };
            subscribers.iter().map(|(_, tx)| tx.clone()).collect()
        };
        if let Some((last_tx, rest)) = senders.split_last() {
            for tx in rest {
                let _ = tx.send(update.clone());
            }
            let _ = last_tx.send(update);
        }
    }
}

struct SlotGuard {
    inner: Arc<HubInner>,
    slot_id: u64,
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock_unpoisoned();
        if state.remove_slot(self.slot_id) {
            drop(state);
            self.inner.bump_notify();
        }
    }
}

#[derive(Serialize)]
struct WsStreamControlCommand<'a> {
    method: &'static str,
    params: Vec<String>,
    id: u64,
    #[serde(skip)]
    _marker: std::marker::PhantomData<&'a ()>,
}

pub(crate) fn combined_stream_url(ws_base_url: &str, symbols: &BTreeSet<String>) -> String {
    let base = ws_base_url.trim_end_matches('/');
    let streams = symbols
        .iter()
        .flat_map(|sym| {
            let lower = sym.to_ascii_lowercase();
            [format!("{lower}@ticker"), format!("{lower}@markPrice@1s")]
        })
        .collect::<Vec<_>>()
        .join("/");
    format!("{base}/market/stream?streams={streams}")
}

fn symbol_stream_params(symbols: &[String]) -> Vec<String> {
    symbols
        .iter()
        .flat_map(|sym| {
            let lower = sym.to_ascii_lowercase();
            [format!("{lower}@ticker"), format!("{lower}@markPrice@1s")]
        })
        .collect()
}

/// Multiplexes public market streams (`@ticker` and `@markPrice@1s`) across all active pairs.
pub(crate) struct MarketStreamHub {
    inner: Arc<HubInner>,
}

impl Default for MarketStreamHub {
    fn default() -> Self {
        Self::new()
    }
}

impl MarketStreamHub {
    pub(crate) fn new() -> Self {
        Self::with_base_url_and_timeout(DEFAULT_WS_BASE_URL.to_string(), WS_READ_IDLE_TIMEOUT)
    }

    pub(crate) fn with_base_url_and_timeout(ws_base_url: String, read_timeout: Duration) -> Self {
        let (notify_tx, _) = watch::channel(0u64);
        Self {
            inner: Arc::new(HubInner {
                state: Mutex::new(HubState::default()),
                notify_tx,
                ws_base_url,
                read_timeout,
            }),
        }
    }

    /// Register a pair's symbol receiver and ticker broadcast channel with the multiplexed hub.
    /// Aborting or dropping the returned `JoinHandle` automatically unregisters the pair.
    pub(crate) fn register(
        &self,
        mut config_rx: watch::Receiver<String>,
        ticker_tx: broadcast::Sender<TickerInfo>,
    ) -> JoinHandle<()> {
        let initial_symbol = config_rx.borrow_and_update().trim().to_ascii_uppercase();
        let (slot_id, start_worker) = {
            let mut state = self.inner.state.lock_unpoisoned();
            state.next_slot_id = state.next_slot_id.wrapping_add(1);
            let slot_id = state.next_slot_id;
            state.insert_slot(slot_id, initial_symbol, ticker_tx);
            let start_worker = !state.worker_running;
            if start_worker {
                state.worker_running = true;
            }
            (slot_id, start_worker)
        };
        if start_worker {
            let worker_inner = Arc::clone(&self.inner);
            let notify_rx = self.inner.notify_tx.subscribe();
            tokio::spawn(run_hub_worker(worker_inner, notify_rx));
        } else {
            self.inner.bump_notify();
        }

        let watcher_inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            let _guard = SlotGuard {
                inner: Arc::clone(&watcher_inner),
                slot_id,
            };
            while config_rx.changed().await.is_ok() {
                let updated = config_rx.borrow_and_update().trim().to_ascii_uppercase();
                let changed = {
                    let mut state = watcher_inner.state.lock_unpoisoned();
                    state.update_slot_symbol(slot_id, updated)
                };
                if changed {
                    watcher_inner.bump_notify();
                }
            }
        })
    }

    #[cfg(test)]
    pub(super) fn test_dispatch(&self, symbol: &str, update: TickerInfo) {
        self.inner.dispatch_ticker(symbol, update);
    }

    #[cfg(test)]
    pub(super) fn test_desired_symbols(&self) -> BTreeSet<String> {
        self.inner.desired_symbols()
    }
}

enum HubSessionExit {
    Reconnect,
    Idle,
}

async fn send_subscription_diff<W>(
    write: &mut W,
    method: &'static str,
    symbols: &[String],
    cmd_id: &mut u64,
    ws_url: &str,
) -> bool
where
    W: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    if symbols.is_empty() {
        return true;
    }
    *cmd_id = cmd_id.wrapping_add(1);
    let payload = WsStreamControlCommand {
        method,
        params: symbol_stream_params(symbols),
        id: *cmd_id,
        _marker: std::marker::PhantomData,
    };
    let Ok(json) = serde_json::to_string(&payload) else {
        return false;
    };
    match tokio::time::timeout(WS_PONG_WRITE_TIMEOUT, write.send(Message::Text(json))).await {
        Ok(Ok(())) => true,
        Ok(Err(error)) => {
            warn!("WebSocket {method} send error on {ws_url}: {error}");
            false
        }
        Err(_) => {
            warn!("WebSocket {method} send timed out on {ws_url}");
            false
        }
    }
}

fn sync_times_map(
    times_by_symbol: &mut HashMap<String, EventTimes>,
    active_symbols: &BTreeSet<String>,
) {
    times_by_symbol.retain(|sym, _| active_symbols.contains(sym));
    for sym in active_symbols {
        times_by_symbol.entry(sym.clone()).or_default();
    }
}

async fn read_hub_session<S>(
    inner: &HubInner,
    ws_stream: S,
    ws_url: &str,
    active_symbols: &mut BTreeSet<String>,
    times_by_symbol: &mut HashMap<String, EventTimes>,
    notify_rx: &mut watch::Receiver<u64>,
) -> HubSessionExit
where
    S: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
        + futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error>
        + Unpin,
{
    let (mut write, mut read) = ws_stream.split();
    let mut cmd_id = 0u64;
    loop {
        let next_msg = tokio::select! {
            msg = tokio::time::timeout(inner.read_timeout, read.next()) => msg,
            changed = notify_rx.changed() => {
                if changed.is_err() {
                    return HubSessionExit::Idle;
                }
                let desired = inner.desired_symbols();
                if desired.is_empty() {
                    return HubSessionExit::Idle;
                }
                let added: Vec<String> = desired.difference(active_symbols).cloned().collect();
                let removed: Vec<String> = active_symbols.difference(&desired).cloned().collect();
                if !added.is_empty() || !removed.is_empty() {
                    *active_symbols = desired;
                    sync_times_map(times_by_symbol, active_symbols);
                    if !send_subscription_diff(&mut write, "SUBSCRIBE", &added, &mut cmd_id, ws_url).await
                        || !send_subscription_diff(&mut write, "UNSUBSCRIBE", &removed, &mut cmd_id, ws_url).await
                    {
                        return HubSessionExit::Reconnect;
                    }
                }
                continue;
            }
        };
        let msg_res = match next_msg {
            Ok(Some(res)) => res,
            Ok(None) => return HubSessionExit::Reconnect,
            Err(_) => {
                warn!(
                    "Multiplexed WebSocket read timed out after {:?} for {}; reconnecting",
                    inner.read_timeout, ws_url
                );
                return HubSessionExit::Reconnect;
            }
        };
        match msg_res {
            Ok(Message::Ping(payload)) => {
                if !send_ws_pong(&mut write, payload, ws_url).await {
                    return HubSessionExit::Reconnect;
                }
            }
            Ok(Message::Close(_)) => return HubSessionExit::Reconnect,
            Ok(msg) if msg.is_text() => {
                if let Ok(text) = msg.to_text() {
                    let (local_now, exchange_now) = current_local_and_exchange_now();
                    if let Some((symbol, mut update)) =
                        parse_multiplexed_market_update(text, exchange_now, times_by_symbol)
                    {
                        apply_local_observed_times(&mut update, local_now, exchange_now);
                        inner.dispatch_ticker(&symbol, update);
                    }
                }
            }
            Ok(_) => {}
            Err(error) => {
                warn!("Multiplexed WebSocket read error: {error}");
                return HubSessionExit::Reconnect;
            }
        }
    }
}

async fn run_hub_worker(inner: Arc<HubInner>, mut notify_rx: watch::Receiver<u64>) {
    let mut backoff_secs = 1u64;
    let mut times_by_symbol: HashMap<String, EventTimes> = HashMap::new();
    'connect: loop {
        let _ = *notify_rx.borrow_and_update();
        let mut active_symbols = inner.desired_symbols();
        if active_symbols.is_empty() {
            let mut state = inner.state.lock_unpoisoned();
            let still_empty = state.by_symbol.is_empty();
            if still_empty {
                state.worker_running = false;
                return;
            }
            drop(state);
            continue 'connect;
        }
        sync_times_map(&mut times_by_symbol, &active_symbols);
        let ws_url = combined_stream_url(&inner.ws_base_url, &active_symbols);
        info!(
            symbols = active_symbols.len(),
            "Starting multiplexed Binance market WebSocket stream: {}", ws_url
        );

        let connection = tokio::select! {
            result = tokio::time::timeout(
                WS_CONNECT_TIMEOUT,
                connect_async_with_config(&ws_url, Some(upstream_ws_config()), false),
            ) => result,
            changed = notify_rx.changed() => {
                if changed.is_err() {
                    inner.state.lock_unpoisoned().worker_running = false;
                    return;
                }
                continue 'connect;
            }
        };

        match connection {
            Ok(Ok((ws_stream, _))) => {
                info!("Connected to multiplexed Binance WebSocket: {}", ws_url);
                backoff_secs = 1;
                // Re-check if desired symbols changed while connecting.
                let latest = inner.desired_symbols();
                if latest != active_symbols {
                    continue 'connect;
                }
                match read_hub_session(
                    &inner,
                    ws_stream,
                    &ws_url,
                    &mut active_symbols,
                    &mut times_by_symbol,
                    &mut notify_rx,
                )
                .await
                {
                    HubSessionExit::Idle => continue 'connect,
                    HubSessionExit::Reconnect => {}
                }
            }
            Ok(Err(error)) => {
                error!("Failed to connect multiplexed Binance WebSocket: {error}");
            }
            Err(_) => {
                error!("Timed out connecting multiplexed Binance WebSocket: {ws_url}");
            }
        }

        if inner.desired_symbols().is_empty() {
            continue 'connect;
        }
        warn!(
            "Multiplexed WebSocket disconnected. Reconnecting in {} seconds...",
            backoff_secs
        );
        crate::metrics::MARKET_WS_RECONNECTS.inc();
        tokio::select! {
            _ = sleep(Duration::from_millis(
                rand::random::<u64>() % (backoff_secs * 500 + 1) + backoff_secs * 500,
            )) => {}
            changed = notify_rx.changed() => {
                if changed.is_err() {
                    inner.state.lock_unpoisoned().worker_running = false;
                    return;
                }
                continue 'connect;
            }
        }
        backoff_secs = (backoff_secs * 2).min(30);
    }
}
