use crate::exchange::account_cache::AccountCache;
use crate::exchange::client::{BinanceFuturesClient, ExchangeError, NewOrderRequest};
use crate::exchange::market_cache::MarketCache;
use crate::exchange::position_cache::PositionCache;
use crate::exchange::{BinanceOrderResponse, BinanceUserTrade};
use crate::server::state::AppState;
use crate::strategy::precision::SymbolRules;
use crate::types::*;
use anyhow::{anyhow, Result};
use chrono::Utc;
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{interval, interval_at};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

// An unknown submission can take time to become queryable. Never release its
// reservation on an initial negative lookup, or for an already accepted order.
const UNKNOWN_SUBMISSION_GRACE_SECS: i64 = 30;

#[derive(Clone, Copy)]
enum ReconciliationPriority {
    Periodic,
    Critical,
}

/// Binance Futures allows at most 36 characters for newClientOrderId.
fn new_grid_client_order_id(side: OrderSide) -> String {
    let mut uuid_buf = [0u8; 32];
    let uuid = Uuid::new_v4().simple().encode_lower(&mut uuid_buf);
    let side_code = match side {
        OrderSide::Buy => "b",
        OrderSide::Sell => "s",
    };
    let mut id = String::with_capacity(35);
    id.push_str("gb_");
    id.push_str(side_code);
    id.push('_');
    id.push_str(&uuid[..30]);
    id
}

#[path = "account_sync.rs"]
mod account_sync;
#[path = "config_transition.rs"]
mod config_transition;
#[path = "conflict_resolution.rs"]
mod conflict_resolution;
#[path = "control_actions.rs"]
mod control_actions;
#[path = "evidence.rs"]
mod evidence;
#[path = "evidence_book.rs"]
mod evidence_book;
#[path = "live_submission.rs"]
mod live_submission;
#[path = "market_feed.rs"]
mod market_feed;
#[path = "order_cancel.rs"]
mod order_cancel;
#[path = "order_ops.rs"]
mod order_ops;
#[path = "order_sizing.rs"]
mod order_sizing;
#[path = "pair_intents.rs"]
mod pair_intents;
#[path = "pnl_reconcile.rs"]
mod pnl_reconcile;
#[path = "reconciliation.rs"]
mod reconciliation;
#[path = "reconciliation_anomalies.rs"]
mod reconciliation_anomalies;
#[path = "reconciliation_settlement.rs"]
mod reconciliation_settlement;
#[path = "remainder.rs"]
mod remainder;
#[path = "remainder_settlement.rs"]
mod remainder_settlement;
#[path = "risk_guards.rs"]
mod risk_guards;
#[path = "snapshot_conflicts.rs"]
mod snapshot_conflicts;
#[path = "window.rs"]
mod window;

#[cfg(test)]
use account_sync::live_sync_freshness;
use evidence_book::OrderEvidenceBook;
use live_submission::LiveGridOrderSubmission;
use market_feed::{market_completion, MarkPriceCompletion, MarketRetry, TickerCompletion};
use order_sizing::{GridOrderSpec, WindowPlacementContext};
#[cfg(test)]
use pair_intents::{new_pair_intent, paired_grid_client_order_id, PairPlacementDecision};
#[cfg(test)]
use pnl_reconcile::aggregate_execution_pnl;
use pnl_reconcile::exchange_fill_time;
#[cfg(test)]
use risk_guards::{buy_exposure_usdc, liquidation_too_close};
use risk_guards::{opening_exposure_usdc, RiskConfigSnapshot};
use window::{LastFillSnapshot, PaperWindowCache};

const GRID_ORDER_PREFIX: &str = "gb_";

const PERIODIC_TIME_SYNC_INTERVAL: Duration = Duration::from_secs(30 * 60);
const TIME_SYNC_RETRY_INTERVAL: Duration = Duration::from_secs(60);
fn is_grid_order(client_order_id: &str) -> bool {
    client_order_id.starts_with(GRID_ORDER_PREFIX)
}

fn unresolved_filled_quantity(order: &BinanceOrderResponse) -> bool {
    order.status == "FILLED" && order.executed_qty <= Decimal::ZERO
}

/// All idle orders follow the window. Partial executions finish on their existing order.
fn is_window_order(order: &GridOrder) -> bool {
    is_grid_order(&order.client_order_id) && order.status == OrderStatus::New
}

// Paper accounts have no exchange-provided leverage until one is explicitly set.
// Treat the default zero as 1x so a missing leverage never understates margin.
fn update_paper_account(
    position: &mut PositionInfo,
    account: &mut AccountInfo,
    ticker: &TickerInfo,
) {
    let price = ticker.grid_reference_price();
    if price > Decimal::ZERO {
        position.mark_price = price;
    }
    position.unrealized_pnl = if position.size.is_zero() || position.mark_price <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        let diff = position.mark_price.saturating_sub(position.entry_price);
        diff.checked_mul(position.size).unwrap_or_else(|| {
            if diff.is_sign_negative() == position.size.is_sign_negative() {
                Decimal::MAX
            } else {
                Decimal::MIN
            }
        })
    };
    account.unrealized_profit = position.unrealized_pnl;
    account.margin_balance = account
        .total_wallet_balance
        .saturating_add(position.unrealized_pnl);
    account.available_balance = account
        .margin_balance
        .saturating_sub(position.position_margin());
    account.update_time = if ticker.mark_price > Decimal::ZERO {
        ticker.mark_update_time
    } else {
        ticker.update_time
    };
}

fn has_nearby_grid_order(
    prices: &[Decimal],
    target: Decimal,
    grid_interval: Decimal,
    tick_size: Decimal,
) -> bool {
    let spacing = grid_interval.max(tick_size);
    prices.iter().any(|&price| {
        price
            .checked_sub(target)
            .is_some_and(|diff| diff.abs() < spacing)
    })
}

/// Same-side active orders reserve their current price level.
fn grid_level_occupied<'a>(
    orders: impl IntoIterator<Item = &'a GridOrder>,
    side: OrderSide,
    price: Decimal,
    interval: Decimal,
    tick_size: Decimal,
) -> bool {
    let spacing = interval.max(tick_size);
    orders.into_iter().any(|order| {
        order.side == side
            && order
                .price
                .checked_sub(price)
                .is_some_and(|diff| diff.abs() < spacing)
    })
}

fn reserved_closing_quantity<'a>(
    direction: GridDirection,
    orders: impl IntoIterator<Item = &'a GridOrder>,
) -> Decimal {
    orders
        .into_iter()
        .filter(|o| o.side == direction.closing_side())
        .fold(Decimal::ZERO, |acc, o| {
            acc.checked_add(o.quantity).unwrap_or(Decimal::MAX)
        })
}

fn closing_quantity_available<'a>(
    direction: GridDirection,
    position_size: Decimal,
    orders: impl IntoIterator<Item = &'a GridOrder>,
) -> Decimal {
    direction
        .position_quantity(position_size)
        .checked_sub(reserved_closing_quantity(direction, orders))
        .unwrap_or(Decimal::ZERO)
        .max(Decimal::ZERO)
}

#[cfg(test)]
fn reserved_sell_quantity(orders: &[GridOrder]) -> Decimal {
    reserved_closing_quantity(GridDirection::Long, orders)
}

#[cfg(test)]
fn sell_quantity_available(position_size: Decimal, orders: &[GridOrder]) -> Decimal {
    closing_quantity_available(GridDirection::Long, position_size, orders)
}

fn build_sync_timer(sync_secs: u64) -> tokio::time::Interval {
    let period = Duration::from_secs(sync_secs);
    let offset =
        Duration::from_millis(rand::random::<u64>() % sync_secs.saturating_mul(1000).max(1));
    let mut timer = interval_at(tokio::time::Instant::now() + offset, period);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    timer
}

pub struct GridTradingEngine {
    state: Arc<AppState>,
    client: Arc<BinanceFuturesClient>,
    account_cache: Arc<AccountCache>,
    position_cache: Arc<PositionCache>,
    market_cache: Arc<MarketCache>,
    action_rx: mpsc::Receiver<BotControlAction>,
    ticker_rx: broadcast::Receiver<TickerInfo>,
    pnl_reconcile_offset: usize,
    pnl_reconcile_task: Option<JoinHandle<Option<Instant>>>,
    pnl_retry_at: Option<Instant>,
    pnl_order_ids: HashMap<String, i64>,
    pnl_clear_revision: u64,
    last_account_sync: Option<Instant>,
    last_orders_sync: Option<Instant>,
    sync_interval_secs: u64,
    next_time_sync_at: Instant,
    mark_retry: MarketRetry,
    ticker_retry: MarketRetry,
    market_request_generation: u64,
    mark_price_task: Option<JoinHandle<MarkPriceCompletion>>,
    ticker_task: Option<JoinHandle<TickerCompletion>>,
    pending_paper_ticker: Option<(Decimal, chrono::DateTime<Utc>, u64)>,
    paper_window_cache: Option<PaperWindowCache>,
    reconciliation_blocked: bool,
    pub(super) evidence: OrderEvidenceBook,
    exchange_rules_loaded: bool,
    market_data_blocked: bool,
    last_remainder_change: Option<Instant>,
    market_stream_tx: Option<watch::Sender<String>>,
    last_fill_cache: std::sync::Mutex<Option<(String, TradingMode, LastFillSnapshot)>>,
}

impl Drop for GridTradingEngine {
    fn drop(&mut self) {
        self.retire_market_requests();
        if let Some(task) = &self.pnl_reconcile_task {
            task.abort();
        }
    }
}

impl GridTradingEngine {
    pub fn new(
        state: Arc<AppState>,
        client: Arc<BinanceFuturesClient>,
        action_rx: mpsc::Receiver<BotControlAction>,
        ticker_rx: broadcast::Receiver<TickerInfo>,
    ) -> Self {
        Self::new_with_caches(
            state,
            client,
            action_rx,
            ticker_rx,
            Arc::new(AccountCache::default()),
            Arc::new(PositionCache::default()),
            Arc::new(MarketCache::default()),
        )
    }

    pub(crate) fn new_with_caches(
        state: Arc<AppState>,
        client: Arc<BinanceFuturesClient>,
        action_rx: mpsc::Receiver<BotControlAction>,
        ticker_rx: broadcast::Receiver<TickerInfo>,
        account_cache: Arc<AccountCache>,
        position_cache: Arc<PositionCache>,
        market_cache: Arc<MarketCache>,
    ) -> Self {
        let sync_interval_secs = state
            .config
            .try_read()
            .map(|c| c.exchange.sync_interval_secs.max(1))
            .unwrap_or(5);
        Self {
            state,
            client,
            account_cache,
            position_cache,
            market_cache,
            action_rx,
            ticker_rx,
            pnl_reconcile_offset: 0,
            pnl_reconcile_task: None,
            pnl_retry_at: None,
            pnl_order_ids: HashMap::new(),
            pnl_clear_revision: 0,
            last_account_sync: None,
            last_orders_sync: None,
            sync_interval_secs,
            next_time_sync_at: Instant::now() + PERIODIC_TIME_SYNC_INTERVAL,
            mark_retry: MarketRetry::default(),
            ticker_retry: MarketRetry::default(),
            market_request_generation: 0,
            mark_price_task: None,
            ticker_task: None,
            pending_paper_ticker: None,
            paper_window_cache: None,
            reconciliation_blocked: false,
            evidence: OrderEvidenceBook::default(),
            exchange_rules_loaded: false,
            market_data_blocked: false,
            last_remainder_change: None,
            market_stream_tx: None,
            last_fill_cache: std::sync::Mutex::new(None),
        }
    }

    pub(crate) fn set_market_stream_tx(&mut self, tx: watch::Sender<String>) {
        self.market_stream_tx = Some(tx);
    }

    pub(super) async fn pause_trading(&self) {
        if let Err(error) = self.state.pause_trading().await {
            error!("Could not persist paused bot status: {}", error);
        }
    }

    pub(super) async fn set_global_reconciliation_ready(&self, ready: bool) {
        self.state
            .global_position_ready
            .store(ready, std::sync::atomic::Ordering::SeqCst);
        self.state.check_global_position_limit().await;
    }

    pub(super) async fn report_reconciliation_state(&mut self, blocked: bool) {
        if blocked {
            self.set_global_reconciliation_ready(false).await;
        }
        if self.reconciliation_blocked == blocked {
            return;
        }
        self.reconciliation_blocked = blocked;
        let (level, message) = if blocked {
            (
                "ERROR",
                "Order reconciliation incomplete; new live orders are paused",
            )
        } else {
            (
                "INFO",
                "Order reconciliation restored; live order placement resumed",
            )
        };
        self.state.add_log(level, message).await;
    }

    /// Initialize exchange connection, sync server time, and load symbol rules
    pub(crate) async fn initialize(&mut self) -> Result<()> {
        let config = self.state.config.read().await.clone();
        self.sync_interval_secs = config.exchange.sync_interval_secs.max(1);
        let symbol = config.exchange.symbol.clone();
        let is_paper = config.exchange.is_paper();

        self.state
            .add_log(
                "INFO",
                format!(
                    "Initializing Grid Engine for {} (Interval: {} USDC, Amount: {} USDC, Mode: {})",
                    symbol,
                    config.grid.grid_interval,
                    config.grid.order_amount_usdc,
                    if is_paper { "PAPER (模拟盘)" } else { "LIVE" }
                ),
            )
            .await;

        // Restore managed metadata before any exchange request can fail. A
        // subsequent initial pause must reconcile these orders before clearing them.
        if !is_paper {
            let restored = self.load_managed_orders(&symbol).await?;
            self.insert_restored_managed_orders(restored).await;
        }

        match self.client.sync_server_time().await {
            Ok(_) => self.next_time_sync_at = Instant::now() + PERIODIC_TIME_SYNC_INTERVAL,
            Err(e) => {
                warn!("Failed to synchronize Binance server time: {}", e);
                self.next_time_sync_at = Instant::now() + TIME_SYNC_RETRY_INTERVAL;
            }
        }

        if let Err(e) = self.load_exchange_rules(&symbol).await {
            warn!("Could not load exchange rules for {}: {}", symbol, e);
            if !is_paper {
                return Err(anyhow!(
                    "Could not load Binance exchange rules for {}: {}",
                    symbol,
                    e
                ));
            }
        }

        self.state
            .rules
            .read()
            .await
            .validate_grid_interval(config.grid.grid_interval)?;

        self.seed_initial_market_snapshot(&symbol, is_paper).await?;

        // Reconcile restored orders only after the exchange prerequisites are ready.
        if !is_paper && (!self.sync_account_and_position().await || !self.sync_live_orders().await)
        {
            return Err(anyhow!(
                "Could not reconcile Binance account and managed orders at startup"
            ));
        }

        Ok(())
    }

    async fn seed_initial_market_snapshot(&mut self, symbol: &str, is_paper: bool) -> Result<()> {
        let ticker_requested_at = Utc::now();
        match self.client.get_ticker_price(symbol).await {
            Ok(price) if price > Decimal::ZERO => {
                let mut ticker = self.state.ticker.write().await;
                ticker.symbol = symbol.to_owned();
                ticker.last_price = price;
                ticker.update_time = ticker_requested_at;
                info!("Initial market price for {}: {}", symbol, price);
            }
            Ok(price) => {
                warn!("Invalid initial market price for {}: {}", symbol, price);
                if !is_paper {
                    return Err(anyhow!("Initial market price must be positive"));
                }
            }
            Err(e) => {
                warn!("Failed to fetch initial market price: {}", e);
                if !is_paper {
                    return Err(anyhow!("Could not load initial market price: {}", e));
                }
            }
        }

        let mark_requested_at = Utc::now();
        match self.client.get_mark_price(symbol).await {
            Ok(price) if price > Decimal::ZERO => {
                let mut ticker = self.state.ticker.write().await;
                ticker.mark_price = price;
                ticker.mark_update_time = mark_requested_at;
            }
            Ok(price) => warn!("Invalid initial mark price for {}: {}", symbol, price),
            Err(e) => warn!("Failed to fetch initial mark price: {}", e),
        }
        if is_paper {
            self.update_paper_valuation().await;
        }

        if let Ok(t24) = self.client.get_24hr_ticker(symbol).await {
            let mut ticker = self.state.ticker.write().await;
            ticker.high_24h = t24.high_price;
            ticker.low_24h = t24.low_price;
            ticker.change_24h = t24.price_change;
            ticker.change_percent_24h = t24.price_change_percent;
            ticker.volume_24h = t24.volume;
        }
        Ok(())
    }

    /// Clear recovered live orders before processing controls queued during initialization.
    pub(crate) async fn reconcile_initial_pause(&mut self) {
        let live = !self.state.config.read().await.exchange.is_paper();
        let paused = *self.state.status.read().await == BotStatus::Paused;
        if live && paused && !self.state.is_shutting_down() && !self.cancel_all_orders().await {
            self.state
                .add_log(
                    "ERROR",
                    "Startup cancellation incomplete; trading remains paused",
                )
                .await;
        }
    }

    /// Primary execution loop
    pub(crate) async fn run(&mut self) {
        let mut sync_secs = self
            .state
            .config
            .read()
            .await
            .exchange
            .sync_interval_secs
            .max(1);
        self.sync_interval_secs = sync_secs;
        let mut sync_timer = build_sync_timer(sync_secs);
        let mut pnl_timer = interval(Duration::from_secs(1));
        pnl_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Preserve recovered orders. Reconciliation and the regular window
        // maintenance will add only the missing levels.
        if *self.state.status.read().await == BotStatus::Running {
            self.maintain_grid_window().await;
        }

        let mut ticker_open = true;
        loop {
            tokio::select! {
                biased;
                // UI control actions (Pause, Resume, CancelAll, Rebalance, UpdateConfig)
                action = self.action_rx.recv() => {
                    match action {
                        Some(action) => {
                            let stopping = matches!(action, BotControlAction::Shutdown { .. });
                            let updating_config = matches!(action, BotControlAction::UpdateConfig { .. } | BotControlAction::UpdateGlobalConfig { .. });
                            self.handle_control_action(action).await;
                            if stopping { break; }
                            if updating_config {
                                // Use the installed config even if post-save reconciliation failed.
                                let updated_secs = self.state.config.read().await.exchange.sync_interval_secs.max(1);
                                self.sync_interval_secs = updated_secs;
                                if updated_secs != sync_secs {
                                    sync_secs = updated_secs;
                                    sync_timer = build_sync_timer(sync_secs);
                                }
                            }
                        },
                        None => break,
                    }
                }

                // Live price updates from Binance WebSocket
                ticker_res = self.ticker_rx.recv(), if ticker_open => {
                    match ticker_res {
                        Ok(ticker_update) => self.handle_ticker_update(ticker_update).await,
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => ticker_open = false,
                    }
                }
                // Regular synchronization timer
                _ = sync_timer.tick() => {
                    self.sync_cycle(false).await;
                    self.reconcile_unverified_trades().await;
                }

                _ = pnl_timer.tick() => {
                    self.reconcile_unverified_trades().await;
                }

                completion = market_completion(&mut self.mark_price_task) => {
                    self.finish_mark_price_task(completion).await;
                }

                completion = market_completion(&mut self.ticker_task) => {
                    self.finish_ticker_task(completion).await;
                }
            }
        }
        self.retire_market_requests();
    }
}

#[cfg(test)]
#[path = "reliability_tests.rs"]
mod reliability_tests;

#[cfg(test)]
#[path = "grid_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "fixed_grid_tests.rs"]
mod fixed_grid_tests;

#[cfg(test)]
#[path = "multi_pair_tests.rs"]
mod multi_pair_tests;

#[cfg(test)]
#[path = "capacity_benchmark.rs"]
mod capacity_benchmark;

#[cfg(test)]
#[path = "capacity_orders_benchmark.rs"]
mod capacity_orders_benchmark;

#[cfg(test)]
#[path = "event_loop_latency_benchmark.rs"]
mod event_loop_latency_benchmark;

#[cfg(test)]
#[path = "short_tests.rs"]
mod short_tests;
