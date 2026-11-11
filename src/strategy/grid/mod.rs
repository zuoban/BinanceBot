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
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::interval;
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

mod account_sync;
mod config_transition;
mod conflict_resolution;
mod conflict_verification;
mod control_actions;
mod evidence;
mod evidence_book;
mod live_submission;
mod market_fallback;
mod market_feed;
mod order_cancel;
mod order_domain;
mod order_ops;
mod order_sizing;
mod pair_intents;
mod pnl_reconcile;
mod reconciliation;
mod reconciliation_anomalies;
mod reconciliation_settlement;
mod remainder;
mod remainder_settlement;
mod risk_guards;
mod snapshot_conflicts;
mod snapshot_identities;
mod window;
mod window_prune;

use account_sync::build_sync_timer;
#[cfg(test)]
use account_sync::live_sync_freshness;
use evidence_book::OrderEvidenceBook;
use live_submission::LiveGridOrderSubmission;
use market_fallback::{market_completion, MarkPriceCompletion, MarketRetry, TickerCompletion};
use order_domain::*;
use order_sizing::{GridOrderSpec, WindowPlacementContext};
#[cfg(test)]
use pair_intents::{new_pair_intent, paired_grid_client_order_id, PairPlacementDecision};
#[cfg(test)]
use pnl_reconcile::aggregate_execution_pnl;
use pnl_reconcile::exchange_fill_time;
#[cfg(test)]
use risk_guards::{buy_exposure_usdc, liquidation_too_close};
use risk_guards::{opening_exposure_usdc, RiskConfigSnapshot};
use window::LastFillSnapshot;
use window_prune::PaperWindowCache;

const PERIODIC_TIME_SYNC_INTERVAL: Duration = Duration::from_secs(30 * 60);
const TIME_SYNC_RETRY_INTERVAL: Duration = Duration::from_secs(60);

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

    pub(super) async fn mark_orders_reconciliation_blocked(&mut self) {
        self.last_orders_sync = None;
        self.report_reconciliation_state(true).await;
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

        if let Err(e) = Box::pin(self.load_exchange_rules(&symbol)).await {
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

        Box::pin(self.seed_initial_market_snapshot(&symbol, is_paper)).await?;

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

    async fn handle_loop_action(
        &mut self,
        action: Option<BotControlAction>,
        sync_secs: &mut u64,
        sync_timer: &mut tokio::time::Interval,
    ) -> bool {
        let Some(action) = action else {
            return false;
        };
        let stopping = matches!(action, BotControlAction::Shutdown { .. });
        let updating_config = matches!(
            action,
            BotControlAction::UpdateConfig { .. } | BotControlAction::UpdateGlobalConfig { .. }
        );
        Box::pin(self.handle_control_action(action)).await;
        if stopping {
            return false;
        }
        if updating_config {
            // Use the installed config even if post-save reconciliation failed.
            let updated_secs = self
                .state
                .config
                .read()
                .await
                .exchange
                .sync_interval_secs
                .max(1);
            self.sync_interval_secs = updated_secs;
            if updated_secs != *sync_secs {
                *sync_secs = updated_secs;
                *sync_timer = build_sync_timer(*sync_secs);
            }
        }
        true
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

        if *self.state.status.read().await == BotStatus::Running {
            Box::pin(self.maintain_grid_window()).await;
        }

        let mut ticker_open = true;
        loop {
            tokio::select! {
                biased;
                action = self.action_rx.recv() => {
                    let fut = self.handle_loop_action(action, &mut sync_secs, &mut sync_timer);
                    if !Box::pin(fut).await {
                        break;
                    }
                }
                ticker_res = self.ticker_rx.recv(), if ticker_open => {
                    match ticker_res {
                        Ok(ticker_update) => self.handle_ticker_update(ticker_update).await,
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => ticker_open = false,
                    }
                }
                _ = sync_timer.tick() => {
                    Box::pin(self.sync_cycle(false)).await;
                    self.reconcile_unverified_trades().await;
                }
                _ = pnl_timer.tick() => {
                    self.reconcile_unverified_trades().await;
                }
                completion = market_completion(&mut self.mark_price_task) => {
                    Box::pin(self.finish_mark_price_task(completion)).await;
                }
                completion = market_completion(&mut self.ticker_task) => {
                    Box::pin(self.finish_ticker_task(completion)).await;
                }
            }
        }
        self.retire_market_requests();
    }
}

#[cfg(test)]
mod reliability_tests;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod fixed_grid_tests;

#[cfg(test)]
mod multi_pair_tests;

#[cfg(test)]
mod capacity_benchmark;

#[cfg(test)]
mod capacity_orders_benchmark;

#[cfg(test)]
mod event_loop_latency_benchmark;

#[cfg(test)]
mod short_tests;
