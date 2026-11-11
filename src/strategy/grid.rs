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

#[path = "conflict_resolution.rs"]
mod conflict_resolution;
#[path = "evidence.rs"]
mod evidence;
#[path = "market_feed.rs"]
mod market_feed;
#[path = "order_ops.rs"]
mod order_ops;
#[path = "pair_intents.rs"]
mod pair_intents;
#[path = "pnl_reconcile.rs"]
mod pnl_reconcile;
#[path = "reconciliation.rs"]
mod reconciliation;
#[path = "remainder.rs"]
mod remainder;
#[path = "window.rs"]
mod window;

use evidence::OrderEvidenceBook;
use market_feed::{market_completion, MarkPriceCompletion, MarketRetry, TickerCompletion};
use pnl_reconcile::exchange_fill_time;
use window::{LastFillSnapshot, PaperWindowCache};

#[cfg(test)]
use pair_intents::{new_pair_intent, paired_grid_client_order_id, PairPlacementDecision};
#[cfg(test)]
use pnl_reconcile::aggregate_execution_pnl;

const GRID_ORDER_PREFIX: &str = "gb_";

const LIVE_SYNC_FRESHNESS: Duration = Duration::from_secs(10);
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

fn opening_exposure_usdc<'a>(
    direction: GridDirection,
    position_size: Decimal,
    mark_price: Decimal,
    orders: impl IntoIterator<Item = &'a GridOrder>,
) -> Decimal {
    direction.position_quantity(position_size) * mark_price
        + orders
            .into_iter()
            .filter(|order| order.side == direction.opening_side())
            .map(|order| order.price * order.quantity)
            .sum::<Decimal>()
}

#[cfg(test)]
fn buy_exposure_usdc(position_size: Decimal, mark_price: Decimal, orders: &[GridOrder]) -> Decimal {
    opening_exposure_usdc(GridDirection::Long, position_size, mark_price, orders)
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
        (position.mark_price - position.entry_price) * position.size
    };
    account.unrealized_profit = position.unrealized_pnl;
    account.margin_balance = account.total_wallet_balance + position.unrealized_pnl;
    account.available_balance = account.margin_balance - position.position_margin();
    account.update_time = if ticker.mark_price > Decimal::ZERO {
        ticker.mark_update_time
    } else {
        ticker.update_time
    };
}

/// Distance from `mark` to `liquidation` as a percentage of `mark`, only on the losing side of
/// the position (below mark for longs, above for shorts). Binance reports 0 when no liquidation
/// price applies, which never counts as close.
fn liquidation_too_close(
    size: Decimal,
    mark: Decimal,
    liquidation: Decimal,
    threshold_pct: Option<Decimal>,
) -> bool {
    let Some(threshold) = threshold_pct else {
        return false;
    };
    if size.is_zero() || mark <= Decimal::ZERO || liquidation <= Decimal::ZERO {
        return false;
    }
    let gap = if size > Decimal::ZERO {
        mark - liquidation
    } else {
        liquidation - mark
    };
    gap * Decimal::ONE_HUNDRED <= threshold * mark
}

fn has_nearby_grid_order(
    prices: &[Decimal],
    target: Decimal,
    grid_interval: Decimal,
    tick_size: Decimal,
) -> bool {
    let spacing = grid_interval.max(tick_size);
    prices.iter().any(|&price| (price - target).abs() < spacing)
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
    orders
        .into_iter()
        .any(|order| order.side == side && (order.price - price).abs() < spacing)
}

fn reserved_closing_quantity<'a>(
    direction: GridDirection,
    orders: impl IntoIterator<Item = &'a GridOrder>,
) -> Decimal {
    orders
        .into_iter()
        .filter(|o| o.side == direction.closing_side())
        .map(|o| o.quantity)
        .sum()
}

fn closing_quantity_available<'a>(
    direction: GridDirection,
    position_size: Decimal,
    orders: impl IntoIterator<Item = &'a GridOrder>,
) -> Decimal {
    (direction.position_quantity(position_size) - reserved_closing_quantity(direction, orders))
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

struct ConfigScopeReset<'a> {
    new_config: &'a crate::config::AppConfig,
    new_client: Arc<BinanceFuturesClient>,
    new_rules: SymbolRules,
    new_price: Option<Decimal>,
    paper_state: Option<(PositionInfo, AccountInfo)>,
    market_changed: bool,
    mode_changed: bool,
    client_changed: bool,
    fetch_rules: bool,
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

    pub(super) async fn refresh_position_then_account(&mut self, shared: bool) -> bool {
        let position = self.sync_position(shared).await;
        let account = if position.is_some() {
            self.sync_account(shared, !shared).await
        } else {
            None
        };
        self.last_account_sync = position.zip(account).map(|(p, a)| p.min(a));
        self.last_account_sync.is_some()
    }

    fn mark_price_is_fresh(ticker: &TickerInfo) -> bool {
        ticker.mark_price_is_fresh(Utc::now())
    }

    fn position_mark_is_fresh(
        &self,
        config: &crate::config::AppConfig,
        position: &PositionInfo,
    ) -> bool {
        let max_age = max_reconciliation_age_secs(config.exchange.sync_interval_secs);
        position.mark_price > Decimal::ZERO
            && position
                .update_time
                .is_some_and(|time| is_timestamp_fresh(time, Utc::now(), max_age))
    }

    fn position_valuation_price(
        &self,
        config: &crate::config::AppConfig,
        position: &PositionInfo,
        ticker: &TickerInfo,
    ) -> Decimal {
        position.valuation_price_with_freshness(
            ticker,
            Self::mark_price_is_fresh(ticker),
            self.position_mark_is_fresh(config, position),
        )
    }

    pub fn set_market_stream_tx(&mut self, tx: watch::Sender<String>) {
        self.market_stream_tx = Some(tx);
    }

    async fn pause_trading(&self) {
        if let Err(error) = self.state.pause_trading().await {
            error!("Could not persist paused bot status: {}", error);
        }
    }

    /// Check filled position value independently of opening-order reservations.
    async fn pause_if_position_limit_reached(&mut self) -> bool {
        if let Some(global) = self.state.global_limit() {
            if self
                .state
                .config
                .read()
                .await
                .global_max_position_notional
                .is_some()
                && !global
                    .membership_ready
                    .load(std::sync::atomic::Ordering::SeqCst)
            {
                return true;
            }
            if global.check().await {
                return true;
            }
        }
        if *self.state.status.read().await != BotStatus::Running {
            return false;
        }
        let config = self.state.config.read().await;
        if !config.grid.pause_on_max_position {
            return false;
        }
        let Some(limit) = config
            .grid
            .max_position_usdc
            .filter(|limit| *limit > Decimal::ZERO)
        else {
            return false;
        };
        let position = self.state.position.read().await;
        let ticker = self.state.ticker.read().await;
        let valuation_price = self.position_valuation_price(&config, &position, &ticker);
        let position_value = position.size.abs() * valuation_price;
        drop(ticker);
        drop(position);
        drop(config);
        if valuation_price <= Decimal::ZERO || position_value < limit {
            return false;
        }

        self.pause_for_position_limit(format!(
            "持仓名义价值 {} 已达到最大持仓名义价值 {}",
            position_value, limit
        ))
        .await;
        true
    }

    /// True while opening must be suspended without pausing the strategy: the filled position
    /// value is at the cap in cancel-opening mode, or the mark price is within the configured
    /// distance of the liquidation price.
    async fn opening_suspended_at_limit(&self, config: &crate::config::AppConfig) -> bool {
        let cap = config.grid.max_position_usdc.filter(|limit| {
            *limit > Decimal::ZERO
                && config.grid.cancel_opening_on_max_position
                && !config.grid.pause_on_max_position
        });
        if cap.is_none() && config.grid.min_liquidation_distance_pct.is_none() {
            return false;
        }
        let position = self.state.position.read().await;
        let size = position.size;
        let liquidation_price = position.liquidation_price;
        let liquidation_guard =
            config.grid.min_liquidation_distance_pct.is_some() && liquidation_price > Decimal::ZERO;
        if size.is_zero() || (cap.is_none() && !liquidation_guard) {
            return false;
        }
        let ticker = self.state.ticker.read().await;
        // Market marks follow the existing 15-second market-data freshness limit.
        // Position marks follow the account reconciliation freshness allowance.
        let price = if Self::mark_price_is_fresh(&ticker) {
            ticker.mark_price
        } else if self.position_mark_is_fresh(config, &position) {
            position.mark_price
        } else {
            // Last trades cannot establish liquidation distance. Keep openings
            // suspended until a fresh mark can establish that protection is safe.
            return true;
        };
        drop(ticker);
        drop(position);
        if cap.is_some_and(|limit| size.abs() * price >= limit) {
            return true;
        }
        liquidation_too_close(
            size,
            price,
            liquidation_price,
            config.grid.min_liquidation_distance_pct,
        )
    }

    /// Cancel only idle opening orders; partial executions retain their existing protection.
    /// Keep this outside fill recording so cancellation cannot recurse through another fill.
    async fn cancel_suspended_opening_orders(&mut self, config: &crate::config::AppConfig) -> bool {
        if self.state.is_shutting_down() || *self.state.status.read().await != BotStatus::Running {
            return true;
        }
        if !self.opening_suspended_at_limit(config).await {
            return true;
        }
        let mut openings: Vec<_> = self
            .state
            .active_orders
            .read()
            .await
            .values()
            .filter(|order| {
                order.side == config.grid.direction.opening_side() && is_window_order(order)
            })
            .cloned()
            .collect();
        openings.sort_by(|a, b| a.client_order_id.cmp(&b.client_order_id));
        let mut canceled = true;
        for order in openings {
            if *self.state.status.read().await != BotStatus::Running {
                return false;
            }
            canceled &= self.cancel_single_order(&order).await;
        }
        canceled
    }

    async fn protect_openings_and_check_limits(
        &mut self,
        config: &crate::config::AppConfig,
    ) -> bool {
        let protected = self.cancel_suspended_opening_orders(config).await;
        let paused = self.pause_if_position_limit_reached().await;
        protected && !paused
    }

    async fn pause_for_position_limit(&mut self, reason: String) {
        // Pause before canceling so cancellation cannot restore Running or replenish orders.
        self.pause_trading().await;
        self.state
            .add_log("WARN", format!("{}，策略已暂停，需手动恢复", reason))
            .await;
        if !self.cancel_all_orders().await {
            self.state
                .add_log(
                    "ERROR",
                    "策略已暂停，但部分策略挂单可能尚未撤销，请检查交易所挂单",
                )
                .await;
        }
    }

    async fn position_direction_ready(&mut self) -> bool {
        let direction = self.state.config.read().await.grid.direction;
        if self.state.position.read().await.size * direction.sign() >= Decimal::ZERO {
            return true;
        }
        self.pause_trading().await;
        self.state
            .add_log("ERROR", "持仓方向与策略方向不一致，策略已暂停")
            .await;
        self.cancel_all_orders().await;
        false
    }

    async fn ensure_flat_for_direction_change(&mut self, direction: GridDirection) -> Result<()> {
        if !self.state.config.read().await.exchange.is_paper() {
            anyhow::ensure!(
                self.sync_account_and_position().await,
                "无法确认当前持仓，交易方向未切换"
            );
        }
        anyhow::ensure!(
            self.state.position.read().await.size * direction.sign() >= Decimal::ZERO,
            "切换做多 / 做空方向前，请先平掉当前持仓"
        );
        Ok(())
    }

    async fn set_global_reconciliation_ready(&self, ready: bool) {
        self.state
            .global_position_ready
            .store(ready, std::sync::atomic::Ordering::SeqCst);
        self.state.check_global_position_limit().await;
    }

    async fn report_reconciliation_state(&mut self, blocked: bool) {
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
    pub async fn initialize(&mut self) -> Result<()> {
        let config = self.state.config.read().await.clone();
        let symbol = config.exchange.symbol.clone();

        self.state
            .add_log(
                "INFO",
                format!(
                    "Initializing Grid Engine for {} (Interval: {} USDC, Amount: {} USDC, Mode: {})",
                    symbol,
                    config.grid.grid_interval,
                    config.grid.order_amount_usdc,
                    if config.exchange.is_paper() {
                        "PAPER (模拟盘)"
                    } else {
                        "LIVE"
                    }
                ),
            )
            .await;

        // Restore managed metadata before any exchange request can fail. A
        // subsequent initial pause must reconcile these orders before clearing them.
        if !config.exchange.is_paper() {
            let restored = self.load_managed_orders(&symbol).await?;
            self.insert_restored_managed_orders(restored).await;
        }

        // Synchronize server time
        if let Err(e) = self.client.sync_server_time().await {
            warn!("Failed to synchronize Binance server time: {}", e);
        }

        // Fetch symbol rules from exchangeInfo
        match self.load_exchange_rules(&symbol).await {
            Ok(()) => {}
            Err(e) => {
                warn!("Could not load exchange rules for {}: {}", symbol, e);
                if !config.exchange.is_paper() {
                    return Err(anyhow!(
                        "Could not load Binance exchange rules for {}: {}",
                        symbol,
                        e
                    ));
                }
            }
        }

        self.state
            .rules
            .read()
            .await
            .validate_grid_interval(config.grid.grid_interval)?;

        // Fetch initial market price
        let ticker_requested_at = Utc::now();
        match self.client.get_ticker_price(&symbol).await {
            Ok(price) if price > Decimal::ZERO => {
                let mut ticker = self.state.ticker.write().await;
                ticker.symbol = symbol.clone();
                ticker.last_price = price;
                ticker.update_time = ticker_requested_at;
                info!("Initial market price for {}: {}", symbol, price);
            }
            Ok(price) => {
                warn!("Invalid initial market price for {}: {}", symbol, price);
                if !config.exchange.is_paper() {
                    return Err(anyhow!("Initial market price must be positive"));
                }
            }
            Err(e) => {
                warn!("Failed to fetch initial market price: {}", e);
                if !config.exchange.is_paper() {
                    return Err(anyhow!("Could not load initial market price: {}", e));
                }
            }
        }

        let mark_requested_at = Utc::now();
        match self.client.get_mark_price(&symbol).await {
            Ok(price) if price > Decimal::ZERO => {
                let mut ticker = self.state.ticker.write().await;
                ticker.mark_price = price;
                ticker.mark_update_time = mark_requested_at;
            }
            Ok(price) => warn!("Invalid initial mark price for {}: {}", symbol, price),
            Err(e) => warn!("Failed to fetch initial mark price: {}", e),
        }
        if config.exchange.is_paper() {
            self.update_paper_valuation().await;
        }

        // Fetch 24hr stats
        if let Ok(t24) = self.client.get_24hr_ticker(&symbol).await {
            let mut ticker = self.state.ticker.write().await;
            ticker.high_24h = t24.high_price;
            ticker.low_24h = t24.low_price;
            ticker.change_24h = t24.price_change;
            ticker.change_percent_24h = t24.price_change_percent;
            ticker.volume_24h = t24.volume;
        }

        // Reconcile restored orders only after the exchange prerequisites are ready.
        if !config.exchange.is_paper()
            && (!self.sync_account_and_position().await || !self.sync_live_orders().await)
        {
            return Err(anyhow!(
                "Could not reconcile Binance account and managed orders at startup"
            ));
        }

        Ok(())
    }

    async fn fetch_symbol_rules_with_client(
        client: &BinanceFuturesClient,
        symbol: &str,
    ) -> Result<SymbolRules> {
        let info = client.get_exchange_info(Some(symbol)).await?;
        SymbolRules::from_trading_exchange_info(&info, symbol)
            .map_err(|error| error.into_chinese_error(symbol))
    }

    async fn load_exchange_rules(&mut self, symbol: &str) -> Result<()> {
        let rules = Self::fetch_symbol_rules_with_client(&self.client, symbol).await?;
        info!(
            "Loaded Binance exchange rules for {}: tick_size={}, step_size={}, min_notional={}",
            symbol, rules.tick_size, rules.step_size, rules.min_notional
        );
        self.state
            .add_log(
                "INFO",
                format!(
                    "Loaded Binance exchange rules for {}: tick_size={}, step_size={}",
                    symbol, rules.tick_size, rules.step_size
                ),
            )
            .await;
        *self.state.rules.write().await = rules;
        self.exchange_rules_loaded = true;
        Ok(())
    }

    /// Clear recovered live orders before processing controls queued during initialization.
    pub async fn reconcile_initial_pause(&mut self) {
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
    pub async fn run(&mut self) {
        let mut sync_secs = self
            .state
            .config
            .read()
            .await
            .exchange
            .sync_interval_secs
            .max(1);
        let mut sync_timer = build_sync_timer(sync_secs);
        let mut pnl_timer = interval(Duration::from_secs(1));
        pnl_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Preserve recovered orders. Reconciliation and the regular window
        // maintenance will add only the missing levels.
        if *self.state.status.read().await == BotStatus::Running {
            self.maintain_grid_window().await;
        }

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
                Ok(ticker_update) = self.ticker_rx.recv() => {
                    self.handle_ticker_update(ticker_update).await;
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

    async fn handle_control_action(&mut self, action: BotControlAction) {
        if self.state.is_shutting_down() && !matches!(action, BotControlAction::Shutdown { .. }) {
            if let BotControlAction::UpdateConfig { reply, .. }
            | BotControlAction::UpdateGlobalConfig { reply, .. }
            | BotControlAction::Resume { reply, .. }
            | BotControlAction::Rebalance { reply, .. } = action
            {
                let _ = reply.send(Err("程序正在退出，操作未执行".into()));
            }
            return;
        }
        match action {
            BotControlAction::Shutdown { removing, reply } => {
                let result = self
                    .shutdown_inner(removing)
                    .await
                    .map_err(|error| error.to_string());
                let _ = reply.send(result);
            }
            BotControlAction::Pause
            | BotControlAction::PauseAlreadyPersisted
            | BotControlAction::CancelAll
            | BotControlAction::CancelAllAlreadyPersisted => {
                let is_cancel_all = matches!(
                    action,
                    BotControlAction::CancelAll | BotControlAction::CancelAllAlreadyPersisted
                );
                let already_persisted = matches!(
                    action,
                    BotControlAction::PauseAlreadyPersisted
                        | BotControlAction::CancelAllAlreadyPersisted
                );
                let (info_msg, ok_log, err_log) = if is_cancel_all {
                    (
                        "Cancel all orders requested",
                        "Managed grid orders canceled; bot paused",
                        "Cancellation incomplete; bot paused",
                    )
                } else {
                    (
                        "Bot paused by user command",
                        "Trading bot paused and managed orders canceled",
                        "Trading bot paused, but some managed orders may still be open on Binance",
                    )
                };
                info!("{}", info_msg);
                if !already_persisted {
                    self.pause_trading().await;
                }
                if self.cancel_all_orders_inner(already_persisted).await {
                    self.state.add_log("WARN", ok_log).await;
                } else {
                    self.state.add_log("ERROR", err_log).await;
                }
            }
            BotControlAction::GlobalRiskCancel => {
                // The engine serializes submissions and cancellations. Any order already
                // in flight when the global gate closed has finished before this runs.
                let complete = self.cancel_all_orders().await;
                if complete {
                    self.state
                        .global_cancel_pending
                        .store(false, std::sync::atomic::Ordering::SeqCst);
                } else {
                    self.state
                        .add_log("ERROR", "全局风控撤单未完成，将继续重试")
                        .await;
                }
                self.state
                    .global_cancel_queued
                    .store(false, std::sync::atomic::Ordering::SeqCst);
            }
            BotControlAction::Resume {
                expected_mode,
                expected_epoch,
                reply,
            } => {
                self.handle_resume_action(expected_mode, expected_epoch, reply)
                    .await;
            }
            BotControlAction::Rebalance {
                expected_mode,
                reply,
            } => {
                let config = self.state.config.read().await;
                let current_mode = config.exchange.trading_mode();
                drop(config);
                let result =
                    if expected_mode == TradingMode::Unknown || expected_mode != current_mode {
                        Err("策略运行模式已改变，无法重建网格".into())
                    } else if *self.state.status.read().await != BotStatus::Running {
                        Err("策略未运行，无法重建网格".into())
                    } else {
                        info!("Grid rebalance requested");
                        self.state
                            .add_log("INFO", "Manual grid rebalance triggered")
                            .await;
                        self.rebalance_grid().await
                    };
                let _ = reply.send(result);
            }
            BotControlAction::UpdateConfig { config, reply } => {
                self.apply_and_reply_config(*config, "Configuration update rejected", reply)
                    .await;
            }
            BotControlAction::UpdateGlobalConfig { config, reply } => {
                let mut updated = self.state.config.read().await.clone();
                config.apply_to(&mut updated);
                self.apply_and_reply_config(updated, "Global configuration update rejected", reply)
                    .await;
            }
        }
    }

    async fn handle_resume_action(
        &mut self,
        expected_mode: TradingMode,
        expected_epoch: u64,
        reply: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    ) {
        let config = self.state.config.read().await.clone();
        let current_mode = config.exchange.trading_mode();
        if expected_mode == TradingMode::Unknown || current_mode != expected_mode {
            let _ = reply.send(Err("策略运行模式已改变，无法恢复策略".into()));
            return;
        }
        if !self.exchange_rules_loaded {
            if let Err(error) = self.load_exchange_rules(&config.exchange.symbol).await {
                if !config.exchange.is_paper() {
                    let message = format!(
                        "Resume failed: could not load Binance exchange rules for {}: {}",
                        config.exchange.symbol, error
                    );
                    self.state.add_log("ERROR", &message).await;
                    let _ = reply.send(Err(message));
                    return;
                }
                self.state
                    .add_log("WARN", format!("Could not load Binance exchange rules for {}; paper trading will use default rules: {}", config.exchange.symbol, error))
                    .await;
            }
        }
        if let Err(error) = self
            .state
            .rules
            .read()
            .await
            .validate_grid_interval(config.grid.grid_interval)
        {
            let message = format!("Resume failed: {}", error);
            self.state.add_log("ERROR", &message).await;
            let _ = reply.send(Err(message));
            return;
        }
        if let Err(error) = self
            .state
            .resume_trading_in_mode(expected_mode, expected_epoch)
            .await
        {
            error!("Could not resume bot: {}", error);
            let message = format!("Resume failed: {}", error);
            self.state.add_log("ERROR", &message).await;
            let _ = reply.send(Err(message));
            return;
        }
        info!("Bot resumed by user command");
        self.state
            .add_log("INFO", "Trading bot resumed by user")
            .await;
        let _ = reply.send(Ok(()));
        self.sync_cycle(false).await;
    }

    async fn apply_and_reply_config(
        &mut self,
        config: crate::config::AppConfig,
        log_prefix: &str,
        reply: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    ) {
        let result = self.apply_config(config).await;
        if let Err(error) = &result {
            self.state
                .add_log("ERROR", format!("{}: {}", log_prefix, error))
                .await;
        }
        let _ = reply.send(result.map_err(|error| error.to_string()));
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        self.shutdown_inner(false).await
    }

    async fn shutdown_inner(&mut self, removing: bool) -> Result<()> {
        self.retire_market_requests();
        self.state.request_shutdown();
        let paused = self.state.pause_for_shutdown().await;
        // Shutdown can interrupt initialization before durable orders have been
        // installed in memory. Restore them before canceling so racing fills
        // are recorded rather than discarded with the recovery metadata.
        let config = self.state.config.read().await.clone();
        if removing && !config.exchange.is_paper() {
            if let Err(error) = self.client.get_open_orders(&config.exchange.symbol).await {
                if matches!(
                    error.downcast_ref::<crate::exchange::client::ExchangeError>(),
                    Some(crate::exchange::client::ExchangeError::ApiError { code: -1121, .. })
                ) {
                    paused?;
                    anyhow::ensure!(
                        self.state.active_orders.read().await.is_empty(),
                        "无效交易对仍有内存挂单，请核实恢复数据"
                    );
                    let symbol = config.exchange.symbol.clone();
                    let clear = self
                        .state
                        .db
                        .run_blocking(move |db| {
                            if !db.load_managed_orders(&symbol)?.is_empty() {
                                return Ok(false);
                            }
                            for mode in
                                [TradingMode::Unknown, TradingMode::Paper, TradingMode::Live]
                            {
                                if !db.load_pair_intents(&symbol, mode)?.is_empty()
                                    || db.load_remainder_plan(&symbol, mode)?.is_some()
                                {
                                    return Ok(false);
                                }
                            }
                            Ok(true)
                        })
                        .await?;
                    anyhow::ensure!(clear, "无效交易对仍有待恢复订单或意图，请核实恢复数据");
                    if let Some(task) = self.pnl_reconcile_task.take() {
                        task.abort();
                        let _ = task.await;
                    }
                    return Ok(());
                }
                return Err(error.context("无法确认删除交易对的交易所挂单"));
            }
        }
        if !config.exchange.is_paper() {
            let restored = self.load_managed_orders(&config.exchange.symbol).await?;
            self.insert_restored_managed_orders(restored).await;
            // An interrupted initialization may have committed a fill before
            // its runtime statistics were updated. Reload before recording
            // additional cancellation-time executions.
            self.state.refresh_current_scope().await;
        }
        let canceled = self.cancel_all_orders().await;
        if let Some(task) = self.pnl_reconcile_task.take() {
            task.abort();
            let _ = task.await;
        }
        paused?;
        anyhow::ensure!(
            canceled,
            "停机撤单未完成，恢复元数据已保留；请核实交易所挂单"
        );
        Ok(())
    }

    async fn load_target_paper_state(
        &self,
        new_config: &crate::config::AppConfig,
        direction_changed: bool,
    ) -> Result<Option<(PositionInfo, AccountInfo)>> {
        let symbol = new_config.exchange.symbol.clone();
        let state = self
            .state
            .db
            .run_blocking(move |db| db.load_paper_state(&symbol))
            .await?;
        anyhow::ensure!(
            !direction_changed
                || state.as_ref().is_none_or(|(position, _)| {
                    position.size * new_config.grid.direction.sign() >= Decimal::ZERO
                }),
            "切换做多 / 做空方向前，请先平掉目标账户的持仓"
        );
        Ok(state)
    }

    async fn apply_config(&mut self, new_config: crate::config::AppConfig) -> Result<()> {
        new_config.validate()?;
        self.retire_market_requests();
        if let Some(symbol) = self.state.pair_scope() {
            anyhow::ensure!(
                symbol == new_config.exchange.symbol,
                "交易对不能直接重命名，请在页面中添加新交易对"
            );
        }
        let old_config = self.state.config.read().await.clone();
        let direction_changed = old_config.grid.direction != new_config.grid.direction;
        if direction_changed {
            self.ensure_flat_for_direction_change(new_config.grid.direction)
                .await?;
        }
        let strategy_changed =
            old_config.exchange != new_config.exchange || old_config.grid != new_config.grid;
        let mut resized_grid = old_config.grid.clone();
        resized_grid.buy_window = new_config.grid.buy_window;
        resized_grid.sell_window = new_config.grid.sell_window;
        let only_window_changed =
            old_config.exchange == new_config.exchange && resized_grid == new_config.grid;
        let market_changed = old_config.exchange.symbol != new_config.exchange.symbol;
        let mode_changed = old_config.exchange.trading_mode() != new_config.exchange.trading_mode();
        let client_changed = old_config.exchange.api_key != new_config.exchange.api_key
            || old_config.exchange.api_secret != new_config.exchange.api_secret
            || old_config.exchange.recv_window != new_config.exchange.recv_window;

        if !strategy_changed {
            let saved = new_config.clone();
            self.state.save_runtime_config(saved).await?;
            self.state
                .add_log("SUCCESS", "Notification configuration updated")
                .await;
            return Ok(());
        }
        let mut paper_state = if new_config.exchange.is_paper() && (market_changed || mode_changed)
        {
            self.load_target_paper_state(&new_config, direction_changed)
                .await?
        } else {
            None
        };

        let new_client = if client_changed {
            Arc::new(self.client.reconfig(&new_config.exchange))
        } else {
            Arc::clone(&self.client)
        };
        if !new_config.exchange.is_paper() {
            Self::validate_live_config_target(&new_client, &new_config, direction_changed).await?;
        }

        let fetch_rules = market_changed
            || mode_changed
            || (!new_config.exchange.is_paper() && !self.exchange_rules_loaded);
        let new_rules = if fetch_rules {
            Self::fetch_symbol_rules_with_client(&new_client, &new_config.exchange.symbol).await?
        } else {
            self.state.rules.read().await.clone()
        };
        new_rules.validate_grid_interval(new_config.grid.grid_interval)?;
        let new_price = if market_changed || mode_changed {
            let price = new_client
                .get_ticker_price(&new_config.exchange.symbol)
                .await?;
            if price <= Decimal::ZERO {
                return Err(anyhow!(
                    "Invalid market price for {}",
                    new_config.exchange.symbol
                ));
            }
            Some(price)
        } else {
            None
        };

        // Resizing uses ordinary window pruning so existing exits keep their
        // prices and queue priority. Other changes still cancel in the old scope.
        if !only_window_changed && !self.cancel_all_orders().await {
            return Err(anyhow!(
                "Could not cancel old managed orders; configuration unchanged"
            ));
        }
        // Cancellation may reveal a racing fill. Recheck the old account before
        // committing the new direction so an exit cannot become an opening order.
        if direction_changed {
            if let Err(error) = self
                .ensure_flat_for_direction_change(new_config.grid.direction)
                .await
            {
                self.pause_trading().await;
                return Err(error);
            }
        }
        if new_config.exchange.is_paper() && (market_changed || mode_changed) {
            paper_state = self
                .load_target_paper_state(&new_config, direction_changed)
                .await?;
        }
        // Snapshot readers must see the new config together with its matching runtime data.
        let transition_state = Arc::clone(&self.state);
        let scope_transition = transition_state.snapshot_scope_lock.write().await;
        let saved = new_config.clone();
        if let Err(error) = self.state.save_runtime_config(saved).await {
            self.pause_trading().await;
            return Err(error);
        }

        self.reset_runtime_scope_after_config_change(ConfigScopeReset {
            new_config: &new_config,
            new_client,
            new_rules,
            new_price,
            paper_state,
            market_changed,
            mode_changed,
            client_changed,
            fetch_rules,
        })
        .await;
        drop(scope_transition);

        let ready = if new_config.exchange.is_paper() {
            self.update_paper_valuation().await;
            !self
                .state
                .paper_restore_failed
                .load(std::sync::atomic::Ordering::SeqCst)
        } else {
            self.sync_live_orders().await && self.sync_account_and_position().await
        };
        // A dirty target scope is installed paused, but reconciliation can have
        // recovered its live orders. Apply the same cancellation as startup.
        let cancellation_complete = if (market_changed || mode_changed)
            && !new_config.exchange.is_paper()
            && *self.state.status.read().await == BotStatus::Paused
        {
            self.cancel_all_orders().await
        } else {
            true
        };
        if !cancellation_complete {
            self.set_global_reconciliation_ready(false).await;
            return Err(anyhow!(
                "Configuration saved, but paused target order cancellation failed; bot paused"
            ));
        }
        self.set_global_reconciliation_ready(ready).await;
        if !ready {
            self.pause_trading().await;
            return Err(anyhow!(
                "Configuration saved, but exchange reconciliation failed; bot paused"
            ));
        }
        if *self.state.status.read().await == BotStatus::Running {
            self.maintain_grid_window().await;
        }
        self.state
            .add_log(
                "SUCCESS",
                format!(
                    "Strategy configuration updated for {}",
                    new_config.exchange.symbol
                ),
            )
            .await;
        Ok(())
    }

    async fn validate_live_config_target(
        new_client: &BinanceFuturesClient,
        new_config: &crate::config::AppConfig,
        direction_changed: bool,
    ) -> Result<()> {
        new_client.sync_server_time().await?;
        let new_position = new_client
            .get_position(&new_config.exchange.symbol)
            .await?
            .ok_or_else(|| anyhow!("New symbol has no position information"))?;
        anyhow::ensure!(
            !direction_changed
                || new_position.position_amt * new_config.grid.direction.sign() >= Decimal::ZERO,
            "切换做多 / 做空方向前，请先平掉目标账户的持仓"
        );
        let account = new_client.get_account().await?;
        if account
            .margin_asset_for_symbol(&new_config.exchange.symbol)
            .is_none()
        {
            return Err(anyhow!("New symbol has no matching margin asset"));
        }
        Ok(())
    }

    async fn reset_runtime_scope_after_config_change(&mut self, reset: ConfigScopeReset<'_>) {
        let ConfigScopeReset {
            new_config,
            new_client,
            new_rules,
            new_price,
            paper_state,
            market_changed,
            mode_changed,
            client_changed,
            fetch_rules,
        } = reset;
        self.client = new_client;
        if market_changed || mode_changed || client_changed {
            if let Some(task) = self.pnl_reconcile_task.take() {
                task.abort();
            }
            self.pnl_order_ids.clear();
            self.pnl_clear_revision = 0;
            self.pnl_reconcile_offset = 0;
            self.pnl_retry_at = None;
            self.reconciliation_blocked = false;
            self.market_data_blocked = false;
            self.paper_window_cache = None;
        }
        if client_changed {
            self.state.position.write().await.update_time = None;
            self.state.account.write().await.update_time = chrono::DateTime::<Utc>::default();
        }
        if fetch_rules {
            *self.state.rules.write().await = new_rules;
            self.exchange_rules_loaded = true;
        }
        if market_changed || mode_changed {
            self.mark_retry.reset();
            self.ticker_retry.reset();
            let mut ticker = TickerInfo {
                symbol: new_config.exchange.symbol.clone(),
                ..Default::default()
            };
            if let Some(price) = new_price {
                ticker.last_price = price;
                ticker.update_time = Utc::now();
            }
            *self.state.ticker.write().await = ticker;
            *self.state.position.write().await = PositionInfo::default();
            self.last_account_sync = None;
            self.last_orders_sync = None;
            if new_config.exchange.is_paper() {
                if let Some((position, account)) = paper_state {
                    *self.state.position.write().await = position;
                    *self.state.account.write().await = account;
                } else {
                    *self.state.account.write().await =
                        AccountInfo::initial_paper(&new_config.exchange.symbol, Some(Utc::now()));
                }
            } else {
                *self.state.account.write().await = AccountInfo::default();
            }
            self.state
                .paper_restore_failed
                .store(false, std::sync::atomic::Ordering::SeqCst);
            if let Some(tx) = &self.market_stream_tx {
                let _ = tx.send(new_config.exchange.symbol.clone());
            }
            while let Ok(_) | Err(broadcast::error::TryRecvError::Lagged(_)) =
                self.ticker_rx.try_recv()
            {}
            self.state.refresh_current_scope().await;
        }
    }

    /// Periodic sync cycle
    async fn sync_cycle(&mut self, wait_for_market: bool) {
        let started = tracing::enabled!(tracing::Level::DEBUG).then(Instant::now);
        macro_rules! finish_sync {
            ($result:expr) => {
                if let Some(started) = started {
                    debug!(
                        result = $result,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "Periodic sync finished"
                    );
                }
            };
        }
        let pause_epoch = self.state.pause_epoch();
        let config = self.state.config.read().await.clone();
        let paper = config.exchange.is_paper();

        if wait_for_market {
            self.refresh_mark_price(&config.exchange.symbol).await;
        } else {
            self.start_mark_price_request(&config.exchange.symbol).await;
        }
        if let Some(started) = started {
            debug!(
                stage = "mark_price",
                cycle_elapsed_ms = started.elapsed().as_millis() as u64,
                "Periodic sync stage finished"
            );
        }
        if self.state.pause_epoch() != pause_epoch {
            finish_sync!("paused_after_mark_price");
            return;
        }
        if wait_for_market {
            self.refresh_stale_ticker(&config.exchange.symbol).await;
        } else {
            self.start_ticker_request(&config.exchange.symbol).await;
        }
        if let Some(started) = started {
            debug!(
                stage = "ticker",
                cycle_elapsed_ms = started.elapsed().as_millis() as u64,
                "Periodic sync stage finished"
            );
        }
        if self.state.pause_epoch() != pause_epoch {
            finish_sync!("paused_after_ticker");
            return;
        }
        // Cancellation remains available when the following reconciliation cannot
        // establish replenishment readiness, including the sync performed on resume.
        self.cancel_suspended_opening_orders(&config).await;
        if self.state.pause_epoch() != pause_epoch {
            finish_sync!("paused_after_opening_protection");
            return;
        }

        let account_ready = if !paper {
            let orders_ready = self
                .sync_live_orders_with_priority(ReconciliationPriority::Periodic)
                .await;
            if let Some(started) = started {
                debug!(
                    stage = "orders",
                    ready = orders_ready,
                    cycle_elapsed_ms = started.elapsed().as_millis() as u64,
                    "Periodic sync stage finished"
                );
            }
            if !orders_ready {
                self.last_orders_sync = None;
            }
            if self.state.pause_epoch() != pause_epoch {
                finish_sync!("paused_after_orders");
                return;
            }
            let account_ready = self.sync_account_and_position_inner(true).await;
            if let Some(started) = started {
                debug!(
                    stage = "account_position",
                    ready = account_ready,
                    cycle_elapsed_ms = started.elapsed().as_millis() as u64,
                    "Periodic sync stage finished"
                );
            }
            let ready = orders_ready && account_ready;
            self.set_global_reconciliation_ready(ready).await;
            ready
        } else {
            self.set_global_reconciliation_ready(true).await;
            true
        };

        if self.state.pause_epoch() != pause_epoch {
            finish_sync!("paused_after_reconciliation");
            return;
        }

        let openings_protected = self.cancel_suspended_opening_orders(&config).await;
        if self.pause_if_position_limit_reached().await {
            finish_sync!("position_limit");
            return;
        }
        self.trim_closing_orders_to_position().await;

        let maintain_window = *self.state.status.read().await == BotStatus::Running
            && account_ready
            && openings_protected;
        if maintain_window {
            self.maintain_grid_window().await;
        }
        if let Some(started) = started {
            debug!(
                result = if account_ready {
                    "complete"
                } else {
                    "reconciliation_failed"
                },
                window_maintained = maintain_window,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "Periodic sync finished"
            );
        }
    }

    // Compatibility recovery only: adopted exchange orders are reconciled, but
    // an absent legacy counter-order is retired instead of creating a new pair.
    pub(super) fn sync_timestamps_fresh(&self) -> bool {
        self.last_account_sync
            .is_some_and(|at| at.elapsed() < LIVE_SYNC_FRESHNESS)
            && self
                .last_orders_sync
                .is_some_and(|at| at.elapsed() < LIVE_SYNC_FRESHNESS)
    }

    /// Fetch position and account balances from Binance in Live mode
    async fn sync_account_and_position(&mut self) -> bool {
        self.sync_account_and_position_inner(false).await
    }

    async fn sync_position(&self, shared_position: bool) -> Option<Instant> {
        let symbol = self.state.config.read().await.exchange.symbol.clone();
        let position_result = if shared_position {
            self.position_cache.get(&self.client, &symbol).await
        } else {
            self.position_cache
                .refresh_critical(&self.client, &symbol)
                .await
        };
        if let Ok((Some(pos), received_at)) = position_result {
            let synced_at = chrono::Duration::from_std(received_at.elapsed())
                .ok()
                .map(|age| Utc::now() - age);
            *self.state.position.write().await = pos.into_position_info(synced_at);
            Some(received_at)
        } else {
            self.state.position.write().await.update_time = None;
            warn!("Could not sync Binance position for {}", symbol);
            self.set_global_reconciliation_ready(false).await;
            None
        }
    }

    async fn sync_account(&self, shared: bool, critical: bool) -> Option<Instant> {
        let symbol = self.state.config.read().await.exchange.symbol.clone();
        let account = if critical {
            self.account_cache.refresh_critical(&self.client).await
        } else if shared {
            self.account_cache.get(&self.client).await
        } else {
            self.account_cache.refresh(&self.client).await
        };
        match account {
            Ok(snapshot) => {
                if let Some(asset) = snapshot.account.margin_asset_for_symbol(&symbol) {
                    let mut state_acc = self.state.account.write().await;
                    state_acc.asset = asset.asset.clone();
                    state_acc.total_wallet_balance = asset.wallet_balance;
                    state_acc.available_balance = asset.available_balance;
                    state_acc.margin_balance = asset.margin_balance;
                    state_acc.unrealized_profit = asset.unrealized_profit;
                    state_acc.update_time = snapshot.received_at;
                    Some(snapshot.received_instant)
                } else {
                    warn!(
                        "No matching margin asset in Binance account response for {}",
                        symbol
                    );
                    self.state.account.write().await.update_time =
                        chrono::DateTime::<Utc>::default();
                    self.set_global_reconciliation_ready(false).await;
                    None
                }
            }
            Err(e) => {
                warn!("Could not sync Binance account balance: {}", e);
                self.state.account.write().await.update_time = chrono::DateTime::<Utc>::default();
                self.set_global_reconciliation_ready(false).await;
                None
            }
        }
    }

    pub(super) async fn invalidate_account_and_position_caches(&mut self) {
        self.account_cache.invalidate(&self.client).await;
        self.position_cache.invalidate(&self.client).await;
        self.last_account_sync = None;
    }

    pub(super) async fn block_reconciliation_and_invalidate_caches(&mut self) {
        self.last_orders_sync = None;
        self.report_reconciliation_state(true).await;
        self.invalidate_account_and_position_caches().await;
    }

    async fn sync_account_and_position_inner(&mut self, shared_position: bool) -> bool {
        let pause_epoch = self.state.pause_epoch();
        let position_instant = self.sync_position(shared_position).await;
        let position_ok = position_instant.is_some();
        if self.state.pause_epoch() != pause_epoch {
            return false;
        }
        let account_instant = self.sync_account(shared_position, !shared_position).await;
        let account_ok = account_instant.is_some();
        self.state.check_global_position_limit().await;
        if position_ok && account_ok {
            self.last_account_sync = account_instant
                .zip(position_instant)
                .map(|(account, position)| account.min(position));
            true
        } else {
            self.last_account_sync = None;
            false
        }
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
