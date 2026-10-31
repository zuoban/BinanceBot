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
use rust_decimal::{prelude::Signed, Decimal};
#[cfg(test)]
use sha2::{Digest, Sha256};
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
const MARKET_FALLBACK_AFTER_SECS: i64 = 8;

#[derive(Clone, Copy)]
enum ReconciliationPriority {
    Periodic,
    Critical,
}

/// Binance Futures allows at most 36 characters for newClientOrderId.
fn new_grid_client_order_id(side: OrderSide) -> String {
    let uuid = Uuid::new_v4().simple().to_string();
    let side_code = match side {
        OrderSide::Buy => "b",
        OrderSide::Sell => "s",
    };
    format!("gb_{}_{}", side_code, &uuid[..30])
}

#[cfg(test)]
fn paired_grid_client_order_id(parent_id: &str, side: OrderSide) -> String {
    let digest = Sha256::digest(format!("{}:{}", parent_id, side.as_str()).as_bytes());
    let side_code = if side == OrderSide::Buy { "b" } else { "s" };
    format!("gb_{}_{}", side_code, &hex::encode(digest)[..30])
}

#[cfg(test)]
fn new_pair_intent(
    parent: &GridOrder,
    side: OrderSide,
    price: Decimal,
    quantity: Decimal,
) -> GridOrder {
    GridOrder {
        client_order_id: paired_grid_client_order_id(&parent.client_order_id, side),
        order_id: None,
        symbol: parent.symbol.clone(),
        side,
        price,
        quantity,
        amount_usdc: price * quantity,
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: parent.grid_level + if side == OrderSide::Sell { 1 } else { -1 },
        paired_client_order_id: Some(parent.client_order_id.clone()),
        is_take_profit: false,
        purpose: OrderPurpose::Grid,
        merge_sources: Vec::new(),
    }
}

enum PairPlacementDecision {
    Wait,
    Skip(&'static str),
}

#[path = "remainder.rs"]
mod remainder;

const GRID_ORDER_PREFIX: &str = "gb_";

fn is_grid_order(client_order_id: &str) -> bool {
    client_order_id.starts_with(GRID_ORDER_PREFIX)
}

/// All idle orders follow the window. Partial executions finish on their existing order.
fn is_window_order(order: &GridOrder) -> bool {
    is_grid_order(&order.client_order_id) && order.status == OrderStatus::New
}

fn opening_exposure_usdc(
    direction: GridDirection,
    position_size: Decimal,
    mark_price: Decimal,
    orders: &[GridOrder],
) -> Decimal {
    direction.position_quantity(position_size) * mark_price
        + orders
            .iter()
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
fn update_paper_account(position: &mut PositionInfo, account: &mut AccountInfo, price: Decimal) {
    if price > Decimal::ZERO {
        position.mark_price = price;
    }
    position.unrealized_pnl = if position.size.is_zero() || position.mark_price <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        (position.mark_price - position.entry_price) * position.size
    };
    let margin_price = if position.mark_price > Decimal::ZERO {
        position.mark_price
    } else {
        position.entry_price
    };
    let position_margin =
        position.size.abs() * margin_price / Decimal::from(position.leverage.max(1));
    account.unrealized_profit = position.unrealized_pnl;
    account.margin_balance = account.total_wallet_balance + position.unrealized_pnl;
    account.available_balance = account.margin_balance - position_margin;
    account.update_time = Utc::now();
}

fn position_valuation_price(position: &PositionInfo, ticker: &TickerInfo) -> Decimal {
    if ticker.mark_price > Decimal::ZERO {
        ticker.mark_price
    } else if position.mark_price > Decimal::ZERO {
        position.mark_price
    } else {
        ticker.last_price
    }
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
fn grid_level_occupied(
    orders: &[GridOrder],
    side: OrderSide,
    price: Decimal,
    interval: Decimal,
    tick_size: Decimal,
) -> bool {
    let occupied: Vec<_> = orders
        .iter()
        .filter(|order| order.side == side)
        .map(|order| order.price)
        .collect();
    has_nearby_grid_order(&occupied, price, interval, tick_size)
}

fn reserved_closing_quantity(direction: GridDirection, orders: &[GridOrder]) -> Decimal {
    orders
        .iter()
        .filter(|o| o.side == direction.closing_side())
        .map(|o| o.quantity)
        .sum()
}

fn closing_quantity_available(
    direction: GridDirection,
    position_size: Decimal,
    orders: &[GridOrder],
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

fn aggregate_execution_pnl(
    symbol: &str,
    fills: &[BinanceUserTrade],
) -> Result<(Decimal, Decimal, bool)> {
    let quote = ["USDC", "USDT", "FDUSD", "BUSD"]
        .into_iter()
        .find(|asset| symbol.ends_with(asset))
        .ok_or_else(|| anyhow!("Unsupported quote asset for {}", symbol))?;
    if fills.is_empty() {
        return Err(anyhow!("No exchange fills found for {}", symbol));
    }
    if fills
        .iter()
        .any(|fill| !fill.commission_asset.eq_ignore_ascii_case(quote))
    {
        return Err(anyhow!(
            "Execution commission is not denominated in {}",
            quote
        ));
    }
    Ok((
        fills.iter().map(|fill| fill.realized_pnl).sum(),
        fills.iter().map(|fill| fill.commission).sum(),
        fills.iter().all(|fill| fill.maker),
    ))
}

fn exchange_fill_time(order: &BinanceOrderResponse) -> chrono::DateTime<Utc> {
    order
        .update_time
        .filter(|time| *time > 0)
        .and_then(chrono::DateTime::from_timestamp_millis)
        .unwrap_or_else(Utc::now)
}

#[derive(Default)]
struct MarketRetry {
    failures: u32,
    retry_at: Option<Instant>,
}

impl MarketRetry {
    fn ready(&self) -> bool {
        self.retry_at.is_none_or(|at| Instant::now() >= at)
    }

    fn failed(&mut self) {
        let seconds = (2_u64 << self.failures.min(4)).min(30);
        let jitter_ms = rand::random::<u64>() % 501;
        self.retry_at = Some(Instant::now() + Duration::from_millis(seconds * 1000 + jitter_ms));
        self.failures = self.failures.saturating_add(1);
    }

    fn reset(&mut self) {
        *self = Self::default();
    }
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
    pnl_reconcile_task: Option<JoinHandle<()>>,
    last_account_sync: Option<Instant>,
    last_orders_sync: Option<Instant>,
    mark_retry: MarketRetry,
    ticker_retry: MarketRetry,
    reconciliation_blocked: bool,
    exchange_rules_loaded: bool,
    market_data_blocked: bool,
    last_remainder_change: Option<Instant>,
    market_stream_tx: Option<watch::Sender<(String, bool)>>,
}

impl Drop for GridTradingEngine {
    fn drop(&mut self) {
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
        Self {
            state,
            client,
            account_cache: Arc::new(AccountCache::default()),
            position_cache: Arc::new(PositionCache::default()),
            market_cache: Arc::new(MarketCache::default()),
            action_rx,
            ticker_rx,
            pnl_reconcile_offset: 0,
            pnl_reconcile_task: None,
            last_account_sync: None,
            last_orders_sync: None,
            mark_retry: MarketRetry::default(),
            ticker_retry: MarketRetry::default(),
            reconciliation_blocked: false,
            exchange_rules_loaded: false,
            market_data_blocked: false,
            last_remainder_change: None,
            market_stream_tx: None,
        }
    }

    pub fn set_market_stream_tx(&mut self, tx: watch::Sender<(String, bool)>) {
        self.market_stream_tx = Some(tx);
    }

    pub(crate) fn set_market_cache(&mut self, cache: Arc<MarketCache>) {
        self.market_cache = cache;
    }

    pub(crate) fn set_account_cache(&mut self, cache: Arc<AccountCache>) {
        self.account_cache = cache;
    }

    pub(crate) fn set_position_cache(&mut self, cache: Arc<PositionCache>) {
        self.position_cache = cache;
    }

    async fn pause_trading(&self) {
        self.pause_trading_with_reason("策略运行异常，已暂停，请查看系统运行日志")
            .await;
    }

    async fn pause_trading_with_reason(&self, reason: &str) {
        if let Err(error) = self.state.pause_trading_with_reason(reason).await {
            error!("Could not persist paused bot status: {}", error);
        }
    }

    /// Check filled position value independently of opening-order reservations.
    async fn pause_if_position_limit_reached(&mut self) -> bool {
        let global = self
            .state
            .global_position_limit
            .get()
            .and_then(std::sync::Weak::upgrade);
        if let Some(global) = global {
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
        let config = self.state.config.read().await.clone();
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
        let position = self.state.position.read().await.clone();
        let ticker = self.state.ticker.read().await.clone();
        let valuation_price = position_valuation_price(&position, &ticker);
        let position_value = position.size.abs() * valuation_price;
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

    async fn pause_for_position_limit(&mut self, reason: String) {
        // Pause before canceling so cancellation cannot restore Running or replenish orders.
        self.pause_trading_with_reason(&format!("{}，需手动恢复", reason))
            .await;
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
        self.pause_trading_with_reason("持仓方向与策略方向不一致，请先平仓后手动恢复")
            .await;
        self.state
            .add_log("ERROR", "持仓方向与策略方向不一致，策略已暂停")
            .await;
        self.cancel_all_orders().await;
        false
    }

    async fn ensure_flat_for_direction_change(&mut self) -> Result<()> {
        if !self.state.config.read().await.exchange.dry_run {
            anyhow::ensure!(
                self.sync_account_and_position().await,
                "无法确认当前持仓，交易方向未切换"
            );
        }
        anyhow::ensure!(
            self.state.position.read().await.size.is_zero(),
            "切换做多 / 做空方向前，请先平掉当前持仓"
        );
        Ok(())
    }

    async fn save_managed_order(&self, order: &GridOrder) -> Result<()> {
        let order = order.clone();
        self.state
            .db
            .run_blocking(move |db| db.save_managed_order(&order))
            .await
    }

    async fn delete_managed_order(&self, client_order_id: &str) -> Result<()> {
        let client_order_id = client_order_id.to_string();
        self.state
            .db
            .run_blocking(move |db| db.delete_managed_order(&client_order_id))
            .await
    }

    async fn window_prices(
        &self,
        config: &crate::config::AppConfig,
        rules: &SymbolRules,
        market: Decimal,
    ) -> Result<(Vec<Decimal>, Vec<Decimal>)> {
        let symbol = config.exchange.symbol.clone();
        let mode = TradingMode::from_exchange(config.exchange.dry_run, config.exchange.is_testnet);
        let last_fill = self
            .state
            .db
            .run_blocking(move |db| db.last_fill_for(&symbol, mode))
            .await?;
        let interval = config.grid.grid_interval;
        // A recent fill only reserves its immediate neighborhood. It must never
        // pin either window to an old price while the mark price moves away.
        Ok(rules.grid_window_prices(
            market,
            interval,
            (config.grid.buy_window, config.grid.sell_window),
            (config.grid.min_price, config.grid.max_price),
            |_, price| {
                last_fill.is_none_or(|(_, fill_price)| (price - fill_price).abs() >= interval)
            },
        ))
    }

    async fn set_global_reconciliation_ready(&self, ready: bool) {
        self.state
            .global_position_ready
            .store(ready, std::sync::atomic::Ordering::SeqCst);
        if let Some(global) = self
            .state
            .global_position_limit
            .get()
            .and_then(std::sync::Weak::upgrade)
        {
            global.check().await;
        }
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

    async fn market_data_ready(&mut self) -> bool {
        let ready = self.state.ticker.read().await.prices_are_fresh(Utc::now());
        let blocked = !ready;
        if self.market_data_blocked != blocked {
            self.market_data_blocked = blocked;
            let (level, message) = if blocked {
                ("WARN", "行情缺失或超过 15 秒未更新，暂停新增订单和窗口调整")
            } else {
                ("INFO", "行情已恢复，账户及订单对账有效后继续维护窗口")
            };
            self.state.add_log(level, message).await;
        }
        ready
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
                    if config.exchange.dry_run {
                        "DRY_RUN (Paper Trading)"
                    } else if config.exchange.is_testnet {
                        "TESTNET"
                    } else {
                        "LIVE"
                    }
                ),
            )
            .await;

        // Restore managed metadata before any exchange request can fail. A
        // subsequent initial pause must reconcile these orders before clearing them.
        if !config.exchange.dry_run {
            let restore_symbol = symbol.clone();
            let restored = self
                .state
                .db
                .run_blocking(move |db| db.load_managed_orders(&restore_symbol))
                .await?;
            let mut active = self.state.active_orders.write().await;
            for order in restored
                .into_iter()
                .filter(|order| is_grid_order(&order.client_order_id))
            {
                active.insert(order.client_order_id.clone(), order);
            }
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
                if !config.exchange.dry_run {
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
            Ok(price) => {
                let mut ticker = self.state.ticker.write().await;
                ticker.symbol = symbol.clone();
                ticker.last_price = price;
                ticker.update_time = ticker_requested_at;
                info!("Initial market price for {}: {}", symbol, price);
            }
            Err(e) => {
                warn!("Failed to fetch initial market price: {}", e);
                if !config.exchange.dry_run {
                    return Err(anyhow!("Could not load initial market price: {}", e));
                }
            }
        }

        let mark_requested_at = Utc::now();
        match self.client.get_mark_price(&symbol).await {
            Ok(price) => {
                let mut ticker = self.state.ticker.write().await;
                ticker.mark_price = price;
                ticker.mark_update_time = mark_requested_at;
            }
            Err(e) => warn!("Failed to fetch initial mark price: {}", e),
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
        if !config.exchange.dry_run
            && (!self.sync_account_and_position().await || !self.sync_live_orders().await)
        {
            return Err(anyhow!(
                "Could not reconcile Binance account and managed orders at startup"
            ));
        }

        Ok(())
    }

    async fn load_exchange_rules(&mut self, symbol: &str) -> Result<()> {
        let info = self.client.get_exchange_info(Some(symbol)).await?;
        let symbol_info = info
            .symbols
            .iter()
            .find(|item| item.symbol.eq_ignore_ascii_case(symbol))
            .ok_or_else(|| anyhow!("Missing Binance exchange rules for {}", symbol))?;
        anyhow::ensure!(
            symbol_info.status == "TRADING",
            "Symbol {} is not trading",
            symbol
        );
        let rules = SymbolRules::from_exchange_info(&info, symbol)
            .ok_or_else(|| anyhow!("Missing Binance exchange rules for {}", symbol))?;
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
        let live = !self.state.config.read().await.exchange.dry_run;
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
        let period = Duration::from_secs(sync_secs);
        let offset =
            Duration::from_millis(rand::random::<u64>() % sync_secs.saturating_mul(1000).max(1));
        let mut sync_timer = interval_at(tokio::time::Instant::now() + offset, period);
        sync_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut pnl_timer = interval(Duration::from_secs(60));

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
                                    let period = Duration::from_secs(sync_secs);
                                    let offset = Duration::from_millis(rand::random::<u64>() % sync_secs.saturating_mul(1000).max(1));
                                    sync_timer = interval_at(tokio::time::Instant::now() + offset, period);
                                    sync_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
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
                    self.sync_cycle().await;
                }

                _ = pnl_timer.tick() => {
                    self.reconcile_unverified_trades().await;
                }
            }
        }
    }

    async fn handle_control_action(&mut self, action: BotControlAction) {
        if self.state.is_shutting_down() && !matches!(action, BotControlAction::Shutdown { .. }) {
            if let BotControlAction::UpdateConfig { reply, .. }
            | BotControlAction::UpdateGlobalConfig { reply, .. } = action
            {
                let _ = reply.send(Err("程序正在退出，配置未应用".into()));
            }
            return;
        }
        match action {
            BotControlAction::Shutdown { reply } => {
                let result = self.shutdown().await.map_err(|error| error.to_string());
                let _ = reply.send(result);
            }
            BotControlAction::Pause => {
                info!("Bot paused by user command");
                self.pause_trading_with_reason("手动暂停策略").await;
                if self.cancel_all_orders().await {
                    self.state
                        .add_log("WARN", "Trading bot paused and managed orders canceled")
                        .await;
                } else {
                    self.state.add_log("ERROR", "Trading bot paused, but some managed orders may still be open on Binance").await;
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
            BotControlAction::Resume => {
                info!("Bot resumed by user command");
                let config = self.state.config.read().await.clone();
                if !self.exchange_rules_loaded {
                    if let Err(error) = self.load_exchange_rules(&config.exchange.symbol).await {
                        if !config.exchange.dry_run {
                            self.state
                                .add_log("ERROR", format!("Resume failed: could not load Binance exchange rules for {}: {}", config.exchange.symbol, error))
                                .await;
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
                    self.state
                        .add_log("ERROR", format!("Resume failed: {}", error))
                        .await;
                    return;
                }
                if let Err(error) = self.state.resume_trading().await {
                    error!("Could not persist resumed bot status: {}", error);
                    self.state
                        .add_log("ERROR", format!("Resume failed: {}", error))
                        .await;
                    return;
                }
                self.state
                    .add_log("INFO", "Trading bot resumed by user")
                    .await;
                self.sync_cycle().await;
            }
            BotControlAction::CancelAll => {
                info!("Cancel all orders requested");
                self.pause_trading_with_reason("手动撤销全部策略挂单并暂停策略")
                    .await;
                if self.cancel_all_orders().await {
                    self.state
                        .add_log("WARN", "Managed grid orders canceled; bot paused")
                        .await;
                } else {
                    self.state
                        .add_log("ERROR", "Cancellation incomplete; bot paused")
                        .await;
                }
            }
            BotControlAction::Rebalance => {
                if *self.state.status.read().await == BotStatus::Running {
                    info!("Grid rebalance requested");
                    self.state
                        .add_log("INFO", "Manual grid rebalance triggered")
                        .await;
                    self.rebalance_grid().await;
                }
            }
            BotControlAction::UpdateConfig { config, reply } => {
                let result = self.apply_config(*config).await;
                if let Err(error) = &result {
                    self.state
                        .add_log("ERROR", format!("Configuration update rejected: {}", error))
                        .await;
                }
                let _ = reply.send(result.map_err(|error| error.to_string()));
            }
            BotControlAction::UpdateGlobalConfig { config, reply } => {
                let mut updated = self.state.config.read().await.clone();
                config.apply_to(&mut updated);
                let result = self.apply_config(updated).await;
                if let Err(error) = &result {
                    self.state
                        .add_log(
                            "ERROR",
                            format!("Global configuration update rejected: {}", error),
                        )
                        .await;
                }
                let _ = reply.send(result.map_err(|error| error.to_string()));
            }
        }
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        self.state.request_shutdown();
        let paused = self
            .state
            .pause_trading_with_reason("程序退出，暂停策略并撤销机器人挂单")
            .await;
        // Shutdown can interrupt initialization before durable orders have been
        // installed in memory. Restore them before canceling so racing fills
        // are recorded rather than discarded with the recovery metadata.
        let config = self.state.config.read().await.clone();
        if !config.exchange.dry_run {
            let restored = self
                .state
                .db
                .run_blocking(move |db| db.load_managed_orders(&config.exchange.symbol))
                .await?;
            let mut active = self.state.active_orders.write().await;
            for order in restored
                .into_iter()
                .filter(|order| is_grid_order(&order.client_order_id))
            {
                active.entry(order.client_order_id.clone()).or_insert(order);
            }
            drop(active);
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

    async fn apply_config(&mut self, new_config: crate::config::AppConfig) -> Result<()> {
        new_config.validate()?;
        if let Some(symbol) = self.state.pair_scope() {
            anyhow::ensure!(
                symbol == new_config.exchange.symbol,
                "交易对不能直接重命名，请在页面中添加新交易对"
            );
        }
        let old_config = self.state.config.read().await.clone();
        let direction_changed = old_config.grid.direction != new_config.grid.direction;
        if direction_changed {
            self.ensure_flat_for_direction_change().await?;
        }
        let strategy_changed =
            old_config.exchange != new_config.exchange || old_config.grid != new_config.grid;
        let mut resized_grid = old_config.grid.clone();
        resized_grid.buy_window = new_config.grid.buy_window;
        resized_grid.sell_window = new_config.grid.sell_window;
        let only_window_changed =
            old_config.exchange == new_config.exchange && resized_grid == new_config.grid;
        let market_changed = old_config.exchange.symbol != new_config.exchange.symbol
            || old_config.exchange.is_testnet != new_config.exchange.is_testnet;
        let mode_changed = old_config.exchange.dry_run != new_config.exchange.dry_run;
        let client_changed = old_config.exchange.api_key != new_config.exchange.api_key
            || old_config.exchange.api_secret != new_config.exchange.api_secret
            || old_config.exchange.is_testnet != new_config.exchange.is_testnet
            || old_config.exchange.recv_window != new_config.exchange.recv_window;

        if !strategy_changed {
            let saved = new_config.clone();
            self.state.save_runtime_config(saved).await?;
            self.state
                .add_log("SUCCESS", "Notification configuration updated")
                .await;
            return Ok(());
        }

        let new_client = if client_changed {
            Arc::new(BinanceFuturesClient::new_with_limiter(
                &new_config.exchange,
                self.client.limiter(),
            ))
        } else {
            Arc::clone(&self.client)
        };
        if !new_config.exchange.dry_run {
            new_client.sync_server_time().await?;
            let new_position = new_client
                .get_position(&new_config.exchange.symbol)
                .await?
                .ok_or_else(|| anyhow!("New symbol has no position information"))?;
            anyhow::ensure!(
                !direction_changed || new_position.position_amt.is_zero(),
                "切换做多 / 做空方向前，请先平掉目标账户的持仓"
            );
            let account = new_client.get_account().await?;
            if account
                .margin_asset_for_symbol(&new_config.exchange.symbol)
                .is_none()
            {
                return Err(anyhow!("New symbol has no matching margin asset"));
            }
        }

        let fetch_rules = market_changed
            || mode_changed
            || (!new_config.exchange.dry_run && !self.exchange_rules_loaded);
        let new_rules = if fetch_rules {
            let info = new_client
                .get_exchange_info(Some(&new_config.exchange.symbol))
                .await?;
            let symbol_info = info
                .symbols
                .iter()
                .find(|item| {
                    item.symbol
                        .eq_ignore_ascii_case(&new_config.exchange.symbol)
                })
                .ok_or_else(|| anyhow!("Unknown Binance symbol {}", new_config.exchange.symbol))?;
            if symbol_info.status != "TRADING" {
                return Err(anyhow!(
                    "Symbol {} is not trading",
                    new_config.exchange.symbol
                ));
            }
            SymbolRules::from_exchange_info(&info, &new_config.exchange.symbol).ok_or_else(
                || anyhow!("Missing exchange rules for {}", new_config.exchange.symbol),
            )?
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
            if let Err(error) = self.ensure_flat_for_direction_change().await {
                self.pause_trading_with_reason("撤单后未确认空仓，交易方向未切换")
                    .await;
                return Err(error);
            }
        }
        // Snapshot readers must see the new config together with its matching runtime data.
        let transition_state = Arc::clone(&self.state);
        let scope_transition = transition_state.snapshot_scope_lock.write().await;
        let saved = new_config.clone();
        if let Err(error) = self.state.save_runtime_config(saved).await {
            self.pause_trading().await;
            return Err(error);
        }

        self.client = new_client;
        if client_changed {
            self.state.position.write().await.update_time = None;
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
            if new_config.exchange.dry_run {
                *self.state.account.write().await = AccountInfo {
                    asset: quote_asset(&new_config.exchange.symbol)
                        .unwrap_or(&new_config.exchange.symbol)
                        .to_string(),
                    total_wallet_balance: rust_decimal_macros::dec!(10000),
                    available_balance: rust_decimal_macros::dec!(10000),
                    margin_balance: rust_decimal_macros::dec!(10000),
                    unrealized_profit: Decimal::ZERO,
                    update_time: Utc::now(),
                };
            } else {
                *self.state.account.write().await = AccountInfo::default();
            }
            if let Some(tx) = &self.market_stream_tx {
                let _ = tx.send((
                    new_config.exchange.symbol.clone(),
                    new_config.exchange.is_testnet,
                ));
            }
            while let Ok(_) | Err(broadcast::error::TryRecvError::Lagged(_)) =
                self.ticker_rx.try_recv()
            {}
            self.state.refresh_current_scope().await;
        }
        drop(scope_transition);

        let ready = if new_config.exchange.dry_run {
            true
        } else {
            self.sync_live_orders().await && self.sync_account_and_position().await
        };
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

    async fn handle_ticker_update(&mut self, ticker_update: TickerInfo) {
        if ticker_update.symbol != self.state.config.read().await.exchange.symbol {
            return;
        }

        let (last_updated, mark_updated, valuation_price) = {
            let mut ticker = self.state.ticker.write().await;
            let now = Utc::now();
            let fresh = |time| {
                let age = now.signed_duration_since(time);
                age >= chrono::Duration::zero() && age < chrono::Duration::seconds(15)
            };
            let last_updated = ticker_update.last_price > Decimal::ZERO
                && fresh(ticker_update.update_time)
                && ticker_update.update_time >= ticker.update_time;
            if last_updated {
                ticker.last_price = ticker_update.last_price;
                ticker.high_24h = ticker_update.high_24h;
                ticker.low_24h = ticker_update.low_24h;
                ticker.change_24h = ticker_update.change_24h;
                ticker.change_percent_24h = ticker_update.change_percent_24h;
                ticker.volume_24h = ticker_update.volume_24h;
                ticker.update_time = ticker_update.update_time;
            }
            let mark_updated = ticker_update.mark_price > Decimal::ZERO
                && fresh(ticker_update.mark_update_time)
                && ticker_update.mark_update_time >= ticker.mark_update_time;
            if mark_updated {
                ticker.mark_price = ticker_update.mark_price;
                ticker.mark_update_time = ticker_update.mark_update_time;
            }
            ticker.symbol = ticker_update.symbol.clone();
            (last_updated, mark_updated, ticker.grid_reference_price())
        };
        // Rejected market events cannot change risk inputs or trigger paper fills.
        if !last_updated && !mark_updated {
            return;
        }
        if last_updated {
            self.ticker_retry.reset();
        }
        if mark_updated {
            self.mark_retry.reset();
        }

        // Update unrealized PnL for current position
        let is_dry_run = self.state.config.read().await.exchange.dry_run;
        if is_dry_run {
            let mut pos = self.state.position.write().await;
            let mut account = self.state.account.write().await;
            update_paper_account(&mut pos, &mut account, valuation_price);
        }

        if self.pause_if_position_limit_reached().await {
            return;
        }
        let is_running = *self.state.status.read().await == BotStatus::Running;

        // In Paper Trading / Dry Run mode, match simulated orders against live market price
        let prices_fresh = self.state.ticker.read().await.prices_are_fresh(Utc::now());
        if is_running && is_dry_run && prices_fresh && last_updated {
            self.simulate_order_fills(ticker_update.last_price).await;
        }
    }

    /// Check simulated fills in Dry Run mode
    async fn simulate_order_fills(&mut self, current_price: Decimal) {
        if !self.position_direction_ready().await {
            return;
        }
        let mut filled_orders = Vec::new();

        {
            let orders = self.state.active_orders.read().await;
            for order in orders.values() {
                match order.side {
                    OrderSide::Buy => {
                        // Buy order fills when market price drops to or below order price
                        if current_price <= order.price {
                            filled_orders.push(order.clone());
                        }
                    }
                    OrderSide::Sell => {
                        // Sell order fills when market price rises to or above order price
                        if current_price >= order.price {
                            filled_orders.push(order.clone());
                        }
                    }
                }
            }
        }

        filled_orders.sort_by(|a, b| {
            a.side
                .as_str()
                .cmp(b.side.as_str())
                .then_with(|| {
                    if a.side == OrderSide::Buy {
                        b.price.cmp(&a.price)
                    } else {
                        a.price.cmp(&b.price)
                    }
                })
                .then_with(|| a.client_order_id.cmp(&b.client_order_id))
        });
        for mut order in filled_orders {
            if *self.state.status.read().await != BotStatus::Running {
                break;
            }
            let direction = self.state.config.read().await.grid.direction;
            if order.side == direction.closing_side() {
                order.quantity = order
                    .quantity
                    .min(direction.position_quantity(self.state.position.read().await.size));
                if order.quantity <= Decimal::ZERO {
                    self.state
                        .active_orders
                        .write()
                        .await
                        .remove(&order.client_order_id);
                    continue;
                }
                order.amount_usdc = order.price * order.quantity;
            }
            self.on_order_filled(&mut order).await;
            if self.pause_if_position_limit_reached().await {
                break;
            }
        }

        if !self.state.active_orders.read().await.is_empty() {
            self.maintain_grid_window().await;
        }
    }

    /// Periodic sync cycle
    async fn sync_cycle(&mut self) {
        let pause_epoch = self.state.pause_epoch();
        let config = self.state.config.read().await.clone();
        let is_dry_run = config.exchange.dry_run;

        self.refresh_mark_price(&config.exchange.symbol, is_dry_run)
            .await;
        if self.state.pause_epoch() != pause_epoch {
            return;
        }
        self.refresh_stale_ticker(&config.exchange.symbol).await;
        if self.state.pause_epoch() != pause_epoch {
            return;
        }

        let account_ready = if !is_dry_run {
            let orders_ready = self
                .sync_live_orders_with_priority(ReconciliationPriority::Periodic)
                .await;
            if !orders_ready {
                self.last_orders_sync = None;
            }
            if self.state.pause_epoch() != pause_epoch {
                return;
            }
            let account_ready = self.sync_account_and_position_inner(true).await;
            let ready = orders_ready && account_ready;
            self.set_global_reconciliation_ready(ready).await;
            ready
        } else {
            self.set_global_reconciliation_ready(true).await;
            true
        };

        if self.state.pause_epoch() != pause_epoch {
            return;
        }

        if self.pause_if_position_limit_reached().await {
            return;
        }
        self.trim_closing_orders_to_position().await;

        if *self.state.status.read().await == BotStatus::Running && account_ready {
            self.maintain_grid_window().await;
        }
    }

    async fn refresh_mark_price(&mut self, symbol: &str, is_dry_run: bool) {
        let previous = self.state.ticker.read().await.clone();
        let age = Utc::now().signed_duration_since(previous.mark_update_time);
        if previous.symbol == symbol
            && previous.mark_price > Decimal::ZERO
            && age >= chrono::Duration::zero()
            && age < chrono::Duration::seconds(MARKET_FALLBACK_AFTER_SECS)
        {
            self.mark_retry.reset();
            return;
        }
        if !self.mark_retry.ready() {
            return;
        }
        match self.market_cache.mark_price(&self.client, symbol).await {
            Ok((price, requested_at)) if price > Decimal::ZERO => {
                self.mark_retry.reset();
                let mut ticker = self.state.ticker.write().await;
                if ticker.symbol != previous.symbol
                    || ticker.mark_update_time != previous.mark_update_time
                    || ticker.mark_price != previous.mark_price
                    || (ticker.mark_price > Decimal::ZERO && requested_at < ticker.mark_update_time)
                {
                    return;
                }
                ticker.mark_price = price;
                ticker.mark_update_time = requested_at;
                drop(ticker);

                if is_dry_run {
                    let mut pos = self.state.position.write().await;
                    let mut account = self.state.account.write().await;
                    update_paper_account(&mut pos, &mut account, price);
                }
            }
            Ok(_) => {
                self.mark_retry.failed();
                warn!("Binance returned a zero mark price for {}", symbol);
            }
            Err(e) => {
                self.mark_retry.failed();
                debug!("Could not refresh mark price for {}: {}", symbol, e);
            }
        }
    }

    async fn refresh_stale_ticker(&mut self, symbol: &str) {
        let ticker = self.state.ticker.read().await.clone();
        let age = Utc::now() - ticker.update_time;
        if ticker.symbol == symbol
            && age >= chrono::Duration::zero()
            && age < chrono::Duration::seconds(MARKET_FALLBACK_AFTER_SECS)
        {
            self.ticker_retry.reset();
            return;
        }
        if !self.ticker_retry.ready() {
            return;
        }

        let mut update = ticker;
        let mut requested_at = Utc::now();
        update.symbol = symbol.to_string();
        match self.client.get_24hr_ticker_for_fallback(symbol).await {
            Ok(stats) => {
                update.last_price = stats.last_price;
                update.high_24h = stats.high_price;
                update.low_24h = stats.low_price;
                update.change_24h = stats.price_change;
                update.change_percent_24h = stats.price_change_percent;
                update.volume_24h = stats.volume;
            }
            Err(e) => {
                debug!("Could not refresh 24h ticker for {}: {}", symbol, e);
                requested_at = Utc::now();
                match self.client.get_ticker_price_for_fallback(symbol).await {
                    Ok(price) => update.last_price = price,
                    Err(e) => {
                        self.ticker_retry.failed();
                        warn!("Could not refresh stale market price for {}: {}", symbol, e);
                        return;
                    }
                }
            }
        }
        if update.last_price <= Decimal::ZERO {
            self.ticker_retry.failed();
            return;
        }
        self.ticker_retry.reset();
        update.mark_price = Decimal::ZERO;
        update.update_time = requested_at;
        self.handle_ticker_update(update).await;
    }

    /// Reconcile open orders from Binance in Live mode
    async fn sync_live_orders(&mut self) -> bool {
        self.sync_live_orders_with_priority(ReconciliationPriority::Critical)
            .await
    }

    async fn sync_live_orders_with_priority(&mut self, priority: ReconciliationPriority) -> bool {
        let pause_epoch = self.state.pause_epoch();
        let config = self.state.config.read().await.clone();
        let symbol = config.exchange.symbol.clone();
        let mode = TradingMode::from_exchange(config.exchange.dry_run, config.exchange.is_testnet);
        let intent_symbol = symbol.clone();
        let pair_intents = match self
            .state
            .db
            .run_blocking(move |db| db.load_pair_intents(&intent_symbol, mode))
            .await
        {
            Ok(intents) => intents
                .into_iter()
                .map(|intent| (intent.client_order_id.clone(), intent))
                .collect::<HashMap<_, _>>(),
            Err(e) => {
                error!("Could not load pending pair intents: {}", e);
                self.report_reconciliation_state(true).await;
                return false;
            }
        };
        let mut remainder_plan = match self.load_remainder_plan().await {
            Ok(plan) => plan,
            Err(e) => {
                error!("Could not load remainder recovery metadata: {}", e);
                self.last_orders_sync = None;
                return false;
            }
        };
        let open_orders = match priority {
            ReconciliationPriority::Periodic => {
                self.client.get_open_orders_for_periodic_sync(&symbol).await
            }
            ReconciliationPriority::Critical => self.client.get_open_orders(&symbol).await,
        };
        match open_orders {
            Ok(live_orders) => {
                self.last_orders_sync = None;
                if self.state.pause_epoch() != pause_epoch {
                    return false;
                }
                let live_ids: HashMap<String, BinanceOrderResponse> = live_orders
                    .into_iter()
                    .filter(|order| is_grid_order(&order.client_order_id))
                    .map(|o| (o.client_order_id.clone(), o))
                    .collect();

                let mut filled_or_closed = Vec::new();
                let mut persistence_ok = true;
                let mut orders_to_persist = Vec::new();

                {
                    let mut active = self.state.active_orders.write().await;
                    for (client_id, live_order) in &live_ids {
                        if let Some(plan) = &remainder_plan {
                            if let Some(metadata) = plan
                                .sources
                                .iter()
                                .chain(std::iter::once(&plan.target))
                                .find(|order| &order.client_order_id == client_id)
                            {
                                active.insert(client_id.clone(), metadata.clone());
                            }
                        }
                        if let Some(intent) = pair_intents.get(client_id) {
                            active.insert(client_id.clone(), intent.clone());
                        }
                        if let Some(order) = active.get_mut(client_id) {
                            order.order_id = Some(live_order.order_id);
                            order.quantity =
                                (live_order.orig_qty - live_order.executed_qty).max(Decimal::ZERO);
                            order.amount_usdc = order.price * order.quantity;
                            order.status = if live_order.executed_qty > Decimal::ZERO {
                                OrderStatus::PartiallyFilled
                            } else {
                                OrderStatus::New
                            };
                            orders_to_persist.push(order.clone());
                        } else {
                            let order = GridOrder {
                                client_order_id: client_id.clone(),
                                order_id: Some(live_order.order_id),
                                symbol: live_order.symbol.clone(),
                                side: if live_order.side == "BUY" {
                                    OrderSide::Buy
                                } else {
                                    OrderSide::Sell
                                },
                                price: live_order.price,
                                quantity: (live_order.orig_qty - live_order.executed_qty)
                                    .max(Decimal::ZERO),
                                amount_usdc: live_order.price
                                    * (live_order.orig_qty - live_order.executed_qty)
                                        .max(Decimal::ZERO),
                                status: if live_order.executed_qty > Decimal::ZERO {
                                    OrderStatus::PartiallyFilled
                                } else {
                                    OrderStatus::New
                                },
                                created_at: Utc::now(),
                                updated_at: Utc::now(),
                                grid_level: 0,
                                paired_client_order_id: None,
                                is_take_profit: false,
                                purpose: crate::types::OrderPurpose::Legacy,
                                merge_sources: Vec::new(),
                            };
                            orders_to_persist.push(order.clone());
                            active.insert(client_id.clone(), order);
                        }
                    }
                }

                if !orders_to_persist.is_empty() {
                    if let Err(e) = self
                        .state
                        .db
                        .run_blocking(move |db| db.save_managed_orders(&orders_to_persist))
                        .await
                    {
                        error!("Could not persist reconciled orders: {}", e);
                        persistence_ok = false;
                    }
                }
                if self.state.pause_epoch() != pause_epoch {
                    return false;
                }

                {
                    let active = self.state.active_orders.read().await;
                    for (client_id, order) in active.iter() {
                        if !live_ids.contains_key(client_id) {
                            filled_or_closed.push(order.clone());
                        }
                    }
                }

                let mut terminal_orders = Vec::new();
                let mut unresolved_orders = false;
                for mut order in filled_or_closed {
                    if self.state.pause_epoch() != pause_epoch {
                        return false;
                    }
                    let exchange_result = match order.order_id {
                        Some(order_id) => self
                            .client
                            .get_order(&order.symbol, order_id)
                            .await
                            .map(Some),
                        None => {
                            self.client
                                .lookup_order_by_client_id(&order.symbol, &order.client_order_id)
                                .await
                        }
                    };
                    if self.state.pause_epoch() != pause_epoch {
                        return false;
                    }
                    match exchange_result {
                        Ok(None)
                            if (Utc::now() - order.created_at).num_seconds()
                                >= UNKNOWN_SUBMISSION_GRACE_SECS =>
                        {
                            if let Err(error) =
                                self.delete_managed_order(&order.client_order_id).await
                            {
                                error!(
                                    "Could not retire absent order intent {}: {}",
                                    order.client_order_id, error
                                );
                                persistence_ok = false;
                            } else {
                                self.state
                                    .active_orders
                                    .write()
                                    .await
                                    .remove(&order.client_order_id);
                                self.state.add_log("WARN", format!("Unaccepted order intent {} retired after confirmed absence", order.client_order_id)).await;
                            }
                        }
                        Ok(None) => unresolved_orders = true,
                        Ok(Some(exchange_order)) => {
                            // A query may confirm acceptance before the open-
                            // orders snapshot catches up. Persist it before a
                            // later lookup or restart can lose that distinction.
                            if order.order_id.is_none() {
                                order.order_id = Some(exchange_order.order_id);
                                if let Err(error) = self.save_managed_order(&order).await {
                                    error!(
                                        "Could not persist confirmed order {}: {}",
                                        order.client_order_id, error
                                    );
                                    persistence_ok = false;
                                    continue;
                                }
                                self.state
                                    .active_orders
                                    .write()
                                    .await
                                    .insert(order.client_order_id.clone(), order.clone());
                            }
                            let terminal = matches!(
                                exchange_order.status.as_str(),
                                "FILLED" | "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" | "REJECTED"
                            );
                            if !terminal {
                                unresolved_orders = true;
                                continue;
                            }
                            terminal_orders.push((order, exchange_order));
                        }
                        Err(e) => {
                            warn!(
                                "Could not verify order {} status: {}",
                                order.client_order_id, e
                            );
                            unresolved_orders = true;
                        }
                    }
                }

                if !persistence_ok || unresolved_orders {
                    self.report_reconciliation_state(true).await;
                    return false;
                }
                self.last_orders_sync = Some(Instant::now());
                terminal_orders
                    .sort_by_key(|(_, response)| (response.update_time, response.order_id));
                for (mut order, exchange_order) in terminal_orders {
                    if exchange_order.executed_qty > Decimal::ZERO {
                        order.updated_at = exchange_fill_time(&exchange_order);
                        order.quantity = exchange_order.executed_qty;
                        if let Some(avg_price) = exchange_order
                            .avg_price
                            .filter(|price| *price > Decimal::ZERO)
                        {
                            order.price = avg_price;
                        }
                        order.amount_usdc = order.price * order.quantity;
                        if !self.on_order_filled(&mut order).await {
                            persistence_ok = false;
                        }
                    } else {
                        if let Err(e) = self.delete_managed_order(&order.client_order_id).await {
                            error!(
                                "Could not delete closed order {}: {}",
                                order.client_order_id, e
                            );
                            persistence_ok = false;
                        } else {
                            self.state
                                .active_orders
                                .write()
                                .await
                                .remove(&order.client_order_id);
                        }
                        debug!(
                            "Order {} closed without a fill ({})",
                            order.client_order_id, exchange_order.status
                        );
                    }
                }
                if persistence_ok {
                    if let Some(plan) = &mut remainder_plan {
                        if plan.phase == RemainderPhase::Submitting {
                            if let Err(e) = self.adopt_remainder_target(plan).await {
                                warn!("Remainder submission remains unresolved: {}", e);
                                persistence_ok = false;
                            }
                        }
                    }
                }
                if !persistence_ok {
                    self.last_orders_sync = None;
                } else if !self.drain_pair_intents(&symbol, mode).await {
                    persistence_ok = false;
                    self.last_orders_sync = None;
                }
                self.report_reconciliation_state(!persistence_ok).await;
                persistence_ok
            }
            Err(e) => {
                warn!("Error during live orders sync: {}", e);
                self.last_orders_sync = None;
                self.report_reconciliation_state(true).await;
                false
            }
        }
    }

    // Compatibility recovery only: adopted exchange orders are reconciled, but
    // an absent legacy counter-order is retired instead of creating a new pair.
    async fn pair_submission_decision(&mut self, _intent: &GridOrder) -> PairPlacementDecision {
        if self
            .last_account_sync
            .is_none_or(|at| at.elapsed() >= Duration::from_secs(10))
            || self
                .last_orders_sync
                .is_none_or(|at| at.elapsed() >= Duration::from_secs(10))
        {
            return PairPlacementDecision::Wait;
        }
        PairPlacementDecision::Skip("paired replenishment retired; sliding window owns new orders")
    }

    async fn drain_pair_intents(&mut self, symbol: &str, mode: TradingMode) -> bool {
        let scope = symbol.to_string();
        let intents = match self
            .state
            .db
            .run_blocking(move |db| db.load_pair_intents(&scope, mode))
            .await
        {
            Ok(intents) => intents,
            Err(e) => {
                error!("Could not load pending pair intents: {}", e);
                return false;
            }
        };

        for mut intent in intents {
            let parent_id = match intent.paired_client_order_id.clone() {
                Some(id) => id,
                None => {
                    error!("Pair intent {} has no parent", intent.client_order_id);
                    return false;
                }
            };
            let exchange_order = match self
                .client
                .lookup_order_by_client_id(&intent.symbol, &intent.client_order_id)
                .await
            {
                Ok(found) => found,
                Err(e) => {
                    warn!(
                        "Pair order {} remains uncertain: {}",
                        intent.client_order_id, e
                    );
                    return false;
                }
            };
            if let Some(found) = exchange_order {
                if found.client_order_id != intent.client_order_id
                    || found.symbol != intent.symbol
                    || found.side != intent.side.as_str()
                {
                    error!(
                        "Pair order {} has mismatched exchange identity",
                        intent.client_order_id
                    );
                    return false;
                }
                intent.order_id = Some(found.order_id);
                if matches!(
                    found.status.as_str(),
                    "FILLED" | "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" | "REJECTED"
                ) {
                    if found.executed_qty > Decimal::ZERO {
                        intent.updated_at = exchange_fill_time(&found);
                        intent.quantity = found.executed_qty;
                        if let Some(price) = found.avg_price.filter(|price| *price > Decimal::ZERO)
                        {
                            intent.price = price;
                        }
                        intent.amount_usdc = intent.price * intent.quantity;
                        if !self.on_order_filled(&mut intent).await {
                            return false;
                        }
                        // A reused destination may have been journaled by normal
                        // reconciliation already. Retire its parent intent even
                        // when the fill callback takes the idempotent path.
                        let parent = parent_id.clone();
                        if let Err(error) = self
                            .state
                            .db
                            .run_blocking(move |db| db.delete_pair_intent(&parent))
                            .await
                        {
                            error!("Could not retire filled counter-order intent: {}", error);
                            return false;
                        }
                    } else {
                        if let Err(e) = self.delete_managed_order(&intent.client_order_id).await {
                            error!("Could not clear closed pair order: {}", e);
                            return false;
                        }
                        self.state
                            .active_orders
                            .write()
                            .await
                            .remove(&intent.client_order_id);
                        let parent = parent_id.clone();
                        if let Err(e) = self
                            .state
                            .db
                            .run_blocking(move |db| db.delete_pair_intent(&parent))
                            .await
                        {
                            error!("Could not clear closed pair intent: {}", e);
                            return false;
                        }
                    }
                    continue;
                }
                intent.quantity = (found.orig_qty - found.executed_qty).max(Decimal::ZERO);
                intent.amount_usdc = intent.price * intent.quantity;
                intent.status = if found.executed_qty > Decimal::ZERO {
                    OrderStatus::PartiallyFilled
                } else {
                    OrderStatus::New
                };
            } else {
                if *self.state.status.read().await != BotStatus::Running {
                    continue;
                }
                match self.pair_submission_decision(&intent).await {
                    PairPlacementDecision::Wait => return false,
                    PairPlacementDecision::Skip(reason) => {
                        if let Err(e) = self.delete_managed_order(&intent.client_order_id).await {
                            error!("Could not clear skipped pair order: {}", e);
                            return false;
                        }
                        self.state
                            .active_orders
                            .write()
                            .await
                            .remove(&intent.client_order_id);
                        if let Err(e) = self
                            .state
                            .db
                            .run_blocking(move |db| db.delete_pair_intent(&parent_id))
                            .await
                        {
                            error!("Could not clear skipped pair intent: {}", e);
                            return false;
                        }
                        self.state
                            .add_log(
                                "WARN",
                                format!(
                                    "Pair order {} skipped: {}",
                                    intent.client_order_id, reason
                                ),
                            )
                            .await;
                        continue;
                    }
                }
            }
            if let Err(e) = self.save_managed_order(&intent).await {
                error!(
                    "Could not persist pair order {}: {}",
                    intent.client_order_id, e
                );
                self.pause_trading().await;
                return false;
            }
            self.state
                .active_orders
                .write()
                .await
                .insert(intent.client_order_id.clone(), intent);
            let parent = parent_id.clone();
            if let Err(e) = self
                .state
                .db
                .run_blocking(move |db| db.delete_pair_intent(&parent))
                .await
            {
                error!("Could not clear submitted pair intent: {}", e);
                self.pause_trading().await;
                return false;
            }
        }

        if *self.state.status.read().await != BotStatus::Running {
            return true;
        }
        let scope = symbol.to_string();
        match self
            .state
            .db
            .run_blocking(move |db| db.load_pair_intents(&scope, mode))
            .await
        {
            Ok(remaining) => remaining.is_empty(),
            Err(e) => {
                error!("Could not verify pending pair intents: {}", e);
                false
            }
        }
    }

    async fn fetch_execution_pnl(
        &self,
        symbol: &str,
        order_id: i64,
    ) -> Result<(Decimal, Decimal, bool)> {
        let fills = self.client.get_user_trades(symbol, order_id).await?;
        aggregate_execution_pnl(symbol, &fills)
    }

    /// Fill the exchange PnL and fees for rows saved before this version or during API outages.
    async fn reconcile_unverified_trades(&mut self) {
        if self
            .pnl_reconcile_task
            .as_ref()
            .is_some_and(|task| !task.is_finished())
        {
            return;
        }
        let config = self.state.config.read().await.clone();
        if config.exchange.dry_run {
            return;
        }
        let mode = TradingMode::from_exchange(config.exchange.dry_run, config.exchange.is_testnet);
        let pending_count = self.state.stats.read().await.pending_pnl_trades;
        if pending_count == 0 {
            self.pnl_reconcile_offset = 0;
            return;
        }
        let offset = self.pnl_reconcile_offset % pending_count;
        let pending_symbol = config.exchange.symbol.clone();
        let pending = match self
            .state
            .db
            .run_blocking(move |db| db.get_unverified_trades(&pending_symbol, mode, 3, offset))
            .await
        {
            Ok(trades) => trades,
            Err(e) => {
                warn!("Could not load trades awaiting PnL verification: {}", e);
                return;
            }
        };
        self.pnl_reconcile_offset = (offset + pending.len()) % pending_count;
        let client = Arc::clone(&self.client);
        let state = Arc::clone(&self.state);
        self.pnl_reconcile_task = Some(tokio::spawn(async move {
            for mut trade in pending {
                let result = async {
                    let order = client
                        .get_order_by_client_id(&trade.symbol, &trade.client_order_id)
                        .await?;
                    let fills = client
                        .get_user_trades(&trade.symbol, order.order_id)
                        .await?;
                    aggregate_execution_pnl(&trade.symbol, &fills)
                }
                .await;
                match result {
                    Ok((pnl, commission, maker)) => {
                        trade.realized_pnl = pnl;
                        trade.commission = commission;
                        trade.is_maker = maker;
                        trade.pnl_verified = true;
                        if let Err(e) = state.verify_trade_pnl(trade).await {
                            warn!("Could not save verified trade PnL: {}", e);
                        }
                    }
                    Err(e) => debug!(
                        "PnL verification deferred for {}: {}",
                        trade.client_order_id, e
                    ),
                }
            }
        }));
    }

    /// Fetch position and account balances from Binance in Live mode
    async fn sync_account_and_position(&mut self) -> bool {
        self.sync_account_and_position_inner(false).await
    }

    async fn sync_account_and_position_inner(&mut self, shared_position: bool) -> bool {
        let pause_epoch = self.state.pause_epoch();
        let symbol = self.state.config.read().await.exchange.symbol.clone();

        let position_result = if shared_position {
            self.position_cache.get(&self.client, &symbol).await
        } else {
            let result = self.client.get_position(&symbol).await;
            let received_at = Instant::now();
            // Critical reconciliations and fills must not leave an older
            // account-wide position snapshot available for later polling.
            self.position_cache.invalidate(&self.client).await;
            result.map(|position| (position, received_at))
        };
        let position_instant = position_result.as_ref().ok().map(|(_, at)| *at);
        let position_ok = if let Ok((Some(pos), received_at)) = position_result {
            let mut state_pos = self.state.position.write().await;
            state_pos.symbol = pos.symbol;
            state_pos.size = pos.position_amt;
            state_pos.entry_price = pos.entry_price;
            state_pos.mark_price = pos.mark_price;
            state_pos.unrealized_pnl = pos.un_realized_profit;
            state_pos.liquidation_price = pos.liquidation_price;
            state_pos.leverage = pos.leverage.parse().unwrap_or(20);
            state_pos.update_time = chrono::Duration::from_std(received_at.elapsed())
                .ok()
                .map(|age| Utc::now() - age);
            true
        } else {
            self.state.position.write().await.update_time = None;
            warn!("Could not sync Binance position for {}", symbol);
            false
        };

        if !position_ok {
            self.set_global_reconciliation_ready(false).await;
        }
        if self.state.pause_epoch() != pause_epoch {
            return false;
        }

        let account = if shared_position {
            self.account_cache.get(&self.client).await
        } else {
            self.account_cache.refresh(&self.client).await
        };
        let account_instant = account
            .as_ref()
            .ok()
            .map(|snapshot| snapshot.received_instant);
        let account_ok = match account {
            Ok(snapshot) => {
                if let Some(asset) = snapshot.account.margin_asset_for_symbol(&symbol) {
                    let mut state_acc = self.state.account.write().await;
                    state_acc.asset = asset.asset.clone();
                    state_acc.total_wallet_balance = asset.wallet_balance;
                    state_acc.available_balance = asset.available_balance;
                    state_acc.margin_balance = asset.margin_balance;
                    state_acc.unrealized_profit = asset.unrealized_profit;
                    state_acc.update_time = snapshot.received_at;
                    true
                } else {
                    warn!(
                        "No matching margin asset in Binance account response for {}",
                        symbol
                    );
                    false
                }
            }
            Err(e) => {
                warn!("Could not sync Binance account balance: {}", e);
                false
            }
        };
        if !account_ok {
            self.state.account.write().await.update_time = chrono::DateTime::<Utc>::default();
            self.set_global_reconciliation_ready(false).await;
        }
        if let Some(global) = self
            .state
            .global_position_limit
            .get()
            .and_then(std::sync::Weak::upgrade)
        {
            global.check().await;
        }
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

    /// Executed when an order is confirmed filled
    async fn on_order_filled(&mut self, order: &mut GridOrder) -> bool {
        order.status = OrderStatus::Filled;
        let config = self.state.config.read().await.clone();
        if config.exchange.dry_run {
            order.updated_at = Utc::now();
        } else {
            self.account_cache.invalidate(&self.client).await;
        }
        let mode = TradingMode::from_exchange(config.exchange.dry_run, config.exchange.is_testnet);
        let recorded_id = order.client_order_id.clone();
        match self
            .state
            .db
            .run_blocking(move |db| db.has_recorded_order(&recorded_id, mode))
            .await
        {
            Ok(true) => {
                self.state
                    .active_orders
                    .write()
                    .await
                    .remove(&order.client_order_id);
                if !config.exchange.dry_run {
                    if let Err(e) = self.delete_managed_order(&order.client_order_id).await {
                        error!("Could not clear already-recorded order: {}", e);
                        return false;
                    }
                }
                return true;
            }
            Ok(false) => {}
            Err(e) => {
                error!(
                    "Could not check recorded fill {}: {}",
                    order.client_order_id, e
                );
                self.pause_trading().await;
                return false;
            }
        }

        // Remove from active orders
        self.state
            .active_orders
            .write()
            .await
            .remove(&order.client_order_id);

        let mut cycle_profit = Decimal::ZERO;
        let mut simulated_pnl = Decimal::ZERO;
        let mut is_completed_cycle = false;
        let mut note = format!(
            "Grid {} filled at {} (Qty: {})",
            order.side.as_str(),
            order.price,
            order.quantity
        );

        // Legacy paired cycles retain their spread estimate in either direction.
        if order.side == config.grid.direction.closing_side() {
            if let Some(paired_id) = &order.paired_client_order_id {
                let paired_id = paired_id.clone();
                let opening_trade = self
                    .state
                    .db
                    .run_blocking(move |db| db.get_trade_by_client_id(&paired_id, mode))
                    .await
                    .ok()
                    .flatten()
                    .filter(|trade| trade.side == config.grid.direction.opening_side());
                if let Some(opening) = opening_trade {
                    cycle_profit = (order.price - opening.price)
                        * config.grid.direction.sign()
                        * order.quantity.min(opening.quantity);
                    is_completed_cycle = true;
                    note = format!(
                        "Completed Grid Cycle! {} at {} (Paired Entry: {}, Spread Estimate: {} USDC)",
                        order.side.as_str(), order.price, opening.price, cycle_profit
                    );
                    self.state.add_log("SUCCESS", note.clone()).await;
                }
            }
        }
        if !is_completed_cycle {
            self.state.add_log("INFO", note.clone()).await;
        }

        if config.exchange.dry_run {
            let mut pos = self.state.position.write().await;
            let delta = if order.side == OrderSide::Buy {
                order.quantity
            } else {
                -order.quantity
            };
            let prev_size = pos.size;
            let new_size = prev_size + delta;
            if prev_size.is_zero() || prev_size.signum() == delta.signum() {
                pos.entry_price = (pos.entry_price * prev_size.abs()
                    + order.price * order.quantity)
                    / new_size.abs();
            } else {
                simulated_pnl = (order.price - pos.entry_price)
                    * prev_size.signum()
                    * order.quantity.min(prev_size.abs());
                if new_size.is_zero() {
                    pos.entry_price = Decimal::ZERO;
                } else if new_size.signum() != prev_size.signum() {
                    pos.entry_price = order.price;
                }
            }
            pos.size = new_size;
            let ticker = self.state.ticker.read().await.clone();
            let mut acc = self.state.account.write().await;
            acc.total_wallet_balance += simulated_pnl;
            update_paper_account(&mut pos, &mut acc, ticker.grid_reference_price());
        } else {
            self.sync_account_and_position().await;
        }

        let (realized_pnl, commission, is_maker, pnl_verified) = if config.exchange.dry_run {
            (simulated_pnl, Decimal::ZERO, true, true)
        } else if let Some(order_id) = order.order_id {
            match self.fetch_execution_pnl(&order.symbol, order_id).await {
                Ok((pnl, fee, maker)) => (pnl, fee, maker, true),
                Err(e) => {
                    warn!(
                        "Exchange PnL unavailable for {}: {}",
                        order.client_order_id, e
                    );
                    (Decimal::ZERO, Decimal::ZERO, true, false)
                }
            }
        } else {
            (Decimal::ZERO, Decimal::ZERO, true, false)
        };

        // Record trade in history
        let trade = TradeRecord {
            trade_id: format!("grid:{}", order.client_order_id),
            client_order_id: order.client_order_id.clone(),
            symbol: order.symbol.clone(),
            mode,
            side: order.side,
            price: order.price,
            quantity: order.quantity,
            amount_usdc: order.price * order.quantity,
            realized_pnl,
            commission,
            pnl_verified,
            is_maker,
            timestamp: order.updated_at,
            note,
        };

        // Persist trade to SQLite and update runtime stats
        let recorded = self
            .state
            .record_trade(
                trade,
                is_completed_cycle.then_some(cycle_profit),
                None,
                order.paired_client_order_id.clone(),
            )
            .await;
        if !recorded {
            self.pause_trading().await;
            self.state
                .active_orders
                .write()
                .await
                .insert(order.client_order_id.clone(), order.clone());
            return false;
        }
        true
    }

    /// Ensure the active pre-placed order window matches buy_window and sell_window
    async fn maintain_grid_window(&mut self) {
        if self.state.try_order_submission().is_none() {
            return;
        }
        let config = self.state.config.read().await.clone();
        if self.state.is_shutting_down() || *self.state.status.read().await != BotStatus::Running {
            return;
        }
        if self.pause_if_position_limit_reached().await {
            return;
        }
        // Opposite positions require cancellation even while prices are stale.
        if !self.position_direction_ready().await {
            return;
        }
        if !self.market_data_ready().await {
            return;
        }
        match self.recover_remainder_plan().await {
            Ok(true) => return,
            Ok(false) => {}
            Err(e) => {
                self.state
                    .add_log("WARN", format!("Remainder recovery pending: {}", e))
                    .await;
                return;
            }
        }
        if !config.exchange.dry_run {
            let symbol = config.exchange.symbol.clone();
            let mode = TradingMode::from_exchange(false, config.exchange.is_testnet);
            match self
                .state
                .db
                .run_blocking(move |db| db.load_pair_intents(&symbol, mode))
                .await
            {
                Ok(intents) if !intents.is_empty() => return,
                Ok(_) => {}
                Err(e) => {
                    error!("Could not inspect pending pair intents: {}", e);
                    self.pause_trading().await;
                    return;
                }
            }
        }
        self.trim_closing_orders_to_position().await;
        if !config.exchange.dry_run {
            let symbol = config.exchange.symbol.clone();
            let mode = TradingMode::from_exchange(false, config.exchange.is_testnet);
            match self
                .state
                .db
                .run_blocking(move |db| db.load_pair_intents(&symbol, mode))
                .await
            {
                Ok(intents) if !intents.is_empty() => return,
                Ok(_) => {}
                Err(e) => {
                    error!("Could not inspect pending pair intents: {}", e);
                    self.pause_trading().await;
                    return;
                }
            }
        }
        let current_price = self.state.ticker.read().await.grid_reference_price();
        if current_price.is_zero() {
            return;
        }

        let rules = self.state.rules.read().await.clone();

        let grid_interval = config.grid.grid_interval;
        if let Err(error) = rules.validate_grid_interval(grid_interval) {
            self.state.add_log("ERROR", error.to_string()).await;
            self.pause_trading().await;
            return;
        }
        let existing: Vec<_> = self
            .state
            .active_orders
            .read()
            .await
            .values()
            .cloned()
            .collect();
        let misaligned: Vec<_> = existing
            .into_iter()
            .filter(|o| is_window_order(o) && !rules.is_grid_price(o.price, grid_interval))
            .collect();
        if !misaligned.is_empty() {
            for order in misaligned {
                if *self.state.status.read().await != BotStatus::Running
                    || !self.cancel_single_order(&order).await
                {
                    return;
                }
            }
            // Cancellation can reveal a fill and change reserved levels.
            // Reconcile on the next cycle before allocating any released funds.
            return;
        }
        // Collect current active order prices
        let active_orders: Vec<GridOrder> = self
            .state
            .active_orders
            .read()
            .await
            .values()
            .cloned()
            .collect();
        let mut active_buy_prices: Vec<Decimal> = active_orders
            .iter()
            .filter(|o| o.side == OrderSide::Buy)
            .map(|o| o.price)
            .collect();
        let mut active_sell_prices: Vec<Decimal> = active_orders
            .iter()
            .filter(|o| o.side == OrderSide::Sell)
            .map(|o| o.price)
            .collect();

        active_buy_prices.sort();
        active_sell_prices.sort();

        // Preserve partial executions. Their remaining quantity still reserves
        // funds or inventory without displacing the nearest window.
        let preserved: Vec<_> = active_orders
            .iter()
            .filter(|o| !is_window_order(o))
            .collect();
        let reserved_buys: Vec<_> = preserved
            .iter()
            .filter(|o| o.side == OrderSide::Buy)
            .map(|o| o.price)
            .collect();
        let reserved_sells: Vec<_> = preserved
            .iter()
            .filter(|o| o.side == OrderSide::Sell)
            .map(|o| o.price)
            .collect();
        let (mut desired_buy_prices, mut desired_sell_prices) =
            match self.window_prices(&config, &rules, current_price).await {
                Ok(prices) => prices,
                Err(error) => {
                    self.state
                        .add_log("ERROR", format!("Could not load last fills: {}", error))
                        .await;
                    self.pause_trading().await;
                    return;
                }
            };
        // Protected orders inside either nearest band occupy their level without
        // extending the band to compensate for far-away protected orders.
        desired_buy_prices.retain(|price| {
            !has_nearby_grid_order(&reserved_buys, *price, grid_interval, rules.tick_size)
        });
        desired_sell_prices.retain(|price| {
            !has_nearby_grid_order(&reserved_sells, *price, grid_interval, rules.tick_size)
        });

        if self
            .prune_grid_window(&desired_buy_prices, &desired_sell_prices)
            .await
        {
            return;
        }

        for &target_price in &desired_buy_prices {
            // Retain queue priority for active buys still inside the nearest band.
            let already_exists = has_nearby_grid_order(
                &active_buy_prices,
                target_price,
                grid_interval,
                rules.tick_size,
            );

            if !already_exists && target_price < current_price {
                let client_id = new_grid_client_order_id(OrderSide::Buy);
                if self
                    .place_grid_order(OrderSide::Buy, target_price, client_id, -1, None)
                    .await
                {
                    active_buy_prices.push(target_price);
                    if self
                        .prune_grid_window(&desired_buy_prices, &desired_sell_prices)
                        .await
                    {
                        return;
                    }
                }
            }
        }

        // Ordinary sells use the same lattice as buys.
        for &target_price in &desired_sell_prices {
            let already_exists = has_nearby_grid_order(
                &active_sell_prices,
                target_price,
                grid_interval,
                rules.tick_size,
            );

            if !already_exists && target_price > current_price {
                let client_id = new_grid_client_order_id(OrderSide::Sell);
                if self
                    .place_grid_order(OrderSide::Sell, target_price, client_id, 1, None)
                    .await
                {
                    active_sell_prices.push(target_price);
                    if self
                        .prune_grid_window(&desired_buy_prices, &desired_sell_prices)
                        .await
                    {
                        return;
                    }
                }
            }
        }

        let closing_prices = if config.grid.direction == GridDirection::Long {
            &desired_sell_prices
        } else {
            &desired_buy_prices
        };
        if let Err(e) = self.reconcile_remainder(closing_prices).await {
            self.state
                .add_log("WARN", format!("Remainder reconciliation pending: {}", e))
                .await;
            return;
        }
        self.prune_grid_window(&desired_buy_prices, &desired_sell_prices)
            .await;
    }

    /// Confirm stale ordinary orders before reusing their funds or inventory.
    /// Return after cancellation so racing fills reconcile before replenishment.
    async fn prune_grid_window(
        &mut self,
        desired_buys: &[Decimal],
        desired_sells: &[Decimal],
    ) -> bool {
        let mut active: Vec<_> = self
            .state
            .active_orders
            .read()
            .await
            .values()
            .cloned()
            .collect();
        let total = active.len();
        // Preserve existing queue priority, without preferring any order label.
        active.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.client_order_id.cmp(&b.client_order_id))
        });
        let mut occupied: HashSet<_> = active
            .iter()
            .filter(|o| !is_window_order(o))
            .map(|o| (o.side, o.price))
            .collect();
        let mut obsolete: Vec<_> = active
            .into_iter()
            .filter(|order| {
                let desired = if order.side == OrderSide::Buy {
                    desired_buys
                } else {
                    desired_sells
                };
                is_window_order(order)
                    && (!desired.contains(&order.price)
                        || !occupied.insert((order.side, order.price)))
            })
            .collect();
        if obsolete.is_empty() {
            return false;
        }
        self.state
            .add_log(
                "INFO",
                format!(
                    "Grid window cleanup: {} orders, canceling {} stale or duplicate orders",
                    total,
                    obsolete.len()
                ),
            )
            .await;
        obsolete.sort_by(|a, b| a.client_order_id.cmp(&b.client_order_id));
        for order in obsolete {
            if *self.state.status.read().await != BotStatus::Running
                || !self.cancel_single_order(&order).await
            {
                return true;
            }
        }
        true
    }

    /// Keep closing orders within the directional position, nearest prices first.
    async fn trim_closing_orders_to_position(&mut self) {
        let direction = self.state.config.read().await.grid.direction;
        let mut closing_orders: Vec<GridOrder> = self
            .state
            .active_orders
            .read()
            .await
            .values()
            .filter(|order| order.side == direction.closing_side())
            .cloned()
            .collect();
        closing_orders.sort_by(|a, b| {
            let price_order = if direction == GridDirection::Long {
                a.price.cmp(&b.price)
            } else {
                b.price.cmp(&a.price)
            };
            price_order.then_with(|| a.client_order_id.cmp(&b.client_order_id))
        });
        let mut remaining = direction.position_quantity(self.state.position.read().await.size);
        for order in closing_orders {
            if order.quantity <= remaining {
                remaining -= order.quantity;
            } else if !self.cancel_single_order(&order).await {
                remaining = Decimal::ZERO;
            }
        }
    }

    /// Place a single grid order (either Maker GTX on Binance or simulated)
    async fn place_grid_order(
        &mut self,
        side: OrderSide,
        price: Decimal,
        client_order_id: String,
        grid_level: i32,
        paired_client_order_id: Option<String>,
    ) -> bool {
        self.place_grid_order_with_quantity(
            side,
            price,
            client_order_id,
            grid_level,
            paired_client_order_id,
            None,
        )
        .await
    }

    async fn place_grid_order_with_quantity(
        &mut self,
        side: OrderSide,
        price: Decimal,
        client_order_id: String,
        grid_level: i32,
        paired_client_order_id: Option<String>,
        order_quantity: Option<Decimal>,
    ) -> bool {
        let Some(_submission) = self.state.try_order_submission() else {
            return false;
        };
        if self.state.is_shutting_down() || *self.state.status.read().await != BotStatus::Running {
            return false;
        }
        if self.pause_if_position_limit_reached().await {
            return false;
        }
        if !self.position_direction_ready().await {
            return false;
        }
        let config = self.state.config.read().await.clone();
        let rules = self.state.rules.read().await.clone();
        let symbol = config.exchange.symbol.clone();
        let Some(full_quantity) = order_quantity
            .or_else(|| rules.calculate_quantity(price, config.grid.order_amount_usdc))
        else {
            return false;
        };
        if price <= Decimal::ZERO
            || price < rules.min_price
            || price > rules.max_price
            || rules.round_price(price) != price
            || full_quantity < rules.min_qty
            || full_quantity > rules.max_qty
            || rules.round_quantity(full_quantity) != full_quantity
            || price * full_quantity < rules.min_notional
            || !rules.is_grid_price(price, config.grid.grid_interval)
            || config.grid.min_price.is_some_and(|min| price < min)
            || config.grid.max_price.is_some_and(|max| price > max)
        {
            return false;
        }

        if !config.exchange.dry_run
            && self
                .last_account_sync
                .is_none_or(|at| at.elapsed() >= Duration::from_secs(10))
        {
            warn!("Skipping order: Binance account or position has not been synchronized recently");
            return false;
        }
        if !config.exchange.dry_run
            && self
                .last_orders_sync
                .is_none_or(|at| at.elapsed() >= Duration::from_secs(10))
        {
            warn!("Skipping order: Binance open orders have not been synchronized recently");
            return false;
        }

        // Enforce Maker pricing check
        let ticker = self.state.ticker.read().await.clone();
        if !self.market_data_ready().await {
            return false;
        }
        let current_market_price = ticker.last_price;
        let reference_price = ticker.grid_reference_price();
        if !current_market_price.is_zero() {
            if side == OrderSide::Buy && price >= current_market_price {
                warn!(
                    "Buy price {} >= market price {}, skipping to prevent Taker fill",
                    price, current_market_price
                );
                return false;
            }
            if side == OrderSide::Sell && price <= current_market_price {
                warn!(
                    "Sell price {} <= market price {}, skipping to prevent Taker fill",
                    price, current_market_price
                );
                return false;
            }
        }
        if reference_price > Decimal::ZERO {
            let (buys, sells) = match self.window_prices(&config, &rules, reference_price).await {
                Ok(prices) => prices,
                Err(error) => {
                    self.state
                        .add_log("ERROR", format!("Could not load last fills: {}", error))
                        .await;
                    self.pause_trading().await;
                    return false;
                }
            };
            let window = if side == OrderSide::Buy { buys } else { sells };
            if !window.contains(&price) {
                return false;
            }
        }

        let orders: Vec<GridOrder> = self
            .state
            .active_orders
            .read()
            .await
            .values()
            .cloned()
            .collect();
        if grid_level_occupied(
            &orders,
            side,
            price,
            config.grid.grid_interval,
            rules.tick_size,
        ) {
            return false;
        }
        let quantity = if side == config.grid.direction.opening_side() {
            if let Some(limit) = config.grid.max_position_usdc {
                let position = self.state.position.read().await.clone();
                let mark_price = position_valuation_price(&position, &ticker);
                let position_value = position.size.abs() * mark_price;
                let projected_position_value =
                    (position.size + config.grid.direction.sign() * full_quantity).abs()
                        * mark_price;
                // A full next opening order must fit the filled position even when the position
                // has not yet reached the cap. Pending orders alone still only reserve funds.
                if config.grid.pause_on_max_position
                    && mark_price > Decimal::ZERO
                    && projected_position_value > limit
                    && projected_position_value > position_value
                {
                    self.pause_for_position_limit(format!(
                        "持仓名义价值 {}，下一笔{}单（价格 {}，数量 {}）成交后预计持仓名义价值 {} 将超过最大持仓名义价值 {}",
                        position_value, if side == OrderSide::Buy { "买" } else { "卖" }, price, full_quantity, projected_position_value, limit
                    ))
                    .await;
                    return false;
                }
                let valuation_price = reference_price.max(current_market_price).max(price);
                let exposure = opening_exposure_usdc(
                    config.grid.direction,
                    position.size,
                    valuation_price,
                    &orders,
                ) + price * full_quantity;
                if exposure > limit {
                    debug!(
                        "Skipping {} at {}: projected exposure {} exceeds limit {}",
                        side.as_str(),
                        price,
                        exposure,
                        limit
                    );
                    return false;
                }
            }
            full_quantity
        } else {
            let available = closing_quantity_available(
                config.grid.direction,
                self.state.position.read().await.size,
                &orders,
            );
            if available >= full_quantity {
                full_quantity
            } else {
                debug!(
                    "Skipping {} at {}: available {} < full grid quantity {}",
                    side.as_str(),
                    price,
                    available,
                    full_quantity
                );
                return false;
            }
        };

        let formatted_price = rules.format_price(price);
        let formatted_qty = rules.format_quantity(quantity);
        let mut order = GridOrder {
            client_order_id: client_order_id.clone(),
            order_id: None,
            symbol: symbol.clone(),
            side,
            price,
            quantity,
            amount_usdc: price * quantity,
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level,
            paired_client_order_id,
            is_take_profit: false,
            purpose: OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };

        if config.exchange.dry_run {
            // Paper Trading simulation
            order.order_id = Some(rand::random::<i64>().abs());

            self.state
                .active_orders
                .write()
                .await
                .insert(client_order_id.clone(), order);
            debug!(
                "[DRY-RUN] Placed {} Maker order: price={}, qty={}, id={}",
                side.as_str(),
                formatted_price,
                formatted_qty,
                client_order_id
            );
            true
        } else {
            // Real Binance Futures API order with GTX Post-Only
            if let Err(e) = self.save_managed_order(&order).await {
                error!("Could not persist order intent {}: {}", client_order_id, e);
                self.pause_trading().await;
                return false;
            }
            if self.state.is_shutting_down()
                || *self.state.status.read().await != BotStatus::Running
                || !self.state.ticker.read().await.prices_are_fresh(Utc::now())
            {
                if let Err(e) = self.delete_managed_order(&client_order_id).await {
                    error!(
                        "Could not clear paused order intent {}: {}",
                        client_order_id, e
                    );
                }
                return false;
            }
            match self
                .client
                .place_order(NewOrderRequest {
                    symbol: &symbol,
                    side: side.as_str(),
                    price: &formatted_price,
                    quantity: &formatted_qty,
                    client_order_id: &client_order_id,
                    post_only: true,
                    reduce_only: side == config.grid.direction.closing_side(),
                })
                .await
            {
                Ok(resp) => {
                    order.order_id = Some(resp.order_id);
                    order.symbol = resp.symbol;
                    order.client_order_id = resp.client_order_id.clone();

                    if let Err(e) = self.save_managed_order(&order).await {
                        error!(
                            "Order {} was accepted but could not be persisted: {}",
                            resp.client_order_id, e
                        );
                        self.pause_trading().await;
                    }
                    self.state
                        .active_orders
                        .write()
                        .await
                        .insert(resp.client_order_id, order);
                    info!(
                        "Placed {} Maker order on Binance: price={}, qty={}, orderId={}",
                        side.as_str(),
                        formatted_price,
                        formatted_qty,
                        resp.order_id
                    );
                    true
                }
                Err(ExchangeError::PostOnlyRejected(msg)) => {
                    if let Err(e) = self.delete_managed_order(&client_order_id).await {
                        error!(
                            "Could not clear rejected order intent {}: {}",
                            client_order_id, e
                        );
                        self.pause_trading().await;
                    }
                    warn!(
                        "Maker GTX rejected (would take liquidity): {}. Will retry next tick.",
                        msg
                    );
                    false
                }
                Err(e) => {
                    error!(
                        "Failed to place {} order at {}: {}",
                        side.as_str(),
                        formatted_price,
                        e
                    );
                    self.state
                        .add_log(
                            "ERROR",
                            format!(
                                "Order placement failed ({} @ {}): {}",
                                side.as_str(),
                                formatted_price,
                                e
                            ),
                        )
                        .await;
                    // A timed-out request may already have reached Binance. Reserve
                    // its grid level until exchange reconciliation resolves this ID.
                    if e.execution_status_unknown() {
                        self.last_orders_sync = None;
                        match self
                            .client
                            .get_order_by_client_id(&symbol, &client_order_id)
                            .await
                        {
                            Ok(found) => {
                                order.order_id = Some(found.order_id);
                                order.status = if found.executed_qty > Decimal::ZERO {
                                    OrderStatus::PartiallyFilled
                                } else {
                                    OrderStatus::New
                                };
                                if let Err(error) = self.save_managed_order(&order).await {
                                    error!(
                                        "Could not persist accepted order {}: {}",
                                        client_order_id, error
                                    );
                                    self.pause_trading().await;
                                }
                            }
                            Err(query_error) => warn!(
                                "Order {} remains unresolved after placement error: {}",
                                client_order_id, query_error
                            ),
                        }
                        self.state
                            .active_orders
                            .write()
                            .await
                            .insert(client_order_id, order);
                    } else if let Err(clear_error) =
                        self.delete_managed_order(&client_order_id).await
                    {
                        error!(
                            "Could not clear failed order intent {}: {}",
                            client_order_id, clear_error
                        );
                        self.pause_trading().await;
                    }
                    false
                }
            }
        }
    }

    /// Cancel a single order
    async fn cancel_single_order(&mut self, order: &GridOrder) -> bool {
        let is_dry_run = self.state.config.read().await.exchange.dry_run;
        if !is_dry_run {
            if let Err(e) = self
                .client
                .cancel_order(&order.symbol, order.order_id, Some(&order.client_order_id))
                .await
            {
                warn!("Failed to cancel order {}: {}", order.client_order_id, e);
                // A lost response or a fill preceding cancellation can still
                // leave a terminal order. Confirm it through the query below.
            }
            let exchange_order = match order.order_id {
                Some(id) => self.client.get_order(&order.symbol, id).await,
                None => {
                    self.client
                        .get_order_by_client_id(&order.symbol, &order.client_order_id)
                        .await
                }
            };
            match exchange_order {
                Ok(exchange_order)
                    if !matches!(
                        exchange_order.status.as_str(),
                        "FILLED" | "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" | "REJECTED"
                    ) =>
                {
                    self.last_orders_sync = None;
                    warn!("Cancellation is not terminal for {}", order.client_order_id);
                    return false;
                }
                Ok(exchange_order) if exchange_order.executed_qty > Decimal::ZERO => {
                    let mut filled = order.clone();
                    filled.updated_at = exchange_fill_time(&exchange_order);
                    filled.quantity = exchange_order.executed_qty;
                    if let Some(price) = exchange_order
                        .avg_price
                        .filter(|price| *price > Decimal::ZERO)
                    {
                        filled.price = price;
                    }
                    return self.on_order_filled(&mut filled).await;
                }
                Ok(_) => {}
                Err(e) => {
                    warn!(
                        "Canceled order {} but could not confirm fill status: {}",
                        order.client_order_id, e
                    );
                    return false;
                }
            }
        }
        self.state
            .active_orders
            .write()
            .await
            .remove(&order.client_order_id);
        if let Err(e) = self.delete_managed_order(&order.client_order_id).await {
            error!(
                "Could not delete managed order {}: {}",
                order.client_order_id, e
            );
            self.pause_trading().await;
            return false;
        }
        true
    }

    /// Cancel all active orders
    async fn cancel_all_orders(&mut self) -> bool {
        let config = self.state.config.read().await.clone();
        let is_dry_run = config.exchange.dry_run;
        let symbol = config.exchange.symbol.clone();
        let mode = TradingMode::from_exchange(config.exchange.dry_run, config.exchange.is_testnet);
        let pause_epoch = self.state.pause_epoch();
        let was_running = *self.state.status.read().await == BotStatus::Running;
        let mut recovery_failed = false;
        if let Err(e) = self.state.pause_trading_silently().await {
            error!("Failed to persist paused status before cancellation: {}", e);
            self.pause_trading_with_reason("撤单前保存策略状态失败")
                .await;
            recovery_failed = true;
        }

        if let Err(e) = self.cancel_remainder_plan().await {
            error!(
                "Could not resolve pending remainder plan during cancellation: {}",
                e
            );
            self.pause_trading_with_reason("未能确认余量挂单撤销，策略已暂停")
                .await;
            recovery_failed = true;
        }

        if !is_dry_run {
            // Initialization may have failed while loading recovery metadata.
            // Never clear persisted orders without first restoring and reconciling them.
            let restore_symbol = symbol.clone();
            let restored = match self
                .state
                .db
                .run_blocking(move |db| db.load_managed_orders(&restore_symbol))
                .await
            {
                Ok(orders) => orders,
                Err(e) => {
                    error!("Failed to load managed orders before cancellation: {}", e);
                    self.pause_trading().await;
                    recovery_failed = true;
                    Vec::new()
                }
            };
            {
                let mut active = self.state.active_orders.write().await;
                for order in restored
                    .into_iter()
                    .filter(|order| is_grid_order(&order.client_order_id))
                {
                    active.entry(order.client_order_id.clone()).or_insert(order);
                }
            }
            let live_orders = match self.client.get_open_orders(&symbol).await {
                Ok(orders) => orders,
                Err(e) => {
                    error!("Failed to list managed orders before cancellation: {}", e);
                    self.pause_trading().await;
                    // The exchange listing is incomplete, but known orders can still
                    // be canceled individually. Preserve all recovery metadata.
                    let known: Vec<_> = self
                        .state
                        .active_orders
                        .read()
                        .await
                        .values()
                        .cloned()
                        .collect();
                    for order in known
                        .iter()
                        .filter(|order| is_grid_order(&order.client_order_id))
                    {
                        if let Err(error) = self
                            .client
                            .cancel_order(&symbol, order.order_id, Some(&order.client_order_id))
                            .await
                        {
                            warn!(
                                "Failed to cancel known managed order {}: {}",
                                order.client_order_id, error
                            );
                        }
                    }
                    return false;
                }
            };
            let mut cancellation_failed = false;
            for order in live_orders
                .into_iter()
                .filter(|order| is_grid_order(&order.client_order_id))
            {
                if let Err(e) = self
                    .client
                    .cancel_order(&symbol, Some(order.order_id), None)
                    .await
                {
                    if self
                        .client
                        .get_order(&symbol, order.order_id)
                        .await
                        .is_ok_and(|confirmed| {
                            matches!(
                                confirmed.status.as_str(),
                                "FILLED" | "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" | "REJECTED"
                            )
                        })
                    {
                        warn!("Cancellation response failed for {}, but its terminal state was confirmed", order.client_order_id);
                        continue;
                    }
                    error!(
                        "Failed to cancel managed order {}: {}",
                        order.client_order_id, e
                    );
                    self.pause_trading().await;
                    cancellation_failed = true;
                }
            }
            // Without the complete recovery metadata, even a terminal exchange
            // response cannot safely account for fills or justify clearing records.
            if recovery_failed || cancellation_failed {
                return false;
            }
            if !self.sync_live_orders().await || !self.state.active_orders.read().await.is_empty() {
                error!("Managed orders could not be fully reconciled after cancellation");
                self.pause_trading().await;
                return false;
            }
        }

        if recovery_failed {
            return false;
        }
        self.state.active_orders.write().await.clear();
        if !is_dry_run {
            if let Err(e) = self
                .state
                .db
                .run_blocking(move |db| {
                    db.clear_managed_orders(&symbol)?;
                    db.clear_pair_intents(&symbol, mode)
                })
                .await
            {
                error!("Failed to clear persisted managed orders: {}", e);
                self.pause_trading().await;
                return false;
            }
        }
        if was_running && self.state.pause_epoch() == pause_epoch + 1 {
            if let Err(e) = self.state.resume_trading_silently().await {
                error!("Failed to restore running status after cancellation: {}", e);
                self.pause_trading_with_reason("撤单后恢复策略状态失败")
                    .await;
                return false;
            }
        }
        true
    }

    /// Rebalance grid centered around current price
    async fn rebalance_grid(&mut self) {
        if !self.cancel_all_orders().await {
            self.state
                .add_log(
                    "ERROR",
                    "Grid rebalance stopped: failed to cancel existing orders",
                )
                .await;
            return;
        }
        if *self.state.status.read().await == BotStatus::Running {
            self.maintain_grid_window().await;
        }
        self.state
            .add_log("INFO", "Grid window refreshed and rebalanced")
            .await;
    }
}

#[cfg(test)]
#[path = "reliability_tests.rs"]
mod reliability_tests;

#[cfg(test)]
mod tests {
    use super::{
        aggregate_execution_pnl, buy_exposure_usdc, has_nearby_grid_order, is_grid_order,
        new_grid_client_order_id, new_pair_intent, paired_grid_client_order_id,
        reserved_sell_quantity, sell_quantity_available, GridTradingEngine,
    };
    use crate::config::AppConfig;
    use crate::db::Database;
    use crate::exchange::client::BinanceFuturesClient;
    use crate::exchange::{BinanceOrderResponse, BinanceUserTrade};
    use crate::server::state::AppState;
    use crate::types::OrderPurpose;
    use crate::types::{
        BotControlAction, BotStatus, GridOrder, GridStats, OrderSide, OrderStatus, TickerInfo,
        TradeRecord, TradingMode,
    };
    use axum::{
        extract::{Query, State},
        routing::{delete, get},
        Json, Router,
    };
    use chrono::Utc;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };
    use std::time::Instant;
    use tokio::sync::{broadcast, mpsc};

    #[derive(Clone, Default)]
    struct PairExchange {
        orders: Arc<Mutex<HashMap<String, BinanceOrderResponse>>>,
        posts: Arc<Mutex<Vec<String>>>,
        fail_lookup: Arc<AtomicUsize>,
        fail_open_orders: Arc<AtomicUsize>,
        lose_response: Arc<AtomicUsize>,
    }

    fn consume_failure(counter: &AtomicUsize) -> bool {
        let mut count = counter.load(Ordering::SeqCst);
        while let Some(next) = count.checked_sub(1) {
            match counter.compare_exchange(count, next, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => return true,
                Err(actual) => count = actual,
            }
        }
        false
    }

    async fn pair_exchange_server(fake: PairExchange) -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new()
            .route(
                "/fapi/v1/openOrders",
                get(|State(fake): State<PairExchange>| async move {
                    if consume_failure(&fake.fail_open_orders) {
                        return (axum::http::StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"code": -1000})));
                    }
                    let orders: Vec<_> = fake.orders.lock().unwrap().values()
                        .filter(|order| matches!(order.status.as_str(), "NEW" | "PARTIALLY_FILLED"))
                        .cloned()
                        .collect();
                    (axum::http::StatusCode::OK, Json(serde_json::to_value(orders).unwrap()))
                }),
            )
            .route(
                "/fapi/v1/order",
                get(|State(fake): State<PairExchange>, Query(query): Query<HashMap<String, String>>| async move {
                    if consume_failure(&fake.fail_lookup) {
                        return (axum::http::StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"code": -1000})));
                    }
                    match fake.orders.lock().unwrap().get(&query["origClientOrderId"]).cloned() {
                        Some(order) => (axum::http::StatusCode::OK, Json(serde_json::to_value(order).unwrap())),
                        None => (axum::http::StatusCode::BAD_REQUEST, Json(serde_json::json!({"code": -2013, "msg": "Order does not exist"}))),
                    }
                })
                .delete(|State(fake): State<PairExchange>, Query(query): Query<HashMap<String, String>>| async move {
                    let id: i64 = query["orderId"].parse().unwrap();
                    let mut orders = fake.orders.lock().unwrap();
                    let order = orders.values_mut().find(|order| order.order_id == id).unwrap();
                    order.status = "CANCELED".into();
                    Json(order.clone())
                })
                .post(|State(fake): State<PairExchange>, Query(query): Query<HashMap<String, String>>| async move {
                    let id = query["newClientOrderId"].clone();
                    fake.posts.lock().unwrap().push(id.clone());
                    let order = BinanceOrderResponse {
                        order_id: 500,
                        client_order_id: id.clone(),
                        symbol: query["symbol"].clone(),
                        status: "NEW".into(),
                        price: query["price"].parse().unwrap(),
                        avg_price: None,
                        orig_qty: query["quantity"].parse().unwrap(),
                        executed_qty: dec!(0),
                        side: query["side"].clone(),
                        order_type: "LIMIT".into(),
                        time_in_force: "GTX".into(),
                        update_time: None,
                    };
                    fake.orders.lock().unwrap().insert(id, order.clone());
                    if consume_failure(&fake.lose_response) {
                        (axum::http::StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"code": -1000})))
                    } else {
                        (axum::http::StatusCode::OK, Json(serde_json::to_value(order).unwrap()))
                    }
                }),
            )
            .with_state(fake);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (url, server)
    }

    fn pending_pair_fixture() -> (AppConfig, Arc<Database>, GridOrder) {
        let mut config = AppConfig::default();
        config.exchange.dry_run = false;
        config.grid.grid_interval = dec!(1);
        let db = Arc::new(Database::open(":memory:").unwrap());
        db.save_bot_status(BotStatus::Running).unwrap();
        let parent = GridOrder {
            client_order_id: "gb_b_recovery_parent".into(),
            order_id: Some(42),
            symbol: "SOLUSDC".into(),
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(1),
            amount_usdc: dec!(100),
            status: OrderStatus::Filled,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: -1,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Legacy,
            merge_sources: Vec::new(),
        };
        let intent = new_pair_intent(&parent, OrderSide::Sell, dec!(101), dec!(1));
        let trade = TradeRecord {
            trade_id: format!("grid:{}", parent.client_order_id),
            client_order_id: parent.client_order_id,
            symbol: parent.symbol,
            mode: TradingMode::Live,
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(1),
            amount_usdc: dec!(100),
            realized_pnl: dec!(0),
            commission: dec!(0),
            pnl_verified: true,
            is_maker: true,
            timestamp: Utc::now(),
            note: "fill".into(),
        };
        db.insert_trade_with_recovery(
            &trade,
            &GridStats {
                total_trades: 1,
                ..GridStats::default()
            },
            Some(&intent),
            None,
        )
        .unwrap();
        (config, db, intent)
    }

    async fn pair_recovery_engine(
        config: &AppConfig,
        db: Arc<Database>,
        url: String,
    ) -> GridTradingEngine {
        let (action_tx, action_rx) = mpsc::channel(1);
        let (_ticker_tx, ticker_rx) = broadcast::channel(1);
        let state = AppState::new(config.clone(), db, action_tx);
        {
            let mut ticker = state.ticker.write().await;
            ticker.last_price = dec!(100);
            ticker.update_time = Utc::now();
            ticker.mark_update_time = Utc::now();
        }
        state.position.write().await.size = dec!(1);
        let client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
        let mut engine = GridTradingEngine::new(state, client, action_rx, ticker_rx);
        engine.last_account_sync = Some(Instant::now());
        engine.last_orders_sync = Some(Instant::now());
        engine
    }

    #[tokio::test]
    async fn cancellation_continues_when_managed_metadata_cannot_be_read() {
        let (config, _, mut order) = pending_pair_fixture();
        let path = std::env::temp_dir().join(format!(
            "grid-cancel-fault-{}-{}.db",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap()
        ));
        let db = Arc::new(Database::open(&path).unwrap());
        db.save_bot_status(BotStatus::Running).unwrap();
        order.client_order_id = "gb_b_partial_fault".into();
        order.order_id = Some(777);
        db.save_managed_order(&order).unwrap();
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute(
            "UPDATE managed_orders SET order_json = 'invalid' WHERE client_order_id = ?1",
            [&order.client_order_id],
        )
        .unwrap();
        let fake = PairExchange::default();
        let exchange_order = BinanceOrderResponse {
            order_id: 777,
            client_order_id: order.client_order_id.clone(),
            symbol: order.symbol.clone(),
            status: "PARTIALLY_FILLED".into(),
            price: order.price,
            avg_price: Some(order.price),
            orig_qty: order.quantity,
            executed_qty: dec!(0.5),
            side: order.side.as_str().into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        };
        fake.orders
            .lock()
            .unwrap()
            .insert(order.client_order_id.clone(), exchange_order.clone());
        let mut manual = exchange_order;
        manual.order_id = 778;
        manual.client_order_id = "manual_order".into();
        fake.orders
            .lock()
            .unwrap()
            .insert(manual.client_order_id.clone(), manual);
        let (url, server) = pair_exchange_server(fake.clone()).await;
        let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(order.client_order_id.clone(), order.clone());

        assert!(!engine.cancel_all_orders().await);
        assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
        assert!(engine
            .state
            .active_orders
            .read()
            .await
            .contains_key(&order.client_order_id));
        assert_eq!(
            fake.orders.lock().unwrap()[&order.client_order_id].status,
            "CANCELED"
        );
        assert_eq!(
            fake.orders.lock().unwrap()["manual_order"].status,
            "PARTIALLY_FILLED"
        );
        let json: String = conn
            .query_row(
                "SELECT order_json FROM managed_orders WHERE client_order_id = ?1",
                [&order.client_order_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(json, "invalid");
        assert!(db
            .get_trade_by_client_id(&order.client_order_id, TradingMode::Live)
            .unwrap()
            .is_none());
        server.abort();
        drop(engine);
        drop(db);
        drop(conn);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn cancellation_continues_when_exchange_listing_fails() {
        let (config, _, mut order) = pending_pair_fixture();
        let db = Arc::new(Database::open(":memory:").unwrap());
        db.save_bot_status(BotStatus::Running).unwrap();
        order.client_order_id = "gb_b_listing_fault".into();
        order.order_id = Some(779);
        db.save_managed_order(&order).unwrap();
        let fake = PairExchange::default();
        fake.fail_open_orders.store(100, Ordering::SeqCst);
        fake.orders.lock().unwrap().insert(
            order.client_order_id.clone(),
            BinanceOrderResponse {
                order_id: 779,
                client_order_id: order.client_order_id.clone(),
                symbol: order.symbol.clone(),
                status: "PARTIALLY_FILLED".into(),
                price: order.price,
                avg_price: Some(order.price),
                orig_qty: order.quantity,
                executed_qty: dec!(0.5),
                side: order.side.as_str().into(),
                order_type: "LIMIT".into(),
                time_in_force: "GTX".into(),
                update_time: None,
            },
        );
        let (url, server) = pair_exchange_server(fake.clone()).await;
        let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
        assert!(!engine.cancel_all_orders().await);
        assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
        assert_eq!(
            fake.orders.lock().unwrap()[&order.client_order_id].status,
            "CANCELED"
        );
        assert!(engine
            .state
            .active_orders
            .read()
            .await
            .contains_key(&order.client_order_id));
        assert!(db
            .load_managed_orders(&order.symbol)
            .unwrap()
            .iter()
            .any(|saved| saved.client_order_id == order.client_order_id));
        assert!(db
            .get_trade_by_client_id(&order.client_order_id, TradingMode::Live)
            .unwrap()
            .is_none());
        server.abort();
    }

    #[tokio::test]
    async fn failed_initialization_restores_orders_before_initial_cancellation() {
        let (config, db, mut order) = pending_pair_fixture();
        order.client_order_id = "gb_b_startup_unknown".into();
        order.order_id = None;
        order.created_at = Utc::now();
        db.save_managed_order(&order).unwrap();
        let (url, server) = pair_exchange_server(PairExchange::default()).await;
        let mut engine = pair_recovery_engine(&config, db.clone(), url).await;

        assert!(engine.initialize().await.is_err());
        assert!(engine
            .state
            .active_orders
            .read()
            .await
            .contains_key(&order.client_order_id));
        assert!(!engine.cancel_all_orders().await);
        assert!(db
            .load_managed_orders("SOLUSDC")
            .unwrap()
            .iter()
            .any(|saved| saved.client_order_id == order.client_order_id));
        server.abort();
    }

    #[tokio::test]
    async fn cancellation_restores_orders_even_if_initial_recovery_was_skipped() {
        let (config, db, mut order) = pending_pair_fixture();
        order.client_order_id = "gb_b_cancel_unknown".into();
        order.order_id = None;
        order.created_at = Utc::now();
        db.save_managed_order(&order).unwrap();
        let (url, server) = pair_exchange_server(PairExchange::default()).await;
        let mut engine = pair_recovery_engine(&config, db.clone(), url).await;

        assert!(!engine.cancel_all_orders().await);
        assert!(engine
            .state
            .active_orders
            .read()
            .await
            .contains_key(&order.client_order_id));
        assert!(db
            .load_managed_orders("SOLUSDC")
            .unwrap()
            .iter()
            .any(|saved| saved.client_order_id == order.client_order_id));
        server.abort();
    }

    #[tokio::test]
    async fn unresolved_order_keeps_confirmed_fill_behind_reconciliation_barrier() {
        let (config, db, mut unresolved) = pending_pair_fixture();
        unresolved.client_order_id = "gb_b_unknown_frontier".into();
        unresolved.order_id = None;
        unresolved.created_at = Utc::now();
        let mut filled = unresolved.clone();
        filled.client_order_id = "gb_b_confirmed_frontier".into();
        db.save_managed_order(&unresolved).unwrap();
        db.save_managed_order(&filled).unwrap();
        let fake = PairExchange::default();
        fake.orders.lock().unwrap().insert(
            filled.client_order_id.clone(),
            BinanceOrderResponse {
                order_id: 777,
                client_order_id: filled.client_order_id.clone(),
                symbol: filled.symbol.clone(),
                status: "FILLED".into(),
                price: filled.price,
                avg_price: None,
                orig_qty: filled.quantity,
                executed_qty: filled.quantity,
                side: filled.side.as_str().into(),
                order_type: "LIMIT".into(),
                time_in_force: "GTX".into(),
                update_time: Some(Utc::now().timestamp_millis()),
            },
        );
        let (url, server) = pair_exchange_server(fake).await;
        let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
        {
            let mut active = engine.state.active_orders.write().await;
            active.insert(unresolved.client_order_id.clone(), unresolved);
            active.insert(filled.client_order_id.clone(), filled.clone());
        }
        assert!(!engine.sync_live_orders().await);
        assert!(engine.last_orders_sync.is_none());
        assert!(engine
            .state
            .active_orders
            .read()
            .await
            .contains_key(&filled.client_order_id));
        assert!(db
            .load_managed_orders("SOLUSDC")
            .unwrap()
            .iter()
            .any(|saved| saved.client_order_id == filled.client_order_id));
        server.abort();
    }

    #[test]
    fn small_price_moves_do_not_duplicate_grid_orders() {
        let existing = [dec!(113.13), dec!(112.13), dec!(111.13)];
        assert!(has_nearby_grid_order(
            &existing,
            dec!(113.15),
            dec!(1),
            dec!(0.01)
        ));
        assert!(has_nearby_grid_order(
            &existing,
            dec!(112.15),
            dec!(1),
            dec!(0.01)
        ));
        assert!(!has_nearby_grid_order(
            &existing,
            dec!(114.15),
            dec!(1),
            dec!(0.01)
        ));
    }

    #[test]
    fn managed_order_prefix_excludes_manual_orders() {
        assert!(is_grid_order("gb_b_123"));
        assert!(!is_grid_order("manual-123"));
    }

    #[test]
    fn paired_ids_are_stable_and_bounded() {
        let parent = "gb_b_123456789012345678901234567890";
        let sell = paired_grid_client_order_id(parent, OrderSide::Sell);
        assert_eq!(sell, paired_grid_client_order_id(parent, OrderSide::Sell));
        assert_ne!(sell, paired_grid_client_order_id(parent, OrderSide::Buy));
        assert!(sell.len() <= 36);
    }

    #[tokio::test]
    async fn order_reconciliation_failure_revokes_readiness_until_full_recovery() {
        let (config, db, _intent) = pending_pair_fixture();
        let fake = PairExchange::default();
        let (url, server) = pair_exchange_server(fake).await;
        let mut engine = pair_recovery_engine(&config, db, url).await;
        let state = engine.state.clone();
        state
            .global_position_initialized
            .store(true, std::sync::atomic::Ordering::SeqCst);
        state
            .global_position_ready
            .store(true, std::sync::atomic::Ordering::SeqCst);
        engine.report_reconciliation_state(true).await;
        assert!(!state
            .global_position_ready
            .load(std::sync::atomic::Ordering::SeqCst));
        engine.report_reconciliation_state(false).await;
        assert!(!state
            .global_position_ready
            .load(std::sync::atomic::Ordering::SeqCst));
        engine.set_global_reconciliation_ready(true).await;
        assert!(state
            .global_position_ready
            .load(std::sync::atomic::Ordering::SeqCst));
        server.abort();
    }

    #[tokio::test]
    async fn accepted_legacy_pair_is_adopted_after_restart_without_resubmission() {
        let (config, db, intent) = pending_pair_fixture();
        let fake = PairExchange::default();
        fake.orders.lock().unwrap().insert(
            intent.client_order_id.clone(),
            BinanceOrderResponse {
                order_id: 501,
                client_order_id: intent.client_order_id.clone(),
                symbol: intent.symbol.clone(),
                status: "NEW".into(),
                price: intent.price,
                avg_price: None,
                orig_qty: intent.quantity,
                executed_qty: Decimal::ZERO,
                side: "SELL".into(),
                order_type: "LIMIT".into(),
                time_in_force: "GTX".into(),
                update_time: None,
            },
        );
        let (url, server) = pair_exchange_server(fake.clone()).await;
        let mut restarted = pair_recovery_engine(&config, db.clone(), url).await;
        assert!(restarted.sync_live_orders().await);
        assert!(fake.posts.lock().unwrap().is_empty());
        assert!(db
            .load_pair_intents("SOLUSDC", TradingMode::Live)
            .unwrap()
            .is_empty());
        let stored = db.load_managed_orders("SOLUSDC").unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(
            stored[0].paired_client_order_id,
            intent.paired_client_order_id
        );
        server.abort();
    }

    #[tokio::test]
    async fn uncertain_pair_lookup_blocks_other_orders_until_resolved() {
        let (config, db, _intent) = pending_pair_fixture();
        let fake = PairExchange::default();
        fake.fail_lookup.store(1, Ordering::SeqCst);
        let (url, server) = pair_exchange_server(fake.clone()).await;
        let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
        assert!(
            !engine
                .drain_pair_intents("SOLUSDC", TradingMode::Live)
                .await
        );
        engine.maintain_grid_window().await;
        assert!(fake.posts.lock().unwrap().is_empty());
        assert_eq!(
            db.load_pair_intents("SOLUSDC", TradingMode::Live)
                .unwrap()
                .len(),
            1
        );

        assert!(
            engine
                .drain_pair_intents("SOLUSDC", TradingMode::Live)
                .await
        );
        assert!(fake.posts.lock().unwrap().is_empty());
        assert!(db
            .load_pair_intents("SOLUSDC", TradingMode::Live)
            .unwrap()
            .is_empty());
        server.abort();
    }

    #[tokio::test]
    async fn skipped_counter_order_does_not_leave_a_historical_lock() {
        let (mut config, db, intent) = pending_pair_fixture();
        config.grid.grid_interval = dec!(1);
        let fake = PairExchange::default();
        let (url, server) = pair_exchange_server(fake.clone()).await;
        let mut engine = pair_recovery_engine(&config, db.clone(), url.clone()).await;
        engine.state.ticker.write().await.last_price = dec!(102);
        assert!(
            engine
                .drain_pair_intents("SOLUSDC", TradingMode::Live)
                .await
        );
        assert!(db
            .load_pair_intents("SOLUSDC", TradingMode::Live)
            .unwrap()
            .is_empty());
        assert!(fake.posts.lock().unwrap().is_empty());
        assert!(
            engine
                .place_grid_order(OrderSide::Buy, dec!(98), "gb_b_duplicate".into(), -1, None)
                .await
        );
        drop(engine);
        let mut restarted = pair_recovery_engine(&config, db.clone(), url).await;
        restarted.state.ticker.write().await.last_price = dec!(100.5);
        assert!(restarted.sync_live_orders().await);
        assert!(
            !restarted
                .place_grid_order(
                    OrderSide::Buy,
                    dec!(100),
                    "gb_b_after_restart".into(),
                    -1,
                    None
                )
                .await
        );
        let mut rebuy = new_pair_intent(&intent, OrderSide::Buy, dec!(100), dec!(1));
        rebuy.symbol = "SOLUSDC".into();
        assert!(matches!(
            restarted.pair_submission_decision(&rebuy).await,
            super::PairPlacementDecision::Skip(_)
        ));
        assert_eq!(fake.posts.lock().unwrap().len(), 1);
        server.abort();
    }

    #[tokio::test]
    async fn counter_sells_for_different_buys_cannot_duplicate_the_same_level() {
        let (config, db, intent) = pending_pair_fixture();
        let fake = PairExchange::default();
        let (url, server) = pair_exchange_server(fake.clone()).await;
        let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
        engine.state.position.write().await.size = dec!(10);
        let mut existing = intent.clone();
        existing.client_order_id = "gb_s_existing_exit".into();
        existing.paired_client_order_id = Some("gb_b_existing_parent".into());
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(existing.client_order_id.clone(), existing.clone());
        assert!(
            engine
                .drain_pair_intents("SOLUSDC", TradingMode::Live)
                .await
        );
        assert!(db
            .load_pair_intents("SOLUSDC", TradingMode::Live)
            .unwrap()
            .is_empty());
        let active = engine.state.active_orders.read().await;
        assert_eq!(active.len(), 1);
        assert!(active
            .values()
            .all(|o| o.price == intent.price && !o.is_take_profit));
        assert!(reserved_sell_quantity(&active.values().cloned().collect::<Vec<_>>()) <= dec!(10));
        drop(active);
        // Draining the same journal again must not submit either exit twice.
        assert!(
            engine
                .drain_pair_intents("SOLUSDC", TradingMode::Live)
                .await
        );
        assert!(fake.posts.lock().unwrap().is_empty());
        server.abort();
    }

    #[tokio::test]
    async fn cancel_all_clears_absent_pair_without_submitting_it() {
        let (config, db, _) = pending_pair_fixture();
        let fake = PairExchange::default();
        let (url, server) = pair_exchange_server(fake.clone()).await;
        let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
        assert!(engine.cancel_all_orders().await);
        assert!(db
            .load_pair_intents("SOLUSDC", TradingMode::Live)
            .unwrap()
            .is_empty());
        assert!(fake.posts.lock().unwrap().is_empty());
        server.abort();
    }

    #[tokio::test]
    async fn terminal_pair_fill_after_restart_keeps_cycle_metadata() {
        let (config, db, intent) = pending_pair_fixture();
        let fake = PairExchange::default();
        fake.orders.lock().unwrap().insert(
            intent.client_order_id.clone(),
            BinanceOrderResponse {
                order_id: 501,
                client_order_id: intent.client_order_id.clone(),
                symbol: intent.symbol.clone(),
                status: "FILLED".into(),
                price: intent.price,
                avg_price: Some(intent.price),
                orig_qty: intent.quantity,
                executed_qty: intent.quantity,
                side: "SELL".into(),
                order_type: "LIMIT".into(),
                time_in_force: "GTX".into(),
                update_time: None,
            },
        );
        let (url, server) = pair_exchange_server(fake.clone()).await;
        let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
        engine.state.pause_trading().await.unwrap();
        assert!(
            engine
                .drain_pair_intents("SOLUSDC", TradingMode::Live)
                .await
        );
        assert!(fake.posts.lock().unwrap().is_empty());
        assert!(db
            .load_pair_intents("SOLUSDC", TradingMode::Live)
            .unwrap()
            .is_empty());
        let trade = db
            .get_trade_by_client_id(&intent.client_order_id, TradingMode::Live)
            .unwrap()
            .unwrap();
        assert_eq!(trade.side, OrderSide::Sell);
        assert_eq!(
            db.load_scoped_stats("SOLUSDC", TradingMode::Live)
                .unwrap()
                .unwrap()
                .1,
            1
        );
        server.abort();
    }

    #[tokio::test]
    async fn live_buy_restarts_with_last_price_blocked_and_only_ordinary_window_orders() {
        let (mut config, db, _) = pending_pair_fixture();
        db.clear_pair_intents("SOLUSDC", TradingMode::Live).unwrap();
        config.grid.order_amount_usdc = dec!(2000);
        let fake = PairExchange::default();
        let (url, server) = pair_exchange_server(fake.clone()).await;
        let mut engine = pair_recovery_engine(&config, db.clone(), url.clone()).await;
        engine.state.ticker.write().await.last_price = dec!(114.5);
        let mut buy = new_pair_intent(
            &GridOrder {
                client_order_id: "gb_b_parent".into(),
                order_id: None,
                symbol: "SOLUSDC".into(),
                side: OrderSide::Buy,
                price: dec!(114),
                quantity: dec!(17.54),
                amount_usdc: dec!(1999.56),
                status: OrderStatus::New,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                grid_level: -1,
                paired_client_order_id: None,
                is_take_profit: false,
                purpose: OrderPurpose::Grid,
                merge_sources: vec![],
            },
            OrderSide::Buy,
            dec!(114),
            dec!(17.54),
        );
        buy.paired_client_order_id = None;
        assert!(engine.on_order_filled(&mut buy).await);
        assert!(db
            .load_pair_intents("SOLUSDC", TradingMode::Live)
            .unwrap()
            .is_empty());
        assert!(fake.posts.lock().unwrap().is_empty());
        drop(engine);
        let mut restarted = pair_recovery_engine(&config, db.clone(), url).await;
        restarted.state.ticker.write().await.last_price = dec!(114.5);
        restarted.state.position.write().await.size = buy.quantity;
        assert!(
            !restarted
                .place_grid_order(OrderSide::Buy, dec!(114), "gb_b_repeat".into(), -1, None)
                .await
        );
        restarted.maintain_grid_window().await;
        let active = restarted.state.active_orders.read().await;
        let sell = active.values().find(|o| o.side == OrderSide::Sell).unwrap();
        assert_eq!(sell.price, dec!(115));
        assert_eq!(sell.quantity, dec!(17.39));
        assert!(sell.paired_client_order_id.is_none());
        server.abort();
    }

    #[tokio::test]
    async fn existing_destination_order_keeps_its_identity_without_pairing() {
        let (config, db, mut existing) = pending_pair_fixture();
        db.clear_pair_intents("SOLUSDC", TradingMode::Live).unwrap();
        existing.client_order_id = "gb_s_existing_destination".into();
        existing.order_id = Some(500);
        existing.paired_client_order_id = None;
        db.save_managed_order(&existing).unwrap();
        let fake = PairExchange::default();
        fake.orders.lock().unwrap().insert(
            existing.client_order_id.clone(),
            BinanceOrderResponse {
                order_id: 500,
                client_order_id: existing.client_order_id.clone(),
                symbol: "SOLUSDC".into(),
                status: "NEW".into(),
                price: dec!(101),
                avg_price: None,
                orig_qty: dec!(1),
                executed_qty: dec!(0),
                side: "SELL".into(),
                order_type: "LIMIT".into(),
                time_in_force: "GTX".into(),
                update_time: None,
            },
        );
        let (url, server) = pair_exchange_server(fake.clone()).await;
        let mut engine = pair_recovery_engine(&config, db.clone(), url.clone()).await;
        engine.state.ticker.write().await.last_price = dec!(100.5);
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(existing.client_order_id.clone(), existing.clone());
        let mut buy = existing.clone();
        buy.client_order_id = "gb_b_destination_parent".into();
        buy.order_id = None;
        buy.side = OrderSide::Buy;
        buy.price = dec!(100);
        buy.quantity = dec!(0.46);
        buy.amount_usdc = buy.price * buy.quantity;
        assert!(engine.on_order_filled(&mut buy).await);
        let intents = db.load_pair_intents("SOLUSDC", TradingMode::Live).unwrap();
        assert!(intents.is_empty());
        drop(engine);
        let mut restarted = pair_recovery_engine(&config, db.clone(), url).await;
        restarted.state.ticker.write().await.last_price = dec!(100.5);
        assert!(restarted.sync_live_orders().await);
        assert!(fake.posts.lock().unwrap().is_empty());
        assert!(db
            .load_pair_intents("SOLUSDC", TradingMode::Live)
            .unwrap()
            .is_empty());
        let managed = db.load_managed_orders("SOLUSDC").unwrap();
        assert_eq!(managed.len(), 1);
        assert_eq!(managed[0].paired_client_order_id.as_deref(), None);
        assert!(
            !restarted
                .place_grid_order(OrderSide::Buy, dec!(100), "gb_b_repeat".into(), -1, None)
                .await
        );
        server.abort();
    }

    #[tokio::test]
    async fn already_recorded_terminal_counter_order_retires_its_pending_intent() {
        let (config, db, intent) = pending_pair_fixture();
        db.insert_trade(&TradeRecord {
            trade_id: format!("grid:{}", intent.client_order_id),
            client_order_id: intent.client_order_id.clone(),
            symbol: intent.symbol.clone(),
            mode: TradingMode::Live,
            side: OrderSide::Sell,
            price: intent.price,
            quantity: intent.quantity,
            amount_usdc: intent.amount_usdc,
            realized_pnl: dec!(1),
            commission: dec!(0),
            pnl_verified: true,
            is_maker: true,
            timestamp: Utc::now(),
            note: "already reconciled".into(),
        })
        .unwrap();
        let fake = PairExchange::default();
        fake.orders.lock().unwrap().insert(
            intent.client_order_id.clone(),
            BinanceOrderResponse {
                order_id: 500,
                client_order_id: intent.client_order_id.clone(),
                symbol: intent.symbol.clone(),
                status: "FILLED".into(),
                price: intent.price,
                avg_price: Some(intent.price),
                orig_qty: intent.quantity,
                executed_qty: intent.quantity,
                side: "SELL".into(),
                order_type: "LIMIT".into(),
                time_in_force: "GTX".into(),
                update_time: None,
            },
        );
        let (url, server) = pair_exchange_server(fake.clone()).await;
        let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
        assert!(
            engine
                .drain_pair_intents("SOLUSDC", TradingMode::Live)
                .await
        );
        assert!(db
            .load_pair_intents("SOLUSDC", TradingMode::Live)
            .unwrap()
            .is_empty());
        assert!(fake.posts.lock().unwrap().is_empty());
        assert_eq!(db.get_recent_trades(10).unwrap().len(), 2);
        server.abort();
    }

    #[tokio::test]
    async fn repeated_live_fill_records_one_trade_without_a_pair_intent() {
        let (config, db, _) = pending_pair_fixture();
        db.clear_pair_intents("SOLUSDC", TradingMode::Live).unwrap();
        let fake = PairExchange::default();
        let (url, server) = pair_exchange_server(fake.clone()).await;
        let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
        engine.state.ticker.write().await.last_price = dec!(102.1);
        let price = dec!(102);
        let quantity = engine
            .state
            .rules
            .read()
            .await
            .calculate_quantity(price, config.grid.order_amount_usdc)
            .unwrap();
        let mut order = GridOrder {
            client_order_id: "gb_s_replayed".into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side: OrderSide::Sell,
            price,
            quantity,
            amount_usdc: price * quantity,
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 1,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Legacy,
            merge_sources: Vec::new(),
        };
        assert!(engine.on_order_filled(&mut order).await);
        assert!(engine.on_order_filled(&mut order).await);
        assert!(fake.posts.lock().unwrap().is_empty());
        assert_eq!(
            db.get_recent_trades_for("SOLUSDC", TradingMode::Live, 10)
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            db.load_scoped_stats("SOLUSDC", TradingMode::Live)
                .unwrap()
                .unwrap()
                .0,
            2
        );
        let intents = db.load_pair_intents("SOLUSDC", TradingMode::Live).unwrap();
        assert!(intents.is_empty());
        server.abort();
    }

    #[tokio::test]
    async fn unresolved_order_blocks_replenishment() {
        let app = Router::new()
            .route(
                "/fapi/v1/openOrders",
                get(|| async { Json(Vec::<BinanceOrderResponse>::new()) }),
            )
            .route(
                "/fapi/v1/order",
                get(|Query(query): Query<HashMap<String, String>>| async move {
                    if query.get("orderId").is_some_and(|id| id == "43") {
                        let order = BinanceOrderResponse {
                            order_id: 43,
                            client_order_id: "gb_b_filled".into(),
                            symbol: "SOLUSDC".into(),
                            status: "FILLED".into(),
                            price: dec!(100),
                            avg_price: Some(dec!(100)),
                            orig_qty: dec!(1),
                            executed_qty: dec!(1),
                            side: "BUY".into(),
                            order_type: "LIMIT".into(),
                            time_in_force: "GTX".into(),
                            update_time: None,
                        };
                        (
                            axum::http::StatusCode::OK,
                            Json(serde_json::to_value(order).unwrap()),
                        )
                    } else {
                        (
                            axum::http::StatusCode::SERVICE_UNAVAILABLE,
                            Json(serde_json::json!({})),
                        )
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let mut config = AppConfig::default();
        config.exchange.dry_run = false;
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (action_tx, action_rx) = mpsc::channel(1);
        let (_ticker_tx, ticker_rx) = broadcast::channel(1);
        let state = AppState::new(config.clone(), db.clone(), action_tx);
        let order = GridOrder {
            client_order_id: "gb_b_unresolved".into(),
            order_id: Some(42),
            symbol: "SOLUSDC".into(),
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(1),
            amount_usdc: dec!(100),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: -1,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Legacy,
            merge_sources: Vec::new(),
        };
        let mut filled = order.clone();
        filled.client_order_id = "gb_b_filled".into();
        filled.order_id = Some(43);
        {
            let mut active = state.active_orders.write().await;
            active.insert(order.client_order_id.clone(), order);
            active.insert(filled.client_order_id.clone(), filled);
        }
        let client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
        let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);

        assert!(!engine.sync_live_orders().await);
        assert!(engine.last_orders_sync.is_none());
        assert_eq!(state.active_orders.read().await.len(), 2);
        assert!(db.get_recent_trades(10).unwrap().is_empty());
        server.abort();
    }

    #[tokio::test]
    async fn external_pause_is_not_overridden_by_internal_resume() {
        let config = AppConfig::default();
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (action_tx, action_rx) = mpsc::channel(1);
        let (_ticker_tx, ticker_rx) = broadcast::channel(1);
        let state = AppState::new(config.clone(), db.clone(), action_tx);
        let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
        let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
        let active_guard = state.active_orders.write().await;
        let cancellation = tokio::spawn(async move { engine.cancel_all_orders().await });

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while state.pause_epoch() == 0
                || db.load_bot_status().unwrap() != Some(BotStatus::Paused)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        state.pause_trading().await.unwrap();
        drop(active_guard);

        assert!(cancellation.await.unwrap());
        assert_eq!(*state.status.read().await, BotStatus::Paused);
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
    }

    #[tokio::test]
    async fn cancel_all_only_touches_managed_exchange_orders() {
        #[derive(Clone)]
        struct FakeExchange {
            fetches: Arc<AtomicUsize>,
            canceled: Arc<Mutex<Vec<String>>>,
        }
        let fake = FakeExchange {
            fetches: Arc::new(AtomicUsize::new(0)),
            canceled: Arc::new(Mutex::new(Vec::new())),
        };
        let app = Router::new()
            .route(
                "/fapi/v1/openOrders",
                get(|State(fake): State<FakeExchange>| async move {
                    let order = |id, client_id: &str| BinanceOrderResponse {
                        order_id: id,
                        client_order_id: client_id.into(),
                        symbol: "SOLUSDC".into(),
                        status: "NEW".into(),
                        price: dec!(100),
                        avg_price: None,
                        orig_qty: dec!(1),
                        executed_qty: dec!(0),
                        side: "BUY".into(),
                        order_type: "LIMIT".into(),
                        time_in_force: "GTX".into(),
                        update_time: None,
                    };
                    let orders = if fake.fetches.fetch_add(1, Ordering::SeqCst) == 0 {
                        vec![order(99, "manual-order"), order(42, "gb_b_owned")]
                    } else {
                        Vec::new()
                    };
                    Json(orders)
                }),
            )
            .route(
                "/fapi/v1/order",
                delete(
                    |State(fake): State<FakeExchange>,
                     Query(query): Query<HashMap<String, String>>| async move {
                        fake.canceled.lock().unwrap().push(query["orderId"].clone());
                        Json(serde_json::json!({}))
                    },
                ),
            )
            .with_state(fake.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let mut config = AppConfig::default();
        config.exchange.dry_run = false;
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (action_tx, action_rx) = mpsc::channel(1);
        let (_ticker_tx, ticker_rx) = broadcast::channel(1);
        let state = AppState::new(config.clone(), db, action_tx);
        let client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
        let mut engine = GridTradingEngine::new(state, client, action_rx, ticker_rx);

        assert!(engine.cancel_all_orders().await);
        assert_eq!(*fake.canceled.lock().unwrap(), vec!["42"]);
        server.abort();
    }

    #[tokio::test]
    async fn switching_live_to_paper_cancels_live_orders_before_commit() {
        #[derive(Clone)]
        struct FakeExchange {
            fetches: Arc<AtomicUsize>,
            cancellations: Arc<AtomicUsize>,
        }
        let fake = FakeExchange {
            fetches: Arc::new(AtomicUsize::new(0)),
            cancellations: Arc::new(AtomicUsize::new(0)),
        };
        let app = Router::new()
            .route("/fapi/v1/exchangeInfo", get(|| async {
                Json(serde_json::json!({"symbols":[{"symbol":"SOLUSDC","status":"TRADING","pricePrecision":2,"quantityPrecision":2,"filters":[{"filterType":"PRICE_FILTER","minPrice":"0.01","maxPrice":"100000","tickSize":"0.01"},{"filterType":"LOT_SIZE","minQty":"0.01","maxQty":"100000","stepSize":"0.01"},{"filterType":"MIN_NOTIONAL","notional":"5"}]}]}))
            }))
            .route("/fapi/v1/ticker/price", get(|| async {
                Json(serde_json::json!({"symbol":"SOLUSDC","price":"100","time":0}))
            }))
            .route("/fapi/v1/openOrders", get(|State(fake): State<FakeExchange>| async move {
                if fake.fetches.fetch_add(1, Ordering::SeqCst) == 0 {
                    Json(serde_json::json!([{"orderId":42,"clientOrderId":"gb_b_live","symbol":"SOLUSDC","status":"NEW","price":"99","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX","updateTime":0}]))
                } else {
                    Json(serde_json::json!([]))
                }
            }))
            .route("/fapi/v1/order", delete(|State(fake): State<FakeExchange>| async move {
                fake.cancellations.fetch_add(1, Ordering::SeqCst);
                Json(serde_json::json!({}))
            }))
            .with_state(fake.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let mut config = AppConfig::default();
        config.exchange.dry_run = false;
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (action_tx, action_rx) = mpsc::channel(1);
        let (_ticker_tx, ticker_rx) = broadcast::channel(1);
        let state = AppState::new(config.clone(), db, action_tx);
        let client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
        let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
        let mut paper = config;
        paper.exchange.dry_run = true;

        engine.apply_config(paper).await.unwrap();
        assert_eq!(fake.cancellations.load(Ordering::SeqCst), 1);
        assert!(state.config.read().await.exchange.dry_run);
        server.abort();
    }

    #[tokio::test]
    async fn buy_window_respects_position_and_open_buy_exposure() {
        let mut config = AppConfig::default();
        config.grid.grid_interval = dec!(1);
        config.grid.order_amount_usdc = dec!(100);
        config.grid.buy_window = 3;
        config.grid.sell_window = 1;
        config.grid.max_position_usdc = Some(dec!(150));
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (action_tx, action_rx) = mpsc::channel(1);
        let (_ticker_tx, ticker_rx) = broadcast::channel(1);
        let state = AppState::new(config.clone(), db, action_tx);
        let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
        let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
        {
            let mut ticker = state.ticker.write().await;
            ticker.last_price = dec!(100);
            ticker.update_time = Utc::now();
            ticker.mark_update_time = Utc::now();
        }
        state.position.write().await.size = dec!(0.5);

        engine.maintain_grid_window().await;
        let orders: Vec<_> = state.active_orders.read().await.values().cloned().collect();
        assert_eq!(
            orders
                .iter()
                .filter(|order| order.side == OrderSide::Buy)
                .count(),
            1
        );
        assert!(buy_exposure_usdc(dec!(0.5), dec!(100), &orders) <= dec!(150));

        engine.handle_control_action(BotControlAction::Pause).await;
        assert_eq!(*state.status.read().await, BotStatus::Paused);
        assert!(state.active_orders.read().await.is_empty());
    }

    #[test]
    fn exchange_fills_include_losses_and_fees() {
        let fills = vec![
            BinanceUserTrade {
                realized_pnl: dec!(1.20),
                commission: dec!(0.03),
                commission_asset: "USDC".into(),
                maker: true,
            },
            BinanceUserTrade {
                realized_pnl: dec!(-2.00),
                commission: dec!(0.04),
                commission_asset: "USDC".into(),
                maker: false,
            },
        ];
        assert_eq!(
            aggregate_execution_pnl("SOLUSDC", &fills).unwrap(),
            (dec!(-0.80), dec!(0.07), false)
        );
        let mut wrong_asset = fills;
        wrong_asset[0].commission_asset = "BNB".into();
        assert!(aggregate_execution_pnl("SOLUSDC", &wrong_asset).is_err());
        assert!(aggregate_execution_pnl("SOLUSDC", &[]).is_err());
    }

    #[tokio::test]
    async fn unpaired_paper_sell_records_realized_pnl() {
        let config = AppConfig::default();
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (action_tx, action_rx) = mpsc::channel(1);
        let (_ticker_tx, ticker_rx) = broadcast::channel(1);
        let state = AppState::new(config.clone(), db, action_tx);
        let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
        let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
        state.position.write().await.size = dec!(2.63);
        state.position.write().await.entry_price = dec!(114);
        {
            let mut ticker = state.ticker.write().await;
            ticker.last_price = dec!(115);
            ticker.update_time = Utc::now();
            ticker.mark_update_time = Utc::now();
        }
        let mut order = GridOrder {
            client_order_id: "paper-sell".into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side: OrderSide::Sell,
            price: dec!(115.1),
            quantity: dec!(1),
            amount_usdc: dec!(115.1),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 1,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Legacy,
            merge_sources: Vec::new(),
        };
        state
            .active_orders
            .write()
            .await
            .insert(order.client_order_id.clone(), order.clone());

        engine.on_order_filled(&mut order).await;
        let stats = state.stats.read().await;
        assert_eq!(stats.total_realized_pnl, dec!(1.1));
        assert_eq!(stats.completed_cycles, 0);
        drop(stats);
        let trades = state.db.get_recent_trades(10).unwrap();
        assert_eq!(trades[0].realized_pnl, dec!(1.1));
        assert!(trades[0].pnl_verified);
    }

    #[tokio::test]
    async fn sell_window_places_one_remainder_and_replaces_oversized_orders() {
        let mut config = AppConfig::default();
        config.grid.grid_interval = dec!(1);
        config.grid.order_amount_usdc = dec!(2000);
        config.grid.buy_window = 1;
        config.grid.sell_window = 3;
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (action_tx, action_rx) = mpsc::channel(1);
        let (_ticker_tx, ticker_rx) = broadcast::channel(1);
        let state = AppState::new(config.clone(), db, action_tx);
        let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
        let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
        {
            let mut ticker = state.ticker.write().await;
            ticker.last_price = dec!(115);
            ticker.update_time = Utc::now();
            ticker.mark_update_time = Utc::now();
        }
        state.position.write().await.size = dec!(19.67);
        engine.maintain_grid_window().await;
        let orders: Vec<_> = state.active_orders.read().await.values().cloned().collect();
        let sell = orders
            .iter()
            .filter(|o| o.side == OrderSide::Sell)
            .min_by_key(|o| o.price)
            .unwrap();
        let expected_qty = state
            .rules
            .read()
            .await
            .calculate_quantity(sell.price, dec!(2000))
            .unwrap();
        assert_eq!(sell.quantity, expected_qty);
        assert_eq!(
            orders.iter().filter(|o| o.side == OrderSide::Sell).count(),
            2
        );
        assert_eq!(reserved_sell_quantity(&orders), dec!(19.67));
        assert_eq!(sell_quantity_available(dec!(19.67), &orders), dec!(0));

        engine.maintain_grid_window().await;
        assert_eq!(state.active_orders.read().await.len(), orders.len());

        let mut oversized = sell.clone();
        oversized.quantity = dec!(20);
        oversized.amount_usdc = oversized.price * oversized.quantity;
        state
            .active_orders
            .write()
            .await
            .insert(oversized.client_order_id.clone(), oversized);

        engine.maintain_grid_window().await;
        let orders: Vec<_> = state.active_orders.read().await.values().cloned().collect();
        assert_eq!(reserved_sell_quantity(&orders), dec!(19.67));
        assert_eq!(
            orders.iter().filter(|o| o.side == OrderSide::Sell).count(),
            2
        );

        state.position.write().await.size = dec!(-1);
        engine.trim_closing_orders_to_position().await;
        let orders: Vec<_> = state.active_orders.read().await.values().cloned().collect();
        assert_eq!(reserved_sell_quantity(&orders), dec!(0));
    }

    #[tokio::test]
    async fn sell_remainder_respects_symbol_minimum_after_step_rounding() {
        let mut config = AppConfig::default();
        config.grid.grid_interval = dec!(1);
        config.grid.order_amount_usdc = dec!(2000);
        config.grid.buy_window = 0;
        config.grid.sell_window = 3;
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (action_tx, action_rx) = mpsc::channel(1);
        let (_ticker_tx, ticker_rx) = broadcast::channel(1);
        let state = AppState::new(config.clone(), db, action_tx);
        let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
        let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
        {
            let mut ticker = state.ticker.write().await;
            ticker.last_price = dec!(115);
            ticker.update_time = Utc::now();
            ticker.mark_update_time = Utc::now();
        }
        state.rules.write().await.min_notional = dec!(50);
        state.position.write().await.size = dec!(0.429);

        engine.maintain_grid_window().await;
        assert!(state.active_orders.read().await.is_empty());

        state.position.write().await.size = dec!(0.449);
        engine.maintain_grid_window().await;
        let orders: Vec<_> = state.active_orders.read().await.values().cloned().collect();
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].side, OrderSide::Sell);
        assert_eq!(orders[0].quantity, dec!(0.44));
        assert_eq!(orders[0].amount_usdc, dec!(51.04));

        engine.maintain_grid_window().await;
        assert_eq!(state.active_orders.read().await.len(), 1);
    }

    #[tokio::test]
    async fn sell_window_adds_remainder_after_existing_full_orders() {
        let mut config = AppConfig::default();
        config.grid.grid_interval = dec!(1.2);
        config.grid.order_amount_usdc = dec!(3000);
        config.grid.buy_window = 0;
        config.grid.sell_window = 3;
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (action_tx, action_rx) = mpsc::channel(1);
        let (_ticker_tx, ticker_rx) = broadcast::channel(1);
        let state = AppState::new(config.clone(), db, action_tx);
        let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
        let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
        {
            let mut ticker = state.ticker.write().await;
            ticker.last_price = dec!(122.15);
            ticker.update_time = Utc::now();
            ticker.mark_update_time = Utc::now();
        }
        state.position.write().await.size = dec!(50.45);

        let first = GridOrder {
            client_order_id: "gb_s_existing_1".into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side: OrderSide::Sell,
            price: dec!(122.4),
            quantity: dec!(24.50),
            amount_usdc: dec!(2998.8),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 1,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Legacy,
            merge_sources: Vec::new(),
        };
        let second = GridOrder {
            client_order_id: "gb_s_existing_2".into(),
            price: dec!(123.6),
            quantity: dec!(24.27),
            amount_usdc: dec!(2999.772),
            ..first.clone()
        };
        let mut active = state.active_orders.write().await;
        active.insert(first.client_order_id.clone(), first);
        active.insert(second.client_order_id.clone(), second);
        drop(active);

        engine.maintain_grid_window().await;
        let orders: Vec<_> = state.active_orders.read().await.values().cloned().collect();
        assert_eq!(orders.len(), 3);
        assert_eq!(reserved_sell_quantity(&orders), dec!(50.45));
        let remainder = orders.iter().find(|o| o.price == dec!(124.8)).unwrap();
        assert_eq!(remainder.quantity, dec!(1.68));
        assert_eq!(remainder.amount_usdc, dec!(209.664));
    }

    #[tokio::test]
    async fn fills_record_actual_quantities_without_creating_counter_orders() {
        let config = AppConfig::default();
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (tx, rx) = mpsc::channel(1);
        let (_, ticker_rx) = broadcast::channel(1);
        let state = AppState::new(config.clone(), db.clone(), tx);
        let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
        let mut engine = GridTradingEngine::new(state.clone(), client, rx, ticker_rx);
        state.position.write().await.size = dec!(20);
        for (id, side, price, quantity) in [
            ("gb_b_actual", OrderSide::Buy, dec!(114.24), dec!(17.50)),
            ("gb_s_actual", OrderSide::Sell, dec!(115.24), dec!(0.46)),
        ] {
            let mut order = GridOrder {
                client_order_id: id.into(),
                order_id: None,
                symbol: "SOLUSDC".into(),
                side,
                price,
                quantity,
                amount_usdc: price * quantity,
                status: OrderStatus::New,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                grid_level: 0,
                paired_client_order_id: None,
                is_take_profit: false,
                purpose: OrderPurpose::Grid,
                merge_sources: vec![],
            };
            assert!(engine.on_order_filled(&mut order).await);
            assert_eq!(
                db.get_trade_by_client_id(id, TradingMode::Paper)
                    .unwrap()
                    .unwrap()
                    .quantity,
                quantity
            );
            assert!(state.active_orders.read().await.is_empty());
        }
        assert_eq!(state.position.read().await.size, dec!(37.04));
        assert!(db
            .load_pair_intents("SOLUSDC", TradingMode::Paper)
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn ticker_update_keeps_independent_mark_price() {
        let config = AppConfig::default();
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (action_tx, action_rx) = mpsc::channel(1);
        let (_ticker_tx, ticker_rx) = broadcast::channel(1);
        let state = AppState::new(config.clone(), db, action_tx);
        let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
        let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
        let mark_updated_at = Utc::now();
        {
            let mut ticker = state.ticker.write().await;
            ticker.mark_price = dec!(114.3);
            ticker.mark_update_time = mark_updated_at;
        }

        engine
            .handle_ticker_update(TickerInfo {
                symbol: config.exchange.symbol,
                last_price: dec!(114.5),
                update_time: Utc::now(),
                ..Default::default()
            })
            .await;

        let ticker = state.ticker.read().await;
        assert_eq!(ticker.last_price, dec!(114.5));
        assert_eq!(ticker.mark_price, dec!(114.3));
        assert_eq!(ticker.mark_update_time, mark_updated_at);
    }

    #[tokio::test]
    async fn mark_update_preserves_ticker_and_does_not_fill_paper_orders() {
        let config = AppConfig::default();
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (action_tx, action_rx) = mpsc::channel(1);
        let (_, ticker_rx) = broadcast::channel(1);
        let state = AppState::new(config.clone(), db, action_tx);
        let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
        let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
        let now = Utc::now();
        {
            let mut ticker = state.ticker.write().await;
            ticker.last_price = dec!(100);
            ticker.high_24h = dec!(110);
            ticker.update_time = now;
        }
        *state.status.write().await = BotStatus::Running;
        state.active_orders.write().await.insert(
            "gb_b_test".into(),
            GridOrder {
                client_order_id: "gb_b_test".into(),
                order_id: None,
                symbol: config.exchange.symbol.clone(),
                side: OrderSide::Buy,
                price: dec!(99),
                quantity: dec!(1),
                amount_usdc: dec!(99),
                status: OrderStatus::New,
                created_at: now,
                updated_at: now,
                grid_level: -1,
                paired_client_order_id: None,
                is_take_profit: false,
                purpose: OrderPurpose::Grid,
                merge_sources: Vec::new(),
            },
        );
        {
            let mut position = state.position.write().await;
            position.size = dec!(1);
            position.entry_price = dec!(90);
        }
        engine
            .handle_ticker_update(TickerInfo {
                symbol: config.exchange.symbol.clone(),
                mark_price: dec!(102),
                mark_update_time: Utc::now(),
                ..Default::default()
            })
            .await;
        let ticker = state.ticker.read().await.clone();
        assert_eq!(ticker.last_price, dec!(100));
        assert_eq!(ticker.high_24h, dec!(110));
        assert_eq!(ticker.update_time, now);
        assert_eq!(ticker.mark_price, dec!(102));
        assert_eq!(state.position.read().await.unrealized_pnl, dec!(12));
        assert!(state.active_orders.read().await.contains_key("gb_b_test"));

        engine
            .handle_ticker_update(TickerInfo {
                symbol: config.exchange.symbol,
                mark_price: dec!(50),
                mark_update_time: now - chrono::Duration::seconds(1),
                ..Default::default()
            })
            .await;
        assert_eq!(state.ticker.read().await.mark_price, dec!(102));
    }

    #[tokio::test]
    async fn mark_rest_fallback_only_refreshes_missing_or_stale_values() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        let calls = Arc::new(AtomicUsize::new(0));
        let fail = Arc::new(AtomicBool::new(false));
        let app = Router::new().route(
            "/fapi/v1/premiumIndex",
            get({
                let calls = calls.clone();
                let fail = fail.clone();
                move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let failed = fail.load(Ordering::SeqCst);
                    async move {
                        if failed {
                            (
                                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                                Json(serde_json::json!({"code": -1000})),
                            )
                        } else {
                            (
                                axum::http::StatusCode::OK,
                                Json(serde_json::json!({"symbol": "SOLUSDC", "markPrice": "105"})),
                            )
                        }
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let config = AppConfig::default();
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (action_tx, action_rx) = mpsc::channel(1);
        let (_, ticker_rx) = broadcast::channel(1);
        let state = AppState::new(config.clone(), db, action_tx);
        let client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
        let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
        let symbol = &config.exchange.symbol;
        {
            let mut ticker = state.ticker.write().await;
            ticker.symbol = symbol.clone();
            ticker.last_price = dec!(100);
            ticker.update_time = Utc::now();
        }
        engine.refresh_mark_price(symbol, true).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(state.ticker.read().await.mark_price, dec!(105));
        engine.refresh_mark_price(symbol, true).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let stale = Utc::now() - chrono::Duration::seconds(16);
        state.ticker.write().await.mark_update_time = stale;
        fail.store(true, Ordering::SeqCst);
        engine.refresh_mark_price(symbol, true).await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let ticker = state.ticker.read().await;
        assert_eq!(ticker.mark_update_time, stale);
        assert!(!ticker.prices_are_fresh(Utc::now()));
        drop(ticker);
        fail.store(false, Ordering::SeqCst);
        engine.refresh_mark_price(symbol, true).await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(!state.ticker.read().await.prices_are_fresh(Utc::now()));
        engine.mark_retry.retry_at = Some(Instant::now() - std::time::Duration::from_secs(1));
        engine.refresh_mark_price(symbol, true).await;
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert!(state.ticker.read().await.prices_are_fresh(Utc::now()));

        state.ticker.write().await.mark_update_time = stale;
        fail.store(true, Ordering::SeqCst);
        engine.refresh_mark_price(symbol, true).await;
        assert!(!engine.mark_retry.ready());
        engine
            .handle_ticker_update(TickerInfo {
                symbol: symbol.clone(),
                mark_price: dec!(106),
                mark_update_time: Utc::now(),
                ..Default::default()
            })
            .await;
        assert!(engine.mark_retry.ready());
        assert!(state.ticker.read().await.prices_are_fresh(Utc::now()));
        let future = Utc::now() + chrono::Duration::seconds(30);
        state.ticker.write().await.mark_update_time = future;
        fail.store(false, Ordering::SeqCst);
        engine.refresh_mark_price(symbol, true).await;
        let ticker = state.ticker.read().await;
        assert_eq!(ticker.mark_price, dec!(106));
        assert_eq!(ticker.mark_update_time, future);
        assert!(!ticker.prices_are_fresh(Utc::now()));
        server.abort();
    }

    #[tokio::test]
    async fn failed_ticker_fallback_backs_off_and_recovers_on_push_or_retry() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let calls = Arc::new(AtomicUsize::new(0));
        let available = Arc::new(AtomicBool::new(false));
        let app = Router::new()
            .route(
                "/fapi/v1/ticker/24hr",
                get({
                    let calls = calls.clone();
                    move || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        async { axum::http::StatusCode::SERVICE_UNAVAILABLE }
                    }
                }),
            )
            .route(
                "/fapi/v1/ticker/price",
                get({
                    let calls = calls.clone();
                    let available = available.clone();
                    move || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        let available = available.load(Ordering::SeqCst);
                        async move {
                            if available {
                                (
                                    axum::http::StatusCode::OK,
                                    Json(serde_json::json!({"symbol": "SOLUSDC", "price": "101", "time": 1})),
                                )
                            } else {
                                (
                                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                                    Json(serde_json::json!({})),
                                )
                            }
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let config = AppConfig::default();
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (action_tx, action_rx) = mpsc::channel(1);
        let (_, ticker_rx) = broadcast::channel(1);
        let state = AppState::new(config.clone(), db, action_tx);
        let client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
        let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
        let symbol = &config.exchange.symbol;

        engine.refresh_stale_ticker(symbol).await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        engine.refresh_stale_ticker(symbol).await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(!state.ticker.read().await.prices_are_fresh(Utc::now()));
        engine
            .handle_ticker_update(TickerInfo {
                symbol: symbol.clone(),
                last_price: dec!(100),
                update_time: Utc::now(),
                ..Default::default()
            })
            .await;
        assert!(engine.ticker_retry.ready());
        assert!(state.ticker.read().await.prices_are_fresh(Utc::now()));

        state.ticker.write().await.update_time = Utc::now() - chrono::Duration::seconds(16);
        engine.refresh_stale_ticker(symbol).await;
        assert!(!engine.ticker_retry.ready());
        available.store(true, Ordering::SeqCst);
        engine.ticker_retry.retry_at = Some(Instant::now() - std::time::Duration::from_secs(1));
        engine.refresh_stale_ticker(symbol).await;
        assert_eq!(calls.load(Ordering::SeqCst), 6);
        assert_eq!(state.ticker.read().await.last_price, dec!(101));
        assert!(state.ticker.read().await.prices_are_fresh(Utc::now()));
        server.abort();
    }

    #[tokio::test]
    async fn delayed_mark_rest_response_does_not_overwrite_a_newer_update() {
        let requested = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let app = Router::new().route(
            "/fapi/v1/premiumIndex",
            get({
                let requested = requested.clone();
                let release = release.clone();
                move || {
                    let requested = requested.clone();
                    let release = release.clone();
                    async move {
                        requested.notify_one();
                        release.notified().await;
                        Json(serde_json::json!({"symbol": "SOLUSDC", "markPrice": "90"}))
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let config = AppConfig::default();
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (action_tx, action_rx) = mpsc::channel(1);
        let (_, ticker_rx) = broadcast::channel(1);
        let state = AppState::new(config.clone(), db, action_tx);
        let client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
        let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
        let ((), ()) = tokio::join!(
            engine.refresh_mark_price(&config.exchange.symbol, true),
            async {
                requested.notified().await;
                let mut ticker = state.ticker.write().await;
                ticker.mark_price = dec!(110);
                ticker.mark_update_time = Utc::now();
                drop(ticker);
                release.notify_one();
            }
        );
        assert_eq!(state.ticker.read().await.mark_price, dec!(110));
        server.abort();
    }

    #[tokio::test]
    async fn queued_mark_event_from_during_rest_request_is_not_discarded() {
        let requested = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let app = Router::new().route(
            "/fapi/v1/premiumIndex",
            get({
                let requested = requested.clone();
                let release = release.clone();
                move || {
                    let requested = requested.clone();
                    let release = release.clone();
                    async move {
                        requested.notify_one();
                        release.notified().await;
                        Json(serde_json::json!({"symbol": "SOLUSDC", "markPrice": "90"}))
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let config = AppConfig::default();
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (action_tx, action_rx) = mpsc::channel(1);
        let (_, ticker_rx) = broadcast::channel(1);
        let state = AppState::new(config.clone(), db, action_tx);
        let client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
        let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
        let (_, event_time) = tokio::join!(
            engine.refresh_mark_price(&config.exchange.symbol, true),
            async {
                requested.notified().await;
                let event_time = Utc::now();
                release.notify_one();
                event_time
            }
        );
        engine
            .handle_ticker_update(TickerInfo {
                symbol: config.exchange.symbol,
                mark_price: dec!(110),
                mark_update_time: event_time,
                ..Default::default()
            })
            .await;
        assert_eq!(state.ticker.read().await.mark_price, dec!(110));
        server.abort();
    }

    #[test]
    fn grid_client_order_ids_fit_binance_limit() {
        for (side, prefix) in [(OrderSide::Buy, "gb_b_"), (OrderSide::Sell, "gb_s_")] {
            let first = new_grid_client_order_id(side);
            let second = new_grid_client_order_id(side);

            assert!(first.starts_with(prefix));
            assert_eq!(first.len(), 35);
            assert!(first
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_'));
            assert_ne!(first, second);
        }
    }
}

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
#[path = "short_tests.rs"]
mod short_tests;
