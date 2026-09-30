use crate::exchange::client::{BinanceFuturesClient, ExchangeError, NewOrderRequest};
use crate::exchange::{BinanceOrderResponse, BinanceUserTrade};
use crate::server::state::AppState;
use crate::strategy::precision::SymbolRules;
use crate::types::*;
use anyhow::{anyhow, Result};
use chrono::Utc;
use rust_decimal::Decimal;
#[cfg(test)]
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::interval;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

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

fn buy_exposure_usdc(position_size: Decimal, mark_price: Decimal, orders: &[GridOrder]) -> Decimal {
    position_size.max(Decimal::ZERO) * mark_price
        + orders
            .iter()
            .filter(|order| order.side == OrderSide::Buy)
            .map(|order| order.price * order.quantity)
            .sum::<Decimal>()
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

fn reserved_sell_quantity(orders: &[GridOrder]) -> Decimal {
    orders
        .iter()
        .filter(|o| o.side == OrderSide::Sell)
        .map(|o| o.quantity)
        .sum()
}

fn sell_quantity_available(position_size: Decimal, orders: &[GridOrder]) -> Decimal {
    (position_size.max(Decimal::ZERO) - reserved_sell_quantity(orders)).max(Decimal::ZERO)
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

pub struct GridTradingEngine {
    state: Arc<AppState>,
    client: Arc<BinanceFuturesClient>,
    action_rx: mpsc::Receiver<BotControlAction>,
    ticker_rx: broadcast::Receiver<TickerInfo>,
    pnl_reconcile_offset: usize,
    pnl_reconcile_task: Option<JoinHandle<()>>,
    last_account_sync: Option<Instant>,
    last_orders_sync: Option<Instant>,
    reconciliation_blocked: bool,
    last_remainder_change: Option<Instant>,
    market_stream_tx: Option<watch::Sender<(String, bool)>>,
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
            action_rx,
            ticker_rx,
            pnl_reconcile_offset: 0,
            pnl_reconcile_task: None,
            last_account_sync: None,
            last_orders_sync: None,
            reconciliation_blocked: false,
            last_remainder_change: None,
            market_stream_tx: None,
        }
    }

    pub fn set_market_stream_tx(&mut self, tx: watch::Sender<(String, bool)>) {
        self.market_stream_tx = Some(tx);
    }

    async fn pause_trading(&self) {
        if let Err(error) = self.state.pause_trading().await {
            error!("Could not persist paused bot status: {}", error);
        }
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
        // The most recent execution moves the frontier. An opposite fill
        // automatically makes the previous level available again.
        let buy_max = last_fill.map(|(_, price)| {
            (price - interval).min(config.grid.max_price.unwrap_or(rules.max_price))
        });
        let sell_min = last_fill.map(|(_, price)| {
            (price + interval).max(config.grid.min_price.unwrap_or(rules.min_price))
        });
        let (buys, _) = rules.grid_window_prices(
            market,
            interval,
            (config.grid.buy_window, 0),
            (config.grid.min_price, buy_max.or(config.grid.max_price)),
            |_, _| true,
        );
        let (_, sells) = rules.grid_window_prices(
            market,
            interval,
            (0, config.grid.sell_window),
            (sell_min.or(config.grid.min_price), config.grid.max_price),
            |_, _| true,
        );
        Ok((buys, sells))
    }

    async fn report_reconciliation_state(&mut self, blocked: bool) {
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

        // Synchronize server time
        if let Err(e) = self.client.sync_server_time().await {
            warn!("Failed to synchronize Binance server time: {}", e);
        }

        // Fetch symbol rules from exchangeInfo
        match self.client.get_exchange_info(Some(&symbol)).await {
            Ok(info) => {
                if let Some(rules) = SymbolRules::from_exchange_info(&info, &symbol) {
                    info!(
                        "Loaded Binance exchange rules for {}: tick_size={}, step_size={}, min_notional={}",
                        symbol, rules.tick_size, rules.step_size, rules.min_notional
                    );
                    *self.state.rules.write().await = rules;
                } else if !config.exchange.dry_run {
                    return Err(anyhow!("Missing Binance exchange rules for {}", symbol));
                }
            }
            Err(e) => {
                warn!(
                    "Could not fetch exchangeInfo for {}, using defaults: {}",
                    symbol, e
                );
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
        match self.client.get_ticker_price(&symbol).await {
            Ok(price) => {
                let mut ticker = self.state.ticker.write().await;
                ticker.symbol = symbol.clone();
                ticker.last_price = price;
                ticker.update_time = Utc::now();
                info!("Initial market price for {}: {}", symbol, price);
            }
            Err(e) => {
                warn!("Failed to fetch initial market price: {}", e);
                if !config.exchange.dry_run {
                    return Err(anyhow!("Could not load initial market price: {}", e));
                }
            }
        }

        match self.client.get_mark_price(&symbol).await {
            Ok(price) => {
                let mut ticker = self.state.ticker.write().await;
                ticker.mark_price = price;
                ticker.mark_update_time = Utc::now();
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

        // If not dry-run, load existing open orders
        if !config.exchange.dry_run {
            let restored = self
                .state
                .db
                .run_blocking(move |db| db.load_managed_orders(&symbol))
                .await?;
            {
                let mut active_map = self.state.active_orders.write().await;
                for order in restored
                    .into_iter()
                    .filter(|order| is_grid_order(&order.client_order_id))
                {
                    active_map.insert(order.client_order_id.clone(), order);
                }
            }
            if !self.sync_account_and_position().await || !self.sync_live_orders().await {
                return Err(anyhow!(
                    "Could not reconcile Binance account and managed orders at startup"
                ));
            }
        }

        Ok(())
    }

    /// Primary execution loop
    pub async fn run(&mut self) {
        let sync_secs = self
            .state
            .config
            .read()
            .await
            .exchange
            .sync_interval_secs
            .max(1);
        let mut sync_timer = interval(Duration::from_secs(sync_secs));
        let mut snapshot_timer = interval(Duration::from_millis(800));
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
                        Some(action) => self.handle_control_action(action).await,
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

                // WebSocket broadcast timer for frontend dashboard
                _ = snapshot_timer.tick() => {
                    self.broadcast_snapshot().await;
                }

                _ = pnl_timer.tick() => {
                    self.reconcile_unverified_trades().await;
                }
            }
        }
    }

    async fn handle_control_action(&mut self, action: BotControlAction) {
        match action {
            BotControlAction::Pause => {
                info!("Bot paused by user command");
                self.pause_trading().await;
                if self.cancel_all_orders().await {
                    self.state
                        .add_log("WARN", "Trading bot paused and managed orders canceled")
                        .await;
                } else {
                    self.state.add_log("ERROR", "Trading bot paused, but some managed orders may still be open on Binance").await;
                }
            }
            BotControlAction::Resume => {
                info!("Bot resumed by user command");
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
                self.pause_trading().await;
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
        }
    }

    async fn apply_config(&mut self, new_config: crate::config::AppConfig) -> Result<()> {
        new_config.validate()?;
        let old_config = self.state.config.read().await.clone();
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
            self.state
                .db
                .run_blocking(move |db| db.save_config(&saved))
                .await?;
            *self.state.config.write().await = new_config;
            self.state
                .add_log("SUCCESS", "Notification configuration updated")
                .await;
            return Ok(());
        }

        let new_client = if client_changed {
            Arc::new(BinanceFuturesClient::new(&new_config.exchange))
        } else {
            Arc::clone(&self.client)
        };
        if !new_config.exchange.dry_run {
            new_client.sync_server_time().await?;
            new_client
                .get_position(&new_config.exchange.symbol)
                .await?
                .ok_or_else(|| anyhow!("New symbol has no position information"))?;
            let account = new_client.get_account().await?;
            if account
                .margin_asset_for_symbol(&new_config.exchange.symbol)
                .is_none()
            {
                return Err(anyhow!("New symbol has no matching margin asset"));
            }
        }

        let new_rules = if market_changed || mode_changed {
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
        let saved = new_config.clone();
        if let Err(error) = self
            .state
            .db
            .run_blocking(move |db| db.save_config(&saved))
            .await
        {
            self.pause_trading().await;
            return Err(error);
        }

        self.client = new_client;
        *self.state.config.write().await = new_config.clone();
        if market_changed || mode_changed {
            *self.state.rules.write().await = new_rules;
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
                    asset: if new_config.exchange.symbol.ends_with("USDT") {
                        "USDT"
                    } else {
                        "USDC"
                    }
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
            self.state.refresh_current_scope().await;
        }

        let ready = if new_config.exchange.dry_run {
            true
        } else {
            self.sync_live_orders().await && self.sync_account_and_position().await
        };
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

        // Update state ticker
        {
            let mut ticker = self.state.ticker.write().await;
            ticker.last_price = ticker_update.last_price;
            if ticker_update.mark_price > Decimal::ZERO {
                ticker.mark_price = ticker_update.mark_price;
                ticker.mark_update_time = ticker_update.mark_update_time;
            }
            ticker.high_24h = ticker_update.high_24h;
            ticker.low_24h = ticker_update.low_24h;
            ticker.change_24h = ticker_update.change_24h;
            ticker.change_percent_24h = ticker_update.change_percent_24h;
            ticker.volume_24h = ticker_update.volume_24h;
            ticker.update_time = Utc::now();
        }

        // Update unrealized PnL for current position
        let is_dry_run = self.state.config.read().await.exchange.dry_run;
        if is_dry_run {
            let mark_price = self.state.ticker.read().await.mark_price;
            let valuation_price = if mark_price > Decimal::ZERO {
                mark_price
            } else {
                ticker_update.last_price
            };
            let mut pos = self.state.position.write().await;
            if !pos.size.is_zero() {
                pos.mark_price = valuation_price;
                pos.unrealized_pnl = (valuation_price - pos.entry_price) * pos.size;
            }
        }

        let is_running = *self.state.status.read().await == BotStatus::Running;

        // In Paper Trading / Dry Run mode, match simulated orders against live market price
        if is_running && is_dry_run {
            self.simulate_order_fills(ticker_update.last_price).await;
        }
    }

    /// Check simulated fills in Dry Run mode
    async fn simulate_order_fills(&mut self, current_price: Decimal) {
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
            self.on_order_filled(&mut order).await;
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
            let orders_ready = self.sync_live_orders().await;
            if !orders_ready {
                self.last_orders_sync = None;
            }
            if self.state.pause_epoch() != pause_epoch {
                return;
            }
            let account_ready = self.sync_account_and_position().await;
            orders_ready && account_ready
        } else {
            true
        };

        if self.state.pause_epoch() != pause_epoch {
            return;
        }

        self.trim_sell_orders_to_position().await;

        if *self.state.status.read().await == BotStatus::Running && account_ready {
            self.maintain_grid_window().await;
        }
    }

    async fn refresh_mark_price(&self, symbol: &str, is_dry_run: bool) {
        match self.client.get_mark_price(symbol).await {
            Ok(price) if price > Decimal::ZERO => {
                let mut ticker = self.state.ticker.write().await;
                ticker.mark_price = price;
                ticker.mark_update_time = Utc::now();
                drop(ticker);

                if is_dry_run {
                    let mut pos = self.state.position.write().await;
                    if !pos.size.is_zero() {
                        pos.mark_price = price;
                        pos.unrealized_pnl = (price - pos.entry_price) * pos.size;
                    }
                }
            }
            Ok(_) => warn!("Binance returned a zero mark price for {}", symbol),
            Err(e) => debug!("Could not refresh mark price for {}: {}", symbol, e),
        }
    }

    async fn refresh_stale_ticker(&mut self, symbol: &str) {
        let ticker = self.state.ticker.read().await.clone();
        if ticker.symbol == symbol && (Utc::now() - ticker.update_time).num_seconds() < 5 {
            return;
        }

        let mut update = ticker;
        update.symbol = symbol.to_string();
        match self.client.get_24hr_ticker(symbol).await {
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
                match self.client.get_ticker_price(symbol).await {
                    Ok(price) => update.last_price = price,
                    Err(e) => {
                        warn!("Could not refresh stale market price for {}: {}", symbol, e);
                        return;
                    }
                }
            }
        }
        if update.last_price <= Decimal::ZERO {
            return;
        }
        update.mark_price = Decimal::ZERO;
        update.update_time = Utc::now();
        self.handle_ticker_update(update).await;
    }

    /// Reconcile open orders from Binance in Live mode
    async fn sync_live_orders(&mut self) -> bool {
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
        match self.client.get_open_orders(&symbol).await {
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
                for order in filled_or_closed {
                    if self.state.pause_epoch() != pause_epoch {
                        return false;
                    }
                    let exchange_result = match order.order_id {
                        Some(order_id) => self.client.get_order(&order.symbol, order_id).await,
                        None => {
                            self.client
                                .get_order_by_client_id(&order.symbol, &order.client_order_id)
                                .await
                        }
                    };
                    if self.state.pause_epoch() != pause_epoch {
                        return false;
                    }
                    match exchange_result {
                        Ok(exchange_order) => {
                            let terminal = matches!(
                                exchange_order.status.as_str(),
                                "FILLED" | "CANCELED" | "EXPIRED" | "REJECTED"
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
        if !self
            .last_account_sync
            .is_some_and(|at| at.elapsed() < Duration::from_secs(10))
            || !self
                .last_orders_sync
                .is_some_and(|at| at.elapsed() < Duration::from_secs(10))
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
                    "FILLED" | "CANCELED" | "EXPIRED" | "REJECTED"
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
        let pause_epoch = self.state.pause_epoch();
        let symbol = self.state.config.read().await.exchange.symbol.clone();

        let position_ok = if let Ok(Some(pos)) = self.client.get_position(&symbol).await {
            let mut state_pos = self.state.position.write().await;
            state_pos.symbol = pos.symbol;
            state_pos.size = pos.position_amt;
            state_pos.entry_price = pos.entry_price;
            state_pos.mark_price = pos.mark_price;
            state_pos.unrealized_pnl = pos.un_realized_profit;
            state_pos.liquidation_price = pos.liquidation_price;
            state_pos.leverage = pos.leverage.parse().unwrap_or(20);
            true
        } else {
            warn!("Could not sync Binance position for {}", symbol);
            false
        };

        if self.state.pause_epoch() != pause_epoch {
            return false;
        }

        let account_ok = match self.client.get_account().await {
            Ok(acc) => {
                if let Some(asset) = acc.margin_asset_for_symbol(&symbol) {
                    let mut state_acc = self.state.account.write().await;
                    state_acc.asset = asset.asset.clone();
                    state_acc.total_wallet_balance = asset.wallet_balance;
                    state_acc.available_balance = asset.available_balance;
                    state_acc.margin_balance = asset.margin_balance;
                    state_acc.unrealized_profit = asset.unrealized_profit;
                    state_acc.update_time = Utc::now();
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
        if position_ok && account_ok {
            self.last_account_sync = Some(Instant::now());
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
        }
        let mode = TradingMode::from_exchange(config.exchange.dry_run, config.exchange.is_testnet);
        let recorded_id = order.client_order_id.clone();
        match self
            .state
            .db
            .run_blocking(move |db| db.get_trade_by_client_id(&recorded_id, mode))
            .await
        {
            Ok(Some(_)) => {
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
            Ok(None) => {}
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
        let mut completed_buy_price = None;
        let note;

        match order.side {
            OrderSide::Buy => {
                note = format!(
                    "Grid BUY filled at {} (Qty: {})",
                    order.price, order.quantity
                );

                // Update simulated position in dry-run
                if config.exchange.dry_run {
                    let mut pos = self.state.position.write().await;
                    let prev_size = pos.size;
                    let new_size = prev_size + order.quantity;
                    if !new_size.is_zero() {
                        pos.entry_price = if prev_size >= Decimal::ZERO {
                            (pos.entry_price * prev_size + order.price * order.quantity) / new_size
                        } else {
                            order.price
                        };
                    }
                    pos.size = new_size;
                } else {
                    // Refresh the position after this confirmed fill before reserving the paired exit.
                    self.sync_account_and_position().await;
                }
            }
            OrderSide::Sell => {
                // Check if this sell closed a previously tracked buy order
                if let Some(paired_id) = &order.paired_client_order_id {
                    // Legacy pairs keep their historical accounting only.
                    let paired_id = paired_id.clone();
                    let purchase = self
                        .state
                        .db
                        .run_blocking(move |db| db.get_trade_by_client_id(&paired_id, mode))
                        .await
                        .ok()
                        .flatten()
                        .filter(|trade| trade.side == OrderSide::Buy)
                        .map(|trade| (trade.price, trade.quantity));
                    if let Some((buy_price, buy_qty)) = purchase {
                        let exec_qty = order.quantity.min(buy_qty);
                        cycle_profit = (order.price - buy_price) * exec_qty;
                        completed_buy_price = Some(buy_price);
                        is_completed_cycle = true;
                    }
                }

                if is_completed_cycle {
                    note = format!(
                        "Completed Grid Cycle! Sold at {} (Paired Buy: {}, Spread Estimate: +{} USDC)",
                        order.price, completed_buy_price.unwrap(), cycle_profit
                    );
                    self.state
                        .add_log(
                            "SUCCESS",
                            format!(
                                "🎉 Grid Cycle Completed! Sold at {}, Spread Estimate: +{} USDC",
                                order.price, cycle_profit
                            ),
                        )
                        .await;
                } else {
                    note = format!(
                        "Grid SELL filled at {} (Qty: {})",
                        order.price, order.quantity
                    );
                    self.state
                        .add_log(
                            "INFO",
                            format!(
                                "🔴 SELL Filled at {} (Qty: {})",
                                order.price, order.quantity
                            ),
                        )
                        .await;
                }

                // Update simulated position in dry-run
                if config.exchange.dry_run {
                    let mut pos = self.state.position.write().await;
                    let prev_size = pos.size;
                    let closed_qty = order.quantity.min(prev_size.max(Decimal::ZERO));
                    simulated_pnl = (order.price - pos.entry_price) * closed_qty;
                    let new_size = prev_size - order.quantity;
                    pos.size = new_size;

                    let mut acc = self.state.account.write().await;
                    acc.total_wallet_balance += simulated_pnl;
                    acc.available_balance += simulated_pnl;
                } else {
                    self.sync_account_and_position().await;
                }
            }
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
        let config = self.state.config.read().await.clone();
        if *self.state.status.read().await != BotStatus::Running {
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
        self.trim_sell_orders_to_position().await;
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
        let current_price = self.state.ticker.read().await.last_price;
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
            // Cancellation can reveal a fill and move the execution frontier.
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

        if let Err(e) = self.reconcile_remainder(&desired_sell_prices).await {
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

    /// Keep outstanding sells within the actual long position, nearest prices first.
    async fn trim_sell_orders_to_position(&mut self) {
        let mut sell_orders: Vec<GridOrder> = self
            .state
            .active_orders
            .read()
            .await
            .values()
            .filter(|order| order.side == OrderSide::Sell)
            .cloned()
            .collect();
        sell_orders.sort_by(|a, b| {
            a.price
                .cmp(&b.price)
                .then_with(|| a.client_order_id.cmp(&b.client_order_id))
        });

        let mut remaining = self.state.position.read().await.size.max(Decimal::ZERO);
        for order in sell_orders {
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
        if *self.state.status.read().await != BotStatus::Running {
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
            && !self
                .last_account_sync
                .is_some_and(|at| at.elapsed() < Duration::from_secs(10))
        {
            warn!("Skipping order: Binance account or position has not been synchronized recently");
            return false;
        }
        if !config.exchange.dry_run
            && !self
                .last_orders_sync
                .is_some_and(|at| at.elapsed() < Duration::from_secs(10))
        {
            warn!("Skipping order: Binance open orders have not been synchronized recently");
            return false;
        }

        // Enforce Maker pricing check
        let current_market_price = self.state.ticker.read().await.last_price;
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
            let (buys, sells) = match self
                .window_prices(&config, &rules, current_market_price)
                .await
            {
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
        let quantity = if side == OrderSide::Buy {
            if let Some(limit) = config.grid.max_position_usdc {
                let position_size = self.state.position.read().await.size;
                let valuation_price = current_market_price.max(price);
                let exposure = buy_exposure_usdc(position_size, valuation_price, &orders)
                    + price * full_quantity;
                if exposure > limit {
                    debug!(
                        "Skipping BUY at {}: projected exposure {} exceeds limit {}",
                        price, exposure, limit
                    );
                    return false;
                }
            }
            full_quantity
        } else {
            let available = sell_quantity_available(self.state.position.read().await.size, &orders);
            if available >= full_quantity {
                full_quantity
            } else {
                debug!(
                    "Skipping SELL at {}: available {} < full grid quantity {}",
                    price, available, full_quantity
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
            if *self.state.status.read().await != BotStatus::Running {
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
                    reduce_only: side == OrderSide::Sell,
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
                    if matches!(e, ExchangeError::HttpError(_) | ExchangeError::Other(_)) {
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
                return false;
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
                        "FILLED" | "CANCELED" | "EXPIRED" | "REJECTED"
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
        if let Err(e) = self.state.pause_trading().await {
            error!("Failed to persist paused status before cancellation: {}", e);
            return false;
        }

        if let Err(e) = self.cancel_remainder_plan().await {
            error!(
                "Could not resolve pending remainder plan during cancellation: {}",
                e
            );
            return false;
        }

        if !is_dry_run {
            let live_orders = match self.client.get_open_orders(&symbol).await {
                Ok(orders) => orders,
                Err(e) => {
                    error!("Failed to list managed orders before cancellation: {}", e);
                    self.pause_trading().await;
                    return false;
                }
            };
            for order in live_orders
                .into_iter()
                .filter(|order| is_grid_order(&order.client_order_id))
            {
                if let Err(e) = self
                    .client
                    .cancel_order(&symbol, Some(order.order_id), None)
                    .await
                {
                    error!(
                        "Failed to cancel managed order {}: {}",
                        order.client_order_id, e
                    );
                    self.pause_trading().await;
                    return false;
                }
            }
            if !self.sync_live_orders().await || !self.state.active_orders.read().await.is_empty() {
                error!("Managed orders could not be fully reconciled after cancellation");
                self.pause_trading().await;
                return false;
            }
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
            if let Err(e) = self.state.resume_trading().await {
                error!("Failed to restore running status after cancellation: {}", e);
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

    /// Broadcast latest snapshot to WebSocket clients
    async fn broadcast_snapshot(&self) {
        let snapshot = self.state.snapshot().await;
        if let Ok(json) = serde_json::to_string(&serde_json::json!({
            "type": "snapshot",
            "data": snapshot
        })) {
            let _ = self.state.ws_broadcast_tx.send(json);
        }
    }
}

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
        lose_response: Arc<AtomicUsize>,
    }

    async fn pair_exchange_server(fake: PairExchange) -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new()
            .route(
                "/fapi/v1/openOrders",
                get(|State(fake): State<PairExchange>| async move {
                    let orders: Vec<_> = fake.orders.lock().unwrap().values()
                        .filter(|order| matches!(order.status.as_str(), "NEW" | "PARTIALLY_FILLED"))
                        .cloned()
                        .collect();
                    Json(orders)
                }),
            )
            .route(
                "/fapi/v1/order",
                get(|State(fake): State<PairExchange>, Query(query): Query<HashMap<String, String>>| async move {
                    if fake.fail_lookup.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1)).is_ok() {
                        return (axum::http::StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"code": -1000})));
                    }
                    match fake.orders.lock().unwrap().get(&query["origClientOrderId"]).cloned() {
                        Some(order) => (axum::http::StatusCode::OK, Json(serde_json::to_value(order).unwrap())),
                        None => (axum::http::StatusCode::BAD_REQUEST, Json(serde_json::json!({"code": -2013, "msg": "Order does not exist"}))),
                    }
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
                    if fake.lose_response.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1)).is_ok() {
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
        state.ticker.write().await.last_price = dec!(100);
        state.position.write().await.size = dec!(1);
        let client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
        let mut engine = GridTradingEngine::new(state, client, action_rx, ticker_rx);
        engine.last_account_sync = Some(Instant::now());
        engine.last_orders_sync = Some(Instant::now());
        engine
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
        state.ticker.write().await.last_price = dec!(100);
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
        state.ticker.write().await.last_price = dec!(115);
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
        state.ticker.write().await.last_price = dec!(115);
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
        engine.trim_sell_orders_to_position().await;
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
        state.ticker.write().await.last_price = dec!(115);
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
        state.ticker.write().await.last_price = dec!(122.15);
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
                ..Default::default()
            })
            .await;

        let ticker = state.ticker.read().await;
        assert_eq!(ticker.last_price, dec!(114.5));
        assert_eq!(ticker.mark_price, dec!(114.3));
        assert_eq!(ticker.mark_update_time, mark_updated_at);
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
