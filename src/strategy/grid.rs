use crate::exchange::client::{BinanceFuturesClient, ExchangeError, NewOrderRequest};
use crate::exchange::{BinanceOrderResponse, BinanceUserTrade};
use crate::server::state::AppState;
use crate::strategy::precision::SymbolRules;
use crate::types::*;
use anyhow::{anyhow, Result};
use chrono::Utc;
use rust_decimal::Decimal;
use std::collections::HashMap;
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

const GRID_ORDER_PREFIX: &str = "gb_";

fn is_grid_order(client_order_id: &str) -> bool {
    client_order_id.starts_with(GRID_ORDER_PREFIX)
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

pub struct GridTradingEngine {
    state: Arc<AppState>,
    client: Arc<BinanceFuturesClient>,
    action_rx: mpsc::Receiver<BotControlAction>,
    ticker_rx: broadcast::Receiver<TickerInfo>,
    // Map of client_order_id -> purchase price for calculating paired grid cycle profit
    paired_buy_prices: HashMap<String, (Decimal, Decimal)>,
    pnl_reconcile_offset: usize,
    pnl_reconcile_task: Option<JoinHandle<()>>,
    last_account_sync: Option<Instant>,
    last_orders_sync: Option<Instant>,
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
            paired_buy_prices: HashMap::new(),
            pnl_reconcile_offset: 0,
            pnl_reconcile_task: None,
            last_account_sync: None,
            last_orders_sync: None,
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
            let restored = self.state.db.load_managed_orders(&symbol)?;
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
        let market_changed = old_config.exchange.symbol != new_config.exchange.symbol
            || old_config.exchange.is_testnet != new_config.exchange.is_testnet;
        let mode_changed = old_config.exchange.dry_run != new_config.exchange.dry_run;
        let client_changed = old_config.exchange.api_key != new_config.exchange.api_key
            || old_config.exchange.api_secret != new_config.exchange.api_secret
            || old_config.exchange.is_testnet != new_config.exchange.is_testnet
            || old_config.exchange.recv_window != new_config.exchange.recv_window;

        if !strategy_changed {
            self.state.db.save_config(&new_config)?;
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

        // The old client, symbol and mode are still active here. Never switch
        // credentials or environment before old managed orders are canceled.
        if !self.cancel_all_orders().await {
            return Err(anyhow!(
                "Could not cancel old managed orders; configuration unchanged"
            ));
        }
        if let Err(error) = self.state.db.save_config(&new_config) {
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
            self.state.refresh_pnl_stats().await;
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

        for mut order in filled_orders {
            self.on_order_filled(&mut order).await;
        }

        if !self.state.active_orders.read().await.is_empty() {
            self.maintain_grid_window().await;
        }
    }

    /// Periodic sync cycle
    async fn sync_cycle(&mut self) {
        let config = self.state.config.read().await.clone();
        let is_dry_run = config.exchange.dry_run;

        self.refresh_mark_price(&config.exchange.symbol, is_dry_run)
            .await;
        self.refresh_stale_ticker(&config.exchange.symbol).await;

        let account_ready = if !is_dry_run {
            let orders_ready = self.sync_live_orders().await;
            if !orders_ready {
                self.last_orders_sync = None;
            }
            let account_ready = self.sync_account_and_position().await;
            orders_ready && account_ready
        } else {
            true
        };

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
        let symbol = self.state.config.read().await.exchange.symbol.clone();
        match self.client.get_open_orders(&symbol).await {
            Ok(live_orders) => {
                self.last_orders_sync = Some(Instant::now());
                let live_ids: HashMap<String, BinanceOrderResponse> = live_orders
                    .into_iter()
                    .filter(|order| is_grid_order(&order.client_order_id))
                    .map(|o| (o.client_order_id.clone(), o))
                    .collect();

                let mut filled_or_closed = Vec::new();
                let mut persistence_ok = true;

                {
                    let mut active = self.state.active_orders.write().await;
                    for (client_id, live_order) in &live_ids {
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
                            if let Err(e) = self.state.db.save_managed_order(order) {
                                error!("Could not persist managed order {}: {}", client_id, e);
                                persistence_ok = false;
                            }
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
                            };
                            if let Err(e) = self.state.db.save_managed_order(&order) {
                                error!("Could not persist recovered order {}: {}", client_id, e);
                                persistence_ok = false;
                            }
                            active.insert(client_id.clone(), order);
                        }
                    }
                }

                {
                    let active = self.state.active_orders.read().await;
                    for (client_id, order) in active.iter() {
                        if !live_ids.contains_key(client_id) {
                            filled_or_closed.push(order.clone());
                        }
                    }
                }

                for mut order in filled_or_closed {
                    let exchange_result = match order.order_id {
                        Some(order_id) => self.client.get_order(&order.symbol, order_id).await,
                        None => {
                            self.client
                                .get_order_by_client_id(&order.symbol, &order.client_order_id)
                                .await
                        }
                    };
                    match exchange_result {
                        Ok(exchange_order) => {
                            let terminal = matches!(
                                exchange_order.status.as_str(),
                                "FILLED" | "CANCELED" | "EXPIRED" | "REJECTED"
                            );
                            if !terminal {
                                continue;
                            }
                            if exchange_order.executed_qty > Decimal::ZERO {
                                order.quantity = exchange_order.executed_qty;
                                if let Some(avg_price) = exchange_order
                                    .avg_price
                                    .filter(|price| *price > Decimal::ZERO)
                                {
                                    order.price = avg_price;
                                }
                                order.amount_usdc = order.price * order.quantity;
                                self.on_order_filled(&mut order).await;
                            } else {
                                self.state
                                    .active_orders
                                    .write()
                                    .await
                                    .remove(&order.client_order_id);
                                if let Err(e) =
                                    self.state.db.delete_managed_order(&order.client_order_id)
                                {
                                    error!(
                                        "Could not delete closed order {}: {}",
                                        order.client_order_id, e
                                    );
                                    persistence_ok = false;
                                }
                                debug!(
                                    "Order {} closed without a fill ({})",
                                    order.client_order_id, exchange_order.status
                                );
                            }
                        }
                        Err(e) => warn!(
                            "Could not verify order {} status: {}",
                            order.client_order_id, e
                        ),
                    }
                }
                persistence_ok
            }
            Err(e) => {
                warn!("Error during live orders sync: {}", e);
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
        let pending_count = self.state.stats.read().await.pending_pnl_trades;
        if pending_count == 0 {
            self.pnl_reconcile_offset = 0;
            return;
        }
        let offset = self.pnl_reconcile_offset % pending_count;
        let pending = match self
            .state
            .db
            .get_unverified_trades(&config.exchange.symbol, 3, offset)
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
    async fn on_order_filled(&mut self, order: &mut GridOrder) {
        order.status = OrderStatus::Filled;
        order.updated_at = Utc::now();

        // Remove from active orders
        self.state
            .active_orders
            .write()
            .await
            .remove(&order.client_order_id);
        let config = self.state.config.read().await.clone();
        let rules = self.state.rules.read().await.clone();
        let grid_interval = config.grid.grid_interval;
        // An old or partially executed small order must not trigger a much
        // larger paired trade. The regular grid window can place full units.
        let full_grid_fill = rules.calculate_quantity(order.price, config.grid.order_amount_usdc)
            == Some(order.quantity);
        let trading_enabled = *self.state.status.read().await == BotStatus::Running;

        let mut cycle_profit = Decimal::ZERO;
        let mut simulated_pnl = Decimal::ZERO;
        let mut is_completed_cycle = false;
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

                // Place paired SELL order at (buy_price + grid_interval) to lock in profit!
                let paired_sell_price = rules.round_price(order.price + grid_interval);
                let paired_client_id = new_grid_client_order_id(OrderSide::Sell);
                let paired_exists =
                    self.state
                        .active_orders
                        .read()
                        .await
                        .values()
                        .any(|candidate| {
                            candidate.side == OrderSide::Sell
                                && candidate.paired_client_order_id.as_deref()
                                    == Some(order.client_order_id.as_str())
                        });

                let placed = if full_grid_fill && trading_enabled {
                    paired_exists
                        || self
                            .place_grid_order(
                                OrderSide::Sell,
                                paired_sell_price,
                                paired_client_id.clone(),
                                order.grid_level + 1,
                                Some(order.client_order_id.clone()),
                                true,
                            )
                            .await
                } else {
                    false
                };

                if placed {
                    // Only a placed paired exit needs this purchase price.
                    self.paired_buy_prices
                        .insert(order.client_order_id.clone(), (order.price, order.quantity));
                    self.state
                        .add_log(
                            "INFO",
                            format!(
                            "🟢 BUY Filled at {}! Placed paired Maker SELL at {} (+{} USDC spread)",
                            order.price, paired_sell_price, grid_interval
                        ),
                        )
                        .await;
                }
            }
            OrderSide::Sell => {
                // Check if this sell closed a previously tracked buy order
                if let Some(paired_id) = &order.paired_client_order_id {
                    let purchase = self.paired_buy_prices.remove(paired_id).or_else(|| {
                        self.state
                            .db
                            .get_trade_by_client_id(paired_id)
                            .ok()
                            .flatten()
                            .filter(|trade| trade.side == OrderSide::Buy)
                            .map(|trade| (trade.price, trade.quantity))
                    });
                    if let Some((buy_price, buy_qty)) = purchase {
                        let exec_qty = order.quantity.min(buy_qty);
                        cycle_profit = (order.price - buy_price) * exec_qty;
                        is_completed_cycle = true;
                    }
                }

                if is_completed_cycle {
                    note = format!(
                        "Completed Grid Cycle! Sold at {} (Paired Buy: {}, Spread Estimate: +{} USDC)",
                        order.price, order.price - grid_interval, cycle_profit
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
                }

                // Place paired BUY order at (sell_price - grid_interval)
                let paired_buy_price = rules.round_price(order.price - grid_interval);
                let paired_client_id = new_grid_client_order_id(OrderSide::Buy);
                let paired_exists =
                    self.state
                        .active_orders
                        .read()
                        .await
                        .values()
                        .any(|candidate| {
                            candidate.side == OrderSide::Buy
                                && candidate.paired_client_order_id.as_deref()
                                    == Some(order.client_order_id.as_str())
                        });

                if full_grid_fill && trading_enabled && !paired_exists {
                    self.place_grid_order(
                        OrderSide::Buy,
                        paired_buy_price,
                        paired_client_id.clone(),
                        order.grid_level - 1,
                        Some(order.client_order_id.clone()),
                        false,
                    )
                    .await;
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
            side: order.side,
            price: order.price,
            quantity: order.quantity,
            amount_usdc: order.price * order.quantity,
            realized_pnl,
            commission,
            pnl_verified,
            is_maker,
            timestamp: Utc::now(),
            note,
        };

        // Persist trade to SQLite and update runtime stats
        let recorded = self
            .state
            .record_trade(trade, is_completed_cycle.then_some(cycle_profit))
            .await;
        if !recorded {
            self.pause_trading().await;
            self.state
                .active_orders
                .write()
                .await
                .insert(order.client_order_id.clone(), order.clone());
            return;
        }
        if !config.exchange.dry_run {
            if let Err(e) = self.state.db.delete_managed_order(&order.client_order_id) {
                error!(
                    "Could not clear filled managed order {}: {}",
                    order.client_order_id, e
                );
                self.pause_trading().await;
            }
        }
    }

    /// Ensure the active pre-placed order window matches buy_window and sell_window
    async fn maintain_grid_window(&mut self) {
        self.trim_sell_orders_to_position().await;
        let current_price = self.state.ticker.read().await.last_price;
        if current_price.is_zero() {
            return;
        }

        let config = self.state.config.read().await.clone();
        let rules = self.state.rules.read().await.clone();

        let grid_interval = config.grid.grid_interval;
        let buy_window = config.grid.buy_window;
        let sell_window = config.grid.sell_window;
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

        // 1. Maintain Buy Window:
        // We want buy orders at: P_curr - 1*step, P_curr - 2*step, ... up to buy_window
        let mut desired_buy_prices = Vec::new();
        for i in 1..=buy_window {
            let target_price = rules.round_price(current_price - grid_interval * Decimal::from(i));
            if target_price > Decimal::ZERO {
                if let Some(min_p) = config.grid.min_price {
                    if target_price < min_p {
                        continue;
                    }
                }
                desired_buy_prices.push(target_price);
            }
        }

        for target_price in desired_buy_prices {
            // Keep the existing grid anchor while the market moves within one grid interval.
            let already_exists = has_nearby_grid_order(
                &active_buy_prices,
                target_price,
                grid_interval,
                rules.tick_size,
            );

            if !already_exists && target_price < current_price {
                let client_id = new_grid_client_order_id(OrderSide::Buy);
                if self
                    .place_grid_order(OrderSide::Buy, target_price, client_id, -1, None, false)
                    .await
                {
                    active_buy_prices.push(target_price);
                }
            }
        }

        // 2. Maintain Sell Window:
        // We want sell orders at: P_curr + 1*step, P_curr + 2*step, ... up to sell_window
        let mut desired_sell_prices = Vec::new();
        for i in 1..=sell_window {
            let target_price = rules.round_price(current_price + grid_interval * Decimal::from(i));
            if let Some(max_p) = config.grid.max_price {
                if target_price > max_p {
                    continue;
                }
            }
            desired_sell_prices.push(target_price);
        }

        for target_price in desired_sell_prices {
            let already_exists = has_nearby_grid_order(
                &active_sell_prices,
                target_price,
                grid_interval,
                rules.tick_size,
            );

            if !already_exists && target_price > current_price {
                let client_id = new_grid_client_order_id(OrderSide::Sell);
                if self
                    .place_grid_order(OrderSide::Sell, target_price, client_id, 1, None, false)
                    .await
                {
                    active_sell_prices.push(target_price);
                }
            }
        }

        // 3. Prune orders that drifted too far outside the active window buffer to conserve margin
        let max_drift_buy = current_price - grid_interval * Decimal::from(buy_window + 3);
        let max_drift_sell = current_price + grid_interval * Decimal::from(sell_window + 3);

        let mut orders_to_cancel = Vec::new();
        for order in active_orders {
            // Keep take-profit orders alive, but prune background grid orders that are too far away
            if !order.is_take_profit
                && ((order.side == OrderSide::Buy && order.price < max_drift_buy)
                    || (order.side == OrderSide::Sell && order.price > max_drift_sell))
            {
                orders_to_cancel.push(order);
            }
        }

        for order in orders_to_cancel {
            debug!(
                "Pruning drifted grid order at {}: client_id={}",
                order.price, order.client_order_id
            );
            self.cancel_single_order(&order).await;
        }
    }

    /// Keep outstanding sells within the actual long position. Paired exits take priority.
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
            b.is_take_profit
                .cmp(&a.is_take_profit)
                .then_with(|| a.price.cmp(&b.price))
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
        is_take_profit: bool,
    ) -> bool {
        let config = self.state.config.read().await.clone();
        let rules = self.state.rules.read().await.clone();
        let symbol = config.exchange.symbol.clone();
        let Some(quantity) = rules.calculate_quantity(price, config.grid.order_amount_usdc) else {
            return false;
        };

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
        }

        let orders: Vec<GridOrder> = self
            .state
            .active_orders
            .read()
            .await
            .values()
            .cloned()
            .collect();
        if side == OrderSide::Buy {
            if let Some(limit) = config.grid.max_position_usdc {
                let position_size = self.state.position.read().await.size;
                let valuation_price = current_market_price.max(price);
                let exposure =
                    buy_exposure_usdc(position_size, valuation_price, &orders) + price * quantity;
                if exposure > limit {
                    debug!(
                        "Skipping BUY at {}: projected exposure {} exceeds limit {}",
                        price, exposure, limit
                    );
                    return false;
                }
            }
        } else {
            let available = sell_quantity_available(self.state.position.read().await.size, &orders);
            // A reduce-only sell must be funded by a full grid unit. Never turn
            // the leftover position into a smaller order.
            if available < quantity {
                debug!(
                    "Skipping SELL at {}: available {} < full grid quantity {}",
                    price, available, quantity
                );
                return false;
            }
        }

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
            is_take_profit,
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
            if let Err(e) = self.state.db.save_managed_order(&order) {
                error!("Could not persist order intent {}: {}", client_order_id, e);
                self.pause_trading().await;
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
                    post_only: config.grid.post_only,
                    reduce_only: side == OrderSide::Sell,
                })
                .await
            {
                Ok(resp) => {
                    order.order_id = Some(resp.order_id);
                    order.symbol = resp.symbol;
                    order.client_order_id = resp.client_order_id.clone();

                    if let Err(e) = self.state.db.save_managed_order(&order) {
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
                    if let Err(e) = self.state.db.delete_managed_order(&client_order_id) {
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
                        self.state.db.delete_managed_order(&client_order_id)
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
                Ok(exchange_order) if exchange_order.executed_qty > Decimal::ZERO => {
                    let mut filled = order.clone();
                    filled.quantity = exchange_order.executed_qty;
                    if let Some(price) = exchange_order
                        .avg_price
                        .filter(|price| *price > Decimal::ZERO)
                    {
                        filled.price = price;
                    }
                    self.on_order_filled(&mut filled).await;
                    return true;
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
        if let Err(e) = self.state.db.delete_managed_order(&order.client_order_id) {
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
        let is_dry_run = self.state.config.read().await.exchange.dry_run;
        let symbol = self.state.config.read().await.exchange.symbol.clone();
        let was_running = *self.state.status.read().await == BotStatus::Running;
        if let Err(e) = self.state.pause_trading().await {
            error!("Failed to persist paused status before cancellation: {}", e);
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
        self.paired_buy_prices.clear();
        if !is_dry_run {
            if let Err(e) = self.state.db.clear_managed_orders(&symbol) {
                error!("Failed to clear persisted managed orders: {}", e);
                self.pause_trading().await;
                return false;
            }
        }
        if was_running {
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
        new_grid_client_order_id, reserved_sell_quantity, sell_quantity_available,
        GridTradingEngine,
    };
    use crate::config::AppConfig;
    use crate::db::Database;
    use crate::exchange::client::BinanceFuturesClient;
    use crate::exchange::{BinanceOrderResponse, BinanceUserTrade};
    use crate::server::state::AppState;
    use crate::types::{
        BotControlAction, BotStatus, GridOrder, OrderSide, OrderStatus, TickerInfo,
    };
    use axum::{
        extract::{Query, State},
        routing::{delete, get},
        Json, Router,
    };
    use chrono::Utc;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };
    use tokio::sync::{broadcast, mpsc};

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
    async fn sell_window_skips_unfunded_orders_and_replaces_oversized_orders() {
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
        state.position.write().await.size = dec!(2.63);

        engine.maintain_grid_window().await;
        let orders: Vec<_> = state.active_orders.read().await.values().cloned().collect();
        assert_eq!(reserved_sell_quantity(&orders), dec!(0));
        assert_eq!(
            orders.iter().filter(|o| o.side == OrderSide::Sell).count(),
            0
        );

        state.position.write().await.size = dec!(19.67);
        engine.maintain_grid_window().await;
        let orders: Vec<_> = state.active_orders.read().await.values().cloned().collect();
        let sell = orders.iter().find(|o| o.side == OrderSide::Sell).unwrap();
        let expected_qty = state
            .rules
            .read()
            .await
            .calculate_quantity(sell.price, dec!(2000))
            .unwrap();
        assert_eq!(sell.quantity, expected_qty);
        assert_eq!(
            orders.iter().filter(|o| o.side == OrderSide::Sell).count(),
            1
        );
        assert!(sell_quantity_available(dec!(19.67), &orders) < expected_qty);

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
        assert_eq!(reserved_sell_quantity(&orders), expected_qty);
        assert_eq!(
            orders.iter().filter(|o| o.side == OrderSide::Sell).count(),
            1
        );

        state.position.write().await.size = dec!(-1);
        engine.trim_sell_orders_to_position().await;
        let orders: Vec<_> = state.active_orders.read().await.values().cloned().collect();
        assert_eq!(reserved_sell_quantity(&orders), dec!(0));
    }

    #[tokio::test]
    async fn paired_orders_use_configured_notional_and_skip_small_fills() {
        let mut config = AppConfig::default();
        config.grid.grid_interval = dec!(1);
        config.grid.order_amount_usdc = dec!(2000);
        let db = Arc::new(Database::open(":memory:").unwrap());
        let (action_tx, action_rx) = mpsc::channel(1);
        let (_ticker_tx, ticker_rx) = broadcast::channel(1);
        let state = AppState::new(config.clone(), db, action_tx);
        let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
        let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
        state.ticker.write().await.last_price = dec!(114.5);

        let mut buy = GridOrder {
            client_order_id: "filled-buy".into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side: OrderSide::Buy,
            price: dec!(114.24),
            quantity: dec!(17.50),
            amount_usdc: dec!(1999.20),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: -1,
            paired_client_order_id: None,
            is_take_profit: false,
        };
        engine.on_order_filled(&mut buy).await;
        let orders: Vec<_> = state.active_orders.read().await.values().cloned().collect();
        let sell = orders.iter().find(|o| o.side == OrderSide::Sell).unwrap();
        assert_eq!(sell.price, dec!(115.24));
        assert_eq!(
            sell.quantity,
            state
                .rules
                .read()
                .await
                .calculate_quantity(sell.price, dec!(2000))
                .unwrap()
        );

        engine.cancel_all_orders().await;
        state.position.write().await.size = dec!(0.46);
        let mut small_sell = GridOrder {
            client_order_id: "filled-small-sell".into(),
            side: OrderSide::Sell,
            price: dec!(115.24),
            quantity: dec!(0.46),
            amount_usdc: dec!(53.0104),
            ..buy.clone()
        };
        engine.on_order_filled(&mut small_sell).await;
        assert!(state.active_orders.read().await.is_empty());

        let full_sell_qty = state
            .rules
            .read()
            .await
            .calculate_quantity(dec!(115.24), dec!(2000))
            .unwrap();
        state.position.write().await.size = full_sell_qty;
        let mut full_sell = GridOrder {
            client_order_id: "filled-full-sell".into(),
            side: OrderSide::Sell,
            price: dec!(115.24),
            quantity: full_sell_qty,
            amount_usdc: dec!(115.24) * full_sell_qty,
            ..buy
        };
        engine.on_order_filled(&mut full_sell).await;
        let orders: Vec<_> = state.active_orders.read().await.values().cloned().collect();
        let paired_buy = orders.iter().find(|o| o.side == OrderSide::Buy).unwrap();
        assert_eq!(paired_buy.price, dec!(114.24));
        assert_eq!(
            paired_buy.quantity,
            state
                .rules
                .read()
                .await
                .calculate_quantity(paired_buy.price, dec!(2000))
                .unwrap()
        );
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
