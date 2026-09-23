use crate::exchange::client::{BinanceFuturesClient, ExchangeError};
use crate::exchange::BinanceOrderResponse;
use crate::server::state::AppState;
use crate::strategy::precision::SymbolRules;
use crate::types::*;
use anyhow::Result;
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
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

pub struct GridTradingEngine {
    state: Arc<AppState>,
    client: Arc<BinanceFuturesClient>,
    action_rx: mpsc::Receiver<BotControlAction>,
    ticker_rx: broadcast::Receiver<TickerInfo>,
    // Map of client_order_id -> purchase price for calculating paired grid cycle profit
    paired_buy_prices: HashMap<String, (Decimal, Decimal)>,
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
                }
            }
            Err(e) => {
                warn!("Could not fetch exchangeInfo for {}, using defaults: {}", symbol, e);
            }
        }

        // Fetch initial market price
        match self.client.get_ticker_price(&symbol).await {
            Ok(price) => {
                let mut ticker = self.state.ticker.write().await;
                ticker.symbol = symbol.clone();
                ticker.last_price = price;
                ticker.mark_price = price;
                ticker.update_time = Utc::now();
                info!("Initial market price for {}: {}", symbol, price);
            }
            Err(e) => {
                warn!("Failed to fetch initial market price: {}", e);
            }
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
            match self.client.get_open_orders(&symbol).await {
                Ok(open_orders) => {
                    info!("Found {} existing open orders on exchange", open_orders.len());
                    let mut active_map = self.state.active_orders.write().await;
                    for o in open_orders {
                        let side = if o.side == "BUY" { OrderSide::Buy } else { OrderSide::Sell };
                        let grid_order = GridOrder {
                            client_order_id: o.client_order_id.clone(),
                            order_id: Some(o.order_id),
                            symbol: o.symbol.clone(),
                            side,
                            price: o.price,
                            quantity: o.orig_qty,
                            amount_usdc: o.price * o.orig_qty,
                            status: OrderStatus::New,
                            created_at: Utc::now(),
                            updated_at: Utc::now(),
                            grid_level: 0,
                            paired_client_order_id: None,
                            is_take_profit: false,
                        };
                        active_map.insert(o.client_order_id, grid_order);
                    }
                }
                Err(e) => {
                    warn!("Failed to fetch existing open orders: {}", e);
                }
            }

            // Fetch live position and account
            self.sync_account_and_position().await;
        }

        Ok(())
    }

    /// Primary execution loop
    pub async fn run(&mut self) {
        let mut sync_timer = interval(Duration::from_secs(3));
        let mut snapshot_timer = interval(Duration::from_millis(800));

        // Initial grid placement
        self.rebalance_grid().await;

        loop {
            tokio::select! {
                // UI control actions (Pause, Resume, CancelAll, Rebalance, UpdateConfig)
                Some(action) = self.action_rx.recv() => {
                    self.handle_control_action(action).await;
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
            }
        }
    }

    async fn handle_control_action(&mut self, action: BotControlAction) {
        match action {
            BotControlAction::Pause => {
                info!("Bot paused by user command");
                *self.state.status.write().await = BotStatus::Paused;
                self.state.add_log("WARN", "Trading bot paused by user").await;
            }
            BotControlAction::Resume => {
                info!("Bot resumed by user command");
                *self.state.status.write().await = BotStatus::Running;
                self.state.add_log("INFO", "Trading bot resumed by user").await;
                self.rebalance_grid().await;
            }
            BotControlAction::CancelAll => {
                info!("Cancel all orders requested");
                self.cancel_all_orders().await;
                self.state.add_log("WARN", "All active grid orders canceled").await;
            }
            BotControlAction::Rebalance => {
                info!("Grid rebalance requested");
                self.state.add_log("INFO", "Manual grid rebalance triggered").await;
                self.rebalance_grid().await;
            }
            BotControlAction::UpdateConfig(new_config) => {
                let old_config = self.state.config.read().await.clone();
                let old_exchange = &old_config.exchange;
                let strategy_changed = *old_exchange != new_config.exchange
                    || old_config.grid != new_config.grid;
                let symbol_changed = old_exchange.symbol != new_config.exchange.symbol;
                let client_changed = old_exchange.api_key != new_config.exchange.api_key
                    || old_exchange.api_secret != new_config.exchange.api_secret
                    || old_exchange.is_testnet != new_config.exchange.is_testnet
                    || old_exchange.recv_window != new_config.exchange.recv_window;

                // Update config in state
                *self.state.config.write().await = *new_config.clone();

                // API credentials and endpoint are stored in the HTTP client, not in AppState.
                if client_changed {
                    self.client = Arc::new(BinanceFuturesClient::new(&new_config.exchange));
                    if let Err(e) = self.client.sync_server_time().await {
                        warn!("Failed to synchronize Binance server time after config update: {}", e);
                    }
                }

                if symbol_changed {
                    self.cancel_all_orders().await;
                    // Fetch new rules
                    if let Ok(info) = self.client.get_exchange_info(Some(&new_config.exchange.symbol)).await {
                        if let Some(rules) = SymbolRules::from_exchange_info(&info, &new_config.exchange.symbol) {
                            *self.state.rules.write().await = rules;
                        }
                    }
                    if let Ok(price) = self.client.get_ticker_price(&new_config.exchange.symbol).await {
                        let mut ticker = self.state.ticker.write().await;
                        ticker.symbol = new_config.exchange.symbol.clone();
                        ticker.last_price = price;
                        ticker.mark_price = price;
                    }
                }

                if strategy_changed {
                    self.state
                        .add_log(
                            "SUCCESS",
                            format!(
                                "⚙️ 策略配置已在线更新: 币种={}, 间距={} USDC, 每单={} U, 窗口={}/{}, 模式={}",
                                new_config.exchange.symbol,
                                new_config.grid.grid_interval,
                                new_config.grid.order_amount_usdc,
                                new_config.grid.buy_window,
                                new_config.grid.sell_window,
                                if new_config.exchange.dry_run { "模拟盘" } else { "实盘" }
                            ),
                        )
                        .await;
                    self.rebalance_grid().await;
                } else {
                    self.state.add_log("SUCCESS", "Telegram 通知配置已更新").await;
                }
            }
        }
    }

    async fn handle_ticker_update(&mut self, ticker_update: TickerInfo) {
        // Update state ticker
        {
            let mut ticker = self.state.ticker.write().await;
            ticker.last_price = ticker_update.last_price;
            ticker.mark_price = ticker_update.mark_price;
            ticker.high_24h = ticker_update.high_24h;
            ticker.low_24h = ticker_update.low_24h;
            ticker.change_24h = ticker_update.change_24h;
            ticker.change_percent_24h = ticker_update.change_percent_24h;
            ticker.volume_24h = ticker_update.volume_24h;
            ticker.update_time = Utc::now();
        }

        // Update unrealized PnL for current position
        {
            let mut pos = self.state.position.write().await;
            if !pos.size.is_zero() {
                pos.mark_price = ticker_update.last_price;
                pos.unrealized_pnl = (ticker_update.last_price - pos.entry_price) * pos.size;
            }
        }

        let is_running = *self.state.status.read().await == BotStatus::Running;
        let is_dry_run = self.state.config.read().await.exchange.dry_run;

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
        let is_running = *self.state.status.read().await == BotStatus::Running;
        let is_dry_run = self.state.config.read().await.exchange.dry_run;

        if !is_dry_run {
            self.sync_live_orders().await;
            self.sync_account_and_position().await;
        }

        if is_running {
            self.maintain_grid_window().await;
        }
    }

    /// Reconcile open orders from Binance in Live mode
    async fn sync_live_orders(&mut self) {
        let symbol = self.state.config.read().await.exchange.symbol.clone();
        match self.client.get_open_orders(&symbol).await {
            Ok(live_orders) => {
                let live_ids: HashMap<String, BinanceOrderResponse> = live_orders
                    .into_iter()
                    .map(|o| (o.client_order_id.clone(), o))
                    .collect();

                let mut filled_or_closed = Vec::new();

                {
                    let active = self.state.active_orders.read().await;
                    for (client_id, order) in active.iter() {
                        if !live_ids.contains_key(client_id) {
                            filled_or_closed.push(order.clone());
                        }
                    }
                }

                for mut order in filled_or_closed {
                    let Some(order_id) = order.order_id else { continue };
                    match self.client.get_order(&order.symbol, order_id).await {
                        Ok(exchange_order) => {
                            let terminal = matches!(exchange_order.status.as_str(), "FILLED" | "CANCELED" | "EXPIRED" | "REJECTED");
                            if !terminal {
                                continue;
                            }
                            if exchange_order.executed_qty > Decimal::ZERO {
                                order.quantity = exchange_order.executed_qty;
                                if let Some(avg_price) = exchange_order.avg_price.filter(|price| *price > Decimal::ZERO) {
                                    order.price = avg_price;
                                }
                                order.amount_usdc = order.price * order.quantity;
                                self.on_order_filled(&mut order).await;
                            } else {
                                self.state.active_orders.write().await.remove(&order.client_order_id);
                                debug!("Order {} closed without a fill ({})", order.client_order_id, exchange_order.status);
                            }
                        }
                        Err(e) => warn!("Could not verify order {} status: {}", order.client_order_id, e),
                    }
                }
            }
            Err(e) => {
                debug!("Error during live orders sync: {}", e);
            }
        }
    }

    /// Fetch position and account balances from Binance in Live mode
    async fn sync_account_and_position(&mut self) {
        let symbol = self.state.config.read().await.exchange.symbol.clone();

        if let Ok(Some(pos)) = self.client.get_position(&symbol).await {
            let mut state_pos = self.state.position.write().await;
            state_pos.symbol = pos.symbol;
            state_pos.size = pos.position_amt;
            state_pos.entry_price = pos.entry_price;
            state_pos.mark_price = pos.mark_price;
            state_pos.unrealized_pnl = pos.un_realized_profit;
            state_pos.liquidation_price = pos.liquidation_price;
            state_pos.leverage = pos.leverage.parse().unwrap_or(20);
        }

        if let Ok(acc) = self.client.get_account().await {
            let mut state_acc = self.state.account.write().await;
            state_acc.total_wallet_balance = acc.total_wallet_balance;
            state_acc.available_balance = acc.available_balance;
            state_acc.margin_balance = acc.total_margin_balance;
            state_acc.unrealized_profit = acc.total_unrealized_profit;
            state_acc.update_time = Utc::now();
        }
    }

    /// Executed when an order is confirmed filled
    async fn on_order_filled(&mut self, order: &mut GridOrder) {
        order.status = OrderStatus::Filled;
        order.updated_at = Utc::now();

        // Remove from active orders
        self.state.active_orders.write().await.remove(&order.client_order_id);

        let config = self.state.config.read().await.clone();
        let rules = self.state.rules.read().await.clone();
        let grid_interval = config.grid.grid_interval;

        let mut realized_pnl = Decimal::ZERO;
        let mut is_completed_cycle = false;
        let note;

        match order.side {
            OrderSide::Buy => {
                // Record purchase price for paired sell calculation
                self.paired_buy_prices.insert(order.client_order_id.clone(), (order.price, order.quantity));
                note = format!("Grid BUY filled at {} (Qty: {})", order.price, order.quantity);

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
                }

                // Place paired SELL order at (buy_price + grid_interval) to lock in profit!
                let paired_sell_price = rules.round_price(order.price + grid_interval);
                let paired_client_id = new_grid_client_order_id(OrderSide::Sell);

                self.place_grid_order(
                    OrderSide::Sell,
                    paired_sell_price,
                    order.quantity,
                    paired_client_id.clone(),
                    order.grid_level + 1,
                    Some(order.client_order_id.clone()),
                    true,
                )
                .await;

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
            OrderSide::Sell => {
                // Check if this sell closed a previously tracked buy order
                if let Some(paired_id) = &order.paired_client_order_id {
                    if let Some((buy_price, buy_qty)) = self.paired_buy_prices.remove(paired_id) {
                        let exec_qty = order.quantity.min(buy_qty);
                        realized_pnl = (order.price - buy_price) * exec_qty;
                        is_completed_cycle = true;
                    }
                }

                if is_completed_cycle {
                    note = format!(
                        "Completed Grid Cycle! Sold at {} (Paired Buy: {}, Profit: +{} USDC)",
                        order.price, order.price - grid_interval, realized_pnl
                    );
                    self.state
                        .add_log(
                            "SUCCESS",
                            format!(
                                "🎉 Grid Cycle Completed! Sold at {}, Realized Profit: +{} USDC",
                                order.price, realized_pnl
                            ),
                        )
                        .await;
                } else {
                    note = format!("Grid SELL filled at {} (Qty: {})", order.price, order.quantity);
                    self.state
                        .add_log(
                            "INFO",
                            format!("🔴 SELL Filled at {} (Qty: {})", order.price, order.quantity),
                        )
                        .await;
                }

                // Update simulated position in dry-run
                if config.exchange.dry_run {
                    let mut pos = self.state.position.write().await;
                    let prev_size = pos.size;
                    let new_size = prev_size - order.quantity;
                    pos.size = new_size;

                    let mut acc = self.state.account.write().await;
                    acc.total_wallet_balance += realized_pnl;
                    acc.available_balance += realized_pnl;
                }

                // Place paired BUY order at (sell_price - grid_interval)
                let paired_buy_price = rules.round_price(order.price - grid_interval);
                let paired_client_id = new_grid_client_order_id(OrderSide::Buy);

                self.place_grid_order(
                    OrderSide::Buy,
                    paired_buy_price,
                    order.quantity,
                    paired_client_id.clone(),
                    order.grid_level - 1,
                    Some(order.client_order_id.clone()),
                    false,
                )
                .await;
            }
        }

        // Record trade in history
        let trade = TradeRecord {
            trade_id: Uuid::new_v4().to_string(),
            client_order_id: order.client_order_id.clone(),
            symbol: order.symbol.clone(),
            side: order.side,
            price: order.price,
            quantity: order.quantity,
            amount_usdc: order.price * order.quantity,
            realized_pnl,
            commission: dec!(0.0), // Maker fee is 0 or minimal
           is_maker: true,
           timestamp: Utc::now(),
           note,
       };

        // Persist trade to SQLite and update runtime stats
        self.state.record_trade(trade, is_completed_cycle).await;
    }

    /// Ensure the active pre-placed order window matches buy_window and sell_window
    async fn maintain_grid_window(&mut self) {
        let current_price = self.state.ticker.read().await.last_price;
        if current_price.is_zero() {
            return;
        }

        let config = self.state.config.read().await.clone();
        let rules = self.state.rules.read().await.clone();

        let grid_interval = config.grid.grid_interval;
        let buy_window = config.grid.buy_window;
        let sell_window = config.grid.sell_window;
        let amount_usdc = config.grid.order_amount_usdc;

        // Collect current active order prices
        let active_orders: Vec<GridOrder> = self.state.active_orders.read().await.values().cloned().collect();
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
            // Check if we already have an active order near this price (within 0.5 tick)
            let already_exists = active_buy_prices
                .iter()
                .any(|&p| (p - target_price).abs() < (rules.tick_size / dec!(2.0)));

            if !already_exists && target_price < current_price {
                if let Some(qty) = rules.calculate_quantity(target_price, amount_usdc) {
                    let client_id = new_grid_client_order_id(OrderSide::Buy);
                    self.place_grid_order(
                        OrderSide::Buy,
                        target_price,
                        qty,
                        client_id,
                        -1,
                        None,
                        false,
                    )
                    .await;
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
            let already_exists = active_sell_prices
                .iter()
                .any(|&p| (p - target_price).abs() < (rules.tick_size / dec!(2.0)));

            if !already_exists && target_price > current_price {
                if let Some(qty) = rules.calculate_quantity(target_price, amount_usdc) {
                    let client_id = new_grid_client_order_id(OrderSide::Sell);
                    self.place_grid_order(
                        OrderSide::Sell,
                        target_price,
                        qty,
                        client_id,
                        1,
                        None,
                        false,
                    )
                    .await;
                }
            }
        }

        // 3. Prune orders that drifted too far outside the active window buffer to conserve margin
        let max_drift_buy = current_price - grid_interval * Decimal::from(buy_window + 3);
        let max_drift_sell = current_price + grid_interval * Decimal::from(sell_window + 3);

        let mut orders_to_cancel = Vec::new();
        for order in active_orders {
            // Keep take-profit orders alive, but prune background grid orders that are too far away
            if !order.is_take_profit {
                if order.side == OrderSide::Buy && order.price < max_drift_buy {
                    orders_to_cancel.push(order);
                } else if order.side == OrderSide::Sell && order.price > max_drift_sell {
                    orders_to_cancel.push(order);
                }
            }
        }

        for order in orders_to_cancel {
            debug!("Pruning drifted grid order at {}: client_id={}", order.price, order.client_order_id);
            self.cancel_single_order(&order).await;
        }
    }

    /// Place a single grid order (either Maker GTX on Binance or simulated)
    async fn place_grid_order(
        &mut self,
        side: OrderSide,
        price: Decimal,
        quantity: Decimal,
        client_order_id: String,
        grid_level: i32,
        paired_client_order_id: Option<String>,
        is_take_profit: bool,
    ) {
        let config = self.state.config.read().await.clone();
        let rules = self.state.rules.read().await.clone();
        let symbol = config.exchange.symbol.clone();

        // Enforce Maker pricing check
        let current_market_price = self.state.ticker.read().await.last_price;
        if !current_market_price.is_zero() {
            if side == OrderSide::Buy && price >= current_market_price {
                warn!("Buy price {} >= market price {}, skipping to prevent Taker fill", price, current_market_price);
                return;
            }
            if side == OrderSide::Sell && price <= current_market_price {
                warn!("Sell price {} <= market price {}, skipping to prevent Taker fill", price, current_market_price);
                return;
            }
        }

        let formatted_price = rules.format_price(price);
        let formatted_qty = rules.format_quantity(quantity);

        if config.exchange.dry_run {
            // Paper Trading simulation
            let order = GridOrder {
                client_order_id: client_order_id.clone(),
                order_id: Some(rand::random::<i64>().abs()),
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

            self.state.active_orders.write().await.insert(client_order_id.clone(), order);
            debug!(
                "[DRY-RUN] Placed {} Maker order: price={}, qty={}, id={}",
                side.as_str(), formatted_price, formatted_qty, client_order_id
            );
        } else {
            // Real Binance Futures API order with GTX Post-Only
            match self
                .client
                .place_order(
                    &symbol,
                    side.as_str(),
                    &formatted_price,
                    &formatted_qty,
                    &client_order_id,
                    config.grid.post_only,
                )
                .await
            {
                Ok(resp) => {
                    let order = GridOrder {
                        client_order_id: resp.client_order_id.clone(),
                        order_id: Some(resp.order_id),
                        symbol: resp.symbol,
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

                    self.state.active_orders.write().await.insert(resp.client_order_id, order);
                    info!(
                        "Placed {} Maker order on Binance: price={}, qty={}, orderId={}",
                        side.as_str(), formatted_price, formatted_qty, resp.order_id
                    );
                }
                Err(ExchangeError::PostOnlyRejected(msg)) => {
                    warn!(
                        "Maker GTX rejected (would take liquidity): {}. Will retry next tick.",
                        msg
                    );
                }
                Err(e) => {
                    error!("Failed to place {} order at {}: {}", side.as_str(), formatted_price, e);
                    self.state
                        .add_log("ERROR", format!("Order placement failed ({} @ {}): {}", side.as_str(), formatted_price, e))
                        .await;
                }
            }
        }
    }

    /// Cancel a single order
    async fn cancel_single_order(&self, order: &GridOrder) {
        let is_dry_run = self.state.config.read().await.exchange.dry_run;
        if !is_dry_run {
            if let Err(e) = self
                .client
                .cancel_order(&order.symbol, order.order_id, Some(&order.client_order_id))
                .await
            {
                warn!("Failed to cancel order {}: {}", order.client_order_id, e);
            }
        }
        self.state.active_orders.write().await.remove(&order.client_order_id);
    }

    /// Cancel all active orders
    async fn cancel_all_orders(&mut self) {
        let is_dry_run = self.state.config.read().await.exchange.dry_run;
        let symbol = self.state.config.read().await.exchange.symbol.clone();

        if !is_dry_run {
            if let Err(e) = self.client.cancel_all_orders(&symbol).await {
                error!("Failed to cancel all orders on Binance: {}", e);
            }
        }

        self.state.active_orders.write().await.clear();
        self.paired_buy_prices.clear();
    }

    /// Rebalance grid centered around current price
    async fn rebalance_grid(&mut self) {
        self.cancel_all_orders().await;
        self.maintain_grid_window().await;
        self.state.add_log("INFO", "Grid window refreshed and rebalanced").await;
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
    use super::new_grid_client_order_id;
    use crate::types::OrderSide;

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
