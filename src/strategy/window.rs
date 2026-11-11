use super::*;

#[derive(Clone, Copy)]
pub(super) struct LastFillSnapshot {
    pub(super) revision: u64,
    pub(super) fill: Option<(OrderSide, Decimal)>,
}

#[derive(PartialEq)]
pub(super) struct PaperWindowInputs {
    symbol: String,
    grid: crate::config::GridConfig,
    rules: SymbolRules,
    reference_band: (Decimal, Decimal),
    maker_band: (Decimal, Decimal),
    position: (Decimal, Decimal, Decimal, u32),
    trade_revision: u64,
    pause_epoch: u64,
}

pub(super) struct PaperWindowCache {
    inputs: PaperWindowInputs,
    orders: HashMap<String, GridOrder>,
}

#[derive(Clone, Copy)]
pub(super) struct WindowPlacementContext<'a> {
    pub(super) last_fill: LastFillSnapshot,
    pub(super) precomputed: Option<(Decimal, &'a [Decimal])>,
}

struct DesiredGridWindow {
    active_buy_prices: Vec<Decimal>,
    active_sell_prices: Vec<Decimal>,
    desired_buy_prices: Vec<Decimal>,
    desired_sell_prices: Vec<Decimal>,
    last_fill: LastFillSnapshot,
}

impl GridTradingEngine {
    pub(super) async fn load_last_fill(
        &self,
        config: &crate::config::AppConfig,
    ) -> Result<LastFillSnapshot> {
        let symbol = &config.exchange.symbol;
        let mode = config.exchange.trading_mode();
        // Capture before the read so a concurrent fill or history clear invalidates
        // this context even when it commits while the blocking query is pending.
        let revision = self.state.db.symbol_trade_revision(symbol);
        if let Some((_, _, cached)) = self
            .last_fill_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .filter(|(s, m, snap)| s == symbol && *m == mode && snap.revision == revision)
        {
            return Ok(*cached);
        }
        let query_symbol = symbol.clone();
        let fill = self
            .state
            .db
            .run_blocking(move |db| db.last_fill_for(&query_symbol, mode))
            .await?;
        let snapshot = LastFillSnapshot { revision, fill };
        if self.state.db.symbol_trade_revision(symbol) == revision {
            *self
                .last_fill_cache
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some((symbol.clone(), mode, snapshot));
        }
        Ok(snapshot)
    }

    pub(super) async fn window_prices(
        &self,
        config: &crate::config::AppConfig,
        rules: &SymbolRules,
        market: Decimal,
    ) -> Result<(Vec<Decimal>, Vec<Decimal>)> {
        let last_fill = self.load_last_fill(config).await?;
        Ok(Self::window_prices_with_last_fill(
            config, rules, market, last_fill,
        ))
    }

    fn window_prices_with_last_fill(
        config: &crate::config::AppConfig,
        rules: &SymbolRules,
        market: Decimal,
        last_fill: LastFillSnapshot,
    ) -> (Vec<Decimal>, Vec<Decimal>) {
        let interval = config.grid.grid_interval;
        // A recent fill only reserves its immediate neighborhood. It must never
        // pin either window to an old price while the mark price moves away.
        rules.grid_window_prices(
            market,
            interval,
            (config.grid.buy_window, config.grid.sell_window),
            (config.grid.min_price, config.grid.max_price),
            |_, price| {
                last_fill
                    .fill
                    .is_none_or(|(_, fill_price)| (price - fill_price).abs() >= interval)
            },
        )
    }

    fn live_reconciliation_ready(&self) -> bool {
        !self.reconciliation_blocked && self.sync_timestamps_fresh()
    }

    /// Ensure the active pre-placed order window matches buy_window and sell_window.
    pub(super) async fn maintain_grid_window(&mut self) {
        self.maintain_grid_window_inner(false).await;
    }

    async fn window_maintenance_admitted(&mut self, config: &crate::config::AppConfig) -> bool {
        if self.state.is_shutting_down() || *self.state.status.read().await != BotStatus::Running {
            return false;
        }
        // Opposite positions require cancellation even while prices are stale.
        if !self.position_direction_ready().await {
            return false;
        }
        // Risk cancellation needs a valuation price, not a fresh last-trade price or
        // a completed replenishment workflow. Never reprice orders through this path.
        if !self.cancel_suspended_opening_orders(config).await {
            return false;
        }
        if self.state.try_order_submission().is_none() {
            return false;
        }
        if self.pause_if_position_limit_reached().await {
            return false;
        }
        if !config.exchange.is_paper() && !self.live_reconciliation_ready() {
            return false;
        }
        self.market_data_ready().await
    }

    async fn capture_paper_window_inputs(
        &self,
        config: &crate::config::AppConfig,
    ) -> Option<PaperWindowInputs> {
        if !config.exchange.is_paper()
            || self
                .last_remainder_change
                .is_some_and(|at| at.elapsed() < Duration::from_secs(30))
        {
            return None;
        }
        let ticker = self.state.ticker.read().await;
        let position = self.state.position.read().await;
        let price_band = |price: Decimal| {
            // Exposure and liquidation thresholds can move inside a grid cell.
            if config.grid.max_position_usdc.is_some()
                || config.grid.min_liquidation_distance_pct.is_some()
            {
                (price, price)
            } else {
                price
                    .checked_div(config.grid.grid_interval)
                    .map(|steps| (steps.floor(), steps.ceil()))
                    .unwrap_or((price, price))
            }
        };
        Some(PaperWindowInputs {
            symbol: config.exchange.symbol.clone(),
            grid: config.grid.clone(),
            rules: self.state.rules.read().await.clone(),
            reference_band: price_band(ticker.grid_reference_price()),
            maker_band: price_band(ticker.last_price),
            position: (
                position.size,
                position.entry_price,
                position.liquidation_price,
                position.leverage,
            ),
            trade_revision: self.state.db.symbol_trade_revision(&config.exchange.symbol),
            pause_epoch: self.state.pause_epoch(),
        })
    }

    async fn paper_window_matches_cache(&self, paper_inputs: Option<&PaperWindowInputs>) -> bool {
        let Some(cached) = self
            .paper_window_cache
            .as_ref()
            .filter(|cached| paper_inputs == Some(&cached.inputs))
        else {
            return false;
        };
        let active = self.state.active_orders.read().await;
        active.len() == cached.orders.len()
            && active.iter().all(|(id, order)| {
                cached
                    .orders
                    .get(id)
                    .is_some_and(|previous| previous.matches_persisted_state(order))
            })
    }

    async fn prepare_window_recovery_and_inventory(
        &mut self,
        config: &crate::config::AppConfig,
    ) -> bool {
        match self.recover_remainder_plan().await {
            Ok(true) => return false,
            Ok(false) => {}
            Err(e) => {
                self.state
                    .add_log("WARN", format!("Remainder recovery pending: {}", e))
                    .await;
                return false;
            }
        }
        if !config.exchange.is_paper()
            && self
                .has_pending_live_pair_intents(&config.exchange.symbol)
                .await
        {
            return false;
        }
        let trimmed = self.trim_closing_orders_to_position().await;
        if trimmed
            && !config.exchange.is_paper()
            && self
                .has_pending_live_pair_intents(&config.exchange.symbol)
                .await
        {
            return false;
        }
        true
    }

    async fn replenish_missing_window_orders(
        &mut self,
        config: &crate::config::AppConfig,
        rules: &SymbolRules,
        current_price: Decimal,
        window: &mut DesiredGridWindow,
    ) -> Option<bool> {
        let grid_interval = config.grid.grid_interval;
        let mut placement_failed = false;
        for (side, desired_prices, active_prices, grid_level) in [
            (
                OrderSide::Buy,
                &window.desired_buy_prices,
                &mut window.active_buy_prices,
                -1,
            ),
            (
                OrderSide::Sell,
                &window.desired_sell_prices,
                &mut window.active_sell_prices,
                1,
            ),
        ] {
            for &target_price in desired_prices {
                // Retain queue priority for active orders still inside the nearest band.
                let already_exists = has_nearby_grid_order(
                    active_prices,
                    target_price,
                    grid_interval,
                    rules.tick_size,
                );
                let on_maker_side = if side == OrderSide::Buy {
                    target_price < current_price
                } else {
                    target_price > current_price
                };

                if !already_exists && on_maker_side {
                    let client_id = new_grid_client_order_id(side);
                    if self
                        .place_grid_order_in_window(
                            side,
                            target_price,
                            client_id,
                            grid_level,
                            None,
                            Some(WindowPlacementContext {
                                last_fill: window.last_fill,
                                precomputed: Some((current_price, desired_prices)),
                            }),
                        )
                        .await
                    {
                        active_prices.push(target_price);
                        if !config.exchange.is_paper()
                            && self
                                .prune_grid_window(
                                    &window.desired_buy_prices,
                                    &window.desired_sell_prices,
                                )
                                .await
                        {
                            return None;
                        }
                    } else {
                        placement_failed = true;
                    }
                }
            }
        }
        Some(placement_failed)
    }

    async fn store_paper_window_cache_if_clean(
        &mut self,
        paper_inputs: Option<PaperWindowInputs>,
        placement_failed: bool,
    ) {
        if let Some(inputs) = paper_inputs.filter(|inputs| {
            !placement_failed
                && inputs.trade_revision == self.state.db.symbol_trade_revision(&inputs.symbol)
        }) {
            // A deferred remainder workflow must retry even without a price move.
            if matches!(self.load_remainder_plan().await, Ok(None)) {
                self.paper_window_cache = Some(PaperWindowCache {
                    inputs,
                    orders: self.state.active_orders.read().await.clone(),
                });
            }
        }
    }

    pub(super) async fn maintain_grid_window_inner(&mut self, skip_unchanged_paper: bool) {
        let config = self.state.config.read().await.clone();
        if !self.window_maintenance_admitted(&config).await {
            return;
        }
        // Keep all admission, freshness and protective cancellation checks above
        // the cache. Only a completed paper maintenance cycle can be reused.
        let paper_inputs = self.capture_paper_window_inputs(&config).await;
        if skip_unchanged_paper && self.paper_window_matches_cache(paper_inputs.as_ref()).await {
            return;
        }
        self.paper_window_cache = None;
        if !self.prepare_window_recovery_and_inventory(&config).await {
            return;
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
        if self
            .cancel_misaligned_window_orders(&rules, grid_interval)
            .await
        {
            // Cancellation can reveal a fill and change reserved levels.
            // Reconcile on the next cycle before allocating any released funds.
            return;
        }
        let Some(mut window) = self
            .compute_desired_window_prices(&config, &rules, current_price)
            .await
        else {
            return;
        };
        if self
            .prune_grid_window(&window.desired_buy_prices, &window.desired_sell_prices)
            .await
        {
            return;
        }

        let Some(placement_failed) = self
            .replenish_missing_window_orders(&config, &rules, current_price, &mut window)
            .await
        else {
            return;
        };

        let closing_prices = if config.grid.direction == GridDirection::Long {
            &window.desired_sell_prices
        } else {
            &window.desired_buy_prices
        };
        if let Err(e) = self.reconcile_remainder(closing_prices).await {
            self.state
                .add_log("WARN", format!("Remainder reconciliation pending: {}", e))
                .await;
            return;
        }
        if self
            .prune_grid_window(&window.desired_buy_prices, &window.desired_sell_prices)
            .await
        {
            return;
        }
        self.store_paper_window_cache_if_clean(paper_inputs, placement_failed)
            .await;
    }

    async fn cancel_misaligned_window_orders(
        &mut self,
        rules: &SymbolRules,
        grid_interval: Decimal,
    ) -> bool {
        let misaligned: Vec<_> = self
            .state
            .active_orders
            .read()
            .await
            .values()
            .filter(|o| is_window_order(o) && !rules.is_grid_price(o.price, grid_interval))
            .cloned()
            .collect();
        if misaligned.is_empty() {
            return false;
        }
        for order in misaligned {
            if *self.state.status.read().await != BotStatus::Running
                || !self.cancel_single_order(&order).await
            {
                return true;
            }
        }
        true
    }

    async fn compute_desired_window_prices(
        &mut self,
        config: &crate::config::AppConfig,
        rules: &SymbolRules,
        current_price: Decimal,
    ) -> Option<DesiredGridWindow> {
        let grid_interval = config.grid.grid_interval;
        let mut active_buy_prices = Vec::new();
        let mut active_sell_prices = Vec::new();
        let mut reserved_buys = Vec::new();
        let mut reserved_sells = Vec::new();
        {
            let active_orders = self.state.active_orders.read().await;
            for order in active_orders.values() {
                let (active, reserved) = if order.side == OrderSide::Buy {
                    (&mut active_buy_prices, &mut reserved_buys)
                } else {
                    (&mut active_sell_prices, &mut reserved_sells)
                };
                active.push(order.price);
                // Partial executions still reserve funds or inventory without
                // displacing the nearest window.
                if !is_window_order(order) {
                    reserved.push(order.price);
                }
            }
        }
        let last_fill = match self.load_last_fill(config).await {
            Ok(last_fill) => last_fill,
            Err(error) => {
                self.state
                    .add_log("ERROR", format!("Could not load last fills: {}", error))
                    .await;
                self.pause_trading().await;
                return None;
            }
        };
        let (mut desired_buy_prices, mut desired_sell_prices) =
            Self::window_prices_with_last_fill(config, rules, current_price, last_fill);
        // Protected orders inside either nearest band occupy their level without
        // extending the band to compensate for far-away protected orders.
        desired_buy_prices.retain(|price| {
            !has_nearby_grid_order(&reserved_buys, *price, grid_interval, rules.tick_size)
        });
        desired_sell_prices.retain(|price| {
            !has_nearby_grid_order(&reserved_sells, *price, grid_interval, rules.tick_size)
        });
        // At the cap in cancel-opening mode, an empty opening band makes the prune
        // cancel opening-side window orders while exits stay on the book.
        if self.opening_suspended_at_limit(config).await {
            if config.grid.direction == GridDirection::Long {
                desired_buy_prices.clear();
            } else {
                desired_sell_prices.clear();
            }
        }
        Some(DesiredGridWindow {
            active_buy_prices,
            active_sell_prices,
            desired_buy_prices,
            desired_sell_prices,
            last_fill,
        })
    }

    /// Confirm stale ordinary orders before reusing their funds or inventory.
    /// Return after cancellation so racing fills reconcile before replenishment.
    pub(super) async fn prune_grid_window(
        &mut self,
        desired_buys: &[Decimal],
        desired_sells: &[Decimal],
    ) -> bool {
        let desired_buy_set: HashSet<Decimal> = desired_buys.iter().copied().collect();
        let desired_sell_set: HashSet<Decimal> = desired_sells.iter().copied().collect();
        let (total, mut obsolete) = {
            let active_guard = self.state.active_orders.read().await;
            let total = active_guard.len();
            let mut occupied: HashSet<_> = active_guard
                .values()
                .filter(|o| !is_window_order(o))
                .map(|o| (o.side, o.price))
                .collect();
            let mut window_orders: Vec<&GridOrder> = active_guard
                .values()
                .filter(|o| is_window_order(o))
                .collect();
            if window_orders.is_empty() {
                return false;
            }
            // Preserve existing queue priority, without preferring any order label.
            window_orders.sort_by(|a, b| {
                a.created_at
                    .cmp(&b.created_at)
                    .then_with(|| a.client_order_id.cmp(&b.client_order_id))
            });
            let obsolete: Vec<GridOrder> = window_orders
                .into_iter()
                .filter(|order| {
                    let desired = if order.side == OrderSide::Buy {
                        &desired_buy_set
                    } else {
                        &desired_sell_set
                    };
                    !desired.contains(&order.price) || !occupied.insert((order.side, order.price))
                })
                .cloned()
                .collect();
            (total, obsolete)
        };
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
    pub(super) async fn trim_closing_orders_to_position(&mut self) -> bool {
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
            direction
                .closing_price_cmp(a.price, b.price)
                .then_with(|| a.client_order_id.cmp(&b.client_order_id))
        });
        let mut remaining = direction.position_quantity(self.state.position.read().await.size);
        let mut canceled_any = false;
        for order in closing_orders {
            if order.quantity <= remaining {
                remaining -= order.quantity;
            } else if self.cancel_single_order(&order).await {
                canceled_any = true;
            } else {
                remaining = Decimal::ZERO;
            }
        }
        canceled_any
    }

    async fn validate_opening_order_capacity(
        &mut self,
        config: &crate::config::AppConfig,
        rules: &SymbolRules,
        ticker: &TickerInfo,
        side: OrderSide,
        price: Decimal,
        full_quantity: Decimal,
    ) -> Option<Decimal> {
        let current_market_price = ticker.last_price;
        let reference_price = ticker.grid_reference_price();
        let exposure = {
            let active_orders = self.state.active_orders.read().await;
            if grid_level_occupied(
                active_orders.values(),
                side,
                price,
                config.grid.grid_interval,
                rules.tick_size,
            ) {
                return None;
            }
            if config.grid.max_position_usdc.is_some() {
                let position = self.state.position.read().await;
                let valuation_price = reference_price.max(current_market_price).max(price);
                let exposure = opening_exposure_usdc(
                    config.grid.direction,
                    position.size,
                    valuation_price,
                    active_orders.values(),
                ) + price * full_quantity;
                Some((position.clone(), exposure))
            } else {
                None
            }
        };
        if let (Some(limit), Some((position, exposure))) = (config.grid.max_position_usdc, exposure)
        {
            let mark_price = self.position_valuation_price(config, &position, ticker);
            let position_value = position.size.abs() * mark_price;
            let projected_position_value =
                (position.size + config.grid.direction.sign() * full_quantity).abs() * mark_price;
            // A full next opening order must fit the filled position even when the position
            // has not yet reached the cap. Pending orders alone still only reserve funds.
            if config.grid.pause_on_max_position
                && mark_price > Decimal::ZERO
                && projected_position_value > limit
                && projected_position_value > position_value
            {
                self.pause_for_position_limit(format!(
                    "持仓名义价值 {}，下一笔{}单（价格 {}，数量 {}）成交后预计持仓名义价值 {} 将超过最大持仓名义价值 {}",
                    position_value,
                    if side == OrderSide::Buy { "买" } else { "卖" },
                    price,
                    full_quantity,
                    projected_position_value,
                    limit
                ))
                .await;
                return None;
            }
            if exposure > limit {
                debug!(
                    "Skipping {} at {}: projected exposure {} exceeds limit {}",
                    side.as_str(),
                    price,
                    exposure,
                    limit
                );
                return None;
            }
        }
        Some(full_quantity)
    }

    async fn validate_and_size_grid_order(
        &mut self,
        config: &crate::config::AppConfig,
        rules: &SymbolRules,
        side: OrderSide,
        price: Decimal,
        placement_ctx: Option<WindowPlacementContext<'_>>,
    ) -> Option<Decimal> {
        let full_quantity = rules.calculate_quantity(price, config.grid.order_amount_usdc)?;
        if !config.exchange.is_paper() && !self.live_reconciliation_ready() {
            warn!("Skipping order: Binance account, position, or open orders are not reconciled recently");
            return None;
        }

        // Enforce Maker pricing check
        let ticker = self.state.ticker.read().await.clone();
        if !self.market_data_ready().await {
            return None;
        }
        let current_market_price = ticker.last_price;
        let reference_price = ticker.grid_reference_price();
        if !rules.is_valid_maker_grid_price(&config.grid, side, price, current_market_price) {
            if !current_market_price.is_zero() {
                if side == OrderSide::Buy && price >= current_market_price {
                    warn!(
                        "Buy price {} >= market price {}, skipping to prevent Taker fill",
                        price, current_market_price
                    );
                } else if side == OrderSide::Sell && price <= current_market_price {
                    warn!(
                        "Sell price {} <= market price {}, skipping to prevent Taker fill",
                        price, current_market_price
                    );
                }
            }
            return None;
        }
        if reference_price > Decimal::ZERO {
            let current_rev = self.state.db.symbol_trade_revision(&config.exchange.symbol);
            if let Some(window_prices) = placement_ctx
                .filter(|ctx| ctx.last_fill.revision == current_rev)
                .and_then(|ctx| ctx.precomputed)
                .filter(|(window_ref, _)| *window_ref == reference_price)
                .map(|(_, prices)| prices)
            {
                if !window_prices.contains(&price) {
                    return None;
                }
            } else {
                let last_fill = match placement_ctx
                    .map(|ctx| ctx.last_fill)
                    .filter(|snapshot| snapshot.revision == current_rev)
                {
                    Some(last_fill) => last_fill,
                    None => match self.load_last_fill(config).await {
                        Ok(last_fill) => last_fill,
                        Err(error) => {
                            self.state
                                .add_log("ERROR", format!("Could not load last fills: {}", error))
                                .await;
                            self.pause_trading().await;
                            return None;
                        }
                    },
                };
                let (buys, sells) =
                    Self::window_prices_with_last_fill(config, rules, reference_price, last_fill);
                let window = if side == OrderSide::Buy { buys } else { sells };
                if !window.contains(&price) {
                    return None;
                }
            }
        }

        if side == config.grid.direction.opening_side() {
            self.validate_opening_order_capacity(config, rules, &ticker, side, price, full_quantity)
                .await
        } else {
            let available = {
                let active_orders = self.state.active_orders.read().await;
                if grid_level_occupied(
                    active_orders.values(),
                    side,
                    price,
                    config.grid.grid_interval,
                    rules.tick_size,
                ) {
                    return None;
                }
                closing_quantity_available(
                    config.grid.direction,
                    self.state.position.read().await.size,
                    active_orders.values(),
                )
            };
            if available >= full_quantity {
                Some(full_quantity)
            } else {
                debug!(
                    "Skipping {} at {}: available {} < full grid quantity {}",
                    side.as_str(),
                    price,
                    available,
                    full_quantity
                );
                None
            }
        }
    }

    /// Place a single grid order (either Maker GTX on Binance or simulated)
    #[cfg(test)]
    pub(super) async fn place_grid_order(
        &mut self,
        side: OrderSide,
        price: Decimal,
        client_order_id: String,
        grid_level: i32,
        paired_client_order_id: Option<String>,
    ) -> bool {
        self.place_grid_order_with_last_fill(
            side,
            price,
            client_order_id,
            grid_level,
            paired_client_order_id,
            None,
        )
        .await
    }

    #[cfg(test)]
    pub(super) async fn place_grid_order_with_last_fill(
        &mut self,
        side: OrderSide,
        price: Decimal,
        client_order_id: String,
        grid_level: i32,
        paired_client_order_id: Option<String>,
        cached_last_fill: Option<LastFillSnapshot>,
    ) -> bool {
        self.place_grid_order_in_window(
            side,
            price,
            client_order_id,
            grid_level,
            paired_client_order_id,
            cached_last_fill.map(|last_fill| WindowPlacementContext {
                last_fill,
                precomputed: None,
            }),
        )
        .await
    }

    pub(super) async fn place_grid_order_in_window(
        &mut self,
        side: OrderSide,
        price: Decimal,
        client_order_id: String,
        grid_level: i32,
        paired_client_order_id: Option<String>,
        placement_ctx: Option<WindowPlacementContext<'_>>,
    ) -> bool {
        if self.state.is_shutting_down() || *self.state.status.read().await != BotStatus::Running {
            return false;
        }
        if !self.position_direction_ready().await {
            return false;
        }
        let config = self.state.config.read().await.clone();
        if side == config.grid.direction.opening_side()
            && self.opening_suspended_at_limit(&config).await
        {
            self.cancel_suspended_opening_orders(&config).await;
            return false;
        }
        let Some(_submission) = self.state.try_order_submission() else {
            return false;
        };
        if self.pause_if_position_limit_reached().await {
            return false;
        }
        let rules = self.state.rules.read().await.clone();
        let Some(quantity) = self
            .validate_and_size_grid_order(&config, &rules, side, price, placement_ctx)
            .await
        else {
            return false;
        };

        let symbol = config.exchange.symbol.clone();
        let now = Utc::now();
        let mut order = GridOrder {
            client_order_id: client_order_id.clone(),
            order_id: None,
            symbol: symbol.clone(),
            side,
            price,
            quantity,
            amount_usdc: price * quantity,
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
            grid_level,
            paired_client_order_id,
            is_take_profit: false,
            purpose: OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };

        if config.exchange.is_paper() {
            if !self.insert_paper_order(&mut order).await {
                return false;
            }
            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(
                    "[PAPER] Placed {} Maker order: price={}, qty={}, id={}",
                    side.as_str(),
                    rules.format_price(price),
                    rules.format_quantity(quantity),
                    client_order_id
                );
            }
            true
        } else {
            let formatted_price = rules.format_price(price);
            let formatted_qty = rules.format_quantity(quantity);
            self.submit_and_reconcile_live_grid_order(super::order_ops::LiveGridOrderSubmission {
                config: &config,
                symbol: &symbol,
                side,
                formatted_price: &formatted_price,
                formatted_qty: &formatted_qty,
                client_order_id,
                order,
            })
            .await
        }
    }
}
