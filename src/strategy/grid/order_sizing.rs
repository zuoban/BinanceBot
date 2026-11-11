use super::*;

#[derive(Clone, Copy)]
pub(super) struct WindowPlacementContext<'a> {
    pub(super) last_fill: LastFillSnapshot,
    pub(super) precomputed: Option<(Decimal, &'a [Decimal])>,
}

pub(super) struct GridOrderSpec {
    pub(super) side: OrderSide,
    pub(super) price: Decimal,
    pub(super) client_order_id: String,
    pub(super) grid_level: i32,
    pub(super) paired_client_order_id: Option<String>,
}
impl GridTradingEngine {
    async fn check_opening_position_limit(
        &mut self,
        config: &crate::config::AppConfig,
        ticker: &TickerInfo,
        position: &PositionInfo,
        limit: Decimal,
        order: (OrderSide, Decimal, Decimal, Decimal),
    ) -> bool {
        let (side, price, full_quantity, exposure) = order;
        let mark_price =
            self.position_valuation_price(config.exchange.sync_interval_secs, position, ticker);
        let position_value = position
            .size
            .abs()
            .checked_mul(mark_price)
            .unwrap_or(Decimal::MAX);
        let projected_position_value = config
            .grid
            .direction
            .sign()
            .checked_mul(full_quantity)
            .and_then(|delta| position.size.checked_add(delta))
            .and_then(|projected_size| projected_size.abs().checked_mul(mark_price))
            .unwrap_or(Decimal::MAX);
        // A full next opening order must fit the filled position even when the position
        // has not yet reached the cap. Pending orders alone still only reserve funds.
        if config.grid.pause_on_max_position
            && mark_price > Decimal::ZERO
            && projected_position_value > limit
            && (projected_position_value > position_value
                || projected_position_value == Decimal::MAX)
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
            return false;
        }
        if exposure > limit || exposure == Decimal::MAX {
            debug!(
                "Skipping {} at {}: projected exposure {} exceeds limit {}",
                side.as_str(),
                price,
                exposure,
                limit
            );
            return false;
        }
        true
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
        let order_notional = price.checked_mul(full_quantity)?;
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
                let valuation_price = ticker
                    .grid_reference_price()
                    .max(ticker.last_price)
                    .max(price);
                let exposure = opening_exposure_usdc(
                    config.grid.direction,
                    position.size,
                    valuation_price,
                    active_orders.values(),
                )
                .checked_add(order_notional)
                .unwrap_or(Decimal::MAX);
                Some((position.clone(), exposure))
            } else {
                None
            }
        };
        if let (Some(limit), Some((position, exposure))) = (config.grid.max_position_usdc, exposure)
        {
            if !self
                .check_opening_position_limit(
                    config,
                    ticker,
                    &position,
                    limit,
                    (side, price, full_quantity, exposure),
                )
                .await
            {
                return None;
            }
        }
        Some(full_quantity)
    }

    async fn price_in_active_grid_window(
        &mut self,
        config: &crate::config::AppConfig,
        rules: &SymbolRules,
        side: OrderSide,
        price: Decimal,
        reference_price: Decimal,
        placement_ctx: Option<WindowPlacementContext<'_>>,
    ) -> bool {
        if reference_price <= Decimal::ZERO {
            return true;
        }
        let current_rev = self.state.db.symbol_trade_revision(&config.exchange.symbol);
        if let Some(window_prices) = placement_ctx
            .filter(|ctx| ctx.last_fill.revision == current_rev)
            .and_then(|ctx| ctx.precomputed)
            .filter(|(window_ref, _)| *window_ref == reference_price)
            .map(|(_, prices)| prices)
        {
            return window_prices.contains(&price);
        }
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
                    return false;
                }
            },
        };
        let (buys, sells) =
            Self::window_prices_with_last_fill(config, rules, reference_price, last_fill);
        let window = if side == OrderSide::Buy { buys } else { sells };
        window.contains(&price)
    }

    async fn validate_closing_order_capacity(
        &self,
        config: &crate::config::AppConfig,
        rules: &SymbolRules,
        side: OrderSide,
        price: Decimal,
        full_quantity: Decimal,
    ) -> Option<Decimal> {
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
        let ticker = self.state.ticker.read().await.clone();
        if !self.market_data_ready().await {
            return None;
        }
        let current_market_price = ticker.last_price;
        let reference_price = ticker.grid_reference_price();
        if !rules.is_valid_maker_grid_price(&config.grid, side, price, current_market_price) {
            if !current_market_price.is_zero() && !side.is_maker_price(price, current_market_price)
            {
                warn!(
                    "{} price {} crosses market price {}, skipping to prevent Taker fill",
                    if side == OrderSide::Buy {
                        "Buy"
                    } else {
                        "Sell"
                    },
                    price,
                    current_market_price
                );
            }
            return None;
        }
        if !self
            .price_in_active_grid_window(config, rules, side, price, reference_price, placement_ctx)
            .await
        {
            return None;
        }
        if side == config.grid.direction.opening_side() {
            self.validate_opening_order_capacity(config, rules, &ticker, side, price, full_quantity)
                .await
        } else {
            self.validate_closing_order_capacity(config, rules, side, price, full_quantity)
                .await
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
        let config = self.state.config.read().await.clone();
        let rules = self.state.rules.read().await.clone();
        self.place_grid_order_in_window(
            &config,
            &rules,
            GridOrderSpec {
                side,
                price,
                client_order_id,
                grid_level,
                paired_client_order_id,
            },
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
        let config = self.state.config.read().await.clone();
        let rules = self.state.rules.read().await.clone();
        let ctx = cached_last_fill.map(|last_fill| WindowPlacementContext {
            last_fill,
            precomputed: None,
        });
        self.place_grid_order_in_window(
            &config,
            &rules,
            GridOrderSpec {
                side,
                price,
                client_order_id,
                grid_level,
                paired_client_order_id,
            },
            ctx,
        )
        .await
    }

    pub(super) async fn place_grid_order_in_window(
        &mut self,
        config: &crate::config::AppConfig,
        rules: &SymbolRules,
        spec: GridOrderSpec,
        placement_ctx: Option<WindowPlacementContext<'_>>,
    ) -> bool {
        let GridOrderSpec {
            side,
            price,
            client_order_id,
            grid_level,
            paired_client_order_id,
        } = spec;
        if self.state.is_shutting_down() || *self.state.status.read().await != BotStatus::Running {
            return false;
        }
        if !self.position_direction_ready().await {
            return false;
        }
        let risk = RiskConfigSnapshot::from_config(config);
        if side == risk.grid.direction.opening_side() && self.opening_suspended_at_limit(risk).await
        {
            self.cancel_suspended_opening_orders(risk).await;
            return false;
        }
        let Some(_submission) = self.state.try_order_submission() else {
            return false;
        };
        if self.pause_if_position_limit_reached().await {
            return false;
        }
        let Some(quantity) = self
            .validate_and_size_grid_order(config, rules, side, price, placement_ctx)
            .await
        else {
            return false;
        };
        let Some(amount_usdc) = price.checked_mul(quantity) else {
            return false;
        };
        let symbol = &config.exchange.symbol;
        let now = Utc::now();
        let order = GridOrder {
            client_order_id,
            order_id: None,
            symbol: symbol.clone(),
            side,
            price,
            quantity,
            amount_usdc,
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
            grid_level,
            paired_client_order_id,
            is_take_profit: false,
            purpose: OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        self.submit_sized_grid_order(config, rules, risk.is_paper, order)
            .await
    }

    async fn submit_sized_grid_order(
        &mut self,
        config: &crate::config::AppConfig,
        rules: &SymbolRules,
        is_paper: bool,
        mut order: GridOrder,
    ) -> bool {
        if is_paper {
            if !self.insert_paper_order(&mut order).await {
                return false;
            }
            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(
                    "[PAPER] Placed {} Maker order: price={}, qty={}, id={}",
                    order.side.as_str(),
                    rules.format_price(order.price),
                    rules.format_quantity(order.quantity),
                    order.client_order_id
                );
            }
            return true;
        }
        let formatted_price = rules.format_price(order.price);
        let formatted_qty = rules.format_quantity(order.quantity);
        let client_order_id = order.client_order_id.clone();
        let side = order.side;
        let submission = LiveGridOrderSubmission {
            config,
            symbol: &config.exchange.symbol,
            side,
            formatted_price: &formatted_price,
            formatted_qty: &formatted_qty,
            client_order_id,
            order,
        };
        Box::pin(self.submit_and_reconcile_live_grid_order(submission)).await
    }
}
