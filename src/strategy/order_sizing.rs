use super::*;

#[derive(Clone, Copy)]
pub(super) struct WindowPlacementContext<'a> {
    pub(super) last_fill: LastFillSnapshot,
    pub(super) precomputed: Option<(Decimal, &'a [Decimal])>,
}

impl GridTradingEngine {
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
        let order_notional = price.checked_mul(full_quantity)?;
        let exposure = {
            let active_orders = self.state.active_orders.read().await;
            let interval = config.grid.grid_interval;
            if grid_level_occupied(
                active_orders.values(),
                side,
                price,
                interval,
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
            let mark_price = self.position_valuation_price(config, &position, ticker);
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
                return None;
            }
            if exposure > limit || exposure == Decimal::MAX {
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
            let available = {
                let active_orders = self.state.active_orders.read().await;
                let interval = config.grid.grid_interval;
                if grid_level_occupied(
                    active_orders.values(),
                    side,
                    price,
                    interval,
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
        self.place_grid_order_in_window(
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
        let ctx = cached_last_fill.map(|last_fill| WindowPlacementContext {
            last_fill,
            precomputed: None,
        });
        self.place_grid_order_in_window(
            side,
            price,
            client_order_id,
            grid_level,
            paired_client_order_id,
            ctx,
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
        let Some(amount_usdc) = price.checked_mul(quantity) else {
            return false;
        };
        let now = Utc::now();
        let mut order = GridOrder {
            client_order_id: client_order_id.clone(),
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
