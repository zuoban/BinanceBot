//! A durable cancel/reconcile/submit workflow for explicitly identified closing-position remainders.
use super::*;

struct RemainderTargetSubmission<'a> {
    config: &'a crate::config::AppConfig,
    rules: &'a SymbolRules,
    plan: &'a mut RemainderPlan,
    paper: bool,
    pause_epoch: u64,
    closing_side: OrderSide,
    price: Decimal,
    quantity: Decimal,
}

impl GridTradingEngine {
    pub(super) async fn load_remainder_plan(&self) -> Result<Option<RemainderPlan>> {
        let (symbol, mode) = {
            let config = self.state.config.read().await;
            (
                config.exchange.symbol.clone(),
                config.exchange.trading_mode(),
            )
        };
        self.state
            .db
            .run_blocking(move |db| db.load_remainder_plan(&symbol, mode))
            .await
    }

    pub(super) async fn persist_remainder_plan(&self, plan: &RemainderPlan) -> Result<()> {
        let saved = plan.clone();
        self.state
            .db
            .run_blocking(move |db| db.save_remainder_plan(&saved))
            .await
    }

    /// Return true when a pending workflow owns this maintenance cycle.
    pub(super) async fn recover_remainder_plan(&mut self) -> Result<bool> {
        let Some(mut plan) = self.load_remainder_plan().await? else {
            return Ok(false);
        };
        Box::pin(self.execute_remainder_plan(&mut plan)).await?;
        // A stale, confirmed-unsubmitted target has no inventory reservation.
        // Let the ordinary window replenish immediately after retiring it.
        Ok(plan.phase != RemainderPhase::Abandoned)
    }

    fn build_remainder_target_order(
        symbol: &str,
        closing_side: OrderSide,
        price: Decimal,
        quantity: Decimal,
        purpose: OrderPurpose,
        sources: &[GridOrder],
    ) -> Option<GridOrder> {
        let amount_usdc = price.checked_mul(quantity)?;
        let now = Utc::now();
        Some(GridOrder {
            client_order_id: new_grid_client_order_id(closing_side),
            order_id: None,
            symbol: symbol.to_owned(),
            side: closing_side,
            price,
            quantity,
            amount_usdc,
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
            grid_level: if closing_side == OrderSide::Buy {
                -1
            } else {
                1
            },
            paired_client_order_id: None,
            is_take_profit: false,
            purpose,
            merge_sources: sources
                .iter()
                .map(|o| OrderSource {
                    client_order_id: o.client_order_id.clone(),
                    price: o.price,
                    quantity: o.quantity,
                })
                .collect(),
        })
    }

    async fn plan_remainder_target(
        &self,
        config: &crate::config::AppConfig,
        rules: &SymbolRules,
        desired_prices: &[Decimal],
    ) -> Option<(Vec<GridOrder>, GridOrder)> {
        let direction = config.grid.direction;
        let closing_side = direction.closing_side();
        let (mut sources, other_orders): (Vec<_>, Vec<_>) = self
            .state
            .active_orders
            .read()
            .await
            .values()
            .cloned()
            .partition(|o| {
                o.symbol == config.exchange.symbol
                    && o.side == closing_side
                    && o.purpose == OrderPurpose::Remainder
                    && o.paired_client_order_id.is_none()
                    && is_grid_order(&o.client_order_id)
            });
        sources.sort_by(|a, b| a.client_order_id.cmp(&b.client_order_id));
        let occupied: Vec<_> = other_orders
            .iter()
            .filter(|o| o.side == closing_side)
            .map(|o| o.price)
            .collect();
        let price = desired_prices.iter().copied().find(|price| {
            !has_nearby_grid_order(
                &occupied,
                *price,
                config.grid.grid_interval,
                rules.tick_size,
            )
        })?;
        let market = self.state.ticker.read().await.last_price;
        if !rules.is_valid_maker_grid_price(&config.grid, closing_side, price, market) {
            return None;
        }
        let position = self.state.position.read().await.size;
        let available = closing_quantity_available(direction, position, &other_orders);
        let (quantity, purpose) =
            rules.plan_remainder_quantity(price, config.grid.order_amount_usdc, available)?;
        if sources.len() == 1 {
            let source = &sources[0];
            if source.price == price && source.quantity == quantity && purpose == source.purpose {
                return None;
            }
            if quantity > source.quantity
                && purpose == OrderPurpose::Remainder
                && self
                    .last_remainder_change
                    .is_some_and(|at| at.elapsed() < Duration::from_secs(30))
            {
                return None;
            }
        }
        let target = Self::build_remainder_target_order(
            &config.exchange.symbol,
            closing_side,
            price,
            quantity,
            purpose,
            &sources,
        )?;
        Some((sources, target))
    }

    pub(super) async fn reconcile_remainder(&mut self, desired_prices: &[Decimal]) -> Result<()> {
        let config = self.state.config.read().await.clone();
        let closing_side = config.grid.direction.closing_side();
        let closing_window = if closing_side == OrderSide::Buy {
            config.grid.buy_window
        } else {
            config.grid.sell_window
        };
        if self.state.is_shutting_down()
            || closing_window == 0
            || *self.state.status.read().await != BotStatus::Running
            || !self.state.ticker.read().await.prices_are_fresh(Utc::now())
        {
            return Ok(());
        }
        let rules = self.state.rules.read().await.clone();
        let Some((sources, target)) = self
            .plan_remainder_target(&config, &rules, desired_prices)
            .await
        else {
            return Ok(());
        };
        let mut plan = RemainderPlan {
            mode: config.exchange.trading_mode(),
            symbol: config.exchange.symbol,
            sources,
            target,
            phase: RemainderPhase::Canceling,
        };
        // No cancellation or submission can precede this durable intent.
        self.persist_remainder_plan(&plan).await?;
        Box::pin(self.execute_remainder_plan(&mut plan)).await
    }

    async fn validate_remainder_submission_window(
        &self,
        config: &crate::config::AppConfig,
        rules: &SymbolRules,
        plan: &mut RemainderPlan,
    ) -> Result<Option<(OrderSide, Decimal, Decimal)>> {
        let ticker = self.state.ticker.read().await.clone();
        if !ticker.prices_are_fresh(Utc::now()) || ticker.last_price <= Decimal::ZERO {
            return Ok(None);
        }
        let market = ticker.last_price;
        let price = plan.target.price;
        let direction = config.grid.direction;
        let closing_side = direction.closing_side();
        let (buys, sells) = self
            .window_prices(config, rules, ticker.grid_reference_price())
            .await?;
        let window = if closing_side == OrderSide::Buy {
            buys
        } else {
            sells
        };
        if plan.target.side != closing_side
            || !window.contains(&price)
            || !rules.is_valid_maker_grid_price(&config.grid, plan.target.side, price, market)
        {
            plan.phase = RemainderPhase::Abandoned;
            self.persist_remainder_plan(plan).await?;
            self.state.add_log("WARN", format!(
                "Stale unsubmitted remainder plan {} retired (target {}, market {}); normal grid replenishment resumed",
                plan.target.client_order_id, price, market,
            )).await;
            return Ok(None);
        }
        let orders: Vec<_> = self
            .state
            .active_orders
            .read()
            .await
            .values()
            .cloned()
            .collect();
        if orders
            .iter()
            .any(|o| o.side == closing_side && o.price == price)
        {
            plan.phase = RemainderPhase::Abandoned;
            self.persist_remainder_plan(plan).await?;
            return Ok(None);
        }
        let available =
            closing_quantity_available(direction, self.state.position.read().await.size, &orders);
        let quantity = rules
            .round_quantity(available.min(plan.target.quantity))
            .min(rules.max_qty);
        if quantity < rules.min_qty
            || quantity
                .checked_mul(price)
                .is_none_or(|notional| notional < rules.min_notional)
        {
            plan.phase = RemainderPhase::Complete;
            self.persist_remainder_plan(plan).await?;
            return Ok(None);
        }
        Ok(Some((closing_side, price, quantity)))
    }

    async fn execute_remainder_plan(&mut self, plan: &mut RemainderPlan) -> Result<()> {
        if self.state.is_shutting_down() || *self.state.status.read().await != BotStatus::Running {
            return Ok(());
        }
        let config = self.state.config.read().await.clone();
        let paper = config.exchange.is_paper();
        let pause_epoch = self.state.pause_epoch();
        if plan.phase == RemainderPhase::Canceling
            && !self
                .cancel_remainder_plan_sources(plan, paper, pause_epoch)
                .await?
        {
            return Ok(());
        }
        if self.state.pause_epoch() != pause_epoch
            || *self.state.status.read().await != BotStatus::Running
        {
            return Ok(());
        }
        if self.pause_if_position_limit_reached().await {
            return Ok(());
        }

        // Check the stable target identity before ever attempting a (re)submission.
        if !paper && self.adopt_remainder_target(plan).await? {
            return Ok(());
        }
        let mode = plan.mode;
        if self
            .has_recorded_order(&plan.target.client_order_id, mode)
            .await?
        {
            plan.phase = RemainderPhase::Complete;
            return self.persist_remainder_plan(plan).await;
        }
        if !self.load_pair_intents(&plan.symbol, mode).await?.is_empty() {
            return Ok(()); // Paired exits retain priority and their inventory reservation.
        }
        if !paper && !self.sync_timestamps_fresh() {
            return Ok(());
        }
        let rules = self.state.rules.read().await.clone();
        let Some((closing_side, price, quantity)) = self
            .validate_remainder_submission_window(&config, &rules, plan)
            .await?
        else {
            return Ok(());
        };
        self.submit_remainder_plan_target(RemainderTargetSubmission {
            config: &config,
            rules: &rules,
            plan,
            paper,
            pause_epoch,
            closing_side,
            price,
            quantity,
        })
        .await
    }

    async fn cancel_remainder_plan_sources(
        &mut self,
        plan: &mut RemainderPlan,
        paper: bool,
        pause_epoch: u64,
    ) -> Result<bool> {
        for source in &plan.sources {
            if self.state.pause_epoch() != pause_epoch {
                return Ok(false);
            }
            if paper {
                if !self.cancel_single_order(source).await {
                    return Err(anyhow!("Could not cancel paper remainder"));
                }
            } else {
                // Query first: a prior cancellation may have succeeded even if
                // its response was lost or the process exited immediately after it.
                let found = self
                    .client
                    .get_order_by_client_id(&plan.symbol, &source.client_order_id)
                    .await?;
                self.cancel_and_settle_remainder_order(source.clone(), found, None)
                    .await?;
            }
        }
        // All sources are now terminal. Live quantities must be recalculated
        // from the exchange position, never from the pre-cancellation sum.
        if !paper && !self.sync_account_and_position().await {
            return Err(anyhow!(
                "Position refresh failed after remainder cancellation"
            ));
        }
        plan.phase = RemainderPhase::Submitting;
        self.persist_remainder_plan(plan).await?;
        Ok(true)
    }

    async fn place_live_remainder_target(
        &mut self,
        rules: &SymbolRules,
        plan: &mut RemainderPlan,
        closing_side: OrderSide,
        price: Decimal,
        quantity: Decimal,
    ) -> Result<bool> {
        let formatted_price = rules.format_price(price);
        let formatted_qty = rules.format_quantity(quantity);
        let Some(exchange_guard) = self.state.try_exchange_submission() else {
            return Ok(false);
        };
        let (found, mut fill_guard) = self
            .submit_exchange_order(
                exchange_guard,
                NewOrderRequest {
                    symbol: &plan.symbol,
                    side: closing_side.as_str(),
                    price: &formatted_price,
                    quantity: &formatted_qty,
                    client_order_id: &plan.target.client_order_id,
                    post_only: true,
                    reduce_only: true,
                },
            )
            .await;
        let found = found?;
        self.settle_remainder_order(plan.target.clone(), found, Some(&mut *plan))
            .await?;
        if let Some(guard) = &mut fill_guard {
            guard.resolve();
        }
        Ok(true)
    }

    async fn submit_remainder_plan_target(
        &mut self,
        target: RemainderTargetSubmission<'_>,
    ) -> Result<()> {
        let RemainderTargetSubmission {
            config,
            rules,
            plan,
            paper,
            pause_epoch,
            closing_side,
            price,
            quantity,
        } = target;
        let Some(_submission) = self.state.try_order_submission() else {
            return Ok(());
        };
        let Some(amount_usdc) = quantity.checked_mul(price) else {
            return Ok(());
        };
        plan.target.quantity = quantity;
        plan.target.amount_usdc = amount_usdc;
        if rules.calculate_quantity(price, config.grid.order_amount_usdc) != Some(quantity) {
            plan.target.purpose = OrderPurpose::Remainder;
        }
        self.persist_remainder_plan(plan).await?;
        if self.state.is_shutting_down()
            || !self.state.ticker.read().await.prices_are_fresh(Utc::now())
            || self.state.pause_epoch() != pause_epoch
            || *self.state.status.read().await != BotStatus::Running
        {
            return Ok(());
        }
        if paper {
            if !self.insert_paper_order(&mut plan.target).await {
                return Ok(());
            }
        } else if !self
            .place_live_remainder_target(rules, plan, closing_side, price, quantity)
            .await?
        {
            return Ok(());
        }
        plan.phase = RemainderPhase::Complete;
        self.persist_remainder_plan(plan).await?;
        self.last_remainder_change = Some(Instant::now());
        self.state
            .add_log(
                "INFO",
                format!(
                    "Remainder allocation confirmed: {} sources → {} {} at {} (id: {})",
                    plan.sources.len(),
                    closing_side.as_str(),
                    plan.target.quantity,
                    plan.target.price,
                    plan.target.client_order_id
                ),
            )
            .await;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
