//! A durable cancel/reconcile/submit workflow for explicitly identified long-position remainders.
use super::*;

impl GridTradingEngine {
    pub(super) async fn load_remainder_plan(&self) -> Result<Option<RemainderPlan>> {
        let config = self.state.config.read().await.clone();
        let mode = TradingMode::from_exchange(config.exchange.dry_run, config.exchange.is_testnet);
        self.state
            .db
            .run_blocking(move |db| db.load_remainder_plan(&config.exchange.symbol, mode))
            .await
    }

    async fn persist_remainder_plan(&self, plan: &RemainderPlan) -> Result<()> {
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
        self.execute_remainder_plan(&mut plan).await?;
        // A stale, confirmed-unsubmitted target has no inventory reservation.
        // Let the ordinary window replenish immediately after retiring it.
        Ok(plan.phase != RemainderPhase::Abandoned)
    }

    pub(super) async fn reconcile_remainder(&mut self, desired_prices: &[Decimal]) -> Result<()> {
        let config = self.state.config.read().await.clone();
        if config.grid.sell_window == 0 || *self.state.status.read().await != BotStatus::Running {
            return Ok(());
        }
        let rules = self.state.rules.read().await.clone();
        let orders: Vec<_> = self
            .state
            .active_orders
            .read()
            .await
            .values()
            .cloned()
            .collect();
        let mut sources: Vec<_> = orders
            .iter()
            .filter(|o| {
                o.symbol == config.exchange.symbol
                    && o.side == OrderSide::Sell
                    && o.purpose == OrderPurpose::Remainder
                    && o.paired_client_order_id.is_none()
                    && is_grid_order(&o.client_order_id)
            })
            .cloned()
            .collect();
        sources.sort_by(|a, b| a.client_order_id.cmp(&b.client_order_id));
        let occupied: Vec<_> = orders
            .iter()
            .filter(|o| {
                o.side == OrderSide::Sell
                    && !sources
                        .iter()
                        .any(|s| s.client_order_id == o.client_order_id)
            })
            .map(|o| o.price)
            .collect();
        let price = desired_prices.iter().copied().find(|price| {
            !has_nearby_grid_order(
                &occupied,
                *price,
                config.grid.grid_interval,
                rules.tick_size,
            )
        });
        let Some(price) = price else { return Ok(()) };
        let market = self.state.ticker.read().await.last_price;
        if market <= Decimal::ZERO
            || price <= market
            || price < rules.min_price
            || price > rules.max_price
            || config.grid.max_price.is_some_and(|max| price > max)
            || rules.round_price(price) != price
        {
            return Ok(());
        }
        let Some(full_quantity) = rules.calculate_quantity(price, config.grid.order_amount_usdc)
        else {
            return Ok(());
        };
        let position = self.state.position.read().await.size;
        let other_orders: Vec<_> = orders
            .iter()
            .filter(|o| {
                !sources
                    .iter()
                    .any(|s| s.client_order_id == o.client_order_id)
            })
            .cloned()
            .collect();
        let quantity = rules
            .round_quantity(sell_quantity_available(position, &other_orders))
            .min(full_quantity);
        if quantity < rules.min_qty || quantity * price < rules.min_notional {
            return Ok(());
        }
        // Once a full grid unit can be funded, promote it; later cycles allocate
        // further whole units first and leave at most one final remainder.
        let purpose = if quantity == full_quantity {
            OrderPurpose::Grid
        } else {
            OrderPurpose::Remainder
        };
        if sources.len() == 1 {
            let source = &sources[0];
            if source.price == price && source.quantity == quantity && purpose == source.purpose {
                return Ok(());
            }
            // Coalesce small top-ups rather than repeatedly losing queue priority.
            if quantity > source.quantity
                && purpose == OrderPurpose::Remainder
                && self
                    .last_remainder_change
                    .is_some_and(|at| at.elapsed() < Duration::from_secs(30))
            {
                return Ok(());
            }
        }
        let now = Utc::now();
        let target = GridOrder {
            client_order_id: new_grid_client_order_id(OrderSide::Sell),
            order_id: None,
            symbol: config.exchange.symbol.clone(),
            side: OrderSide::Sell,
            price,
            quantity,
            amount_usdc: price * quantity,
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
            grid_level: 1,
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
        };
        let mut plan = RemainderPlan {
            symbol: config.exchange.symbol,
            mode: TradingMode::from_exchange(config.exchange.dry_run, config.exchange.is_testnet),
            sources,
            target,
            phase: RemainderPhase::Canceling,
        };
        // No cancellation or submission can precede this durable intent.
        self.persist_remainder_plan(&plan).await?;
        self.execute_remainder_plan(&mut plan).await
    }

    async fn execute_remainder_plan(&mut self, plan: &mut RemainderPlan) -> Result<()> {
        if *self.state.status.read().await != BotStatus::Running {
            return Ok(());
        }
        let config = self.state.config.read().await.clone();
        let paper = config.exchange.dry_run;
        let pause_epoch = self.state.pause_epoch();
        if plan.phase == RemainderPhase::Canceling {
            for source in &plan.sources {
                if self.state.pause_epoch() != pause_epoch {
                    return Ok(());
                }
                if paper {
                    if !self.cancel_single_order(source).await {
                        return Err(anyhow!("Could not cancel paper remainder"));
                    }
                } else {
                    // Query first: a prior cancellation may have succeeded even if
                    // its response was lost or the process exited immediately after it.
                    let mut found = self
                        .client
                        .get_order_by_client_id(&plan.symbol, &source.client_order_id)
                        .await?;
                    if !terminal(&found.status) {
                        self.client
                            .cancel_order(&plan.symbol, None, Some(&source.client_order_id))
                            .await?;
                        found = self
                            .client
                            .get_order_by_client_id(&plan.symbol, &source.client_order_id)
                            .await?;
                    }
                    if !terminal(&found.status) {
                        return Err(anyhow!("Source cancellation is not terminal"));
                    }
                    self.settle_remainder_order(source.clone(), found).await?;
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
        }
        if self.state.pause_epoch() != pause_epoch
            || *self.state.status.read().await != BotStatus::Running
        {
            return Ok(());
        }

        // Check the stable target identity before ever attempting a (re)submission.
        if !paper && self.adopt_remainder_target(plan).await? {
            return Ok(());
        }
        let id = plan.target.client_order_id.clone();
        let mode = plan.mode;
        if self
            .state
            .db
            .run_blocking(move |db| db.get_trade_by_client_id(&id, mode))
            .await?
            .is_some()
        {
            plan.phase = RemainderPhase::Complete;
            return self.persist_remainder_plan(plan).await;
        }
        let symbol = plan.symbol.clone();
        if !self
            .state
            .db
            .run_blocking(move |db| db.load_pair_intents(&symbol, mode))
            .await?
            .is_empty()
        {
            return Ok(()); // Paired exits retain priority and their inventory reservation.
        }
        if !paper
            && (!self
                .last_account_sync
                .is_some_and(|at| at.elapsed() < Duration::from_secs(10))
                || !self
                    .last_orders_sync
                    .is_some_and(|at| at.elapsed() < Duration::from_secs(10)))
        {
            return Ok(());
        }
        let rules = self.state.rules.read().await.clone();
        let market = self.state.ticker.read().await.last_price;
        let price = plan.target.price;
        if market <= Decimal::ZERO {
            return Ok(());
        }
        let (_, sells) = self.window_prices(&config, &rules, market).await?;
        if !sells.contains(&price)
            || !rules.is_grid_price(price, config.grid.grid_interval)
            || price <= market
            || rules.round_price(price) != price
            || price < rules.min_price
            || price > rules.max_price
            || config.grid.max_price.is_some_and(|max| price > max)
        {
            // Sources are terminal and the target lookup above confirmed that
            // no order was accepted. Keeping this plan active would freeze both
            // windows indefinitely after a GTX rejection followed by a price rise.
            // Retire it durably; ordinary orders use fresh prices and new IDs.
            // Unknown lookups and accepted targets never reach this branch.
            plan.phase = RemainderPhase::Abandoned;
            self.persist_remainder_plan(plan).await?;
            self.state.add_log("WARN", format!(
                "Stale unsubmitted remainder plan {} retired (target {}, market {}); normal grid replenishment resumed",
                plan.target.client_order_id, price, market,
            )).await;
            return Ok(());
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
            .any(|o| o.side == OrderSide::Sell && o.price == price)
        {
            plan.phase = RemainderPhase::Abandoned;
            return self.persist_remainder_plan(plan).await;
        }
        let available = sell_quantity_available(self.state.position.read().await.size, &orders);
        // Never grow beyond the planned allocation when fills arrive during cancellation.
        let quantity = rules
            .round_quantity(available.min(plan.target.quantity))
            .min(rules.max_qty);
        if quantity < rules.min_qty || quantity * price < rules.min_notional {
            plan.phase = RemainderPhase::Complete;
            return self.persist_remainder_plan(plan).await;
        }
        plan.target.quantity = quantity;
        plan.target.amount_usdc = quantity * price;
        if rules.calculate_quantity(price, config.grid.order_amount_usdc) != Some(quantity) {
            plan.target.purpose = OrderPurpose::Remainder;
        }
        self.persist_remainder_plan(plan).await?;
        if self.state.pause_epoch() != pause_epoch
            || *self.state.status.read().await != BotStatus::Running
        {
            return Ok(());
        }
        if paper {
            plan.target.order_id = Some(rand::random::<i64>().saturating_abs());
            self.state
                .active_orders
                .write()
                .await
                .insert(plan.target.client_order_id.clone(), plan.target.clone());
        } else {
            let price = rules.format_price(price);
            let quantity = rules.format_quantity(quantity);
            let found = self
                .client
                .place_order(NewOrderRequest {
                    symbol: &plan.symbol,
                    side: "SELL",
                    price: &price,
                    quantity: &quantity,
                    client_order_id: &plan.target.client_order_id,
                    post_only: true,
                    reduce_only: true,
                })
                .await?;
            self.settle_remainder_order(plan.target.clone(), found)
                .await?;
        }
        plan.phase = RemainderPhase::Complete;
        self.persist_remainder_plan(plan).await?;
        self.last_remainder_change = Some(Instant::now());
        self.state
            .add_log(
                "INFO",
                format!(
                    "Remainder allocation confirmed: {} sources → SELL {} at {} (id: {})",
                    plan.sources.len(),
                    plan.target.quantity,
                    plan.target.price,
                    plan.target.client_order_id
                ),
            )
            .await;
        Ok(())
    }

    /// Also used before draining paired exits: an uncertain accepted target must
    /// reserve its inventory even when absent from an open-orders snapshot.
    pub(super) async fn adopt_remainder_target(
        &mut self,
        plan: &mut RemainderPlan,
    ) -> Result<bool> {
        let Some(found) = self
            .client
            .lookup_order_by_client_id(&plan.symbol, &plan.target.client_order_id)
            .await?
        else {
            return Ok(false);
        };
        self.settle_remainder_order(plan.target.clone(), found)
            .await?;
        plan.phase = RemainderPhase::Complete;
        self.persist_remainder_plan(plan).await?;
        self.last_remainder_change = Some(Instant::now());
        Ok(true)
    }

    async fn settle_remainder_order(
        &mut self,
        mut order: GridOrder,
        found: BinanceOrderResponse,
    ) -> Result<()> {
        if found.client_order_id != order.client_order_id
            || found.symbol != order.symbol
            || found.side != "SELL"
        {
            return Err(anyhow!("Remainder exchange identity mismatch"));
        }
        order.order_id = Some(found.order_id);
        if terminal(&found.status) {
            if found.executed_qty > Decimal::ZERO {
                order.updated_at = exchange_fill_time(&found);
                order.quantity = found.executed_qty;
                order.price = found
                    .avg_price
                    .filter(|p| *p > Decimal::ZERO)
                    .unwrap_or(found.price);
                order.amount_usdc = order.price * order.quantity;
                if !self.on_order_filled(&mut order).await {
                    return Err(anyhow!("Could not record remainder fill"));
                }
            } else {
                self.delete_managed_order(&order.client_order_id).await?;
                self.state
                    .active_orders
                    .write()
                    .await
                    .remove(&order.client_order_id);
            }
        } else {
            order.price = found.price;
            order.quantity = (found.orig_qty - found.executed_qty).max(Decimal::ZERO);
            order.amount_usdc = order.price * order.quantity;
            order.status = if found.executed_qty > Decimal::ZERO {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::New
            };
            self.save_managed_order(&order).await?;
            self.state
                .active_orders
                .write()
                .await
                .insert(order.client_order_id.clone(), order);
        }
        Ok(())
    }

    /// Called by cancel-all while trading is paused; resolves a possibly accepted
    /// target that has not yet appeared in the open-orders snapshot.
    pub(super) async fn cancel_remainder_plan(&mut self) -> Result<()> {
        let Some(mut plan) = self.load_remainder_plan().await? else {
            return Ok(());
        };
        if plan.mode != TradingMode::Paper && plan.phase == RemainderPhase::Submitting {
            if let Some(mut found) = self
                .client
                .lookup_order_by_client_id(&plan.symbol, &plan.target.client_order_id)
                .await?
            {
                if !terminal(&found.status) {
                    self.client
                        .cancel_order(&plan.symbol, None, Some(&plan.target.client_order_id))
                        .await?;
                    found = self
                        .client
                        .get_order_by_client_id(&plan.symbol, &plan.target.client_order_id)
                        .await?;
                }
                if !terminal(&found.status) {
                    return Err(anyhow!("Remainder target cancellation unresolved"));
                }
                self.settle_remainder_order(plan.target.clone(), found)
                    .await?;
            }
        }
        plan.phase = RemainderPhase::Abandoned;
        self.persist_remainder_plan(&plan).await
    }
}

fn terminal(status: &str) -> bool {
    matches!(
        status,
        "FILLED" | "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" | "REJECTED"
    )
}

#[cfg(test)]
#[path = "remainder_tests.rs"]
mod tests;
