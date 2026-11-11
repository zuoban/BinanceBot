use super::*;

pub(super) struct LiveGridOrderSubmission<'a> {
    pub(super) config: &'a crate::config::AppConfig,
    pub(super) symbol: &'a str,
    pub(super) side: OrderSide,
    pub(super) formatted_price: &'a str,
    pub(super) formatted_qty: &'a str,
    pub(super) client_order_id: String,
    pub(super) order: GridOrder,
}

impl GridTradingEngine {
    pub(super) async fn lookup_order(
        &self,
        order: &GridOrder,
    ) -> Result<Option<BinanceOrderResponse>> {
        match order.order_id {
            Some(id) => self.client.get_order(&order.symbol, id).await.map(Some),
            None => {
                self.client
                    .lookup_order_by_client_id(&order.symbol, &order.client_order_id)
                    .await
            }
        }
    }

    pub(super) async fn insert_paper_order(&self, order: &mut GridOrder) -> bool {
        let mut active_orders = self.state.active_orders.write().await;
        if self.state.is_shutting_down() {
            return false;
        }
        order.order_id = Some(rand::random::<i64>().saturating_abs());
        active_orders.insert(order.client_order_id.clone(), order.clone());
        true
    }

    pub(super) async fn submit_exchange_order(
        &self,
        exchange_guard: crate::server::state::ExchangeSubmissionGuard,
        request: NewOrderRequest<'_>,
    ) -> (
        std::result::Result<BinanceOrderResponse, ExchangeError>,
        Option<crate::server::state::PendingFillReconciliation>,
    ) {
        let result = self.client.place_order(request).await;
        let mut fill_guard = if result.as_ref().is_ok_and(|response| {
            response.executed_qty > Decimal::ZERO || unresolved_filled_quantity(response)
        }) || result
            .as_ref()
            .is_err_and(|error| error.execution_status_unknown())
        {
            Some(self.state.publish_fill_reconciliation())
        } else {
            None
        };
        exchange_guard.complete();
        if let Some(guard) = &mut fill_guard {
            guard.acquire().await;
        }
        (result, fill_guard)
    }

    pub(super) async fn load_managed_orders(&self, symbol: &str) -> Result<Vec<GridOrder>> {
        let symbol = symbol.to_string();
        self.state
            .db
            .run_blocking(move |db| db.load_managed_orders(&symbol))
            .await
    }

    pub(super) async fn insert_restored_managed_orders(&self, restored: Vec<GridOrder>) {
        let mut active = self.state.active_orders.write().await;
        for order in restored
            .into_iter()
            .filter(|order| is_grid_order(&order.client_order_id))
        {
            active.entry(order.client_order_id.clone()).or_insert(order);
        }
    }

    pub(super) async fn save_managed_order(&self, order: &GridOrder) -> Result<()> {
        let order = order.clone();
        self.state
            .db
            .run_blocking(move |db| db.save_managed_order(&order))
            .await
    }

    pub(super) async fn save_managed_orders(&self, orders: Vec<GridOrder>) -> Result<()> {
        self.state
            .db
            .run_blocking(move |db| db.save_managed_orders(&orders))
            .await
    }

    pub(super) async fn has_recorded_order(
        &self,
        client_order_id: &str,
        mode: TradingMode,
    ) -> Result<bool> {
        let client_order_id = client_order_id.to_string();
        self.state
            .db
            .run_blocking(move |db| db.has_recorded_order(&client_order_id, mode))
            .await
    }

    pub(super) async fn delete_managed_order(&self, client_order_id: &str) -> Result<()> {
        let client_order_id = client_order_id.to_string();
        self.state
            .db
            .run_blocking(move |db| db.delete_managed_order(&client_order_id))
            .await
    }

    pub(super) async fn remove_managed_and_active_order(
        &self,
        client_order_id: &str,
    ) -> Result<()> {
        self.delete_managed_order(client_order_id).await?;
        self.state
            .active_orders
            .write()
            .await
            .remove(client_order_id);
        Ok(())
    }

    // Direct order lookups can observe a fill absent from the open-orders
    // snapshot. Preserve the old reservation until both risk inputs are fresh.
    pub(super) async fn prepare_partial_adoption(
        &mut self,
        order: &mut GridOrder,
        found: &BinanceOrderResponse,
    ) -> bool {
        if found.executed_qty <= Decimal::ZERO {
            return true;
        }
        let remaining = found.remaining_qty();
        let previous = self
            .state
            .active_orders
            .read()
            .await
            .get(&order.client_order_id)
            .cloned();
        if self
            .last_account_sync
            .is_some_and(|at| at.elapsed() < self.sync_freshness())
            && previous.as_ref().is_some_and(|old| {
                old.status == OrderStatus::PartiallyFilled && old.quantity == remaining
            })
        {
            return true;
        }
        let pause_epoch = self.state.pause_epoch();
        self.invalidate_account_and_position_caches().await;
        if self.refresh_position_then_account(false).await
            && self.state.pause_epoch() == pause_epoch
        {
            return true;
        }

        // An unknown accepted order must remain durable and visible to cancel
        // and reconciliation even when the exchange cannot refresh risk data.
        let quantity = previous
            .as_ref()
            .map_or(found.orig_qty, |old| old.quantity.max(remaining));
        let mut reserved = previous.unwrap_or_else(|| order.clone());
        reserved.order_id = Some(found.order_id);
        reserved.quantity = quantity;
        reserved.amount_usdc = reserved
            .price
            .checked_mul(reserved.quantity)
            .unwrap_or(Decimal::MAX);
        reserved.status = OrderStatus::PartiallyFilled;
        self.state
            .active_orders
            .write()
            .await
            .insert(reserved.client_order_id.clone(), reserved.clone());
        if let Err(error) = self.save_managed_order(&reserved).await {
            error!(
                "Could not persist partial order {}: {}",
                reserved.client_order_id, error
            );
            self.pause_trading().await;
        }
        self.last_orders_sync = None;
        self.report_reconciliation_state(true).await;
        false
    }

    pub(super) async fn adopt_open_exchange_order(
        &mut self,
        order: &mut GridOrder,
        original: GridOrder,
        found: &BinanceOrderResponse,
    ) -> bool {
        order.quantity = found.remaining_qty();
        order.amount_usdc = order
            .price
            .checked_mul(order.quantity)
            .unwrap_or(Decimal::MAX);
        order.status = found.open_order_status();
        if let Err(error) = self.save_managed_order(order).await {
            error!(
                "Could not persist adopted order {}: {}",
                order.client_order_id, error
            );
            self.state
                .active_orders
                .write()
                .await
                .entry(original.client_order_id.clone())
                .or_insert(original);
            self.last_orders_sync = None;
            self.report_reconciliation_state(true).await;
            self.pause_trading().await;
            return false;
        }
        self.state
            .active_orders
            .write()
            .await
            .insert(order.client_order_id.clone(), order.clone());
        if found.executed_qty > Decimal::ZERO {
            self.state.check_global_position_limit().await;
        }
        true
    }

    pub(super) async fn prepare_and_adopt_open_order(
        &mut self,
        order: &mut GridOrder,
        original: GridOrder,
        found: &BinanceOrderResponse,
    ) -> bool {
        self.prepare_partial_adoption(order, found).await
            && self.adopt_open_exchange_order(order, original, found).await
    }

    pub(super) async fn retain_uncertain_active_order(&self, order: &GridOrder) -> Result<()> {
        self.state
            .active_orders
            .write()
            .await
            .entry(order.client_order_id.clone())
            .or_insert_with(|| order.clone());
        if let Err(error) = self.save_managed_order(order).await {
            self.pause_trading().await;
            return Err(error);
        }
        Ok(())
    }

    /// Executed when an order is confirmed filled outside a batched observation.
    pub(super) async fn on_order_filled(&mut self, order: &mut GridOrder) -> bool {
        if !self.state.config.read().await.exchange.is_paper() {
            order.status = OrderStatus::Filled;
            self.invalidate_account_and_position_caches().await;
            if !self.sync_account_and_position().await {
                let missing = {
                    let mut active = self.state.active_orders.write().await;
                    if active.contains_key(&order.client_order_id) {
                        false
                    } else {
                        active.insert(order.client_order_id.clone(), order.clone());
                        true
                    }
                };
                if missing {
                    if let Err(error) = self.save_managed_order(order).await {
                        error!(%error, "Could not persist unverified terminal order");
                        self.pause_trading().await;
                    }
                }
                return false;
            }
        }
        self.commit_order_fill(order).await
    }

    /// Risk refresh and its global check must precede this durable commit.
    pub(super) async fn commit_order_fill(&mut self, order: &mut GridOrder) -> bool {
        order.status = OrderStatus::Filled;
        let config = self.state.config.read().await.clone();
        if config.exchange.is_paper() {
            order.updated_at = Utc::now();
        }
        let mode = config.exchange.trading_mode();
        let recorded_id = order.client_order_id.clone();
        let recorded = if config.exchange.is_paper() {
            self.state.has_recorded_paper_order(recorded_id, mode).await
        } else {
            self.has_recorded_order(&recorded_id, mode).await
        };
        match recorded {
            Ok(true) => {
                // Retire durable recovery metadata before releasing its reservation.
                if !config.exchange.is_paper() {
                    if let Err(e) = self.delete_managed_order(&order.client_order_id).await {
                        error!("Could not clear already-recorded order: {}", e);
                        return false;
                    }
                }
                self.state
                    .active_orders
                    .write()
                    .await
                    .remove(&order.client_order_id);
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

        let Some((cycle_profit, is_completed_cycle, note)) =
            self.resolve_paired_cycle_fill(&config, order, mode).await
        else {
            return false;
        };
        let mut simulated_pnl = Decimal::ZERO;
        let mut paper_state = None;
        if config.exchange.is_paper() {
            let mut pos = self.state.position.read().await.clone();
            (pos.size, pos.entry_price, simulated_pnl) = crate::types::apply_fill_to_position(
                pos.size,
                pos.entry_price,
                order.side,
                order.price,
                order.quantity,
            );
            let ticker = self.state.ticker.read().await.clone();
            let mut acc = self.state.account.read().await.clone();
            acc.total_wallet_balance = acc
                .total_wallet_balance
                .checked_add(simulated_pnl)
                .unwrap_or(acc.total_wallet_balance);
            update_paper_account(&mut pos, &mut acc, &ticker);
            paper_state = Some((pos, acc));
        }

        let (realized_pnl, commission, is_maker, pnl_verified) = if config.exchange.is_paper() {
            (simulated_pnl, Decimal::ZERO, true, true)
        } else {
            // Unknown PnL stays explicitly pending until complete exchange fills
            // are verified. Pagination must never hold the global risk barrier.
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
            amount_usdc: order
                .price
                .checked_mul(order.quantity)
                .unwrap_or(order.amount_usdc),
            realized_pnl,
            commission,
            pnl_verified,
            is_maker,
            timestamp: order.updated_at,
            note: note.clone(),
        };

        // Persist trade to SQLite and update runtime stats
        let recorded = self
            .state
            .record_trade_with_paper_state(
                trade,
                is_completed_cycle.then_some(cycle_profit),
                None,
                order.paired_client_order_id.clone(),
                paper_state,
            )
            .await;
        if !recorded {
            self.pause_trading().await;
            self.state
                .active_orders
                .write()
                .await
                .entry(order.client_order_id.clone())
                .or_insert_with(|| order.clone());
            return false;
        }
        if !config.exchange.is_paper() {
            self.state
                .active_orders
                .write()
                .await
                .remove(&order.client_order_id);
            if let Some(order_id) = order.order_id {
                if self.pnl_order_ids.len() >= 4096 {
                    self.pnl_order_ids.clear();
                }
                self.pnl_order_ids
                    .insert(order.client_order_id.clone(), order_id);
            }
        }
        self.state
            .add_log(
                if is_completed_cycle {
                    "SUCCESS"
                } else {
                    "INFO"
                },
                note,
            )
            .await;
        true
    }

    async fn reconcile_accepted_live_submission(
        &mut self,
        submission: LiveGridOrderSubmission<'_>,
        resp: BinanceOrderResponse,
        fill_guard: &mut Option<crate::server::state::PendingFillReconciliation>,
    ) -> bool {
        let LiveGridOrderSubmission {
            config,
            side,
            formatted_price,
            formatted_qty,
            mut order,
            ..
        } = submission;
        if unresolved_filled_quantity(&resp) {
            self.block_reconciliation_and_invalidate_caches().await;
        }
        order.order_id = Some(resp.order_id);
        order.symbol = resp.symbol.clone();
        order.client_order_id = resp.client_order_id.clone();

        let persisted = if let Err(e) = self.save_managed_order(&order).await {
            error!(
                "Order {} was accepted but could not be persisted: {}",
                resp.client_order_id, e
            );
            self.last_orders_sync = None;
            self.report_reconciliation_state(true).await;
            self.pause_trading().await;
            false
        } else {
            true
        };
        self.state
            .active_orders
            .write()
            .await
            .insert(resp.client_order_id.clone(), order.clone());
        if !persisted {
            if resp.executed_qty > Decimal::ZERO {
                self.invalidate_account_and_position_caches().await;
            }
            return false;
        }
        if resp.executed_qty <= Decimal::ZERO {
            if resp.is_unfilled_terminal() {
                if let Err(error) = self
                    .remove_managed_and_active_order(&order.client_order_id)
                    .await
                {
                    error!(
                        "Could not clear terminal order {}: {}",
                        order.client_order_id, error
                    );
                    self.last_orders_sync = None;
                    self.report_reconciliation_state(true).await;
                    self.pause_trading().await;
                    return false;
                }
                return false;
            }
            if resp.status == "FILLED" {
                // The contradictory response was blocked before persistence.
                return false;
            }
        }
        if resp.executed_qty > Decimal::ZERO {
            let resolved = if resp.is_terminal() {
                order.apply_terminal_fill(&resp, exchange_fill_time(&resp));
                self.on_order_filled(&mut order).await
            } else {
                let original = order.clone();
                if !self
                    .prepare_and_adopt_open_order(&mut order, original, &resp)
                    .await
                {
                    return false;
                }
                true
            };
            if resolved {
                if let Some(guard) = fill_guard {
                    guard.resolve();
                }
            }
            let protected = self.protect_openings_and_check_limits(config).await;
            let ok = resolved && protected;
            if resp.is_terminal() || !ok {
                return ok;
            }
        }
        info!(
            "Placed {} Maker order on Binance: price={}, qty={}, orderId={}",
            side.as_str(),
            formatted_price,
            formatted_qty,
            resp.order_id
        );
        true
    }

    async fn recover_failed_live_submission(
        &mut self,
        submission: LiveGridOrderSubmission<'_>,
        error: ExchangeError,
    ) -> bool {
        let LiveGridOrderSubmission {
            symbol,
            side,
            formatted_price,
            client_order_id,
            mut order,
            ..
        } = submission;
        if let ExchangeError::PostOnlyRejected(msg) = error {
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
            return false;
        }
        error!(
            "Failed to place {} order at {}: {}",
            side.as_str(),
            formatted_price,
            error
        );
        self.state
            .add_log(
                "ERROR",
                format!(
                    "Order placement failed ({} @ {}): {}",
                    side.as_str(),
                    formatted_price,
                    error
                ),
            )
            .await;
        // A timed-out request may already have reached Binance. Reserve
        // its grid level until exchange reconciliation resolves this ID.
        if error.execution_status_unknown() {
            self.last_orders_sync = None;
            match self
                .client
                .get_order_by_client_id(symbol, &client_order_id)
                .await
            {
                Ok(found) => {
                    if found.executed_qty > Decimal::ZERO || unresolved_filled_quantity(&found) {
                        self.report_reconciliation_state(true).await;
                        self.invalidate_account_and_position_caches().await;
                    }
                    order.order_id = Some(found.order_id);
                    order.status = found.open_order_status();
                    if let Err(save_error) = self.save_managed_order(&order).await {
                        error!(
                            "Could not persist accepted order {}: {}",
                            client_order_id, save_error
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
        } else if let Err(clear_error) = self.delete_managed_order(&client_order_id).await {
            error!(
                "Could not clear failed order intent {}: {}",
                client_order_id, clear_error
            );
            self.pause_trading().await;
        }
        false
    }

    pub(super) async fn submit_and_reconcile_live_grid_order(
        &mut self,
        submission: LiveGridOrderSubmission<'_>,
    ) -> bool {
        if let Err(e) = self.save_managed_order(&submission.order).await {
            error!(
                "Could not persist order intent {}: {}",
                submission.client_order_id, e
            );
            self.pause_trading().await;
            return false;
        }
        if self.state.is_shutting_down()
            || *self.state.status.read().await != BotStatus::Running
            || !self.state.ticker.read().await.prices_are_fresh(Utc::now())
        {
            if let Err(e) = self.delete_managed_order(&submission.client_order_id).await {
                error!(
                    "Could not clear paused order intent {}: {}",
                    submission.client_order_id, e
                );
            }
            return false;
        }
        let Some(exchange_guard) = self.state.try_exchange_submission() else {
            if let Err(e) = self.delete_managed_order(&submission.client_order_id).await {
                error!(
                    "Could not clear gated order intent {}: {}",
                    submission.client_order_id, e
                );
                self.report_reconciliation_state(true).await;
            }
            return false;
        };
        let (result, mut fill_guard) = self
            .submit_exchange_order(
                exchange_guard,
                NewOrderRequest {
                    symbol: submission.symbol,
                    side: submission.side.as_str(),
                    price: submission.formatted_price,
                    quantity: submission.formatted_qty,
                    client_order_id: &submission.client_order_id,
                    post_only: true,
                    reduce_only: submission.side == submission.config.grid.direction.closing_side(),
                },
            )
            .await;
        match result {
            Ok(resp) => {
                self.reconcile_accepted_live_submission(submission, resp, &mut fill_guard)
                    .await
            }
            Err(error) => self.recover_failed_live_submission(submission, error).await,
        }
    }
}
