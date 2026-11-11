use super::snapshot_identities::invalid_snapshot_price;
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
    pub(super) async fn submit_exchange_order(
        &self,
        exchange_guard: crate::server::state::ExchangeSubmissionGuard,
        request: NewOrderRequest<'_>,
    ) -> (
        std::result::Result<BinanceOrderResponse, ExchangeError>,
        Option<crate::server::state::PendingFillReconciliation>,
    ) {
        let expected_client_id = request.client_order_id;
        let expected_symbol = request.symbol;
        let expected_side = request.side;
        let result = self.client.place_order(request).await;
        let mut fill_guard = if result.as_ref().is_ok_and(|response| {
            response.executed_qty > Decimal::ZERO
                || unresolved_filled_quantity(response)
                || invalid_snapshot_price(response)
                || response.client_order_id != expected_client_id
                || response.symbol != expected_symbol
                || response.side != expected_side
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

    async fn persist_accepted_live_order(
        &mut self,
        order: &mut GridOrder,
        resp: &BinanceOrderResponse,
    ) -> bool {
        if !order.matches_exchange_identity(resp) || invalid_snapshot_price(resp) {
            error!(
                "Accepted order {} failed identity or price validation (orderId={}, symbol={}, side={}, price={})",
                order.client_order_id, resp.order_id, resp.symbol, resp.side, resp.price
            );
            self.block_reconciliation_and_invalidate_caches().await;
            if !order.matches_exchange_identity(resp) {
                self.track_identity_conflict(order, resp);
            }
            self.state
                .active_orders
                .write()
                .await
                .insert(order.client_order_id.clone(), order.clone());
            if !self
                .persist_identity_observations(self.client.account_identity_key(), &order.symbol)
                .await
            {
                self.pause_trading().await;
            }
            return false;
        }
        if unresolved_filled_quantity(resp) {
            self.block_reconciliation_and_invalidate_caches().await;
        }
        order.order_id = Some(resp.order_id);
        let persisted = if let Err(e) = self.save_managed_order(order).await {
            error!(
                "Order {} was accepted but could not be persisted: {}",
                resp.client_order_id, e
            );
            self.mark_orders_reconciliation_blocked().await;
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
        if !persisted && resp.executed_qty > Decimal::ZERO {
            self.invalidate_account_and_position_caches();
        }
        persisted
    }

    async fn reconcile_accepted_fill(
        &mut self,
        risk: RiskConfigSnapshot,
        order: &mut GridOrder,
        resp: &BinanceOrderResponse,
        fill_guard: &mut Option<crate::server::state::PendingFillReconciliation>,
    ) -> Option<bool> {
        let resolved = if resp.is_terminal() {
            order.apply_terminal_fill(resp, exchange_fill_time(resp));
            self.on_order_filled(order).await
        } else {
            let original = order.clone();
            if !self
                .prepare_and_adopt_open_order(order, original, resp)
                .await
            {
                return Some(false);
            }
            true
        };
        if resolved {
            if let Some(guard) = fill_guard {
                guard.resolve();
            }
        }
        let protected = self.protect_openings_and_check_limits(risk).await;
        let ok = resolved && protected;
        if resp.is_terminal() || !ok {
            Some(ok)
        } else {
            None
        }
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
        let risk = RiskConfigSnapshot::from_config(config);
        if !self.persist_accepted_live_order(&mut order, &resp).await {
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
                    self.mark_orders_reconciliation_blocked().await;
                    self.pause_trading().await;
                }
                return false;
            }
            if resp.status == "FILLED" {
                // The contradictory response was blocked before persistence.
                return false;
            }
        } else if let Some(outcome) = self
            .reconcile_accepted_fill(risk, &mut order, &resp, fill_guard)
            .await
        {
            return outcome;
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

    async fn recover_unknown_live_submission(
        &mut self,
        symbol: &str,
        client_order_id: String,
        mut order: GridOrder,
    ) {
        self.last_orders_sync = None;
        match self
            .client
            .get_order_by_client_id(symbol, &client_order_id)
            .await
        {
            Ok(found) => {
                if !order.matches_exchange_identity(&found) || invalid_snapshot_price(&found) {
                    error!(
                        "Recovered order {} failed identity or price validation (orderId={}, symbol={}, side={}, price={})",
                        client_order_id, found.order_id, found.symbol, found.side, found.price
                    );
                    self.block_reconciliation_and_invalidate_caches().await;
                    if !order.matches_exchange_identity(&found) {
                        self.track_identity_conflict(&order, &found);
                    }
                    let order_symbol = order.symbol.clone();
                    self.state
                        .active_orders
                        .write()
                        .await
                        .insert(client_order_id, order);
                    if !self
                        .persist_identity_observations(
                            self.client.account_identity_key(),
                            &order_symbol,
                        )
                        .await
                    {
                        self.pause_trading().await;
                    }
                    return;
                }
                if found.is_terminal()
                    || found.executed_qty > Decimal::ZERO
                    || unresolved_filled_quantity(&found)
                {
                    self.block_reconciliation_and_invalidate_caches().await;
                }
                order.order_id = Some(found.order_id);
                if !found.is_terminal() {
                    order.status = found.open_order_status();
                }
                if let Err(save_error) = self.save_managed_order(&order).await {
                    error!(
                        "Could not persist accepted order {}: {}",
                        client_order_id, save_error
                    );
                    self.mark_orders_reconciliation_blocked().await;
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
    }

    async fn delete_unsubmitted_order_intent(&mut self, client_order_id: &str, context: &str) {
        if let Err(error) = self.delete_managed_order(client_order_id).await {
            error!(
                "Could not clear {} order intent {}: {}",
                context, client_order_id, error
            );
            self.mark_orders_reconciliation_blocked().await;
            self.pause_trading().await;
        }
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
            order,
            ..
        } = submission;
        if let ExchangeError::PostOnlyRejected(msg) = error {
            self.delete_unsubmitted_order_intent(&client_order_id, "rejected")
                .await;
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
            self.recover_unknown_live_submission(symbol, client_order_id, order)
                .await;
        } else {
            self.delete_unsubmitted_order_intent(&client_order_id, "failed")
                .await;
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
            self.delete_unsubmitted_order_intent(&submission.client_order_id, "paused")
                .await;
            return false;
        }
        let Some(exchange_guard) = self.state.try_exchange_submission() else {
            self.delete_unsubmitted_order_intent(&submission.client_order_id, "gated")
                .await;
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
