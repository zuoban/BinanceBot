use super::snapshot_identities::invalid_snapshot_price;
use super::*;
use crate::exchange::client::is_batch_order_endpoint_unsupported;
use crate::exchange::BinanceBatchOrderItem;

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

    pub(super) async fn delete_unsubmitted_order_intent(
        &mut self,
        client_order_id: &str,
        context: &str,
    ) {
        if let Err(error) = self.delete_managed_order(client_order_id).await {
            error!(
                "Could not clear {} order intent {}: {}",
                context, client_order_id, error
            );
            self.mark_orders_reconciliation_blocked().await;
            self.pause_trading().await;
        }
    }

    async fn delete_unsubmitted_order_intents(
        &mut self,
        client_order_ids: &[String],
        context: &str,
    ) {
        if client_order_ids.is_empty() {
            return;
        }
        let to_delete = client_order_ids.to_vec();
        if let Err(error) = self
            .state
            .db
            .run_blocking(move |db| db.delete_managed_orders(&to_delete))
            .await
        {
            error!(
                "Could not clear {} order intents ({} orders): {}",
                context,
                client_order_ids.len(),
                error
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

    async fn submit_persisted_orders_sequentially(
        &mut self,
        config: &crate::config::AppConfig,
        formatted: &[(String, String)],
        orders: Vec<GridOrder>,
        first_guard: crate::server::state::ExchangeSubmissionGuard,
    ) -> (Vec<Decimal>, bool) {
        let mut accepted_prices = Vec::with_capacity(orders.len());
        let mut any_failed = false;
        let mut initial_guard = Some(first_guard);
        let symbol = config.exchange.symbol.as_str();
        let closing_side = config.grid.direction.closing_side();
        let mut iter = orders.into_iter().zip(formatted.iter()).peekable();

        while let Some((order, (fmt_price, fmt_qty))) = iter.next() {
            let guard = if let Some(guard) = initial_guard.take() {
                guard
            } else {
                if self.state.is_shutting_down()
                    || *self.state.status.read().await != BotStatus::Running
                    || !self.state.ticker.read().await.prices_are_fresh(Utc::now())
                {
                    let mut remaining = vec![order.client_order_id];
                    remaining.extend(iter.map(|(rest, _)| rest.client_order_id));
                    self.delete_unsubmitted_order_intents(&remaining, "paused")
                        .await;
                    return (accepted_prices, true);
                }
                let Some(next_guard) = self.state.try_exchange_submission() else {
                    let mut remaining = vec![order.client_order_id];
                    remaining.extend(iter.map(|(rest, _)| rest.client_order_id));
                    self.delete_unsubmitted_order_intents(&remaining, "gated")
                        .await;
                    return (accepted_prices, true);
                };
                next_guard
            };
            let price = order.price;
            let side = order.side;
            let client_order_id = order.client_order_id.clone();
            let (result, mut fill_guard) = self
                .submit_exchange_order(
                    guard,
                    NewOrderRequest {
                        symbol,
                        side: side.as_str(),
                        price: fmt_price,
                        quantity: fmt_qty,
                        client_order_id: &client_order_id,
                        post_only: true,
                        reduce_only: side == closing_side,
                    },
                )
                .await;
            let submission = LiveGridOrderSubmission {
                config,
                symbol,
                side,
                formatted_price: fmt_price,
                formatted_qty: fmt_qty,
                client_order_id,
                order,
            };
            let placed = match result {
                Ok(resp) => {
                    self.reconcile_accepted_live_submission(submission, resp, &mut fill_guard)
                        .await
                }
                Err(error) => self.recover_failed_live_submission(submission, error).await,
            };
            if placed {
                accepted_prices.push(price);
            } else {
                any_failed = true;
            }
        }
        (accepted_prices, any_failed)
    }

    pub(super) async fn submit_and_reconcile_live_grid_batch(
        &mut self,
        config: &crate::config::AppConfig,
        rules: &SymbolRules,
        mut orders: Vec<GridOrder>,
    ) -> (Vec<Decimal>, bool) {
        if orders.is_empty() {
            return (Vec::new(), false);
        }
        if orders.len() == 1 {
            let order = orders.pop().expect("checked single order");
            let price = order.price;
            let placed = self
                .submit_sized_grid_order(config, rules, false, order)
                .await;
            return if placed {
                (vec![price], false)
            } else {
                (Vec::new(), true)
            };
        }

        let cids: Vec<String> = orders.iter().map(|o| o.client_order_id.clone()).collect();
        if let Err(e) = self.save_managed_orders(orders.clone()).await {
            error!(
                "Could not persist batch order intents ({} orders): {}",
                orders.len(),
                e
            );
            self.pause_trading().await;
            return (Vec::new(), true);
        }
        if self.state.is_shutting_down()
            || *self.state.status.read().await != BotStatus::Running
            || !self.state.ticker.read().await.prices_are_fresh(Utc::now())
        {
            self.delete_unsubmitted_order_intents(&cids, "paused").await;
            return (Vec::new(), true);
        }
        let Some(exchange_guard) = self.state.try_exchange_submission() else {
            self.delete_unsubmitted_order_intents(&cids, "gated").await;
            return (Vec::new(), true);
        };

        let formatted: Vec<(String, String)> = orders
            .iter()
            .map(|o| {
                (
                    rules.format_price(o.price),
                    rules.format_quantity(o.quantity),
                )
            })
            .collect();
        let symbol = config.exchange.symbol.as_str();
        let closing_side = config.grid.direction.closing_side();
        let requests: Vec<NewOrderRequest<'_>> = orders
            .iter()
            .zip(formatted.iter())
            .map(|(order, (price, qty))| NewOrderRequest {
                symbol,
                side: order.side.as_str(),
                price,
                quantity: qty,
                client_order_id: &order.client_order_id,
                post_only: true,
                reduce_only: order.side == closing_side,
            })
            .collect();

        let batch_result = self.client.place_batch_orders(&requests).await;
        if batch_result
            .as_ref()
            .is_err_and(is_batch_order_endpoint_unsupported)
        {
            return self
                .submit_persisted_orders_sequentially(config, &formatted, orders, exchange_guard)
                .await;
        }

        let needs_fill_guard = match &batch_result {
            Ok(items) => orders.iter().zip(items.iter()).any(|(expected, item)| {
                matches!(
                    item,
                    BinanceBatchOrderItem::Order(resp)
                        if resp.executed_qty > Decimal::ZERO
                            || unresolved_filled_quantity(resp)
                            || invalid_snapshot_price(resp)
                            || resp.client_order_id != expected.client_order_id
                            || resp.symbol != expected.symbol
                            || resp.side != expected.side.as_str()
                )
            }),
            Err(err) => err.execution_status_unknown(),
        };
        let mut fill_guard = needs_fill_guard.then(|| self.state.publish_fill_reconciliation());
        exchange_guard.complete();
        if let Some(guard) = &mut fill_guard {
            guard.acquire().await;
        }

        let items = match batch_result {
            Ok(items) => items,
            Err(ExchangeError::PostOnlyRejected(msg)) => {
                self.delete_unsubmitted_order_intents(&cids, "rejected")
                    .await;
                warn!(
                    "Maker GTX batch rejected (would take liquidity): {}. Will retry next tick.",
                    msg
                );
                return (Vec::new(), true);
            }
            Err(error) => {
                error!(
                    "Failed to place batch orders ({} orders): {}",
                    orders.len(),
                    error
                );
                self.state
                    .add_log(
                        "ERROR",
                        format!(
                            "Batch order placement failed ({} orders): {}",
                            orders.len(),
                            error
                        ),
                    )
                    .await;
                if error.execution_status_unknown() {
                    for order in orders {
                        let cid = order.client_order_id.clone();
                        self.recover_unknown_live_submission(symbol, cid, order)
                            .await;
                    }
                } else {
                    self.delete_unsubmitted_order_intents(&cids, "failed").await;
                }
                return (Vec::new(), true);
            }
        };

        let risk = RiskConfigSnapshot::from_config(config);
        let mut any_failed = false;
        let mut to_delete = Vec::new();
        let mut accepted_valid = Vec::with_capacity(orders.len());

        for ((mut order, (fmt_price, fmt_qty)), item) in
            orders.into_iter().zip(formatted).zip(items)
        {
            match item {
                BinanceBatchOrderItem::Error(api_err) => {
                    any_failed = true;
                    to_delete.push(order.client_order_id);
                    if api_err.code == -5022 {
                        warn!(
                            "Maker GTX rejected (would take liquidity): {}. Will retry next tick.",
                            api_err.msg
                        );
                    } else {
                        error!(
                            "Failed to place {} order at {} (code {}): {}",
                            order.side.as_str(),
                            fmt_price,
                            api_err.code,
                            api_err.msg
                        );
                        self.state
                            .add_log(
                                "ERROR",
                                format!(
                                    "Order placement failed ({} @ {}): Binance API error (code: {}): {}",
                                    order.side.as_str(),
                                    fmt_price,
                                    api_err.code,
                                    api_err.msg
                                ),
                            )
                            .await;
                    }
                }
                BinanceBatchOrderItem::Order(resp) => {
                    if !order.matches_exchange_identity(&resp) || invalid_snapshot_price(&resp) {
                        any_failed = true;
                        let _ = self.persist_accepted_live_order(&mut order, &resp).await;
                    } else if resp.executed_qty <= Decimal::ZERO && resp.is_unfilled_terminal() {
                        any_failed = true;
                        to_delete.push(order.client_order_id);
                    } else if resp.executed_qty <= Decimal::ZERO && resp.status == "FILLED" {
                        any_failed = true;
                        let _ = self.persist_accepted_live_order(&mut order, &resp).await;
                    } else {
                        if unresolved_filled_quantity(&resp) {
                            self.block_reconciliation_and_invalidate_caches().await;
                        }
                        order.order_id = Some(resp.order_id);
                        accepted_valid.push((order, fmt_price, fmt_qty, resp));
                    }
                }
            }
        }

        self.delete_unsubmitted_order_intents(&to_delete, "rejected")
            .await;

        if accepted_valid.is_empty() {
            return (Vec::new(), any_failed);
        }

        let to_persist: Vec<GridOrder> = accepted_valid
            .iter()
            .map(|(order, _, _, _)| order.clone())
            .collect();
        let persisted = if let Err(e) = self.save_managed_orders(to_persist).await {
            error!(
                "Accepted batch orders could not be persisted ({} orders): {}",
                accepted_valid.len(),
                e
            );
            self.mark_orders_reconciliation_blocked().await;
            self.pause_trading().await;
            false
        } else {
            true
        };
        {
            let mut active = self.state.active_orders.write().await;
            for (order, _, _, resp) in &accepted_valid {
                active.insert(resp.client_order_id.clone(), order.clone());
            }
        }
        if !persisted {
            if accepted_valid
                .iter()
                .any(|(_, _, _, resp)| resp.executed_qty > Decimal::ZERO)
            {
                self.invalidate_account_and_position_caches();
            }
            return (Vec::new(), true);
        }

        let mut accepted_prices = Vec::with_capacity(accepted_valid.len());
        for (mut order, fmt_price, fmt_qty, resp) in accepted_valid {
            if resp.executed_qty > Decimal::ZERO {
                match self
                    .reconcile_accepted_fill(risk, &mut order, &resp, &mut fill_guard)
                    .await
                {
                    Some(true) => accepted_prices.push(order.price),
                    Some(false) => any_failed = true,
                    None => {
                        info!(
                            "Placed {} Maker order on Binance: price={}, qty={}, orderId={}",
                            order.side.as_str(),
                            fmt_price,
                            fmt_qty,
                            resp.order_id
                        );
                        accepted_prices.push(order.price);
                    }
                }
            } else {
                info!(
                    "Placed {} Maker order on Binance: price={}, qty={}, orderId={}",
                    order.side.as_str(),
                    fmt_price,
                    fmt_qty,
                    resp.order_id
                );
                accepted_prices.push(order.price);
            }
        }
        (accepted_prices, any_failed)
    }
}
