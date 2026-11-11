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

type RecoverySources = (HashMap<String, GridOrder>, Option<RemainderPlan>);

struct CancelRecoveryMetadata {
    recovery_sources: Option<RecoverySources>,
    metadata_ready: bool,
    recovery_failed: bool,
}

struct UncertainCanceledOrder<'a> {
    symbol: &'a str,
    order: &'a BinanceOrderResponse,
    confirmation: Option<&'a BinanceOrderResponse>,
    managed_sources: &'a HashMap<String, GridOrder>,
    recovery_sources: Option<&'a RecoverySources>,
    managed_read_failed: bool,
    metadata_ready: bool,
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
        let remaining = (found.orig_qty - found.executed_qty).max(Decimal::ZERO);
        let previous = self
            .state
            .active_orders
            .read()
            .await
            .get(&order.client_order_id)
            .cloned();
        if self
            .last_account_sync
            .is_some_and(|at| at.elapsed() < LIVE_SYNC_FRESHNESS)
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
        reserved.amount_usdc = reserved.price * reserved.quantity;
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
        order.quantity = (found.orig_qty - found.executed_qty).max(Decimal::ZERO);
        order.amount_usdc = order.price * order.quantity;
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
            acc.total_wallet_balance += simulated_pnl;
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
            amount_usdc: order.price * order.quantity,
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
                if !self.prepare_partial_adoption(&mut order, &resp).await {
                    return false;
                }
                let original = order.clone();
                if !self
                    .adopt_open_exchange_order(&mut order, original, &resp)
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

    /// Cancel a single order
    pub(super) async fn cancel_single_order(&mut self, order: &GridOrder) -> bool {
        let paper = self.state.config.read().await.exchange.is_paper();
        if !paper {
            if let Err(e) = self
                .client
                .cancel_order(&order.symbol, order.order_id, Some(&order.client_order_id))
                .await
            {
                warn!("Failed to cancel order {}: {}", order.client_order_id, e);
                // A lost response or a fill preceding cancellation can still
                // leave a terminal order. Confirm it through the query below.
            }
            let exchange_order = self.lookup_order(order).await;
            if let Ok(Some(exchange_order)) = &exchange_order {
                if !order.matches_exchange_identity(exchange_order) {
                    self.track_identity_conflict(order, exchange_order);
                    self.last_orders_sync = None;
                    self.report_reconciliation_state(true).await;
                    if !self
                        .persist_identity_observations(
                            self.client.account_identity_key(),
                            &order.symbol,
                        )
                        .await
                    {
                        self.pause_trading().await;
                    }
                    return false;
                }
            }
            match exchange_order {
                Ok(Some(exchange_order)) if unresolved_filled_quantity(&exchange_order) => {
                    self.block_reconciliation_and_invalidate_caches().await;
                    return false;
                }
                Ok(Some(exchange_order)) if !exchange_order.is_terminal() => {
                    self.last_orders_sync = None;
                    warn!("Cancellation is not terminal for {}", order.client_order_id);
                    return false;
                }
                Ok(Some(exchange_order)) if exchange_order.executed_qty > Decimal::ZERO => {
                    let mut filled = order.clone();
                    filled
                        .apply_terminal_fill(&exchange_order, exchange_fill_time(&exchange_order));
                    return self.on_order_filled(&mut filled).await;
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    warn!(
                        "Canceled order {} but could not confirm fill status: order not found",
                        order.client_order_id
                    );
                    return false;
                }
                Err(e) => {
                    warn!(
                        "Canceled order {} but could not confirm fill status: {}",
                        order.client_order_id, e
                    );
                    return false;
                }
            }
        }
        if let Err(e) = self
            .remove_managed_and_active_order(&order.client_order_id)
            .await
        {
            error!(
                "Could not delete managed order {}: {}",
                order.client_order_id, e
            );
            self.last_orders_sync = None;
            self.report_reconciliation_state(true).await;
            self.pause_trading().await;
            return false;
        }
        true
    }

    async fn restore_provisional_before_cancel(&mut self, symbol: &str, mode: TradingMode) -> bool {
        let account = self.client.account_identity_key();
        let ids = self.provisional_order_ids_for(account);
        if ids.is_empty() {
            return true;
        }
        let records = async {
            Ok::<_, anyhow::Error>((
                self.load_managed_orders(symbol).await?,
                self.load_pair_intent_map(symbol, mode).await?,
                self.load_remainder_plan().await?,
            ))
        }
        .await;
        let (managed, intents, remainder) = match records {
            Ok(records) => records,
            Err(error) => {
                error!(
                    "Could not restore provisional orders before cancellation: {}",
                    error
                );
                self.last_orders_sync = None;
                self.report_reconciliation_state(true).await;
                return false;
            }
        };
        let mut conflict = false;
        {
            let state = self.state.clone();
            let mut active = state.active_orders.write().await;
            for id in ids {
                let key = (account, id.clone());
                let Some(current) = active
                    .get(&id)
                    .cloned()
                    .or_else(|| self.evidence.order_identity_conflicts.get(&key).cloned())
                else {
                    continue;
                };
                let durable =
                    reconciliation::find_durable_order(&id, &managed, &intents, remainder.as_ref());
                let Some(durable) = durable else {
                    if let Some(expected) = self.evidence.order_identity_conflicts.get(&key) {
                        active.entry(id).or_insert_with(|| expected.clone());
                        conflict = true;
                    }
                    continue;
                };
                let expected = self.evidence.order_identity_conflicts.get(&key).cloned();
                let identities_agree = durable.has_compatible_identity(&current)
                    && expected.as_ref().is_none_or(|expected| {
                        durable.has_compatible_identity(expected)
                            && current.has_compatible_identity(expected)
                    });
                let mut restored = durable.clone();
                if identities_agree {
                    restored.order_id = restored
                        .order_id
                        .or(current.order_id)
                        .or_else(|| expected.as_ref().and_then(|order| order.order_id));
                } else if let Some(expected) =
                    expected.as_ref().filter(|order| order.order_id.is_some())
                {
                    self.record_differing_identity(&key, durable, expected);
                    restored = expected.clone();
                }
                let durable = &restored;
                if expected.is_some() || !identities_agree {
                    self.record_differing_identity(&key, &current, durable);
                    self.promote_provisional_observations(&key, durable);
                    if let Some(old) = expected.as_ref() {
                        self.record_differing_identity(&key, old, durable);
                    }
                    self.evidence
                        .order_identity_conflicts
                        .insert(key, durable.clone());
                    active.insert(id, durable.clone());
                    conflict = true;
                    continue;
                }
                let mut restored = durable.clone();
                restored.reserve_quantity(current.quantity);
                active.insert(id, restored);
                self.clear_provisional_order(&key);
            }
        }
        if conflict {
            self.last_orders_sync = None;
            self.report_reconciliation_state(true).await;
            self.persist_identity_observations(account, symbol).await;
        }
        !conflict
    }

    async fn load_cancel_recovery_metadata(
        &mut self,
        paper: bool,
        symbol: &str,
        mode: TradingMode,
        mut recovery_failed: bool,
    ) -> CancelRecoveryMetadata {
        let observations_ready = paper
            || self
                .load_identity_observations(self.client.account_identity_key(), symbol)
                .await;
        let recovery_sources = if paper {
            Some((HashMap::new(), None))
        } else {
            match async {
                Ok::<_, anyhow::Error>((
                    self.load_pair_intent_map(symbol, mode).await?,
                    self.load_remainder_plan().await?,
                ))
            }
            .await
            {
                Ok(sources) => Some(sources),
                Err(error) => {
                    error!("Could not load cancellation recovery sources: {}", error);
                    self.last_orders_sync = None;
                    self.report_reconciliation_state(true).await;
                    None
                }
            }
        };
        let metadata_ready = observations_ready
            && recovery_sources.is_some()
            && (paper || self.restore_provisional_before_cancel(symbol, mode).await);
        if !metadata_ready {
            recovery_failed = true;
        } else if let Err(e) = self.cancel_remainder_plan().await {
            error!(
                "Could not resolve pending remainder plan during cancellation: {}",
                e
            );
            self.pause_trading().await;
            recovery_failed = true;
        }
        CancelRecoveryMetadata {
            recovery_sources,
            metadata_ready,
            recovery_failed,
        }
    }

    async fn cancel_known_orders_without_snapshot(&self, symbol: &str) {
        // The exchange listing is incomplete, but known orders can still
        // be canceled individually. Preserve all recovery metadata.
        let known: Vec<_> = self
            .state
            .active_orders
            .read()
            .await
            .values()
            .cloned()
            .collect();
        for order in known
            .iter()
            .filter(|order| is_grid_order(&order.client_order_id))
        {
            if let Err(error) = self
                .client
                .cancel_order(symbol, order.order_id, Some(&order.client_order_id))
                .await
            {
                warn!(
                    "Failed to cancel known managed order {}: {}",
                    order.client_order_id, error
                );
            }
        }
    }

    async fn cancel_live_exchange_orders(
        &mut self,
        symbol: &str,
        recovery: &CancelRecoveryMetadata,
    ) -> bool {
        // Initialization may have failed while loading recovery metadata.
        // Never clear persisted orders without first restoring and reconciling them.
        let mut managed_read_failed = false;
        let mut recovery_failed = recovery.recovery_failed;
        let restored = match self.load_managed_orders(symbol).await {
            Ok(orders) => orders,
            Err(e) => {
                error!("Failed to load managed orders before cancellation: {}", e);
                self.pause_trading().await;
                managed_read_failed = true;
                recovery_failed = true;
                Vec::new()
            }
        };
        let managed_sources: HashMap<_, _> = restored
            .iter()
            .map(|order| (order.client_order_id.clone(), order.clone()))
            .collect();
        self.insert_restored_managed_orders(restored).await;
        let live_orders = match self.client.get_open_orders(symbol).await {
            Ok(orders) => orders,
            Err(e) => {
                error!("Failed to list managed orders before cancellation: {}", e);
                self.pause_trading().await;
                self.cancel_known_orders_without_snapshot(symbol).await;
                return false;
            }
        };
        let mut cancellation_failed = false;
        for order in live_orders
            .into_iter()
            .filter(|order| is_grid_order(&order.client_order_id))
        {
            if let Err(e) = self
                .client
                .cancel_order(symbol, Some(order.order_id), None)
                .await
            {
                let confirmation = self.client.get_order(symbol, order.order_id).await;
                if confirmation.as_ref().is_ok_and(unresolved_filled_quantity) {
                    self.retain_uncertain_canceled_order(UncertainCanceledOrder {
                        symbol,
                        order: &order,
                        confirmation: confirmation.as_ref().ok(),
                        managed_sources: &managed_sources,
                        recovery_sources: recovery.recovery_sources.as_ref(),
                        managed_read_failed,
                        metadata_ready: recovery.metadata_ready,
                    })
                    .await;
                    cancellation_failed = true;
                    continue;
                }
                if confirmation.is_ok_and(|confirmed| confirmed.is_terminal()) {
                    warn!(
                        "Cancellation response failed for {}, but its terminal state was confirmed",
                        order.client_order_id
                    );
                    continue;
                }
                error!(
                    "Failed to cancel managed order {}: {}",
                    order.client_order_id, e
                );
                self.pause_trading().await;
                cancellation_failed = true;
            }
        }
        // Without the complete recovery metadata, even a terminal exchange
        // response cannot safely account for fills or justify clearing records.
        if recovery_failed || cancellation_failed {
            return false;
        }
        if !self.sync_live_orders().await || !self.state.active_orders.read().await.is_empty() {
            error!("Managed orders could not be fully reconciled after cancellation");
            self.pause_trading().await;
            return false;
        }
        true
    }

    /// Cancel all active orders
    pub(super) async fn cancel_all_orders(&mut self) -> bool {
        self.cancel_all_orders_inner(false).await
    }

    pub(super) async fn cancel_all_orders_inner(&mut self, pause_already_persisted: bool) -> bool {
        let config = self.state.config.read().await.clone();
        let paper = config.exchange.is_paper();
        let symbol = config.exchange.symbol.clone();
        let mode = config.exchange.trading_mode();
        let pause_epoch = self.state.pause_epoch();
        let was_running =
            !pause_already_persisted && *self.state.status.read().await == BotStatus::Running;
        let mut pause_failed = false;
        if !pause_already_persisted {
            if let Err(e) = self.state.pause_trading_silently().await {
                error!("Failed to persist paused status before cancellation: {}", e);
                self.pause_trading().await;
                pause_failed = true;
            }
        }

        let recovery = self
            .load_cancel_recovery_metadata(paper, &symbol, mode, pause_failed)
            .await;
        if !paper && !self.cancel_live_exchange_orders(&symbol, &recovery).await {
            return false;
        }
        if recovery.recovery_failed {
            return false;
        }
        self.state.active_orders.write().await.clear();
        if !paper {
            if let Err(e) = self
                .state
                .db
                .run_blocking(move |db| {
                    db.clear_managed_orders(&symbol)?;
                    db.clear_pair_intents(&symbol, mode)
                })
                .await
            {
                error!("Failed to clear persisted managed orders: {}", e);
                self.pause_trading().await;
                return false;
            }
        }
        if was_running && self.state.pause_epoch() == pause_epoch + 1 {
            if let Err(e) = self.state.resume_trading_silently(pause_epoch + 1).await {
                error!("Failed to restore running status after cancellation: {}", e);
                self.pause_trading().await;
                return false;
            }
        }
        true
    }

    async fn retain_uncertain_canceled_order(&mut self, ctx: UncertainCanceledOrder<'_>) {
        let UncertainCanceledOrder {
            symbol,
            order,
            confirmation,
            managed_sources,
            recovery_sources,
            managed_read_failed,
            metadata_ready,
        } = ctx;
        self.block_reconciliation_and_invalidate_caches().await;
        // A managed exchange order can be absent from local
        // metadata. Keep it recoverable before the list changes.
        let source = managed_sources
            .get(&order.client_order_id)
            .or_else(|| {
                recovery_sources.and_then(|(intents, plan)| {
                    reconciliation::find_durable_order(
                        &order.client_order_id,
                        &[],
                        intents,
                        plan.as_ref(),
                    )
                })
            })
            .cloned();
        let confirmation_conflict =
            confirmation.is_some_and(|found| !found.matches_identity(order));
        let (retained, identity_conflict, previous) = {
            let mut active = self.state.active_orders.write().await;
            let current = active.get(&order.client_order_id).cloned();
            let mut retained = source.or_else(|| current.clone()).unwrap_or_else(|| {
                GridOrder::from_exchange_response(
                    order,
                    order.orig_qty,
                    OrderPurpose::Legacy,
                    Utc::now(),
                )
            });
            if let Some(current) = &current {
                retained.adopt_compatible_order_id(current);
            }
            let identity_matches = order.symbol == symbol
                && OrderSide::from_exchange_str(&order.side).is_some()
                && retained.matches_exchange_identity(order);
            let active_matches = current
                .as_ref()
                .is_none_or(|current| current.has_compatible_identity(&retained));
            let active_list_matches = current
                .as_ref()
                .is_none_or(|current| current.matches_exchange_identity(order));
            if identity_matches && active_matches && active_list_matches && !confirmation_conflict {
                retained.order_id = Some(order.order_id);
                retained.reserve_quantity(order.orig_qty);
                if let Some(current) = &current {
                    retained.reserve_quantity(current.quantity);
                }
            }
            active.insert(retained.client_order_id.clone(), retained.clone());
            (
                retained,
                !identity_matches
                    || !active_matches
                    || !active_list_matches
                    || confirmation_conflict,
                current,
            )
        };
        let account = self.client.account_identity_key();
        if identity_conflict {
            if confirmation_conflict {
                if let Some(found) = confirmation {
                    self.track_identity_conflict(&retained, found);
                }
            }
            if !retained.matches_exchange_identity(order) {
                self.track_identity_conflict(&retained, order);
            }
            if let Some(observed) = previous
                .as_ref()
                .and_then(ObservedOrderIdentity::from_order)
            {
                if !observed.matches_order(&retained) {
                    self.record_identity_conflict_observation(account, &retained, observed);
                }
            }
        }
        if managed_read_failed || !metadata_ready {
            self.track_provisional_observation(order);
        } else if let Err(error) = self.save_managed_order(&retained).await {
            self.track_provisional_observation(order);
            error!(
                "Could not persist uncertain canceled order {}: {}",
                retained.client_order_id, error
            );
            self.pause_trading().await;
        }
        if identity_conflict {
            self.persist_identity_observations(account, symbol).await;
        }
    }

    /// Rebalance grid centered around current price
    pub(super) async fn rebalance_grid(&mut self) -> Result<(), String> {
        if !self.cancel_all_orders().await {
            let error = "Grid rebalance stopped: failed to cancel existing orders";
            self.state.add_log("ERROR", error).await;
            return Err(error.into());
        }
        if *self.state.status.read().await != BotStatus::Running {
            return Err("策略已暂停，网格重建未完成".into());
        }
        self.maintain_grid_window().await;
        if *self.state.status.read().await != BotStatus::Running {
            return Err("策略已暂停，网格重建未完成".into());
        }
        self.state
            .add_log("INFO", "Grid rebalance request processed")
            .await;
        Ok(())
    }
}
