use super::*;

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
    async fn confirm_live_order_canceled(&mut self, order: &GridOrder) -> Option<bool> {
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
                self.mark_orders_reconciliation_blocked().await;
                if !self
                    .persist_identity_observations(
                        self.client.account_identity_key(),
                        &order.symbol,
                    )
                    .await
                {
                    self.pause_trading().await;
                }
                return Some(false);
            }
        }
        match exchange_order {
            Ok(Some(exchange_order)) if unresolved_filled_quantity(&exchange_order) => {
                self.block_reconciliation_and_invalidate_caches().await;
                Some(false)
            }
            Ok(Some(exchange_order)) if !exchange_order.is_terminal() => {
                self.last_orders_sync = None;
                warn!("Cancellation is not terminal for {}", order.client_order_id);
                Some(false)
            }
            Ok(Some(exchange_order)) if exchange_order.executed_qty > Decimal::ZERO => {
                let mut filled = order.clone();
                filled.apply_terminal_fill(&exchange_order, exchange_fill_time(&exchange_order));
                Some(self.on_order_filled(&mut filled).await)
            }
            Ok(Some(_)) => None,
            Ok(None) => {
                warn!(
                    "Canceled order {} but could not confirm fill status: order not found",
                    order.client_order_id
                );
                Some(false)
            }
            Err(e) => {
                warn!(
                    "Canceled order {} but could not confirm fill status: {}",
                    order.client_order_id, e
                );
                Some(false)
            }
        }
    }

    /// Cancel a single order
    pub(super) async fn cancel_single_order(&mut self, order: &GridOrder) -> bool {
        let paper = self.state.config.read().await.exchange.is_paper();
        if !paper {
            if let Some(outcome) = Box::pin(self.confirm_live_order_canceled(order)).await {
                return outcome;
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
            self.mark_orders_reconciliation_blocked().await;
            self.pause_trading().await;
            return false;
        }
        true
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
                    self.mark_orders_reconciliation_blocked().await;
                    None
                }
            }
        };
        let provisional_ready = match &recovery_sources {
            Some((intents, remainder)) if !paper => self
                .restore_provisional_orders(
                    self.client.account_identity_key(),
                    symbol,
                    intents,
                    remainder.as_ref(),
                )
                .await
                .is_some_and(|(_, conflict)| !conflict),
            _ => paper,
        };
        let metadata_ready = observations_ready && recovery_sources.is_some() && provisional_ready;
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

    async fn cancel_open_grid_order(
        &mut self,
        symbol: &str,
        order: BinanceOrderResponse,
        managed_sources: &HashMap<String, GridOrder>,
        recovery: &CancelRecoveryMetadata,
        managed_read_failed: bool,
    ) -> bool {
        let Err(e) = self
            .client
            .cancel_order(symbol, Some(order.order_id), None)
            .await
        else {
            return true;
        };
        let confirmation = self.client.get_order(symbol, order.order_id).await;
        if confirmation.as_ref().is_ok_and(unresolved_filled_quantity) {
            self.retain_uncertain_canceled_order(UncertainCanceledOrder {
                symbol,
                order: &order,
                confirmation: confirmation.as_ref().ok(),
                managed_sources,
                recovery_sources: recovery.recovery_sources.as_ref(),
                managed_read_failed,
                metadata_ready: recovery.metadata_ready,
            })
            .await;
            return false;
        }
        if confirmation.is_ok_and(|confirmed| confirmed.is_terminal()) {
            warn!(
                "Cancellation response failed for {}, but its terminal state was confirmed",
                order.client_order_id
            );
            return true;
        }
        error!(
            "Failed to cancel managed order {}: {}",
            order.client_order_id, e
        );
        self.pause_trading().await;
        false
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
            if !self
                .cancel_open_grid_order(
                    symbol,
                    order,
                    &managed_sources,
                    recovery,
                    managed_read_failed,
                )
                .await
            {
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
        Box::pin(self.cancel_all_orders_boxed(pause_already_persisted)).await
    }

    async fn cancel_all_orders_boxed(&mut self, pause_already_persisted: bool) -> bool {
        let (paper, symbol, mode) = {
            let config = self.state.config.read().await;
            (
                config.exchange.is_paper(),
                config.exchange.symbol.clone(),
                config.exchange.trading_mode(),
            )
        };
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

    async fn stage_uncertain_canceled_order(
        &self,
        ctx: &UncertainCanceledOrder<'_>,
        confirmation_conflict: bool,
    ) -> (GridOrder, bool, Option<GridOrder>) {
        let source = ctx
            .managed_sources
            .get(&ctx.order.client_order_id)
            .or_else(|| {
                ctx.recovery_sources.and_then(|(intents, plan)| {
                    reconciliation::find_durable_order(
                        &ctx.order.client_order_id,
                        &[],
                        intents,
                        plan.as_ref(),
                    )
                })
            })
            .cloned();
        let mut active = self.state.active_orders.write().await;
        let current = active.get(&ctx.order.client_order_id).cloned();
        let mut retained = source.or_else(|| current.clone()).unwrap_or_else(|| {
            GridOrder::from_exchange_response(
                ctx.order,
                ctx.order.orig_qty,
                OrderPurpose::Legacy,
                Utc::now(),
            )
        });
        if let Some(current) = &current {
            retained.adopt_compatible_order_id(current);
        }
        let identity_matches = ctx.order.has_valid_symbol_and_side(ctx.symbol)
            && retained.matches_exchange_identity(ctx.order);
        let active_matches = current
            .as_ref()
            .is_none_or(|current| current.has_compatible_identity(&retained));
        let active_list_matches = current
            .as_ref()
            .is_none_or(|current| current.matches_exchange_identity(ctx.order));
        if identity_matches && active_matches && active_list_matches && !confirmation_conflict {
            retained.order_id = Some(ctx.order.order_id);
            retained.reserve_quantity(ctx.order.orig_qty);
            if let Some(current) = &current {
                retained.reserve_quantity(current.quantity);
            }
        }
        active.insert(retained.client_order_id.clone(), retained.clone());
        (
            retained,
            !identity_matches || !active_matches || !active_list_matches || confirmation_conflict,
            current,
        )
    }

    async fn retain_uncertain_canceled_order(&mut self, ctx: UncertainCanceledOrder<'_>) {
        self.block_reconciliation_and_invalidate_caches().await;
        let confirmation_conflict = ctx
            .confirmation
            .is_some_and(|found| !found.matches_identity(ctx.order));
        let (retained, identity_conflict, previous) = self
            .stage_uncertain_canceled_order(&ctx, confirmation_conflict)
            .await;
        let account = self.client.account_identity_key();
        if identity_conflict {
            if confirmation_conflict {
                if let Some(found) = ctx.confirmation {
                    self.track_identity_conflict(&retained, found);
                }
            }
            if !retained.matches_exchange_identity(ctx.order) {
                self.track_identity_conflict(&retained, ctx.order);
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
        if ctx.managed_read_failed || !ctx.metadata_ready {
            self.track_provisional_observation(ctx.order);
        } else if let Err(error) = self.save_managed_order(&retained).await {
            self.track_provisional_observation(ctx.order);
            error!(
                "Could not persist uncertain canceled order {}: {}",
                retained.client_order_id, error
            );
            self.pause_trading().await;
        }
        if identity_conflict {
            self.persist_identity_observations(account, ctx.symbol)
                .await;
        }
    }
}
