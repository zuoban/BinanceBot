use super::*;

pub(super) struct LiveSyncSettlement<'a> {
    pub(super) account_key: [u8; 32],
    pub(super) symbol: &'a str,
    pub(super) mode: TradingMode,
    pub(super) pause_epoch: u64,
    pub(super) terminal_orders: Vec<(GridOrder, BinanceOrderResponse)>,
    pub(super) remainder_plan: &'a mut Option<RemainderPlan>,
    pub(super) has_fill_guard: bool,
    pub(super) resolved_conflict_ids: &'a [String],
}

pub(super) struct OpenLiveOrderAdoption<'a> {
    pub(super) pause_epoch: u64,
    pub(super) position_ready: bool,
    pub(super) live_ids: &'a HashMap<String, BinanceOrderResponse>,
    pub(super) uncertain_ids: &'a HashSet<String>,
    pub(super) restored_ids: &'a HashSet<String>,
    pub(super) pair_intents: &'a HashMap<String, GridOrder>,
    pub(super) remainder_plan: Option<&'a RemainderPlan>,
    pub(super) persisted_managed: &'a HashMap<String, GridOrder>,
}

enum MissingOrderOutcome {
    Terminal(Box<(GridOrder, BinanceOrderResponse)>),
    Retired,
    Unresolved,
    PersistenceFailed,
}

impl GridTradingEngine {
    async fn commit_or_remove_terminal_orders(
        &mut self,
        terminal_orders: Vec<(GridOrder, BinanceOrderResponse)>,
    ) -> bool {
        let mut persistence_ok = true;
        let mut unfilled_ids = Vec::new();
        for (mut order, exchange_order) in terminal_orders {
            if exchange_order.executed_qty > Decimal::ZERO {
                order.apply_terminal_fill(&exchange_order, exchange_fill_time(&exchange_order));
                if !self.commit_order_fill(&mut order).await {
                    persistence_ok = false;
                }
            } else {
                debug!(
                    "Order {} closed without a fill ({})",
                    order.client_order_id, exchange_order.status
                );
                unfilled_ids.push(order.client_order_id);
            }
        }
        if !unfilled_ids.is_empty() {
            if let Err(e) = self.remove_managed_and_active_orders(unfilled_ids).await {
                error!("Could not delete closed orders: {}", e);
                persistence_ok = false;
            }
        }
        persistence_ok
    }

    pub(super) async fn settle_terminal_and_pending_workflows(
        &mut self,
        settlement: LiveSyncSettlement<'_>,
    ) -> Option<bool> {
        let LiveSyncSettlement {
            account_key,
            symbol,
            mode,
            pause_epoch,
            mut terminal_orders,
            remainder_plan,
            has_fill_guard,
            resolved_conflict_ids,
        } = settlement;
        self.last_orders_sync = Some(Instant::now());
        terminal_orders.sort_by_key(|(_, response)| (response.update_time, response.order_id));
        if terminal_orders
            .iter()
            .any(|(_, response)| response.executed_qty > Decimal::ZERO)
        {
            // One refresh covers every execution observed before this
            // point. Keep all reservations and the global submission
            // barrier until fresh risk inputs and durable commits exist.
            self.invalidate_account_and_position_caches();
            if !self.sync_account_and_position().await {
                self.mark_orders_reconciliation_blocked().await;
                return None;
            }
        }
        let mut persistence_ok = self.commit_or_remove_terminal_orders(terminal_orders).await;
        if persistence_ok {
            if let Some(plan) = remainder_plan {
                if plan.phase == RemainderPhase::Submitting {
                    if let Err(e) = self.adopt_remainder_target_under_gate(plan).await {
                        warn!("Remainder submission remains unresolved: {}", e);
                        persistence_ok = false;
                    }
                }
            }
        }
        if !persistence_ok {
            self.last_orders_sync = None;
        } else if !(if has_fill_guard {
            self.drain_pair_intents_under_gate(symbol, mode).await
        } else {
            self.drain_pair_intents(symbol, mode).await
        }) {
            persistence_ok = false;
            self.last_orders_sync = None;
        }
        if self.state.pause_epoch() != pause_epoch {
            self.mark_orders_reconciliation_blocked().await;
            return None;
        }
        if persistence_ok
            && !self
                .clear_order_evidence_groups(account_key, symbol, resolved_conflict_ids)
                .await
        {
            self.last_orders_sync = None;
            persistence_ok = false;
        }
        Some(persistence_ok)
    }

    async fn retire_absent_order_intent(&mut self, order: &GridOrder) -> MissingOrderOutcome {
        if let Err(error) = self
            .remove_managed_and_active_order(&order.client_order_id)
            .await
        {
            error!(
                "Could not retire absent order intent {}: {}",
                order.client_order_id, error
            );
            MissingOrderOutcome::PersistenceFailed
        } else {
            self.state
                .add_log(
                    "WARN",
                    format!(
                        "Unaccepted order intent {} retired after confirmed absence",
                        order.client_order_id
                    ),
                )
                .await;
            MissingOrderOutcome::Retired
        }
    }

    async fn inspect_found_missing_order(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        mut order: GridOrder,
        exchange_order: BinanceOrderResponse,
        inconsistent: bool,
    ) -> MissingOrderOutcome {
        if !order.matches_exchange_identity(&exchange_order) {
            self.track_identity_conflict(&order, &exchange_order);
            self.report_reconciliation_state(true).await;
            self.persist_identity_observations(account_key, symbol)
                .await;
            return MissingOrderOutcome::Unresolved;
        }
        // A query may confirm acceptance before the open-orders snapshot catches up.
        // Persist it before a later lookup or restart can lose that distinction.
        if order.order_id.is_none() {
            order.order_id = Some(exchange_order.order_id);
            if let Err(error) = self.save_managed_order(&order).await {
                error!(
                    "Could not persist confirmed order {}: {}",
                    order.client_order_id, error
                );
                return MissingOrderOutcome::PersistenceFailed;
            }
            self.state
                .active_orders
                .write()
                .await
                .insert(order.client_order_id.clone(), order.clone());
        }
        if inconsistent
            || !exchange_order.is_terminal()
            || (exchange_order.executed_qty > Decimal::ZERO
                && exchange_order.price <= Decimal::ZERO
                && exchange_order
                    .avg_price
                    .is_none_or(|price| price <= Decimal::ZERO))
        {
            return MissingOrderOutcome::Unresolved;
        }
        MissingOrderOutcome::Terminal(Box::new((order, exchange_order)))
    }

    async fn inspect_missing_active_order(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        pause_epoch: u64,
        order: GridOrder,
        corrected_terminal: &mut HashMap<String, BinanceOrderResponse>,
    ) -> Option<MissingOrderOutcome> {
        if self.state.pause_epoch() != pause_epoch {
            return None;
        }
        let exchange_result =
            if let Some(corrected) = corrected_terminal.remove(&order.client_order_id) {
                Ok(Some(corrected))
            } else {
                self.lookup_order(&order).await
            };
        let inconsistent = exchange_result
            .as_ref()
            .is_ok_and(|found| found.as_ref().is_some_and(unresolved_filled_quantity));
        if inconsistent {
            self.report_reconciliation_state(true).await;
            self.invalidate_account_and_position_caches();
        }
        if self.state.pause_epoch() != pause_epoch {
            return None;
        }
        match exchange_result {
            Ok(None)
                if (Utc::now() - order.created_at).num_seconds()
                    >= UNKNOWN_SUBMISSION_GRACE_SECS =>
            {
                Some(self.retire_absent_order_intent(&order).await)
            }
            Ok(None) => Some(MissingOrderOutcome::Unresolved),
            Ok(Some(exchange_order)) => Some(
                self.inspect_found_missing_order(
                    account_key,
                    symbol,
                    order,
                    exchange_order,
                    inconsistent,
                )
                .await,
            ),
            Err(e) => {
                warn!(
                    "Could not verify order {} status: {}",
                    order.client_order_id, e
                );
                Some(MissingOrderOutcome::Unresolved)
            }
        }
    }

    pub(super) async fn collect_missing_terminal_orders(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        pause_epoch: u64,
        live_ids: &HashMap<String, BinanceOrderResponse>,
        corrected_terminal: &mut HashMap<String, BinanceOrderResponse>,
    ) -> Option<Vec<(GridOrder, BinanceOrderResponse)>> {
        let mut filled_or_closed: Vec<_> = self
            .state
            .active_orders
            .read()
            .await
            .iter()
            .filter(|(client_id, _)| !live_ids.contains_key(*client_id))
            .map(|(_, order)| order.clone())
            .collect();
        filled_or_closed.sort_by(|a, b| a.client_order_id.cmp(&b.client_order_id));
        let mut terminal_orders = Vec::new();
        let mut unresolved_orders = false;
        let mut persistence_ok = true;
        for order in filled_or_closed {
            match self
                .inspect_missing_active_order(
                    account_key,
                    symbol,
                    pause_epoch,
                    order,
                    corrected_terminal,
                )
                .await?
            {
                MissingOrderOutcome::Terminal(pair) => terminal_orders.push(*pair),
                MissingOrderOutcome::Retired => {}
                MissingOrderOutcome::Unresolved => unresolved_orders = true,
                MissingOrderOutcome::PersistenceFailed => persistence_ok = false,
            }
        }
        if !persistence_ok || unresolved_orders {
            self.report_reconciliation_state(true).await;
            return None;
        }
        Some(terminal_orders)
    }

    fn apply_open_live_order_to_active(
        active: &mut HashMap<String, GridOrder>,
        client_id: &str,
        live_order: &BinanceOrderResponse,
        trust_snapshot: bool,
        uncertain_ids: &HashSet<String>,
        restored_ids: &HashSet<String>,
        sources: (&HashMap<String, GridOrder>, Option<&RemainderPlan>),
    ) -> GridOrder {
        let remaining = live_order.remaining_qty();
        let quantity = if trust_snapshot {
            remaining
        } else {
            active
                .get(client_id)
                .map_or(live_order.orig_qty, |order| order.quantity.max(remaining))
        };
        if !uncertain_ids.contains(client_id) && !restored_ids.contains(client_id) {
            if let Some(plan) = sources.1 {
                if let Some(metadata) = plan.find_order(client_id) {
                    active.insert(client_id.to_owned(), metadata.clone());
                }
            }
            if let Some(intent) = sources.0.get(client_id) {
                active.insert(client_id.to_owned(), intent.clone());
            }
        }
        if let Some(order) = active.get_mut(client_id) {
            order.order_id = Some(live_order.order_id);
            order.price = if trust_snapshot {
                live_order.price
            } else {
                order.price.max(live_order.price)
            };
            order.quantity = quantity;
            order.amount_usdc = order
                .price
                .checked_mul(order.quantity)
                .unwrap_or(Decimal::MAX);
            order.status = live_order.open_order_status();
            order.clone()
        } else {
            let order = GridOrder::from_exchange_response(
                live_order,
                quantity,
                OrderPurpose::Legacy,
                Utc::now(),
            );
            active.insert(client_id.to_owned(), order.clone());
            order
        }
    }

    pub(super) async fn adopt_and_persist_open_live_orders(
        &mut self,
        ctx: OpenLiveOrderAdoption<'_>,
    ) -> bool {
        let mut orders_to_persist = Vec::new();
        {
            let trust_snapshot = ctx.position_ready && self.state.pause_epoch() == ctx.pause_epoch;
            let mut active = self.state.active_orders.write().await;
            for (client_id, live_order) in ctx.live_ids {
                let updated = Self::apply_open_live_order_to_active(
                    &mut active,
                    client_id,
                    live_order,
                    trust_snapshot,
                    ctx.uncertain_ids,
                    ctx.restored_ids,
                    (ctx.pair_intents, ctx.remainder_plan),
                );
                if ctx
                    .persisted_managed
                    .get(client_id)
                    .is_none_or(|persisted| !persisted.matches_persisted_state(&updated))
                {
                    orders_to_persist.push(updated);
                }
            }
        }
        if !orders_to_persist.is_empty() {
            if let Err(e) = self.save_managed_orders(orders_to_persist).await {
                error!("Could not persist reconciled orders: {}", e);
                self.report_reconciliation_state(true).await;
                return false;
            }
        }
        true
    }
}
