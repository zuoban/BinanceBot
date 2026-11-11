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
        for (mut order, exchange_order) in terminal_orders {
            if exchange_order.executed_qty > Decimal::ZERO {
                order.apply_terminal_fill(&exchange_order, exchange_fill_time(&exchange_order));
                if !self.commit_order_fill(&mut order).await {
                    persistence_ok = false;
                }
            } else {
                if let Err(e) = self
                    .remove_managed_and_active_order(&order.client_order_id)
                    .await
                {
                    error!(
                        "Could not delete closed order {}: {}",
                        order.client_order_id, e
                    );
                    persistence_ok = false;
                }
                debug!(
                    "Order {} closed without a fill ({})",
                    order.client_order_id, exchange_order.status
                );
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
            self.invalidate_account_and_position_caches().await;
            if !self.sync_account_and_position().await {
                self.last_orders_sync = None;
                self.report_reconciliation_state(true).await;
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
            self.last_orders_sync = None;
            self.report_reconciliation_state(true).await;
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
            self.invalidate_account_and_position_caches().await;
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
        let filled_or_closed: Vec<_> = self
            .state
            .active_orders
            .read()
            .await
            .iter()
            .filter(|(client_id, _)| !live_ids.contains_key(*client_id))
            .map(|(_, order)| order.clone())
            .collect();
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
}
