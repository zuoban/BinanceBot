#[cfg(test)]
pub(super) use super::evidence::terminal_responses_disagree;
use super::reconciliation_anomalies::invalid_snapshot_price;
use super::snapshot_conflicts::SnapshotConflictScan;
use super::*;

struct OpenLiveOrderAdoption<'a> {
    pause_epoch: u64,
    position_ready: bool,
    live_ids: &'a HashMap<String, BinanceOrderResponse>,
    uncertain_ids: &'a HashSet<String>,
    restored_ids: &'a HashSet<String>,
    pair_intents: &'a HashMap<String, GridOrder>,
    remainder_plan: Option<&'a RemainderPlan>,
    persisted_managed: &'a HashMap<String, GridOrder>,
}

struct LiveSyncSettlement<'a> {
    account_key: [u8; 32],
    symbol: &'a str,
    mode: TradingMode,
    pause_epoch: u64,
    terminal_orders: Vec<(GridOrder, BinanceOrderResponse)>,
    remainder_plan: &'a mut Option<RemainderPlan>,
    has_fill_guard: bool,
    resolved_conflict_keys: &'a [([u8; 32], String)],
}

type LiveSyncPreflight = (
    HashMap<String, GridOrder>,
    Option<RemainderPlan>,
    HashSet<String>,
);

enum MissingOrderOutcome {
    Terminal(Box<(GridOrder, BinanceOrderResponse)>),
    Retired,
    Unresolved,
    PersistenceFailed,
}

struct LiveSnapshotValidation<'a> {
    priority: ReconciliationPriority,
    pause_epoch: u64,
    account_key: [u8; 32],
    symbol: &'a str,
    mode: TradingMode,
    live_orders: Vec<BinanceOrderResponse>,
    pair_intents: &'a HashMap<String, GridOrder>,
    remainder_plan: Option<&'a RemainderPlan>,
    restored_ids: HashSet<String>,
}

pub(super) fn find_durable_order<'a>(
    client_order_id: &str,
    managed: &'a [GridOrder],
    pair_intents: &'a HashMap<String, GridOrder>,
    remainder_plan: Option<&'a RemainderPlan>,
) -> Option<&'a GridOrder> {
    managed
        .iter()
        .find(|order| order.client_order_id == client_order_id)
        .or_else(|| pair_intents.get(client_order_id))
        .or_else(|| remainder_plan.and_then(|plan| plan.find_order(client_order_id)))
}

impl GridTradingEngine {
    async fn prepare_live_orders_sync(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        mode: TradingMode,
    ) -> Option<LiveSyncPreflight> {
        let preflight_symbol = symbol.to_owned();
        let preflight = self
            .state
            .db
            .run_blocking(move |db| {
                let intents = match db.load_pair_intents(&preflight_symbol, mode) {
                    Ok(orders) => orders,
                    Err(e) => return Ok(Err((true, e))),
                };
                match db.load_remainder_plan(&preflight_symbol, mode) {
                    Ok(plan) => Ok(Ok((intents, plan))),
                    Err(e) => Ok(Err((false, e))),
                }
            })
            .await;
        let (pair_intents, remainder_plan) = match preflight {
            Ok(Ok((intents, plan))) => {
                let map: HashMap<String, GridOrder> = intents
                    .into_iter()
                    .map(|order| (order.client_order_id.clone(), order))
                    .collect();
                (map, plan)
            }
            Ok(Err((true, e))) | Err(e) => {
                error!("Could not load pending pair intents: {}", e);
                self.report_reconciliation_state(true).await;
                return None;
            }
            Ok(Err((false, e))) => {
                error!("Could not load remainder recovery metadata: {}", e);
                self.last_orders_sync = None;
                return None;
            }
        };
        // A previous failed managed-order read may have left an exchange-only
        // placeholder in memory. Recover durable metadata before any normal or
        // terminal reconciliation can persist or consume that placeholder.
        if !self.load_identity_observations(account_key, symbol).await
            || !self
                .persist_identity_observations(account_key, symbol)
                .await
        {
            self.last_orders_sync = None;
            return None;
        }
        if self
            .terminal_disagreement_blocked(account_key, symbol)
            .await
        {
            self.last_orders_sync = None;
            self.report_reconciliation_state(true).await;
            return None;
        }
        let (restored_ids, _) = self
            .restore_provisional_orders(account_key, symbol, &pair_intents, remainder_plan.as_ref())
            .await?;
        Some((pair_intents, remainder_plan, restored_ids))
    }

    async fn settle_terminal_and_pending_workflows(
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
            resolved_conflict_keys,
        } = settlement;
        let mut persistence_ok = true;
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
        if persistence_ok {
            let client_ids: Vec<_> = resolved_conflict_keys
                .iter()
                .map(|key| key.1.clone())
                .collect();
            if !self
                .clear_order_evidence_groups(account_key, symbol, &client_ids)
                .await
            {
                self.last_orders_sync = None;
                persistence_ok = false;
            }
        }
        Some(persistence_ok)
    }

    /// Reconcile open orders from Binance in Live mode
    pub(super) async fn sync_live_orders(&mut self) -> bool {
        self.sync_live_orders_with_priority(ReconciliationPriority::Critical)
            .await
    }

    async fn validate_and_adopt_live_snapshot(
        &mut self,
        input: LiveSnapshotValidation<'_>,
    ) -> Option<(
        HashMap<String, BinanceOrderResponse>,
        HashMap<String, BinanceOrderResponse>,
        Vec<([u8; 32], String)>,
    )> {
        let LiveSnapshotValidation {
            priority,
            pause_epoch,
            account_key,
            symbol,
            mode,
            mut live_orders,
            pair_intents,
            remainder_plan,
            mut restored_ids,
        } = input;
        if live_orders
            .iter()
            .any(|order| is_grid_order(&order.client_order_id) && unresolved_filled_quantity(order))
        {
            self.set_global_reconciliation_ready(false).await;
        }
        if !self
            .detect_and_persist_snapshot_conflicts(SnapshotConflictScan {
                account_key,
                symbol,
                live_orders: &live_orders,
                pair_intents,
                remainder_plan,
                restored_ids: &restored_ids,
            })
            .await
        {
            return None;
        }
        let (mut corrected_terminal, resolved_conflict_keys) = self
            .resolve_order_identity_conflicts(
                account_key,
                symbol,
                &mut live_orders,
                &mut restored_ids,
            )
            .await?;
        let mut live_ids: HashMap<String, BinanceOrderResponse> = live_orders
            .into_iter()
            .filter(|order| is_grid_order(&order.client_order_id))
            .map(|o| (o.client_order_id.clone(), o))
            .collect();
        let uncertain_ids = self
            .reconcile_uncertain_zero_fills(
                account_key,
                symbol,
                &mut live_ids,
                pair_intents,
                remainder_plan,
                &mut corrected_terminal,
            )
            .await?;
        let inconsistent_fill = live_ids.values().any(unresolved_filled_quantity);
        if live_ids.values().any(invalid_snapshot_price) {
            let snapshot: Vec<_> = live_ids.values().cloned().collect();
            self.retain_invalid_price_snapshot(account_key, symbol, mode, &snapshot)
                .await;
            return None;
        }
        // Every live snapshot is checked against durable identities;
        // in-memory conflict barriers do not survive a process restart.
        let persisted_managed = self
            .verify_live_snapshot_identities(
                account_key,
                symbol,
                &live_ids,
                pair_intents,
                remainder_plan,
                &mut restored_ids,
            )
            .await?;
        let (needs_position_refresh, position_ready) = self
            .refresh_live_partial_fill_risk(priority, pause_epoch, inconsistent_fill, &live_ids)
            .await;
        if !self
            .adopt_and_persist_open_live_orders(OpenLiveOrderAdoption {
                pause_epoch,
                position_ready,
                live_ids: &live_ids,
                uncertain_ids: &uncertain_ids,
                restored_ids: &restored_ids,
                pair_intents,
                remainder_plan,
                persisted_managed: &persisted_managed,
            })
            .await
        {
            return None;
        }
        if !position_ready {
            self.report_reconciliation_state(true).await;
            return None;
        }
        if needs_position_refresh {
            self.state.check_global_position_limit().await;
        }
        if self.state.pause_epoch() != pause_epoch {
            return None;
        }
        for id in corrected_terminal.keys() {
            live_ids.remove(id);
        }
        Some((live_ids, corrected_terminal, resolved_conflict_keys))
    }

    pub(super) async fn sync_live_orders_with_priority(
        &mut self,
        priority: ReconciliationPriority,
    ) -> bool {
        let pause_epoch = self.state.pause_epoch();
        let (symbol, mode) = {
            let config = self.state.config.read().await;
            (
                config.exchange.symbol.clone(),
                config.exchange.trading_mode(),
            )
        };
        let account_key = self.client.account_identity_key();
        let Some((pair_intents, mut remainder_plan, restored_ids)) = self
            .prepare_live_orders_sync(account_key, &symbol, mode)
            .await
        else {
            return false;
        };
        let open_orders = match priority {
            ReconciliationPriority::Periodic => {
                self.client.get_open_orders_for_periodic_sync(&symbol).await
            }
            ReconciliationPriority::Critical => self.client.get_open_orders(&symbol).await,
        };
        let live_orders = match open_orders {
            Ok(orders) => orders,
            Err(e) => {
                warn!("Error during live orders sync: {}", e);
                self.last_orders_sync = None;
                self.report_reconciliation_state(true).await;
                return false;
            }
        };

        self.last_orders_sync = None;
        let invalid_price = live_orders
            .iter()
            .any(|order| is_grid_order(&order.client_order_id) && invalid_snapshot_price(order));
        let mut fill_guard = self
            .acquire_live_sync_fill_guard(
                invalid_price,
                &live_orders,
                &pair_intents,
                remainder_plan.as_ref(),
            )
            .await;
        if invalid_price {
            self.retain_invalid_price_snapshot(account_key, &symbol, mode, &live_orders)
                .await;
            return false;
        }
        let Some((live_ids, mut corrected_terminal, resolved_conflict_keys)) = self
            .validate_and_adopt_live_snapshot(LiveSnapshotValidation {
                priority,
                pause_epoch,
                account_key,
                symbol: &symbol,
                mode,
                live_orders,
                pair_intents: &pair_intents,
                remainder_plan: remainder_plan.as_ref(),
                restored_ids,
            })
            .await
        else {
            return false;
        };
        let Some(terminal_orders) = self
            .collect_missing_terminal_orders(
                account_key,
                &symbol,
                pause_epoch,
                &live_ids,
                &mut corrected_terminal,
            )
            .await
        else {
            return false;
        };
        let Some(persistence_ok) = self
            .settle_terminal_and_pending_workflows(LiveSyncSettlement {
                account_key,
                symbol: &symbol,
                mode,
                pause_epoch,
                terminal_orders,
                remainder_plan: &mut remainder_plan,
                has_fill_guard: fill_guard.is_some(),
                resolved_conflict_keys: &resolved_conflict_keys,
            })
            .await
        else {
            return false;
        };
        if persistence_ok {
            if let Some(guard) = &mut fill_guard {
                guard.resolve();
            }
        }
        drop(fill_guard);
        self.report_reconciliation_state(!persistence_ok).await;
        persistence_ok
    }

    async fn adopt_and_persist_open_live_orders(&mut self, ctx: OpenLiveOrderAdoption<'_>) -> bool {
        let OpenLiveOrderAdoption {
            pause_epoch,
            position_ready,
            live_ids,
            uncertain_ids,
            restored_ids,
            pair_intents,
            remainder_plan,
            persisted_managed,
        } = ctx;
        let mut orders_to_persist = Vec::new();
        {
            let mut active = self.state.active_orders.write().await;
            for (client_id, live_order) in live_ids {
                // On failure or interruption, retain at least the previous
                // reservation; newly discovered orders reserve original size.
                let remaining = live_order.remaining_qty();
                let quantity = if position_ready && self.state.pause_epoch() == pause_epoch {
                    remaining
                } else {
                    active
                        .get(client_id)
                        .map_or(live_order.orig_qty, |order| order.quantity.max(remaining))
                };
                if !uncertain_ids.contains(client_id) && !restored_ids.contains(client_id) {
                    if let Some(plan) = remainder_plan {
                        if let Some(metadata) = plan.find_order(client_id) {
                            active.insert(client_id.clone(), metadata.clone());
                        }
                    }
                    if let Some(intent) = pair_intents.get(client_id) {
                        active.insert(client_id.clone(), intent.clone());
                    }
                }
                if let Some(order) = active.get_mut(client_id) {
                    order.order_id = Some(live_order.order_id);
                    order.price = if position_ready && self.state.pause_epoch() == pause_epoch {
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
                    if persisted_managed
                        .get(client_id)
                        .is_none_or(|persisted| !persisted.matches_persisted_state(order))
                    {
                        orders_to_persist.push(order.clone());
                    }
                } else {
                    let order = GridOrder::from_exchange_response(
                        live_order,
                        quantity,
                        OrderPurpose::Legacy,
                        Utc::now(),
                    );
                    orders_to_persist.push(order.clone());
                    active.insert(client_id.clone(), order);
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
    async fn inspect_missing_active_order(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        pause_epoch: u64,
        mut order: GridOrder,
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
                if let Err(error) = self
                    .remove_managed_and_active_order(&order.client_order_id)
                    .await
                {
                    error!(
                        "Could not retire absent order intent {}: {}",
                        order.client_order_id, error
                    );
                    Some(MissingOrderOutcome::PersistenceFailed)
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
                    Some(MissingOrderOutcome::Retired)
                }
            }
            Ok(None) => Some(MissingOrderOutcome::Unresolved),
            Ok(Some(exchange_order)) => {
                if !order.matches_exchange_identity(&exchange_order) {
                    self.track_identity_conflict(&order, &exchange_order);
                    self.report_reconciliation_state(true).await;
                    self.persist_identity_observations(account_key, symbol)
                        .await;
                    return Some(MissingOrderOutcome::Unresolved);
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
                        return Some(MissingOrderOutcome::PersistenceFailed);
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
                    return Some(MissingOrderOutcome::Unresolved);
                }
                Some(MissingOrderOutcome::Terminal(Box::new((
                    order,
                    exchange_order,
                ))))
            }
            Err(e) => {
                warn!(
                    "Could not verify order {} status: {}",
                    order.client_order_id, e
                );
                Some(MissingOrderOutcome::Unresolved)
            }
        }
    }

    async fn collect_missing_terminal_orders(
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
