#[cfg(test)]
pub(super) use super::conflict_resolution::terminal_responses_disagree;
use super::conflict_resolution::SnapshotConflictScan;
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

pub(crate) fn find_durable_order<'a>(
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

fn invalid_snapshot_price(order: &BinanceOrderResponse) -> bool {
    order.price <= Decimal::ZERO
        && !(order.is_terminal()
            && order.executed_qty > Decimal::ZERO
            && order.avg_price.is_some_and(|price| price > Decimal::ZERO))
}

impl GridTradingEngine {
    async fn retain_invalid_price_snapshot(
        &mut self,
        account: [u8; 32],
        symbol: &str,
        mode: TradingMode,
        orders: &[BinanceOrderResponse],
    ) {
        self.report_reconciliation_state(true).await;
        let managed = self.load_managed_orders(symbol).await.unwrap_or_default();
        let intents = self
            .load_pair_intent_map(symbol, mode)
            .await
            .unwrap_or_default();
        let remainder = self.load_remainder_plan().await.ok().flatten();
        let state = self.state.clone();
        let mut active = state.active_orders.write().await;
        let mut responses = Vec::new();
        for found in orders
            .iter()
            .filter(|order| is_grid_order(&order.client_order_id))
        {
            let mut expected = find_durable_order(
                &found.client_order_id,
                &managed,
                &intents,
                remainder.as_ref(),
            )
            .or_else(|| active.get(&found.client_order_id))
            .cloned()
            .unwrap_or_else(|| {
                let mut order = GridOrder::from_exchange_response(
                    found,
                    found.orig_qty.max(Decimal::ZERO),
                    OrderPurpose::Unverified,
                    Utc::now(),
                );
                order.order_id = (found.symbol == symbol
                    && OrderSide::from_exchange_str(&found.side).is_some()
                    && orders
                        .iter()
                        .filter(|order| order.client_order_id == found.client_order_id)
                        .count()
                        == 1)
                    .then_some(found.order_id);
                order.symbol = symbol.to_owned();
                order.price = found.price.max(Decimal::ZERO);
                order.amount_usdc = order
                    .price
                    .checked_mul(order.quantity)
                    .unwrap_or(Decimal::MAX);
                order.status = OrderStatus::New;
                order
            });
            if let Some(current) = active.get(&found.client_order_id) {
                if current.has_compatible_identity(&expected) {
                    expected.reserve_max(current.price, current.quantity);
                }
            }
            if expected.matches_exchange_identity(found) {
                expected.reserve_max(found.price, found.orig_qty);
            }
            self.track_identity_conflict(&expected, found);
            active.insert(expected.client_order_id.clone(), expected);
            responses.push((found.client_order_id.clone(), found.clone()));
        }
        let conservative: Vec<_> = managed
            .iter()
            .filter_map(|stored| {
                active
                    .get(&stored.client_order_id)
                    .filter(|order| order.has_compatible_identity(stored))
                    .cloned()
            })
            .collect();
        drop(active);
        let Some(identities) = self.collect_identity_observations(account, symbol) else {
            return;
        };
        let batch = format!("snapshot:{}", Uuid::new_v4());
        if self
            .record_initial_order_response_snapshot(
                account,
                symbol,
                &batch,
                &responses,
                &identities,
            )
            .await
            && !conservative.is_empty()
        {
            if let Err(error) = self.save_managed_orders(conservative).await {
                error!(
                    "Could not persist conservative invalid-snapshot reservations: {}",
                    error
                );
            }
        }
    }

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
        let restored_ids = self
            .restore_provisional_orders_for_sync(
                account_key,
                symbol,
                &pair_intents,
                remainder_plan.as_ref(),
            )
            .await?;
        Some((pair_intents, remainder_plan, restored_ids))
    }

    async fn acquire_live_sync_fill_guard(
        &self,
        invalid_price: bool,
        live_orders: &[BinanceOrderResponse],
        pair_intents: &HashMap<String, GridOrder>,
        remainder_plan: Option<&RemainderPlan>,
    ) -> Option<crate::server::state::PendingFillReconciliation> {
        let known_risk = invalid_price
            || live_orders.iter().any(|order| {
                is_grid_order(&order.client_order_id)
                    && (order.executed_qty > Decimal::ZERO || unresolved_filled_quantity(order))
            })
            || !pair_intents.is_empty()
            || remainder_plan.is_some_and(|plan| plan.phase == RemainderPhase::Submitting)
            || !self.evidence.order_identity_conflicts.is_empty();
        let mut fill_guard = known_risk.then(|| self.state.publish_fill_reconciliation());
        if fill_guard.is_none() {
            let active = match self.state.active_orders.try_read() {
                Ok(active) => active,
                Err(_) => {
                    fill_guard = Some(self.state.publish_fill_reconciliation());
                    self.state.active_orders.read().await
                }
            };
            let missing = if active.is_empty() {
                false
            } else {
                let seen_ids: HashSet<&str> = live_orders
                    .iter()
                    .map(|order| order.client_order_id.as_str())
                    .collect();
                active.keys().any(|id| !seen_ids.contains(id.as_str()))
            };
            if missing && fill_guard.is_none() {
                fill_guard = Some(self.state.publish_fill_reconciliation());
            }
            drop(active);
            if !missing {
                if let Some(mut guard) = fill_guard.take() {
                    guard.resolve();
                }
            }
        }
        if let Some(guard) = &mut fill_guard {
            guard.acquire().await;
        }
        fill_guard
    }

    async fn refresh_live_partial_fill_risk(
        &mut self,
        priority: ReconciliationPriority,
        pause_epoch: u64,
        inconsistent_fill: bool,
        live_ids: &HashMap<String, BinanceOrderResponse>,
    ) -> (bool, bool) {
        // A partial fill moves exposure into the position and changes
        // available margin. Refresh both before reducing its reservation.
        let needs_position_refresh = {
            let active = self.state.active_orders.read().await;
            live_ids.iter().any(|(id, order)| {
                order.executed_qty > Decimal::ZERO
                    && (self.last_account_sync.is_none()
                        || active.get(id).is_none_or(|previous| {
                            previous.status != OrderStatus::PartiallyFilled
                                || previous.quantity
                                    > order
                                        .orig_qty
                                        .checked_sub(order.executed_qty)
                                        .unwrap_or(Decimal::ZERO)
                                        .max(Decimal::ZERO)
                        }))
            })
        };
        if needs_position_refresh || inconsistent_fill {
            self.invalidate_account_and_position_caches().await;
        }
        let position_ready = if inconsistent_fill || self.state.pause_epoch() != pause_epoch {
            false
        } else if needs_position_refresh {
            self.refresh_position_then_account(matches!(priority, ReconciliationPriority::Periodic))
                .await
        } else {
            true
        };
        (needs_position_refresh, position_ready)
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

    pub(super) async fn sync_live_orders_with_priority(
        &mut self,
        priority: ReconciliationPriority,
    ) -> bool {
        let pause_epoch = self.state.pause_epoch();
        let config = self.state.config.read().await.clone();
        let symbol = config.exchange.symbol.clone();
        let mode = config.exchange.trading_mode();
        let account_key = self.client.account_identity_key();
        let Some((pair_intents, mut remainder_plan, mut restored_ids)) = self
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
        let mut live_orders = match open_orders {
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
        if live_orders
            .iter()
            .any(|order| is_grid_order(&order.client_order_id) && unresolved_filled_quantity(order))
        {
            self.set_global_reconciliation_ready(false).await;
        }
        if !self
            .detect_and_persist_snapshot_conflicts(SnapshotConflictScan {
                account_key,
                symbol: &symbol,
                live_orders: &live_orders,
                pair_intents: &pair_intents,
                remainder_plan: remainder_plan.as_ref(),
                restored_ids: &restored_ids,
            })
            .await
        {
            return false;
        }
        let Some((mut corrected_terminal, resolved_conflict_keys)) = self
            .resolve_order_identity_conflicts(
                account_key,
                &symbol,
                &mut live_orders,
                &mut restored_ids,
            )
            .await
        else {
            return false;
        };
        let mut live_ids: HashMap<String, BinanceOrderResponse> = live_orders
            .into_iter()
            .filter(|order| is_grid_order(&order.client_order_id))
            .map(|o| (o.client_order_id.clone(), o))
            .collect();
        let Some(uncertain_ids) = self
            .reconcile_uncertain_zero_fills(
                account_key,
                &symbol,
                &mut live_ids,
                &pair_intents,
                remainder_plan.as_ref(),
                &mut corrected_terminal,
            )
            .await
        else {
            return false;
        };
        let inconsistent_fill = live_ids.values().any(unresolved_filled_quantity);
        if live_ids.values().any(invalid_snapshot_price) {
            let snapshot: Vec<_> = live_ids.values().cloned().collect();
            self.retain_invalid_price_snapshot(account_key, &symbol, mode, &snapshot)
                .await;
            return false;
        }

        // Every live snapshot is checked against durable identities;
        // in-memory conflict barriers do not survive a process restart.
        let Some(persisted_managed) = self
            .verify_live_snapshot_identities(
                account_key,
                &symbol,
                &live_ids,
                &pair_intents,
                remainder_plan.as_ref(),
                &mut restored_ids,
            )
            .await
        else {
            return false;
        };

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
                pair_intents: &pair_intents,
                remainder_plan: remainder_plan.as_ref(),
                persisted_managed: &persisted_managed,
            })
            .await
        {
            return false;
        }
        if !position_ready {
            self.report_reconciliation_state(true).await;
            return false;
        }
        if needs_position_refresh {
            self.state.check_global_position_limit().await;
        }
        if self.state.pause_epoch() != pause_epoch {
            return false;
        }
        for id in corrected_terminal.keys() {
            live_ids.remove(id);
        }

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

    async fn restore_provisional_orders_for_sync(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        pair_intents: &HashMap<String, GridOrder>,
        remainder_plan: Option<&RemainderPlan>,
    ) -> Option<HashSet<String>> {
        let pending_ids = self.provisional_order_ids_for(account_key);
        let mut restored_ids = HashSet::new();
        if pending_ids.is_empty() {
            return Some(restored_ids);
        }
        let managed = match self.load_managed_orders(symbol).await {
            Ok(orders) => orders,
            Err(error) => {
                error!("Could not restore provisional order metadata: {}", error);
                self.last_orders_sync = None;
                self.report_reconciliation_state(true).await;
                return None;
            }
        };
        let mut conflict = false;
        {
            let state = self.state.clone();
            let mut active = state.active_orders.write().await;
            for id in &pending_ids {
                if let Some(expected) = self
                    .evidence
                    .order_identity_conflicts
                    .get(&(account_key, id.clone()))
                    .cloned()
                {
                    // The conflict may have been recorded while durable
                    // metadata was unavailable; the disk record is authoritative.
                    let mut authoritative =
                        find_durable_order(id, &managed, pair_intents, remainder_plan)
                            .cloned()
                            .unwrap_or_else(|| expected.clone());
                    authoritative.adopt_compatible_order_id(&expected);
                    for observed in std::iter::once(&expected).chain(active.get(id)) {
                        if authoritative.has_compatible_identity(observed) {
                            authoritative.reserve_max(observed.price, observed.quantity);
                        }
                    }
                    if let Some(observed) =
                        active.get(id).and_then(ObservedOrderIdentity::from_order)
                    {
                        if !observed.matches_confirmed_order(&authoritative) {
                            self.insert_identity_observation((account_key, id.clone()), observed);
                        }
                    }
                    active.insert(id.clone(), authoritative.clone());
                    let key = (account_key, id.clone());
                    self.evidence
                        .order_identity_conflicts
                        .insert(key.clone(), authoritative.clone());
                    self.record_differing_identity(&key, &expected, &authoritative);
                    self.promote_provisional_observations(&key, &authoritative);
                    conflict = true;
                    continue;
                }
                let Some(current) = active.get(id).cloned() else {
                    self.clear_provisional_order(&(account_key, id.clone()));
                    continue;
                };
                let Some(durable) = find_durable_order(id, &managed, pair_intents, remainder_plan)
                else {
                    continue;
                };
                if !durable.has_compatible_identity(&current) {
                    active.insert(id.clone(), durable.clone());
                    let key = (account_key, id.clone());
                    self.evidence
                        .order_identity_conflicts
                        .insert(key.clone(), durable.clone());
                    self.record_differing_identity(&key, &current, durable);
                    self.promote_provisional_observations(&key, durable);
                    conflict = true;
                } else {
                    let mut restored = durable.clone();
                    restored.order_id = durable.order_id.or(current.order_id);
                    restored.reserve_quantity(current.quantity);
                    active.insert(id.clone(), restored);
                }
                restored_ids.insert(id.clone());
                let key = (account_key, id.clone());
                if !self.evidence.order_identity_conflicts.contains_key(&key) {
                    self.clear_provisional_order(&key);
                }
            }
        }
        if conflict {
            self.last_orders_sync = None;
            self.report_reconciliation_state(true).await;
            if !self
                .persist_identity_observations(account_key, symbol)
                .await
            {
                return None;
            }
            // Still fetch the exchange snapshot; only a matching identity can
            // resolve the retained conflict.
        }
        Some(restored_ids)
    }

    async fn reconcile_uncertain_zero_fills(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        live_ids: &mut HashMap<String, BinanceOrderResponse>,
        pair_intents: &HashMap<String, GridOrder>,
        remainder_plan: Option<&RemainderPlan>,
        corrected_terminal: &mut HashMap<String, BinanceOrderResponse>,
    ) -> Option<HashSet<String>> {
        let uncertain: Vec<_> = live_ids
            .values()
            .filter(|order| unresolved_filled_quantity(order))
            .cloned()
            .collect();
        let uncertain_ids: HashSet<_> = uncertain
            .iter()
            .map(|order| order.client_order_id.clone())
            .collect();
        if uncertain.is_empty() {
            return Some(uncertain_ids);
        }
        // A prior correction cannot prove that the exchange order
        // has not changed since. Block before each verification.
        self.set_global_reconciliation_ready(false).await;
        self.invalidate_account_and_position_caches().await;
        for found in &uncertain {
            self.track_provisional_observation(found);
        }
        // Register newly discovered identities before touching the
        // database, so cancellation can still find them on DB failure.
        let mut provisional_ids: HashSet<_> = uncertain
            .iter()
            .filter(|order| {
                self.evidence
                    .provisional_order_ids
                    .contains(&(account_key, order.client_order_id.clone()))
            })
            .map(|order| order.client_order_id.clone())
            .collect();
        {
            let state = self.state.clone();
            let mut active = state.active_orders.write().await;
            for found in &uncertain {
                if let Some(existing) = active.get(&found.client_order_id) {
                    let key = (account_key, found.client_order_id.clone());
                    self.evidence.provisional_order_ids.insert(key);
                    if !existing.matches_exchange_identity(found) {
                        self.track_identity_conflict(existing, found);
                    }
                    continue;
                }
                let intent = pair_intents.get(&found.client_order_id);
                let remainder =
                    remainder_plan.and_then(|plan| plan.find_order(&found.client_order_id));
                if [intent, remainder]
                    .into_iter()
                    .flatten()
                    .any(|known| !known.matches_exchange_identity(found))
                {
                    if let Some(known) = intent.or(remainder) {
                        self.track_identity_conflict(known, found);
                        active.insert(known.client_order_id.clone(), known.clone());
                    }
                    continue;
                }
                if found.symbol != symbol || OrderSide::from_exchange_str(&found.side).is_none() {
                    continue;
                }
                let mut order = intent.or(remainder).cloned().unwrap_or_else(|| {
                    GridOrder::from_exchange_response(
                        found,
                        found.orig_qty,
                        OrderPurpose::Legacy,
                        Utc::now(),
                    )
                });
                order.order_id = Some(found.order_id);
                order.reserve_quantity(found.orig_qty);
                provisional_ids.insert(order.client_order_id.clone());
                self.evidence
                    .provisional_order_ids
                    .insert((account_key, order.client_order_id.clone()));
                active.insert(order.client_order_id.clone(), order);
            }
        }
        if !self
            .persist_identity_observations(account_key, symbol)
            .await
        {
            return None;
        }
        let managed = match self.load_managed_orders(symbol).await {
            Ok(orders) => orders,
            Err(error) => {
                error!(
                    "Could not load managed orders for uncertain fill: {}",
                    error
                );
                self.report_reconciliation_state(true).await;
                return None;
            }
        };
        let mut identity_conflict = false;
        let mut persisted_ids = Vec::new();
        let mut to_persist = Vec::new();
        {
            let state = self.state.clone();
            let mut active = state.active_orders.write().await;
            for found in &uncertain {
                let mut candidates = Vec::new();
                if let Some(order) = managed
                    .iter()
                    .find(|order| order.client_order_id == found.client_order_id)
                {
                    candidates.push(order.clone());
                }
                if let Some(order) = active.get(&found.client_order_id) {
                    candidates.push(order.clone());
                }
                if let Some(order) = pair_intents.get(&found.client_order_id) {
                    candidates.push(order.clone());
                }
                if let Some(order) =
                    remainder_plan.and_then(|plan| plan.find_order(&found.client_order_id))
                {
                    candidates.push(order.clone());
                }
                let conflict = found.symbol != symbol
                    || OrderSide::from_exchange_str(&found.side).is_none()
                    || candidates
                        .iter()
                        .any(|order| !order.matches_exchange_identity(found));
                if conflict {
                    identity_conflict = true;
                    if let Some(expected) = candidates.first() {
                        self.track_identity_conflict(expected, found);
                    }
                    if provisional_ids.contains(&found.client_order_id) {
                        if let Some(original) = managed
                            .iter()
                            .find(|order| order.client_order_id == found.client_order_id)
                        {
                            active.insert(original.client_order_id.clone(), original.clone());
                        }
                    } else if let Some(original) = candidates.into_iter().next() {
                        active
                            .entry(original.client_order_id.clone())
                            .or_insert(original);
                    }
                    error!(
                        "Conflicting exchange identity for uncertain order {}",
                        found.client_order_id
                    );
                    continue;
                }
                let metadata = candidates.into_iter().next();
                let mut order = metadata.unwrap_or_else(|| {
                    GridOrder::from_exchange_response(
                        found,
                        found.orig_qty,
                        OrderPurpose::Legacy,
                        Utc::now(),
                    )
                });
                order.order_id = Some(found.order_id);
                order.reserve_quantity(found.orig_qty);
                active.insert(order.client_order_id.clone(), order.clone());
                persisted_ids.push(order.client_order_id.clone());
                to_persist.push(order);
            }
        }
        for found in &uncertain {
            let key = (account_key, found.client_order_id.clone());
            if !self.evidence.order_identity_conflicts.contains_key(&key)
                && managed
                    .iter()
                    .any(|order| order.client_order_id == found.client_order_id)
            {
                self.evidence.provisional_order_ids.remove(&key);
            }
        }
        if let Err(error) = self.save_managed_orders(to_persist).await {
            error!(
                "Could not persist orders with unknown fill quantities: {}",
                error
            );
            self.report_reconciliation_state(true).await;
            return None;
        }
        for id in persisted_ids {
            let key = (account_key, id);
            self.clear_provisional_order(&key);
        }
        if identity_conflict {
            self.report_reconciliation_state(true).await;
            self.persist_identity_observations(account_key, symbol)
                .await;
            return None;
        }
        for original in uncertain {
            match self
                .client
                .get_order(&original.symbol, original.order_id)
                .await
            {
                Ok(corrected)
                    if corrected.matches_identity(&original)
                        && !unresolved_filled_quantity(&corrected)
                        && BinanceOrderResponse::is_known_status(&corrected.status) =>
                {
                    if corrected.is_terminal() {
                        corrected_terminal
                            .insert(corrected.client_order_id.clone(), corrected.clone());
                    }
                    live_ids.insert(corrected.client_order_id.clone(), corrected);
                }
                _ => {
                    self.invalidate_account_and_position_caches().await;
                    self.set_global_reconciliation_ready(false).await;
                }
            }
        }
        Some(uncertain_ids)
    }

    async fn verify_live_snapshot_identities(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        live_ids: &HashMap<String, BinanceOrderResponse>,
        pair_intents: &HashMap<String, GridOrder>,
        remainder_plan: Option<&RemainderPlan>,
        restored_ids: &mut HashSet<String>,
    ) -> Option<HashMap<String, GridOrder>> {
        if live_ids.is_empty() {
            return Some(HashMap::new());
        }
        let managed = match self.load_managed_orders(symbol).await {
            Ok(orders) => orders,
            Err(error) => {
                error!("Could not verify live order identities: {}", error);
                self.report_reconciliation_state(true).await;
                for found in live_ids.values() {
                    self.track_provisional_observation(found);
                }
                let state = self.state.clone();
                let mut active = state.active_orders.write().await;
                for found in live_ids.values() {
                    let key = (account_key, found.client_order_id.clone());
                    if let Some(existing) = active.get(&found.client_order_id) {
                        self.evidence.provisional_order_ids.insert(key.clone());
                        if !existing.matches_exchange_identity(found) {
                            self.track_identity_conflict(existing, found);
                        } else {
                            let mut conservative = existing.clone();
                            conservative.reserve_max(found.price, found.orig_qty);
                            active.insert(found.client_order_id.clone(), conservative);
                        }
                        continue;
                    }
                    if found.symbol != symbol || OrderSide::from_exchange_str(&found.side).is_none()
                    {
                        continue;
                    }
                    if let Some(known) = find_durable_order(
                        &found.client_order_id,
                        &[],
                        pair_intents,
                        remainder_plan,
                    ) {
                        let mut order = known.clone();
                        if !known.matches_exchange_identity(found) {
                            self.track_identity_conflict(known, found);
                        } else {
                            order.order_id = Some(found.order_id);
                            order.reserve_max(found.price, found.orig_qty);
                        }
                        self.evidence.provisional_order_ids.insert(key);
                        active.insert(order.client_order_id.clone(), order);
                        continue;
                    }
                    let order = GridOrder::from_exchange_response(
                        found,
                        found.orig_qty,
                        OrderPurpose::Legacy,
                        Utc::now(),
                    );
                    self.evidence
                        .provisional_order_ids
                        .insert((account_key, order.client_order_id.clone()));
                    active.insert(order.client_order_id.clone(), order);
                }
                drop(active);
                self.persist_identity_observations(account_key, symbol)
                    .await;
                return None;
            }
        };
        let managed_by_id: HashMap<String, GridOrder> = managed
            .into_iter()
            .map(|order| (order.client_order_id.clone(), order))
            .collect();
        let mut conflicts = Vec::new();
        let mut invalid_live_identity = false;
        let state = self.state.clone();
        let active = state.active_orders.read().await;
        for found in live_ids.values() {
            if found.symbol != symbol || OrderSide::from_exchange_str(&found.side).is_none() {
                invalid_live_identity = true;
                self.track_provisional_observation(found);
            }
            let candidates = [
                managed_by_id.get(&found.client_order_id),
                active.get(&found.client_order_id),
                pair_intents.get(&found.client_order_id),
                remainder_plan.and_then(|plan| plan.find_order(&found.client_order_id)),
            ];
            if let Some(expected) = candidates
                .into_iter()
                .flatten()
                .find(|order| !order.matches_exchange_identity(found))
            {
                conflicts.push((expected.clone(), found.clone()));
            }
        }
        drop(active);
        if invalid_live_identity && conflicts.is_empty() {
            self.report_reconciliation_state(true).await;
            return None;
        }
        if !conflicts.is_empty() {
            self.report_reconciliation_state(true).await;
            {
                let state = self.state.clone();
                let mut active = state.active_orders.write().await;
                for (durable, found) in conflicts {
                    self.track_identity_conflict(&durable, &found);
                    active.insert(durable.client_order_id.clone(), durable);
                }
            }
            self.persist_identity_observations(account_key, symbol)
                .await;
            return None;
        }
        let mut active = self.state.active_orders.write().await;
        for found in live_ids.values() {
            if let Some(durable) = managed_by_id
                .get(&found.client_order_id)
                .or_else(|| pair_intents.get(&found.client_order_id))
                .or_else(|| remainder_plan.and_then(|plan| plan.find_order(&found.client_order_id)))
            {
                let mut restored = durable.clone();
                if let Some(current) = active.get(&found.client_order_id) {
                    if current.has_compatible_identity(durable) {
                        restored.order_id = durable.order_id.or(current.order_id);
                        restored.reserve_max(current.price, current.quantity);
                    }
                }
                restored.reserve_max(found.price, restored.quantity);
                active.insert(restored.client_order_id.clone(), restored);
                restored_ids.insert(found.client_order_id.clone());
            }
        }
        Some(managed_by_id)
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
                let remaining = live_order
                    .orig_qty
                    .checked_sub(live_order.executed_qty)
                    .unwrap_or(Decimal::ZERO)
                    .max(Decimal::ZERO);
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

    async fn collect_missing_terminal_orders(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        pause_epoch: u64,
        live_ids: &HashMap<String, BinanceOrderResponse>,
        corrected_terminal: &mut HashMap<String, BinanceOrderResponse>,
    ) -> Option<Vec<(GridOrder, BinanceOrderResponse)>> {
        let mut filled_or_closed = Vec::new();
        {
            let active = self.state.active_orders.read().await;
            for (client_id, order) in active.iter() {
                if !live_ids.contains_key(client_id) {
                    filled_or_closed.push(order.clone());
                }
            }
        }
        let mut terminal_orders = Vec::new();
        let mut unresolved_orders = false;
        let mut persistence_ok = true;
        for mut order in filled_or_closed {
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
                        persistence_ok = false;
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
                    }
                }
                Ok(None) => unresolved_orders = true,
                Ok(Some(exchange_order)) => {
                    if !order.matches_exchange_identity(&exchange_order) {
                        self.track_identity_conflict(&order, &exchange_order);
                        self.report_reconciliation_state(true).await;
                        self.persist_identity_observations(account_key, symbol)
                            .await;
                        unresolved_orders = true;
                        continue;
                    }
                    // A query may confirm acceptance before the open-
                    // orders snapshot catches up. Persist it before a
                    // later lookup or restart can lose that distinction.
                    if order.order_id.is_none() {
                        order.order_id = Some(exchange_order.order_id);
                        if let Err(error) = self.save_managed_order(&order).await {
                            error!(
                                "Could not persist confirmed order {}: {}",
                                order.client_order_id, error
                            );
                            persistence_ok = false;
                            continue;
                        }
                        self.state
                            .active_orders
                            .write()
                            .await
                            .insert(order.client_order_id.clone(), order.clone());
                    }
                    if inconsistent {
                        unresolved_orders = true;
                        continue;
                    }
                    if !exchange_order.is_terminal() {
                        unresolved_orders = true;
                        continue;
                    }
                    if exchange_order.executed_qty > Decimal::ZERO
                        && exchange_order.price <= Decimal::ZERO
                        && exchange_order
                            .avg_price
                            .is_none_or(|price| price <= Decimal::ZERO)
                    {
                        unresolved_orders = true;
                        continue;
                    }
                    terminal_orders.push((order, exchange_order));
                }
                Err(e) => {
                    warn!(
                        "Could not verify order {} status: {}",
                        order.client_order_id, e
                    );
                    unresolved_orders = true;
                }
            }
        }
        if !persistence_ok || unresolved_orders {
            self.report_reconciliation_state(true).await;
            return None;
        }
        Some(terminal_orders)
    }
}
