use super::reconciliation::find_durable_order;
use super::*;

struct UncertainManagedReconciliation<'a> {
    account_key: [u8; 32],
    symbol: &'a str,
    uncertain: &'a [BinanceOrderResponse],
    managed: &'a [GridOrder],
    provisional_ids: &'a HashSet<String>,
    pair_intents: &'a HashMap<String, GridOrder>,
    remainder_plan: Option<&'a RemainderPlan>,
}

impl GridTradingEngine {
    fn restore_single_provisional_order(
        &mut self,
        account_key: [u8; 32],
        id: &str,
        managed: &[GridOrder],
        pair_intents: &HashMap<String, GridOrder>,
        remainder_plan: Option<&RemainderPlan>,
        active: &mut HashMap<String, GridOrder>,
    ) -> (bool, bool) {
        if let Some(expected) = self.evidence.get_conflict(account_key, id).cloned() {
            // The conflict may have been recorded while durable metadata
            // was unavailable; the disk record is authoritative.
            let mut authoritative = find_durable_order(id, managed, pair_intents, remainder_plan)
                .cloned()
                .unwrap_or_else(|| expected.clone());
            authoritative.adopt_compatible_order_id(&expected);
            for observed in std::iter::once(&expected).chain(active.get(id)) {
                if authoritative.has_compatible_identity(observed) {
                    authoritative.reserve_max(observed.price, observed.quantity);
                }
            }
            if let Some(observed) = active.get(id).and_then(ObservedOrderIdentity::from_order) {
                if !observed.matches_confirmed_order(&authoritative) {
                    self.insert_identity_observation(account_key, id, observed);
                }
            }
            active.insert(id.to_owned(), authoritative.clone());
            self.evidence
                .insert_conflict(account_key, authoritative.clone());
            self.record_differing_identity(account_key, id, &expected, &authoritative);
            self.promote_provisional_observations(account_key, id, &authoritative);
            return (false, true);
        }
        let Some(current) = active.get(id).cloned() else {
            self.evidence.clear_provisional(account_key, id);
            return (false, false);
        };
        let Some(durable) = find_durable_order(id, managed, pair_intents, remainder_plan) else {
            return (false, false);
        };
        if !durable.has_compatible_identity(&current) {
            active.insert(id.to_owned(), durable.clone());
            self.evidence.insert_conflict(account_key, durable.clone());
            self.record_differing_identity(account_key, id, &current, durable);
            self.promote_provisional_observations(account_key, id, durable);
            return (true, true);
        }
        let mut restored = durable.clone();
        restored.order_id = durable.order_id.or(current.order_id);
        restored.reserve_quantity(current.quantity);
        active.insert(id.to_owned(), restored);
        if !self.evidence.has_conflict(account_key, id) {
            self.evidence.clear_provisional(account_key, id);
        }
        (true, false)
    }

    pub(super) async fn restore_provisional_orders(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        pair_intents: &HashMap<String, GridOrder>,
        remainder_plan: Option<&RemainderPlan>,
    ) -> Option<(HashSet<String>, bool)> {
        let pending_ids = self.evidence.provisional_order_ids_for(account_key);
        let mut restored_ids = HashSet::new();
        if pending_ids.is_empty() {
            return Some((restored_ids, false));
        }
        let managed = match self.load_managed_orders(symbol).await {
            Ok(orders) => orders,
            Err(error) => {
                error!("Could not restore provisional order metadata: {}", error);
                self.mark_orders_reconciliation_blocked().await;
                return None;
            }
        };
        let mut conflict = false;
        {
            let state = self.state.clone();
            let mut active = state.active_orders.write().await;
            for id in &pending_ids {
                let (restored, order_conflict) = self.restore_single_provisional_order(
                    account_key,
                    id,
                    &managed,
                    pair_intents,
                    remainder_plan,
                    &mut active,
                );
                if restored {
                    restored_ids.insert(id.clone());
                }
                conflict |= order_conflict;
            }
        }
        if conflict {
            self.mark_orders_reconciliation_blocked().await;
            if !self
                .persist_identity_observations(account_key, symbol)
                .await
            {
                return None;
            }
        }
        Some((restored_ids, conflict))
    }

    async fn stage_provisional_uncertain_fills(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        uncertain: &[BinanceOrderResponse],
        pair_intents: &HashMap<String, GridOrder>,
        remainder_plan: Option<&RemainderPlan>,
    ) -> HashSet<String> {
        // Register newly discovered identities before touching the
        // database, so cancellation can still find them on DB failure.
        let mut provisional_ids: HashSet<_> = uncertain
            .iter()
            .filter(|order| {
                self.evidence
                    .is_provisional(account_key, &order.client_order_id)
            })
            .map(|order| order.client_order_id.clone())
            .collect();
        let state = self.state.clone();
        let mut active = state.active_orders.write().await;
        for found in uncertain {
            if let Some(existing) = active.get(&found.client_order_id) {
                self.evidence
                    .mark_provisional(account_key, &found.client_order_id);
                if !existing.matches_exchange_identity(found) {
                    self.track_identity_conflict(existing, found);
                }
                continue;
            }
            let intent = pair_intents.get(&found.client_order_id);
            let remainder = remainder_plan.and_then(|plan| plan.find_order(&found.client_order_id));
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
            if !found.has_valid_symbol_and_side(symbol) {
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
                .mark_provisional(account_key, &order.client_order_id);
            active.insert(order.client_order_id.clone(), order);
        }
        provisional_ids
    }

    async fn stage_uncertain_against_managed(
        &mut self,
        ctx: &UncertainManagedReconciliation<'_>,
    ) -> (bool, Vec<String>, Vec<GridOrder>) {
        let mut identity_conflict = false;
        let mut persisted_ids = Vec::new();
        let mut to_persist = Vec::new();
        let state = self.state.clone();
        let mut active = state.active_orders.write().await;
        for found in ctx.uncertain {
            let candidates: Vec<_> = [
                ctx.managed
                    .iter()
                    .find(|order| order.client_order_id == found.client_order_id),
                active.get(&found.client_order_id),
                ctx.pair_intents.get(&found.client_order_id),
                ctx.remainder_plan
                    .and_then(|plan| plan.find_order(&found.client_order_id)),
            ]
            .into_iter()
            .flatten()
            .cloned()
            .collect();
            let conflict = !found.has_valid_symbol_and_side(ctx.symbol)
                || candidates
                    .iter()
                    .any(|order| !order.matches_exchange_identity(found));
            if conflict {
                identity_conflict = true;
                if let Some(expected) = candidates.first() {
                    self.track_identity_conflict(expected, found);
                }
                if ctx.provisional_ids.contains(&found.client_order_id) {
                    if let Some(original) = ctx
                        .managed
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
            let mut order = candidates.into_iter().next().unwrap_or_else(|| {
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
        (identity_conflict, persisted_ids, to_persist)
    }

    async fn reconcile_uncertain_against_managed(
        &mut self,
        ctx: UncertainManagedReconciliation<'_>,
    ) -> bool {
        let (identity_conflict, persisted_ids, to_persist) =
            self.stage_uncertain_against_managed(&ctx).await;
        for found in ctx.uncertain {
            if !self
                .evidence
                .has_conflict(ctx.account_key, &found.client_order_id)
                && ctx
                    .managed
                    .iter()
                    .any(|order| order.client_order_id == found.client_order_id)
            {
                self.evidence
                    .remove_provisional_id(ctx.account_key, &found.client_order_id);
            }
        }
        if let Err(error) = self.save_managed_orders(to_persist).await {
            error!(
                "Could not persist orders with unknown fill quantities: {}",
                error
            );
            self.report_reconciliation_state(true).await;
            return false;
        }
        for id in persisted_ids {
            self.evidence.clear_provisional(ctx.account_key, &id);
        }
        if identity_conflict {
            self.report_reconciliation_state(true).await;
            self.persist_identity_observations(ctx.account_key, ctx.symbol)
                .await;
            return false;
        }
        true
    }

    async fn query_corrected_uncertain_orders(
        &mut self,
        uncertain: Vec<BinanceOrderResponse>,
        live_ids: &mut HashMap<String, BinanceOrderResponse>,
        corrected_terminal: &mut HashMap<String, BinanceOrderResponse>,
    ) {
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
                    self.invalidate_account_and_position_caches();
                    self.set_global_reconciliation_ready(false).await;
                }
            }
        }
    }

    pub(super) async fn reconcile_uncertain_zero_fills(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        live_ids: &mut HashMap<String, BinanceOrderResponse>,
        pair_intents: &HashMap<String, GridOrder>,
        remainder_plan: Option<&RemainderPlan>,
        corrected_terminal: &mut HashMap<String, BinanceOrderResponse>,
    ) -> Option<HashSet<String>> {
        let mut uncertain: Vec<_> = live_ids
            .values()
            .filter(|order| unresolved_filled_quantity(order))
            .cloned()
            .collect();
        uncertain.sort_by(|a, b| a.client_order_id.cmp(&b.client_order_id));
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
        self.invalidate_account_and_position_caches();
        for found in &uncertain {
            self.track_provisional_observation(found);
        }
        let provisional_ids = self
            .stage_provisional_uncertain_fills(
                account_key,
                symbol,
                &uncertain,
                pair_intents,
                remainder_plan,
            )
            .await;
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
        if !self
            .reconcile_uncertain_against_managed(UncertainManagedReconciliation {
                account_key,
                symbol,
                uncertain: &uncertain,
                managed: &managed,
                provisional_ids: &provisional_ids,
                pair_intents,
                remainder_plan,
            })
            .await
        {
            return None;
        }
        self.query_corrected_uncertain_orders(uncertain, live_ids, corrected_terminal)
            .await;
        Some(uncertain_ids)
    }
}
