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

pub(super) fn invalid_snapshot_price(order: &BinanceOrderResponse) -> bool {
    order.price <= Decimal::ZERO
        && !(order.is_terminal()
            && order.executed_qty > Decimal::ZERO
            && order.avg_price.is_some_and(|price| price > Decimal::ZERO))
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
        let key = (account_key, id.to_owned());
        if let Some(expected) = self.evidence.order_identity_conflicts.get(&key).cloned() {
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
                    self.insert_identity_observation(key.clone(), observed);
                }
            }
            active.insert(id.to_owned(), authoritative.clone());
            self.evidence
                .order_identity_conflicts
                .insert(key.clone(), authoritative.clone());
            self.record_differing_identity(&key, &expected, &authoritative);
            self.promote_provisional_observations(&key, &authoritative);
            return (false, true);
        }
        let Some(current) = active.get(id).cloned() else {
            self.clear_provisional_order(&key);
            return (false, false);
        };
        let Some(durable) = find_durable_order(id, managed, pair_intents, remainder_plan) else {
            return (false, false);
        };
        if !durable.has_compatible_identity(&current) {
            active.insert(id.to_owned(), durable.clone());
            self.evidence
                .order_identity_conflicts
                .insert(key.clone(), durable.clone());
            self.record_differing_identity(&key, &current, durable);
            self.promote_provisional_observations(&key, durable);
            return (true, true);
        }
        let mut restored = durable.clone();
        restored.order_id = durable.order_id.or(current.order_id);
        restored.reserve_quantity(current.quantity);
        active.insert(id.to_owned(), restored);
        if !self.evidence.order_identity_conflicts.contains_key(&key) {
            self.clear_provisional_order(&key);
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
        let pending_ids = self.provisional_order_ids_for(account_key);
        let mut restored_ids = HashSet::new();
        if pending_ids.is_empty() {
            return Some((restored_ids, false));
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
            self.last_orders_sync = None;
            self.report_reconciliation_state(true).await;
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
                    .provisional_order_ids
                    .contains(&(account_key, order.client_order_id.clone()))
            })
            .map(|order| order.client_order_id.clone())
            .collect();
        let state = self.state.clone();
        let mut active = state.active_orders.write().await;
        for found in uncertain {
            if let Some(existing) = active.get(&found.client_order_id) {
                let key = (account_key, found.client_order_id.clone());
                self.evidence.provisional_order_ids.insert(key);
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
                .provisional_order_ids
                .insert((account_key, order.client_order_id.clone()));
            active.insert(order.client_order_id.clone(), order);
        }
        provisional_ids
    }

    async fn reconcile_uncertain_against_managed(
        &mut self,
        ctx: UncertainManagedReconciliation<'_>,
    ) -> bool {
        let UncertainManagedReconciliation {
            account_key,
            symbol,
            uncertain,
            managed,
            provisional_ids,
            pair_intents,
            remainder_plan,
        } = ctx;
        // Reconcile staged uncertain fills against durable managed orders.
        let mut identity_conflict = false;
        let mut persisted_ids = Vec::new();
        let mut to_persist = Vec::new();
        {
            let state = self.state.clone();
            let mut active = state.active_orders.write().await;
            for found in uncertain {
                let candidates: Vec<_> = [
                    managed
                        .iter()
                        .find(|order| order.client_order_id == found.client_order_id),
                    active.get(&found.client_order_id),
                    pair_intents.get(&found.client_order_id),
                    remainder_plan.and_then(|plan| plan.find_order(&found.client_order_id)),
                ]
                .into_iter()
                .flatten()
                .cloned()
                .collect();
                let conflict = !found.has_valid_symbol_and_side(symbol)
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
        }
        for found in uncertain {
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
            return false;
        }
        for id in persisted_ids {
            self.clear_provisional_order(&(account_key, id));
        }
        if identity_conflict {
            self.report_reconciliation_state(true).await;
            self.persist_identity_observations(account_key, symbol)
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
                    self.invalidate_account_and_position_caches().await;
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

    async fn retain_provisional_on_managed_load_error(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        live_ids: &HashMap<String, BinanceOrderResponse>,
        pair_intents: &HashMap<String, GridOrder>,
        remainder_plan: Option<&RemainderPlan>,
    ) {
        self.report_reconciliation_state(true).await;
        for found in live_ids.values() {
            self.track_provisional_observation(found);
        }
        let state = self.state.clone();
        let mut active = state.active_orders.write().await;
        for found in live_ids.values() {
            let key = (account_key, found.client_order_id.clone());
            if let Some(existing) = active.get(&found.client_order_id) {
                self.evidence.provisional_order_ids.insert(key);
                if !existing.matches_exchange_identity(found) {
                    self.track_identity_conflict(existing, found);
                } else {
                    let mut conservative = existing.clone();
                    conservative.reserve_max(found.price, found.orig_qty);
                    active.insert(found.client_order_id.clone(), conservative);
                }
                continue;
            }
            if !found.has_valid_symbol_and_side(symbol) {
                continue;
            }
            if let Some(known) =
                find_durable_order(&found.client_order_id, &[], pair_intents, remainder_plan)
            {
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
    }

    pub(super) async fn verify_live_snapshot_identities(
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
                self.retain_provisional_on_managed_load_error(
                    account_key,
                    symbol,
                    live_ids,
                    pair_intents,
                    remainder_plan,
                )
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
            if !found.has_valid_symbol_and_side(symbol) {
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
}
