use super::*;

fn invalid_snapshot_price(order: &BinanceOrderResponse) -> bool {
    order.price <= Decimal::ZERO
        && !(matches!(
            order.status.as_str(),
            "FILLED" | "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" | "REJECTED"
        ) && order.executed_qty > Decimal::ZERO
            && order.avg_price.is_some_and(|price| price > Decimal::ZERO))
}

pub(super) fn terminal_responses_disagree(
    a: &BinanceOrderResponse,
    b: &BinanceOrderResponse,
) -> bool {
    let terminal = |order: &BinanceOrderResponse| {
        matches!(
            order.status.as_str(),
            "FILLED" | "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" | "REJECTED"
        )
    };
    if a.client_order_id != b.client_order_id
        || a.symbol != b.symbol
        || a.side != b.side
        || a.order_id != b.order_id
    {
        return false;
    }
    // An impossible zero-fill FILLED response is itself unresolved, but it
    // cannot establish a terminal transition that prevents later correction.
    // Never discard earlier positive execution evidence for this exception.
    if (unresolved_filled_quantity(a) && a.executed_qty <= Decimal::ZERO)
        || unresolved_filled_quantity(b)
    {
        return false;
    }
    if !terminal(a) && !terminal(b) {
        if !matches!(a.status.as_str(), "NEW" | "PARTIALLY_FILLED")
            || !matches!(b.status.as_str(), "NEW" | "PARTIALLY_FILLED")
        {
            return false;
        }
        return b.executed_qty < a.executed_qty
            || (a.status == "PARTIALLY_FILLED" && b.status == "NEW")
            || a.update_time
                .zip(b.update_time)
                .is_some_and(|(first, second)| second < first)
            || a.orig_qty != b.orig_qty
            || (a.executed_qty > Decimal::ZERO
                && a.executed_qty == b.executed_qty
                && a.avg_price.is_some_and(|first| {
                    first > Decimal::ZERO && b.avg_price.is_none_or(|second| second != first)
                }));
    }
    if matches!(a.status.as_str(), "NEW" | "PARTIALLY_FILLED") && terminal(b) {
        // A fill can occur between the two queries. Leave a legitimate
        // progression for a later consistent verification rather than
        // persisting it as an irreconcilable terminal contradiction.
        let valid_start = match a.status.as_str() {
            "NEW" => a.executed_qty == Decimal::ZERO,
            "PARTIALLY_FILLED" => a.executed_qty > Decimal::ZERO && a.executed_qty < a.orig_qty,
            _ => false,
        };
        let valid_end = b.executed_qty >= a.executed_qty
            && b.executed_qty <= b.orig_qty
            && (b.status != "FILLED" || b.executed_qty == b.orig_qty);
        let stable_average = a.executed_qty != b.executed_qty
            || a.executed_qty == Decimal::ZERO
            || a.avg_price.is_none_or(|first| {
                first <= Decimal::ZERO || b.avg_price.is_some_and(|second| second == first)
            });
        let ordered_time = a
            .update_time
            .zip(b.update_time)
            .is_none_or(|(first, second)| second >= first);
        return !(valid_start
            && valid_end
            && stable_average
            && a.orig_qty == b.orig_qty
            && ordered_time);
    }
    // Missing price fields can be completed independently; positive values
    // already observed remain authoritative.
    let old_avg = a.avg_price.filter(|price| *price > Decimal::ZERO);
    let new_avg = b.avg_price.filter(|price| *price > Decimal::ZERO);
    let price_completed = a.price <= Decimal::ZERO && b.price > Decimal::ZERO;
    let avg_completed = old_avg.is_none() && new_avg.is_some();
    let price_compatible = a.price == b.price
        || (a.price <= Decimal::ZERO && b.price <= Decimal::ZERO)
        || price_completed;
    let avg_compatible = old_avg == new_avg || avg_completed;
    let completed_evidence = a.status == b.status
        && a.executed_qty > Decimal::ZERO
        && a.executed_qty == b.executed_qty
        && price_compatible
        && avg_compatible
        && (price_completed || avg_completed);
    a.status != b.status
        || a.executed_qty != b.executed_qty
        || a.orig_qty != b.orig_qty
        || !price_compatible
        || !avg_compatible
        || (a.update_time != b.update_time
            && !(completed_evidence
                && match (a.update_time, b.update_time) {
                    (Some(first), Some(second)) => second >= first,
                    (Some(_), None) => false,
                    _ => true,
                }))
}

impl GridTradingEngine {
    pub(super) async fn load_identity_observations(
        &mut self,
        account: [u8; 32],
        symbol: &str,
    ) -> bool {
        let scope = (account, symbol.to_owned());
        if self.loaded_identity_scopes.contains(&scope) {
            return true;
        }
        let symbol_owned = symbol.to_owned();
        let records = match self
            .state
            .db
            .run_blocking(move |db| db.load_identity_observations(account, &symbol_owned))
            .await
        {
            Ok(records) => records,
            Err(error) => {
                error!("Could not load order identity observations: {}", error);
                self.report_reconciliation_state(true).await;
                return false;
            }
        };
        if !records.is_empty() {
            self.report_reconciliation_state(true).await;
            let mut active = self.state.active_orders.write().await;
            for (expected, observed) in records {
                let key = (account, expected.client_order_id.clone());
                self.provisional_order_ids.insert(key.clone());
                self.order_identity_conflicts
                    .insert(key.clone(), expected.clone());
                let observations = self.order_identity_observations.entry(key).or_default();
                if !observations.iter().any(|old| {
                    old.client_order_id == observed.client_order_id
                        && old.symbol == observed.symbol
                        && old.side == observed.side
                        && old.order_id == observed.order_id
                }) {
                    observations.push(observed);
                }
                active
                    .entry(expected.client_order_id.clone())
                    .and_modify(|current| {
                        if current.client_order_id == expected.client_order_id
                            && current.symbol == expected.symbol
                            && current.side == expected.side
                            && current.order_id.is_none()
                        {
                            current.order_id = expected.order_id;
                        }
                    })
                    .or_insert(expected);
            }
        }
        self.loaded_identity_scopes.insert(scope);
        true
    }

    pub(super) async fn persist_identity_observations(
        &mut self,
        account: [u8; 32],
        symbol: &str,
    ) -> bool {
        // A pending snapshot includes both full responses and identities. Its
        // transaction must succeed before any identity-only write can proceed.
        if !self.flush_initial_order_responses(account, symbol).await {
            self.report_reconciliation_state(true).await;
            return false;
        }
        let mut records = Vec::new();
        let mut missing_expected = false;
        for (key, observations) in &self.order_identity_observations {
            if key.0 != account {
                continue;
            }
            let Some(expected) = self.order_identity_conflicts.get(key) else {
                missing_expected = true;
                break;
            };
            if expected.symbol == symbol {
                for observed in observations {
                    records.push((expected.clone(), observed.clone()));
                }
            }
        }
        if missing_expected {
            self.report_reconciliation_state(true).await;
            return false;
        }
        if records.is_empty() {
            return true;
        }
        match self
            .state
            .db
            .run_blocking(move |db| db.save_identity_observations(account, &records))
            .await
        {
            Ok(()) => true,
            Err(error) => {
                error!("Could not persist order identity observations: {}", error);
                self.report_reconciliation_state(true).await;
                false
            }
        }
    }

    async fn retain_invalid_price_snapshot(
        &mut self,
        account: [u8; 32],
        symbol: &str,
        mode: TradingMode,
        orders: &[BinanceOrderResponse],
    ) {
        self.report_reconciliation_state(true).await;
        let managed_symbol = symbol.to_owned();
        let managed = self
            .state
            .db
            .run_blocking(move |db| db.load_managed_orders(&managed_symbol))
            .await
            .unwrap_or_default();
        let intent_symbol = symbol.to_owned();
        let intents = self
            .state
            .db
            .run_blocking(move |db| db.load_pair_intents(&intent_symbol, mode))
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
            let mut expected = managed
                .iter()
                .find(|order| order.client_order_id == found.client_order_id)
                .or_else(|| {
                    intents
                        .iter()
                        .find(|order| order.client_order_id == found.client_order_id)
                })
                .or_else(|| {
                    remainder.as_ref().and_then(|plan| {
                        plan.sources
                            .iter()
                            .chain(std::iter::once(&plan.target))
                            .find(|order| order.client_order_id == found.client_order_id)
                    })
                })
                .or_else(|| active.get(&found.client_order_id))
                .cloned()
                .unwrap_or_else(|| GridOrder {
                    client_order_id: found.client_order_id.clone(),
                    order_id: if found.symbol == symbol
                        && matches!(found.side.as_str(), "BUY" | "SELL")
                        && orders
                            .iter()
                            .filter(|order| order.client_order_id == found.client_order_id)
                            .count()
                            == 1
                    {
                        Some(found.order_id)
                    } else {
                        None
                    },
                    symbol: symbol.to_owned(),
                    side: if found.side == "SELL" {
                        OrderSide::Sell
                    } else {
                        OrderSide::Buy
                    },
                    price: found.price.max(Decimal::ZERO),
                    quantity: found.orig_qty.max(Decimal::ZERO),
                    amount_usdc: found.price.max(Decimal::ZERO) * found.orig_qty.max(Decimal::ZERO),
                    status: OrderStatus::New,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                    grid_level: 0,
                    paired_client_order_id: None,
                    is_take_profit: false,
                    purpose: crate::types::OrderPurpose::Unverified,
                    merge_sources: Vec::new(),
                });
            if let Some(current) = active.get(&found.client_order_id) {
                if current.symbol == expected.symbol
                    && current.side == expected.side
                    && current
                        .order_id
                        .zip(expected.order_id)
                        .is_none_or(|(a, b)| a == b)
                {
                    expected.quantity = expected.quantity.max(current.quantity);
                    expected.price = expected.price.max(current.price);
                    expected.amount_usdc = expected.price * expected.quantity;
                }
            }
            if expected.symbol == found.symbol
                && expected.side.as_str() == found.side
                && expected.order_id.is_none_or(|id| id == found.order_id)
            {
                expected.price = expected.price.max(found.price);
                expected.quantity = expected.quantity.max(found.orig_qty);
                expected.amount_usdc = expected.price * expected.quantity;
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
                    .filter(|order| {
                        order.symbol == stored.symbol
                            && order.side == stored.side
                            && order
                                .order_id
                                .zip(stored.order_id)
                                .is_none_or(|(a, b)| a == b)
                    })
                    .cloned()
            })
            .collect();
        drop(active);
        let mut identities = Vec::new();
        for (key, observations) in &self.order_identity_observations {
            if key.0 == account {
                if let Some(expected) = self.order_identity_conflicts.get(key) {
                    if expected.symbol == symbol {
                        identities.extend(
                            observations
                                .iter()
                                .map(|observed| (expected.clone(), observed.clone())),
                        );
                    }
                }
            }
        }
        let batch = format!("snapshot:{}", uuid::Uuid::new_v4());
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
            if let Err(error) = self
                .state
                .db
                .run_blocking(move |db| db.save_managed_orders(&conservative))
                .await
            {
                error!(
                    "Could not persist conservative invalid-snapshot reservations: {}",
                    error
                );
            }
        }
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
        let mode = TradingMode::from_exchange(config.exchange.dry_run, config.exchange.is_testnet);
        let intent_symbol = symbol.clone();
        let pair_intents = match self
            .state
            .db
            .run_blocking(move |db| db.load_pair_intents(&intent_symbol, mode))
            .await
        {
            Ok(intents) => intents
                .into_iter()
                .map(|intent| (intent.client_order_id.clone(), intent))
                .collect::<HashMap<_, _>>(),
            Err(e) => {
                error!("Could not load pending pair intents: {}", e);
                self.report_reconciliation_state(true).await;
                return false;
            }
        };
        let mut remainder_plan = match self.load_remainder_plan().await {
            Ok(plan) => plan,
            Err(e) => {
                error!("Could not load remainder recovery metadata: {}", e);
                self.last_orders_sync = None;
                return false;
            }
        };
        // A previous failed managed-order read may have left an exchange-only
        // placeholder in memory. Recover durable metadata before any normal or
        // terminal reconciliation can persist or consume that placeholder.
        let account_key = self.client.account_identity_key();
        if !self.load_identity_observations(account_key, &symbol).await
            || !self
                .persist_identity_observations(account_key, &symbol)
                .await
        {
            self.last_orders_sync = None;
            return false;
        }
        if self
            .terminal_disagreement_blocked(account_key, &symbol)
            .await
        {
            self.last_orders_sync = None;
            self.report_reconciliation_state(true).await;
            return false;
        }
        let pending_ids: HashSet<_> = self
            .provisional_order_ids
            .iter()
            .filter(|(account, _)| *account == account_key)
            .map(|(_, id)| id.clone())
            .collect();
        let mut restored_ids = HashSet::new();
        if !pending_ids.is_empty() {
            let managed_symbol = symbol.clone();
            let managed = match self
                .state
                .db
                .run_blocking(move |db| db.load_managed_orders(&managed_symbol))
                .await
            {
                Ok(orders) => orders,
                Err(error) => {
                    error!("Could not restore provisional order metadata: {}", error);
                    self.last_orders_sync = None;
                    self.report_reconciliation_state(true).await;
                    return false;
                }
            };
            let mut conflict = false;
            {
                let mut active = self.state.active_orders.write().await;
                for id in &pending_ids {
                    if let Some(expected) = self
                        .order_identity_conflicts
                        .get(&(account_key, id.clone()))
                        .cloned()
                    {
                        // The conflict may have been recorded while durable
                        // metadata was unavailable; the disk record is authoritative.
                        let mut authoritative = managed
                            .iter()
                            .find(|order| &order.client_order_id == id)
                            .or_else(|| pair_intents.get(id))
                            .or_else(|| {
                                remainder_plan.as_ref().and_then(|plan| {
                                    plan.sources
                                        .iter()
                                        .chain(std::iter::once(&plan.target))
                                        .find(|order| &order.client_order_id == id)
                                })
                            })
                            .cloned()
                            .unwrap_or_else(|| expected.clone());
                        if authoritative.order_id.is_none()
                            && authoritative.symbol == expected.symbol
                            && authoritative.side == expected.side
                        {
                            authoritative.order_id = expected.order_id;
                        }
                        for observed in std::iter::once(&expected).chain(active.get(id)) {
                            if authoritative.symbol == observed.symbol
                                && authoritative.side == observed.side
                                && authoritative
                                    .order_id
                                    .zip(observed.order_id)
                                    .is_none_or(|(a, b)| a == b)
                            {
                                authoritative.price = authoritative.price.max(observed.price);
                                authoritative.quantity =
                                    authoritative.quantity.max(observed.quantity);
                                authoritative.amount_usdc =
                                    authoritative.price * authoritative.quantity;
                            }
                        }
                        if let Some(observed) = active.get(id).and_then(known_order_identity) {
                            if authoritative.symbol != observed.symbol
                                || authoritative.side.as_str() != observed.side
                                || authoritative
                                    .order_id
                                    .is_none_or(|order_id| order_id != observed.order_id)
                            {
                                let items = self
                                    .order_identity_observations
                                    .entry((account_key, id.clone()))
                                    .or_default();
                                if !items.iter().any(|old| {
                                    old.client_order_id == observed.client_order_id
                                        && old.symbol == observed.symbol
                                        && old.side == observed.side
                                        && old.order_id == observed.order_id
                                }) {
                                    items.push(observed);
                                }
                            }
                        }
                        active.insert(id.clone(), authoritative.clone());
                        self.order_identity_conflicts
                            .insert((account_key, id.clone()), authoritative.clone());
                        if expected.symbol != authoritative.symbol
                            || expected.side != authoritative.side
                            || expected
                                .order_id
                                .zip(authoritative.order_id)
                                .is_some_and(|(a, b)| a != b)
                        {
                            if let Some(observed) = known_order_identity(&expected) {
                                let items = self
                                    .order_identity_observations
                                    .entry((account_key, id.clone()))
                                    .or_default();
                                if !items.iter().any(|old| {
                                    old.client_order_id == observed.client_order_id
                                        && old.symbol == observed.symbol
                                        && old.side == observed.side
                                        && old.order_id == observed.order_id
                                }) {
                                    items.push(observed);
                                }
                            }
                        }
                        if let Some(observed) = self
                            .provisional_order_observations
                            .remove(&(account_key, id.clone()))
                        {
                            let items = self
                                .order_identity_observations
                                .entry((account_key, id.clone()))
                                .or_default();
                            for item in observed {
                                if identity_differs_from_order(&item, &authoritative)
                                    && !items.iter().any(|old| {
                                        old.client_order_id == item.client_order_id
                                            && old.symbol == item.symbol
                                            && old.side == item.side
                                            && old.order_id == item.order_id
                                    })
                                {
                                    items.push(item);
                                }
                            }
                        }
                        conflict = true;
                        continue;
                    }
                    let Some(current) = active.get(id).cloned() else {
                        self.provisional_order_ids
                            .remove(&(account_key, id.clone()));
                        self.provisional_order_observations
                            .remove(&(account_key, id.clone()));
                        continue;
                    };
                    let Some(durable) = managed
                        .iter()
                        .find(|order| &order.client_order_id == id)
                        .or_else(|| pair_intents.get(id))
                        .or_else(|| {
                            remainder_plan.as_ref().and_then(|plan| {
                                plan.sources
                                    .iter()
                                    .chain(std::iter::once(&plan.target))
                                    .find(|order| &order.client_order_id == id)
                            })
                        })
                    else {
                        continue;
                    };
                    if durable.symbol != current.symbol
                        || durable.side != current.side
                        || durable
                            .order_id
                            .zip(current.order_id)
                            .is_some_and(|(a, b)| a != b)
                    {
                        active.insert(id.clone(), durable.clone());
                        self.order_identity_conflicts
                            .insert((account_key, id.clone()), durable.clone());
                        if let Some(observed) = known_order_identity(&current) {
                            let items = self
                                .order_identity_observations
                                .entry((account_key, id.clone()))
                                .or_default();
                            if !items.iter().any(|old| {
                                old.client_order_id == observed.client_order_id
                                    && old.symbol == observed.symbol
                                    && old.side == observed.side
                                    && old.order_id == observed.order_id
                            }) {
                                items.push(observed);
                            }
                        }
                        if let Some(observed) = self
                            .provisional_order_observations
                            .remove(&(account_key, id.clone()))
                        {
                            let items = self
                                .order_identity_observations
                                .entry((account_key, id.clone()))
                                .or_default();
                            for item in observed {
                                if identity_differs_from_order(&item, durable)
                                    && !items.iter().any(|old| {
                                        old.client_order_id == item.client_order_id
                                            && old.symbol == item.symbol
                                            && old.side == item.side
                                            && old.order_id == item.order_id
                                    })
                                {
                                    items.push(item);
                                }
                            }
                        }
                        conflict = true;
                    } else {
                        let mut restored = durable.clone();
                        restored.order_id = durable.order_id.or(current.order_id);
                        restored.quantity = durable.quantity.max(current.quantity);
                        restored.amount_usdc = restored.price * restored.quantity;
                        active.insert(id.clone(), restored);
                    }
                    restored_ids.insert(id.clone());
                    if !self
                        .order_identity_conflicts
                        .contains_key(&(account_key, id.clone()))
                    {
                        self.provisional_order_ids
                            .remove(&(account_key, id.clone()));
                        self.provisional_order_observations
                            .remove(&(account_key, id.clone()));
                    }
                }
            }
            if conflict {
                self.last_orders_sync = None;
                self.report_reconciliation_state(true).await;
                if !self
                    .persist_identity_observations(account_key, &symbol)
                    .await
                {
                    return false;
                }
                // Still fetch the exchange snapshot below; only a matching
                // identity can resolve the retained conflict.
            }
        }
        let open_orders = match priority {
            ReconciliationPriority::Periodic => {
                self.client.get_open_orders_for_periodic_sync(&symbol).await
            }
            ReconciliationPriority::Critical => self.client.get_open_orders(&symbol).await,
        };
        match open_orders {
            Ok(live_orders) => {
                let mut live_orders = live_orders;
                self.last_orders_sync = None;
                let invalid_price = live_orders.iter().any(|order| {
                    is_grid_order(&order.client_order_id) && invalid_snapshot_price(order)
                });
                let known_risk = invalid_price
                    || live_orders.iter().any(|order| {
                        is_grid_order(&order.client_order_id)
                            && (order.executed_qty > Decimal::ZERO
                                || unresolved_filled_quantity(order))
                    })
                    || !pair_intents.is_empty()
                    || remainder_plan
                        .as_ref()
                        .is_some_and(|plan| plan.phase == RemainderPhase::Submitting)
                    || !self.order_identity_conflicts.is_empty();
                let mut fill_guard = known_risk.then(|| self.state.publish_fill_reconciliation());
                if fill_guard.is_none() {
                    let active = match self.state.active_orders.try_read() {
                        Ok(active) => active,
                        Err(_) => {
                            fill_guard = Some(self.state.publish_fill_reconciliation());
                            self.state.active_orders.read().await
                        }
                    };
                    let missing = active
                        .keys()
                        .any(|id| !live_orders.iter().any(|order| &order.client_order_id == id));
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
                if invalid_price {
                    self.retain_invalid_price_snapshot(account_key, &symbol, mode, &live_orders)
                        .await;
                    return false;
                }
                if live_orders.iter().any(|order| {
                    is_grid_order(&order.client_order_id) && unresolved_filled_quantity(order)
                }) {
                    self.set_global_reconciliation_ready(false).await;
                }
                // Inspect duplicate client IDs before building a map: collecting
                // into a map would discard an identity based on response order.
                let mut groups: HashMap<&str, Vec<&BinanceOrderResponse>> = HashMap::new();
                for found in &live_orders {
                    if is_grid_order(&found.client_order_id) {
                        groups
                            .entry(&found.client_order_id)
                            .or_default()
                            .push(found);
                    }
                }
                let managed_symbol = symbol.clone();
                let managed = self
                    .state
                    .db
                    .run_blocking(move |db| db.load_managed_orders(&managed_symbol))
                    .await;
                if let Err(error) = &managed {
                    error!("Could not verify snapshot order identities: {}", error);
                }
                let state = self.state.clone();
                let active = state.active_orders.read().await;
                let mut duplicates: Vec<_> = groups
                    .values()
                    .filter(|orders| {
                        let first = orders[0];
                        let known = [
                            managed.as_ref().ok().and_then(|items| {
                                items
                                    .iter()
                                    .find(|order| order.client_order_id == first.client_order_id)
                            }),
                            pair_intents.get(&first.client_order_id),
                            remainder_plan.as_ref().and_then(|plan| {
                                plan.sources
                                    .iter()
                                    .chain(std::iter::once(&plan.target))
                                    .find(|order| order.client_order_id == first.client_order_id)
                            }),
                            active.get(&first.client_order_id),
                        ];
                        managed.is_err()
                            || self
                                .order_identity_conflicts
                                .contains_key(&(account_key, first.client_order_id.clone()))
                            || known.into_iter().flatten().any(|expected| {
                                orders.iter().any(|found| {
                                    expected.symbol != found.symbol
                                        || (expected.purpose
                                            != crate::types::OrderPurpose::Unverified
                                            && expected.side.as_str() != found.side)
                                        || expected.order_id.is_some_and(|id| id != found.order_id)
                                })
                            })
                            || orders.iter().any(|order| {
                                order.symbol != symbol
                                    || !matches!(order.side.as_str(), "BUY" | "SELL")
                            })
                            || orders.iter().skip(1).any(|order| {
                                order.symbol != first.symbol
                                    || order.side != first.side
                                    || order.order_id != first.order_id
                                    || order.status != first.status
                                    || order.price != first.price
                                    || order.avg_price != first.avg_price
                                    || order.orig_qty != first.orig_qty
                                    || order.executed_qty != first.executed_qty
                                    || order.order_type != first.order_type
                                    || order.time_in_force != first.time_in_force
                                    || order.update_time != first.update_time
                            })
                    })
                    .cloned()
                    .collect();
                let mut restore_orders = Vec::new();
                let mut active_to_persist = Vec::new();
                let mut active_identity_observations = Vec::new();
                for current in active.values() {
                    let durable = managed
                        .as_ref()
                        .ok()
                        .and_then(|items| {
                            items
                                .iter()
                                .find(|item| item.client_order_id == current.client_order_id)
                        })
                        .or_else(|| pair_intents.get(&current.client_order_id))
                        .or_else(|| {
                            remainder_plan.as_ref().and_then(|plan| {
                                plan.sources
                                    .iter()
                                    .chain(std::iter::once(&plan.target))
                                    .find(|item| item.client_order_id == current.client_order_id)
                            })
                        });
                    if let (Some(expected), Some(observed)) =
                        (durable, known_order_identity(current))
                    {
                        if expected.symbol != current.symbol
                            || expected.side != current.side
                            || expected
                                .order_id
                                .is_none_or(|id| Some(id) != current.order_id)
                        {
                            if expected.order_id.is_none()
                                && expected.symbol == current.symbol
                                && expected.side == current.side
                                && restored_ids.contains(&current.client_order_id)
                                && live_orders.iter().any(|found| {
                                    found.client_order_id == current.client_order_id
                                        && found.symbol == current.symbol
                                        && found.side == current.side.as_str()
                                        && Some(found.order_id) == current.order_id
                                        && !unresolved_filled_quantity(found)
                                })
                            {
                                continue;
                            }
                            let mut retained = expected.clone();
                            if retained.order_id.is_none()
                                && retained.symbol == current.symbol
                                && retained.side == current.side
                                && current.purpose != crate::types::OrderPurpose::Unverified
                                && !restored_ids.contains(&current.client_order_id)
                                && !self
                                    .provisional_order_ids
                                    .contains(&(account_key, current.client_order_id.clone()))
                            {
                                retained.order_id = current.order_id;
                            }
                            active_identity_observations.push((retained, observed));
                        }
                    }
                }
                if !duplicates.is_empty() || !active_identity_observations.is_empty() {
                    // A conflict blocks normal reconciliation for the entire
                    // snapshot. Retain unrelated newly discovered orders too,
                    // since they may fill before the next snapshot or restart.
                    for orders in groups.values() {
                        let id = &orders[0].client_order_id;
                        let durable = managed
                            .as_ref()
                            .ok()
                            .and_then(|items| {
                                items.iter().find(|order| &order.client_order_id == id)
                            })
                            .or_else(|| pair_intents.get(id))
                            .or_else(|| {
                                remainder_plan.as_ref().and_then(|plan| {
                                    plan.sources
                                        .iter()
                                        .chain(std::iter::once(&plan.target))
                                        .find(|order| &order.client_order_id == id)
                                })
                            });
                        if !active.contains_key(id) {
                            if let Some(order) = durable {
                                restore_orders.push(order.clone());
                            }
                        }
                        if durable.is_none_or(|order| order.order_id.is_none())
                            && !duplicates
                                .iter()
                                .any(|group| &group[0].client_order_id == id)
                        {
                            // An in-memory order can be left by a failed write;
                            // persist its identity rather than treating it as durable.
                            duplicates.push(orders.clone());
                        }
                    }
                    for order in active.values() {
                        if order.symbol != symbol
                            || order.purpose == crate::types::OrderPurpose::Unverified
                            || managed.as_ref().ok().is_some_and(|items| {
                                items
                                    .iter()
                                    .any(|item| item.client_order_id == order.client_order_id)
                            })
                            || pair_intents.contains_key(&order.client_order_id)
                            || remainder_plan.as_ref().is_some_and(|plan| {
                                plan.sources
                                    .iter()
                                    .chain(std::iter::once(&plan.target))
                                    .any(|item| item.client_order_id == order.client_order_id)
                            })
                        {
                            continue;
                        }
                        active_to_persist.push(order.clone());
                    }
                }
                drop(active);
                if !duplicates.is_empty() || !active_identity_observations.is_empty() {
                    let block = managed.is_err()
                        || active_identity_observations
                            .iter()
                            .any(|(expected, observed)| {
                                if restored_ids.contains(&expected.client_order_id)
                                    && expected.order_id.is_none()
                                    && expected.client_order_id == observed.client_order_id
                                    && expected.symbol == observed.symbol
                                    && expected.side.as_str() == observed.side
                                {
                                    return false;
                                }
                                let key = (account_key, expected.client_order_id.clone());
                                self.order_identity_observations
                                    .get(&key)
                                    .is_none_or(|items| {
                                        !items.iter().any(|old| {
                                            old.client_order_id == observed.client_order_id
                                                && old.symbol == observed.symbol
                                                && old.side == observed.side
                                                && old.order_id == observed.order_id
                                        })
                                    })
                            })
                        || duplicates.iter().any(|orders| {
                            let key = (account_key, orders[0].client_order_id.clone());
                            if orders.len() == 1
                                && self
                                    .order_identity_conflicts
                                    .get(&key)
                                    .is_some_and(|expected| {
                                        let found = orders[0];
                                        expected.client_order_id == found.client_order_id
                                            && expected.symbol == found.symbol
                                            && expected.side.as_str() == found.side
                                            && expected.order_id == Some(found.order_id)
                                    })
                            {
                                return false;
                            }
                            !self.order_identity_conflicts.contains_key(&key)
                                || orders.iter().any(|found| {
                                    self.order_identity_observations
                                        .get(&key)
                                        .is_none_or(|items| {
                                            !items.iter().any(|old| {
                                                old.client_order_id == found.client_order_id
                                                    && old.symbol == found.symbol
                                                    && old.side == found.side
                                                    && old.order_id == found.order_id
                                            })
                                        })
                                })
                        });
                    self.report_reconciliation_state(true).await;
                    if managed.is_err() {
                        self.last_orders_sync = None;
                    }
                    let state = self.state.clone();
                    let mut active = state.active_orders.write().await;
                    let mut snapshot_responses = Vec::new();
                    for order in restore_orders {
                        active.entry(order.client_order_id.clone()).or_insert(order);
                    }
                    for (mut expected, observed) in active_identity_observations {
                        let key = (account_key, expected.client_order_id.clone());
                        self.provisional_order_ids.insert(key.clone());
                        if let Some(confirmed) = self.order_identity_conflicts.get(&key) {
                            if expected.order_id.is_none()
                                && confirmed.order_id.is_some()
                                && expected.client_order_id == confirmed.client_order_id
                                && expected.symbol == confirmed.symbol
                                && expected.side == confirmed.side
                            {
                                expected.order_id = confirmed.order_id;
                            }
                        }
                        self.order_identity_conflicts.insert(key.clone(), expected);
                        let items = self.order_identity_observations.entry(key).or_default();
                        if !items.iter().any(|old| {
                            old.client_order_id == observed.client_order_id
                                && old.symbol == observed.symbol
                                && old.side == observed.side
                                && old.order_id == observed.order_id
                        }) {
                            items.push(observed);
                        }
                    }
                    duplicates.sort_by(|a, b| a[0].client_order_id.cmp(&b[0].client_order_id));
                    for mut observations in duplicates {
                        observations.sort_by(|a, b| {
                            (&a.symbol, &a.side, a.order_id).cmp(&(&b.symbol, &b.side, b.order_id))
                        });
                        let first = observations[0];
                        let mut expected = managed
                            .as_ref()
                            .ok()
                            .and_then(|orders| {
                                orders
                                    .iter()
                                    .find(|order| order.client_order_id == first.client_order_id)
                            })
                            .or_else(|| pair_intents.get(&first.client_order_id))
                            .or_else(|| {
                                remainder_plan.as_ref().and_then(|plan| {
                                    plan.sources
                                        .iter()
                                        .chain(std::iter::once(&plan.target))
                                        .find(|order| {
                                            order.client_order_id == first.client_order_id
                                        })
                                })
                            })
                            .or_else(|| active.get(&first.client_order_id))
                            .cloned()
                            .unwrap_or_else(|| {
                                let quantity = observations
                                    .iter()
                                    .map(|order| order.orig_qty)
                                    .max()
                                    .unwrap_or(Decimal::ZERO);
                                let price = observations
                                    .iter()
                                    .map(|order| order.price)
                                    .max()
                                    .unwrap_or(Decimal::ZERO);
                                GridOrder {
                                    client_order_id: first.client_order_id.clone(),
                                    order_id: None,
                                    symbol: symbol.clone(),
                                    side: if first.side == "SELL" {
                                        OrderSide::Sell
                                    } else {
                                        OrderSide::Buy
                                    },
                                    price,
                                    quantity,
                                    amount_usdc: price * quantity,
                                    status: OrderStatus::New,
                                    created_at: Utc::now(),
                                    updated_at: Utc::now(),
                                    grid_level: 0,
                                    paired_client_order_id: None,
                                    is_take_profit: false,
                                    purpose: crate::types::OrderPurpose::Unverified,
                                    merge_sources: Vec::new(),
                                }
                            });
                        if let Some(confirmed) = self
                            .order_identity_conflicts
                            .get(&(account_key, expected.client_order_id.clone()))
                        {
                            if expected.order_id.is_none()
                                && confirmed.order_id.is_some()
                                && expected.client_order_id == confirmed.client_order_id
                                && expected.symbol == confirmed.symbol
                                && expected.side == confirmed.side
                            {
                                expected.order_id = confirmed.order_id;
                            }
                        }
                        if expected.purpose == crate::types::OrderPurpose::Unverified {
                            for found in &observations {
                                expected.price = expected.price.max(found.price);
                                expected.quantity = expected.quantity.max(found.orig_qty);
                            }
                            expected.amount_usdc = expected.price * expected.quantity;
                        }
                        for found in observations {
                            self.track_identity_conflict(&expected, found);
                            snapshot_responses
                                .push((expected.client_order_id.clone(), found.clone()));
                        }
                        active.insert(expected.client_order_id.clone(), expected);
                    }
                    drop(active);
                    let snapshot_batch = format!("snapshot:{}", uuid::Uuid::new_v4());
                    let mut snapshot_identities = Vec::new();
                    let mut identities_complete = true;
                    for (key, observations) in &self.order_identity_observations {
                        if key.0 != account_key {
                            continue;
                        }
                        let Some(expected) = self.order_identity_conflicts.get(key) else {
                            identities_complete = false;
                            break;
                        };
                        if expected.symbol == symbol {
                            snapshot_identities.extend(
                                observations
                                    .iter()
                                    .map(|observed| (expected.clone(), observed.clone())),
                            );
                        }
                    }
                    let saved_snapshots = if identities_complete {
                        self.record_initial_order_response_snapshot(
                            account_key,
                            &symbol,
                            &snapshot_batch,
                            &snapshot_responses,
                            &snapshot_identities,
                        )
                        .await
                    } else {
                        false
                    };
                    if !saved_snapshots {
                        self.report_reconciliation_state(true).await;
                        return false;
                    }
                    let saved_active = if active_to_persist.is_empty() {
                        true
                    } else {
                        match self
                            .state
                            .db
                            .run_blocking(move |db| {
                                db.save_managed_orders_if_absent(&active_to_persist)
                            })
                            .await
                        {
                            Ok(()) => true,
                            Err(error) => {
                                error!(
                                    "Could not persist active orders before reconciliation: {}",
                                    error
                                );
                                false
                            }
                        }
                    };
                    let saved_observations = self
                        .persist_identity_observations(account_key, &symbol)
                        .await;
                    if !saved_active || !saved_observations || !saved_snapshots || block {
                        return false;
                    }
                }
                let conflicts: Vec<_> = self
                    .order_identity_conflicts
                    .iter()
                    .filter(|((account, _), order)| {
                        *account == account_key && order.symbol == symbol
                    })
                    .map(|(key, order)| (key.clone(), order.clone()))
                    .collect();
                let mut corrected_terminal = HashMap::new();
                let mut resolved_conflict_keys = Vec::new();
                let mut discovered = false;
                for (_, expected) in &conflicts {
                    for found in &live_orders {
                        if found.client_order_id == expected.client_order_id
                            && (found.symbol != expected.symbol
                                || found.side != expected.side.as_str()
                                || expected.order_id.is_none_or(|id| found.order_id != id))
                        {
                            self.track_identity_conflict(expected, found);
                            discovered = true;
                        }
                    }
                }
                if discovered
                    && !self
                        .persist_identity_observations(account_key, &symbol)
                        .await
                {
                    return false;
                }
                for (key, mut expected) in conflicts {
                    let timeline = match self
                        .load_initial_order_response_timeline(
                            account_key,
                            &symbol,
                            &expected.client_order_id,
                        )
                        .await
                    {
                        Some(responses) => responses,
                        None => {
                            self.report_reconciliation_state(true).await;
                            return false;
                        }
                    };
                    let mut prior_disagreement = false;
                    for (index, (batch, earlier)) in timeline.iter().enumerate() {
                        for (later_batch, later) in timeline.iter().skip(index + 1) {
                            if batch != later_batch
                                && !later_batch.starts_with("snapshot:")
                                && terminal_responses_disagree(earlier, later)
                            {
                                self.record_terminal_disagreement(
                                    account_key,
                                    &symbol,
                                    &expected.client_order_id,
                                    earlier,
                                    later,
                                )
                                .await;
                                prior_disagreement = true;
                            }
                        }
                    }
                    if prior_disagreement {
                        self.report_reconciliation_state(true).await;
                        return false;
                    }
                    let initial_responses: Vec<_> =
                        timeline.into_iter().map(|(_, response)| response).collect();
                    let mut corrected_expected: Option<BinanceOrderResponse> = None;
                    let mut ambiguous_candidate = false;
                    let mut harmless_candidate: Option<BinanceOrderResponse> = None;
                    let mut ambiguous_harmless = false;
                    let mut harmless_identities = HashSet::new();
                    let mut queried_orders: HashMap<(String, String, i64), BinanceOrderResponse> =
                        HashMap::new();
                    let observations = self
                        .order_identity_observations
                        .get(&key)
                        .cloned()
                        .unwrap_or_default();
                    for observed in observations {
                        let verified = match self
                            .client
                            .get_order(&observed.symbol, observed.order_id)
                            .await
                        {
                            Ok(found) => {
                                if !self
                                    .record_initial_order_response(
                                        account_key,
                                        &symbol,
                                        &expected.client_order_id,
                                        &found,
                                    )
                                    .await
                                {
                                    self.report_reconciliation_state(true).await;
                                    return false;
                                }
                                let mut historical_disagreement = false;
                                for initial in &initial_responses {
                                    if terminal_responses_disagree(initial, &found) {
                                        self.record_terminal_disagreement(
                                            account_key,
                                            &symbol,
                                            &expected.client_order_id,
                                            initial,
                                            &found,
                                        )
                                        .await;
                                        historical_disagreement = true;
                                    }
                                }
                                if historical_disagreement {
                                    self.report_reconciliation_state(true).await;
                                    return false;
                                }
                                let query_key = (
                                    found.client_order_id.clone(),
                                    found.symbol.clone(),
                                    found.order_id,
                                );
                                if let Some(previous) = queried_orders.get(&query_key) {
                                    if previous.side != found.side {
                                        self.track_identity_conflict(&expected, previous);
                                        self.track_identity_conflict(&expected, &found);
                                        self.persist_identity_observations(account_key, &symbol)
                                            .await;
                                        self.report_reconciliation_state(true).await;
                                        return false;
                                    }
                                    if terminal_responses_disagree(previous, &found) {
                                        self.record_terminal_disagreement(
                                            account_key,
                                            &symbol,
                                            &expected.client_order_id,
                                            previous,
                                            &found,
                                        )
                                        .await;
                                        self.report_reconciliation_state(true).await;
                                        return false;
                                    }
                                    if previous.status != found.status
                                        || previous.executed_qty != found.executed_qty
                                        || previous.orig_qty != found.orig_qty
                                        || previous.price != found.price
                                        || previous.avg_price != found.avg_price
                                        || previous.update_time != found.update_time
                                    {
                                        self.report_reconciliation_state(true).await;
                                        return false;
                                    }
                                } else {
                                    queried_orders.insert(query_key, found.clone());
                                }
                                let same_order = found.order_id == observed.order_id
                                    && found.symbol == observed.symbol
                                    && found.client_order_id == observed.client_order_id;
                                let harmless_terminal = same_order
                                    && found.executed_qty == Decimal::ZERO
                                    && matches!(
                                        found.status.as_str(),
                                        "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" | "REJECTED"
                                    );
                                if harmless_terminal {
                                    harmless_identities.insert((
                                        found.client_order_id.clone(),
                                        found.symbol.clone(),
                                        found.order_id,
                                    ));
                                }
                                let corrected_to_expected = same_order
                                    && found.client_order_id == expected.client_order_id
                                    && found.symbol == expected.symbol
                                    && matches!(found.side.as_str(), "BUY" | "SELL")
                                    && (expected.purpose == crate::types::OrderPurpose::Unverified
                                        || found.side == expected.side.as_str())
                                    && expected.order_id.is_none_or(|id| id == found.order_id)
                                    && !unresolved_filled_quantity(&found)
                                    && matches!(
                                        found.status.as_str(),
                                        "NEW"
                                            | "PARTIALLY_FILLED"
                                            | "FILLED"
                                            | "CANCELED"
                                            | "EXPIRED"
                                            | "EXPIRED_IN_MATCH"
                                            | "REJECTED"
                                    );
                                if corrected_to_expected
                                    && harmless_terminal
                                    && expected.purpose == crate::types::OrderPurpose::Unverified
                                {
                                    if harmless_candidate
                                        .as_ref()
                                        .is_some_and(|old| old.order_id != found.order_id)
                                    {
                                        ambiguous_harmless = true;
                                    } else {
                                        harmless_candidate = Some(found.clone());
                                    }
                                } else if corrected_to_expected
                                    && (expected.order_id.is_some() || !harmless_terminal)
                                {
                                    if corrected_expected
                                        .as_ref()
                                        .is_some_and(|old| old.order_id != found.order_id)
                                    {
                                        ambiguous_candidate = true;
                                    } else {
                                        corrected_expected = Some(found.clone());
                                    }
                                }
                                corrected_to_expected || harmless_terminal
                            }
                            Err(_) => false,
                        };
                        if !verified {
                            self.report_reconciliation_state(true).await;
                            return false;
                        }
                    }
                    if corrected_expected.is_none()
                        && expected.purpose == crate::types::OrderPurpose::Unverified
                    {
                        if ambiguous_harmless {
                            self.report_reconciliation_state(true).await;
                            return false;
                        }
                        corrected_expected = harmless_candidate;
                    }
                    if ambiguous_candidate {
                        self.report_reconciliation_state(true).await;
                        return false;
                    }
                    if expected.order_id.is_none()
                        || expected.purpose == crate::types::OrderPurpose::Unverified
                        || corrected_expected.is_some()
                    {
                        if let Some(candidate) = &corrected_expected {
                            let unique = self
                                .client
                                .lookup_order_by_client_id(
                                    &expected.symbol,
                                    &expected.client_order_id,
                                )
                                .await;
                            if let Ok(Some(found)) = &unique {
                                if !self
                                    .record_initial_order_response(
                                        account_key,
                                        &symbol,
                                        &expected.client_order_id,
                                        found,
                                    )
                                    .await
                                {
                                    self.report_reconciliation_state(true).await;
                                    return false;
                                }
                            }
                            let unique = match unique {
                                Ok(Some(found))
                                    if found.client_order_id == expected.client_order_id
                                        && found.symbol == expected.symbol
                                        && found.side == candidate.side
                                        && (expected.purpose
                                            == crate::types::OrderPurpose::Unverified
                                            || found.side == expected.side.as_str())
                                        && found.order_id == candidate.order_id
                                        && found.status == candidate.status
                                        && found.executed_qty == candidate.executed_qty
                                        && found.orig_qty == candidate.orig_qty
                                        && found.price == candidate.price
                                        && found.avg_price == candidate.avg_price
                                        && found.update_time == candidate.update_time
                                        && !unresolved_filled_quantity(&found)
                                        && matches!(
                                            found.status.as_str(),
                                            "NEW"
                                                | "PARTIALLY_FILLED"
                                                | "FILLED"
                                                | "CANCELED"
                                                | "EXPIRED"
                                                | "EXPIRED_IN_MATCH"
                                                | "REJECTED"
                                        ) =>
                                {
                                    found
                                }
                                Ok(Some(found)) => {
                                    if terminal_responses_disagree(candidate, &found) {
                                        self.record_terminal_disagreement(
                                            account_key,
                                            &symbol,
                                            &expected.client_order_id,
                                            candidate,
                                            &found,
                                        )
                                        .await;
                                    }
                                    self.track_identity_conflict(&expected, &found);
                                    self.persist_identity_observations(account_key, &symbol)
                                        .await;
                                    self.report_reconciliation_state(true).await;
                                    return false;
                                }
                                _ => {
                                    self.report_reconciliation_state(true).await;
                                    return false;
                                }
                            };
                            let compatible = self
                                .state
                                .active_orders
                                .read()
                                .await
                                .get(&expected.client_order_id)
                                .is_none_or(|order| {
                                    order.order_id.is_none_or(|id| id == unique.order_id)
                                });
                            if !compatible {
                                self.report_reconciliation_state(true).await;
                                return false;
                            }
                            if expected.purpose == crate::types::OrderPurpose::Unverified {
                                expected.side = if unique.side == "SELL" {
                                    OrderSide::Sell
                                } else {
                                    OrderSide::Buy
                                };
                                expected.price = unique.price;
                                expected.amount_usdc = expected.price * expected.quantity;
                                expected.purpose = crate::types::OrderPurpose::Legacy;
                            }
                            expected.order_id = Some(unique.order_id);
                            self.order_identity_conflicts
                                .insert(key.clone(), expected.clone());
                            if !self
                                .persist_identity_observations(account_key, &symbol)
                                .await
                            {
                                return false;
                            }
                            if let Some(order) = self
                                .state
                                .active_orders
                                .write()
                                .await
                                .get_mut(&expected.client_order_id)
                            {
                                order.order_id = expected.order_id;
                                order.side = expected.side;
                                order.price = expected.price;
                                order.amount_usdc = expected.amount_usdc;
                                order.purpose = expected.purpose;
                            }
                            corrected_expected = Some(unique);
                        }
                    }
                    if expected.purpose == crate::types::OrderPurpose::Unverified {
                        self.report_reconciliation_state(true).await;
                        return false;
                    }
                    live_orders.retain(|order| {
                        !harmless_identities.contains(&(
                            order.client_order_id.clone(),
                            order.symbol.clone(),
                            order.order_id,
                        ))
                    });
                    if let Some(corrected) = corrected_expected {
                        // A single-order query is authoritative for this same
                        // exchange ID even if the list still has the old side.
                        live_orders.retain(|order| {
                            !(order.client_order_id == corrected.client_order_id
                                && order.symbol == corrected.symbol
                                && order.order_id == corrected.order_id)
                        });
                        if matches!(
                            corrected.status.as_str(),
                            "FILLED" | "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" | "REJECTED"
                        ) {
                            corrected_terminal.insert(corrected.client_order_id.clone(), corrected);
                        } else {
                            live_orders.push(corrected);
                        }
                    }
                    let (seen, matching) = {
                        let mut seen = false;
                        let mut consistent = true;
                        for order in &live_orders {
                            if order.client_order_id == expected.client_order_id {
                                seen = true;
                                if order.symbol != expected.symbol
                                    || order.side != expected.side.as_str()
                                    || expected.order_id.is_some_and(|id| order.order_id != id)
                                {
                                    consistent = false;
                                }
                            }
                        }
                        (seen, consistent)
                    };
                    if !matching {
                        self.report_reconciliation_state(true).await;
                        return false;
                    }
                    if !seen && !corrected_terminal.contains_key(&expected.client_order_id) {
                        let result = match expected.order_id {
                            Some(id) => self.client.get_order(&expected.symbol, id).await.map(Some),
                            None => {
                                self.client
                                    .lookup_order_by_client_id(
                                        &expected.symbol,
                                        &expected.client_order_id,
                                    )
                                    .await
                            }
                        };
                        match result {
                            Ok(Some(found)) => {
                                let unknown_id = expected.order_id.is_none();
                                let identity_matches = found.client_order_id
                                    == expected.client_order_id
                                    && found.symbol == expected.symbol
                                    && found.side == expected.side.as_str()
                                    && expected.order_id.is_none_or(|id| found.order_id == id);
                                if unknown_id || !identity_matches {
                                    self.track_identity_conflict(&expected, &found);
                                    if !self
                                        .persist_identity_observations(account_key, &symbol)
                                        .await
                                    {
                                        return false;
                                    }
                                }
                                {
                                    if !self
                                        .record_initial_order_response(
                                            account_key,
                                            &symbol,
                                            &expected.client_order_id,
                                            &found,
                                        )
                                        .await
                                    {
                                        self.report_reconciliation_state(true).await;
                                        return false;
                                    }
                                    let mut historical_disagreement = false;
                                    for initial in &initial_responses {
                                        if terminal_responses_disagree(initial, &found) {
                                            self.record_terminal_disagreement(
                                                account_key,
                                                &symbol,
                                                &expected.client_order_id,
                                                initial,
                                                &found,
                                            )
                                            .await;
                                            historical_disagreement = true;
                                        }
                                    }
                                    if historical_disagreement {
                                        self.report_reconciliation_state(true).await;
                                        return false;
                                    }
                                }
                                if !identity_matches
                                    || unresolved_filled_quantity(&found)
                                    || (!unknown_id
                                        && !matches!(
                                            found.status.as_str(),
                                            "FILLED"
                                                | "CANCELED"
                                                | "EXPIRED"
                                                | "EXPIRED_IN_MATCH"
                                                | "REJECTED"
                                        ))
                                {
                                    self.report_reconciliation_state(true).await;
                                    return false;
                                }
                                let found = if unknown_id {
                                    let verified = match self
                                        .client
                                        .get_order(&found.symbol, found.order_id)
                                        .await
                                    {
                                        Ok(verified) => verified,
                                        Err(_) => {
                                            self.report_reconciliation_state(true).await;
                                            return false;
                                        }
                                    };
                                    if !self
                                        .record_initial_order_response(
                                            account_key,
                                            &symbol,
                                            &expected.client_order_id,
                                            &verified,
                                        )
                                        .await
                                    {
                                        self.report_reconciliation_state(true).await;
                                        return false;
                                    }
                                    if terminal_responses_disagree(&found, &verified) {
                                        self.record_terminal_disagreement(
                                            account_key,
                                            &symbol,
                                            &expected.client_order_id,
                                            &found,
                                            &verified,
                                        )
                                        .await;
                                        self.report_reconciliation_state(true).await;
                                        return false;
                                    }
                                    if verified.client_order_id != found.client_order_id
                                        || verified.symbol != found.symbol
                                        || verified.side != found.side
                                        || verified.order_id != found.order_id
                                        || verified.status != found.status
                                        || verified.executed_qty != found.executed_qty
                                        || verified.orig_qty != found.orig_qty
                                        || verified.price != found.price
                                        || verified.avg_price != found.avg_price
                                        || verified.update_time != found.update_time
                                        || unresolved_filled_quantity(&verified)
                                        || !matches!(
                                            verified.status.as_str(),
                                            "FILLED"
                                                | "CANCELED"
                                                | "EXPIRED"
                                                | "EXPIRED_IN_MATCH"
                                                | "REJECTED"
                                        )
                                    {
                                        self.track_identity_conflict(&expected, &verified);
                                        self.persist_identity_observations(account_key, &symbol)
                                            .await;
                                        self.report_reconciliation_state(true).await;
                                        return false;
                                    }
                                    let unique = match self
                                        .client
                                        .lookup_order_by_client_id(
                                            &expected.symbol,
                                            &expected.client_order_id,
                                        )
                                        .await
                                    {
                                        Ok(Some(unique)) => unique,
                                        _ => {
                                            self.report_reconciliation_state(true).await;
                                            return false;
                                        }
                                    };
                                    if !self
                                        .record_initial_order_response(
                                            account_key,
                                            &symbol,
                                            &expected.client_order_id,
                                            &unique,
                                        )
                                        .await
                                    {
                                        self.report_reconciliation_state(true).await;
                                        return false;
                                    }
                                    if terminal_responses_disagree(&verified, &unique) {
                                        self.record_terminal_disagreement(
                                            account_key,
                                            &symbol,
                                            &expected.client_order_id,
                                            &verified,
                                            &unique,
                                        )
                                        .await;
                                        self.report_reconciliation_state(true).await;
                                        return false;
                                    }
                                    if unique.client_order_id != verified.client_order_id
                                        || unique.symbol != verified.symbol
                                        || unique.side != verified.side
                                        || unique.order_id != verified.order_id
                                        || unresolved_filled_quantity(&unique)
                                        || !matches!(
                                            unique.status.as_str(),
                                            "FILLED"
                                                | "CANCELED"
                                                | "EXPIRED"
                                                | "EXPIRED_IN_MATCH"
                                                | "REJECTED"
                                        )
                                        || unique.status != verified.status
                                        || unique.executed_qty != verified.executed_qty
                                        || unique.orig_qty != verified.orig_qty
                                        || unique.price != verified.price
                                        || unique.avg_price != verified.avg_price
                                        || unique.update_time != verified.update_time
                                    {
                                        self.track_identity_conflict(&expected, &unique);
                                        self.persist_identity_observations(account_key, &symbol)
                                            .await;
                                        self.report_reconciliation_state(true).await;
                                        return false;
                                    }
                                    let compatible = self
                                        .state
                                        .active_orders
                                        .read()
                                        .await
                                        .get(&expected.client_order_id)
                                        .is_none_or(|order| {
                                            order.symbol == expected.symbol
                                                && order.side == expected.side
                                                && order
                                                    .order_id
                                                    .is_none_or(|id| id == verified.order_id)
                                        });
                                    if !compatible {
                                        self.report_reconciliation_state(true).await;
                                        return false;
                                    }
                                    expected.order_id = Some(verified.order_id);
                                    self.order_identity_conflicts
                                        .insert(key.clone(), expected.clone());
                                    if !self
                                        .persist_identity_observations(account_key, &symbol)
                                        .await
                                    {
                                        return false;
                                    }
                                    if let Some(order) = self
                                        .state
                                        .active_orders
                                        .write()
                                        .await
                                        .get_mut(&expected.client_order_id)
                                    {
                                        order.order_id = expected.order_id;
                                    }
                                    verified
                                } else {
                                    found
                                };
                                corrected_terminal.insert(found.client_order_id.clone(), found);
                            }
                            _ => {
                                self.report_reconciliation_state(true).await;
                                return false;
                            }
                        }
                    }
                    resolved_conflict_keys.push(key);
                    restored_ids.insert(expected.client_order_id);
                }
                let mut live_ids: HashMap<String, BinanceOrderResponse> = live_orders
                    .into_iter()
                    .filter(|order| is_grid_order(&order.client_order_id))
                    .map(|o| (o.client_order_id.clone(), o))
                    .collect();
                let uncertain: Vec<_> = live_ids
                    .values()
                    .filter(|order| unresolved_filled_quantity(order))
                    .cloned()
                    .collect();
                let uncertain_ids: HashSet<_> = uncertain
                    .iter()
                    .map(|order| order.client_order_id.clone())
                    .collect();
                if !uncertain.is_empty() {
                    // A prior correction cannot prove that the exchange order
                    // has not changed since. Block before each verification.
                    self.set_global_reconciliation_ready(false).await;
                    self.account_cache.invalidate(&self.client).await;
                    self.position_cache.invalidate(&self.client).await;
                    self.last_account_sync = None;
                    let account_key = self.client.account_identity_key();
                    for found in &uncertain {
                        self.track_provisional_observation(found);
                    }
                    // Register newly discovered identities before touching the
                    // database, so cancellation can still find them on DB failure.
                    let mut provisional_ids: HashSet<_> = uncertain
                        .iter()
                        .filter(|order| {
                            self.provisional_order_ids
                                .contains(&(account_key, order.client_order_id.clone()))
                        })
                        .map(|order| order.client_order_id.clone())
                        .collect();
                    {
                        let mut active = self.state.active_orders.write().await;
                        for found in &uncertain {
                            if let Some(existing) = active.get(&found.client_order_id) {
                                let key = (account_key, found.client_order_id.clone());
                                self.provisional_order_ids.insert(key.clone());
                                if existing.symbol != found.symbol
                                    || existing.side.as_str() != found.side
                                    || existing.order_id.is_some_and(|id| id != found.order_id)
                                {
                                    self.order_identity_conflicts
                                        .insert(key.clone(), existing.clone());
                                    let observations =
                                        self.order_identity_observations.entry(key).or_default();
                                    if !observations.iter().any(|old| {
                                        old.client_order_id == found.client_order_id
                                            && old.symbol == found.symbol
                                            && old.side == found.side
                                            && old.order_id == found.order_id
                                    }) {
                                        observations.push(ObservedOrderIdentity {
                                            client_order_id: found.client_order_id.clone(),
                                            symbol: found.symbol.clone(),
                                            side: found.side.clone(),
                                            order_id: found.order_id,
                                        });
                                    }
                                }
                                continue;
                            }
                            let intent = pair_intents.get(&found.client_order_id);
                            let remainder = remainder_plan.as_ref().and_then(|plan| {
                                plan.sources
                                    .iter()
                                    .chain(std::iter::once(&plan.target))
                                    .find(|order| order.client_order_id == found.client_order_id)
                            });
                            if [intent, remainder].into_iter().flatten().any(|known| {
                                known.symbol != found.symbol
                                    || known.side.as_str() != found.side
                                    || known.order_id.is_some_and(|id| id != found.order_id)
                            }) {
                                if let Some(known) = intent.or(remainder) {
                                    let key = (account_key, found.client_order_id.clone());
                                    self.provisional_order_ids.insert(key.clone());
                                    self.order_identity_conflicts
                                        .insert(key.clone(), known.clone());
                                    let observations =
                                        self.order_identity_observations.entry(key).or_default();
                                    if !observations.iter().any(|old| {
                                        old.client_order_id == found.client_order_id
                                            && old.symbol == found.symbol
                                            && old.side == found.side
                                            && old.order_id == found.order_id
                                    }) {
                                        observations.push(ObservedOrderIdentity {
                                            client_order_id: found.client_order_id.clone(),
                                            symbol: found.symbol.clone(),
                                            side: found.side.clone(),
                                            order_id: found.order_id,
                                        });
                                    }
                                    active.insert(known.client_order_id.clone(), known.clone());
                                }
                                continue;
                            }
                            if found.symbol != symbol
                                || !matches!(found.side.as_str(), "BUY" | "SELL")
                            {
                                continue;
                            }
                            let mut order =
                                intent.or(remainder).cloned().unwrap_or_else(|| GridOrder {
                                    client_order_id: found.client_order_id.clone(),
                                    order_id: Some(found.order_id),
                                    symbol: found.symbol.clone(),
                                    side: if found.side == "BUY" {
                                        OrderSide::Buy
                                    } else {
                                        OrderSide::Sell
                                    },
                                    price: found.price,
                                    quantity: found.orig_qty,
                                    amount_usdc: found.price * found.orig_qty,
                                    status: OrderStatus::New,
                                    created_at: Utc::now(),
                                    updated_at: Utc::now(),
                                    grid_level: 0,
                                    paired_client_order_id: None,
                                    is_take_profit: false,
                                    purpose: crate::types::OrderPurpose::Legacy,
                                    merge_sources: Vec::new(),
                                });
                            order.order_id = Some(found.order_id);
                            order.quantity = order.quantity.max(found.orig_qty);
                            order.amount_usdc = order.price * order.quantity;
                            provisional_ids.insert(order.client_order_id.clone());
                            self.provisional_order_ids
                                .insert((account_key, order.client_order_id.clone()));
                            active.insert(order.client_order_id.clone(), order);
                        }
                    }
                    if !self
                        .persist_identity_observations(account_key, &symbol)
                        .await
                    {
                        return false;
                    }
                    let managed_symbol = symbol.clone();
                    let managed = match self
                        .state
                        .db
                        .run_blocking(move |db| db.load_managed_orders(&managed_symbol))
                        .await
                    {
                        Ok(orders) => orders,
                        Err(error) => {
                            error!(
                                "Could not load managed orders for uncertain fill: {}",
                                error
                            );
                            self.report_reconciliation_state(true).await;
                            return false;
                        }
                    };
                    let mut identity_conflict = false;
                    let mut persisted_ids = Vec::new();
                    let mut to_persist = Vec::new();
                    {
                        let mut active = self.state.active_orders.write().await;
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
                            if let Some(order) = remainder_plan.as_ref().and_then(|plan| {
                                plan.sources
                                    .iter()
                                    .chain(std::iter::once(&plan.target))
                                    .find(|order| order.client_order_id == found.client_order_id)
                            }) {
                                candidates.push(order.clone());
                            }
                            let conflict = found.symbol != symbol
                                || !matches!(found.side.as_str(), "BUY" | "SELL")
                                || candidates.iter().any(|order| {
                                    order.symbol != found.symbol
                                        || order.side.as_str() != found.side
                                        || order.order_id.is_some_and(|id| id != found.order_id)
                                });
                            if conflict {
                                identity_conflict = true;
                                if let Some(expected) = candidates.first() {
                                    let key = (account_key, found.client_order_id.clone());
                                    self.order_identity_conflicts
                                        .insert(key.clone(), expected.clone());
                                    self.provisional_order_ids.insert(key.clone());
                                    let items =
                                        self.order_identity_observations.entry(key).or_default();
                                    if !items.iter().any(|old| {
                                        old.client_order_id == found.client_order_id
                                            && old.symbol == found.symbol
                                            && old.side == found.side
                                            && old.order_id == found.order_id
                                    }) {
                                        items.push(ObservedOrderIdentity {
                                            client_order_id: found.client_order_id.clone(),
                                            symbol: found.symbol.clone(),
                                            side: found.side.clone(),
                                            order_id: found.order_id,
                                        });
                                    }
                                }
                                if provisional_ids.contains(&found.client_order_id) {
                                    if let Some(original) = managed.iter().find(|order| {
                                        order.client_order_id == found.client_order_id
                                    }) {
                                        active.insert(
                                            original.client_order_id.clone(),
                                            original.clone(),
                                        );
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
                            let mut order = metadata.unwrap_or_else(|| GridOrder {
                                client_order_id: found.client_order_id.clone(),
                                order_id: Some(found.order_id),
                                symbol: found.symbol.clone(),
                                side: if found.side == "BUY" {
                                    OrderSide::Buy
                                } else {
                                    OrderSide::Sell
                                },
                                price: found.price,
                                quantity: found.orig_qty,
                                amount_usdc: found.price * found.orig_qty,
                                status: OrderStatus::New,
                                created_at: Utc::now(),
                                updated_at: Utc::now(),
                                grid_level: 0,
                                paired_client_order_id: None,
                                is_take_profit: false,
                                purpose: crate::types::OrderPurpose::Legacy,
                                merge_sources: Vec::new(),
                            });
                            order.order_id = Some(found.order_id);
                            order.quantity = order.quantity.max(found.orig_qty);
                            order.amount_usdc = order.price * order.quantity;
                            active.insert(order.client_order_id.clone(), order.clone());
                            persisted_ids.push(order.client_order_id.clone());
                            to_persist.push(order);
                        }
                    }
                    for found in &uncertain {
                        let key = (account_key, found.client_order_id.clone());
                        if !self.order_identity_conflicts.contains_key(&key)
                            && managed
                                .iter()
                                .any(|order| order.client_order_id == found.client_order_id)
                        {
                            self.provisional_order_ids.remove(&key);
                        }
                    }
                    if let Err(error) = self
                        .state
                        .db
                        .run_blocking(move |db| db.save_managed_orders(&to_persist))
                        .await
                    {
                        error!(
                            "Could not persist orders with unknown fill quantities: {}",
                            error
                        );
                        self.report_reconciliation_state(true).await;
                        return false;
                    }
                    for id in persisted_ids {
                        let key = (account_key, id);
                        self.provisional_order_ids.remove(&key);
                        self.provisional_order_observations.remove(&key);
                    }
                    if identity_conflict {
                        self.report_reconciliation_state(true).await;
                        self.persist_identity_observations(account_key, &symbol)
                            .await;
                        return false;
                    }
                    for original in uncertain {
                        match self
                            .client
                            .get_order(&original.symbol, original.order_id)
                            .await
                        {
                            Ok(corrected)
                                if corrected.client_order_id == original.client_order_id
                                    && corrected.symbol == original.symbol
                                    && corrected.side == original.side
                                    && corrected.order_id == original.order_id
                                    && !unresolved_filled_quantity(&corrected)
                                    && matches!(
                                        corrected.status.as_str(),
                                        "NEW"
                                            | "PARTIALLY_FILLED"
                                            | "FILLED"
                                            | "CANCELED"
                                            | "EXPIRED"
                                            | "EXPIRED_IN_MATCH"
                                            | "REJECTED"
                                    ) =>
                            {
                                if matches!(
                                    corrected.status.as_str(),
                                    "FILLED"
                                        | "CANCELED"
                                        | "EXPIRED"
                                        | "EXPIRED_IN_MATCH"
                                        | "REJECTED"
                                ) {
                                    corrected_terminal.insert(
                                        corrected.client_order_id.clone(),
                                        corrected.clone(),
                                    );
                                }
                                live_ids.insert(corrected.client_order_id.clone(), corrected);
                            }
                            _ => {
                                self.account_cache.invalidate(&self.client).await;
                                self.position_cache.invalidate(&self.client).await;
                                self.last_account_sync = None;
                                self.set_global_reconciliation_ready(false).await;
                            }
                        }
                    }
                }
                let inconsistent_fill = live_ids.values().any(unresolved_filled_quantity);
                if live_ids.values().any(invalid_snapshot_price) {
                    let snapshot: Vec<_> = live_ids.values().cloned().collect();
                    self.retain_invalid_price_snapshot(account_key, &symbol, mode, &snapshot)
                        .await;
                    return false;
                }

                // Every live snapshot is checked against durable identities;
                // in-memory conflict barriers do not survive a process restart.
                if !live_ids.is_empty() {
                    let managed_symbol = symbol.clone();
                    let managed = match self
                        .state
                        .db
                        .run_blocking(move |db| db.load_managed_orders(&managed_symbol))
                        .await
                    {
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
                                    self.provisional_order_ids.insert(key.clone());
                                    if existing.symbol != found.symbol
                                        || existing.side.as_str() != found.side
                                        || existing.order_id.is_some_and(|id| id != found.order_id)
                                    {
                                        self.track_identity_conflict(existing, found);
                                    } else {
                                        let mut conservative = existing.clone();
                                        conservative.price = conservative.price.max(found.price);
                                        conservative.quantity =
                                            conservative.quantity.max(found.orig_qty);
                                        conservative.amount_usdc =
                                            conservative.price * conservative.quantity;
                                        active.insert(found.client_order_id.clone(), conservative);
                                    }
                                    continue;
                                }
                                if found.symbol != symbol
                                    || !matches!(found.side.as_str(), "BUY" | "SELL")
                                {
                                    continue;
                                }
                                let intent = pair_intents.get(&found.client_order_id);
                                let remainder = remainder_plan.as_ref().and_then(|plan| {
                                    plan.sources
                                        .iter()
                                        .chain(std::iter::once(&plan.target))
                                        .find(|order| {
                                            order.client_order_id == found.client_order_id
                                        })
                                });
                                if let Some(known) = intent.or(remainder) {
                                    let mut order = known.clone();
                                    if known.symbol != found.symbol
                                        || known.side.as_str() != found.side
                                        || known.order_id.is_some_and(|id| id != found.order_id)
                                    {
                                        self.track_identity_conflict(known, found);
                                    } else {
                                        order.order_id = Some(found.order_id);
                                        order.price = order.price.max(found.price);
                                        order.quantity = order.quantity.max(found.orig_qty);
                                        order.amount_usdc = order.price * order.quantity;
                                    }
                                    self.provisional_order_ids.insert(key);
                                    active.insert(order.client_order_id.clone(), order);
                                    continue;
                                }
                                let order = GridOrder {
                                    client_order_id: found.client_order_id.clone(),
                                    order_id: Some(found.order_id),
                                    symbol: found.symbol.clone(),
                                    side: if found.side == "BUY" {
                                        OrderSide::Buy
                                    } else {
                                        OrderSide::Sell
                                    },
                                    price: found.price,
                                    quantity: found.orig_qty,
                                    amount_usdc: found.price * found.orig_qty,
                                    status: OrderStatus::New,
                                    created_at: Utc::now(),
                                    updated_at: Utc::now(),
                                    grid_level: 0,
                                    paired_client_order_id: None,
                                    is_take_profit: false,
                                    purpose: crate::types::OrderPurpose::Legacy,
                                    merge_sources: Vec::new(),
                                };
                                self.provisional_order_ids
                                    .insert((account_key, order.client_order_id.clone()));
                                active.insert(order.client_order_id.clone(), order);
                            }
                            drop(active);
                            self.persist_identity_observations(account_key, &symbol)
                                .await;
                            return false;
                        }
                    };
                    let mut conflicts = Vec::new();
                    let mut invalid_live_identity = false;
                    let state = self.state.clone();
                    let active = state.active_orders.read().await;
                    for found in live_ids.values() {
                        if found.symbol != symbol || !matches!(found.side.as_str(), "BUY" | "SELL")
                        {
                            invalid_live_identity = true;
                            self.track_provisional_observation(found);
                        }
                        let candidates = [
                            managed
                                .iter()
                                .find(|order| order.client_order_id == found.client_order_id),
                            active.get(&found.client_order_id),
                            pair_intents.get(&found.client_order_id),
                            remainder_plan.as_ref().and_then(|plan| {
                                plan.sources
                                    .iter()
                                    .chain(std::iter::once(&plan.target))
                                    .find(|order| order.client_order_id == found.client_order_id)
                            }),
                        ];
                        if let Some(expected) = candidates.into_iter().flatten().find(|order| {
                            order.client_order_id != found.client_order_id
                                || order.symbol != found.symbol
                                || order.side.as_str() != found.side
                                || order.order_id.is_some_and(|id| id != found.order_id)
                        }) {
                            conflicts.push((expected.clone(), found.clone()));
                        }
                    }
                    drop(active);
                    if invalid_live_identity && conflicts.is_empty() {
                        self.report_reconciliation_state(true).await;
                        return false;
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
                        self.persist_identity_observations(account_key, &symbol)
                            .await;
                        return false;
                    }
                    let mut active = self.state.active_orders.write().await;
                    for found in live_ids.values() {
                        if let Some(durable) = managed
                            .iter()
                            .find(|order| order.client_order_id == found.client_order_id)
                            .or_else(|| pair_intents.get(&found.client_order_id))
                            .or_else(|| {
                                remainder_plan.as_ref().and_then(|plan| {
                                    plan.sources
                                        .iter()
                                        .chain(std::iter::once(&plan.target))
                                        .find(|order| {
                                            order.client_order_id == found.client_order_id
                                        })
                                })
                            })
                        {
                            let mut restored = durable.clone();
                            if let Some(current) = active.get(&found.client_order_id) {
                                if current.symbol == durable.symbol
                                    && current.side == durable.side
                                    && current
                                        .order_id
                                        .zip(durable.order_id)
                                        .is_none_or(|(a, b)| a == b)
                                {
                                    restored.order_id = durable.order_id.or(current.order_id);
                                    restored.price = restored.price.max(current.price);
                                    restored.quantity = durable.quantity.max(current.quantity);
                                    restored.amount_usdc = restored.price * restored.quantity;
                                }
                            }
                            restored.price = restored.price.max(found.price);
                            restored.amount_usdc = restored.price * restored.quantity;
                            active.insert(restored.client_order_id.clone(), restored);
                            restored_ids.insert(found.client_order_id.clone());
                        }
                    }
                }

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
                                            > (order.orig_qty - order.executed_qty)
                                                .max(Decimal::ZERO)
                                }))
                    })
                };
                if needs_position_refresh || inconsistent_fill {
                    self.account_cache.invalidate(&self.client).await;
                    self.position_cache.invalidate(&self.client).await;
                    if inconsistent_fill {
                        self.last_account_sync = None;
                    }
                }
                let position_ready = if inconsistent_fill || self.state.pause_epoch() != pause_epoch
                {
                    false
                } else if needs_position_refresh {
                    let shared = matches!(priority, ReconciliationPriority::Periodic);
                    let position = self.sync_position(shared).await;
                    let account = if position.is_some() {
                        self.sync_account(shared, !shared).await
                    } else {
                        None
                    };
                    self.last_account_sync = position.zip(account).map(|(p, a)| p.min(a));
                    self.last_account_sync.is_some()
                } else {
                    true
                };

                let mut filled_or_closed = Vec::new();
                let mut persistence_ok = true;
                let mut orders_to_persist = Vec::new();

                {
                    let mut active = self.state.active_orders.write().await;
                    for (client_id, live_order) in &live_ids {
                        // On failure or interruption, retain at least the previous
                        // reservation; newly discovered orders reserve original size.
                        let remaining =
                            (live_order.orig_qty - live_order.executed_qty).max(Decimal::ZERO);
                        let quantity = if position_ready && self.state.pause_epoch() == pause_epoch
                        {
                            remaining
                        } else {
                            active
                                .get(client_id)
                                .map_or(live_order.orig_qty, |order| order.quantity.max(remaining))
                        };
                        if !uncertain_ids.contains(client_id) && !restored_ids.contains(client_id) {
                            if let Some(plan) = &remainder_plan {
                                if let Some(metadata) = plan
                                    .sources
                                    .iter()
                                    .chain(std::iter::once(&plan.target))
                                    .find(|order| &order.client_order_id == client_id)
                                {
                                    active.insert(client_id.clone(), metadata.clone());
                                }
                            }
                            if let Some(intent) = pair_intents.get(client_id) {
                                active.insert(client_id.clone(), intent.clone());
                            }
                        }
                        if let Some(order) = active.get_mut(client_id) {
                            order.order_id = Some(live_order.order_id);
                            order.price =
                                if position_ready && self.state.pause_epoch() == pause_epoch {
                                    live_order.price
                                } else {
                                    order.price.max(live_order.price)
                                };
                            order.quantity = quantity;
                            order.amount_usdc = order.price * order.quantity;
                            order.status = if live_order.executed_qty > Decimal::ZERO {
                                OrderStatus::PartiallyFilled
                            } else {
                                OrderStatus::New
                            };
                            orders_to_persist.push(order.clone());
                        } else {
                            let order = GridOrder {
                                client_order_id: client_id.clone(),
                                order_id: Some(live_order.order_id),
                                symbol: live_order.symbol.clone(),
                                side: if live_order.side == "BUY" {
                                    OrderSide::Buy
                                } else {
                                    OrderSide::Sell
                                },
                                price: live_order.price,
                                quantity,
                                amount_usdc: live_order.price * quantity,
                                status: if live_order.executed_qty > Decimal::ZERO {
                                    OrderStatus::PartiallyFilled
                                } else {
                                    OrderStatus::New
                                },
                                created_at: Utc::now(),
                                updated_at: Utc::now(),
                                grid_level: 0,
                                paired_client_order_id: None,
                                is_take_profit: false,
                                purpose: crate::types::OrderPurpose::Legacy,
                                merge_sources: Vec::new(),
                            };
                            orders_to_persist.push(order.clone());
                            active.insert(client_id.clone(), order);
                        }
                    }
                }

                if !orders_to_persist.is_empty() {
                    if let Err(e) = self
                        .state
                        .db
                        .run_blocking(move |db| db.save_managed_orders(&orders_to_persist))
                        .await
                    {
                        error!("Could not persist reconciled orders: {}", e);
                        self.report_reconciliation_state(true).await;
                        return false;
                    }
                }
                if !position_ready {
                    self.report_reconciliation_state(true).await;
                    return false;
                }
                if needs_position_refresh {
                    if let Some(global) = self
                        .state
                        .global_position_limit
                        .get()
                        .and_then(std::sync::Weak::upgrade)
                    {
                        global.check().await;
                    }
                }
                if self.state.pause_epoch() != pause_epoch {
                    return false;
                }
                for id in corrected_terminal.keys() {
                    live_ids.remove(id);
                }

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
                for mut order in filled_or_closed {
                    if self.state.pause_epoch() != pause_epoch {
                        return false;
                    }
                    let exchange_result = if let Some(corrected) =
                        corrected_terminal.remove(&order.client_order_id)
                    {
                        Ok(Some(corrected))
                    } else {
                        match order.order_id {
                            Some(order_id) => self
                                .client
                                .get_order(&order.symbol, order_id)
                                .await
                                .map(Some),
                            None => {
                                self.client
                                    .lookup_order_by_client_id(
                                        &order.symbol,
                                        &order.client_order_id,
                                    )
                                    .await
                            }
                        }
                    };
                    let inconsistent = exchange_result
                        .as_ref()
                        .is_ok_and(|found| found.as_ref().is_some_and(unresolved_filled_quantity));
                    if inconsistent {
                        self.report_reconciliation_state(true).await;
                        self.account_cache.invalidate(&self.client).await;
                        self.position_cache.invalidate(&self.client).await;
                        self.last_account_sync = None;
                    }
                    if self.state.pause_epoch() != pause_epoch {
                        return false;
                    }
                    match exchange_result {
                        Ok(None)
                            if (Utc::now() - order.created_at).num_seconds()
                                >= UNKNOWN_SUBMISSION_GRACE_SECS =>
                        {
                            if let Err(error) =
                                self.delete_managed_order(&order.client_order_id).await
                            {
                                error!(
                                    "Could not retire absent order intent {}: {}",
                                    order.client_order_id, error
                                );
                                persistence_ok = false;
                            } else {
                                self.state
                                    .active_orders
                                    .write()
                                    .await
                                    .remove(&order.client_order_id);
                                self.state.add_log("WARN", format!("Unaccepted order intent {} retired after confirmed absence", order.client_order_id)).await;
                            }
                        }
                        Ok(None) => unresolved_orders = true,
                        Ok(Some(exchange_order)) => {
                            if exchange_order.client_order_id != order.client_order_id
                                || exchange_order.symbol != order.symbol
                                || exchange_order.side != order.side.as_str()
                                || order
                                    .order_id
                                    .is_some_and(|id| id != exchange_order.order_id)
                            {
                                self.track_identity_conflict(&order, &exchange_order);
                                self.report_reconciliation_state(true).await;
                                self.persist_identity_observations(account_key, &symbol)
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
                            let terminal = matches!(
                                exchange_order.status.as_str(),
                                "FILLED" | "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" | "REJECTED"
                            );
                            if !terminal {
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
                    return false;
                }
                self.last_orders_sync = Some(Instant::now());
                terminal_orders
                    .sort_by_key(|(_, response)| (response.update_time, response.order_id));
                for (mut order, exchange_order) in terminal_orders {
                    if exchange_order.executed_qty > Decimal::ZERO {
                        order.updated_at = exchange_fill_time(&exchange_order);
                        order.quantity = exchange_order.executed_qty;
                        if let Some(avg_price) = exchange_order
                            .avg_price
                            .filter(|price| *price > Decimal::ZERO)
                        {
                            order.price = avg_price;
                        } else {
                            order.price = exchange_order.price;
                        }
                        order.amount_usdc = order.price * order.quantity;
                        if !self.on_order_filled(&mut order).await {
                            persistence_ok = false;
                        }
                    } else {
                        if let Err(e) = self.delete_managed_order(&order.client_order_id).await {
                            error!(
                                "Could not delete closed order {}: {}",
                                order.client_order_id, e
                            );
                            persistence_ok = false;
                        } else {
                            self.state
                                .active_orders
                                .write()
                                .await
                                .remove(&order.client_order_id);
                        }
                        debug!(
                            "Order {} closed without a fill ({})",
                            order.client_order_id, exchange_order.status
                        );
                    }
                }
                if persistence_ok {
                    if let Some(plan) = &mut remainder_plan {
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
                } else if !(if fill_guard.is_some() {
                    self.drain_pair_intents_under_gate(&symbol, mode).await
                } else {
                    self.drain_pair_intents(&symbol, mode).await
                }) {
                    persistence_ok = false;
                    self.last_orders_sync = None;
                }
                if self.state.pause_epoch() != pause_epoch {
                    self.last_orders_sync = None;
                    self.report_reconciliation_state(true).await;
                    return false;
                }
                if persistence_ok && self.state.pause_epoch() == pause_epoch {
                    let client_ids: Vec<_> = resolved_conflict_keys
                        .iter()
                        .map(|key| key.1.clone())
                        .collect();
                    if !self
                        .clear_order_evidence_groups(account_key, &symbol, &client_ids)
                        .await
                    {
                        self.last_orders_sync = None;
                        persistence_ok = false;
                    }
                }
                if persistence_ok {
                    if let Some(guard) = &mut fill_guard {
                        guard.resolve();
                    }
                }
                drop(fill_guard);
                self.report_reconciliation_state(!persistence_ok).await;
                persistence_ok
            }
            Err(e) => {
                warn!("Error during live orders sync: {}", e);
                self.last_orders_sync = None;
                self.report_reconciliation_state(true).await;
                false
            }
        }
    }
}
