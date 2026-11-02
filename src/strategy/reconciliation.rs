use super::*;

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
                if !observations
                    .iter()
                    .any(|old| old.symbol == observed.symbol && old.order_id == observed.order_id)
                {
                    observations.push(observed);
                }
                active
                    .entry(expected.client_order_id.clone())
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
                                    old.symbol == observed.symbol
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
                                        old.symbol == item.symbol && old.order_id == item.order_id
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
                                old.symbol == observed.symbol && old.order_id == observed.order_id
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
                                        old.symbol == item.symbol && old.order_id == item.order_id
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
                    let mut corrected_expected: Option<BinanceOrderResponse> = None;
                    let mut ambiguous_candidate = false;
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
                                let same_order = found.order_id == observed.order_id
                                    && found.symbol == observed.symbol
                                    && found.client_order_id == observed.client_order_id;
                                let harmless_terminal = same_order
                                    && found.executed_qty == Decimal::ZERO
                                    && matches!(
                                        found.status.as_str(),
                                        "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" | "REJECTED"
                                    );
                                let corrected_to_expected = same_order
                                    && found.client_order_id == expected.client_order_id
                                    && found.symbol == expected.symbol
                                    && found.side == expected.side.as_str()
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
                    if ambiguous_candidate {
                        self.report_reconciliation_state(true).await;
                        return false;
                    }
                    if expected.order_id.is_none() {
                        if let Some(candidate) = &corrected_expected {
                            let unique = self
                                .client
                                .lookup_order_by_client_id(
                                    &expected.symbol,
                                    &expected.client_order_id,
                                )
                                .await;
                            let unique = match unique {
                                Ok(Some(found))
                                    if found.client_order_id == expected.client_order_id
                                        && found.symbol == expected.symbol
                                        && found.side == expected.side.as_str()
                                        && found.order_id == candidate.order_id
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
                            }
                            corrected_expected = Some(unique);
                        }
                    }
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
                            Ok(Some(found))
                                if found.client_order_id == expected.client_order_id
                                    && found.symbol == expected.symbol
                                    && found.side == expected.side.as_str()
                                    && expected.order_id.is_none_or(|id| found.order_id == id)
                                    && !unresolved_filled_quantity(&found)
                                    && matches!(
                                        found.status.as_str(),
                                        "FILLED"
                                            | "CANCELED"
                                            | "EXPIRED"
                                            | "EXPIRED_IN_MATCH"
                                            | "REJECTED"
                                    ) =>
                            {
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
                                        old.symbol == found.symbol && old.order_id == found.order_id
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
                                        old.symbol == found.symbol && old.order_id == found.order_id
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
                                        old.symbol == found.symbol && old.order_id == found.order_id
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
                            if durable.symbol != found.symbol
                                || durable.side.as_str() != found.side
                                || durable.order_id.is_some_and(|id| id != found.order_id)
                            {
                                conflicts.push((durable.clone(), found.clone()));
                            }
                        }
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
                                    restored.quantity = durable.quantity.max(current.quantity);
                                    restored.amount_usdc = restored.price * restored.quantity;
                                }
                            }
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
                        persistence_ok = false;
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
                            if let Err(e) = self.adopt_remainder_target(plan).await {
                                warn!("Remainder submission remains unresolved: {}", e);
                                persistence_ok = false;
                            }
                        }
                    }
                }
                if !persistence_ok {
                    self.last_orders_sync = None;
                } else if !self.drain_pair_intents(&symbol, mode).await {
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
                        .filter(|key| self.order_identity_observations.contains_key(*key))
                        .map(|key| key.1.clone())
                        .collect();
                    let cleared = if client_ids.is_empty() {
                        Ok(())
                    } else {
                        let scope = symbol.clone();
                        self.state
                            .db
                            .run_blocking(move |db| {
                                db.delete_identity_observation_groups(
                                    account_key,
                                    &scope,
                                    &client_ids,
                                )
                            })
                            .await
                    };
                    if let Err(error) = cleared {
                        error!("Could not clear resolved identity observations: {}", error);
                        self.last_orders_sync = None;
                        persistence_ok = false;
                    } else {
                        for key in resolved_conflict_keys {
                            self.order_identity_observations.remove(&key);
                            self.order_identity_conflicts.remove(&key);
                            self.provisional_order_ids.remove(&key);
                        }
                    }
                }
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
