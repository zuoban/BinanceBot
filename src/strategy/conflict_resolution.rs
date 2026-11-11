use super::reconciliation::find_durable_order;
use super::*;

type ConflictKey = ([u8; 32], String);
type ConflictEntry = (ConflictKey, GridOrder);
type ResolvedConflicts = (HashMap<String, BinanceOrderResponse>, Vec<ConflictKey>);

pub(super) struct SnapshotConflictScan<'a> {
    pub(super) account_key: [u8; 32],
    pub(super) symbol: &'a str,
    pub(super) live_orders: &'a [BinanceOrderResponse],
    pub(super) pair_intents: &'a HashMap<String, GridOrder>,
    pub(super) remainder_plan: Option<&'a RemainderPlan>,
    pub(super) restored_ids: &'a HashSet<String>,
}

struct DetectedSnapshotConflicts<'a> {
    duplicates: Vec<Vec<&'a BinanceOrderResponse>>,
    restore_orders: Vec<GridOrder>,
    active_to_persist: Vec<GridOrder>,
    active_identity_observations: Vec<(GridOrder, ObservedOrderIdentity)>,
}

struct VerifiedConflictObservations {
    corrected_expected: Option<BinanceOrderResponse>,
    harmless_identities: HashSet<(String, String, i64)>,
}

pub(super) fn terminal_responses_disagree(
    a: &BinanceOrderResponse,
    b: &BinanceOrderResponse,
) -> bool {
    if !a.matches_identity(b) {
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
    if !a.is_terminal() && !b.is_terminal() {
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
    if matches!(a.status.as_str(), "NEW" | "PARTIALLY_FILLED") && b.is_terminal() {
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
        if self.evidence.loaded_identity_scopes.contains(&scope) {
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
            let state = self.state.clone();
            let mut active = state.active_orders.write().await;
            for (expected, observed) in records {
                self.record_identity_conflict_observation(account, &expected, observed);
                active
                    .entry(expected.client_order_id.clone())
                    .and_modify(|current| current.adopt_compatible_order_id(&expected))
                    .or_insert(expected);
            }
        }
        self.evidence.loaded_identity_scopes.insert(scope);
        true
    }

    pub(super) fn collect_identity_observations(
        &self,
        account: [u8; 32],
        symbol: &str,
    ) -> Option<Vec<(GridOrder, ObservedOrderIdentity)>> {
        let mut records = Vec::new();
        for (key, observations) in &self.evidence.order_identity_observations {
            if key.0 != account {
                continue;
            }
            let expected = self.evidence.order_identity_conflicts.get(key)?;
            if expected.symbol == symbol {
                records.extend(
                    observations
                        .iter()
                        .map(|observed| (expected.clone(), observed.clone())),
                );
            }
        }
        Some(records)
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
        let Some(records) = self.collect_identity_observations(account, symbol) else {
            self.report_reconciliation_state(true).await;
            return false;
        };
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

    async fn record_and_check_initial_timeline(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        client_order_id: &str,
        found: &BinanceOrderResponse,
        initial_responses: &[BinanceOrderResponse],
    ) -> bool {
        if !self
            .record_initial_order_response(account_key, symbol, client_order_id, found)
            .await
        {
            self.report_reconciliation_state(true).await;
            return false;
        }
        let mut historical_disagreement = false;
        for initial in initial_responses {
            if terminal_responses_disagree(initial, found) {
                self.record_terminal_disagreement(
                    account_key,
                    symbol,
                    client_order_id,
                    initial,
                    found,
                )
                .await;
                historical_disagreement = true;
            }
        }
        if historical_disagreement {
            self.report_reconciliation_state(true).await;
            return false;
        }
        true
    }

    async fn verify_consistent_terminal_followup(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        expected: &GridOrder,
        previous: &BinanceOrderResponse,
        current: &BinanceOrderResponse,
    ) -> bool {
        if !self
            .record_initial_order_response(account_key, symbol, &expected.client_order_id, current)
            .await
        {
            self.report_reconciliation_state(true).await;
            return false;
        }
        if terminal_responses_disagree(previous, current) {
            self.record_terminal_disagreement(
                account_key,
                symbol,
                &expected.client_order_id,
                previous,
                current,
            )
            .await;
            self.report_reconciliation_state(true).await;
            return false;
        }
        if !current.matches_snapshot(previous)
            || unresolved_filled_quantity(current)
            || !current.is_terminal()
        {
            self.track_identity_conflict(expected, current);
            self.persist_identity_observations(account_key, symbol)
                .await;
            self.report_reconciliation_state(true).await;
            return false;
        }
        true
    }

    async fn scan_snapshot_conflicts<'a>(
        &self,
        scan: &SnapshotConflictScan<'a>,
        managed: &Result<Vec<GridOrder>>,
    ) -> DetectedSnapshotConflicts<'a> {
        let SnapshotConflictScan {
            account_key,
            symbol,
            live_orders,
            pair_intents,
            remainder_plan,
            restored_ids,
        } = *scan;
        // Inspect duplicate client IDs before building a map: collecting
        // into a map would discard an identity based on response order.
        let mut groups: HashMap<&str, Vec<&'a BinanceOrderResponse>> = HashMap::new();
        for found in live_orders {
            if is_grid_order(&found.client_order_id) {
                groups
                    .entry(&found.client_order_id)
                    .or_default()
                    .push(found);
            }
        }
        let managed_slice = managed.as_deref().unwrap_or(&[]);
        let managed_by_id: HashMap<&str, &GridOrder> = managed_slice
            .iter()
            .map(|order| (order.client_order_id.as_str(), order))
            .collect();
        let active = self.state.active_orders.read().await;
        let mut duplicates: Vec<_> = groups
            .values()
            .filter(|orders| {
                let first = orders[0];
                let known = [
                    managed_by_id.get(first.client_order_id.as_str()).copied(),
                    pair_intents.get(&first.client_order_id),
                    remainder_plan.and_then(|plan| plan.find_order(&first.client_order_id)),
                    active.get(&first.client_order_id),
                ];
                managed.is_err()
                    || self
                        .evidence
                        .order_identity_conflicts
                        .contains_key(&(account_key, first.client_order_id.clone()))
                    || known.into_iter().flatten().any(|expected| {
                        orders.iter().any(|found| {
                            expected.symbol != found.symbol
                                || (expected.purpose != OrderPurpose::Unverified
                                    && expected.side.as_str() != found.side)
                                || expected.order_id.is_some_and(|id| id != found.order_id)
                        })
                    })
                    || orders.iter().any(|order| {
                        order.symbol != symbol
                            || OrderSide::from_exchange_str(&order.side).is_none()
                    })
                    || orders.iter().skip(1).any(|order| {
                        !order.matches_snapshot(first)
                            || order.order_type != first.order_type
                            || order.time_in_force != first.time_in_force
                    })
            })
            .cloned()
            .collect();
        let mut restore_orders = Vec::new();
        let mut active_to_persist = Vec::new();
        let mut active_identity_observations = Vec::new();
        for current in active.values() {
            let durable = find_durable_order(
                &current.client_order_id,
                managed_slice,
                pair_intents,
                remainder_plan,
            );
            if let (Some(expected), Some(observed)) =
                (durable, ObservedOrderIdentity::from_order(current))
            {
                if !observed.matches_confirmed_order(expected) {
                    if expected.order_id.is_none()
                        && expected.symbol == current.symbol
                        && expected.side == current.side
                        && restored_ids.contains(&current.client_order_id)
                        && live_orders.iter().any(|found| {
                            current.matches_exchange_identity(found)
                                && !unresolved_filled_quantity(found)
                        })
                    {
                        continue;
                    }
                    let mut retained = expected.clone();
                    if retained.order_id.is_none()
                        && retained.symbol == current.symbol
                        && retained.side == current.side
                        && current.purpose != OrderPurpose::Unverified
                        && !restored_ids.contains(&current.client_order_id)
                        && !self
                            .evidence
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
                let durable = find_durable_order(id, managed_slice, pair_intents, remainder_plan);
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
                    || order.purpose == OrderPurpose::Unverified
                    || find_durable_order(
                        &order.client_order_id,
                        managed_slice,
                        pair_intents,
                        remainder_plan,
                    )
                    .is_some()
                {
                    continue;
                }
                active_to_persist.push(order.clone());
            }
        }
        DetectedSnapshotConflicts {
            duplicates,
            restore_orders,
            active_to_persist,
            active_identity_observations,
        }
    }

    fn snapshot_conflicts_require_block(
        &self,
        account_key: [u8; 32],
        restored_ids: &HashSet<String>,
        managed_failed: bool,
        detected: &DetectedSnapshotConflicts<'_>,
    ) -> bool {
        managed_failed
            || detected
                .active_identity_observations
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
                    self.evidence
                        .order_identity_observations
                        .get(&key)
                        .is_none_or(|items| !items.contains(observed))
                })
            || detected.duplicates.iter().any(|orders| {
                let key = (account_key, orders[0].client_order_id.clone());
                if orders.len() == 1
                    && self
                        .evidence
                        .order_identity_conflicts
                        .get(&key)
                        .is_some_and(|expected| {
                            expected.order_id.is_some()
                                && expected.matches_exchange_identity(orders[0])
                        })
                {
                    return false;
                }
                !self.evidence.order_identity_conflicts.contains_key(&key)
                    || orders.iter().any(|found| {
                        let observed = ObservedOrderIdentity::from_response(found);
                        self.evidence
                            .order_identity_observations
                            .get(&key)
                            .is_none_or(|items| !items.contains(&observed))
                    })
            })
    }

    async fn apply_and_persist_snapshot_conflicts(
        &mut self,
        scan: &SnapshotConflictScan<'_>,
        managed: &[GridOrder],
        detected: DetectedSnapshotConflicts<'_>,
    ) -> bool {
        let SnapshotConflictScan {
            account_key,
            symbol,
            pair_intents,
            remainder_plan,
            ..
        } = *scan;
        let DetectedSnapshotConflicts {
            mut duplicates,
            restore_orders,
            active_to_persist,
            active_identity_observations,
        } = detected;
        let state = self.state.clone();
        let mut active = state.active_orders.write().await;
        let mut snapshot_responses = Vec::new();
        for order in restore_orders {
            active.entry(order.client_order_id.clone()).or_insert(order);
        }
        for (mut expected, observed) in active_identity_observations {
            let key = (account_key, expected.client_order_id.clone());
            if let Some(confirmed) = self.evidence.order_identity_conflicts.get(&key) {
                expected.adopt_compatible_order_id(confirmed);
            }
            self.record_identity_conflict_observation(account_key, &expected, observed);
        }
        duplicates.sort_by(|a, b| a[0].client_order_id.cmp(&b[0].client_order_id));
        for mut observations in duplicates {
            observations.sort_by(|a, b| {
                (&a.symbol, &a.side, a.order_id).cmp(&(&b.symbol, &b.side, b.order_id))
            });
            let first = observations[0];
            let mut expected = find_durable_order(
                &first.client_order_id,
                managed,
                pair_intents,
                remainder_plan,
            )
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
                let mut order = GridOrder::from_exchange_response(
                    first,
                    quantity,
                    OrderPurpose::Unverified,
                    Utc::now(),
                );
                order.order_id = None;
                order.symbol = symbol.to_owned();
                order.price = price;
                order.amount_usdc = price * quantity;
                order.status = OrderStatus::New;
                order
            });
            if let Some(confirmed) = self
                .evidence
                .order_identity_conflicts
                .get(&(account_key, expected.client_order_id.clone()))
            {
                expected.adopt_compatible_order_id(confirmed);
            }
            if expected.purpose == OrderPurpose::Unverified {
                for found in &observations {
                    expected.reserve_max(found.price, found.orig_qty);
                }
            }
            for found in observations {
                self.track_identity_conflict(&expected, found);
                snapshot_responses.push((expected.client_order_id.clone(), found.clone()));
            }
            active.insert(expected.client_order_id.clone(), expected);
        }
        drop(active);
        let snapshot_batch = format!("snapshot:{}", Uuid::new_v4());
        let saved_snapshots = if let Some(snapshot_identities) =
            self.collect_identity_observations(account_key, symbol)
        {
            self.record_initial_order_response_snapshot(
                account_key,
                symbol,
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
                .run_blocking(move |db| db.save_managed_orders_if_absent(&active_to_persist))
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
            .persist_identity_observations(account_key, symbol)
            .await;
        saved_active && saved_observations && saved_snapshots
    }

    pub(super) async fn detect_and_persist_snapshot_conflicts(
        &mut self,
        scan: SnapshotConflictScan<'_>,
    ) -> bool {
        let managed = self.load_managed_orders(scan.symbol).await;
        if let Err(error) = &managed {
            error!("Could not verify snapshot order identities: {}", error);
        }
        let detected = self.scan_snapshot_conflicts(&scan, &managed).await;
        if detected.duplicates.is_empty() && detected.active_identity_observations.is_empty() {
            return true;
        }
        let block = self.snapshot_conflicts_require_block(
            scan.account_key,
            scan.restored_ids,
            managed.is_err(),
            &detected,
        );
        self.report_reconciliation_state(true).await;
        if managed.is_err() {
            self.last_orders_sync = None;
        }
        let saved = self
            .apply_and_persist_snapshot_conflicts(
                &scan,
                managed.as_deref().unwrap_or(&[]),
                detected,
            )
            .await;
        saved && !block
    }

    async fn discover_live_conflict_observations(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        conflicts: &[ConflictEntry],
        live_orders: &[BinanceOrderResponse],
    ) -> bool {
        let mut discovered = false;
        for (_, expected) in conflicts {
            for found in live_orders {
                if found.client_order_id == expected.client_order_id
                    && (expected.order_id.is_none() || !expected.matches_exchange_identity(found))
                {
                    self.track_identity_conflict(expected, found);
                    discovered = true;
                }
            }
        }
        !discovered
            || self
                .persist_identity_observations(account_key, symbol)
                .await
    }

    async fn load_verified_conflict_timeline(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        client_order_id: &str,
    ) -> Option<Vec<BinanceOrderResponse>> {
        let timeline = match self
            .load_initial_order_response_timeline(account_key, symbol, client_order_id)
            .await
        {
            Some(responses) => responses,
            None => {
                self.report_reconciliation_state(true).await;
                return None;
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
                        symbol,
                        client_order_id,
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
            return None;
        }
        Some(timeline.into_iter().map(|(_, response)| response).collect())
    }

    async fn verify_conflict_observations(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        key: &ConflictKey,
        expected: &GridOrder,
        initial_responses: &[BinanceOrderResponse],
    ) -> Option<VerifiedConflictObservations> {
        let mut corrected_expected: Option<BinanceOrderResponse> = None;
        let mut ambiguous_candidate = false;
        let mut harmless_candidate: Option<BinanceOrderResponse> = None;
        let mut ambiguous_harmless = false;
        let mut harmless_identities = HashSet::new();
        let mut queried_orders: HashMap<(String, String, i64), BinanceOrderResponse> =
            HashMap::new();
        let observations = self
            .evidence
            .order_identity_observations
            .get(key)
            .cloned()
            .unwrap_or_default();
        for observed in observations {
            let Ok(found) = self
                .client
                .get_order(&observed.symbol, observed.order_id)
                .await
            else {
                self.report_reconciliation_state(true).await;
                return None;
            };
            if !self
                .record_and_check_initial_timeline(
                    account_key,
                    symbol,
                    &expected.client_order_id,
                    &found,
                    initial_responses,
                )
                .await
            {
                return None;
            }
            let query_key = (
                found.client_order_id.clone(),
                found.symbol.clone(),
                found.order_id,
            );
            if let Some(previous) = queried_orders.get(&query_key) {
                if previous.side != found.side {
                    self.track_identity_conflict(expected, previous);
                    self.track_identity_conflict(expected, &found);
                    self.persist_identity_observations(account_key, symbol)
                        .await;
                    self.report_reconciliation_state(true).await;
                    return None;
                }
                if terminal_responses_disagree(previous, &found) {
                    self.record_terminal_disagreement(
                        account_key,
                        symbol,
                        &expected.client_order_id,
                        previous,
                        &found,
                    )
                    .await;
                    self.report_reconciliation_state(true).await;
                    return None;
                }
                if !previous.matches_snapshot(&found) {
                    self.report_reconciliation_state(true).await;
                    return None;
                }
            } else {
                queried_orders.insert(query_key, found.clone());
            }
            let same_order = found.order_id == observed.order_id
                && found.symbol == observed.symbol
                && found.client_order_id == observed.client_order_id;
            let harmless_terminal =
                same_order && found.executed_qty == Decimal::ZERO && found.is_unfilled_terminal();
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
                && OrderSide::from_exchange_str(&found.side).is_some()
                && (expected.purpose == OrderPurpose::Unverified
                    || found.side == expected.side.as_str())
                && expected.order_id.is_none_or(|id| id == found.order_id)
                && !unresolved_filled_quantity(&found)
                && BinanceOrderResponse::is_known_status(&found.status);
            if corrected_to_expected
                && harmless_terminal
                && expected.purpose == OrderPurpose::Unverified
            {
                if harmless_candidate
                    .as_ref()
                    .is_some_and(|old| old.order_id != found.order_id)
                {
                    ambiguous_harmless = true;
                } else {
                    harmless_candidate = Some(found.clone());
                }
            } else if corrected_to_expected && (expected.order_id.is_some() || !harmless_terminal) {
                if corrected_expected
                    .as_ref()
                    .is_some_and(|old| old.order_id != found.order_id)
                {
                    ambiguous_candidate = true;
                } else {
                    corrected_expected = Some(found.clone());
                }
            }
            if !(corrected_to_expected || harmless_terminal) {
                self.report_reconciliation_state(true).await;
                return None;
            }
        }
        if corrected_expected.is_none() && expected.purpose == OrderPurpose::Unverified {
            if ambiguous_harmless {
                self.report_reconciliation_state(true).await;
                return None;
            }
            corrected_expected = harmless_candidate;
        }
        if ambiguous_candidate {
            self.report_reconciliation_state(true).await;
            return None;
        }
        Some(VerifiedConflictObservations {
            corrected_expected,
            harmless_identities,
        })
    }

    async fn confirm_unique_client_id_candidate(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        key: &ConflictKey,
        expected: &mut GridOrder,
        candidate: &BinanceOrderResponse,
    ) -> Option<BinanceOrderResponse> {
        let unique = self
            .client
            .lookup_order_by_client_id(&expected.symbol, &expected.client_order_id)
            .await;
        if let Ok(Some(found)) = &unique {
            if !self
                .record_initial_order_response(
                    account_key,
                    symbol,
                    &expected.client_order_id,
                    found,
                )
                .await
            {
                self.report_reconciliation_state(true).await;
                return None;
            }
        }
        let unique = match unique {
            Ok(Some(found))
                if found.matches_snapshot(candidate)
                    && (expected.purpose == OrderPurpose::Unverified
                        || found.side == expected.side.as_str())
                    && !unresolved_filled_quantity(&found)
                    && BinanceOrderResponse::is_known_status(&found.status) =>
            {
                found
            }
            Ok(Some(found)) => {
                if terminal_responses_disagree(candidate, &found) {
                    self.record_terminal_disagreement(
                        account_key,
                        symbol,
                        &expected.client_order_id,
                        candidate,
                        &found,
                    )
                    .await;
                }
                self.track_identity_conflict(expected, &found);
                self.persist_identity_observations(account_key, symbol)
                    .await;
                self.report_reconciliation_state(true).await;
                return None;
            }
            _ => {
                self.report_reconciliation_state(true).await;
                return None;
            }
        };
        let compatible = self
            .state
            .active_orders
            .read()
            .await
            .get(&expected.client_order_id)
            .is_none_or(|order| order.order_id.is_none_or(|id| id == unique.order_id));
        if !compatible {
            self.report_reconciliation_state(true).await;
            return None;
        }
        if expected.purpose == OrderPurpose::Unverified {
            expected.side = OrderSide::from_exchange_str(&unique.side).unwrap_or(OrderSide::Buy);
            expected.price = unique.price;
            expected.amount_usdc = expected.price * expected.quantity;
            expected.purpose = OrderPurpose::Legacy;
        }
        expected.order_id = Some(unique.order_id);
        self.evidence
            .order_identity_conflicts
            .insert(key.clone(), expected.clone());
        if !self
            .persist_identity_observations(account_key, symbol)
            .await
        {
            return None;
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
        Some(unique)
    }

    async fn resolve_absent_conflict_order(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        key: &ConflictKey,
        expected: &mut GridOrder,
        initial_responses: &[BinanceOrderResponse],
    ) -> Option<BinanceOrderResponse> {
        let Ok(Some(found)) = self.lookup_order(expected).await else {
            self.report_reconciliation_state(true).await;
            return None;
        };
        let unknown_id = expected.order_id.is_none();
        let identity_matches = expected.matches_exchange_identity(&found);
        if unknown_id || !identity_matches {
            self.track_identity_conflict(expected, &found);
            if !self
                .persist_identity_observations(account_key, symbol)
                .await
            {
                return None;
            }
        }
        if !self
            .record_and_check_initial_timeline(
                account_key,
                symbol,
                &expected.client_order_id,
                &found,
                initial_responses,
            )
            .await
        {
            return None;
        }
        if !identity_matches
            || unresolved_filled_quantity(&found)
            || (!unknown_id && !found.is_terminal())
        {
            self.report_reconciliation_state(true).await;
            return None;
        }
        if !unknown_id {
            return Some(found);
        }
        let Ok(verified) = self.client.get_order(&found.symbol, found.order_id).await else {
            self.report_reconciliation_state(true).await;
            return None;
        };
        if !self
            .verify_consistent_terminal_followup(account_key, symbol, expected, &found, &verified)
            .await
        {
            return None;
        }
        let Ok(Some(unique)) = self
            .client
            .lookup_order_by_client_id(&expected.symbol, &expected.client_order_id)
            .await
        else {
            self.report_reconciliation_state(true).await;
            return None;
        };
        if !self
            .verify_consistent_terminal_followup(account_key, symbol, expected, &verified, &unique)
            .await
        {
            return None;
        }
        let compatible = self
            .state
            .active_orders
            .read()
            .await
            .get(&expected.client_order_id)
            .is_none_or(|order| order.matches_exchange_identity(&verified));
        if !compatible {
            self.report_reconciliation_state(true).await;
            return None;
        }
        expected.order_id = Some(verified.order_id);
        self.evidence
            .order_identity_conflicts
            .insert(key.clone(), expected.clone());
        if !self
            .persist_identity_observations(account_key, symbol)
            .await
        {
            return None;
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
        Some(verified)
    }

    pub(super) async fn resolve_order_identity_conflicts(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        live_orders: &mut Vec<BinanceOrderResponse>,
        restored_ids: &mut HashSet<String>,
    ) -> Option<ResolvedConflicts> {
        let conflicts: Vec<ConflictEntry> = self
            .evidence
            .order_identity_conflicts
            .iter()
            .filter(|((account, _), order)| *account == account_key && order.symbol == symbol)
            .map(|(key, order)| (key.clone(), order.clone()))
            .collect();
        if !self
            .discover_live_conflict_observations(account_key, symbol, &conflicts, live_orders)
            .await
        {
            return None;
        }
        let mut corrected_terminal = HashMap::new();
        let mut resolved_conflict_keys = Vec::new();
        for (key, mut expected) in conflicts {
            let initial_responses = self
                .load_verified_conflict_timeline(account_key, symbol, &expected.client_order_id)
                .await?;
            let mut verified = self
                .verify_conflict_observations(
                    account_key,
                    symbol,
                    &key,
                    &expected,
                    &initial_responses,
                )
                .await?;
            if expected.order_id.is_none()
                || expected.purpose == OrderPurpose::Unverified
                || verified.corrected_expected.is_some()
            {
                if let Some(candidate) = &verified.corrected_expected {
                    verified.corrected_expected = Some(
                        self.confirm_unique_client_id_candidate(
                            account_key,
                            symbol,
                            &key,
                            &mut expected,
                            candidate,
                        )
                        .await?,
                    );
                }
            }
            if expected.purpose == OrderPurpose::Unverified {
                self.report_reconciliation_state(true).await;
                return None;
            }
            live_orders.retain(|order| {
                !verified.harmless_identities.contains(&(
                    order.client_order_id.clone(),
                    order.symbol.clone(),
                    order.order_id,
                ))
            });
            if let Some(corrected) = verified.corrected_expected {
                // A single-order query is authoritative for this same
                // exchange ID even if the list still has the old side.
                live_orders.retain(|order| {
                    !(order.client_order_id == corrected.client_order_id
                        && order.symbol == corrected.symbol
                        && order.order_id == corrected.order_id)
                });
                if corrected.is_terminal() {
                    corrected_terminal.insert(corrected.client_order_id.clone(), corrected);
                } else {
                    live_orders.push(corrected);
                }
            }
            let (seen, matching) = live_orders
                .iter()
                .filter(|order| order.client_order_id == expected.client_order_id)
                .fold((false, true), |(_, consistent), order| {
                    (
                        true,
                        consistent && expected.matches_exchange_identity(order),
                    )
                });
            if !matching {
                self.report_reconciliation_state(true).await;
                return None;
            }
            if !seen && !corrected_terminal.contains_key(&expected.client_order_id) {
                let found = self
                    .resolve_absent_conflict_order(
                        account_key,
                        symbol,
                        &key,
                        &mut expected,
                        &initial_responses,
                    )
                    .await?;
                corrected_terminal.insert(found.client_order_id.clone(), found);
            }
            resolved_conflict_keys.push(key);
            restored_ids.insert(expected.client_order_id);
        }
        Some((corrected_terminal, resolved_conflict_keys))
    }
}
