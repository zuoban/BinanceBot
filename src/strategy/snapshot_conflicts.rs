use super::reconciliation::find_durable_order;
use super::*;

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

impl GridTradingEngine {
    fn group_has_snapshot_conflict(
        &self,
        scan: &SnapshotConflictScan<'_>,
        orders: &[&BinanceOrderResponse],
        managed_failed: bool,
        managed_by_id: &HashMap<&str, &GridOrder>,
        active: &HashMap<String, GridOrder>,
    ) -> bool {
        let first = orders[0];
        let known = [
            managed_by_id.get(first.client_order_id.as_str()).copied(),
            scan.pair_intents.get(&first.client_order_id),
            scan.remainder_plan
                .and_then(|plan| plan.find_order(&first.client_order_id)),
            active.get(&first.client_order_id),
        ];
        managed_failed
            || self
                .evidence
                .has_conflict(scan.account_key, &first.client_order_id)
            || known.into_iter().flatten().any(|expected| {
                orders.iter().any(|found| {
                    expected.symbol != found.symbol
                        || (expected.purpose != OrderPurpose::Unverified
                            && expected.side.as_str() != found.side)
                        || expected.order_id.is_some_and(|id| id != found.order_id)
                })
            })
            || orders
                .iter()
                .any(|order| !order.has_valid_symbol_and_side(scan.symbol))
            || orders.iter().skip(1).any(|order| {
                !order.matches_snapshot(first)
                    || order.order_type != first.order_type
                    || order.time_in_force != first.time_in_force
            })
    }

    fn collect_active_identity_observations(
        &self,
        scan: &SnapshotConflictScan<'_>,
        managed_slice: &[GridOrder],
        active: &HashMap<String, GridOrder>,
    ) -> Vec<(GridOrder, ObservedOrderIdentity)> {
        let mut observations = Vec::new();
        for current in active.values() {
            let durable = find_durable_order(
                &current.client_order_id,
                managed_slice,
                scan.pair_intents,
                scan.remainder_plan,
            );
            let (Some(expected), Some(observed)) =
                (durable, ObservedOrderIdentity::from_order(current))
            else {
                continue;
            };
            if observed.matches_confirmed_order(expected) {
                continue;
            }
            if expected.order_id.is_none()
                && expected.symbol == current.symbol
                && expected.side == current.side
                && scan.restored_ids.contains(&current.client_order_id)
                && scan.live_orders.iter().any(|found| {
                    current.matches_exchange_identity(found) && !unresolved_filled_quantity(found)
                })
            {
                continue;
            }
            let mut retained = expected.clone();
            if retained.order_id.is_none()
                && retained.symbol == current.symbol
                && retained.side == current.side
                && current.purpose != OrderPurpose::Unverified
                && !scan.restored_ids.contains(&current.client_order_id)
                && !self
                    .evidence
                    .is_provisional(scan.account_key, &current.client_order_id)
            {
                retained.order_id = current.order_id;
            }
            observations.push((retained, observed));
        }
        observations
    }

    fn collect_snapshot_conflict_retention<'a>(
        scan: &SnapshotConflictScan<'a>,
        managed_slice: &[GridOrder],
        active: &HashMap<String, GridOrder>,
        groups: &HashMap<&str, Vec<&'a BinanceOrderResponse>>,
        duplicates: &mut Vec<Vec<&'a BinanceOrderResponse>>,
    ) -> (Vec<GridOrder>, Vec<GridOrder>) {
        let mut restore_orders = Vec::new();
        let mut active_to_persist = Vec::new();
        for orders in groups.values() {
            let id = &orders[0].client_order_id;
            let durable =
                find_durable_order(id, managed_slice, scan.pair_intents, scan.remainder_plan);
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
            if order.symbol != scan.symbol
                || order.purpose == OrderPurpose::Unverified
                || find_durable_order(
                    &order.client_order_id,
                    managed_slice,
                    scan.pair_intents,
                    scan.remainder_plan,
                )
                .is_some()
            {
                continue;
            }
            active_to_persist.push(order.clone());
        }
        (restore_orders, active_to_persist)
    }

    async fn scan_snapshot_conflicts<'a>(
        &self,
        scan: &SnapshotConflictScan<'a>,
        managed: &Result<Vec<GridOrder>>,
    ) -> DetectedSnapshotConflicts<'a> {
        // Inspect duplicate client IDs before building a map: collecting
        // into a map would discard an identity based on response order.
        let mut groups: HashMap<&str, Vec<&'a BinanceOrderResponse>> = HashMap::new();
        for found in scan.live_orders {
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
                self.group_has_snapshot_conflict(
                    scan,
                    orders,
                    managed.is_err(),
                    &managed_by_id,
                    &active,
                )
            })
            .cloned()
            .collect();
        let active_identity_observations =
            self.collect_active_identity_observations(scan, managed_slice, &active);
        let (restore_orders, active_to_persist) =
            if !duplicates.is_empty() || !active_identity_observations.is_empty() {
                Self::collect_snapshot_conflict_retention(
                    scan,
                    managed_slice,
                    &active,
                    &groups,
                    &mut duplicates,
                )
            } else {
                (Vec::new(), Vec::new())
            };
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
                    self.evidence
                        .get_observations(account_key, &expected.client_order_id)
                        .is_none_or(|items| !items.contains(observed))
                })
            || detected.duplicates.iter().any(|orders| {
                let id = &orders[0].client_order_id;
                if orders.len() == 1
                    && self
                        .evidence
                        .get_conflict(account_key, id)
                        .is_some_and(|expected| {
                            expected.order_id.is_some()
                                && expected.matches_exchange_identity(orders[0])
                        })
                {
                    return false;
                }
                !self.evidence.has_conflict(account_key, id)
                    || orders.iter().any(|found| {
                        let observed = ObservedOrderIdentity::from_response(found);
                        self.evidence
                            .get_observations(account_key, id)
                            .is_none_or(|items| !items.contains(&observed))
                    })
            })
    }

    async fn stage_snapshot_conflicts<'a>(
        &mut self,
        scan: &SnapshotConflictScan<'a>,
        managed: &[GridOrder],
        restore_orders: Vec<GridOrder>,
        active_identity_observations: Vec<(GridOrder, ObservedOrderIdentity)>,
        mut duplicates: Vec<Vec<&'a BinanceOrderResponse>>,
    ) -> Vec<(String, BinanceOrderResponse)> {
        let state = self.state.clone();
        let mut active = state.active_orders.write().await;
        let mut snapshot_responses = Vec::new();
        for order in restore_orders {
            active.entry(order.client_order_id.clone()).or_insert(order);
        }
        for (mut expected, observed) in active_identity_observations {
            if let Some(confirmed) = self
                .evidence
                .get_conflict(scan.account_key, &expected.client_order_id)
            {
                expected.adopt_compatible_order_id(confirmed);
            }
            self.record_identity_conflict_observation(scan.account_key, &expected, observed);
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
                scan.pair_intents,
                scan.remainder_plan,
            )
            .or_else(|| active.get(&first.client_order_id))
            .cloned()
            .unwrap_or_else(|| Self::build_unverified_duplicate_order(scan.symbol, &observations));
            if let Some(confirmed) = self
                .evidence
                .get_conflict(scan.account_key, &expected.client_order_id)
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
        snapshot_responses
    }

    fn build_unverified_duplicate_order(
        symbol: &str,
        observations: &[&BinanceOrderResponse],
    ) -> GridOrder {
        let first = observations[0];
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
        order.amount_usdc = price.checked_mul(quantity).unwrap_or(Decimal::MAX);
        order.status = OrderStatus::New;
        order
    }

    async fn persist_snapshot_conflicts(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        snapshot_responses: &[(String, BinanceOrderResponse)],
        active_to_persist: Vec<GridOrder>,
    ) -> bool {
        let snapshot_batch = format!("snapshot:{}", Uuid::new_v4());
        let saved_snapshots = if let Some(snapshot_identities) =
            self.collect_identity_observations(account_key, symbol)
        {
            self.record_initial_order_response_snapshot(
                account_key,
                symbol,
                &snapshot_batch,
                snapshot_responses,
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

    async fn apply_and_persist_snapshot_conflicts(
        &mut self,
        scan: &SnapshotConflictScan<'_>,
        managed: &[GridOrder],
        detected: DetectedSnapshotConflicts<'_>,
    ) -> bool {
        let DetectedSnapshotConflicts {
            duplicates,
            restore_orders,
            active_to_persist,
            active_identity_observations,
        } = detected;
        let snapshot_responses = self
            .stage_snapshot_conflicts(
                scan,
                managed,
                restore_orders,
                active_identity_observations,
                duplicates,
            )
            .await;
        self.persist_snapshot_conflicts(
            scan.account_key,
            scan.symbol,
            &snapshot_responses,
            active_to_persist,
        )
        .await
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
}
