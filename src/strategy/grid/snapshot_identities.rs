use super::reconciliation::find_durable_order;
use super::*;

pub(super) struct VerifiedConflictObservations {
    pub(super) corrected_expected: Option<BinanceOrderResponse>,
    pub(super) harmless_identities: HashSet<(String, String, i64)>,
}

#[derive(Default)]
pub(super) struct ConflictObservationCandidates {
    corrected_expected: Option<BinanceOrderResponse>,
    ambiguous_candidate: bool,
    harmless_candidate: Option<BinanceOrderResponse>,
    ambiguous_harmless: bool,
    harmless_identities: HashSet<(String, String, i64)>,
}

impl ConflictObservationCandidates {
    pub(super) fn record(
        &mut self,
        expected: &GridOrder,
        found: BinanceOrderResponse,
        harmless_terminal: bool,
        corrected_to_expected: bool,
    ) {
        if harmless_terminal {
            self.harmless_identities.insert((
                found.client_order_id.clone(),
                found.symbol.clone(),
                found.order_id,
            ));
        }
        if corrected_to_expected
            && harmless_terminal
            && expected.purpose == OrderPurpose::Unverified
        {
            if self
                .harmless_candidate
                .as_ref()
                .is_some_and(|old| old.order_id != found.order_id)
            {
                self.ambiguous_harmless = true;
            } else {
                self.harmless_candidate = Some(found);
            }
        } else if corrected_to_expected && (expected.order_id.is_some() || !harmless_terminal) {
            if self
                .corrected_expected
                .as_ref()
                .is_some_and(|old| old.order_id != found.order_id)
            {
                self.ambiguous_candidate = true;
            } else {
                self.corrected_expected = Some(found);
            }
        }
    }

    pub(super) fn finish(mut self, expected: &GridOrder) -> Option<VerifiedConflictObservations> {
        if self.corrected_expected.is_none() && expected.purpose == OrderPurpose::Unverified {
            if self.ambiguous_harmless {
                return None;
            }
            self.corrected_expected = self.harmless_candidate;
        }
        if self.ambiguous_candidate {
            return None;
        }
        Some(VerifiedConflictObservations {
            corrected_expected: self.corrected_expected,
            harmless_identities: self.harmless_identities,
        })
    }
}

pub(super) fn invalid_snapshot_price(order: &BinanceOrderResponse) -> bool {
    if order.price < Decimal::ZERO {
        return true;
    }
    if order.price > Decimal::ZERO {
        return false;
    }
    if order.order_type.eq_ignore_ascii_case("MARKET")
        && (!order.is_terminal()
            || (order.is_unfilled_terminal() && order.executed_qty <= Decimal::ZERO))
    {
        return false;
    }
    !(order.is_terminal()
        && order.executed_qty > Decimal::ZERO
        && order.avg_price.is_some_and(|price| price > Decimal::ZERO))
}

impl GridTradingEngine {
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
            if let Some(existing) = active.get(&found.client_order_id) {
                self.evidence
                    .mark_provisional(account_key, &found.client_order_id);
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
                self.evidence
                    .mark_provisional(account_key, &found.client_order_id);
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
                .mark_provisional(account_key, &found.client_order_id);
            active.insert(order.client_order_id.clone(), order);
        }
        drop(active);
        self.persist_identity_observations(account_key, symbol)
            .await;
    }

    async fn restore_verified_snapshot_orders(
        &self,
        live_ids: &HashMap<String, BinanceOrderResponse>,
        managed_by_id: &HashMap<String, GridOrder>,
        pair_intents: &HashMap<String, GridOrder>,
        remainder_plan: Option<&RemainderPlan>,
        restored_ids: &mut HashSet<String>,
    ) {
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
    }

    pub(super) async fn verify_live_snapshot_identities(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        live_ids: &HashMap<String, BinanceOrderResponse>,
        recovery: (
            &HashMap<String, GridOrder>,
            Option<&RemainderPlan>,
            Option<Vec<GridOrder>>,
        ),
        restored_ids: &mut HashSet<String>,
    ) -> Option<HashMap<String, GridOrder>> {
        let (pair_intents, remainder_plan, preloaded_managed) = recovery;
        if live_ids.is_empty() {
            return Some(HashMap::new());
        }
        let managed_result = match preloaded_managed {
            Some(orders) => Ok(orders),
            None => self.load_managed_orders(symbol).await,
        };
        let managed = match managed_result {
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
        let (conflicts, invalid_live_identity) = self
            .collect_snapshot_identity_conflicts(
                symbol,
                live_ids,
                &managed_by_id,
                pair_intents,
                remainder_plan,
            )
            .await;
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
        self.restore_verified_snapshot_orders(
            live_ids,
            &managed_by_id,
            pair_intents,
            remainder_plan,
            restored_ids,
        )
        .await;
        Some(managed_by_id)
    }

    async fn collect_snapshot_identity_conflicts(
        &mut self,
        symbol: &str,
        live_ids: &HashMap<String, BinanceOrderResponse>,
        managed_by_id: &HashMap<String, GridOrder>,
        pair_intents: &HashMap<String, GridOrder>,
        remainder_plan: Option<&RemainderPlan>,
    ) -> (Vec<(GridOrder, BinanceOrderResponse)>, bool) {
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
        (conflicts, invalid_live_identity)
    }

    async fn stage_invalid_price_orders(
        &mut self,
        symbol: &str,
        orders: &[BinanceOrderResponse],
        managed: &[GridOrder],
        intents: &HashMap<String, GridOrder>,
        remainder: Option<&RemainderPlan>,
    ) -> (Vec<(String, BinanceOrderResponse)>, Vec<GridOrder>) {
        let mut matching: Vec<&BinanceOrderResponse> = orders
            .iter()
            .filter(|order| is_grid_order(&order.client_order_id))
            .collect();
        matching.sort_by(|a, b| {
            (&a.client_order_id, &a.symbol, &a.side, a.order_id).cmp(&(
                &b.client_order_id,
                &b.symbol,
                &b.side,
                b.order_id,
            ))
        });
        let state = self.state.clone();
        let mut active = state.active_orders.write().await;
        let mut responses = Vec::with_capacity(matching.len());
        for found in matching {
            let mut expected =
                find_durable_order(&found.client_order_id, managed, intents, remainder)
                    .or_else(|| active.get(&found.client_order_id))
                    .cloned()
                    .unwrap_or_else(|| {
                        let mut order = GridOrder::from_exchange_response(
                            found,
                            found.orig_qty.max(Decimal::ZERO),
                            OrderPurpose::Unverified,
                            Utc::now(),
                        );
                        order.order_id = (found.has_valid_symbol_and_side(symbol)
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
        (responses, conservative)
    }

    pub(super) async fn retain_invalid_price_snapshot(
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
        let (responses, conservative) = self
            .stage_invalid_price_orders(symbol, orders, &managed, &intents, remainder.as_ref())
            .await;
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
}
