use super::evidence::terminal_responses_disagree;
use super::*;

type ConflictKey = ([u8; 32], String);
type ConflictEntry = (ConflictKey, GridOrder);
type ResolvedConflicts = (HashMap<String, BinanceOrderResponse>, Vec<ConflictKey>);

struct VerifiedConflictObservations {
    corrected_expected: Option<BinanceOrderResponse>,
    harmless_identities: HashSet<(String, String, i64)>,
}

impl GridTradingEngine {
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

    async fn verify_single_conflict_observation(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        expected: &GridOrder,
        observed: ObservedOrderIdentity,
        initial_responses: &[BinanceOrderResponse],
        queried_orders: &mut HashMap<(String, String, i64), BinanceOrderResponse>,
    ) -> Option<(BinanceOrderResponse, bool, bool)> {
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
        let corrected_to_expected = same_order
            && found.client_order_id == expected.client_order_id
            && found.has_valid_symbol_and_side(&expected.symbol)
            && (expected.purpose == OrderPurpose::Unverified
                || found.side == expected.side.as_str())
            && expected.order_id.is_none_or(|id| id == found.order_id)
            && !unresolved_filled_quantity(&found)
            && BinanceOrderResponse::is_known_status(&found.status);
        if !(corrected_to_expected || harmless_terminal) {
            self.report_reconciliation_state(true).await;
            return None;
        }
        Some((found, harmless_terminal, corrected_to_expected))
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
        let mut queried_orders = HashMap::new();
        let observations = self
            .evidence
            .order_identity_observations
            .get(key)
            .cloned()
            .unwrap_or_default();
        for observed in observations {
            let (found, harmless_terminal, corrected_to_expected) = self
                .verify_single_conflict_observation(
                    account_key,
                    symbol,
                    expected,
                    observed,
                    initial_responses,
                    &mut queried_orders,
                )
                .await?;
            if harmless_terminal {
                harmless_identities.insert((
                    found.client_order_id.clone(),
                    found.symbol.clone(),
                    found.order_id,
                ));
            }
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
                    harmless_candidate = Some(found);
                }
            } else if corrected_to_expected && (expected.order_id.is_some() || !harmless_terminal) {
                if corrected_expected
                    .as_ref()
                    .is_some_and(|old| old.order_id != found.order_id)
                {
                    ambiguous_candidate = true;
                } else {
                    corrected_expected = Some(found);
                }
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
            let Some(side) = OrderSide::from_exchange_str(&unique.side) else {
                self.report_reconciliation_state(true).await;
                return None;
            };
            expected.side = side;
            expected.price = unique.price;
            expected.amount_usdc = expected
                .price
                .checked_mul(expected.quantity)
                .unwrap_or(Decimal::MAX);
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
