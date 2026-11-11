use super::evidence_book::terminal_responses_disagree;
use super::snapshot_identities::{ConflictObservationCandidates, VerifiedConflictObservations};
use super::*;

impl GridTradingEngine {
    async fn record_queried_conflict_order(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        expected: &GridOrder,
        found: &BinanceOrderResponse,
        queried_orders: &mut HashMap<(String, String, i64), BinanceOrderResponse>,
    ) -> bool {
        let query_key = (
            found.client_order_id.clone(),
            found.symbol.clone(),
            found.order_id,
        );
        if let Some(previous) = queried_orders.get(&query_key) {
            if previous.side != found.side {
                self.track_identity_conflict(expected, previous);
                self.track_identity_conflict(expected, found);
                self.persist_identity_observations(account_key, symbol)
                    .await;
                self.report_reconciliation_state(true).await;
                return false;
            }
            if terminal_responses_disagree(previous, found) {
                self.record_terminal_disagreement(
                    account_key,
                    symbol,
                    &expected.client_order_id,
                    previous,
                    found,
                )
                .await;
                self.report_reconciliation_state(true).await;
                return false;
            }
            if !previous.matches_snapshot(found) {
                self.report_reconciliation_state(true).await;
                return false;
            }
        } else {
            queried_orders.insert(query_key, found.clone());
        }
        true
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
            || !self
                .record_queried_conflict_order(
                    account_key,
                    symbol,
                    expected,
                    &found,
                    queried_orders,
                )
                .await
        {
            return None;
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

    pub(super) async fn verify_conflict_observations(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        expected: &GridOrder,
        initial_responses: &[BinanceOrderResponse],
    ) -> Option<VerifiedConflictObservations> {
        let mut candidates = ConflictObservationCandidates::default();
        let mut queried_orders = HashMap::new();
        let observations = self
            .evidence
            .get_observations(account_key, &expected.client_order_id)
            .map(|items| items.to_vec())
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
            candidates.record(expected, found, harmless_terminal, corrected_to_expected);
        }
        let Some(verified) = candidates.finish(expected) else {
            self.report_reconciliation_state(true).await;
            return None;
        };
        Some(verified)
    }

    async fn adopt_confirmed_unique_candidate(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        expected: &mut GridOrder,
        unique: BinanceOrderResponse,
    ) -> Option<BinanceOrderResponse> {
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
        self.evidence.insert_conflict(account_key, expected.clone());
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

    pub(super) async fn confirm_unique_client_id_candidate(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
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
        self.adopt_confirmed_unique_candidate(account_key, symbol, expected, unique)
            .await
    }
}
