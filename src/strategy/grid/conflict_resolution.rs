use super::snapshot_identities::VerifiedConflictObservations;
use super::*;

type ResolvedConflicts = (HashMap<String, BinanceOrderResponse>, Vec<String>);

impl GridTradingEngine {
    async fn discover_live_conflict_observations(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        conflicts: &[GridOrder],
        live_orders: &[BinanceOrderResponse],
    ) -> bool {
        let mut discovered = false;
        for expected in conflicts {
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
    async fn verify_unknown_id_absent_conflict(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        expected: &mut GridOrder,
        found: &BinanceOrderResponse,
    ) -> Option<BinanceOrderResponse> {
        let Ok(verified) = self.client.get_order(&found.symbol, found.order_id).await else {
            self.report_reconciliation_state(true).await;
            return None;
        };
        if !self
            .verify_consistent_terminal_followup(account_key, symbol, expected, found, &verified)
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
        }
        Some(verified)
    }

    async fn resolve_absent_conflict_order(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
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
        self.verify_unknown_id_absent_conflict(account_key, symbol, expected, &found)
            .await
    }

    fn apply_verified_conflict_to_snapshot(
        expected: &GridOrder,
        verified: VerifiedConflictObservations,
        live_orders: &mut Vec<BinanceOrderResponse>,
        corrected_terminal: &mut HashMap<String, BinanceOrderResponse>,
    ) -> Option<bool> {
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
        matching.then_some(seen)
    }

    async fn resolve_single_identity_conflict(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        mut expected: GridOrder,
        live_orders: &mut Vec<BinanceOrderResponse>,
        corrected_terminal: &mut HashMap<String, BinanceOrderResponse>,
    ) -> Option<String> {
        let initial_responses = self
            .load_verified_conflict_timeline(account_key, symbol, &expected.client_order_id)
            .await?;
        let mut verified = self
            .verify_conflict_observations(account_key, symbol, &expected, &initial_responses)
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
        let Some(seen) = Self::apply_verified_conflict_to_snapshot(
            &expected,
            verified,
            live_orders,
            corrected_terminal,
        ) else {
            self.report_reconciliation_state(true).await;
            return None;
        };
        if !seen && !corrected_terminal.contains_key(&expected.client_order_id) {
            let found = self
                .resolve_absent_conflict_order(
                    account_key,
                    symbol,
                    &mut expected,
                    &initial_responses,
                )
                .await?;
            corrected_terminal.insert(found.client_order_id.clone(), found);
        }
        Some(expected.client_order_id)
    }

    pub(super) async fn resolve_order_identity_conflicts(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        live_orders: &mut Vec<BinanceOrderResponse>,
        restored_ids: &mut HashSet<String>,
    ) -> Option<ResolvedConflicts> {
        let conflicts = self.evidence.conflicts_for_scope(account_key, symbol);
        if !self
            .discover_live_conflict_observations(account_key, symbol, &conflicts, live_orders)
            .await
        {
            return None;
        }
        let mut corrected_terminal = HashMap::new();
        let mut resolved_conflict_ids = Vec::with_capacity(conflicts.len());
        for expected in conflicts {
            let resolved_id = self
                .resolve_single_identity_conflict(
                    account_key,
                    symbol,
                    expected,
                    live_orders,
                    &mut corrected_terminal,
                )
                .await?;
            restored_ids.insert(resolved_id.clone());
            resolved_conflict_ids.push(resolved_id);
        }
        Some((corrected_terminal, resolved_conflict_ids))
    }
}
