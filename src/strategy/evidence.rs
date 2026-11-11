use super::evidence_book::{terminal_responses_disagree, InitialOrderResponseBatch};
use super::*;
impl GridTradingEngine {
    pub(super) async fn record_initial_order_response(
        &mut self,
        account: [u8; 32],
        symbol: &str,
        expected_client_id: &str,
        response: &BinanceOrderResponse,
    ) -> bool {
        let batch_id = Uuid::new_v4().to_string();
        self.record_initial_order_response_batch(
            account,
            symbol,
            expected_client_id,
            &batch_id,
            response,
        )
        .await
    }

    async fn record_initial_order_response_batch(
        &mut self,
        account: [u8; 32],
        symbol: &str,
        expected_client_id: &str,
        batch_id: &str,
        response: &BinanceOrderResponse,
    ) -> bool {
        let observation = (
            expected_client_id.to_owned(),
            Uuid::new_v4().to_string(),
            batch_id.to_owned(),
            response.clone(),
        );
        self.evidence
            .initial_order_responses
            .entry(account)
            .or_default()
            .entry(symbol.to_owned())
            .or_default()
            .push(InitialOrderResponseBatch {
                batch_id: batch_id.to_owned(),
                responses: vec![observation],
                identities: Vec::new(),
            });
        self.flush_initial_order_responses(account, symbol).await
    }

    pub(super) async fn record_initial_order_response_snapshot(
        &mut self,
        account: [u8; 32],
        symbol: &str,
        batch_id: &str,
        responses: &[(String, BinanceOrderResponse)],
        identities: &[(GridOrder, ObservedOrderIdentity)],
    ) -> bool {
        let observations: Vec<_> = responses
            .iter()
            .enumerate()
            .map(|(index, (client_id, response))| {
                (
                    client_id.clone(),
                    format!("snapshot:{batch_id}:{index}"),
                    batch_id.to_owned(),
                    response.clone(),
                )
            })
            .collect();
        if !observations.is_empty() || !identities.is_empty() {
            let batches = self
                .evidence
                .initial_order_responses
                .entry(account)
                .or_default()
                .entry(symbol.to_owned())
                .or_default();
            if let Some(existing) = batches.iter().find(|batch| batch.batch_id == batch_id) {
                let same = existing.responses == observations
                    && existing.identities.as_slice() == identities;
                if !same {
                    error!("Initial order snapshot batch ID reused with different data");
                    self.last_orders_sync = None;
                    self.report_reconciliation_state(true).await;
                    return false;
                }
            } else {
                batches.push(InitialOrderResponseBatch {
                    batch_id: batch_id.to_owned(),
                    responses: observations,
                    identities: identities.to_vec(),
                });
            }
        }
        self.flush_initial_order_responses(account, symbol).await
    }

    pub(super) async fn flush_initial_order_responses(
        &mut self,
        account: [u8; 32],
        symbol: &str,
    ) -> bool {
        if !self.evidence.has_pending_initial_responses(account, symbol) {
            return true;
        }
        loop {
            let Some(responses) = self.evidence.first_initial_response_batch(account, symbol)
            else {
                return true;
            };
            let save_symbol = symbol.to_owned();
            match self
                .state
                .db
                .run_blocking(move |db| {
                    db.save_initial_order_response_snapshot(
                        account,
                        &save_symbol,
                        &responses.responses,
                        &responses.identities,
                    )
                })
                .await
            {
                Ok(()) => {
                    self.evidence.pop_initial_response_batch(account, symbol);
                }
                Err(error) => {
                    error!("Could not persist initial order responses: {}", error);
                    self.last_orders_sync = None;
                    self.report_reconciliation_state(true).await;
                    return false;
                }
            }
        }
    }

    pub(super) async fn load_initial_order_response_timeline(
        &mut self,
        account: [u8; 32],
        symbol: &str,
        expected_client_id: &str,
    ) -> Option<Vec<(String, BinanceOrderResponse)>> {
        if !self.flush_initial_order_responses(account, symbol).await {
            return None;
        }
        let symbol = symbol.to_owned();
        let expected_client_id = expected_client_id.to_owned();
        match self
            .state
            .db
            .run_blocking(move |db| {
                db.load_initial_order_response_timeline(account, &symbol, &expected_client_id)
            })
            .await
        {
            Ok(responses) => Some(responses),
            Err(error) => {
                error!("Could not load initial order responses: {}", error);
                self.last_orders_sync = None;
                self.report_reconciliation_state(true).await;
                None
            }
        }
    }

    pub(super) async fn clear_order_evidence_groups(
        &mut self,
        account: [u8; 32],
        symbol: &str,
        client_ids: &[String],
    ) -> bool {
        if client_ids.is_empty() {
            return true;
        }
        if !self.persist_identity_observations(account, symbol).await {
            self.last_orders_sync = None;
            return false;
        }
        if !self.flush_initial_order_responses(account, symbol).await {
            return false;
        }
        let scope = symbol.to_owned();
        let ids = client_ids.to_vec();
        if let Err(error) = self
            .state
            .db
            .run_blocking(move |db| db.delete_order_evidence_groups(account, &scope, &ids))
            .await
        {
            error!("Could not clear resolved order evidence: {}", error);
            self.last_orders_sync = None;
            self.report_reconciliation_state(true).await;
            return false;
        }
        for id in client_ids {
            if self
                .evidence
                .get_conflict(account, id)
                .is_some_and(|expected| expected.symbol == symbol)
            {
                self.evidence.remove_observations(account, id);
                self.evidence.remove_conflict(account, id);
                self.evidence.clear_provisional(account, id);
            }
        }
        true
    }

    pub(super) async fn record_terminal_disagreement(
        &mut self,
        account: [u8; 32],
        symbol: &str,
        expected_client_id: &str,
        first: &BinanceOrderResponse,
        conflicting: &BinanceOrderResponse,
    ) -> bool {
        let evidence = (first.clone(), conflicting.clone());
        self.evidence
            .terminal_order_disagreements
            .entry(account)
            .or_default()
            .entry(symbol.to_owned())
            .or_default()
            .entry(expected_client_id.to_owned())
            .or_default()
            .push(evidence);
        self.last_orders_sync = None;
        self.report_reconciliation_state(true).await;
        self.flush_terminal_disagreements(account, symbol).await
    }

    async fn flush_terminal_disagreements(&mut self, account: [u8; 32], symbol: &str) -> bool {
        if !self.evidence.has_terminal_disagreements(account, symbol) {
            return true;
        }
        while let Some((client_id, evidence)) =
            self.evidence.first_terminal_disagreement(account, symbol)
        {
            let save_symbol = symbol.to_owned();
            let save_client_id = client_id.clone();
            if let Err(error) = self
                .state
                .db
                .run_blocking(move |db| {
                    db.save_terminal_order_disagreement(
                        account,
                        &save_symbol,
                        &save_client_id,
                        &evidence.0,
                        &evidence.1,
                    )
                })
                .await
            {
                error!("Could not persist terminal order disagreement: {}", error);
                self.last_orders_sync = None;
                self.report_reconciliation_state(true).await;
                return false;
            }
            self.evidence
                .pop_terminal_disagreement(account, symbol, &client_id);
        }
        true
    }

    pub(super) async fn terminal_disagreement_blocked(
        &mut self,
        account: [u8; 32],
        symbol: &str,
    ) -> bool {
        if !self.flush_terminal_disagreements(account, symbol).await {
            return true;
        }
        let scope_symbol = symbol.to_owned();
        let result = self
            .state
            .db
            .run_blocking(move |db| db.load_terminal_order_disagreements(account, &scope_symbol))
            .await;
        let stored_blocked = match result {
            Ok(records) => !records.is_empty(),
            Err(error) => {
                error!(
                    "Could not load or persist terminal order disagreements: {}",
                    error
                );
                self.last_orders_sync = None;
                self.report_reconciliation_state(true).await;
                return true;
            }
        };
        let blocked = stored_blocked || self.evidence.has_terminal_disagreements(account, symbol);
        if blocked {
            warn!("Terminal order disagreement for {} requires manual investigation; automatic clearing is unavailable", symbol);
            self.last_orders_sync = None;
            self.report_reconciliation_state(true).await;
        }
        blocked
    }

    pub(super) async fn load_identity_observations(
        &mut self,
        account: [u8; 32],
        symbol: &str,
    ) -> bool {
        if self.evidence.is_scope_loaded(account, symbol) {
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
        self.evidence.mark_scope_loaded(account, symbol);
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

    pub(super) async fn record_and_check_initial_timeline(
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

    pub(super) async fn verify_consistent_terminal_followup(
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

    pub(super) async fn load_verified_conflict_timeline(
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
}
