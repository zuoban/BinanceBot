use super::*;

type InitialResponseObservation = (String, String, String, BinanceOrderResponse);
type IdentityObservationPair = (GridOrder, ObservedOrderIdentity);

#[derive(Clone)]
pub(super) struct InitialOrderResponseBatch {
    pub(super) batch_id: String,
    pub(super) responses: Vec<InitialResponseObservation>,
    pub(super) identities: Vec<IdentityObservationPair>,
}

pub(super) type TerminalDisagreements =
    HashMap<([u8; 32], String, String), Vec<(BinanceOrderResponse, BinanceOrderResponse)>>;

#[derive(Default)]
pub(crate) struct OrderEvidenceBook {
    pub(super) provisional_order_ids: HashSet<([u8; 32], String)>,
    pub(super) order_identity_conflicts: HashMap<([u8; 32], String), GridOrder>,
    pub(super) order_identity_observations: HashMap<([u8; 32], String), Vec<ObservedOrderIdentity>>,
    pub(super) provisional_order_observations:
        HashMap<([u8; 32], String), Vec<ObservedOrderIdentity>>,
    pub(super) loaded_identity_scopes: HashSet<([u8; 32], String)>,
    pub(super) terminal_order_disagreements: TerminalDisagreements,
    pub(super) initial_order_responses: HashMap<([u8; 32], String), Vec<InitialOrderResponseBatch>>,
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
            .entry((account, symbol.to_owned()))
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
        let key = (account, symbol.to_owned());
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
                .entry(key)
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
        let key = (account, symbol.to_owned());
        loop {
            let Some(responses) = self
                .evidence
                .initial_order_responses
                .get(&key)
                .and_then(|batches| batches.first())
                .cloned()
            else {
                return true;
            };
            let save_key = key.clone();
            match self
                .state
                .db
                .run_blocking(move |db| {
                    db.save_initial_order_response_snapshot(
                        save_key.0,
                        &save_key.1,
                        &responses.responses,
                        &responses.identities,
                    )
                })
                .await
            {
                Ok(()) => {
                    if let Some(batches) = self.evidence.initial_order_responses.get_mut(&key) {
                        if !batches.is_empty() {
                            batches.remove(0);
                        }
                        if batches.is_empty() {
                            self.evidence.initial_order_responses.remove(&key);
                        }
                    }
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
            let key = (account, id.clone());
            if self
                .evidence
                .order_identity_conflicts
                .get(&key)
                .is_some_and(|expected| expected.symbol == symbol)
            {
                self.evidence.order_identity_observations.remove(&key);
                self.evidence.order_identity_conflicts.remove(&key);
                self.clear_provisional_order(&key);
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
        let key = (account, symbol.to_owned(), expected_client_id.to_owned());
        let evidence = (first.clone(), conflicting.clone());
        self.evidence
            .terminal_order_disagreements
            .entry(key)
            .or_default()
            .push(evidence);
        self.last_orders_sync = None;
        self.report_reconciliation_state(true).await;
        self.flush_terminal_disagreements(account, symbol).await
    }

    async fn flush_terminal_disagreements(&mut self, account: [u8; 32], symbol: &str) -> bool {
        while let Some((key, evidence)) = self
            .evidence
            .terminal_order_disagreements
            .iter()
            .find(|(key, _)| key.0 == account && key.1 == symbol)
            .and_then(|(key, observations)| {
                observations
                    .first()
                    .map(|evidence| (key.clone(), evidence.clone()))
            })
        {
            let save_key = key.clone();
            if let Err(error) = self
                .state
                .db
                .run_blocking(move |db| {
                    db.save_terminal_order_disagreement(
                        save_key.0,
                        &save_key.1,
                        &save_key.2,
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
            if let Some(observations) = self.evidence.terminal_order_disagreements.get_mut(&key) {
                if !observations.is_empty() {
                    observations.remove(0);
                }
                if observations.is_empty() {
                    self.evidence.terminal_order_disagreements.remove(&key);
                }
            }
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
        let blocked = stored_blocked
            || self
                .evidence
                .terminal_order_disagreements
                .keys()
                .any(|key| key.0 == account && key.1 == symbol);
        if blocked {
            warn!("Terminal order disagreement for {} requires manual investigation; automatic clearing is unavailable", symbol);
            self.last_orders_sync = None;
            self.report_reconciliation_state(true).await;
        }
        blocked
    }

    pub(super) fn insert_identity_observation(
        &mut self,
        key: ([u8; 32], String),
        observed: ObservedOrderIdentity,
    ) {
        let observations = self
            .evidence
            .order_identity_observations
            .entry(key)
            .or_default();
        if !observations.contains(&observed) {
            observations.push(observed);
        }
    }

    pub(super) fn record_identity_conflict_observation(
        &mut self,
        account: [u8; 32],
        expected: &GridOrder,
        observed: ObservedOrderIdentity,
    ) {
        let key = (account, expected.client_order_id.clone());
        self.evidence.provisional_order_ids.insert(key.clone());
        self.evidence
            .order_identity_conflicts
            .insert(key.clone(), expected.clone());
        self.insert_identity_observation(key, observed);
    }

    pub(super) fn record_differing_identity(
        &mut self,
        key: &([u8; 32], String),
        candidate: &GridOrder,
        expected: &GridOrder,
    ) {
        if let Some(observed) = ObservedOrderIdentity::from_order(candidate) {
            if !observed.matches_order(expected) {
                self.insert_identity_observation(key.clone(), observed);
            }
        }
    }

    pub(super) fn promote_provisional_observations(
        &mut self,
        key: &([u8; 32], String),
        expected: &GridOrder,
    ) {
        if let Some(observed) = self.evidence.provisional_order_observations.remove(key) {
            for item in observed {
                if !item.matches_order(expected) {
                    self.insert_identity_observation(key.clone(), item);
                }
            }
        }
    }

    pub(super) fn track_identity_conflict(
        &mut self,
        expected: &GridOrder,
        observed: &BinanceOrderResponse,
    ) {
        self.record_identity_conflict_observation(
            self.client.account_identity_key(),
            expected,
            ObservedOrderIdentity::from_response(observed),
        );
    }

    pub(super) fn track_provisional_observation(&mut self, observed: &BinanceOrderResponse) {
        let key = (
            self.client.account_identity_key(),
            observed.client_order_id.clone(),
        );
        self.evidence.provisional_order_ids.insert(key.clone());
        let observations = self
            .evidence
            .provisional_order_observations
            .entry(key)
            .or_default();
        let identity = ObservedOrderIdentity::from_response(observed);
        if !observations.contains(&identity) {
            observations.push(identity);
        }
    }

    pub(super) fn clear_provisional_order(&mut self, key: &([u8; 32], String)) {
        self.evidence.provisional_order_ids.remove(key);
        self.evidence.provisional_order_observations.remove(key);
    }

    pub(super) fn provisional_order_ids_for(&self, account: [u8; 32]) -> Vec<String> {
        self.evidence
            .provisional_order_ids
            .iter()
            .filter(|(key, _)| *key == account)
            .map(|(_, id)| id.clone())
            .collect()
    }

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
