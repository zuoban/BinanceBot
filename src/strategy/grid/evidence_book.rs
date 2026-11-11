use super::*;

pub(super) type InitialResponseObservation = (String, String, String, BinanceOrderResponse);
pub(super) type IdentityObservationPair = (GridOrder, ObservedOrderIdentity);

#[derive(Clone)]
pub(super) struct InitialOrderResponseBatch {
    pub(super) batch_id: String,
    pub(super) responses: Vec<InitialResponseObservation>,
    pub(super) identities: Vec<IdentityObservationPair>,
}

pub(super) type DisagreementPair = (BinanceOrderResponse, BinanceOrderResponse);
pub(super) type SymbolDisagreements = BTreeMap<String, Vec<DisagreementPair>>;

#[derive(Default)]
pub(crate) struct OrderEvidenceBook {
    pub(super) provisional_order_ids: HashMap<[u8; 32], BTreeSet<String>>,
    pub(super) order_identity_conflicts: HashMap<[u8; 32], BTreeMap<String, GridOrder>>,
    pub(super) order_identity_observations:
        HashMap<[u8; 32], BTreeMap<String, Vec<ObservedOrderIdentity>>>,
    pub(super) provisional_order_observations:
        HashMap<[u8; 32], BTreeMap<String, Vec<ObservedOrderIdentity>>>,
    pub(super) loaded_identity_scopes: HashMap<[u8; 32], HashSet<String>>,
    pub(super) terminal_order_disagreements:
        HashMap<[u8; 32], BTreeMap<String, SymbolDisagreements>>,
    pub(super) initial_order_responses:
        HashMap<[u8; 32], HashMap<String, Vec<InitialOrderResponseBatch>>>,
}

impl OrderEvidenceBook {
    pub(super) fn has_conflict(&self, account: [u8; 32], id: &str) -> bool {
        self.order_identity_conflicts
            .get(&account)
            .is_some_and(|orders| orders.contains_key(id))
    }

    pub(super) fn get_conflict(&self, account: [u8; 32], id: &str) -> Option<&GridOrder> {
        self.order_identity_conflicts
            .get(&account)
            .and_then(|orders| orders.get(id))
    }

    pub(super) fn insert_conflict(&mut self, account: [u8; 32], order: GridOrder) {
        self.order_identity_conflicts
            .entry(account)
            .or_default()
            .insert(order.client_order_id.clone(), order);
    }

    pub(super) fn remove_conflict(&mut self, account: [u8; 32], id: &str) {
        if let Some(orders) = self.order_identity_conflicts.get_mut(&account) {
            orders.remove(id);
            if orders.is_empty() {
                self.order_identity_conflicts.remove(&account);
            }
        }
    }

    pub(super) fn has_conflicts_for_scope(&self, account: [u8; 32], symbol: &str) -> bool {
        self.order_identity_conflicts
            .get(&account)
            .is_some_and(|orders| orders.values().any(|order| order.symbol == symbol))
    }

    pub(super) fn conflicts_for_scope(&self, account: [u8; 32], symbol: &str) -> Vec<GridOrder> {
        self.order_identity_conflicts
            .get(&account)
            .map(|orders| {
                orders
                    .values()
                    .filter(|order| order.symbol == symbol)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(super) fn is_provisional(&self, account: [u8; 32], id: &str) -> bool {
        self.provisional_order_ids
            .get(&account)
            .is_some_and(|ids| ids.contains(id))
    }

    pub(super) fn mark_provisional(&mut self, account: [u8; 32], id: &str) {
        if !self.is_provisional(account, id) {
            self.provisional_order_ids
                .entry(account)
                .or_default()
                .insert(id.to_owned());
        }
    }

    pub(super) fn remove_provisional_id(&mut self, account: [u8; 32], id: &str) {
        if let Some(ids) = self.provisional_order_ids.get_mut(&account) {
            ids.remove(id);
            if ids.is_empty() {
                self.provisional_order_ids.remove(&account);
            }
        }
    }

    pub(super) fn clear_provisional(&mut self, account: [u8; 32], id: &str) {
        self.remove_provisional_id(account, id);
        if let Some(obs) = self.provisional_order_observations.get_mut(&account) {
            obs.remove(id);
            if obs.is_empty() {
                self.provisional_order_observations.remove(&account);
            }
        }
    }

    pub(super) fn provisional_order_ids_for(&self, account: [u8; 32]) -> Vec<String> {
        self.provisional_order_ids
            .get(&account)
            .map(|ids| ids.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub(super) fn is_scope_loaded(&self, account: [u8; 32], symbol: &str) -> bool {
        self.loaded_identity_scopes
            .get(&account)
            .is_some_and(|scopes| scopes.contains(symbol))
    }

    pub(super) fn mark_scope_loaded(&mut self, account: [u8; 32], symbol: &str) {
        if !self.is_scope_loaded(account, symbol) {
            self.loaded_identity_scopes
                .entry(account)
                .or_default()
                .insert(symbol.to_owned());
        }
    }

    pub(super) fn has_observations(&self, account: [u8; 32], id: &str) -> bool {
        self.order_identity_observations
            .get(&account)
            .is_some_and(|obs| obs.contains_key(id))
    }

    pub(super) fn get_observations(
        &self,
        account: [u8; 32],
        id: &str,
    ) -> Option<&[ObservedOrderIdentity]> {
        self.order_identity_observations
            .get(&account)
            .and_then(|obs| obs.get(id))
            .map(Vec::as_slice)
    }

    pub(super) fn insert_observation(
        &mut self,
        account: [u8; 32],
        id: &str,
        observed: ObservedOrderIdentity,
    ) {
        let account_obs = self.order_identity_observations.entry(account).or_default();
        if let Some(items) = account_obs.get_mut(id) {
            if !items.contains(&observed) {
                items.push(observed);
            }
        } else {
            account_obs.insert(id.to_owned(), vec![observed]);
        }
    }

    pub(super) fn remove_observations(&mut self, account: [u8; 32], id: &str) {
        if let Some(obs) = self.order_identity_observations.get_mut(&account) {
            obs.remove(id);
            if obs.is_empty() {
                self.order_identity_observations.remove(&account);
            }
        }
    }

    pub(super) fn has_pending_initial_responses(&self, account: [u8; 32], symbol: &str) -> bool {
        self.initial_order_responses
            .get(&account)
            .and_then(|symbols| symbols.get(symbol))
            .is_some_and(|batches| !batches.is_empty())
    }

    pub(super) fn first_initial_response_batch(
        &self,
        account: [u8; 32],
        symbol: &str,
    ) -> Option<InitialOrderResponseBatch> {
        self.initial_order_responses
            .get(&account)
            .and_then(|symbols| symbols.get(symbol))
            .and_then(|batches| batches.first())
            .cloned()
    }

    pub(super) fn pop_initial_response_batch(&mut self, account: [u8; 32], symbol: &str) {
        if let Some(symbols) = self.initial_order_responses.get_mut(&account) {
            if let Some(batches) = symbols.get_mut(symbol) {
                if !batches.is_empty() {
                    batches.remove(0);
                }
                if batches.is_empty() {
                    symbols.remove(symbol);
                }
            }
            if symbols.is_empty() {
                self.initial_order_responses.remove(&account);
            }
        }
    }

    pub(super) fn has_terminal_disagreements(&self, account: [u8; 32], symbol: &str) -> bool {
        self.terminal_order_disagreements
            .get(&account)
            .and_then(|symbols| symbols.get(symbol))
            .is_some_and(|orders| !orders.is_empty())
    }

    pub(super) fn first_terminal_disagreement(
        &self,
        account: [u8; 32],
        symbol: &str,
    ) -> Option<(String, DisagreementPair)> {
        self.terminal_order_disagreements
            .get(&account)
            .and_then(|symbols| symbols.get(symbol))
            .and_then(|orders| {
                orders
                    .iter()
                    .find_map(|(id, list)| list.first().map(|pair| (id.clone(), pair.clone())))
            })
    }

    pub(super) fn pop_terminal_disagreement(
        &mut self,
        account: [u8; 32],
        symbol: &str,
        client_id: &str,
    ) {
        if let Some(symbols) = self.terminal_order_disagreements.get_mut(&account) {
            if let Some(orders) = symbols.get_mut(symbol) {
                if let Some(list) = orders.get_mut(client_id) {
                    if !list.is_empty() {
                        list.remove(0);
                    }
                    if list.is_empty() {
                        orders.remove(client_id);
                    }
                }
                if orders.is_empty() {
                    symbols.remove(symbol);
                }
            }
            if symbols.is_empty() {
                self.terminal_order_disagreements.remove(&account);
            }
        }
    }
}

fn non_terminal_responses_disagree(a: &BinanceOrderResponse, b: &BinanceOrderResponse) -> bool {
    if !matches!(a.status.as_str(), "NEW" | "PARTIALLY_FILLED")
        || !matches!(b.status.as_str(), "NEW" | "PARTIALLY_FILLED")
    {
        return false;
    }
    b.executed_qty < a.executed_qty
        || (a.status == "PARTIALLY_FILLED" && b.status == "NEW")
        || a.update_time
            .zip(b.update_time)
            .is_some_and(|(first, second)| second < first)
        || a.orig_qty != b.orig_qty
        || (a.executed_qty > Decimal::ZERO
            && a.executed_qty == b.executed_qty
            && a.avg_price.is_some_and(|first| {
                first > Decimal::ZERO && b.avg_price.is_none_or(|second| second != first)
            }))
}

fn open_to_terminal_disagrees(a: &BinanceOrderResponse, b: &BinanceOrderResponse) -> bool {
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
    !(valid_start && valid_end && stable_average && a.orig_qty == b.orig_qty && ordered_time)
}

fn terminal_snapshots_disagree(a: &BinanceOrderResponse, b: &BinanceOrderResponse) -> bool {
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
        return non_terminal_responses_disagree(a, b);
    }
    if matches!(a.status.as_str(), "NEW" | "PARTIALLY_FILLED") && b.is_terminal() {
        return open_to_terminal_disagrees(a, b);
    }
    terminal_snapshots_disagree(a, b)
}

impl GridTradingEngine {
    pub(super) fn insert_identity_observation(
        &mut self,
        account: [u8; 32],
        id: &str,
        observed: ObservedOrderIdentity,
    ) {
        self.evidence.insert_observation(account, id, observed);
    }

    pub(super) fn record_identity_conflict_observation(
        &mut self,
        account: [u8; 32],
        expected: &GridOrder,
        observed: ObservedOrderIdentity,
    ) {
        self.evidence
            .mark_provisional(account, &expected.client_order_id);
        self.evidence.insert_conflict(account, expected.clone());
        self.evidence
            .insert_observation(account, &expected.client_order_id, observed);
    }

    pub(super) fn record_differing_identity(
        &mut self,
        account: [u8; 32],
        id: &str,
        candidate: &GridOrder,
        expected: &GridOrder,
    ) {
        if let Some(observed) = ObservedOrderIdentity::from_order(candidate) {
            if !observed.matches_order(expected) {
                self.evidence.insert_observation(account, id, observed);
            }
        }
    }

    pub(super) fn promote_provisional_observations(
        &mut self,
        account: [u8; 32],
        id: &str,
        expected: &GridOrder,
    ) {
        let mut empty_account = false;
        let removed = self
            .evidence
            .provisional_order_observations
            .get_mut(&account)
            .and_then(|obs| {
                let items = obs.remove(id);
                empty_account = obs.is_empty();
                items
            });
        if empty_account {
            self.evidence
                .provisional_order_observations
                .remove(&account);
        }
        if let Some(observed) = removed {
            for item in observed {
                if !item.matches_order(expected) {
                    self.evidence.insert_observation(account, id, item);
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
        let account = self.client.account_identity_key();
        self.evidence
            .mark_provisional(account, &observed.client_order_id);
        let account_obs = self
            .evidence
            .provisional_order_observations
            .entry(account)
            .or_default();
        let identity = ObservedOrderIdentity::from_response(observed);
        if let Some(observations) = account_obs.get_mut(&observed.client_order_id) {
            if !observations.contains(&identity) {
                observations.push(identity);
            }
        } else {
            account_obs.insert(observed.client_order_id.clone(), vec![identity]);
        }
    }

    pub(super) fn collect_identity_observations(
        &self,
        account: [u8; 32],
        symbol: &str,
    ) -> Option<Vec<(GridOrder, ObservedOrderIdentity)>> {
        let Some(account_obs) = self.evidence.order_identity_observations.get(&account) else {
            return Some(Vec::new());
        };
        let mut records = Vec::new();
        for (id, observations) in account_obs {
            let expected = self.evidence.get_conflict(account, id)?;
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
}
