use super::*;

#[cfg(test)]
pub(super) fn paired_grid_client_order_id(parent_id: &str, side: OrderSide) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(format!("{}:{}", parent_id, side.as_str()).as_bytes());
    let side_code = if side == OrderSide::Buy { "b" } else { "s" };
    format!("gb_{}_{}", side_code, &hex::encode(digest)[..30])
}

#[cfg(test)]
pub(super) fn new_pair_intent(
    parent: &GridOrder,
    side: OrderSide,
    price: Decimal,
    quantity: Decimal,
) -> GridOrder {
    GridOrder {
        client_order_id: paired_grid_client_order_id(&parent.client_order_id, side),
        order_id: None,
        symbol: parent.symbol.clone(),
        side,
        price,
        quantity,
        amount_usdc: price * quantity,
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: parent.grid_level + if side == OrderSide::Sell { 1 } else { -1 },
        paired_client_order_id: Some(parent.client_order_id.clone()),
        is_take_profit: false,
        purpose: OrderPurpose::Grid,
        merge_sources: Vec::new(),
    }
}

pub(super) enum PairPlacementDecision {
    Wait,
    Skip(&'static str),
}

impl GridTradingEngine {
    pub(super) async fn load_pair_intents(
        &self,
        symbol: &str,
        mode: TradingMode,
    ) -> Result<Vec<GridOrder>> {
        let symbol = symbol.to_string();
        self.state
            .db
            .run_blocking(move |db| db.load_pair_intents(&symbol, mode))
            .await
    }

    pub(super) async fn load_pair_intent_map(
        &self,
        symbol: &str,
        mode: TradingMode,
    ) -> Result<HashMap<String, GridOrder>> {
        Ok(self
            .load_pair_intents(symbol, mode)
            .await?
            .into_iter()
            .map(|intent| (intent.client_order_id.clone(), intent))
            .collect())
    }

    pub(super) async fn delete_pair_intent(&self, parent_client_order_id: &str) -> Result<()> {
        let parent_client_order_id = parent_client_order_id.to_string();
        self.state
            .db
            .run_blocking(move |db| db.delete_pair_intent(&parent_client_order_id))
            .await
    }

    pub(super) fn pair_submission_decision(&self) -> PairPlacementDecision {
        if !self.sync_timestamps_fresh() {
            return PairPlacementDecision::Wait;
        }
        PairPlacementDecision::Skip("paired replenishment retired; sliding window owns new orders")
    }

    pub(super) async fn drain_pair_intents(&mut self, symbol: &str, mode: TradingMode) -> bool {
        match self.load_pair_intents(symbol, mode).await {
            Ok(intents) if intents.is_empty() => return true,
            Ok(_) => {}
            Err(error) => {
                error!("Could not load pending pair intents: {}", error);
                return false;
            }
        }
        // Re-read under the gate: intents may change while the writer is acquired.
        let mut guard = self.state.begin_fill_reconciliation().await;
        let result = self.drain_pair_intents_under_gate(symbol, mode).await;
        if result {
            guard.resolve();
        }
        result
    }

    pub(super) async fn drain_pair_intents_under_gate(
        &mut self,
        symbol: &str,
        mode: TradingMode,
    ) -> bool {
        let intents = match self.load_pair_intents(symbol, mode).await {
            Ok(intents) => intents,
            Err(e) => {
                error!("Could not load pending pair intents: {}", e);
                return false;
            }
        };

        for intent in intents {
            let original = intent.clone();
            let parent_id = match intent.paired_client_order_id.clone() {
                Some(id) => id,
                None => {
                    error!("Pair intent {} has no parent", intent.client_order_id);
                    return false;
                }
            };
            let exchange_order = match self
                .client
                .lookup_order_by_client_id(&intent.symbol, &intent.client_order_id)
                .await
            {
                Ok(found) => found,
                Err(e) => {
                    warn!(
                        "Pair order {} remains uncertain: {}",
                        intent.client_order_id, e
                    );
                    return false;
                }
            };
            let conflict = self
                .evidence
                .order_identity_conflicts
                .get(&(
                    self.client.account_identity_key(),
                    intent.client_order_id.clone(),
                ))
                .cloned();
            if let Some(found) = exchange_order {
                if !self
                    .reconcile_found_pair_intent(intent, original, &parent_id, conflict, found)
                    .await
                {
                    return false;
                }
            } else if !self
                .reconcile_absent_pair_intent(&intent, &parent_id, conflict)
                .await
            {
                return false;
            }
        }

        if *self.state.status.read().await != BotStatus::Running {
            return true;
        }
        match self.load_pair_intents(symbol, mode).await {
            Ok(remaining) => remaining.is_empty(),
            Err(e) => {
                error!("Could not verify pending pair intents: {}", e);
                false
            }
        }
    }

    async fn reconcile_found_pair_intent(
        &mut self,
        mut intent: GridOrder,
        mut original: GridOrder,
        parent_id: &str,
        conflict: Option<GridOrder>,
        found: BinanceOrderResponse,
    ) -> bool {
        if unresolved_filled_quantity(&found) {
            self.last_orders_sync = None;
            self.report_reconciliation_state(true).await;
        }
        let active = self
            .state
            .active_orders
            .read()
            .await
            .get(&intent.client_order_id)
            .cloned();
        let mismatch = [
            &intent,
            active.as_ref().unwrap_or(&intent),
            conflict.as_ref().unwrap_or(&intent),
        ]
        .into_iter()
        .find(|expected| !expected.matches_exchange_identity(&found));
        if let Some(expected) = mismatch {
            error!(
                "Pair order {} has mismatched exchange identity",
                intent.client_order_id
            );
            self.track_identity_conflict(expected, &found);
            self.last_orders_sync = None;
            self.report_reconciliation_state(true).await;
            if !self
                .persist_identity_observations(self.client.account_identity_key(), &intent.symbol)
                .await
            {
                self.pause_trading().await;
            }
            return false;
        }
        if conflict
            .as_ref()
            .is_some_and(|order| order.order_id.is_none())
        {
            self.last_orders_sync = None;
            self.report_reconciliation_state(true).await;
            return false;
        }
        intent.order_id = Some(found.order_id);
        original.order_id = Some(found.order_id);
        original.reserve_quantity(found.orig_qty);
        if unresolved_filled_quantity(&found) {
            self.invalidate_account_and_position_caches().await;
            self.state
                .active_orders
                .write()
                .await
                .entry(original.client_order_id.clone())
                .or_insert(original.clone());
            if let Err(error) = self.save_managed_order(&original).await {
                error!(
                    "Could not persist uncertain pair order {}: {}",
                    original.client_order_id, error
                );
                self.pause_trading().await;
            }
            return false;
        }
        if found.is_terminal() {
            if found.executed_qty > Decimal::ZERO {
                intent.apply_terminal_fill(&found, exchange_fill_time(&found));
                if !self.on_order_filled(&mut intent).await {
                    return false;
                }
                // A reused destination may have been journaled by normal
                // reconciliation already. Retire its parent intent even
                // when the fill callback takes the idempotent path.
                if let Err(error) = self.delete_pair_intent(parent_id).await {
                    error!("Could not retire filled counter-order intent: {}", error);
                    return false;
                }
            } else {
                if let Err(e) = self
                    .remove_managed_and_active_order(&intent.client_order_id)
                    .await
                {
                    error!("Could not clear closed pair order: {}", e);
                    return false;
                }
                if let Err(e) = self.delete_pair_intent(parent_id).await {
                    error!("Could not clear closed pair intent: {}", e);
                    return false;
                }
            }
            return true;
        }
        if !self.prepare_partial_adoption(&mut intent, &found).await {
            return false;
        }
        if !self
            .adopt_open_exchange_order(&mut intent, original, &found)
            .await
        {
            return false;
        }
        if let Err(e) = self.delete_pair_intent(parent_id).await {
            error!("Could not clear submitted pair intent: {}", e);
            self.pause_trading().await;
            return false;
        }
        true
    }

    async fn reconcile_absent_pair_intent(
        &mut self,
        intent: &GridOrder,
        parent_id: &str,
        conflict: Option<GridOrder>,
    ) -> bool {
        if conflict.is_some() {
            self.last_orders_sync = None;
            self.report_reconciliation_state(true).await;
            return false;
        }
        if *self.state.status.read().await != BotStatus::Running {
            return true;
        }
        match self.pair_submission_decision() {
            PairPlacementDecision::Wait => false,
            PairPlacementDecision::Skip(reason) => {
                if let Err(e) = self
                    .remove_managed_and_active_order(&intent.client_order_id)
                    .await
                {
                    error!("Could not clear skipped pair order: {}", e);
                    return false;
                }
                if let Err(e) = self.delete_pair_intent(parent_id).await {
                    error!("Could not clear skipped pair intent: {}", e);
                    return false;
                }
                self.state
                    .add_log(
                        "WARN",
                        format!("Pair order {} skipped: {}", intent.client_order_id, reason),
                    )
                    .await;
                true
            }
        }
    }

    pub(super) async fn resolve_paired_cycle_fill(
        &mut self,
        config: &crate::config::AppConfig,
        order: &GridOrder,
        mode: TradingMode,
    ) -> Option<(Decimal, bool, String)> {
        let mut cycle_profit = Decimal::ZERO;
        let mut is_completed_cycle = false;
        let mut note = format!(
            "Grid {} filled at {} (Qty: {})",
            order.side.as_str(),
            order.price,
            order.quantity
        );
        // Legacy paired cycles retain their spread estimate in either direction.
        if order.side == config.grid.direction.closing_side() {
            if let Some(paired_id) = &order.paired_client_order_id {
                let paired_id = paired_id.clone();
                let symbol = order.symbol.clone();
                let opening = match self
                    .state
                    .db
                    .run_blocking(move |db| db.get_pair_opening(&symbol, &paired_id, mode))
                    .await
                {
                    Ok(opening) => opening,
                    Err(error) => {
                        error!("Could not load paired opening: {}", error);
                        self.state
                            .active_orders
                            .write()
                            .await
                            .entry(order.client_order_id.clone())
                            .or_insert_with(|| order.clone());
                        self.pause_trading().await;
                        return None;
                    }
                };
                if let Some((_, price, quantity)) =
                    opening.filter(|(side, _, _)| *side == config.grid.direction.opening_side())
                {
                    cycle_profit = (order.price - price)
                        * config.grid.direction.sign()
                        * order.quantity.min(quantity);
                    is_completed_cycle = true;
                    note = format!(
                        "Completed Grid Cycle! {} at {} (Paired Entry: {}, Spread Estimate: {} USDC)",
                        order.side.as_str(), order.price, price, cycle_profit
                    );
                }
            }
        }
        Some((cycle_profit, is_completed_cycle, note))
    }

    pub(super) async fn has_pending_live_pair_intents(&mut self, symbol: &str) -> bool {
        match self.load_pair_intents(symbol, TradingMode::Live).await {
            Ok(intents) => !intents.is_empty(),
            Err(e) => {
                error!("Could not inspect pending pair intents: {}", e);
                self.pause_trading().await;
                true
            }
        }
    }
}
