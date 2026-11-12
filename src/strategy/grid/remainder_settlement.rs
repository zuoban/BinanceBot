use super::snapshot_identities::invalid_snapshot_price;
use super::*;

impl GridTradingEngine {
    /// Also used before draining paired exits: an uncertain accepted target must
    /// reserve its inventory even when absent from an open-orders snapshot.
    pub(super) async fn adopt_remainder_target(
        &mut self,
        plan: &mut RemainderPlan,
    ) -> Result<bool> {
        let mut guard = self.state.begin_fill_reconciliation().await;
        let result = self.adopt_remainder_target_under_gate(plan).await;
        if result.is_ok() {
            guard.resolve();
        }
        result
    }

    pub(super) async fn adopt_remainder_target_under_gate(
        &mut self,
        plan: &mut RemainderPlan,
    ) -> Result<bool> {
        let Some(found) = self
            .client
            .lookup_order_by_client_id(&plan.symbol, &plan.target.client_order_id)
            .await?
        else {
            return Ok(false);
        };
        self.settle_remainder_order(plan.target.clone(), found, Some(&mut *plan))
            .await?;
        plan.phase = RemainderPhase::Complete;
        self.persist_remainder_plan(plan).await?;
        self.last_remainder_change = Some(Instant::now());
        Ok(true)
    }

    fn select_trusted_remainder_order(
        &self,
        account: [u8; 32],
        order: &GridOrder,
        active: Option<GridOrder>,
        found_order_id: i64,
    ) -> GridOrder {
        let conflict = self.evidence.get_conflict(account, &order.client_order_id);
        conflict
            .filter(|item| item.order_id.is_some_and(|id| id != found_order_id))
            .cloned()
            .or_else(|| {
                order
                    .order_id
                    .filter(|id| *id != found_order_id)
                    .map(|_| order.clone())
            })
            .or_else(|| {
                active
                    .as_ref()
                    .filter(|item| item.order_id.is_some_and(|id| id != found_order_id))
                    .cloned()
            })
            .or_else(|| conflict.filter(|item| item.order_id.is_some()).cloned())
            .or_else(|| active.filter(|item| item.order_id.is_some()))
            .unwrap_or_else(|| order.clone())
    }

    async fn confirm_remainder_identity(
        &mut self,
        order: &GridOrder,
        found: &BinanceOrderResponse,
        plan: Option<&mut RemainderPlan>,
    ) -> Result<()> {
        let active = self
            .state
            .active_orders
            .read()
            .await
            .get(&order.client_order_id)
            .cloned();
        let account = self.client.account_identity_key();
        let trusted = self.select_trusted_remainder_order(account, order, active, found.order_id);
        if !trusted.matches_exchange_identity(found) || invalid_snapshot_price(found) {
            self.block_reconciliation_and_invalidate_caches().await;
            if !trusted.matches_exchange_identity(found) {
                self.track_identity_conflict(&trusted, found);
            }
            self.state
                .active_orders
                .write()
                .await
                .insert(trusted.client_order_id.clone(), trusted.clone());
            if !self
                .persist_identity_observations(account, &trusted.symbol)
                .await
            {
                self.pause_trading().await;
            }
            return Err(anyhow!(
                "Remainder exchange identity or price validation failed"
            ));
        }
        if self
            .evidence
            .has_observations(account, &order.client_order_id)
            && !self
                .persist_identity_observations(account, &order.symbol)
                .await
        {
            return Err(anyhow!("Could not persist remainder identity observations"));
        }
        if let Some(plan) = plan {
            plan.target.order_id = Some(found.order_id);
            if let Err(error) = self.persist_remainder_plan(plan).await {
                let mut confirmed = order.clone();
                confirmed.order_id = Some(found.order_id);
                self.track_identity_conflict(&confirmed, found);
                self.state
                    .active_orders
                    .write()
                    .await
                    .insert(confirmed.client_order_id.clone(), confirmed);
                self.block_reconciliation_and_invalidate_caches().await;
                self.persist_identity_observations(account, &order.symbol)
                    .await;
                return Err(error);
            }
        }
        Ok(())
    }

    pub(super) async fn settle_remainder_order(
        &mut self,
        mut order: GridOrder,
        found: BinanceOrderResponse,
        plan: Option<&mut RemainderPlan>,
    ) -> Result<()> {
        let inconsistent_fill = unresolved_filled_quantity(&found);
        if inconsistent_fill {
            self.block_reconciliation_and_invalidate_caches().await;
        }
        self.confirm_remainder_identity(&order, &found, plan)
            .await?;
        order.order_id = Some(found.order_id);
        if inconsistent_fill {
            order.reserve_quantity(found.orig_qty);
            self.retain_uncertain_active_order(&order).await?;
            return Err(anyhow!("Remainder fill quantity remains unresolved"));
        }
        if found.is_terminal() {
            if found.executed_qty > Decimal::ZERO {
                order.apply_terminal_fill(&found, exchange_fill_time(&found));
                if !self.on_order_filled(&mut order).await {
                    return Err(anyhow!("Could not record remainder fill"));
                }
            } else {
                self.remove_managed_and_active_order(&order.client_order_id)
                    .await?;
            }
        } else {
            let mut original = order.clone();
            original.reserve_quantity(found.orig_qty);
            order.price = found.price;
            if !self
                .prepare_and_adopt_open_order(&mut order, original, &found)
                .await
            {
                return Err(anyhow!("Could not persist remainder order"));
            }
        }
        Ok(())
    }

    pub(super) async fn cancel_and_settle_remainder_order(
        &mut self,
        mut order: GridOrder,
        mut found: BinanceOrderResponse,
        mut plan: Option<&mut RemainderPlan>,
    ) -> Result<()> {
        self.confirm_remainder_identity(&order, &found, plan.as_deref_mut())
            .await?;
        order.order_id = Some(found.order_id);
        if !found.is_terminal() {
            let canceled = self
                .client
                .cancel_order(&order.symbol, None, Some(&order.client_order_id))
                .await?;
            found = if canceled.is_terminal() {
                canceled
            } else {
                self.client
                    .get_order_by_client_id(&order.symbol, &order.client_order_id)
                    .await?
            };
        }
        if !found.is_terminal() {
            return Err(anyhow!("Remainder order cancellation is not terminal"));
        }
        self.settle_remainder_order(order, found, plan).await
    }

    /// Called by cancel-all while trading is paused; resolves a possibly accepted
    /// target that has not yet appeared in the open-orders snapshot.
    pub(super) async fn cancel_remainder_plan(&mut self) -> Result<()> {
        let Some(mut plan) = self.load_remainder_plan().await? else {
            return Ok(());
        };
        if plan.mode != TradingMode::Paper && plan.phase == RemainderPhase::Submitting {
            if let Some(found) = self
                .client
                .lookup_order_by_client_id(&plan.symbol, &plan.target.client_order_id)
                .await?
            {
                self.cancel_and_settle_remainder_order(plan.target.clone(), found, Some(&mut plan))
                    .await?;
            }
        }
        plan.phase = RemainderPhase::Abandoned;
        self.persist_remainder_plan(&plan).await
    }
}
