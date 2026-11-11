use super::*;

impl GridTradingEngine {
    pub(super) async fn lookup_order(
        &self,
        order: &GridOrder,
    ) -> Result<Option<BinanceOrderResponse>> {
        match order.order_id {
            Some(id) => self.client.get_order(&order.symbol, id).await.map(Some),
            None => {
                self.client
                    .lookup_order_by_client_id(&order.symbol, &order.client_order_id)
                    .await
            }
        }
    }

    pub(super) async fn insert_paper_order(&self, order: &mut GridOrder) -> bool {
        let mut active_orders = self.state.active_orders.write().await;
        if self.state.is_shutting_down() {
            return false;
        }
        order.order_id = Some(rand::random::<i64>().saturating_abs());
        active_orders.insert(order.client_order_id.clone(), order.clone());
        true
    }

    pub(super) async fn load_managed_orders(&self, symbol: &str) -> Result<Vec<GridOrder>> {
        let symbol = symbol.to_string();
        self.state
            .db
            .run_blocking(move |db| db.load_managed_orders(&symbol))
            .await
    }

    pub(super) async fn insert_restored_managed_orders(&self, restored: Vec<GridOrder>) {
        let mut active = self.state.active_orders.write().await;
        for order in restored
            .into_iter()
            .filter(|order| is_grid_order(&order.client_order_id))
        {
            active.entry(order.client_order_id.clone()).or_insert(order);
        }
    }

    pub(super) async fn save_managed_order(&self, order: &GridOrder) -> Result<()> {
        let order = order.clone();
        self.state
            .db
            .run_blocking(move |db| db.save_managed_order(&order))
            .await
    }

    pub(super) async fn save_managed_orders(&self, orders: Vec<GridOrder>) -> Result<()> {
        self.state
            .db
            .run_blocking(move |db| db.save_managed_orders(&orders))
            .await
    }

    pub(super) async fn has_recorded_order(
        &self,
        client_order_id: &str,
        mode: TradingMode,
    ) -> Result<bool> {
        let client_order_id = client_order_id.to_string();
        self.state
            .db
            .run_blocking(move |db| db.has_recorded_order(&client_order_id, mode))
            .await
    }

    pub(super) async fn delete_managed_order(&self, client_order_id: &str) -> Result<()> {
        let client_order_id = client_order_id.to_string();
        self.state
            .db
            .run_blocking(move |db| db.delete_managed_order(&client_order_id))
            .await
    }

    pub(super) async fn remove_managed_and_active_order(
        &self,
        client_order_id: &str,
    ) -> Result<()> {
        self.delete_managed_order(client_order_id).await?;
        self.state
            .active_orders
            .write()
            .await
            .remove(client_order_id);
        Ok(())
    }

    // Direct order lookups can observe a fill absent from the open-orders
    // snapshot. Preserve the old reservation until both risk inputs are fresh.
    pub(super) async fn prepare_partial_adoption(
        &mut self,
        order: &mut GridOrder,
        found: &BinanceOrderResponse,
    ) -> bool {
        if found.executed_qty <= Decimal::ZERO {
            return true;
        }
        let remaining = found.remaining_qty();
        let previous = self
            .state
            .active_orders
            .read()
            .await
            .get(&order.client_order_id)
            .cloned();
        if self
            .last_account_sync
            .is_some_and(|at| at.elapsed() < self.sync_freshness())
            && previous.as_ref().is_some_and(|old| {
                old.status == OrderStatus::PartiallyFilled && old.quantity == remaining
            })
        {
            return true;
        }
        let pause_epoch = self.state.pause_epoch();
        self.invalidate_account_and_position_caches();
        if self.refresh_position_then_account(false).await
            && self.state.pause_epoch() == pause_epoch
        {
            return true;
        }

        // An unknown accepted order must remain durable and visible to cancel
        // and reconciliation even when the exchange cannot refresh risk data.
        let quantity = previous
            .as_ref()
            .map_or(found.orig_qty, |old| old.quantity.max(remaining));
        let mut reserved = previous.unwrap_or_else(|| order.clone());
        reserved.order_id = Some(found.order_id);
        reserved.quantity = quantity;
        reserved.amount_usdc = reserved
            .price
            .checked_mul(reserved.quantity)
            .unwrap_or(Decimal::MAX);
        reserved.status = OrderStatus::PartiallyFilled;
        self.state
            .active_orders
            .write()
            .await
            .insert(reserved.client_order_id.clone(), reserved.clone());
        if let Err(error) = self.save_managed_order(&reserved).await {
            error!(
                "Could not persist partial order {}: {}",
                reserved.client_order_id, error
            );
            self.pause_trading().await;
        }
        self.mark_orders_reconciliation_blocked().await;
        false
    }

    pub(super) async fn adopt_open_exchange_order(
        &mut self,
        order: &mut GridOrder,
        original: GridOrder,
        found: &BinanceOrderResponse,
    ) -> bool {
        order.quantity = found.remaining_qty();
        order.amount_usdc = order
            .price
            .checked_mul(order.quantity)
            .unwrap_or(Decimal::MAX);
        order.status = found.open_order_status();
        if let Err(error) = self.save_managed_order(order).await {
            error!(
                "Could not persist adopted order {}: {}",
                order.client_order_id, error
            );
            self.state
                .active_orders
                .write()
                .await
                .entry(original.client_order_id.clone())
                .or_insert(original);
            self.mark_orders_reconciliation_blocked().await;
            self.pause_trading().await;
            return false;
        }
        self.state
            .active_orders
            .write()
            .await
            .insert(order.client_order_id.clone(), order.clone());
        if found.executed_qty > Decimal::ZERO {
            self.state.check_global_position_limit().await;
        }
        true
    }

    pub(super) async fn prepare_and_adopt_open_order(
        &mut self,
        order: &mut GridOrder,
        original: GridOrder,
        found: &BinanceOrderResponse,
    ) -> bool {
        self.prepare_partial_adoption(order, found).await
            && self.adopt_open_exchange_order(order, original, found).await
    }

    pub(super) async fn retain_uncertain_active_order(&self, order: &GridOrder) -> Result<()> {
        self.state
            .active_orders
            .write()
            .await
            .entry(order.client_order_id.clone())
            .or_insert_with(|| order.clone());
        if let Err(error) = self.save_managed_order(order).await {
            self.pause_trading().await;
            return Err(error);
        }
        Ok(())
    }

    /// Executed when an order is confirmed filled outside a batched observation.
    pub(super) async fn on_order_filled(&mut self, order: &mut GridOrder) -> bool {
        if !self.state.config.read().await.exchange.is_paper() {
            order.status = OrderStatus::Filled;
            self.invalidate_account_and_position_caches();
            if !self.sync_account_and_position().await {
                let missing = {
                    let mut active = self.state.active_orders.write().await;
                    if active.contains_key(&order.client_order_id) {
                        false
                    } else {
                        active.insert(order.client_order_id.clone(), order.clone());
                        true
                    }
                };
                if missing {
                    if let Err(error) = self.save_managed_order(order).await {
                        error!(%error, "Could not persist unverified terminal order");
                        self.pause_trading().await;
                    }
                }
                return false;
            }
        }
        self.commit_order_fill(order).await
    }

    async fn retire_already_recorded_fill(
        &self,
        order: &GridOrder,
        is_paper: bool,
        mode: TradingMode,
    ) -> Option<bool> {
        let recorded_id = order.client_order_id.clone();
        let recorded = if is_paper {
            self.state.has_recorded_paper_order(recorded_id, mode).await
        } else {
            self.has_recorded_order(&recorded_id, mode).await
        };
        match recorded {
            Ok(true) => {
                // Retire durable recovery metadata before releasing its reservation.
                if !is_paper {
                    if let Err(e) = self.delete_managed_order(&order.client_order_id).await {
                        error!("Could not clear already-recorded order: {}", e);
                        return Some(false);
                    }
                }
                self.state
                    .active_orders
                    .write()
                    .await
                    .remove(&order.client_order_id);
                Some(true)
            }
            Ok(false) => None,
            Err(e) => {
                error!(
                    "Could not check recorded fill {}: {}",
                    order.client_order_id, e
                );
                self.pause_trading().await;
                Some(false)
            }
        }
    }

    async fn simulate_paper_fill(
        &self,
        order: &GridOrder,
    ) -> (Decimal, (PositionInfo, AccountInfo)) {
        let mut pos = self.state.position.read().await.clone();
        let (size, entry_price, simulated_pnl) = crate::types::apply_fill_to_position(
            pos.size,
            pos.entry_price,
            order.side,
            order.price,
            order.quantity,
        );
        pos.size = size;
        pos.entry_price = entry_price;
        let ticker = self.state.ticker.read().await.clone();
        let mut acc = self.state.account.read().await.clone();
        acc.total_wallet_balance = acc
            .total_wallet_balance
            .checked_add(simulated_pnl)
            .unwrap_or(acc.total_wallet_balance);
        update_paper_account(&mut pos, &mut acc, &ticker);
        (simulated_pnl, (pos, acc))
    }

    /// Risk refresh and its global check must precede this durable commit.
    pub(super) async fn commit_order_fill(&mut self, order: &mut GridOrder) -> bool {
        order.status = OrderStatus::Filled;
        let config = self.state.config.read().await.clone();
        let is_paper = config.exchange.is_paper();
        if is_paper {
            order.updated_at = Utc::now();
        }
        let mode = config.exchange.trading_mode();
        if let Some(outcome) = self
            .retire_already_recorded_fill(order, is_paper, mode)
            .await
        {
            return outcome;
        }
        let Some((cycle_profit, is_completed_cycle, note)) =
            self.resolve_paired_cycle_fill(&config, order, mode).await
        else {
            return false;
        };
        let (realized_pnl, pnl_verified, paper_state) = if is_paper {
            let (simulated_pnl, state) = self.simulate_paper_fill(order).await;
            (simulated_pnl, true, Some(state))
        } else {
            // Unknown PnL stays explicitly pending until complete exchange fills
            // are verified. Pagination must never hold the global risk barrier.
            (Decimal::ZERO, false, None)
        };
        let trade = TradeRecord {
            trade_id: format!("grid:{}", order.client_order_id),
            client_order_id: order.client_order_id.clone(),
            symbol: order.symbol.clone(),
            mode,
            side: order.side,
            price: order.price,
            quantity: order.quantity,
            amount_usdc: order
                .price
                .checked_mul(order.quantity)
                .unwrap_or(order.amount_usdc),
            realized_pnl,
            commission: Decimal::ZERO,
            pnl_verified,
            is_maker: true,
            timestamp: order.updated_at,
            note: note.clone(),
        };
        let recorded = self
            .state
            .record_trade_with_paper_state(
                trade,
                is_completed_cycle.then_some(cycle_profit),
                None,
                order.paired_client_order_id.clone(),
                paper_state,
            )
            .await;
        if !recorded {
            self.pause_trading().await;
            self.state
                .active_orders
                .write()
                .await
                .entry(order.client_order_id.clone())
                .or_insert_with(|| order.clone());
            return false;
        }
        self.finalize_committed_fill(order, is_paper, is_completed_cycle, note)
            .await;
        true
    }

    async fn finalize_committed_fill(
        &mut self,
        order: &GridOrder,
        is_paper: bool,
        is_completed_cycle: bool,
        note: String,
    ) {
        if !is_paper {
            self.state
                .active_orders
                .write()
                .await
                .remove(&order.client_order_id);
            if let Some(order_id) = order.order_id {
                if self.pnl_order_ids.len() >= 4096 {
                    self.pnl_order_ids.clear();
                }
                self.pnl_order_ids
                    .insert(order.client_order_id.clone(), order_id);
            }
        }
        self.state
            .add_log(
                if is_completed_cycle {
                    "SUCCESS"
                } else {
                    "INFO"
                },
                note,
            )
            .await;
    }
}
