use super::*;

#[derive(PartialEq)]
pub(super) struct PaperWindowInputs {
    pub(super) symbol: String,
    grid: crate::config::GridConfig,
    rules: SymbolRules,
    reference_band: (Decimal, Decimal),
    maker_band: (Decimal, Decimal),
    position: (Decimal, Decimal, Decimal, u32),
    pub(super) trade_revision: u64,
    pause_epoch: u64,
}

pub(super) struct PaperWindowCache {
    inputs: PaperWindowInputs,
    orders: HashMap<String, GridOrder>,
}

impl GridTradingEngine {
    pub(super) async fn capture_paper_window_inputs(
        &self,
        config: &crate::config::AppConfig,
    ) -> Option<PaperWindowInputs> {
        if !config.exchange.is_paper()
            || self
                .last_remainder_change
                .is_some_and(|at| at.elapsed() < Duration::from_secs(30))
        {
            return None;
        }
        let ticker = self.state.ticker.read().await;
        let position = self.state.position.read().await;
        let price_band = |price: Decimal| {
            // Exposure and liquidation thresholds can move inside a grid cell.
            if config.grid.max_position_usdc.is_some()
                || config.grid.min_liquidation_distance_pct.is_some()
            {
                (price, price)
            } else {
                price
                    .checked_div(config.grid.grid_interval)
                    .map(|steps| (steps.floor(), steps.ceil()))
                    .unwrap_or((price, price))
            }
        };
        Some(PaperWindowInputs {
            symbol: config.exchange.symbol.clone(),
            grid: config.grid,
            rules: self.state.rules.read().await.clone(),
            reference_band: price_band(ticker.grid_reference_price()),
            maker_band: price_band(ticker.last_price),
            position: (
                position.size,
                position.entry_price,
                position.liquidation_price,
                position.leverage,
            ),
            trade_revision: self.state.db.symbol_trade_revision(&config.exchange.symbol),
            pause_epoch: self.state.pause_epoch(),
        })
    }

    pub(super) async fn paper_window_matches_cache(
        &self,
        paper_inputs: Option<&PaperWindowInputs>,
    ) -> bool {
        let Some(cached) = self
            .paper_window_cache
            .as_ref()
            .filter(|cached| paper_inputs == Some(&cached.inputs))
        else {
            return false;
        };
        let active = self.state.active_orders.read().await;
        active.len() == cached.orders.len()
            && active.iter().all(|(id, order)| {
                cached
                    .orders
                    .get(id)
                    .is_some_and(|previous| previous.matches_persisted_state(order))
            })
    }

    pub(super) async fn store_paper_window_cache_if_clean(
        &mut self,
        paper_inputs: Option<PaperWindowInputs>,
        placement_failed: bool,
    ) {
        if let Some(inputs) = paper_inputs.filter(|inputs| {
            !placement_failed
                && inputs.trade_revision == self.state.db.symbol_trade_revision(&inputs.symbol)
        }) {
            // A deferred remainder workflow must retry even without a price move.
            if matches!(self.load_remainder_plan().await, Ok(None)) {
                self.paper_window_cache = Some(PaperWindowCache {
                    inputs,
                    orders: self.state.active_orders.read().await.clone(),
                });
            }
        }
    }

    /// Confirm stale ordinary orders before reusing their funds or inventory.
    /// Return after cancellation so racing fills reconcile before replenishment.
    pub(super) fn window_needs_pruning<'a>(
        orders: impl Clone + IntoIterator<Item = &'a GridOrder>,
        desired_buys: &[Decimal],
        desired_sells: &[Decimal],
    ) -> bool {
        if desired_buys.len() > 64 || desired_sells.len() > 64 {
            return true;
        }
        let mut buy_seen = 0_u64;
        let mut sell_seen = 0_u64;
        for order in orders.clone().into_iter().filter(|o| !is_window_order(o)) {
            let (desired, seen) = if order.side == OrderSide::Buy {
                (desired_buys, &mut buy_seen)
            } else {
                (desired_sells, &mut sell_seen)
            };
            if let Some(idx) = desired.iter().position(|price| *price == order.price) {
                *seen |= 1_u64 << idx;
            }
        }
        for order in orders.into_iter().filter(|o| is_window_order(o)) {
            let (desired, seen) = if order.side == OrderSide::Buy {
                (desired_buys, &mut buy_seen)
            } else {
                (desired_sells, &mut sell_seen)
            };
            let Some(idx) = desired.iter().position(|price| *price == order.price) else {
                return true;
            };
            let bit = 1_u64 << idx;
            if (*seen & bit) != 0 {
                return true;
            }
            *seen |= bit;
        }
        false
    }

    pub(super) async fn prune_grid_window(
        &mut self,
        desired_buys: &[Decimal],
        desired_sells: &[Decimal],
    ) -> bool {
        let (total, mut obsolete) = {
            let active_guard = self.state.active_orders.read().await;
            if !Self::window_needs_pruning(active_guard.values(), desired_buys, desired_sells) {
                return false;
            }
            let desired_buy_set: HashSet<Decimal> = desired_buys.iter().copied().collect();
            let desired_sell_set: HashSet<Decimal> = desired_sells.iter().copied().collect();
            let total = active_guard.len();
            let mut occupied: HashSet<_> = active_guard
                .values()
                .filter(|o| !is_window_order(o))
                .map(|o| (o.side, o.price))
                .collect();
            let mut window_orders: Vec<&GridOrder> = active_guard
                .values()
                .filter(|o| is_window_order(o))
                .collect();
            if window_orders.is_empty() {
                return false;
            }
            // Preserve existing queue priority, without preferring any order label.
            window_orders.sort_by(|a, b| {
                a.created_at
                    .cmp(&b.created_at)
                    .then_with(|| a.client_order_id.cmp(&b.client_order_id))
            });
            let obsolete: Vec<GridOrder> = window_orders
                .into_iter()
                .filter(|order| {
                    let desired = if order.side == OrderSide::Buy {
                        &desired_buy_set
                    } else {
                        &desired_sell_set
                    };
                    !desired.contains(&order.price) || !occupied.insert((order.side, order.price))
                })
                .cloned()
                .collect();
            (total, obsolete)
        };
        if obsolete.is_empty() {
            return false;
        }
        self.state
            .add_log(
                "INFO",
                format!(
                    "Grid window cleanup: {} orders, canceling {} stale or duplicate orders",
                    total,
                    obsolete.len()
                ),
            )
            .await;
        obsolete.sort_by(|a, b| a.client_order_id.cmp(&b.client_order_id));
        for order in obsolete {
            if *self.state.status.read().await != BotStatus::Running
                || !self.cancel_single_order(&order).await
            {
                return true;
            }
        }
        true
    }

    /// Rebalance grid centered around current price
    pub(super) async fn rebalance_grid(&mut self) -> Result<(), String> {
        if !self.cancel_all_orders().await {
            let error = "Grid rebalance stopped: failed to cancel existing orders";
            self.state.add_log("ERROR", error).await;
            return Err(error.into());
        }
        if *self.state.status.read().await != BotStatus::Running {
            return Err("策略已暂停，网格重建未完成".into());
        }
        self.maintain_grid_window().await;
        if *self.state.status.read().await != BotStatus::Running {
            return Err("策略已暂停，网格重建未完成".into());
        }
        self.state
            .add_log("INFO", "Grid rebalance request processed")
            .await;
        Ok(())
    }

    pub(super) async fn cancel_misaligned_window_orders(
        &mut self,
        rules: &SymbolRules,
        grid_interval: Decimal,
    ) -> bool {
        let misaligned: Vec<_> = self
            .state
            .active_orders
            .read()
            .await
            .values()
            .filter(|o| is_window_order(o) && !rules.is_grid_price(o.price, grid_interval))
            .cloned()
            .collect();
        if misaligned.is_empty() {
            return false;
        }
        for order in misaligned {
            if *self.state.status.read().await != BotStatus::Running
                || !self.cancel_single_order(&order).await
            {
                return true;
            }
        }
        true
    }

    /// Keep closing orders within the directional position, nearest prices first.
    pub(super) async fn trim_closing_orders_to_position(&mut self) -> bool {
        let direction = self.state.config.read().await.grid.direction;
        let mut closing_orders: Vec<GridOrder> = self
            .state
            .active_orders
            .read()
            .await
            .values()
            .filter(|order| order.side == direction.closing_side())
            .cloned()
            .collect();
        closing_orders.sort_by(|a, b| {
            direction
                .closing_price_cmp(a.price, b.price)
                .then_with(|| a.client_order_id.cmp(&b.client_order_id))
        });
        let mut remaining = direction.position_quantity(self.state.position.read().await.size);
        let mut canceled_any = false;
        for order in closing_orders {
            if order.quantity <= remaining {
                remaining = remaining
                    .checked_sub(order.quantity)
                    .unwrap_or(Decimal::ZERO);
            } else if self.cancel_single_order(&order).await {
                canceled_any = true;
            } else {
                remaining = Decimal::ZERO;
            }
        }
        canceled_any
    }
}
