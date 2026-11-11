use super::window_prune::PaperWindowInputs;
use super::*;
use crate::sync_ext::MutexUnpoisonedExt;
#[derive(Clone, Copy)]
pub(super) struct LastFillSnapshot {
    pub(super) revision: u64,
    pub(super) fill: Option<(OrderSide, Decimal)>,
}

struct DesiredGridWindow {
    active_buy_prices: Vec<Decimal>,
    active_sell_prices: Vec<Decimal>,
    desired_buy_prices: Vec<Decimal>,
    desired_sell_prices: Vec<Decimal>,
    last_fill: LastFillSnapshot,
}

impl GridTradingEngine {
    pub(super) async fn load_last_fill(
        &self,
        config: &crate::config::AppConfig,
    ) -> Result<LastFillSnapshot> {
        let symbol = &config.exchange.symbol;
        let mode = config.exchange.trading_mode();
        // Capture before the read so a concurrent fill or history clear invalidates
        // this context even when it commits while the blocking query is pending.
        let revision = self.state.db.symbol_trade_revision(symbol);
        if let Some((_, _, cached)) = self
            .last_fill_cache
            .lock_unpoisoned()
            .as_ref()
            .filter(|(s, m, snap)| s == symbol && *m == mode && snap.revision == revision)
        {
            return Ok(*cached);
        }
        let query_symbol = symbol.clone();
        let fill = self
            .state
            .db
            .run_blocking(move |db| db.last_fill_for(&query_symbol, mode))
            .await?;
        let snapshot = LastFillSnapshot { revision, fill };
        if self.state.db.symbol_trade_revision(symbol) == revision {
            *self.last_fill_cache.lock_unpoisoned() = Some((symbol.clone(), mode, snapshot));
        }
        Ok(snapshot)
    }

    pub(super) async fn window_prices(
        &self,
        config: &crate::config::AppConfig,
        rules: &SymbolRules,
        market: Decimal,
    ) -> Result<(Vec<Decimal>, Vec<Decimal>)> {
        let last_fill = self.load_last_fill(config).await?;
        Ok(Self::window_prices_with_last_fill(
            config, rules, market, last_fill,
        ))
    }

    pub(super) fn window_prices_with_last_fill(
        config: &crate::config::AppConfig,
        rules: &SymbolRules,
        market: Decimal,
        last_fill: LastFillSnapshot,
    ) -> (Vec<Decimal>, Vec<Decimal>) {
        let interval = config.grid.grid_interval;
        // A recent fill only reserves its immediate neighborhood. It must never
        // pin either window to an old price while the mark price moves away.
        rules.grid_window_prices(
            market,
            interval,
            (config.grid.buy_window, config.grid.sell_window),
            (config.grid.min_price, config.grid.max_price),
            |_, price| {
                last_fill.fill.is_none_or(|(_, fill_price)| {
                    price
                        .checked_sub(fill_price)
                        .is_none_or(|diff| diff.abs() >= interval)
                })
            },
        )
    }

    pub(super) fn live_reconciliation_ready(&self) -> bool {
        !self.reconciliation_blocked && self.sync_timestamps_fresh()
    }

    /// Ensure the active pre-placed order window matches buy_window and sell_window.
    pub(super) async fn maintain_grid_window(&mut self) {
        self.maintain_grid_window_inner(false).await;
    }

    async fn window_maintenance_admitted(&mut self, config: &crate::config::AppConfig) -> bool {
        if self.state.is_shutting_down() || *self.state.status.read().await != BotStatus::Running {
            return false;
        }
        // Opposite positions require cancellation even while prices are stale.
        if !self.position_direction_ready().await {
            return false;
        }
        // Risk cancellation needs a valuation price, not a fresh last-trade price or
        // a completed replenishment workflow. Never reprice orders through this path.
        if !self
            .cancel_suspended_opening_orders(RiskConfigSnapshot::from_config(config))
            .await
        {
            return false;
        }
        if self.state.try_order_submission().is_none() {
            return false;
        }
        if self.pause_if_position_limit_reached().await {
            return false;
        }
        if !config.exchange.is_paper() && !self.live_reconciliation_ready() {
            return false;
        }
        self.market_data_ready().await
    }

    async fn prepare_window_recovery_and_inventory(
        &mut self,
        config: &crate::config::AppConfig,
    ) -> bool {
        match self.recover_remainder_plan().await {
            Ok(true) => return false,
            Ok(false) => {}
            Err(e) => {
                self.state
                    .add_log("WARN", format!("Remainder recovery pending: {}", e))
                    .await;
                return false;
            }
        }
        if !config.exchange.is_paper()
            && self
                .has_pending_live_pair_intents(&config.exchange.symbol)
                .await
        {
            return false;
        }
        let trimmed = self.trim_closing_orders_to_position().await;
        if trimmed
            && !config.exchange.is_paper()
            && self
                .has_pending_live_pair_intents(&config.exchange.symbol)
                .await
        {
            return false;
        }
        true
    }

    async fn replenish_missing_window_orders(
        &mut self,
        config: &crate::config::AppConfig,
        rules: &SymbolRules,
        current_price: Decimal,
        window: &mut DesiredGridWindow,
    ) -> Option<bool> {
        let grid_interval = config.grid.grid_interval;
        let mut placement_failed = false;
        for (side, desired_prices, active_prices, grid_level) in [
            (
                OrderSide::Buy,
                &window.desired_buy_prices,
                &mut window.active_buy_prices,
                -1,
            ),
            (
                OrderSide::Sell,
                &window.desired_sell_prices,
                &mut window.active_sell_prices,
                1,
            ),
        ] {
            for &target_price in desired_prices {
                // Retain queue priority for active orders still inside the nearest band.
                let already_exists = has_nearby_grid_order(
                    active_prices,
                    target_price,
                    grid_interval,
                    rules.tick_size,
                );
                if !already_exists && side.is_maker_price(target_price, current_price) {
                    let client_id = new_grid_client_order_id(side);
                    if self
                        .place_grid_order_in_window(
                            config,
                            rules,
                            GridOrderSpec {
                                side,
                                price: target_price,
                                client_order_id: client_id,
                                grid_level,
                                paired_client_order_id: None,
                            },
                            Some(WindowPlacementContext {
                                last_fill: window.last_fill,
                                precomputed: Some((current_price, desired_prices)),
                            }),
                        )
                        .await
                    {
                        active_prices.push(target_price);
                        if !config.exchange.is_paper()
                            && self
                                .prune_grid_window(
                                    &window.desired_buy_prices,
                                    &window.desired_sell_prices,
                                )
                                .await
                        {
                            return None;
                        }
                    } else {
                        placement_failed = true;
                    }
                }
            }
        }
        Some(placement_failed)
    }

    pub(super) async fn maintain_grid_window_inner(&mut self, skip_unchanged_paper: bool) {
        let config = self.state.config.read().await.clone();
        if !self.window_maintenance_admitted(&config).await {
            return;
        }
        // Keep all admission, freshness and protective cancellation checks above
        // the cache. Only a completed paper maintenance cycle can be reused.
        let paper_inputs = self.capture_paper_window_inputs(&config).await;
        if skip_unchanged_paper && self.paper_window_matches_cache(paper_inputs.as_ref()).await {
            return;
        }
        self.paper_window_cache = None;
        if !self.prepare_window_recovery_and_inventory(&config).await {
            return;
        }
        let current_price = self.state.ticker.read().await.grid_reference_price();
        if current_price.is_zero() {
            return;
        }

        let rules = self.state.rules.read().await.clone();
        let grid_interval = config.grid.grid_interval;
        if let Err(error) = rules.validate_grid_interval(grid_interval) {
            self.state.add_log("ERROR", error.to_string()).await;
            self.pause_trading().await;
            return;
        }
        if self
            .cancel_misaligned_window_orders(&rules, grid_interval)
            .await
        {
            // Cancellation can reveal a fill and change reserved levels.
            // Reconcile on the next cycle before allocating any released funds.
            return;
        }
        let Some(mut window) = self
            .compute_desired_window_prices(&config, &rules, current_price)
            .await
        else {
            return;
        };
        if self
            .prune_grid_window(&window.desired_buy_prices, &window.desired_sell_prices)
            .await
        {
            return;
        }

        let Some(placement_failed) = self
            .replenish_missing_window_orders(&config, &rules, current_price, &mut window)
            .await
        else {
            return;
        };
        self.finalize_window_maintenance(&config, &window, paper_inputs, placement_failed)
            .await;
    }

    async fn finalize_window_maintenance(
        &mut self,
        config: &crate::config::AppConfig,
        window: &DesiredGridWindow,
        paper_inputs: Option<PaperWindowInputs>,
        placement_failed: bool,
    ) {
        let closing_prices = if config.grid.direction == GridDirection::Long {
            &window.desired_sell_prices
        } else {
            &window.desired_buy_prices
        };
        if let Err(e) = self.reconcile_remainder(closing_prices).await {
            self.state
                .add_log("WARN", format!("Remainder reconciliation pending: {}", e))
                .await;
            return;
        }
        if self
            .prune_grid_window(&window.desired_buy_prices, &window.desired_sell_prices)
            .await
        {
            return;
        }
        self.store_paper_window_cache_if_clean(paper_inputs, placement_failed)
            .await;
    }

    async fn compute_desired_window_prices(
        &mut self,
        config: &crate::config::AppConfig,
        rules: &SymbolRules,
        current_price: Decimal,
    ) -> Option<DesiredGridWindow> {
        let grid_interval = config.grid.grid_interval;
        let mut active_buy_prices = Vec::new();
        let mut active_sell_prices = Vec::new();
        let mut reserved_buys = Vec::new();
        let mut reserved_sells = Vec::new();
        {
            let active_orders = self.state.active_orders.read().await;
            for order in active_orders.values() {
                let (active, reserved) = if order.side == OrderSide::Buy {
                    (&mut active_buy_prices, &mut reserved_buys)
                } else {
                    (&mut active_sell_prices, &mut reserved_sells)
                };
                active.push(order.price);
                // Partial executions still reserve funds or inventory without
                // displacing the nearest window.
                if !is_window_order(order) {
                    reserved.push(order.price);
                }
            }
        }
        let last_fill = match self.load_last_fill(config).await {
            Ok(last_fill) => last_fill,
            Err(error) => {
                self.state
                    .add_log("ERROR", format!("Could not load last fills: {}", error))
                    .await;
                self.pause_trading().await;
                return None;
            }
        };
        let (mut desired_buy_prices, mut desired_sell_prices) =
            Self::window_prices_with_last_fill(config, rules, current_price, last_fill);
        // Protected orders inside either nearest band occupy their level without
        // extending the band to compensate for far-away protected orders.
        desired_buy_prices.retain(|price| {
            !has_nearby_grid_order(&reserved_buys, *price, grid_interval, rules.tick_size)
        });
        desired_sell_prices.retain(|price| {
            !has_nearby_grid_order(&reserved_sells, *price, grid_interval, rules.tick_size)
        });
        // At the cap in cancel-opening mode, an empty opening band makes the prune
        // cancel opening-side window orders while exits stay on the book.
        if self
            .opening_suspended_at_limit(RiskConfigSnapshot::from_config(config))
            .await
        {
            if config.grid.direction == GridDirection::Long {
                desired_buy_prices.clear();
            } else {
                desired_sell_prices.clear();
            }
        }
        Some(DesiredGridWindow {
            active_buy_prices,
            active_sell_prices,
            desired_buy_prices,
            desired_sell_prices,
            last_fill,
        })
    }
}
