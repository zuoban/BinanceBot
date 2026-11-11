use super::*;

impl GridTradingEngine {
    pub(super) async fn market_data_ready(&mut self) -> bool {
        let ready = self.state.ticker.read().await.prices_are_fresh(Utc::now());
        let blocked = !ready;
        if self.market_data_blocked != blocked {
            self.market_data_blocked = blocked;
            let (level, message) = if blocked {
                ("WARN", "行情缺失或超过 15 秒未更新，暂停新增订单和窗口调整")
            } else {
                ("INFO", "行情已恢复，账户及订单对账有效后继续维护窗口")
            };
            self.state.add_log(level, message).await;
        }
        ready
    }

    pub(super) async fn update_paper_valuation(&self) {
        let symbol = self.state.config.read().await.exchange.symbol.clone();
        let ticker = self.state.ticker.read().await.clone();
        let mut position = self.state.position.write().await;
        let mut account = self.state.account.write().await;
        update_paper_account(&mut position, &mut account, &ticker);
        if let Err(error) = self
            .state
            .db
            .shared_paper_account(&symbol, &position, &account)
        {
            error!(%error, "Could not value shared paper account");
            self.state
                .paper_restore_failed
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    fn apply_fresh_ticker_fields(
        ticker: &mut TickerInfo,
        ticker_update: &TickerInfo,
        now: chrono::DateTime<Utc>,
    ) -> (bool, bool) {
        let fresh = |time| is_timestamp_fresh(time, now, MARKET_PRICE_MAX_AGE_SECS);
        let last_updated = ticker_update.last_price > Decimal::ZERO
            && fresh(ticker_update.update_time)
            && ticker_update.update_time >= ticker.update_time;
        if last_updated {
            ticker.last_price = ticker_update.last_price;
            ticker.high_24h = ticker_update.high_24h;
            ticker.low_24h = ticker_update.low_24h;
            ticker.change_24h = ticker_update.change_24h;
            ticker.change_percent_24h = ticker_update.change_percent_24h;
            ticker.volume_24h = ticker_update.volume_24h;
            ticker.update_time = ticker_update.update_time;
        }
        let mark_updated = ticker_update.mark_price > Decimal::ZERO
            && fresh(ticker_update.mark_update_time)
            && ticker_update.mark_update_time >= ticker.mark_update_time;
        if mark_updated {
            ticker.mark_price = ticker_update.mark_price;
            ticker.mark_update_time = ticker_update.mark_update_time;
        }
        if ticker.symbol != ticker_update.symbol {
            ticker.symbol = ticker_update.symbol.clone();
        }
        (last_updated, mark_updated)
    }

    pub(super) async fn handle_ticker_update(&mut self, ticker_update: TickerInfo) {
        let (symbol_matches, risk) = {
            let config = self.state.config.read().await;
            (
                ticker_update.symbol == config.exchange.symbol,
                RiskConfigSnapshot::from_config(&config),
            )
        };
        if !symbol_matches {
            return;
        }
        let (last_updated, mark_updated) = {
            let mut ticker = self.state.ticker.write().await;
            Self::apply_fresh_ticker_fields(&mut ticker, &ticker_update, Utc::now())
        };
        // Rejected market events cannot change risk inputs or trigger paper fills.
        if !last_updated && !mark_updated {
            return;
        }
        if last_updated {
            self.ticker_retry.reset();
        }
        if mark_updated {
            self.mark_retry.reset();
        }

        // Update unrealized PnL for current position
        if risk.is_paper {
            self.update_paper_valuation().await;
        }

        if !self.protect_openings_and_check_limits(risk).await {
            return;
        }
        let is_running = *self.state.status.read().await == BotStatus::Running;

        // In paper mode, match simulated orders against live market price
        if is_running
            && risk.is_paper
            && last_updated
            && self.state.ticker.read().await.prices_are_fresh(Utc::now())
        {
            self.pending_paper_ticker = None;
            self.simulate_order_fills(ticker_update.last_price).await;
        }
        if mark_updated && self.pending_paper_ticker.is_some() {
            Box::pin(self.match_pending_paper_ticker()).await;
        }
    }

    /// Check simulated fills in paper mode
    async fn collect_paper_fill_candidates(&self, current_price: Decimal) -> Vec<GridOrder> {
        let mut filled_orders = Vec::new();
        {
            let orders = self.state.active_orders.read().await;
            for order in orders.values() {
                let crossed = match order.side {
                    OrderSide::Buy => current_price <= order.price,
                    OrderSide::Sell => current_price >= order.price,
                };
                if crossed {
                    filled_orders.push(order.clone());
                }
            }
        }
        filled_orders.sort_by(|a, b| {
            a.side
                .as_str()
                .cmp(b.side.as_str())
                .then_with(|| {
                    if a.side == OrderSide::Buy {
                        b.price.cmp(&a.price)
                    } else {
                        a.price.cmp(&b.price)
                    }
                })
                .then_with(|| a.client_order_id.cmp(&b.client_order_id))
        });
        filled_orders
    }

    pub(super) async fn simulate_order_fills(&mut self, current_price: Decimal) {
        if !self.position_direction_ready().await {
            return;
        }
        let filled_orders = self.collect_paper_fill_candidates(current_price).await;
        let had_fills = !filled_orders.is_empty();
        for mut order in filled_orders {
            if *self.state.status.read().await != BotStatus::Running {
                break;
            }
            let direction = self.state.config.read().await.grid.direction;
            if order.side == direction.closing_side() {
                order.quantity = order
                    .quantity
                    .min(direction.position_quantity(self.state.position.read().await.size));
                if order.quantity <= Decimal::ZERO {
                    self.state
                        .active_orders
                        .write()
                        .await
                        .remove(&order.client_order_id);
                    continue;
                }
                order.amount_usdc = order
                    .price
                    .checked_mul(order.quantity)
                    .unwrap_or(Decimal::MAX);
            }
            Box::pin(self.on_order_filled(&mut order)).await;
            if self.pause_if_position_limit_reached().await {
                break;
            }
        }

        if had_fills || !self.state.active_orders.read().await.is_empty() {
            self.maintain_grid_window_inner(!had_fills).await;
        }
    }

    pub(super) async fn match_pending_paper_ticker(&mut self) {
        let Some((price, timestamp, pause_epoch)) = self.pending_paper_ticker else {
            return;
        };
        let ticker = self.state.ticker.read().await;
        if self.state.pause_epoch() != pause_epoch
            || ticker.last_price != price
            || ticker.update_time != timestamp
        {
            self.pending_paper_ticker = None;
            return;
        }
        if !ticker.prices_are_fresh(Utc::now()) {
            return;
        }
        drop(ticker);
        self.pending_paper_ticker = None;
        let risk = RiskConfigSnapshot::from_config(&*self.state.config.read().await);
        if risk.is_paper
            && *self.state.status.read().await == BotStatus::Running
            && self.protect_openings_and_check_limits(risk).await
        {
            self.simulate_order_fills(price).await;
        }
    }
}
