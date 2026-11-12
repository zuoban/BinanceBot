use super::*;
use crate::exchange::SharedRequestPending;

#[derive(Clone, Copy)]
enum AccountSyncResult {
    Ready(Instant),
    Pending,
    Failed,
}

impl AccountSyncResult {
    fn instant(self) -> Option<Instant> {
        match self {
            Self::Ready(at) => Some(at),
            Self::Pending | Self::Failed => None,
        }
    }
}

pub(super) fn build_sync_timer(sync_secs: u64) -> tokio::time::Interval {
    let sync_secs = sync_secs.clamp(1, crate::config::MAX_SYNC_INTERVAL_SECS);
    let period = Duration::from_secs(sync_secs);
    let offset =
        Duration::from_millis(rand::random::<u64>() % sync_secs.saturating_mul(1000).max(1));
    let start = tokio::time::Instant::now()
        .checked_add(offset)
        .unwrap_or_else(tokio::time::Instant::now);
    let mut timer = tokio::time::interval_at(start, period);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    timer
}

pub(super) fn live_sync_freshness(sync_interval_secs: u64) -> Duration {
    Duration::from_secs(
        sync_interval_secs
            .saturating_mul(2)
            .saturating_add(4)
            .max(10)
            .min(max_reconciliation_age_secs(sync_interval_secs) as u64),
    )
}

impl GridTradingEngine {
    pub(super) fn sync_freshness(&self) -> Duration {
        live_sync_freshness(self.sync_interval_secs)
    }

    // Compatibility recovery only: adopted exchange orders are reconciled, but
    // an absent legacy counter-order is retired instead of creating a new pair.
    pub(super) fn sync_timestamps_fresh(&self) -> bool {
        !self.account_sync_pending && self.reconciliation_data_fresh()
    }

    fn reconciliation_data_fresh(&self) -> bool {
        let freshness = self.sync_freshness();
        self.last_account_sync
            .is_some_and(|at| at.elapsed() < freshness)
            && self
                .last_orders_sync
                .is_some_and(|at| at.elapsed() < freshness)
    }

    pub(super) async fn refresh_position_then_account(&mut self, shared: bool) -> bool {
        self.account_sync_pending = false;
        let position = self.sync_position(shared).await.instant();
        let account = if position.is_some() {
            self.sync_account(shared, !shared).await.instant()
        } else {
            None
        };
        self.last_account_sync = position.zip(account).map(|(p, a)| p.min(a));
        self.last_account_sync.is_some()
    }

    async fn refresh_sync_cycle_market(
        &mut self,
        symbol: &str,
        wait_for_market: bool,
        pause_epoch: u64,
        started: Option<Instant>,
    ) -> Option<&'static str> {
        if wait_for_market {
            Box::pin(self.refresh_mark_price(symbol)).await;
        } else {
            self.start_mark_price_request(symbol).await;
        }
        if let Some(started) = started {
            debug!(
                stage = "mark_price",
                cycle_elapsed_ms = started.elapsed().as_millis() as u64,
                "Periodic sync stage finished"
            );
        }
        if self.state.pause_epoch() != pause_epoch {
            return Some("paused_after_mark_price");
        }
        if wait_for_market {
            Box::pin(self.refresh_stale_ticker(symbol)).await;
        } else {
            self.start_ticker_request(symbol).await;
        }
        if let Some(started) = started {
            debug!(
                stage = "ticker",
                cycle_elapsed_ms = started.elapsed().as_millis() as u64,
                "Periodic sync stage finished"
            );
        }
        if self.state.pause_epoch() != pause_epoch {
            return Some("paused_after_ticker");
        }
        None
    }

    fn log_sync_finished(started: Option<Instant>, result: &'static str) {
        if let Some(started) = started {
            debug!(
                result,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "Periodic sync finished"
            );
        }
    }

    async fn reconcile_sync_cycle_live(
        &mut self,
        pause_epoch: u64,
        started: Option<Instant>,
    ) -> std::result::Result<bool, &'static str> {
        let sync_fut = self.sync_live_orders_with_priority(ReconciliationPriority::Periodic);
        let orders_ready = Box::pin(sync_fut).await;
        if let Some(started) = started {
            debug!(
                stage = "orders",
                ready = orders_ready,
                cycle_elapsed_ms = started.elapsed().as_millis() as u64,
                "Periodic sync stage finished"
            );
        }
        if !orders_ready {
            self.last_orders_sync = None;
        }
        if self.state.pause_epoch() != pause_epoch {
            return Err("paused_after_orders");
        }
        let account_ready = Box::pin(self.sync_account_and_position_inner(true)).await;
        if let Some(started) = started {
            debug!(
                stage = "account_position",
                ready = account_ready,
                cycle_elapsed_ms = started.elapsed().as_millis() as u64,
                "Periodic sync stage finished"
            );
        }
        // Waiting for a sibling's HTTP request does not invalidate an otherwise
        // fresh risk snapshot. Replenishment still waits for this refresh.
        let risk_ready = orders_ready
            && (account_ready || self.account_sync_pending)
            && self.reconciliation_data_fresh()
            && !self.state.is_stats_dirty();
        self.set_global_reconciliation_ready(risk_ready).await;
        Ok(risk_ready && account_ready)
    }

    pub(super) async fn retry_pending_account_sync(&mut self) {
        if !self.account_sync_pending || self.state.is_shutting_down() {
            return;
        }
        let risk = RiskConfigSnapshot::from_config(&*self.state.config.read().await);
        if risk.is_paper {
            self.account_sync_pending = false;
            return;
        }
        let pause_epoch = self.state.pause_epoch();
        let account_ready = self.sync_account_and_position_inner(true).await;
        let risk_ready = !self.reconciliation_blocked
            && (account_ready || self.account_sync_pending)
            && self.reconciliation_data_fresh()
            && !self.state.is_stats_dirty();
        self.set_global_reconciliation_ready(risk_ready).await;
        if self.state.pause_epoch() == pause_epoch {
            Box::pin(self.finish_sync_cycle_window(risk, risk_ready && account_ready, None)).await;
        }
    }

    async fn maybe_sync_server_time(&mut self) {
        if Instant::now() < self.next_time_sync_at {
            return;
        }
        match self.client.sync_server_time().await {
            Ok(_) => self.next_time_sync_at = Instant::now() + PERIODIC_TIME_SYNC_INTERVAL,
            Err(error) => {
                warn!("Periodic Binance server time sync failed: {}", error);
                self.next_time_sync_at = Instant::now() + TIME_SYNC_RETRY_INTERVAL;
            }
        }
    }

    /// Periodic sync cycle
    pub(super) async fn sync_cycle(&mut self, wait_for_market: bool) {
        let started = tracing::enabled!(tracing::Level::DEBUG).then(Instant::now);
        let pause_epoch = self.state.pause_epoch();
        let (symbol, risk) = {
            let config = self.state.config.read().await;
            (
                config.exchange.symbol.clone(),
                RiskConfigSnapshot::from_config(&config),
            )
        };
        self.sync_interval_secs = risk.sync_interval_secs.max(1);
        if self.state.is_stats_dirty() {
            self.state.refresh_current_scope().await;
        }
        self.maybe_sync_server_time().await;
        let fut = self.refresh_sync_cycle_market(&symbol, wait_for_market, pause_epoch, started);
        if let Some(reason) = Box::pin(fut).await {
            Self::log_sync_finished(started, reason);
            return;
        }
        // Cancellation remains available when the following reconciliation cannot
        // establish replenishment readiness, including the sync performed on resume.
        Box::pin(self.cancel_suspended_opening_orders(risk)).await;
        if self.state.pause_epoch() != pause_epoch {
            Self::log_sync_finished(started, "paused_after_opening_protection");
            return;
        }

        let account_ready = if !risk.is_paper {
            match Box::pin(self.reconcile_sync_cycle_live(pause_epoch, started)).await {
                Ok(ready) => ready,
                Err(reason) => {
                    Self::log_sync_finished(started, reason);
                    return;
                }
            }
        } else {
            let ready = !self.state.is_stats_dirty();
            self.set_global_reconciliation_ready(ready).await;
            ready
        };

        if self.state.pause_epoch() != pause_epoch {
            Self::log_sync_finished(started, "paused_after_reconciliation");
            return;
        }

        Box::pin(self.finish_sync_cycle_window(risk, account_ready, started)).await;
    }

    async fn finish_sync_cycle_window(
        &mut self,
        risk: RiskConfigSnapshot,
        account_ready: bool,
        started: Option<Instant>,
    ) {
        let openings_protected = self.cancel_suspended_opening_orders(risk).await;
        if self.pause_if_position_limit_reached().await {
            Self::log_sync_finished(started, "position_limit");
            return;
        }
        self.trim_closing_orders_to_position().await;

        let maintain_window = *self.state.status.read().await == BotStatus::Running
            && account_ready
            && openings_protected;
        if maintain_window {
            Box::pin(self.maintain_grid_window()).await;
        }
        if let Some(started) = started {
            debug!(
                result = if account_ready {
                    "complete"
                } else if self.account_sync_pending {
                    "account_position_pending"
                } else {
                    "reconciliation_failed"
                },
                window_maintained = maintain_window,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "Periodic sync finished"
            );
        }
    }

    /// Fetch position and account balances from Binance in Live mode
    pub(super) async fn sync_account_and_position(&mut self) -> bool {
        self.sync_account_and_position_inner(false).await
    }

    async fn sync_position(&self, shared_position: bool) -> AccountSyncResult {
        let symbol = self.state.config.read().await.exchange.symbol.clone();
        let started = Instant::now();
        let position_result = if shared_position {
            self.position_cache.get(&self.client, &symbol).await
        } else {
            self.position_cache
                .refresh_critical(&self.client, &symbol)
                .await
        };
        let elapsed_ms = started.elapsed().as_millis() as u64;
        match position_result {
            Ok((Some(pos), received_at)) => {
                let synced_at = chrono::Duration::from_std(received_at.elapsed())
                    .ok()
                    .map(|age| Utc::now() - age);
                *self.state.position.write().await = pos.into_position_info(synced_at);
                return AccountSyncResult::Ready(received_at);
            }
            Err(error) if error.is::<SharedRequestPending>() => {
                debug!(%symbol, elapsed_ms, "Binance position sync is waiting for a shared request");
                return AccountSyncResult::Pending;
            }
            Err(error) => {
                warn!(%symbol, elapsed_ms, error = %error, "Could not sync Binance position");
                crate::metrics::POSITION_SYNC_FAILURES.inc();
            }
            Ok((None, _)) => {
                warn!(%symbol, elapsed_ms, "Binance position response contains no record for symbol");
            }
        }
        self.state.position.write().await.update_time = None;
        self.set_global_reconciliation_ready(false).await;
        AccountSyncResult::Failed
    }

    async fn sync_account(&self, shared: bool, critical: bool) -> AccountSyncResult {
        let symbol = self.state.config.read().await.exchange.symbol.clone();
        let started = Instant::now();
        let account = if critical {
            self.account_cache.refresh_critical(&self.client).await
        } else if shared {
            self.account_cache.get(&self.client).await
        } else {
            self.account_cache.refresh(&self.client).await
        };
        match account {
            Ok(snapshot) => {
                if let Some(asset) = snapshot.account.margin_asset_for_symbol(&symbol) {
                    let mut state_acc = self.state.account.write().await;
                    state_acc.asset = asset.asset.clone();
                    state_acc.total_wallet_balance = asset.wallet_balance;
                    state_acc.available_balance = asset.available_balance;
                    state_acc.margin_balance = asset.margin_balance;
                    state_acc.unrealized_profit = asset.unrealized_profit;
                    state_acc.update_time = snapshot.received_at;
                    AccountSyncResult::Ready(snapshot.received_instant)
                } else {
                    warn!(
                        %symbol,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "No matching margin asset in Binance account response for {}",
                        symbol
                    );
                    self.state.account.write().await.update_time =
                        chrono::DateTime::<Utc>::default();
                    self.set_global_reconciliation_ready(false).await;
                    AccountSyncResult::Failed
                }
            }
            Err(error) if error.is::<SharedRequestPending>() => {
                debug!(
                    %symbol,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "Binance account sync is waiting for a shared request"
                );
                AccountSyncResult::Pending
            }
            Err(e) => {
                crate::metrics::ACCOUNT_SYNC_FAILURES.inc();
                warn!(
                    %symbol,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    error = %e,
                    "Could not sync Binance account balance"
                );
                self.state.account.write().await.update_time = chrono::DateTime::<Utc>::default();
                self.set_global_reconciliation_ready(false).await;
                AccountSyncResult::Failed
            }
        }
    }

    pub(super) fn invalidate_account_and_position_caches(&mut self) {
        self.account_cache.invalidate(&self.client);
        self.position_cache.invalidate(&self.client);
        self.last_account_sync = None;
        self.account_sync_pending = false;
    }

    pub(super) async fn block_reconciliation_and_invalidate_caches(&mut self) {
        self.mark_orders_reconciliation_blocked().await;
        self.invalidate_account_and_position_caches();
    }

    pub(super) async fn sync_account_and_position_inner(&mut self, shared_position: bool) -> bool {
        let pause_epoch = self.state.pause_epoch();
        self.account_sync_pending = false;
        let position = self.sync_position(shared_position).await;
        if self.state.pause_epoch() != pause_epoch {
            self.last_account_sync = None;
            return false;
        }
        let account = self.sync_account(shared_position, !shared_position).await;
        let ready = match (position, account) {
            (AccountSyncResult::Ready(position), AccountSyncResult::Ready(account)) => {
                self.last_account_sync = Some(account.min(position));
                true
            }
            (AccountSyncResult::Failed, _) | (_, AccountSyncResult::Failed) => {
                self.last_account_sync = None;
                false
            }
            _ => {
                self.account_sync_pending = true;
                if self
                    .last_account_sync
                    .is_none_or(|at| at.elapsed() >= self.sync_freshness())
                {
                    if let Some(previous_sync) = self.last_account_sync.take() {
                        let symbol = self.state.config.read().await.exchange.symbol.clone();
                        warn!(
                            %symbol,
                            snapshot_age_ms = previous_sync.elapsed().as_millis() as u64,
                            "Binance account sync is still pending and the previous risk snapshot expired"
                        );
                    }
                    self.set_global_reconciliation_ready(false).await;
                }
                false
            }
        };
        self.state.check_global_position_limit().await;
        ready
    }

    pub(super) async fn acquire_live_sync_fill_guard(
        &self,
        account_key: [u8; 32],
        symbol: &str,
        invalid_price: bool,
        live_orders: &[BinanceOrderResponse],
        pair_intents: &HashMap<String, GridOrder>,
        remainder_plan: Option<&RemainderPlan>,
    ) -> Option<crate::server::state::PendingFillReconciliation> {
        let known_risk = invalid_price
            || live_orders.iter().any(|order| {
                is_grid_order(&order.client_order_id)
                    && (order.executed_qty > Decimal::ZERO || unresolved_filled_quantity(order))
            })
            || !pair_intents.is_empty()
            || remainder_plan.is_some_and(|plan| plan.phase == RemainderPhase::Submitting)
            || self.evidence.has_conflicts_for_scope(account_key, symbol);
        let mut fill_guard = known_risk.then(|| self.state.publish_fill_reconciliation());
        if fill_guard.is_none() {
            let active = match self.state.active_orders.try_read() {
                Ok(active) => active,
                Err(_) => {
                    fill_guard = Some(self.state.publish_fill_reconciliation());
                    self.state.active_orders.read().await
                }
            };
            let missing = if active.is_empty() {
                false
            } else {
                let seen_ids: HashSet<&str> = live_orders
                    .iter()
                    .map(|order| order.client_order_id.as_str())
                    .collect();
                active.keys().any(|id| !seen_ids.contains(id.as_str()))
            };
            if missing && fill_guard.is_none() {
                fill_guard = Some(self.state.publish_fill_reconciliation());
            }
            drop(active);
            if !missing {
                if let Some(mut guard) = fill_guard.take() {
                    guard.resolve();
                }
            }
        }
        if let Some(guard) = &mut fill_guard {
            guard.acquire().await;
        }
        fill_guard
    }

    pub(super) async fn refresh_live_partial_fill_risk(
        &mut self,
        priority: ReconciliationPriority,
        pause_epoch: u64,
        inconsistent_fill: bool,
        live_ids: &HashMap<String, BinanceOrderResponse>,
    ) -> (bool, bool) {
        // A partial fill moves exposure into the position and changes
        // available margin. Refresh both before reducing its reservation.
        let needs_position_refresh = {
            let active = self.state.active_orders.read().await;
            live_ids.iter().any(|(id, order)| {
                order.executed_qty > Decimal::ZERO
                    && (self.last_account_sync.is_none()
                        || active.get(id).is_none_or(|previous| {
                            previous.status != OrderStatus::PartiallyFilled
                                || previous.quantity > order.remaining_qty()
                        }))
            })
        };
        if needs_position_refresh || inconsistent_fill {
            self.invalidate_account_and_position_caches();
        }
        let position_ready = if inconsistent_fill || self.state.pause_epoch() != pause_epoch {
            false
        } else if needs_position_refresh {
            self.refresh_position_then_account(matches!(priority, ReconciliationPriority::Periodic))
                .await
        } else {
            true
        };
        (needs_position_refresh, position_ready)
    }
}
