use super::*;
use futures_util::{stream, StreamExt};
use std::sync::atomic::{AtomicBool, Ordering};

pub(super) fn aggregate_execution_pnl(
    symbol: &str,
    fills: &[BinanceUserTrade],
) -> Result<(Decimal, Decimal, bool)> {
    let quote =
        quote_asset(symbol).ok_or_else(|| anyhow!("Unsupported quote asset for {}", symbol))?;
    if fills.is_empty() {
        return Err(anyhow!("No exchange fills found for {}", symbol));
    }
    if fills
        .iter()
        .any(|fill| !fill.commission_asset.eq_ignore_ascii_case(quote))
    {
        return Err(anyhow!(
            "Execution commission is not denominated in {}",
            quote
        ));
    }
    let realized_pnl = fills.iter().try_fold(Decimal::ZERO, |acc, fill| {
        acc.checked_add(fill.realized_pnl)
            .ok_or_else(|| anyhow!("Realized PnL overflow for {}", symbol))
    })?;
    let commission = fills.iter().try_fold(Decimal::ZERO, |acc, fill| {
        acc.checked_add(fill.commission)
            .ok_or_else(|| anyhow!("Commission overflow for {}", symbol))
    })?;
    Ok((
        realized_pnl,
        commission,
        fills.iter().all(|fill| fill.maker),
    ))
}

pub(super) fn exchange_fill_time(order: &BinanceOrderResponse) -> chrono::DateTime<Utc> {
    order
        .update_time
        .filter(|time| *time > 0)
        .and_then(chrono::DateTime::from_timestamp_millis)
        .unwrap_or_else(Utc::now)
}

struct PnlVerificationBatch<'a> {
    client: &'a BinanceFuturesClient,
    state: &'a Arc<AppState>,
    clear_revision: u64,
    quota_blocked: &'a AtomicBool,
    retry_needed: &'a AtomicBool,
}

impl PnlVerificationBatch<'_> {
    async fn verify_trade(&self, mut trade: TradeRecord, order_id: Option<i64>) {
        if self.quota_blocked.load(Ordering::Relaxed) {
            return;
        }
        let result = async {
            let order_id = match order_id {
                Some(order_id) => order_id,
                None => {
                    self.client
                        .get_order_by_client_id_for_pnl(&trade.symbol, &trade.client_order_id)
                        .await?
                        .order_id
                }
            };
            let fills = self
                .client
                .get_user_trades(&trade.symbol, order_id, trade.quantity, self.clear_revision)
                .await?;
            aggregate_execution_pnl(&trade.symbol, &fills)
        }
        .await;
        match result {
            Ok((pnl, commission, maker)) => {
                trade.realized_pnl = pnl;
                trade.commission = commission;
                trade.is_maker = maker;
                trade.pnl_verified = true;
                if let Err(e) = self
                    .state
                    .verify_trade_pnl(trade, self.clear_revision)
                    .await
                {
                    warn!("Could not save verified trade PnL: {}", e);
                    self.retry_needed.store(true, Ordering::Relaxed);
                }
            }
            Err(e) => {
                self.retry_needed.store(true, Ordering::Relaxed);
                if e.downcast_ref::<crate::exchange::client::LocalRateLimitError>()
                    .is_some()
                {
                    // Stop this batch at the first local rejection;
                    // only the other already-active requests may finish.
                    self.quota_blocked.store(true, Ordering::Relaxed);
                }
                debug!(
                    "PnL verification deferred for {}: {}",
                    trade.client_order_id, e
                );
            }
        }
    }
}

impl GridTradingEngine {
    async fn poll_pnl_reconcile_ready(&mut self) -> bool {
        if self
            .pnl_reconcile_task
            .as_ref()
            .is_some_and(|task| !task.is_finished())
        {
            return false;
        }
        if let Some(task) = self.pnl_reconcile_task.take() {
            if let Ok(Some(retry_at)) = task.await {
                self.pnl_retry_at = Some(retry_at);
            }
        }
        if self
            .pnl_retry_at
            .is_some_and(|retry_at| Instant::now() < retry_at)
        {
            return false;
        }
        self.pnl_retry_at = None;
        true
    }

    /// Bounded compensation runs independently of order admission and risk refreshes.
    pub(super) async fn reconcile_unverified_trades(&mut self) {
        if !self.poll_pnl_reconcile_ready().await {
            return;
        }
        let (is_paper, mode) = {
            let config = self.state.config.read().await;
            (config.exchange.is_paper(), config.exchange.trading_mode())
        };
        if is_paper {
            return;
        }
        if self.state.is_stats_dirty() {
            self.state.refresh_current_scope().await;
            if self.state.is_stats_dirty() {
                return;
            }
        }
        let pending_count = self.state.stats.read().await.pending_pnl_trades;
        self.client.note_pnl_reconcile_backlog(pending_count);
        if pending_count == 0 {
            self.pnl_reconcile_offset = 0;
            self.pnl_order_ids.clear();
            return;
        }
        let offset = self.pnl_reconcile_offset % pending_count;
        let pending_symbol = self.state.config.read().await.exchange.symbol.clone();
        let (pending, clear_revision) = match self
            .state
            .db
            .run_blocking(move |db| {
                db.get_unverified_trades_with_clear_revision(&pending_symbol, mode, 32, offset)
            })
            .await
        {
            Ok(trades) => trades,
            Err(e) => {
                warn!("Could not load trades awaiting PnL verification: {}", e);
                return;
            }
        };
        if clear_revision != self.pnl_clear_revision {
            self.pnl_order_ids.clear();
            self.pnl_clear_revision = clear_revision;
        }
        self.pnl_reconcile_offset = (offset + pending.len()) % pending_count;
        let pending: Vec<_> = pending
            .into_iter()
            .map(|trade| {
                let order_id = self.pnl_order_ids.get(&trade.client_order_id).copied();
                (trade, order_id)
            })
            .collect();
        let client = Arc::clone(&self.client);
        let state = Arc::clone(&self.state);
        self.pnl_reconcile_task = Some(tokio::spawn(async move {
            let quota_blocked = AtomicBool::new(false);
            let retry_needed = AtomicBool::new(false);
            let batch = PnlVerificationBatch {
                client: &client,
                state: &state,
                clear_revision,
                quota_blocked: &quota_blocked,
                retry_needed: &retry_needed,
            };
            stream::iter(pending)
                .for_each_concurrent(4, |(trade, order_id)| batch.verify_trade(trade, order_id))
                .await;
            retry_needed.load(Ordering::Relaxed).then(|| {
                Instant::now() + Duration::from_millis(60_000 + rand::random::<u64>() % 1001)
            })
        }));
    }
}
