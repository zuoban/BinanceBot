use super::*;
use futures_util::{stream, StreamExt};
use std::sync::atomic::{AtomicBool, Ordering};

pub(super) fn aggregate_execution_pnl(
    symbol: &str,
    fills: &[BinanceUserTrade],
) -> Result<(Decimal, Decimal, Option<String>, bool)> {
    let quote =
        quote_asset(symbol).ok_or_else(|| anyhow!("Unsupported quote asset for {}", symbol))?;
    if fills.is_empty() {
        return Err(anyhow!("No exchange fills found for {}", symbol));
    }
    let realized_pnl = fills.iter().try_fold(Decimal::ZERO, |acc, fill| {
        acc.checked_add(fill.realized_pnl)
            .ok_or_else(|| anyhow!("Realized PnL overflow for {}", symbol))
    })?;
    let mut quote_commission = Decimal::ZERO;
    let mut has_quote_nonzero = false;
    let mut non_quote_asset: Option<String> = None;
    let mut non_quote_commission = Decimal::ZERO;
    let mut multiple_non_quote = false;

    for fill in fills {
        let asset = fill.commission_asset.as_str();
        anyhow::ensure!(
            (1..=16).contains(&asset.len())
                && asset.bytes().all(|b| b.is_ascii_alphanumeric())
                && !asset.eq_ignore_ascii_case("MIXED"),
            "Invalid commission asset {:?} for {}",
            asset,
            symbol
        );
        if fill.commission.is_zero() {
            continue;
        }
        if asset.eq_ignore_ascii_case(quote) {
            has_quote_nonzero = true;
            quote_commission = quote_commission
                .checked_add(fill.commission)
                .ok_or_else(|| anyhow!("Commission overflow for {}", symbol))?;
        } else {
            let upper = asset.to_ascii_uppercase();
            match &non_quote_asset {
                None => {
                    non_quote_asset = Some(upper);
                    non_quote_commission = fill.commission;
                }
                Some(existing) if existing == &upper => {
                    non_quote_commission = non_quote_commission
                        .checked_add(fill.commission)
                        .ok_or_else(|| anyhow!("Commission overflow for {}", symbol))?;
                }
                Some(_) => {
                    multiple_non_quote = true;
                }
            }
        }
    }

    let is_maker = fills.iter().all(|fill| fill.maker);
    let (commission, commission_asset) = if non_quote_asset.is_none()
        || (!multiple_non_quote && !has_quote_nonzero && non_quote_commission.is_zero())
    {
        (quote_commission, None)
    } else if !has_quote_nonzero && !multiple_non_quote {
        (non_quote_commission, non_quote_asset)
    } else {
        (quote_commission, Some("MIXED".to_string()))
    };
    Ok((realized_pnl, commission, commission_asset, is_maker))
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
    clear_revision: u64,
    quota_blocked: &'a AtomicBool,
    retry_needed: &'a AtomicBool,
}

fn validate_pnl_order(trade: &TradeRecord, order: &BinanceOrderResponse) -> Result<()> {
    anyhow::ensure!(
        order.order_id > 0
            && order.client_order_id == trade.client_order_id
            && order.symbol == trade.symbol
            && order.side == trade.side.as_str(),
        "PnL order identity mismatch for {}",
        trade.client_order_id
    );
    anyhow::ensure!(
        order.is_terminal()
            && trade.quantity > Decimal::ZERO
            && order.executed_qty == trade.quantity
            && order.orig_qty >= order.executed_qty
            && (order.status != "FILLED" || order.orig_qty == order.executed_qty),
        "PnL order terminal quantities do not match saved trade {}",
        trade.client_order_id
    );
    Ok(())
}

impl PnlVerificationBatch<'_> {
    async fn fetch_verified_trade(
        &self,
        mut trade: TradeRecord,
        order_id: Option<i64>,
    ) -> Option<TradeRecord> {
        if self.quota_blocked.load(Ordering::Relaxed) {
            return None;
        }
        let result = async {
            let order_id = match order_id {
                Some(order_id) => order_id,
                None => {
                    let order = self
                        .client
                        .get_order_by_client_id_for_pnl(&trade.symbol, &trade.client_order_id)
                        .await?;
                    // A successful lookup and matching fill total cannot establish identity.
                    validate_pnl_order(&trade, &order)?;
                    order.order_id
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
            Ok((pnl, commission, commission_asset, maker)) => {
                trade.realized_pnl = pnl;
                trade.commission = commission;
                trade.commission_asset = commission_asset;
                trade.is_maker = maker;
                trade.pnl_verified = true;
                Some(trade)
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
                None
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
        if pending.is_empty() {
            self.pnl_reconcile_offset = 0;
            return;
        }
        self.pnl_reconcile_offset = (offset + pending.len()) % pending_count;
        let pending: Vec<_> = pending
            .into_iter()
            .map(|trade| {
                let order_id = self.pnl_order_ids.get(&trade.client_order_id).copied();
                (trade, order_id)
            })
            .collect();
        self.pnl_reconcile_task = Some(Self::spawn_pnl_verification_batch(
            Arc::clone(&self.client),
            Arc::clone(&self.state),
            clear_revision,
            pending,
        ));
    }

    fn spawn_pnl_verification_batch(
        client: Arc<BinanceFuturesClient>,
        state: Arc<AppState>,
        clear_revision: u64,
        pending: Vec<(TradeRecord, Option<i64>)>,
    ) -> JoinHandle<Option<Instant>> {
        tokio::spawn(async move {
            let quota_blocked = AtomicBool::new(false);
            let retry_needed = AtomicBool::new(false);
            let batch = PnlVerificationBatch {
                client: &client,
                clear_revision,
                quota_blocked: &quota_blocked,
                retry_needed: &retry_needed,
            };
            let verified: Vec<TradeRecord> = stream::iter(pending)
                .map(|(trade, order_id)| batch.fetch_verified_trade(trade, order_id))
                .buffered(4)
                .filter_map(|trade| async move { trade })
                .collect()
                .await;
            if !verified.is_empty() {
                if let Err(e) = state
                    .verify_trades_pnl_batch(verified, clear_revision)
                    .await
                {
                    warn!("Could not save verified trade PnL: {}", e);
                    retry_needed.store(true, Ordering::Relaxed);
                }
            }
            retry_needed.load(Ordering::Relaxed).then(|| {
                Instant::now() + Duration::from_millis(60_000 + rand::random::<u64>() % 1001)
            })
        })
    }
}
