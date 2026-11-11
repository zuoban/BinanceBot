#[cfg(test)]
pub(super) use super::evidence_book::terminal_responses_disagree;
use super::reconciliation_settlement::{LiveSyncSettlement, OpenLiveOrderAdoption};
use super::snapshot_conflicts::SnapshotConflictScan;
use super::snapshot_identities::invalid_snapshot_price;
use super::*;

type LiveSyncPreflight = (
    HashMap<String, GridOrder>,
    Option<RemainderPlan>,
    HashSet<String>,
);

struct LiveSnapshotValidation<'a> {
    priority: ReconciliationPriority,
    pause_epoch: u64,
    account_key: [u8; 32],
    symbol: &'a str,
    mode: TradingMode,
    live_orders: Vec<BinanceOrderResponse>,
    pair_intents: &'a HashMap<String, GridOrder>,
    restored_ids: HashSet<String>,
}

pub(super) fn find_durable_order<'a>(
    client_order_id: &str,
    managed: &'a [GridOrder],
    pair_intents: &'a HashMap<String, GridOrder>,
    remainder_plan: Option<&'a RemainderPlan>,
) -> Option<&'a GridOrder> {
    managed
        .iter()
        .find(|order| order.client_order_id == client_order_id)
        .or_else(|| pair_intents.get(client_order_id))
        .or_else(|| remainder_plan.and_then(|plan| plan.find_order(client_order_id)))
}

impl GridTradingEngine {
    async fn prepare_live_orders_sync(
        &mut self,
        account_key: [u8; 32],
        symbol: &str,
        mode: TradingMode,
    ) -> Option<LiveSyncPreflight> {
        let preflight_symbol = symbol.to_owned();
        let preflight = self
            .state
            .db
            .run_blocking(move |db| {
                let intents = match db.load_pair_intents(&preflight_symbol, mode) {
                    Ok(orders) => orders,
                    Err(e) => return Ok(Err((true, e))),
                };
                match db.load_remainder_plan(&preflight_symbol, mode) {
                    Ok(plan) => Ok(Ok((intents, plan))),
                    Err(e) => Ok(Err((false, e))),
                }
            })
            .await;
        let (pair_intents, remainder_plan) = match preflight {
            Ok(Ok((intents, plan))) => {
                let map: HashMap<String, GridOrder> = intents
                    .into_iter()
                    .map(|order| (order.client_order_id.clone(), order))
                    .collect();
                (map, plan)
            }
            Ok(Err((true, e))) | Err(e) => {
                error!("Could not load pending pair intents: {}", e);
                self.report_reconciliation_state(true).await;
                return None;
            }
            Ok(Err((false, e))) => {
                error!("Could not load remainder recovery metadata: {}", e);
                self.last_orders_sync = None;
                return None;
            }
        };
        // A previous failed managed-order read may have left an exchange-only
        // placeholder in memory. Recover durable metadata before any normal or
        // terminal reconciliation can persist or consume that placeholder.
        if !self.load_identity_observations(account_key, symbol).await
            || !self
                .persist_identity_observations(account_key, symbol)
                .await
        {
            self.last_orders_sync = None;
            return None;
        }
        if self
            .terminal_disagreement_blocked(account_key, symbol)
            .await
        {
            self.mark_orders_reconciliation_blocked().await;
            return None;
        }
        let (restored_ids, _) = self
            .restore_provisional_orders(account_key, symbol, &pair_intents, remainder_plan.as_ref())
            .await?;
        Some((pair_intents, remainder_plan, restored_ids))
    }

    /// Reconcile open orders from Binance in Live mode
    pub(super) async fn sync_live_orders(&mut self) -> bool {
        self.sync_live_orders_with_priority(ReconciliationPriority::Critical)
            .await
    }

    async fn verify_and_adopt_open_snapshot(
        &mut self,
        priority: ReconciliationPriority,
        pause_epoch: u64,
        account_key: [u8; 32],
        symbol: &str,
        input: (&HashMap<String, GridOrder>, Option<&RemainderPlan>),
        state: (
            HashMap<String, BinanceOrderResponse>,
            HashSet<String>,
            HashSet<String>,
        ),
    ) -> Option<HashMap<String, BinanceOrderResponse>> {
        let (pair_intents, remainder_plan) = input;
        let (live_ids, uncertain_ids, mut restored_ids) = state;
        let inconsistent_fill = live_ids.values().any(unresolved_filled_quantity);
        // Every live snapshot is checked against durable identities;
        // in-memory conflict barriers do not survive a process restart.
        let persisted_managed = self
            .verify_live_snapshot_identities(
                account_key,
                symbol,
                &live_ids,
                pair_intents,
                remainder_plan,
                &mut restored_ids,
            )
            .await?;
        let (needs_position_refresh, position_ready) = self
            .refresh_live_partial_fill_risk(priority, pause_epoch, inconsistent_fill, &live_ids)
            .await;
        if !self
            .adopt_and_persist_open_live_orders(OpenLiveOrderAdoption {
                pause_epoch,
                position_ready,
                live_ids: &live_ids,
                uncertain_ids: &uncertain_ids,
                restored_ids: &restored_ids,
                pair_intents,
                remainder_plan,
                persisted_managed: &persisted_managed,
            })
            .await
        {
            return None;
        }
        if !position_ready {
            self.report_reconciliation_state(true).await;
            return None;
        }
        if needs_position_refresh {
            self.state.check_global_position_limit().await;
        }
        (self.state.pause_epoch() == pause_epoch).then_some(live_ids)
    }

    async fn validate_and_adopt_live_snapshot(
        &mut self,
        input: LiveSnapshotValidation<'_>,
        remainder_plan: Option<&RemainderPlan>,
    ) -> Option<(
        HashMap<String, BinanceOrderResponse>,
        HashMap<String, BinanceOrderResponse>,
        Vec<String>,
    )> {
        let LiveSnapshotValidation {
            priority,
            pause_epoch,
            account_key,
            symbol,
            mode,
            mut live_orders,
            pair_intents,
            mut restored_ids,
        } = input;
        if live_orders
            .iter()
            .any(|order| is_grid_order(&order.client_order_id) && unresolved_filled_quantity(order))
        {
            self.set_global_reconciliation_ready(false).await;
        }
        if !self
            .detect_and_persist_snapshot_conflicts(SnapshotConflictScan {
                account_key,
                symbol,
                live_orders: &live_orders,
                pair_intents,
                remainder_plan,
                restored_ids: &restored_ids,
            })
            .await
        {
            return None;
        }
        let (mut corrected_terminal, resolved_conflict_ids) = self
            .resolve_order_identity_conflicts(
                account_key,
                symbol,
                &mut live_orders,
                &mut restored_ids,
            )
            .await?;
        let live_ids: HashMap<String, BinanceOrderResponse> = live_orders
            .into_iter()
            .filter(|order| is_grid_order(&order.client_order_id))
            .map(|o| (o.client_order_id.clone(), o))
            .collect();
        let mut live_ids = self
            .adopt_conflict_free_snapshot(
                (priority, pause_epoch, account_key, symbol, mode),
                (pair_intents, remainder_plan),
                live_ids,
                restored_ids,
                &mut corrected_terminal,
            )
            .await?;
        for id in corrected_terminal.keys() {
            live_ids.remove(id);
        }
        Some((live_ids, corrected_terminal, resolved_conflict_ids))
    }

    async fn adopt_conflict_free_snapshot(
        &mut self,
        scope: (ReconciliationPriority, u64, [u8; 32], &str, TradingMode),
        recovery: (&HashMap<String, GridOrder>, Option<&RemainderPlan>),
        mut live_ids: HashMap<String, BinanceOrderResponse>,
        restored_ids: HashSet<String>,
        corrected_terminal: &mut HashMap<String, BinanceOrderResponse>,
    ) -> Option<HashMap<String, BinanceOrderResponse>> {
        let (priority, pause_epoch, account_key, symbol, mode) = scope;
        let (pair_intents, remainder_plan) = recovery;
        let uncertain_ids = self
            .reconcile_uncertain_zero_fills(
                account_key,
                symbol,
                &mut live_ids,
                pair_intents,
                remainder_plan,
                corrected_terminal,
            )
            .await?;
        if live_ids.values().any(invalid_snapshot_price) {
            let snapshot: Vec<_> = live_ids.values().cloned().collect();
            self.retain_invalid_price_snapshot(account_key, symbol, mode, &snapshot)
                .await;
            return None;
        }
        self.verify_and_adopt_open_snapshot(
            priority,
            pause_epoch,
            account_key,
            symbol,
            (pair_intents, remainder_plan),
            (live_ids, uncertain_ids, restored_ids),
        )
        .await
    }

    async fn fetch_open_orders_for_sync(
        &mut self,
        priority: ReconciliationPriority,
        symbol: &str,
    ) -> Option<Vec<BinanceOrderResponse>> {
        let open_orders = match priority {
            ReconciliationPriority::Periodic => {
                self.client.get_open_orders_for_periodic_sync(symbol).await
            }
            ReconciliationPriority::Critical => self.client.get_open_orders(symbol).await,
        };
        match open_orders {
            Ok(orders) => Some(orders),
            Err(e) => {
                warn!("Error during live orders sync: {}", e);
                self.mark_orders_reconciliation_blocked().await;
                None
            }
        }
    }

    pub(super) async fn sync_live_orders_with_priority(
        &mut self,
        priority: ReconciliationPriority,
    ) -> bool {
        let pause_epoch = self.state.pause_epoch();
        let (symbol, mode) = {
            let config = self.state.config.read().await;
            (
                config.exchange.symbol.clone(),
                config.exchange.trading_mode(),
            )
        };
        let account_key = self.client.account_identity_key();
        let Some((pair_intents, remainder_plan, restored_ids)) = self
            .prepare_live_orders_sync(account_key, &symbol, mode)
            .await
        else {
            return false;
        };
        let Some(live_orders) = self.fetch_open_orders_for_sync(priority, &symbol).await else {
            return false;
        };

        self.last_orders_sync = None;
        let invalid_price = live_orders
            .iter()
            .any(|order| is_grid_order(&order.client_order_id) && invalid_snapshot_price(order));
        let fill_guard = self
            .acquire_live_sync_fill_guard(
                account_key,
                &symbol,
                invalid_price,
                &live_orders,
                &pair_intents,
                remainder_plan.as_ref(),
            )
            .await;
        if invalid_price {
            self.retain_invalid_price_snapshot(account_key, &symbol, mode, &live_orders)
                .await;
            return false;
        }
        self.reconcile_fetched_live_orders(
            LiveSnapshotValidation {
                priority,
                pause_epoch,
                account_key,
                symbol: &symbol,
                mode,
                live_orders,
                pair_intents: &pair_intents,
                restored_ids,
            },
            remainder_plan,
            fill_guard,
        )
        .await
    }

    async fn reconcile_fetched_live_orders(
        &mut self,
        validation: LiveSnapshotValidation<'_>,
        mut remainder_plan: Option<RemainderPlan>,
        mut fill_guard: Option<crate::server::state::PendingFillReconciliation>,
    ) -> bool {
        let account_key = validation.account_key;
        let symbol = validation.symbol;
        let mode = validation.mode;
        let pause_epoch = validation.pause_epoch;
        let Some((live_ids, mut corrected_terminal, resolved_conflict_ids)) = self
            .validate_and_adopt_live_snapshot(validation, remainder_plan.as_ref())
            .await
        else {
            return false;
        };
        let Some(terminal_orders) = self
            .collect_missing_terminal_orders(
                account_key,
                symbol,
                pause_epoch,
                &live_ids,
                &mut corrected_terminal,
            )
            .await
        else {
            return false;
        };
        let Some(persistence_ok) = self
            .settle_terminal_and_pending_workflows(LiveSyncSettlement {
                account_key,
                symbol,
                mode,
                pause_epoch,
                terminal_orders,
                remainder_plan: &mut remainder_plan,
                has_fill_guard: fill_guard.is_some(),
                resolved_conflict_ids: &resolved_conflict_ids,
            })
            .await
        else {
            return false;
        };
        if persistence_ok {
            if let Some(guard) = &mut fill_guard {
                guard.resolve();
            }
        }
        drop(fill_guard);
        self.report_reconciliation_state(!persistence_ok).await;
        persistence_ok
    }
}
