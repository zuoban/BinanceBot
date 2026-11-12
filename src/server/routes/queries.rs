use super::{json_err, json_ok, ApiResponse};
use crate::db::Database;
use crate::runtime::RuntimeHandle;
use crate::server::state::AppState;
use crate::types::{
    BotSnapshot, GridOrder, LogEntry, OverviewSnapshot, TradeRecord, TradingPairView,
};
use axum::extract::State;
use axum::Json;
use std::sync::Arc;
use tracing::error;

pub(super) async fn get_single_pair_handler(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<Vec<TradingPairView>>> {
    collect_pairs(std::slice::from_ref(&state)).await
}

pub(super) async fn get_single_overview_handler(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<OverviewSnapshot>> {
    collect_overview(std::slice::from_ref(&state), None).await
}

pub(super) async fn get_pairs_handler(
    State(states): State<Arc<Vec<Arc<AppState>>>>,
) -> Json<ApiResponse<Vec<TradingPairView>>> {
    collect_pairs(&states).await
}

async fn collect_pairs(states: &[Arc<AppState>]) -> Json<ApiResponse<Vec<TradingPairView>>> {
    let pairs = futures_util::future::join_all(states.iter().map(|state| async move {
        let config = state.config.read().await;
        TradingPairView {
            symbol: config.exchange.symbol.clone(),
            scoped: state.pair_scope().is_some(),
            status: *state.status.read().await,
            mode: config.exchange.trading_mode(),
            account_scope: config.exchange.account_scope(),
            grid_config: config.summary(),
        }
    }))
    .await;
    json_ok(pairs)
}

pub(super) async fn get_overview_handler(
    State(states): State<Arc<Vec<Arc<AppState>>>>,
) -> Json<ApiResponse<OverviewSnapshot>> {
    collect_overview(&states, None).await
}

async fn collect_overview(
    states: &[Arc<AppState>],
    db: Option<&Arc<Database>>,
) -> Json<ApiResponse<OverviewSnapshot>> {
    let generated_at = chrono::Utc::now();
    let today_pnl = match db.or_else(|| states.first().map(|s| &s.db)) {
        Some(db) => db.cached_today_pnl(generated_at).await,
        None => None,
    };
    let summaries = futures_util::future::join_all(
        states
            .iter()
            .map(|state| state.overview_snapshot(generated_at)),
    )
    .await;
    let mut pairs = Vec::with_capacity(summaries.len());
    let mut recent_trades = Vec::new();
    for (pair, trades) in summaries {
        pairs.push(pair);
        recent_trades.extend(trades);
    }
    crate::server::state::snapshot::sort_and_truncate_recent_trades(&mut recent_trades, 20);
    json_ok(OverviewSnapshot {
        generated_at,
        pairs,
        today_pnl,
        recent_trades,
    })
}

pub(super) async fn get_managed_pairs_handler(
    State(handle): State<RuntimeHandle>,
) -> Json<ApiResponse<Vec<TradingPairView>>> {
    let states = handle.states.read().await.clone();
    collect_pairs(&states).await
}

pub(super) async fn get_managed_overview_handler(
    State(handle): State<RuntimeHandle>,
) -> Json<ApiResponse<OverviewSnapshot>> {
    // Release the registry lock before collecting snapshots so dashboard reads
    // do not hold up pair additions or removals during database work.
    let states = handle.states.read().await.clone();
    collect_overview(&states, Some(&handle.db)).await
}

pub(super) async fn get_overview_history_handler(
    State(handle): State<RuntimeHandle>,
) -> Json<ApiResponse<crate::snapshots::RawOverviewHistory>> {
    let since = chrono::Utc::now().timestamp() - 8 * 24 * 3600;
    let result = async {
        let current_accounts = crate::snapshots::current_accounts(&handle).await?;
        let (snapshots, daily_snapshots) = handle
            .db
            .run_blocking(move |db| {
                Ok((
                    db.hourly_account_snapshots_raw(since)?,
                    db.daily_snapshots_raw(30)?,
                ))
            })
            .await?;
        Ok::<_, anyhow::Error>(crate::snapshots::RawOverviewHistory {
            snapshots,
            current_accounts,
            daily_snapshots,
        })
    }
    .await;
    match result {
        Ok(data) => json_ok(data),
        Err(error) => {
            tracing::warn!(%error, "Could not read account snapshots");
            json_err("无法读取历史快照")
        }
    }
}

pub(super) async fn get_status_handler(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<BotSnapshot>> {
    json_ok(state.snapshot().await)
}

pub(super) async fn get_orders_handler(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<Vec<GridOrder>>> {
    json_ok(state.sorted_active_orders().await)
}

pub(super) async fn get_trades_handler(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<Vec<TradeRecord>>> {
    let config = state.config.read().await;
    let symbol = config.exchange.symbol.clone();
    let mode = config.exchange.trading_mode();
    drop(config);
    if !state
        .recent_trades_dirty
        .load(std::sync::atomic::Ordering::SeqCst)
        && state
            .recent_trades_revision
            .load(std::sync::atomic::Ordering::Acquire)
            == state.db.symbol_trade_revision(&symbol)
    {
        let recent = state.recent_trades.read().await;
        let matched: Vec<TradeRecord> = recent
            .iter()
            .rev()
            .filter(|t| t.symbol == symbol && t.mode == mode)
            .take(100)
            .cloned()
            .collect();
        if matched.len() == 100 || recent.iter().all(|t| t.symbol == symbol && t.mode == mode) {
            return json_ok(matched);
        }
    }
    let query_symbol = symbol.clone();
    let trades = match state
        .db
        .run_blocking(move |db| db.get_recent_trades_for(&query_symbol, mode, 100))
        .await
    {
        Ok(t) => t,
        Err(e) => {
            error!("Failed to fetch trades from SQLite: {}", e);
            state
                .recent_trades
                .read()
                .await
                .iter()
                .filter(|t| t.symbol == symbol && t.mode == mode)
                .rev()
                .take(100)
                .cloned()
                .collect()
        }
    };
    json_ok(trades)
}

pub(super) async fn get_logs_handler(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<Vec<LogEntry>>> {
    let logs: Vec<LogEntry> = state
        .recent_logs
        .read()
        .await
        .iter()
        .rev()
        .cloned()
        .collect();
    json_ok(logs)
}
