use super::{dispatch_pair, json_ok, json_ok_msg, status_err, ApiResponse};
use crate::runtime::RuntimeHandle;
use crate::server::state::{AppState, AuthContext};
use crate::types::{beijing_offset, BotSnapshot};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use std::future::Future;
use std::sync::Arc;
use tracing::error;

#[derive(Debug, Deserialize)]
pub struct ClearTradeHistoryRequest {
    #[serde(default)]
    pub confirm: bool,
}

#[derive(Debug, serde::Serialize)]
pub struct ClearTradeHistoryResponse {
    pub deleted_trades: usize,
    pub snapshot: BotSnapshot,
}

#[derive(Debug, Deserialize)]
pub(super) struct DatabaseTargetsRequest {
    targets: Vec<String>,
    #[serde(default)]
    confirm: bool,
}

impl DatabaseTargetsRequest {
    fn selected_targets(&self) -> Option<(bool, bool)> {
        let mut trades = false;
        let mut snapshots = false;
        for target in &self.targets {
            match target.as_str() {
                "trade_history" => trades = true,
                "account_hourly_snapshots" => snapshots = true,
                _ => return None,
            }
        }
        (trades || snapshots).then_some((trades, snapshots))
    }

    fn confirmed_targets(&self) -> Option<(bool, bool)> {
        self.confirm.then(|| self.selected_targets()).flatten()
    }
}

#[derive(serde::Serialize)]
struct DatabaseClearResponse {
    deleted_trades: usize,
    deleted_account_hourly_snapshots: usize,
}

pub(super) async fn single_pair_export_handler(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
) -> Response {
    if state.config.read().await.exchange.symbol != symbol {
        return StatusCode::NOT_FOUND.into_response();
    }
    export_symbol(&state, symbol).await
}

pub(super) async fn single_pair_clear_handler(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
    payload: Json<ClearTradeHistoryRequest>,
) -> Response {
    if state.config.read().await.exchange.symbol != symbol {
        return StatusCode::NOT_FOUND.into_response();
    }
    clear_symbol(&state, symbol, payload.0)
        .await
        .into_response()
}

pub(super) async fn dispatch_managed_database_pair(
    State(handle): State<RuntimeHandle>,
    req: axum::extract::Request,
) -> Response {
    let _membership = handle.membership_lock.lock().await;
    dispatch_pair(State(handle.clone()), req).await
}

pub(super) async fn scoped_export_handler(State(state): State<Arc<AppState>>) -> Response {
    let symbol = state.config.read().await.exchange.symbol.clone();
    export_symbol(&state, symbol).await
}

async fn export_symbol(state: &AppState, symbol: String) -> Response {
    database_export_response(
        state
            .db
            .run_blocking(move |db| db.export_analysis_snapshot_for_symbol(&symbol))
            .await,
    )
}

pub(super) async fn scoped_clear_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ClearTradeHistoryRequest>,
) -> (StatusCode, Json<ApiResponse<ClearTradeHistoryResponse>>) {
    let symbol = state.config.read().await.exchange.symbol.clone();
    clear_symbol(&state, symbol, payload).await
}

async fn clear_symbol(
    state: &Arc<AppState>,
    symbol: String,
    payload: ClearTradeHistoryRequest,
) -> (StatusCode, Json<ApiResponse<ClearTradeHistoryResponse>>) {
    if !payload.confirm {
        return status_err(
            StatusCode::BAD_REQUEST,
            "请确认清理当前币种所有模式的成交历史",
        );
    }
    match state.clear_symbol_trade_history(symbol).await {
        Ok((deleted_trades, snapshot)) => (
            StatusCode::OK,
            json_ok(ClearTradeHistoryResponse {
                deleted_trades,
                snapshot,
            }),
        ),
        Err(error) => {
            error!("Scoped trade cleanup failed: {}", error);
            status_err(StatusCode::INTERNAL_SERVER_ERROR, "历史记录清理失败")
        }
    }
}

pub(super) async fn export_selected_database_handler(
    State(auth): State<Arc<AuthContext>>,
    Json(payload): Json<DatabaseTargetsRequest>,
) -> Response {
    let Some((trades, snapshots)) = payload.selected_targets() else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    database_export_response(
        auth.db
            .run_blocking(move |db| db.export_selected_analysis_snapshot(trades, snapshots))
            .await,
    )
}

pub(super) async fn single_database_clear_handler(
    State(state): State<Arc<AppState>>,
    payload: Json<DatabaseTargetsRequest>,
) -> Response {
    database_clear(std::slice::from_ref(&state), payload.0).await
}

pub(super) async fn multi_database_clear_handler(
    State(states): State<Arc<Vec<Arc<AppState>>>>,
    payload: Json<DatabaseTargetsRequest>,
) -> Response {
    database_clear(&states, payload.0).await
}

pub(super) async fn managed_database_clear_handler(
    State(handle): State<RuntimeHandle>,
    payload: Json<DatabaseTargetsRequest>,
) -> Response {
    let Some((trades, snapshots)) = payload.confirmed_targets() else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let membership = handle.membership_lock.clone().lock_owned().await;
    let states = handle.states.read().await.clone();
    database_clear_with_guard(&states, trades, snapshots, Some(membership)).await
}

async fn database_clear(states: &[Arc<AppState>], payload: DatabaseTargetsRequest) -> Response {
    let Some((trades, snapshots)) = payload.confirmed_targets() else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    database_clear_with_guard(states, trades, snapshots, None).await
}

async fn database_clear_with_guard(
    states: &[Arc<AppState>],
    trades: bool,
    snapshots: bool,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
) -> Response {
    if states.is_empty() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let (deleted_trades, deleted_account_hourly_snapshots) =
        match AppState::clear_selected_database_for(states, trades, snapshots, guard).await {
            Ok(counts) => counts,
            Err(error) => {
                error!("Database cleanup failed: {}", error);
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        };
    json_ok(DatabaseClearResponse {
        deleted_trades,
        deleted_account_hourly_snapshots,
    })
    .into_response()
}

pub(super) async fn clear_managed_history_handler(
    State(handle): State<RuntimeHandle>,
    Json(payload): Json<ClearTradeHistoryRequest>,
) -> (StatusCode, Json<ApiResponse<ClearTradeHistoryResponse>>) {
    // Keep the set of caches stable while resetting shared history and statistics.
    let _membership = handle.membership_lock.lock().await;
    clear_multi_trade_history_handler(
        State(Arc::new(handle.states.read().await.clone())),
        Json(payload),
    )
    .await
}

async fn run_clear_trade_history(
    states: &[Arc<AppState>],
    payload: ClearTradeHistoryRequest,
    clear: impl Future<Output = anyhow::Result<usize>>,
    log_msg: impl FnOnce(usize) -> String,
    response_msg: impl FnOnce(usize) -> String,
) -> (StatusCode, Json<ApiResponse<ClearTradeHistoryResponse>>) {
    if !payload.confirm {
        return status_err(
            StatusCode::BAD_REQUEST,
            "请先确认清理全部交易对和运行模式的历史成交及累计统计",
        );
    }
    match clear.await {
        Ok(deleted_trades) => {
            // Invalidate every client's cached statistics after the shared database cleanup.
            let log_message = log_msg(deleted_trades);
            let mut primary_snapshot = None;
            for state in states {
                state.add_log("WARN", log_message.clone()).await;
                let snapshot = state.broadcast_snapshot().await;
                if primary_snapshot.is_none() {
                    primary_snapshot = Some(snapshot);
                }
            }
            let Some(snapshot) = primary_snapshot else {
                return status_err(StatusCode::INTERNAL_SERVER_ERROR, "没有可用的交易对状态");
            };
            (
                StatusCode::OK,
                json_ok_msg(
                    ClearTradeHistoryResponse {
                        deleted_trades,
                        snapshot,
                    },
                    response_msg(deleted_trades),
                ),
            )
        }
        Err(error) => {
            error!("Trade history cleanup failed: {}", error);
            status_err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "历史记录清理失败，请稍后重试或查看服务器日志",
            )
        }
    }
}

pub(super) async fn clear_multi_trade_history_handler(
    State(states): State<Arc<Vec<Arc<AppState>>>>,
    Json(payload): Json<ClearTradeHistoryRequest>,
) -> (StatusCode, Json<ApiResponse<ClearTradeHistoryResponse>>) {
    run_clear_trade_history(
        &states,
        payload,
        AppState::clear_trade_history_for(&states),
        |deleted_trades| format!("已清理全部历史成交记录，共 {} 笔", deleted_trades),
        |deleted_trades| {
            format!(
                "已清理 {} 笔历史成交记录，全部交易对累计统计已重置",
                deleted_trades
            )
        },
    )
    .await
}

pub(super) async fn clear_trade_history_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ClearTradeHistoryRequest>,
) -> (StatusCode, Json<ApiResponse<ClearTradeHistoryResponse>>) {
    run_clear_trade_history(
        std::slice::from_ref(&state),
        payload,
        state.clear_trade_history(),
        |deleted_trades| {
            format!(
                "已清理全部历史成交记录，共 {} 笔，累计统计已重置",
                deleted_trades
            )
        },
        |deleted_trades| format!("已清理 {} 笔历史成交记录，累计统计已重置", deleted_trades),
    )
    .await
}

pub(super) async fn export_database_handler(State(auth): State<Arc<AuthContext>>) -> Response {
    database_export_response(
        auth.db
            .run_blocking(|db| db.export_analysis_snapshot())
            .await,
    )
}

fn database_export_response(result: anyhow::Result<Vec<u8>>) -> Response {
    match result {
        Ok(bytes) => {
            let filename = format!(
                "binancebot-analysis-{}.db",
                chrono::Utc::now()
                    .with_timezone(&beijing_offset())
                    .format("%Y%m%d-%H%M%S")
            );
            (
                [
                    (
                        axum::http::header::CONTENT_TYPE,
                        "application/vnd.sqlite3".to_string(),
                    ),
                    (
                        axum::http::header::CONTENT_DISPOSITION,
                        format!("attachment; filename=\"{}\"", filename),
                    ),
                    (axum::http::header::CACHE_CONTROL, "no-store".to_string()),
                    (
                        axum::http::header::X_CONTENT_TYPE_OPTIONS,
                        "nosniff".to_string(),
                    ),
                ],
                bytes,
            )
                .into_response()
        }
        Err(error) => {
            error!("Database analysis export failed: {}", error);
            status_err::<()>(
                StatusCode::INTERNAL_SERVER_ERROR,
                "数据库导出失败，请稍后重试或查看服务器日志",
            )
            .into_response()
        }
    }
}
