use super::{json_err, json_ok, ApiResponse};
use crate::server::state::AppState;
use crate::types::{BotControlAction, GridDirection, TradingMode};
use axum::extract::State;
use axum::Json;
use serde::Deserialize;
use std::sync::Arc;
use tracing::error;

#[derive(Debug, Deserialize)]
pub(crate) struct ControlRequest {
    pub action: String,
    pub expected_mode: Option<TradingMode>,
    #[serde(default)]
    pub expected_direction: Option<GridDirection>,
}

pub(super) type ControlReply = tokio::sync::oneshot::Receiver<Result<(), String>>;

// A persisted pause must reach the engine before a later resume can use its
// epoch. Keep this guard alive until the pause is queued or dispatch fails.
#[must_use = "dropping this guard immediately releases the lock or barrier"]
pub(super) struct PendingControlPause(Arc<AppState>);

impl PendingControlPause {
    pub(super) fn new(state: &Arc<AppState>, action: &BotControlAction) -> Option<Self> {
        if matches!(
            action,
            BotControlAction::Pause | BotControlAction::CancelAll
        ) {
            state
                .pending_control_pauses
                .send_modify(|count| *count += 1);
            Some(Self(state.clone()))
        } else {
            None
        }
    }
}

impl Drop for PendingControlPause {
    fn drop(&mut self) {
        self.0
            .pending_control_pauses
            .send_modify(|count| *count -= 1);
    }
}

fn require_expected_mode(
    payload: &ControlRequest,
    action_label: &str,
) -> Result<TradingMode, String> {
    payload
        .expected_mode
        .filter(|mode| *mode != TradingMode::Unknown)
        .ok_or_else(|| format!("{}需要提供确认时的运行模式", action_label))
}

pub(super) fn control_action(
    payload: &ControlRequest,
    expected_epoch: u64,
) -> Result<(BotControlAction, Option<ControlReply>), String> {
    match payload.action.to_lowercase().as_str() {
        "pause" => Ok((BotControlAction::Pause, None)),
        "cancel_all" => Ok((BotControlAction::CancelAll, None)),
        "resume" => {
            let expected_mode = require_expected_mode(payload, "恢复策略")?;
            let (reply, receiver) = tokio::sync::oneshot::channel();
            Ok((
                BotControlAction::Resume {
                    expected_mode,
                    expected_epoch,
                    reply,
                },
                Some(receiver),
            ))
        }
        "rebalance" => {
            let expected_mode = require_expected_mode(payload, "重建网格")?;
            let (reply, receiver) = tokio::sync::oneshot::channel();
            Ok((
                BotControlAction::Rebalance {
                    expected_mode,
                    reply,
                },
                Some(receiver),
            ))
        }
        "reverse" => {
            let expected_mode = require_expected_mode(payload, "反手交易")?;
            let expected_direction = payload
                .expected_direction
                .ok_or_else(|| "反手交易需要提供确认时的交易方向".to_string())?;
            let (reply, receiver) = tokio::sync::oneshot::channel();
            Ok((
                BotControlAction::Reverse {
                    expected_mode,
                    expected_direction,
                    reply,
                },
                Some(receiver),
            ))
        }
        _ => Err("未知操作指令".into()),
    }
}

pub(super) async fn post_control_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ControlRequest>,
) -> Json<ApiResponse<String>> {
    let action = match control_action(&payload, state.pause_epoch()) {
        Ok(action) => action,
        Err(message) => return json_err(message),
    };
    let result = tokio::spawn(dispatch_control(state, action)).await;
    match result {
        Ok(response) => response,
        Err(error) => json_err(format!("控制任务失败: {}", error)),
    }
}

pub(super) async fn prepare_control(
    state: &Arc<AppState>,
    action: &BotControlAction,
) -> Result<(), String> {
    let pause = matches!(
        action,
        BotControlAction::Pause | BotControlAction::CancelAll
    );
    let state = state.clone();
    tokio::spawn(async move {
        let _control = state.socket_control_lock.read().await;
        if state.is_retired() {
            return Err("交易对已移除，无法执行操作".into());
        }
        if pause {
            state
                .pause_trading()
                .await
                .map_err(|error| format!("暂停交易失败: {}", error))?;
        }
        Ok(())
    })
    .await
    .map_err(|error| format!("控制任务失败: {}", error))?
}

async fn dispatch_control(
    state: Arc<AppState>,
    action: (BotControlAction, Option<ControlReply>),
) -> Json<ApiResponse<String>> {
    let pending_pause = PendingControlPause::new(&state, &action.0);
    if let Err(message) = prepare_control(&state, &action.0).await {
        return json_err(message);
    }
    dispatch_prepared_control(state, action, pending_pause, None, None, None).await
}

fn control_action_label(action: &BotControlAction) -> &'static str {
    match action {
        BotControlAction::Pause | BotControlAction::PauseAlreadyPersisted => "pause",
        BotControlAction::Resume { .. } => "resume",
        BotControlAction::CancelAll | BotControlAction::CancelAllAlreadyPersisted => "cancel_all",
        BotControlAction::Rebalance { .. } => "rebalance",
        BotControlAction::Reverse { .. } => "reverse",
        BotControlAction::UpdateConfig { .. } => "update_config",
        BotControlAction::UpdateGlobalConfig { .. } => "update_global_config",
        BotControlAction::GlobalRiskCancel => "global_risk_cancel",
        BotControlAction::Shutdown { .. } => "shutdown",
    }
}

pub(super) async fn dispatch_prepared_control(
    state: Arc<AppState>,
    (action, reply): (BotControlAction, Option<ControlReply>),
    pending_pause: Option<PendingControlPause>,
    submitted: Option<tokio::sync::oneshot::Sender<()>>,
    socket_results: Option<tokio::sync::mpsc::Sender<String>>,
    socket_token: Option<String>,
) -> Json<ApiResponse<String>> {
    let action_name = control_action_label(&action);
    let action = match action {
        BotControlAction::Pause => BotControlAction::PauseAlreadyPersisted,
        BotControlAction::CancelAll => BotControlAction::CancelAllAlreadyPersisted,
        action => action,
    };
    let mut retired = state.subscribe_retirement();
    if *retired.borrow() {
        return json_err("交易对已移除，无法执行操作");
    }
    let reservation = tokio::select! {
        biased;
        _ = retired.changed() => return json_err("交易对已移除，无法执行操作"),
        result = state.action_tx.reserve() => result,
    };
    let permit = match reservation {
        Ok(permit) => permit,
        Err(e) => {
            error!("Failed to dispatch action: {}", e);
            return json_err(format!("Failed to send action: {}", e));
        }
    };
    let control = state.socket_control_lock.read().await;
    if state.is_retired() {
        return json_err("交易对已移除，无法执行操作");
    }
    if pending_pause.is_none() {
        if let Some(token) = &socket_token {
            if !state.is_token_valid(token).await {
                return json_err("会话已失效");
            }
        }
        if *state.pending_control_pauses.borrow() != 0 {
            return json_err("暂停或撤单正在提交，请稍后重试");
        }
    }
    permit.send(action);
    drop(pending_pause);
    drop(control);
    if let Some(submitted) = submitted {
        let _ = submitted.send(());
    }
    if let Some(reply) = reply {
        if let Err(message) =
            await_control_reply(reply, &mut retired, socket_results.as_ref()).await
        {
            return json_err(message);
        }
    }
    json_ok(format!("Action {} triggered successfully", action_name))
}

async fn await_control_reply(
    reply: ControlReply,
    retired: &mut tokio::sync::watch::Receiver<bool>,
    socket_results: Option<&tokio::sync::mpsc::Sender<String>>,
) -> Result<(), String> {
    let response = tokio::select! {
        biased;
        _ = retired.changed() => return Err("交易对已移除，无法执行操作".into()),
        _ = async {
            if let Some(results) = socket_results { results.closed().await; }
            else { std::future::pending::<()>().await; }
        } => return Err("控制连接已关闭".into()),
        response = reply => response,
    };
    match response {
        Ok(Ok(())) => Ok(()),
        Ok(Err(message)) => Err(message),
        Err(error) => Err(format!("策略未确认控制操作: {}", error)),
    }
}
