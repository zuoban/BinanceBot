use super::{json_err, json_ok, json_ok_msg, status_err, ApiResponse, ClearTradeHistoryRequest};
use crate::config::{AppConfig, ServerConfig};
use crate::runtime::RuntimeHandle;
use crate::server::state::AppState;
use crate::strategy::SymbolRules;
use crate::types::{
    BotControlAction, UpdateConfigPayload, UpdateGlobalConfigPayload, WebConfigView,
    WebGlobalConfigView,
};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use std::future::Future;
use std::sync::Arc;
use tracing::{error, info, warn};

pub(super) async fn add_pair_handler(
    State(handle): State<RuntimeHandle>,
    Json(payload): Json<UpdateConfigPayload>,
) -> Json<ApiResponse<String>> {
    let result = async {
        anyhow::ensure!(
            !payload.has_global_updates(),
            "连接和 Telegram 通知请在全局设置中保存"
        );
        let saved = handle.db.load_saved_config().await?;
        let mut defaults = AppConfig::default();
        crate::config::GlobalConfig::from_app(&saved).apply_to(&mut defaults);
        let config = apply_config_payload(defaults, payload)?;
        let symbol = config.exchange.symbol.clone();
        handle.add_pair(config).await?;
        Ok::<_, anyhow::Error>(symbol)
    }
    .await;
    match result {
        Ok(symbol) => json_ok_msg(
            symbol,
            "交易对已添加并保存，策略保持暂停；检查配置后手动恢复",
        ),
        Err(error) => json_err(error.to_string()),
    }
}

pub(super) async fn remove_pair_handler(
    State(handle): State<RuntimeHandle>,
    Path(symbol): Path<String>,
    Json(payload): Json<ClearTradeHistoryRequest>,
) -> Json<ApiResponse<String>> {
    if !payload.confirm {
        return json_err("请确认移除交易对；移除前须平仓，历史记录会保留");
    }
    match handle.remove_pair(symbol.clone()).await {
        Ok(()) => json_ok_msg(symbol, "交易对已移除，历史记录保留"),
        Err(error) => json_err(error.to_string()),
    }
}

pub(super) async fn post_managed_config_handler(
    State(state): State<Arc<AppState>>,
    Extension(handle): Extension<RuntimeHandle>,
    Json(payload): Json<UpdateConfigPayload>,
) -> (StatusCode, Json<ApiResponse<WebConfigView>>) {
    spawn_config_update(async move {
        let _membership = handle.membership_lock.lock().await;
        if !handle
            .states
            .read()
            .await
            .iter()
            .any(|current| Arc::ptr_eq(current, &state))
        {
            return status_err(StatusCode::CONFLICT, "交易对已被删除或替换，请刷新后重试");
        }
        apply_pair_config(state, payload).await
    })
    .await
}

async fn map_saved_config<T>(
    handle: &RuntimeHandle,
    missing_msg: &'static str,
    map: impl FnOnce(AppConfig) -> T,
) -> Json<ApiResponse<T>> {
    match handle.db.load_saved_config().await {
        Ok(config) => json_ok(map(config)),
        Err(_) => json_err(missing_msg),
    }
}

pub(super) async fn get_global_config_handler(
    State(handle): State<RuntimeHandle>,
) -> Json<ApiResponse<WebGlobalConfigView>> {
    map_saved_config(&handle, "无法读取全局设置", |config| {
        let global = crate::config::GlobalConfig::from_app(&config);
        web_global_config_view(&global)
    })
    .await
}

fn validate_optional_baseline_version(
    baseline_version: Option<&str>,
    kind: &'static str,
) -> Result<(), &'static str> {
    match baseline_version {
        Some(version) if version.trim().is_empty() => Err("配置版本不能为空"),
        None => {
            warn!("{kind} configuration update submitted without baseline_version");
            Ok(())
        }
        Some(_) => Ok(()),
    }
}

fn versioned_update_error<T>(error: &anyhow::Error) -> (StatusCode, Json<ApiResponse<T>>) {
    let status = if error
        .downcast_ref::<crate::config::ConfigVersionConflict>()
        .is_some()
    {
        StatusCode::CONFLICT
    } else {
        StatusCode::OK
    };
    (status, json_err(error.to_string()))
}

pub(super) async fn post_global_config_handler(
    State(handle): State<RuntimeHandle>,
    Json(payload): Json<UpdateGlobalConfigPayload>,
) -> (StatusCode, Json<ApiResponse<WebGlobalConfigView>>) {
    if let Err(message) =
        validate_optional_baseline_version(payload.baseline_version.as_deref(), "Global")
    {
        return status_err(StatusCode::BAD_REQUEST, message);
    }
    if let Err(error) = crate::config::validate_global_position_limit(
        payload.global_max_position_notional.flatten(),
    ) {
        return (StatusCode::OK, json_err(error.to_string()));
    }
    if let Err(error) =
        crate::config::validate_min_available_margin_pct(payload.min_available_margin_pct.flatten())
    {
        return (StatusCode::OK, json_err(error.to_string()));
    }
    match handle.update_global_config(payload).await {
        Ok(global) => {
            let view = web_global_config_view(&global);
            (
                StatusCode::OK,
                json_ok_msg(view, "全局设置已保存，所有交易对立即生效"),
            )
        }
        Err(error) => versioned_update_error(&error),
    }
}

fn web_global_config_view(global: &crate::config::GlobalConfig) -> WebGlobalConfigView {
    WebGlobalConfigView {
        version: crate::db::config_version(global),
        global_max_position_notional: global.global_max_position_notional,
        pause_on_zero_available_margin: global.pause_on_zero_available_margin,
        min_available_margin_pct: global.min_available_margin_pct,
        recv_window: global.recv_window,
        sync_interval_secs: global.sync_interval_secs,
        has_api_key: !global.api_key.trim().is_empty(),
        has_api_secret: !global.api_secret.trim().is_empty(),
        api_key_preview: mask_api_key_preview(&global.api_key),
        telegram_enabled: global.telegram.enabled,
        has_telegram_bot_token: !global.telegram.bot_token.trim().is_empty(),
        telegram_chat_id: global.telegram.chat_id.clone(),
    }
}

#[derive(serde::Serialize)]
pub(super) struct ServerConfigView {
    version: String,
    saved: ServerConfig,
    active: ServerConfig,
}

pub(super) async fn get_server_config_handler(
    State(handle): State<RuntimeHandle>,
) -> Json<ApiResponse<ServerConfigView>> {
    let active = handle.active_server.clone();
    map_saved_config(&handle, "无法读取服务设置", |config| {
        ServerConfigView {
            version: crate::db::config_version(&config.server),
            saved: config.server,
            active,
        }
    })
    .await
}

#[derive(serde::Deserialize)]
pub(super) struct UpdateServerConfigPayload {
    #[serde(flatten)]
    server: ServerConfig,
    baseline_version: Option<String>,
}

pub(super) async fn post_server_config_handler(
    State(handle): State<RuntimeHandle>,
    Json(payload): Json<UpdateServerConfigPayload>,
) -> (StatusCode, Json<ApiResponse<ServerConfigView>>) {
    if let Err(message) =
        validate_optional_baseline_version(payload.baseline_version.as_deref(), "Server")
    {
        return status_err(StatusCode::BAD_REQUEST, message);
    }
    let server = payload.server;
    let submitted = server.clone();
    let result = handle
        .db
        .run_blocking(move |db| {
            db.save_server_config_checked(&server, payload.baseline_version.as_deref())
        })
        .await;
    match result {
        Ok(()) => (
            StatusCode::OK,
            json_ok_msg(
                ServerConfigView {
                    version: crate::db::config_version(&submitted),
                    saved: submitted,
                    active: handle.active_server,
                },
                "服务设置已保存，重启程序后生效",
            ),
        ),
        Err(error) => versioned_update_error(&error),
    }
}

pub(super) async fn get_config_handler(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<WebConfigView>> {
    let config = state.config.read().await;
    let rules = state.rules.read().await;
    json_ok(web_config_view(&config, state.config_instance, &rules))
}

pub(super) fn pair_config_version(config: &AppConfig, instance: u128) -> String {
    crate::db::config_version(&(
        instance,
        &config.exchange,
        &config.grid,
        &config.telegram,
        config.global_max_position_notional,
        config.pause_on_zero_available_margin,
        config.min_available_margin_pct,
    ))
}

pub(super) fn mask_api_key_preview(api_key: &str) -> String {
    let k = api_key.trim();
    if k.is_empty() {
        return "未配置".to_string();
    }
    let mut prefix_end = 0;
    let mut tail = [0usize; 4];
    let mut count = 0usize;
    for (idx, _) in k.char_indices() {
        if count == 4 {
            prefix_end = idx;
        }
        tail[count % 4] = idx;
        count += 1;
    }
    if count > 8 {
        let suffix_start = tail[count % 4];
        format!("{}...{}", &k[..prefix_end], &k[suffix_start..])
    } else {
        "****".to_string()
    }
}

fn web_config_view(config: &AppConfig, instance: u128, rules: &SymbolRules) -> WebConfigView {
    let global = web_global_config_view(&crate::config::GlobalConfig::from_app(config));
    let matching_rules = rules.symbol.eq_ignore_ascii_case(&config.exchange.symbol);
    let positive_rule = |value: rust_decimal::Decimal| {
        (matching_rules && value > rust_decimal::Decimal::ZERO).then_some(value)
    };

    WebConfigView {
        config_version: pair_config_version(config, instance),
        symbol: config.exchange.symbol.clone(),
        direction: config.grid.direction,
        grid_interval: config.grid.grid_interval,
        order_amount_usdc: config.grid.order_amount_usdc,
        buy_window: config.grid.buy_window,
        sell_window: config.grid.sell_window,
        post_only: config.grid.post_only,
        recv_window: global.recv_window,
        sync_interval_secs: global.sync_interval_secs,
        has_api_key: global.has_api_key,
        has_api_secret: global.has_api_secret,
        api_key_preview: global.api_key_preview,
        telegram_enabled: global.telegram_enabled,
        has_telegram_bot_token: global.has_telegram_bot_token,
        telegram_chat_id: global.telegram_chat_id,
        min_price: config.grid.min_price,
        max_price: config.grid.max_price,
        max_position_usdc: config.grid.max_position_usdc,
        pause_on_max_position: config.grid.pause_on_max_position,
        cancel_opening_on_max_position: config.grid.cancel_opening_on_max_position,
        min_liquidation_distance_pct: config.grid.min_liquidation_distance_pct,
        tick_size: positive_rule(rules.tick_size),
        step_size: positive_rule(rules.step_size),
        min_qty: positive_rule(rules.min_qty),
        min_notional: positive_rule(rules.min_notional),
    }
}

pub(super) fn apply_config_payload(
    mut current_config: AppConfig,
    payload: UpdateConfigPayload,
) -> anyhow::Result<AppConfig> {
    anyhow::ensure!(
        payload.dry_run.is_none(),
        "运行模式已由 API Key 决定，请刷新页面后重试"
    );
    anyhow::ensure!(
        !payload.is_testnet.unwrap_or(false),
        "测试网模式已移除，运行模式由 API Key 决定"
    );
    current_config.exchange.symbol = payload.symbol.trim().to_uppercase();
    if let Some(direction) = payload.direction {
        current_config.grid.direction = direction;
    }
    current_config.grid.grid_interval = payload.grid_interval;
    current_config.grid.order_amount_usdc = payload.order_amount_usdc;
    current_config.grid.buy_window = payload.buy_window;
    current_config.grid.sell_window = payload.sell_window;
    if let Some(post_only) = payload.post_only {
        current_config.grid.post_only = post_only;
    }
    current_config.grid.min_price = payload.min_price;
    current_config.grid.max_price = payload.max_price;
    current_config.grid.max_position_usdc = payload.max_position_usdc;
    if let Some(enabled) = payload.pause_on_max_position {
        current_config.grid.pause_on_max_position = enabled;
    }
    if let Some(enabled) = payload.cancel_opening_on_max_position {
        current_config.grid.cancel_opening_on_max_position = enabled;
    }
    current_config.grid.min_liquidation_distance_pct = payload.min_liquidation_distance_pct;

    let mut global = crate::config::GlobalConfig::from_app(&current_config);
    UpdateGlobalConfigPayload {
        recv_window: payload.recv_window,
        sync_interval_secs: payload.sync_interval_secs,
        api_key: payload.api_key,
        api_secret: payload.api_secret,
        clear_api_credentials: payload.clear_api_credentials,
        clear_telegram_credentials: payload.clear_telegram_credentials,
        telegram_enabled: payload.telegram_enabled,
        telegram_bot_token: payload.telegram_bot_token,
        telegram_chat_id: payload.telegram_chat_id,
        ..Default::default()
    }
    .apply_to(&mut global);
    global.apply_to(&mut current_config);
    current_config.validate()?;
    Ok(current_config)
}

async fn spawn_config_update<F>(fut: F) -> (StatusCode, Json<ApiResponse<WebConfigView>>)
where
    F: Future<Output = (StatusCode, Json<ApiResponse<WebConfigView>>)> + Send + 'static,
{
    match tokio::spawn(fut).await {
        Ok(result) => result,
        Err(error) => status_err(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("配置更新任务异常退出: {}", error),
        ),
    }
}

pub(super) async fn post_config_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<UpdateConfigPayload>,
) -> (StatusCode, Json<ApiResponse<WebConfigView>>) {
    spawn_config_update(apply_pair_config(state, payload)).await
}

async fn apply_pair_config(
    state: Arc<AppState>,
    payload: UpdateConfigPayload,
) -> (StatusCode, Json<ApiResponse<WebConfigView>>) {
    let _update = state.config_update_lock.lock().await;
    let current = state.config.read().await.clone();
    let Some(version) = payload
        .baseline_version
        .as_deref()
        .filter(|version| !version.trim().is_empty())
    else {
        return status_err(
            StatusCode::BAD_REQUEST,
            "更新交易对设置时必须提供 baseline_version",
        );
    };
    if version != pair_config_version(&current, state.config_instance) {
        return status_err(
            StatusCode::CONFLICT,
            "交易对设置已被其他请求修改，请刷新后重试",
        );
    }
    (
        StatusCode::OK,
        apply_pair_config_inner(state.clone(), current, payload).await,
    )
}

async fn apply_pair_config_inner(
    state: Arc<AppState>,
    current: AppConfig,
    payload: UpdateConfigPayload,
) -> Json<ApiResponse<WebConfigView>> {
    if state.pair_scope().is_some() && payload.has_global_updates() {
        return json_err("连接和 Telegram 通知请在全局设置中保存");
    }
    let current_config = match apply_config_payload(current, payload) {
        Ok(config) => config,
        Err(error) => return json_err(error.to_string()),
    };
    if let Some(symbol) = state.pair_scope() {
        if symbol != current_config.exchange.symbol {
            return json_err("交易对不能直接重命名，请在页面中添加新交易对");
        }
    }

    // The engine applies the change and persists it only after the exchange
    // transition succeeds. Wait for its result before reporting success.
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    if let Err(e) = state
        .action_tx
        .send(BotControlAction::UpdateConfig {
            config: Box::new(current_config.clone()),
            reply: reply_tx,
        })
        .await
    {
        error!("Failed to notify strategy engine of config update: {}", e);
        return json_err(format!("策略更新通知失败: {}", e));
    }
    match reply_rx.await {
        Ok(Ok(())) => info!("Configuration applied and saved to {}", state.db.path()),
        Ok(Err(error)) => return json_err(error),
        Err(error) => return json_err(format!("策略未确认配置变更: {}", error)),
    }

    let mut response = get_config_handler(State(state)).await;
    response.0.message = Some("配置已保存至 SQLite 数据库并立即生效".into());
    response
}
