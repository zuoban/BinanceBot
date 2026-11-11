use super::*;
use crate::config::AppConfig;
use crate::db::Database;
use crate::types::BotControlAction;

mod http_auth_and_dispatch;
mod probes_preview_and_config;
mod ws_and_accounts;

fn test_admin_auth() -> Option<Arc<crate::auth::AdminAuth>> {
    Some(Arc::new(
        crate::auth::AdminAuth::from_password("test-password").unwrap(),
    ))
}

pub(super) fn auth_state(
    config: AppConfig,
    db: Arc<Database>,
    action_tx: tokio::sync::mpsc::Sender<BotControlAction>,
) -> Arc<AppState> {
    AppState::new_with_admin_auth(config, db, action_tx, test_admin_auth())
}

pub(super) fn auth_pair_state(
    config: AppConfig,
    db: Arc<Database>,
    action_tx: tokio::sync::mpsc::Sender<BotControlAction>,
    trade_lock: Arc<tokio::sync::Mutex<()>>,
) -> Arc<AppState> {
    AppState::new_for_pair_with_admin_auth(config, db, action_tx, trade_lock, test_admin_auth())
}
