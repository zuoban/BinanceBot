use binance_grid_bot::auth::AdminAuth;
use binance_grid_bot::config::AppConfig;
use binance_grid_bot::db::Database;
use binance_grid_bot::server::AppState;
use binance_grid_bot::types::BotControlAction;
use std::sync::Arc;
use tokio::sync::mpsc;

pub fn auth_state(
    config: AppConfig,
    db: Arc<Database>,
    action_tx: mpsc::Sender<BotControlAction>,
    password: &str,
) -> Arc<AppState> {
    AppState::new_with_admin_auth(
        config,
        db,
        action_tx,
        Some(Arc::new(AdminAuth::from_password(password).unwrap())),
    )
}
