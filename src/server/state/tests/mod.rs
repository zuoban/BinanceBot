use super::*;
use crate::types::{OrderSide, TradingMode};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

mod auth_and_lifecycle;
mod global_risk;
mod notifications;
mod trades_and_stats_cache;

pub(super) fn cache_test_trade(id: &str, seconds: i64, mode: TradingMode) -> TradeRecord {
    TradeRecord {
        trade_id: id.into(),
        client_order_id: format!("gb_b_{id}"),
        symbol: "SOLUSDC".into(),
        mode,
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(1),
        amount_usdc: dec!(100),
        realized_pnl: Decimal::ZERO,
        commission: Decimal::ZERO,
        pnl_verified: false,
        is_maker: true,
        timestamp: chrono::DateTime::from_timestamp(1_700_000_000 + seconds, 0).unwrap(),
        note: String::new(),
    }
}

pub(super) struct NotificationFixture {
    pub(super) state: Arc<AppState>,
    pub(super) requests: mpsc::UnboundedReceiver<serde_json::Value>,
    pub(super) actions: mpsc::Receiver<BotControlAction>,
    pub(super) url: String,
    pub(super) server: tokio::task::JoinHandle<()>,
}

pub(super) async fn notification_fixture(enabled: bool, reject: bool) -> NotificationFixture {
    use axum::{routing::post, Json, Router};
    let (request_tx, requests) = mpsc::unbounded_channel();
    let app = Router::new().route(
        "/botTEST/sendMessage",
        post(move |Json(payload): Json<serde_json::Value>| {
            let tx = request_tx.clone();
            async move {
                tx.send(payload).unwrap();
                Json(serde_json::json!({"ok": !reject, "description": "test rejection"}))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut config = AppConfig::default();
    config.telegram = crate::config::TelegramConfig {
        enabled,
        bot_token: "TEST".into(),
        chat_id: "12345".into(),
    };
    let (action_tx, actions) = mpsc::channel(8);
    let mut state = AppState::new(
        config,
        Arc::new(Database::open(":memory:").unwrap()),
        action_tx,
    );
    Arc::get_mut(&mut state).unwrap().telegram_notifier =
        TelegramNotifier::with_base_url(reqwest::Client::new(), url.clone());
    NotificationFixture {
        state,
        requests,
        actions,
        url,
        server,
    }
}
