use super::*;
use crate::{config::AppConfig, db::Database};
use rust_decimal_macros::dec;

fn source(id: &str, price: Decimal, quantity: Decimal) -> GridOrder {
    GridOrder {
        client_order_id: format!("gb_s_{id}"),
        order_id: None,
        symbol: "SOLUSDC".into(),
        side: OrderSide::Sell,
        price,
        quantity,
        amount_usdc: price * quantity,
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: 1,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: OrderPurpose::Remainder,
        merge_sources: vec![],
    }
}

async fn engine(db: Arc<Database>, url: Option<String>) -> GridTradingEngine {
    let mut config = AppConfig::default();
    if url.is_some() {
        config.exchange.api_key = "live-key".into();
        config.exchange.api_secret = "live-secret".into();
    }
    config.grid.grid_interval = dec!(1.2);
    config.grid.order_amount_usdc = dec!(3000);
    config.grid.buy_window = 0;
    config.grid.sell_window = 4;
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db.clone(), action_tx);
    {
        let mut ticker = state.ticker.write().await;
        ticker.last_price = dec!(122.31);
        ticker.update_time = Utc::now();
        ticker.mark_update_time = Utc::now();
    }
    state.position.write().await.size = dec!(0.48);
    if url.is_some() {
        for order in db.load_managed_orders("SOLUSDC").unwrap() {
            state
                .active_orders
                .write()
                .await
                .insert(order.client_order_id.clone(), order);
        }
    }
    let client = Arc::new(match url {
        Some(url) => BinanceFuturesClient::with_base_url(&config.exchange, url),
        None => BinanceFuturesClient::new(&config.exchange),
    });
    let mut engine = GridTradingEngine::new(state, client, action_rx, ticker_rx);
    engine.last_account_sync = Some(Instant::now());
    engine.last_orders_sync = Some(Instant::now());
    engine
}

fn database() -> Arc<Database> {
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    db
}

async fn seed(engine: &GridTradingEngine, orders: &[GridOrder]) {
    for order in orders {
        engine.state.db.save_managed_order(order).unwrap();
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(order.client_order_id.clone(), order.clone());
    }
}

fn two_sources() -> Vec<GridOrder> {
    vec![
        source("a", dec!(123.6), dec!(0.24)),
        source("b", dec!(126), dec!(0.24)),
    ]
}

mod live_recovery;
mod paper_planning;
