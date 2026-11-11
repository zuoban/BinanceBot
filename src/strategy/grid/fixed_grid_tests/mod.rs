use super::*;
use crate::{config::AppConfig, db::Database};
use rust_decimal_macros::dec;

async fn paper_engine() -> GridTradingEngine {
    paper_engine_with_db(Arc::new(Database::open(":memory:").unwrap())).await
}

async fn paper_engine_with_db(db: Arc<Database>) -> GridTradingEngine {
    let mut config = AppConfig::default();
    config.grid.order_amount_usdc = dec!(200);
    config.grid.buy_window = 3;
    config.grid.sell_window = 3;
    config.grid.max_position_usdc = Some(dec!(2000));
    let (tx, rx) = mpsc::channel(1);
    let (_, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, tx);
    {
        let mut ticker = state.ticker.write().await;
        ticker.last_price = dec!(118.84);
        ticker.update_time = Utc::now();
        ticker.mark_update_time = Utc::now();
    }
    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    GridTradingEngine::new(state, client, rx, ticker_rx)
}

fn order(id: &str, side: OrderSide, price: Decimal, quantity: Decimal) -> GridOrder {
    GridOrder {
        client_order_id: format!("gb_{id}"),
        order_id: None,
        symbol: "SOLUSDC".into(),
        side,
        price,
        quantity,
        amount_usdc: price.saturating_mul(quantity),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: 0,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: OrderPurpose::Grid,
        merge_sources: vec![],
    }
}

async fn orders(engine: &GridTradingEngine) -> Vec<GridOrder> {
    engine
        .state
        .active_orders
        .read()
        .await
        .values()
        .cloned()
        .collect()
}

fn journal_trade(id: usize, side: OrderSide, price: Decimal, quantity: Decimal) -> TradeRecord {
    TradeRecord {
        trade_id: format!("journal-{id}"),
        client_order_id: format!("gb_journal_{id}"),
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Paper,
        side,
        price,
        quantity,
        amount_usdc: price * quantity,
        realized_pnl: Decimal::ZERO,
        commission: Decimal::ZERO,
        pnl_verified: true,
        is_maker: true,
        timestamp: Utc::now(),
        note: String::new(),
    }
}

fn assert_execution_steps(trades: &[TradeRecord], interval: Decimal) {
    for pair in trades.windows(2) {
        assert!(
            (pair[1].price - pair[0].price).abs() >= interval,
            "consecutive executions repeated a reserved level: {pair:?}"
        );
    }
}

mod frontiers_and_cycles;
mod risk_and_limits;
mod window_maintenance;
