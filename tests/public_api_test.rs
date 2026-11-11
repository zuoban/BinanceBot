mod common;
use common::{http, trades};

use binance_grid_bot::config::AppConfig;
use binance_grid_bot::db::Database;
use binance_grid_bot::exchange::BinanceFuturesClient;
use binance_grid_bot::server::AppState;
use binance_grid_bot::strategy::GridTradingEngine;
use binance_grid_bot::types::{GridOrder, OrderPurpose, OrderSide, OrderStatus, TradingMode};
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};

fn create_test_order(
    client_order_id: &str,
    symbol: &str,
    side: OrderSide,
    price: Decimal,
    quantity: Decimal,
    grid_level: i32,
    paired_client_order_id: Option<String>,
) -> GridOrder {
    GridOrder {
        client_order_id: client_order_id.to_string(),
        order_id: None,
        symbol: symbol.to_string(),
        side,
        price,
        quantity,
        amount_usdc: price * quantity,
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level,
        paired_client_order_id,
        is_take_profit: false,
        purpose: OrderPurpose::Grid,
        merge_sources: Vec::new(),
    }
}

use trades::sample_trade as create_test_trade;

#[tokio::test]
async fn test_non_cfg_test_consumer_constructs_grid_trading_engine() {
    let mut config = AppConfig::default();
    config.exchange.symbol = "SOLUSDC".to_string();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(16);
    let state = AppState::new(config.clone(), db.clone(), action_tx);
    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let (_ticker_tx, ticker_rx) = broadcast::channel(16);

    // Integration tests compile against the library without cfg(test).
    let _engine = GridTradingEngine::new(state, client, action_rx, ticker_rx);
}

#[tokio::test]
async fn overview_is_lightweight_while_status_keeps_full_detail() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use binance_grid_bot::server::create_router;
    use tower::ServiceExt;

    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, _) = mpsc::channel(8);
    let state = AppState::new(AppConfig::default(), db, tx);
    let buy = create_test_order(
        "summary-buy",
        "SOLUSDC",
        OrderSide::Buy,
        dec!(100),
        dec!(1),
        0,
        None,
    );
    let sell = create_test_order(
        "summary-sell",
        "SOLUSDC",
        OrderSide::Sell,
        dec!(101),
        dec!(1),
        1,
        None,
    );
    {
        let mut orders = state.active_orders.write().await;
        orders.insert(buy.client_order_id.clone(), buy);
        orders.insert(sell.client_order_id.clone(), sell);
    }
    state.add_log("INFO", "detail-only log").await;
    assert!(
        state
            .record_trade(
                create_test_trade(
                    "summary-fill",
                    "summary-order",
                    "SOLUSDC",
                    TradingMode::Paper,
                    OrderSide::Buy,
                    dec!(100),
                    dec!(1)
                ),
                None,
                None,
                None
            )
            .await
    );
    let router = create_router(state);
    for path in ["/api/overview", "/api/status", "/api/accounts"] {
        let response = router
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = http::body_json(response).await;
        let data = &body["data"];
        match path {
            "/api/overview" => {
                let summary = &data["pairs"][0]["snapshot"];
                assert_eq!(summary["stats"]["active_buy_orders"], 1);
                assert_eq!(summary["stats"]["active_sell_orders"], 1);
                assert_eq!(summary["today_trade_counts"]["buy"], 1);
                for field in [
                    "active_orders",
                    "recent_trades",
                    "recent_logs",
                    "hourly_trade_stats",
                ] {
                    assert!(summary.get(field).is_none());
                }
                assert_eq!(data["recent_trades"][0]["trade_id"], "summary-fill");
            }
            "/api/status" => {
                assert_eq!(data["active_orders"].as_array().unwrap().len(), 2);
                assert_eq!(data["recent_trades"][0]["trade_id"], "summary-fill");
                assert_eq!(data["recent_logs"][0]["message"], "detail-only log");
                assert!(data["hourly_trade_stats"]["buckets"].is_array());
            }
            _ => {
                assert!(data["generated_at"].is_string());
                assert!(data["pairs"][0]["snapshot"]["status"].is_string());
                assert!(data["pairs"][0]["snapshot"].get("grid_config").is_none());
            }
        }
    }
}

#[tokio::test]
async fn test_four_argument_record_trade_persists_pair_intent_atomically() {
    let mut config = AppConfig::default();
    config.exchange.symbol = "SOLUSDC".to_string();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, _action_rx) = mpsc::channel(16);
    let state = AppState::new(config, db.clone(), action_tx);

    let trade = create_test_trade(
        "trade_buy_001",
        "gb_b_parent_001",
        "SOLUSDC",
        TradingMode::Paper,
        OrderSide::Buy,
        dec!(100),
        dec!(1),
    );
    let intent = create_test_order(
        "gb_s_intent_001",
        "SOLUSDC",
        OrderSide::Sell,
        dec!(101),
        dec!(1),
        1,
        Some("gb_b_parent_001".to_string()),
    );

    // Four-argument record_trade with Some(pair_intent)
    let ok = state
        .record_trade(trade.clone(), Some(dec!(2.5)), Some(intent.clone()), None)
        .await;
    assert!(ok, "record_trade should succeed with valid pair intent");

    // Verify pair intent was atomically persisted to database
    let persisted_intents = db.load_pair_intents("SOLUSDC", TradingMode::Paper).unwrap();
    assert_eq!(persisted_intents.len(), 1);
    assert_eq!(persisted_intents[0].client_order_id, "gb_s_intent_001");
    assert_eq!(
        persisted_intents[0].paired_client_order_id.as_deref(),
        Some("gb_b_parent_001")
    );
    assert_eq!(persisted_intents[0].price, dec!(101));
    assert_eq!(persisted_intents[0].quantity, dec!(1));

    // Verify trade history and state stats
    let stats = state.stats.read().await;
    assert_eq!(stats.total_trades, 1);
    assert_eq!(stats.completed_cycles, 1);
    assert_eq!(stats.total_realized_profit, dec!(2.5));
    assert_eq!(stats.total_volume_usdc, dec!(100));

    let recent = db.get_recent_trades(10).unwrap();
    assert_eq!(recent.len(), 1);
    assert_eq!(recent[0].trade_id, "trade_buy_001");
    assert_eq!(state.recent_trades.read().await.len(), 1);
}

#[tokio::test]
async fn test_four_argument_record_trade_duplicate_fill_idempotence() {
    let mut config = AppConfig::default();
    config.exchange.symbol = "SOLUSDC".to_string();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, _action_rx) = mpsc::channel(16);
    let state = AppState::new(config, db.clone(), action_tx);

    let trade = create_test_trade(
        "trade_dup_001",
        "gb_b_dup_001",
        "SOLUSDC",
        TradingMode::Paper,
        OrderSide::Buy,
        dec!(150),
        dec!(2),
    );
    let intent = create_test_order(
        "gb_s_dup_intent_001",
        "SOLUSDC",
        OrderSide::Sell,
        dec!(151),
        dec!(2),
        1,
        Some("gb_b_dup_001".to_string()),
    );

    // Initial record
    assert!(
        state
            .record_trade(trade.clone(), Some(dec!(5.0)), Some(intent.clone()), None,)
            .await
    );

    // Duplicate record invocation with identical trade
    assert!(
        state
            .record_trade(trade.clone(), Some(dec!(5.0)), Some(intent.clone()), None,)
            .await,
        "Duplicate fill should return true (idempotent success)"
    );

    // Verify stats are not double counted
    let stats = state.stats.read().await;
    assert_eq!(
        stats.total_trades, 1,
        "Duplicate trade must not increment total_trades"
    );
    assert_eq!(
        stats.completed_cycles, 1,
        "Duplicate trade must not increment completed_cycles"
    );
    assert_eq!(
        stats.total_realized_profit,
        dec!(5.0),
        "Duplicate trade must not double realized profit"
    );

    // Verify pair intent is not duplicated in database
    let persisted_intents = db.load_pair_intents("SOLUSDC", TradingMode::Paper).unwrap();
    assert_eq!(
        persisted_intents.len(),
        1,
        "Pair intent must not be duplicated"
    );

    // Verify recent trades cache only contains 1 entry
    assert_eq!(state.recent_trades.read().await.len(), 1);
    assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
}

#[tokio::test]
async fn test_four_argument_record_trade_consumed_pair_source_deletes_intent() {
    let mut config = AppConfig::default();
    config.exchange.symbol = "SOLUSDC".to_string();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, _action_rx) = mpsc::channel(16);
    let state = AppState::new(config, db.clone(), action_tx);

    let parent_order_id = "gb_b_origin_100";
    let buy_trade = create_test_trade(
        "trade_buy_100",
        parent_order_id,
        "SOLUSDC",
        TradingMode::Paper,
        OrderSide::Buy,
        dec!(200),
        dec!(0.5),
    );
    let sell_intent = create_test_order(
        "gb_s_counter_100",
        "SOLUSDC",
        OrderSide::Sell,
        dec!(202),
        dec!(0.5),
        1,
        Some(parent_order_id.to_string()),
    );

    // 1. Record buy trade, persisting the sell pair intent
    assert!(
        state
            .record_trade(buy_trade, None, Some(sell_intent), None)
            .await
    );
    assert_eq!(
        db.load_pair_intents("SOLUSDC", TradingMode::Paper)
            .unwrap()
            .len(),
        1
    );

    // 2. Counter-order fill arrives, consuming the parent pair source
    let sell_trade = create_test_trade(
        "trade_sell_100",
        "gb_s_counter_100",
        "SOLUSDC",
        TradingMode::Paper,
        OrderSide::Sell,
        dec!(202),
        dec!(0.5),
    );
    assert!(
        state
            .record_trade(
                sell_trade,
                Some(dec!(1.0)),
                None,
                Some(parent_order_id.to_string()),
            )
            .await
    );

    // 3. Verify pair intent was deleted atomically
    let remaining_intents = db.load_pair_intents("SOLUSDC", TradingMode::Paper).unwrap();
    assert!(
        remaining_intents.is_empty(),
        "Consumed pair intent should be deleted"
    );

    // 4. Verify overall trade count and cycle profit
    let stats = state.stats.read().await;
    assert_eq!(stats.total_trades, 2);
    assert_eq!(stats.completed_cycles, 1);
    assert_eq!(stats.total_realized_profit, dec!(1.0));
    assert_eq!(db.get_recent_trades(10).unwrap().len(), 2);
}

#[tokio::test]
async fn test_four_argument_record_trade_atomic_rollback_on_missing_parent() {
    let mut config = AppConfig::default();
    config.exchange.symbol = "SOLUSDC".to_string();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, _action_rx) = mpsc::channel(16);
    let state = AppState::new(config, db.clone(), action_tx);

    let trade = create_test_trade(
        "trade_invalid_001",
        "gb_b_invalid_001",
        "SOLUSDC",
        TradingMode::Paper,
        OrderSide::Buy,
        dec!(50),
        dec!(1),
    );
    // An intent without paired_client_order_id is invalid for pair_intents table
    let invalid_intent = create_test_order(
        "gb_s_orphaned",
        "SOLUSDC",
        OrderSide::Sell,
        dec!(51),
        dec!(1),
        1,
        None,
    );

    let result = state
        .record_trade(trade, Some(dec!(5.0)), Some(invalid_intent), None)
        .await;

    // Must fail and roll back transaction
    assert!(
        !result,
        "record_trade must return false when pair intent has no parent source"
    );

    // Verify atomicity: neither trade nor intent persisted
    assert!(db
        .load_pair_intents("SOLUSDC", TradingMode::Paper)
        .unwrap()
        .is_empty());
    assert!(db.get_recent_trades(10).unwrap().is_empty());
    assert_eq!(state.stats.read().await.total_trades, 0);
    assert_eq!(state.stats.read().await.completed_cycles, 0);
    assert!(state.recent_trades.read().await.is_empty());
}

#[tokio::test]
async fn test_four_argument_record_trade_live_mode_managed_order_and_pair_intent() {
    let mut config = AppConfig::default();
    config.exchange.symbol = "SOLUSDC".to_string();
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, _action_rx) = mpsc::channel(16);
    let state = AppState::new(config, db.clone(), action_tx);

    let live_managed_order = create_test_order(
        "gb_b_live_managed_01",
        "SOLUSDC",
        OrderSide::Buy,
        dec!(120),
        dec!(1),
        -1,
        None,
    );
    db.save_managed_order(&live_managed_order).unwrap();
    assert_eq!(db.load_managed_orders("SOLUSDC").unwrap().len(), 1);

    let fill_trade = create_test_trade(
        "live_fill_01",
        "gb_b_live_managed_01",
        "SOLUSDC",
        TradingMode::Live,
        OrderSide::Buy,
        dec!(120),
        dec!(1),
    );
    let pair_intent = create_test_order(
        "gb_s_live_intent_01",
        "SOLUSDC",
        OrderSide::Sell,
        dec!(121),
        dec!(1),
        1,
        Some("gb_b_live_managed_01".to_string()),
    );

    // Record trade in live mode: managed order is deleted, pair intent is stored
    assert!(
        state
            .record_trade(fill_trade, None, Some(pair_intent), None)
            .await
    );

    assert!(
        db.load_managed_orders("SOLUSDC").unwrap().is_empty(),
        "Filled managed order must be removed from managed_orders"
    );
    let live_intents = db.load_pair_intents("SOLUSDC", TradingMode::Live).unwrap();
    assert_eq!(live_intents.len(), 1);
    assert_eq!(live_intents[0].client_order_id, "gb_s_live_intent_01");

    // Counter-trade fill consumes the parent intent
    let counter_trade = create_test_trade(
        "live_fill_02",
        "gb_s_live_intent_01",
        "SOLUSDC",
        TradingMode::Live,
        OrderSide::Sell,
        dec!(121),
        dec!(1),
    );
    assert!(
        state
            .record_trade(
                counter_trade,
                Some(dec!(1.0)),
                None,
                Some("gb_b_live_managed_01".to_string()),
            )
            .await
    );

    assert!(
        db.load_pair_intents("SOLUSDC", TradingMode::Live)
            .unwrap()
            .is_empty(),
        "Consumed live pair intent must be deleted"
    );
    let stats = state.stats.read().await;
    assert_eq!(stats.total_trades, 2);
    assert_eq!(stats.completed_cycles, 1);
    assert_eq!(stats.total_realized_profit, dec!(1.0));
}

#[tokio::test]
async fn test_four_argument_record_trade_scope_mismatch_rejected() {
    let mut config = AppConfig::default();
    config.exchange.symbol = "SOLUSDC".to_string();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, _action_rx) = mpsc::channel(16);
    let state = AppState::new(config, db.clone(), action_tx);

    // Mismatched symbol
    let wrong_symbol_trade = create_test_trade(
        "trade_wrong_sym",
        "gb_b_sym",
        "BTCUSDT",
        TradingMode::Paper,
        OrderSide::Buy,
        dec!(50000),
        dec!(0.01),
    );
    assert!(
        !state
            .record_trade(wrong_symbol_trade, None, None, None)
            .await
    );

    // Mismatched mode (Live trade sent to Paper strategy)
    let wrong_mode_trade = create_test_trade(
        "trade_wrong_mode",
        "gb_b_mode",
        "SOLUSDC",
        TradingMode::Live,
        OrderSide::Buy,
        dec!(100),
        dec!(1),
    );
    assert!(!state.record_trade(wrong_mode_trade, None, None, None).await);

    assert!(db.get_recent_trades(10).unwrap().is_empty());
    assert_eq!(state.stats.read().await.total_trades, 0);
}
