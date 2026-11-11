use super::{
    aggregate_execution_pnl, buy_exposure_usdc, grid_level_occupied, has_nearby_grid_order,
    is_grid_order, live_sync_freshness, new_grid_client_order_id, new_pair_intent,
    paired_grid_client_order_id, reserved_sell_quantity, sell_quantity_available,
    GridTradingEngine,
};
use crate::config::AppConfig;
use crate::db::Database;
use crate::exchange::client::BinanceFuturesClient;
use crate::exchange::{BinanceOrderResponse, BinanceUserTrade};
use crate::server::state::AppState;
use crate::types::OrderPurpose;
use crate::types::{
    AccountInfo, BotControlAction, BotStatus, GridDirection, GridOrder, GridStats, OrderSide,
    OrderStatus, PositionInfo, TickerInfo, TradeRecord, TradingMode,
};
use axum::{
    extract::{Query, State},
    routing::{delete, get},
    Json, Router,
};
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Instant;
use tokio::sync::{broadcast, mpsc};

#[derive(Clone, Default)]
struct PairExchange {
    orders: Arc<Mutex<HashMap<String, BinanceOrderResponse>>>,
    posts: Arc<Mutex<Vec<String>>>,
    fail_lookup: Arc<AtomicUsize>,
    filled_zero_lookup: Arc<AtomicUsize>,
    fail_open_orders: Arc<AtomicUsize>,
    fail_cancel: Arc<AtomicUsize>,
    block_cancel: Arc<AtomicUsize>,
    cancel_started: Arc<tokio::sync::Notify>,
    release_cancel: Arc<tokio::sync::Notify>,
    lose_response: Arc<AtomicUsize>,
    fail_position: Arc<AtomicUsize>,
    position_calls: Arc<AtomicUsize>,
}

fn consume_failure(counter: &AtomicUsize) -> bool {
    let mut count = counter.load(Ordering::SeqCst);
    while let Some(next) = count.checked_sub(1) {
        match counter.compare_exchange(count, next, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return true,
            Err(actual) => count = actual,
        }
    }
    false
}

async fn pair_exchange_server(fake: PairExchange) -> (String, tokio::task::JoinHandle<()>) {
    let app = Router::new()
            .route("/fapi/v1/time", get(|| async {
                Json(serde_json::json!({"serverTime": Utc::now().timestamp_millis()}))
            }))
            .route("/fapi/v1/exchangeInfo", get(|| async {
                Json(serde_json::json!({"symbols":[{"symbol":"SOLUSDC","status":"TRADING","pricePrecision":2,"quantityPrecision":2,"filters":[{"filterType":"PRICE_FILTER","minPrice":"0.01","maxPrice":"100000","tickSize":"0.01"},{"filterType":"LOT_SIZE","minQty":"0.01","maxQty":"100000","stepSize":"0.01"},{"filterType":"MIN_NOTIONAL","notional":"5"}]}]}))
            }))
            .route("/fapi/v1/ticker/price", get(|| async {
                Json(serde_json::json!({"symbol":"SOLUSDC","price":"100","time":0}))
            }))
            .route("/fapi/v2/positionRisk", get(|State(fake): State<PairExchange>| async move {
                fake.position_calls.fetch_add(1, Ordering::SeqCst);
                if consume_failure(&fake.fail_position) {
                    return (axum::http::StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({})));
                }
                (axum::http::StatusCode::OK, Json(serde_json::json!([{"symbol":"SOLUSDC","positionAmt":"0","entryPrice":"0","markPrice":"100","unRealizedProfit":"0","liquidationPrice":"0","leverage":"1"}])))
            }))
            .route("/fapi/v2/account", get(|| async {
                Json(serde_json::json!({"totalWalletBalance":"10000","totalMarginBalance":"10000","totalUnrealizedProfit":"0","availableBalance":"10000","assets":[{"asset":"USDC","walletBalance":"10000","marginBalance":"10000","availableBalance":"10000","unrealizedProfit":"0"}]}))
            }))
            .route(
                "/fapi/v1/openOrders",
                get(|State(fake): State<PairExchange>| async move {
                    if consume_failure(&fake.fail_open_orders) {
                        return (axum::http::StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"code": -1000})));
                    }
                    let orders: Vec<_> = fake.orders.lock().unwrap().values()
                        .filter(|order| matches!(order.status.as_str(), "NEW" | "PARTIALLY_FILLED"))
                        .cloned()
                        .collect();
                    (axum::http::StatusCode::OK, Json(serde_json::to_value(orders).unwrap()))
                }),
            )
            .route(
                "/fapi/v1/order",
                get(|State(fake): State<PairExchange>, Query(query): Query<HashMap<String, String>>| async move {
                    if consume_failure(&fake.fail_lookup) {
                        return (axum::http::StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"code": -1000})));
                    }
                    let orders = fake.orders.lock().unwrap();
                    let order = if let Some(client_id) = query.get("origClientOrderId") {
                        orders.get(client_id).cloned()
                    } else {
                        query.get("orderId").and_then(|id| id.parse::<i64>().ok()).and_then(|id| orders.values().find(|order| order.order_id == id).cloned())
                    };
                    match order {
                        Some(mut order) => {
                            if consume_failure(&fake.filled_zero_lookup) {
                                order.status = "FILLED".into();
                                order.executed_qty = Decimal::ZERO;
                            }
                            (axum::http::StatusCode::OK, Json(serde_json::to_value(order).unwrap()))
                        },
                        None => (axum::http::StatusCode::BAD_REQUEST, Json(serde_json::json!({"code": -2013, "msg": "Order does not exist"}))),
                    }
                })
                .delete(|State(fake): State<PairExchange>, Query(query): Query<HashMap<String, String>>| async move {
                    if fake.block_cancel.load(Ordering::SeqCst) != 0 {
                        fake.cancel_started.notify_one();
                        fake.release_cancel.notified().await;
                    }
                    if consume_failure(&fake.fail_cancel) {
                        return (axum::http::StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"code": -1000})));
                    }
                    let id: i64 = query["orderId"].parse().unwrap();
                    let mut orders = fake.orders.lock().unwrap();
                    let order = orders.values_mut().find(|order| order.order_id == id).unwrap();
                    order.status = "CANCELED".into();
                    (axum::http::StatusCode::OK, Json(serde_json::to_value(order.clone()).unwrap()))
                })
                .post(|State(fake): State<PairExchange>, Query(query): Query<HashMap<String, String>>| async move {
                    let id = query["newClientOrderId"].clone();
                    fake.posts.lock().unwrap().push(id.clone());
                    let mut orders = fake.orders.lock().unwrap();
                    let order_id = orders.values().map(|order| order.order_id).max().unwrap_or(499).max(499) + 1;
                    let order = BinanceOrderResponse {
                        order_id,
                        client_order_id: id.clone(),
                        symbol: query["symbol"].clone(),
                        status: "NEW".into(),
                        price: query["price"].parse().unwrap(),
                        avg_price: None,
                        orig_qty: query["quantity"].parse().unwrap(),
                        executed_qty: dec!(0),
                        side: query["side"].clone(),
                        order_type: "LIMIT".into(),
                        time_in_force: "GTX".into(),
                        update_time: None,
                    };
                    orders.insert(id, order.clone());
                    if consume_failure(&fake.lose_response) {
                        (axum::http::StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"code": -1000})))
                    } else {
                        (axum::http::StatusCode::OK, Json(serde_json::to_value(order).unwrap()))
                    }
                }),
            )
            .with_state(fake);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (url, server)
}

fn pending_pair_fixture() -> (AppConfig, Arc<Database>, GridOrder) {
    let mut config = AppConfig::default();
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
    config.grid.grid_interval = dec!(1);
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let parent = GridOrder {
        client_order_id: "gb_b_recovery_parent".into(),
        order_id: Some(42),
        symbol: "SOLUSDC".into(),
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(1),
        amount_usdc: dec!(100),
        status: OrderStatus::Filled,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: -1,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Legacy,
        merge_sources: Vec::new(),
    };
    let intent = new_pair_intent(&parent, OrderSide::Sell, dec!(101), dec!(1));
    let trade = TradeRecord {
        trade_id: format!("grid:{}", parent.client_order_id),
        client_order_id: parent.client_order_id,
        symbol: parent.symbol,
        mode: TradingMode::Live,
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(1),
        amount_usdc: dec!(100),
        realized_pnl: dec!(0),
        commission: dec!(0),
        pnl_verified: true,
        is_maker: true,
        timestamp: Utc::now(),
        note: "fill".into(),
    };
    db.insert_trade_with_recovery(
        &trade,
        &GridStats {
            total_trades: 1,
            ..GridStats::default()
        },
        Some(&intent),
        None,
    )
    .unwrap();
    (config, db, intent)
}
#[tokio::test]
async fn invalid_symbol_removal_requires_empty_recovery_state() {
    let app = Router::new().fallback(|| async {
        (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"code": -1121, "msg": "Invalid symbol."})),
        )
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let (config, db, order) = pending_pair_fixture();
    let mut engine = pair_recovery_engine(&config, db.clone(), url.clone()).await;
    assert!(engine.shutdown_inner(true).await.is_err());
    assert_eq!(
        db.load_pair_intents("SOLUSDC", TradingMode::Live)
            .unwrap()
            .len(),
        1
    );
    db.clear_pair_intents("SOLUSDC", TradingMode::Live).unwrap();
    db.save_managed_order(&order).unwrap();
    assert!(engine.shutdown_inner(true).await.is_err());
    assert_eq!(db.load_managed_orders("SOLUSDC").unwrap().len(), 1);
    db.delete_managed_order(&order.client_order_id).unwrap();
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(order.client_order_id.clone(), order.clone());
    assert!(engine.shutdown_inner(true).await.is_err());
    engine.state.active_orders.write().await.clear();
    db.save_remainder_plan(&crate::types::RemainderPlan {
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Live,
        sources: vec![order.clone()],
        target: order,
        phase: crate::types::RemainderPhase::Canceling,
    })
    .unwrap();
    assert!(engine.shutdown_inner(true).await.is_err());
    let empty_db = Arc::new(Database::open(":memory:").unwrap());
    let mut empty = pair_recovery_engine(&config, empty_db, url).await;
    assert!(empty.shutdown_inner(true).await.is_ok());
    server.abort();
}

#[tokio::test]
async fn invalid_symbol_removal_blocks_unreadable_recovery_state() {
    let app = Router::new().fallback(|| async {
        (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"code": -1121, "msg": "Invalid symbol."})),
        )
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let path = std::env::temp_dir().join(format!("invalid-symbol-db-{}.db", uuid::Uuid::new_v4()));
    let db = Arc::new(Database::open(&path).unwrap());
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("DROP TABLE pair_intents").unwrap();
    drop(connection);
    let mut config = AppConfig::default();
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    assert!(engine.shutdown_inner(true).await.is_err());
    drop(engine);
    drop(db);
    std::fs::remove_file(path).unwrap();
    server.abort();
}

#[tokio::test]
async fn removal_does_not_accept_other_exchange_errors() {
    let app = Router::new().fallback(|| async {
        (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"code": -1000, "msg": "Unknown error"})),
        )
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut config = AppConfig::default();
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let mut engine = pair_recovery_engine(&config, db, url).await;
    assert!(engine.shutdown_inner(true).await.is_err());
    server.abort();
}

async fn pair_recovery_engine(
    config: &AppConfig,
    db: Arc<Database>,
    url: String,
) -> GridTradingEngine {
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_ticker_tx, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    {
        let mut ticker = state.ticker.write().await;
        ticker.last_price = dec!(100);
        ticker.update_time = Utc::now();
        ticker.mark_update_time = Utc::now();
    }
    state.position.write().await.size = dec!(1);
    let client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
    let mut engine = GridTradingEngine::new(state, client, action_rx, ticker_rx);
    engine.last_account_sync = Some(Instant::now());
    engine.last_orders_sync = Some(Instant::now());
    engine
}

fn seed_paper_position(db: &Database) {
    let trade = TradeRecord {
        trade_id: "paper-switch-fill".into(),
        client_order_id: "paper-switch-order".into(),
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Paper,
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(1),
        amount_usdc: dec!(100),
        realized_pnl: dec!(0),
        commission: dec!(0),
        pnl_verified: true,
        is_maker: true,
        timestamp: Utc::now(),
        note: String::new(),
    };
    let position = PositionInfo {
        symbol: "SOLUSDC".into(),
        size: dec!(1),
        entry_price: dec!(100),
        ..Default::default()
    };
    let account = AccountInfo {
        asset: "USDC".into(),
        total_wallet_balance: dec!(900),
        available_balance: dec!(800),
        ..Default::default()
    };
    assert!(db
        .insert_trade_with_paper_recovery(
            &trade,
            &GridStats::default(),
            None,
            None,
            Some(&(position, account)),
        )
        .unwrap());
}

#[tokio::test]
async fn switching_away_and_back_restores_paper_position_and_balance() {
    let mut config = AppConfig::default();
    config.grid.grid_interval = dec!(1);
    let db = Arc::new(Database::open(":memory:").unwrap());
    seed_paper_position(&db);
    let fake = PairExchange::default();
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db, url).await;
    let mut live = config.clone();
    live.exchange.api_key = "live-key".into();
    live.exchange.api_secret = "live-secret".into();
    engine.apply_config(live).await.unwrap();
    assert_eq!(engine.state.position.read().await.size, dec!(0));
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert!(fake.orders.lock().unwrap().is_empty());
    {
        let mut ticker = engine.state.ticker.write().await;
        ticker.mark_price = dec!(100);
        ticker.mark_update_time = Utc::now();
    }
    let (reply, response) = tokio::sync::oneshot::channel();
    engine
        .handle_control_action(BotControlAction::Resume {
            expected_mode: TradingMode::Live,
            expected_epoch: engine.state.pause_epoch(),
            reply,
        })
        .await;
    response.await.unwrap().unwrap();
    let live_order_ids: Vec<_> = fake
        .orders
        .lock()
        .unwrap()
        .values()
        .filter(|order| order.status == "NEW")
        .map(|order| order.client_order_id.clone())
        .collect();
    assert!(live_order_ids.len() > 1);
    engine.apply_config(config).await.unwrap();
    {
        let orders = fake.orders.lock().unwrap();
        assert!(live_order_ids.iter().all(|id| orders
            .get(id)
            .is_some_and(|order| order.status == "CANCELED")));
        assert!(orders.values().all(|order| order.status != "NEW"));
    }
    assert_eq!(engine.state.position.read().await.size, dec!(1));
    let account = engine.state.account.read().await;
    assert_eq!(account.total_wallet_balance, dec!(900));
    assert_eq!(account.available_balance, dec!(800));
    server.abort();
}

#[tokio::test]
async fn switching_direction_rejects_nonempty_target_paper_position() {
    let mut config = AppConfig::default();
    config.exchange.api_key = "test-key".into();
    config.exchange.api_secret = "test-secret".into();
    let db = Arc::new(Database::open(":memory:").unwrap());
    seed_paper_position(&db);
    let (url, server) = pair_exchange_server(PairExchange::default()).await;
    let mut engine = pair_recovery_engine(&config, db, url).await;
    engine.state.position.write().await.size = dec!(0);
    let mut paper = config.clone();
    paper.exchange.api_key.clear();
    paper.exchange.api_secret.clear();
    paper.grid.direction = match config.grid.direction {
        GridDirection::Long => GridDirection::Short,
        GridDirection::Short => GridDirection::Long,
    };
    let error = engine.apply_config(paper).await.unwrap_err();
    assert!(error.to_string().contains("请先平掉目标账户的持仓"));
    let current = engine.state.config.read().await;
    assert!(!current.exchange.is_paper());
    assert_eq!(current.grid.direction, config.grid.direction);
    server.abort();
}

#[tokio::test]
async fn rebalance_rejects_mode_changed_after_enqueue_before_exchange_work() {
    let (mut engine, _db, fake, server) = dirty_live_switch_fixture().await;
    let (reply, result) = tokio::sync::oneshot::channel();
    engine
        .state
        .action_tx
        .send(BotControlAction::Rebalance {
            expected_mode: TradingMode::Paper,
            reply,
        })
        .await
        .unwrap();
    engine.state.config.write().await.exchange.api_key = "live-key".into();
    engine.state.config.write().await.exchange.api_secret = "live-secret".into();
    let action = engine.action_rx.recv().await.unwrap();
    engine.handle_control_action(action).await;
    assert!(result.await.unwrap().unwrap_err().contains("模式已改变"));
    assert_eq!(
        fake.orders
            .lock()
            .unwrap()
            .get("gb_b_dirty_target")
            .unwrap()
            .status,
        "NEW"
    );
    assert!(fake.posts.lock().unwrap().is_empty());
    assert_eq!(*engine.state.status.read().await, BotStatus::Running);
    server.abort();
}

#[tokio::test]
async fn rebalance_rejects_pause_after_enqueue_without_placing_orders() {
    let config = AppConfig::default();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let fake = PairExchange::default();
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db, url).await;
    engine.state.resume_trading().await.unwrap();
    let (reply, result) = tokio::sync::oneshot::channel();
    engine
        .state
        .action_tx
        .send(BotControlAction::Rebalance {
            expected_mode: config.exchange.trading_mode(),
            reply,
        })
        .await
        .unwrap();
    engine.state.pause_trading().await.unwrap();
    let action = engine.action_rx.recv().await.unwrap();
    engine.handle_control_action(action).await;
    assert!(result.await.unwrap().is_err());
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert!(engine.state.active_orders.read().await.is_empty());
    assert!(fake.posts.lock().unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn rebalance_reports_cancellation_failure() {
    let mut config = AppConfig::default();
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let fake = PairExchange::default();
    fake.fail_open_orders.store(100, Ordering::SeqCst);
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db, url).await;
    engine.state.resume_trading().await.unwrap();
    let (reply, result) = tokio::sync::oneshot::channel();
    engine
        .handle_control_action(BotControlAction::Rebalance {
            expected_mode: config.exchange.trading_mode(),
            reply,
        })
        .await;
    assert!(result.await.unwrap().unwrap_err().contains("cancel"));
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert!(fake.posts.lock().unwrap().is_empty());
    server.abort();
}

async fn dirty_live_switch_fixture() -> (
    GridTradingEngine,
    Arc<Database>,
    PairExchange,
    tokio::task::JoinHandle<()>,
) {
    let mut paper = AppConfig::default();
    paper.grid.grid_interval = dec!(1);
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_config(&paper).unwrap();
    db.arm_execution_guard("SOLUSDC", TradingMode::Paper)
        .unwrap();
    db.arm_execution_guard("SOLUSDC", TradingMode::Live)
        .unwrap();
    let fake = PairExchange::default();
    let order = BinanceOrderResponse {
        order_id: 777,
        client_order_id: "gb_b_dirty_target".into(),
        symbol: "SOLUSDC".into(),
        status: "NEW".into(),
        price: dec!(99),
        avg_price: None,
        orig_qty: dec!(1),
        executed_qty: dec!(0),
        side: "BUY".into(),
        order_type: "LIMIT".into(),
        time_in_force: "GTX".into(),
        update_time: None,
    };
    fake.orders
        .lock()
        .unwrap()
        .insert(order.client_order_id.clone(), order);
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let engine = pair_recovery_engine(&paper, db.clone(), url).await;
    engine.state.resume_trading().await.unwrap();
    (engine, db, fake, server)
}

#[tokio::test]
async fn rebalance_reports_pause_during_cancellation() {
    let (mut engine, _db, fake, server) = dirty_live_switch_fixture().await;
    engine.state.config.write().await.exchange.api_key = "live-key".into();
    engine.state.config.write().await.exchange.api_secret = "live-secret".into();
    fake.block_cancel.store(1, Ordering::SeqCst);
    let state = engine.state.clone();
    let expected_mode = {
        let config = state.config.read().await;
        config.exchange.trading_mode()
    };
    let (reply, result) = tokio::sync::oneshot::channel();
    let rebuilding = tokio::spawn(async move {
        engine
            .handle_control_action(BotControlAction::Rebalance {
                expected_mode,
                reply,
            })
            .await;
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        fake.cancel_started.notified(),
    )
    .await
    .unwrap();
    state.pause_trading().await.unwrap();
    fake.release_cancel.notify_one();
    rebuilding.await.unwrap();
    assert!(result.await.unwrap().is_err());
    assert_eq!(*state.status.read().await, BotStatus::Paused);
    assert!(fake.posts.lock().unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn switching_to_dirty_live_cancels_recovered_orders_and_stays_paused() {
    let (mut engine, db, fake, server) = dirty_live_switch_fixture().await;
    let mut live = engine.state.config.read().await.clone();
    live.exchange.api_key = "live-key".into();
    live.exchange.api_secret = "live-secret".into();
    engine.apply_config(live).await.unwrap();

    assert!(!engine.state.config.read().await.exchange.is_paper());
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert_eq!(
        db.load_pair_status("SOLUSDC", TradingMode::Live).unwrap(),
        Some(BotStatus::Paused)
    );
    assert_eq!(
        fake.orders.lock().unwrap()["gb_b_dirty_target"].status,
        "CANCELED"
    );
    assert!(engine.state.active_orders.read().await.is_empty());
    assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn live_credential_switch_invalidates_old_balance_until_reconciled() {
    let mut config = AppConfig::default();
    config.exchange.api_key = "old-key".into();
    config.exchange.api_secret = "old-secret".into();
    config.grid.grid_interval = dec!(1);
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_config(&config).unwrap();
    let fake = PairExchange::default();
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db, url).await;
    let state = engine.state.clone();
    let mut old_account = state.account.write().await;
    old_account.asset = "USDC".into();
    old_account.margin_balance = dec!(321);
    old_account.update_time = Utc::now();

    config.exchange.api_key = "new-key".into();
    config.exchange.api_secret = "new-secret".into();
    let switching = tokio::spawn(async move {
        let result = engine.apply_config(config).await;
        (engine, result)
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if state.config.read().await.exchange.api_key == "new-key" {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    fake.fail_open_orders.store(100, Ordering::SeqCst);
    drop(old_account);

    let (mut engine, result) = tokio::time::timeout(std::time::Duration::from_secs(10), switching)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_err());
    let snapshot = state.snapshot().await;
    assert_eq!(snapshot.account.margin_balance, Decimal::ZERO);
    assert_eq!(
        snapshot.account.update_time,
        chrono::DateTime::<Utc>::default()
    );

    fake.fail_open_orders.store(0, Ordering::SeqCst);
    assert!(engine.sync_account_and_position().await);
    let account = state.account.read().await;
    assert_eq!(account.margin_balance, dec!(10000));
    assert!(account.update_time > chrono::DateTime::<Utc>::default());
    server.abort();
}

#[tokio::test]
async fn dirty_live_cancellation_failure_keeps_pause_and_recovery_record() {
    let (mut engine, db, fake, server) = dirty_live_switch_fixture().await;
    fake.fail_cancel.store(100, Ordering::SeqCst);
    let mut live = engine.state.config.read().await.clone();
    live.exchange.api_key = "live-key".into();
    live.exchange.api_secret = "live-secret".into();
    assert!(engine.apply_config(live).await.is_err());

    assert!(!engine.state.config.read().await.exchange.is_paper());
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert_eq!(
        db.load_pair_status("SOLUSDC", TradingMode::Live).unwrap(),
        Some(BotStatus::Paused)
    );
    assert_eq!(
        fake.orders.lock().unwrap()["gb_b_dirty_target"].status,
        "NEW"
    );
    assert!(engine
        .state
        .active_orders
        .read()
        .await
        .contains_key("gb_b_dirty_target"));
    assert!(db
        .load_managed_orders("SOLUSDC")
        .unwrap()
        .iter()
        .any(|order| order.client_order_id == "gb_b_dirty_target"));
    server.abort();
}

#[tokio::test]
async fn concurrent_pause_during_dirty_live_cancellation_stays_paused() {
    let (mut engine, db, fake, server) = dirty_live_switch_fixture().await;
    fake.block_cancel.store(1, Ordering::SeqCst);
    let state = engine.state.clone();
    let mut live = state.config.read().await.clone();
    live.exchange.api_key = "live-key".into();
    live.exchange.api_secret = "live-secret".into();
    let switching = tokio::spawn(async move { engine.apply_config(live).await });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        fake.cancel_started.notified(),
    )
    .await
    .unwrap();
    state.pause_trading().await.unwrap();
    fake.release_cancel.notify_one();
    switching.await.unwrap().unwrap();

    assert_eq!(*state.status.read().await, BotStatus::Paused);
    assert_eq!(
        db.load_pair_status("SOLUSDC", TradingMode::Live).unwrap(),
        Some(BotStatus::Paused)
    );
    assert_eq!(
        fake.orders.lock().unwrap()["gb_b_dirty_target"].status,
        "CANCELED"
    );
    server.abort();
}

#[tokio::test]
async fn cancellation_continues_when_managed_metadata_cannot_be_read() {
    let (config, _, mut order) = pending_pair_fixture();
    let path = std::env::temp_dir().join(format!(
        "grid-cancel-fault-{}-{}.db",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap()
    ));
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    order.client_order_id = "gb_b_partial_fault".into();
    order.order_id = Some(777);
    db.save_managed_order(&order).unwrap();
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute(
        "UPDATE managed_orders SET order_json = 'invalid' WHERE client_order_id = ?1",
        [&order.client_order_id],
    )
    .unwrap();
    let fake = PairExchange::default();
    let exchange_order = BinanceOrderResponse {
        order_id: 777,
        client_order_id: order.client_order_id.clone(),
        symbol: order.symbol.clone(),
        status: "PARTIALLY_FILLED".into(),
        price: order.price,
        avg_price: Some(order.price),
        orig_qty: order.quantity,
        executed_qty: dec!(0.5),
        side: order.side.as_str().into(),
        order_type: "LIMIT".into(),
        time_in_force: "GTX".into(),
        update_time: None,
    };
    fake.orders
        .lock()
        .unwrap()
        .insert(order.client_order_id.clone(), exchange_order.clone());
    let mut manual = exchange_order;
    manual.order_id = 778;
    manual.client_order_id = "manual_order".into();
    fake.orders
        .lock()
        .unwrap()
        .insert(manual.client_order_id.clone(), manual);
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(order.client_order_id.clone(), order.clone());

    assert!(!engine.cancel_all_orders().await);
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert!(engine
        .state
        .active_orders
        .read()
        .await
        .contains_key(&order.client_order_id));
    assert_eq!(
        fake.orders.lock().unwrap()[&order.client_order_id].status,
        "CANCELED"
    );
    assert_eq!(
        fake.orders.lock().unwrap()["manual_order"].status,
        "PARTIALLY_FILLED"
    );
    let json: String = conn
        .query_row(
            "SELECT order_json FROM managed_orders WHERE client_order_id = ?1",
            [&order.client_order_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(json, "invalid");
    assert!(db
        .get_trade_by_client_id(&order.client_order_id, TradingMode::Live)
        .unwrap()
        .is_none());
    server.abort();
    drop(engine);
    drop(db);
    drop(conn);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn cancellation_continues_when_exchange_listing_fails() {
    let (config, _, mut order) = pending_pair_fixture();
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    order.client_order_id = "gb_b_listing_fault".into();
    order.order_id = Some(779);
    db.save_managed_order(&order).unwrap();
    let fake = PairExchange::default();
    fake.fail_open_orders.store(100, Ordering::SeqCst);
    fake.orders.lock().unwrap().insert(
        order.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 779,
            client_order_id: order.client_order_id.clone(),
            symbol: order.symbol.clone(),
            status: "PARTIALLY_FILLED".into(),
            price: order.price,
            avg_price: Some(order.price),
            orig_qty: order.quantity,
            executed_qty: dec!(0.5),
            side: order.side.as_str().into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    assert!(!engine.cancel_all_orders().await);
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert_eq!(
        fake.orders.lock().unwrap()[&order.client_order_id].status,
        "CANCELED"
    );
    assert!(engine
        .state
        .active_orders
        .read()
        .await
        .contains_key(&order.client_order_id));
    assert!(db
        .load_managed_orders(&order.symbol)
        .unwrap()
        .iter()
        .any(|saved| saved.client_order_id == order.client_order_id));
    assert!(db
        .get_trade_by_client_id(&order.client_order_id, TradingMode::Live)
        .unwrap()
        .is_none());
    server.abort();
}

#[tokio::test]
async fn failed_initialization_restores_orders_before_initial_cancellation() {
    let (config, db, mut order) = pending_pair_fixture();
    order.client_order_id = "gb_b_startup_unknown".into();
    order.order_id = None;
    order.created_at = Utc::now();
    db.save_managed_order(&order).unwrap();
    let (url, server) = pair_exchange_server(PairExchange::default()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;

    assert!(engine.initialize().await.is_err());
    assert!(engine
        .state
        .active_orders
        .read()
        .await
        .contains_key(&order.client_order_id));
    assert!(!engine.cancel_all_orders().await);
    assert!(db
        .load_managed_orders("SOLUSDC")
        .unwrap()
        .iter()
        .any(|saved| saved.client_order_id == order.client_order_id));
    server.abort();
}

#[tokio::test]
async fn cancellation_restores_orders_even_if_initial_recovery_was_skipped() {
    let (config, db, mut order) = pending_pair_fixture();
    order.client_order_id = "gb_b_cancel_unknown".into();
    order.order_id = None;
    order.created_at = Utc::now();
    db.save_managed_order(&order).unwrap();
    let (url, server) = pair_exchange_server(PairExchange::default()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;

    assert!(!engine.cancel_all_orders().await);
    assert!(engine
        .state
        .active_orders
        .read()
        .await
        .contains_key(&order.client_order_id));
    assert!(db
        .load_managed_orders("SOLUSDC")
        .unwrap()
        .iter()
        .any(|saved| saved.client_order_id == order.client_order_id));
    server.abort();
}

#[tokio::test]
async fn unresolved_order_keeps_confirmed_fill_behind_reconciliation_barrier() {
    let (config, db, mut unresolved) = pending_pair_fixture();
    unresolved.client_order_id = "gb_b_unknown_frontier".into();
    unresolved.order_id = None;
    unresolved.created_at = Utc::now();
    let mut filled = unresolved.clone();
    filled.client_order_id = "gb_b_confirmed_frontier".into();
    db.save_managed_order(&unresolved).unwrap();
    db.save_managed_order(&filled).unwrap();
    let fake = PairExchange::default();
    fake.orders.lock().unwrap().insert(
        filled.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 777,
            client_order_id: filled.client_order_id.clone(),
            symbol: filled.symbol.clone(),
            status: "FILLED".into(),
            price: filled.price,
            avg_price: None,
            orig_qty: filled.quantity,
            executed_qty: filled.quantity,
            side: filled.side.as_str().into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: Some(Utc::now().timestamp_millis()),
        },
    );
    let (url, server) = pair_exchange_server(fake).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    {
        let mut active = engine.state.active_orders.write().await;
        active.insert(unresolved.client_order_id.clone(), unresolved);
        active.insert(filled.client_order_id.clone(), filled.clone());
    }
    assert!(!engine.sync_live_orders().await);
    assert!(engine.last_orders_sync.is_none());
    assert!(engine
        .state
        .active_orders
        .read()
        .await
        .contains_key(&filled.client_order_id));
    assert!(db
        .load_managed_orders("SOLUSDC")
        .unwrap()
        .iter()
        .any(|saved| saved.client_order_id == filled.client_order_id));
    server.abort();
}

#[test]
fn grid_level_occupied_respects_side_and_spacing() {
    let order = GridOrder {
        client_order_id: "gb_b_occupancy".into(),
        order_id: None,
        symbol: "SOLUSDC".into(),
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(1),
        amount_usdc: dec!(100),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: 0,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Legacy,
        merge_sources: Vec::new(),
    };
    let orders = [order];
    assert!(grid_level_occupied(
        &orders,
        OrderSide::Buy,
        dec!(100.99),
        dec!(1),
        dec!(0.01)
    ));
    assert!(!grid_level_occupied(
        &orders,
        OrderSide::Buy,
        dec!(101),
        dec!(1),
        dec!(0.01)
    ));
    assert!(!grid_level_occupied(
        &orders,
        OrderSide::Sell,
        dec!(100),
        dec!(1),
        dec!(0.01)
    ));
    assert!(!grid_level_occupied(
        &[],
        OrderSide::Buy,
        dec!(100),
        dec!(1),
        dec!(0.01)
    ));
    assert!(grid_level_occupied(
        &orders,
        OrderSide::Buy,
        dec!(100.99),
        dec!(0.01),
        dec!(1)
    ));
    assert!(!grid_level_occupied(
        &orders,
        OrderSide::Buy,
        dec!(101),
        dec!(0.01),
        dec!(1)
    ));
}

#[test]
fn small_price_moves_do_not_duplicate_grid_orders() {
    let existing = [dec!(113.13), dec!(112.13), dec!(111.13)];
    assert!(has_nearby_grid_order(
        &existing,
        dec!(113.15),
        dec!(1),
        dec!(0.01)
    ));
    assert!(has_nearby_grid_order(
        &existing,
        dec!(112.15),
        dec!(1),
        dec!(0.01)
    ));
    assert!(!has_nearby_grid_order(
        &existing,
        dec!(114.15),
        dec!(1),
        dec!(0.01)
    ));
}

#[test]
fn managed_order_prefix_excludes_manual_orders() {
    assert!(is_grid_order("gb_b_123"));
    assert!(!is_grid_order("manual-123"));
}

#[test]
fn paired_ids_are_stable_and_bounded() {
    let parent = "gb_b_123456789012345678901234567890";
    let sell = paired_grid_client_order_id(parent, OrderSide::Sell);
    assert_eq!(sell, paired_grid_client_order_id(parent, OrderSide::Sell));
    assert_ne!(sell, paired_grid_client_order_id(parent, OrderSide::Buy));
    assert!(sell.len() <= 36);
}

#[tokio::test]
async fn order_reconciliation_failure_revokes_readiness_until_full_recovery() {
    let (config, db, _intent) = pending_pair_fixture();
    let fake = PairExchange::default();
    let (url, server) = pair_exchange_server(fake).await;
    let mut engine = pair_recovery_engine(&config, db, url).await;
    let state = engine.state.clone();
    state
        .global_position_initialized
        .store(true, std::sync::atomic::Ordering::SeqCst);
    state
        .global_position_ready
        .store(true, std::sync::atomic::Ordering::SeqCst);
    engine.report_reconciliation_state(true).await;
    assert!(!state
        .global_position_ready
        .load(std::sync::atomic::Ordering::SeqCst));
    engine.report_reconciliation_state(false).await;
    assert!(!state
        .global_position_ready
        .load(std::sync::atomic::Ordering::SeqCst));
    engine.set_global_reconciliation_ready(true).await;
    assert!(state
        .global_position_ready
        .load(std::sync::atomic::Ordering::SeqCst));
    server.abort();
}

#[tokio::test]
async fn accepted_legacy_pair_is_adopted_after_restart_without_resubmission() {
    let (config, db, intent) = pending_pair_fixture();
    let fake = PairExchange::default();
    fake.orders.lock().unwrap().insert(
        intent.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 501,
            client_order_id: intent.client_order_id.clone(),
            symbol: intent.symbol.clone(),
            status: "NEW".into(),
            price: intent.price,
            avg_price: None,
            orig_qty: intent.quantity,
            executed_qty: Decimal::ZERO,
            side: "SELL".into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut restarted = pair_recovery_engine(&config, db.clone(), url).await;
    assert!(restarted.sync_live_orders().await);
    assert!(fake.posts.lock().unwrap().is_empty());
    assert!(db
        .load_pair_intents("SOLUSDC", TradingMode::Live)
        .unwrap()
        .is_empty());
    let stored = db.load_managed_orders("SOLUSDC").unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(
        stored[0].paired_client_order_id,
        intent.paired_client_order_id
    );
    server.abort();
}

#[tokio::test]
async fn provisional_pair_uses_intent_metadata_for_normal_and_terminal_recovery() {
    for terminal in [false, true] {
        let (config, db, intent) = pending_pair_fixture();
        let fake = PairExchange::default();
        fake.orders.lock().unwrap().insert(
            intent.client_order_id.clone(),
            BinanceOrderResponse {
                order_id: 501,
                client_order_id: intent.client_order_id.clone(),
                symbol: intent.symbol.clone(),
                status: if terminal { "FILLED" } else { "NEW" }.into(),
                price: intent.price,
                avg_price: terminal.then_some(intent.price),
                orig_qty: intent.quantity,
                executed_qty: if terminal {
                    intent.quantity
                } else {
                    Decimal::ZERO
                },
                side: intent.side.as_str().into(),
                order_type: "LIMIT".into(),
                time_in_force: "GTX".into(),
                update_time: None,
            },
        );
        let (url, server) = pair_exchange_server(fake).await;
        let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
        let mut placeholder = intent.clone();
        placeholder.order_id = Some(501);
        placeholder.paired_client_order_id = None;
        placeholder.purpose = crate::types::OrderPurpose::Legacy;
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(placeholder.client_order_id.clone(), placeholder);
        engine.evidence.mark_provisional(
            engine.client.account_identity_key(),
            &intent.client_order_id,
        );
        assert!(engine.sync_live_orders().await);
        if terminal {
            let trades = db.get_recent_trades(10).unwrap();
            let closing = trades
                .iter()
                .find(|trade| trade.client_order_id == intent.client_order_id)
                .unwrap();
            assert!(closing.note.contains("Completed Grid Cycle"));
            assert_eq!(
                db.load_pair_intents("SOLUSDC", TradingMode::Live)
                    .unwrap()
                    .len(),
                0
            );
        } else {
            let stored = db.load_managed_orders("SOLUSDC").unwrap();
            assert_eq!(
                stored[0].paired_client_order_id,
                intent.paired_client_order_id
            );
            assert_eq!(stored[0].purpose, intent.purpose);
        }
        server.abort();
    }
}

#[tokio::test]
async fn bulk_cancel_unknown_pair_preserves_intent_metadata_before_recording_fill() {
    let (config, db, intent) = pending_pair_fixture();
    let fake = PairExchange::default();
    fake.fail_cancel.store(1, Ordering::SeqCst);
    fake.filled_zero_lookup.store(1, Ordering::SeqCst);
    fake.orders.lock().unwrap().insert(
        intent.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 501,
            client_order_id: intent.client_order_id.clone(),
            symbol: intent.symbol.clone(),
            status: "NEW".into(),
            price: intent.price,
            avg_price: None,
            orig_qty: intent.quantity,
            executed_qty: Decimal::ZERO,
            side: intent.side.as_str().into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    assert!(!engine.cancel_all_orders().await);
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    assert_eq!(saved.len(), 1);
    assert_eq!(
        saved[0].paired_client_order_id,
        intent.paired_client_order_id
    );
    assert_eq!(saved[0].purpose, intent.purpose);
    {
        let mut orders = fake.orders.lock().unwrap();
        let order = orders.get_mut(&intent.client_order_id).unwrap();
        order.status = "FILLED".into();
        order.executed_qty = order.orig_qty;
        order.avg_price = Some(order.price);
    }
    assert!(engine.sync_live_orders().await);
    let trades = db.get_recent_trades(10).unwrap();
    let closing = trades
        .iter()
        .find(|trade| trade.client_order_id == intent.client_order_id)
        .unwrap();
    assert!(closing.note.contains("Completed Grid Cycle"));
    server.abort();
}

#[tokio::test]
async fn provisional_pair_uses_intent_metadata_before_cancel_retry() {
    let (config, db, intent) = pending_pair_fixture();
    let fake = PairExchange::default();
    fake.fail_cancel.store(1, Ordering::SeqCst);
    fake.orders.lock().unwrap().insert(
        intent.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 501,
            client_order_id: intent.client_order_id.clone(),
            symbol: intent.symbol.clone(),
            status: "NEW".into(),
            price: intent.price,
            avg_price: None,
            orig_qty: intent.quantity,
            executed_qty: Decimal::ZERO,
            side: intent.side.as_str().into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    let (url, server) = pair_exchange_server(fake).await;
    let mut engine = pair_recovery_engine(&config, db, url).await;
    let mut placeholder = intent.clone();
    placeholder.order_id = Some(501);
    placeholder.paired_client_order_id = None;
    placeholder.purpose = OrderPurpose::Legacy;
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(placeholder.client_order_id.clone(), placeholder);
    engine.evidence.mark_provisional(
        engine.client.account_identity_key(),
        &intent.client_order_id,
    );
    assert!(!engine.cancel_all_orders().await);
    let active = engine.state.active_orders.read().await;
    assert_eq!(
        active[&intent.client_order_id].paired_client_order_id,
        intent.paired_client_order_id
    );
    assert_eq!(active[&intent.client_order_id].purpose, intent.purpose);
    server.abort();
}

#[tokio::test]
async fn partially_filled_pair_lookup_refreshes_risk_and_preserves_intent_on_failure() {
    let (config, db, intent) = pending_pair_fixture();
    let fake = PairExchange::default();
    fake.orders.lock().unwrap().insert(
        intent.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 501,
            client_order_id: intent.client_order_id.clone(),
            symbol: intent.symbol.clone(),
            status: "PARTIALLY_FILLED".into(),
            price: intent.price,
            avg_price: None,
            orig_qty: intent.quantity,
            executed_qty: dec!(0.4),
            side: intent.side.as_str().into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    engine
        .position_cache
        .get(&engine.client, "SOLUSDC")
        .await
        .unwrap();
    engine.account_cache.get(&engine.client).await.unwrap();
    fake.fail_position.store(1, Ordering::SeqCst);
    assert!(
        !engine
            .drain_pair_intents("SOLUSDC", TradingMode::Live)
            .await
    );
    assert_eq!(
        engine.state.active_orders.read().await[&intent.client_order_id].quantity,
        dec!(1)
    );
    assert_eq!(
        db.load_managed_orders("SOLUSDC").unwrap()[0].quantity,
        dec!(1)
    );
    assert_eq!(
        db.load_pair_intents("SOLUSDC", TradingMode::Live)
            .unwrap()
            .len(),
        1
    );
    assert!(engine.last_account_sync.is_none());

    assert!(
        engine
            .drain_pair_intents("SOLUSDC", TradingMode::Live)
            .await
    );
    assert_eq!(
        engine.state.active_orders.read().await[&intent.client_order_id].quantity,
        dec!(0.6)
    );
    assert_eq!(engine.state.position.read().await.size, dec!(0));
    assert_eq!(fake.position_calls.load(Ordering::SeqCst), 3);
    assert!(db
        .load_pair_intents("SOLUSDC", TradingMode::Live)
        .unwrap()
        .is_empty());
    server.abort();
}

#[tokio::test]
async fn conflicting_intent_and_remainder_identities_survive_failed_managed_read_and_restart() {
    for remainder in [false, true] {
        let (config, seed_db, mut intent) = pending_pair_fixture();
        intent.order_id = Some(123);
        let path =
            std::env::temp_dir().join(format!("identity-observation-{}.db", uuid::Uuid::new_v4()));
        let db = Arc::new(Database::open(&path).unwrap());
        db.save_bot_status(BotStatus::Running).unwrap();
        let trade = seed_db.get_recent_trades(10).unwrap().remove(0);
        if remainder {
            db.insert_trade(&trade).unwrap();
            intent.purpose = OrderPurpose::Remainder;
            db.save_remainder_plan(&crate::types::RemainderPlan {
                symbol: "SOLUSDC".into(),
                mode: TradingMode::Live,
                sources: Vec::new(),
                target: intent.clone(),
                phase: crate::types::RemainderPhase::Submitting,
            })
            .unwrap();
        } else {
            db.insert_trade_with_recovery(&trade, &GridStats::default(), Some(&intent), None)
                .unwrap();
        }
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection.execute("INSERT INTO managed_orders(client_order_id,symbol,order_json,updated_at) VALUES ('gb_corrupt','SOLUSDC','invalid','0')", []).unwrap();
        let stage = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(Vec::<i64>::new()));
        let client_id = intent.client_order_id.clone();
        let app = Router::new()
                .route("/fapi/v1/openOrders", get({
                    let stage = stage.clone(); let client_id = client_id.clone();
                    move || { let stage = stage.clone(); let client_id = client_id.clone(); async move {
                        if stage.load(Ordering::SeqCst) > 0 { return Json(serde_json::json!([])); }
                        Json(serde_json::json!([{"orderId":456,"clientOrderId":client_id,"symbol":"SOLUSDC",
                            "status":"FILLED","price":"101","origQty":"1","executedQty":"0",
                            "side":"SELL","type":"LIMIT","timeInForce":"GTX"}]))
                    }}
                }))
                .route("/fapi/v1/order", get({
                    let stage = stage.clone(); let seen = seen.clone(); let client_id = client_id.clone();
                    move |Query(query): Query<HashMap<String, String>>| {
                        let stage = stage.clone(); let seen = seen.clone(); let client_id = client_id.clone();
                        async move {
                            let id = query.get("orderId").and_then(|value| value.parse::<i64>().ok()).unwrap_or(123);
                            seen.lock().unwrap().push(id);
                            if id == 123 && stage.load(Ordering::SeqCst) == 2 {
                                return (axum::http::StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({})));
                            }
                            let status = if id == 456 && stage.load(Ordering::SeqCst) == 1 { "FILLED" } else { "CANCELED" };
                            (axum::http::StatusCode::OK, Json(serde_json::json!({"orderId":id,"clientOrderId":client_id,"symbol":"SOLUSDC",
                                "status":status,"price":"101","origQty":"1","executedQty":"0",
                                "side":"SELL","type":"LIMIT","timeInForce":"GTX"})))
                        }
                    }
                }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut first = pair_recovery_engine(&config, db.clone(), url.clone()).await;
        assert!(!first.sync_live_orders().await);
        assert_eq!(
            first.state.active_orders.read().await[&intent.client_order_id].order_id,
            Some(123)
        );
        let account = first.client.account_identity_key();
        assert_eq!(
            first
                .evidence
                .get_observations(account, &intent.client_order_id)
                .unwrap()[0]
                .order_id,
            456
        );
        assert_eq!(
            db.load_identity_observations(account, "SOLUSDC")
                .unwrap()
                .len(),
            1
        );
        connection
            .execute(
                "DELETE FROM managed_orders WHERE client_order_id='gb_corrupt'",
                [],
            )
            .unwrap();
        drop(first);
        let mut changed_config = config.clone();
        changed_config.exchange.recv_window += 1000;
        stage.store(1, Ordering::SeqCst);
        let mut restarted = pair_recovery_engine(&changed_config, db.clone(), url.clone()).await;
        assert_eq!(restarted.client.account_identity_key(), account);
        assert!(!restarted.sync_live_orders().await);
        assert_eq!(
            restarted.state.active_orders.read().await[&intent.client_order_id].order_id,
            Some(123)
        );
        assert!(seen.lock().unwrap().contains(&456));
        stage.store(2, Ordering::SeqCst);
        assert!(!restarted.sync_live_orders().await);
        assert_eq!(
            db.load_identity_observations(account, "SOLUSDC")
                .unwrap()
                .len(),
            1
        );
        drop(restarted);
        stage.store(3, Ordering::SeqCst);
        let mut restarted = pair_recovery_engine(&changed_config, db.clone(), url).await;
        assert!(restarted.sync_live_orders().await);
        assert!(seen.lock().unwrap().contains(&123));
        assert!(db
            .load_identity_observations(account, "SOLUSDC")
            .unwrap()
            .is_empty());
        server.abort();
        drop(connection);
        drop(restarted);
        drop(db);
        let _ = std::fs::remove_file(path);
    }
}

#[tokio::test]
async fn filled_pair_with_zero_quantity_preserves_intent_until_cancel_confirmed() {
    let (config, db, intent) = pending_pair_fixture();
    let fake = PairExchange::default();
    fake.orders.lock().unwrap().insert(
        intent.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 501,
            client_order_id: intent.client_order_id.clone(),
            symbol: intent.symbol.clone(),
            status: "FILLED".into(),
            price: intent.price,
            avg_price: None,
            orig_qty: intent.quantity,
            executed_qty: Decimal::ZERO,
            side: intent.side.as_str().into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    let state = engine.state.clone();
    state.global_position_ready.store(true, Ordering::SeqCst);
    let guard = state.active_orders.write().await;
    let task = tokio::spawn(async move {
        let ok = engine
            .drain_pair_intents("SOLUSDC", TradingMode::Live)
            .await;
        (engine, ok)
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while state.global_position_ready.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(guard);
    let (mut engine, ok) = task.await.unwrap();
    assert!(!ok);
    assert_eq!(
        engine.state.active_orders.read().await[&intent.client_order_id].quantity,
        intent.quantity
    );
    assert_eq!(
        db.load_pair_intents("SOLUSDC", TradingMode::Live)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(db.load_managed_orders("SOLUSDC").unwrap().len(), 1);
    fake.orders
        .lock()
        .unwrap()
        .get_mut(&intent.client_order_id)
        .unwrap()
        .status = "CANCELED".into();
    assert!(
        engine
            .drain_pair_intents("SOLUSDC", TradingMode::Live)
            .await
    );
    assert!(db
        .load_pair_intents("SOLUSDC", TradingMode::Live)
        .unwrap()
        .is_empty());
    assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn uncertain_pair_lookup_blocks_other_orders_until_resolved() {
    let (config, db, _intent) = pending_pair_fixture();
    let fake = PairExchange::default();
    fake.fail_lookup.store(1, Ordering::SeqCst);
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    assert!(
        !engine
            .drain_pair_intents("SOLUSDC", TradingMode::Live)
            .await
    );
    engine.maintain_grid_window().await;
    assert!(fake.posts.lock().unwrap().is_empty());
    assert_eq!(
        db.load_pair_intents("SOLUSDC", TradingMode::Live)
            .unwrap()
            .len(),
        1
    );

    assert!(
        engine
            .drain_pair_intents("SOLUSDC", TradingMode::Live)
            .await
    );
    assert!(fake.posts.lock().unwrap().is_empty());
    assert!(db
        .load_pair_intents("SOLUSDC", TradingMode::Live)
        .unwrap()
        .is_empty());
    server.abort();
}

#[tokio::test]
async fn skipped_counter_order_does_not_leave_a_historical_lock() {
    let (mut config, db, _intent) = pending_pair_fixture();
    config.grid.grid_interval = dec!(1);
    let fake = PairExchange::default();
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url.clone()).await;
    engine.state.ticker.write().await.last_price = dec!(102);
    assert!(
        engine
            .drain_pair_intents("SOLUSDC", TradingMode::Live)
            .await
    );
    assert!(db
        .load_pair_intents("SOLUSDC", TradingMode::Live)
        .unwrap()
        .is_empty());
    assert!(fake.posts.lock().unwrap().is_empty());
    assert!(
        engine
            .place_grid_order(OrderSide::Buy, dec!(98), "gb_b_duplicate".into(), -1, None)
            .await
    );
    drop(engine);
    let mut restarted = pair_recovery_engine(&config, db.clone(), url).await;
    restarted.state.ticker.write().await.last_price = dec!(100.5);
    assert!(restarted.sync_live_orders().await);
    assert!(
        !restarted
            .place_grid_order(
                OrderSide::Buy,
                dec!(100),
                "gb_b_after_restart".into(),
                -1,
                None
            )
            .await
    );
    assert!(matches!(
        restarted.pair_submission_decision(),
        super::PairPlacementDecision::Skip(_)
    ));
    assert_eq!(fake.posts.lock().unwrap().len(), 1);
    server.abort();
}

#[tokio::test]
async fn counter_sells_for_different_buys_cannot_duplicate_the_same_level() {
    let (config, db, intent) = pending_pair_fixture();
    let fake = PairExchange::default();
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    engine.state.position.write().await.size = dec!(10);
    let mut existing = intent.clone();
    existing.client_order_id = "gb_s_existing_exit".into();
    existing.paired_client_order_id = Some("gb_b_existing_parent".into());
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(existing.client_order_id.clone(), existing.clone());
    assert!(
        engine
            .drain_pair_intents("SOLUSDC", TradingMode::Live)
            .await
    );
    assert!(db
        .load_pair_intents("SOLUSDC", TradingMode::Live)
        .unwrap()
        .is_empty());
    let active = engine.state.active_orders.read().await;
    assert_eq!(active.len(), 1);
    assert!(active
        .values()
        .all(|o| o.price == intent.price && !o.is_take_profit));
    assert!(reserved_sell_quantity(&active.values().cloned().collect::<Vec<_>>()) <= dec!(10));
    drop(active);
    // Draining the same journal again must not submit either exit twice.
    assert!(
        engine
            .drain_pair_intents("SOLUSDC", TradingMode::Live)
            .await
    );
    assert!(fake.posts.lock().unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn cancel_all_clears_absent_pair_without_submitting_it() {
    let (config, db, _) = pending_pair_fixture();
    let fake = PairExchange::default();
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    assert!(engine.cancel_all_orders().await);
    assert!(db
        .load_pair_intents("SOLUSDC", TradingMode::Live)
        .unwrap()
        .is_empty());
    assert!(fake.posts.lock().unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn terminal_pair_fill_after_restart_keeps_cycle_metadata() {
    let (config, db, intent) = pending_pair_fixture();
    let fake = PairExchange::default();
    fake.orders.lock().unwrap().insert(
        intent.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 501,
            client_order_id: intent.client_order_id.clone(),
            symbol: intent.symbol.clone(),
            status: "FILLED".into(),
            price: intent.price,
            avg_price: Some(intent.price),
            orig_qty: intent.quantity,
            executed_qty: intent.quantity,
            side: "SELL".into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    engine.state.pause_trading().await.unwrap();
    assert!(
        engine
            .drain_pair_intents("SOLUSDC", TradingMode::Live)
            .await
    );
    assert!(fake.posts.lock().unwrap().is_empty());
    assert!(db
        .load_pair_intents("SOLUSDC", TradingMode::Live)
        .unwrap()
        .is_empty());
    let trade = db
        .get_trade_by_client_id(&intent.client_order_id, TradingMode::Live)
        .unwrap()
        .unwrap();
    assert_eq!(trade.side, OrderSide::Sell);
    assert_eq!(
        db.load_scoped_stats("SOLUSDC", TradingMode::Live)
            .unwrap()
            .unwrap()
            .1,
        1
    );
    server.abort();
}

#[tokio::test]
async fn live_buy_restarts_with_last_price_blocked_and_only_ordinary_window_orders() {
    let (mut config, db, _) = pending_pair_fixture();
    db.clear_pair_intents("SOLUSDC", TradingMode::Live).unwrap();
    config.grid.order_amount_usdc = dec!(2000);
    let fake = PairExchange::default();
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url.clone()).await;
    engine.state.ticker.write().await.last_price = dec!(114.5);
    let mut buy = new_pair_intent(
        &GridOrder {
            client_order_id: "gb_b_parent".into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side: OrderSide::Buy,
            price: dec!(114),
            quantity: dec!(17.54),
            amount_usdc: dec!(1999.56),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: -1,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: OrderPurpose::Grid,
            merge_sources: vec![],
        },
        OrderSide::Buy,
        dec!(114),
        dec!(17.54),
    );
    buy.paired_client_order_id = None;
    assert!(engine.on_order_filled(&mut buy).await);
    assert!(db
        .load_pair_intents("SOLUSDC", TradingMode::Live)
        .unwrap()
        .is_empty());
    assert!(fake.posts.lock().unwrap().is_empty());
    drop(engine);
    let mut restarted = pair_recovery_engine(&config, db.clone(), url).await;
    restarted.state.ticker.write().await.last_price = dec!(114.5);
    restarted.state.position.write().await.size = buy.quantity;
    assert!(
        !restarted
            .place_grid_order(OrderSide::Buy, dec!(114), "gb_b_repeat".into(), -1, None)
            .await
    );
    restarted.maintain_grid_window().await;
    let active = restarted.state.active_orders.read().await;
    let sell = active.values().find(|o| o.side == OrderSide::Sell).unwrap();
    assert_eq!(sell.price, dec!(115));
    assert_eq!(sell.quantity, dec!(17.39));
    assert!(sell.paired_client_order_id.is_none());
    server.abort();
}

#[tokio::test]
async fn existing_destination_order_keeps_its_identity_without_pairing() {
    let (config, db, mut existing) = pending_pair_fixture();
    db.clear_pair_intents("SOLUSDC", TradingMode::Live).unwrap();
    existing.client_order_id = "gb_s_existing_destination".into();
    existing.order_id = Some(500);
    existing.paired_client_order_id = None;
    db.save_managed_order(&existing).unwrap();
    let fake = PairExchange::default();
    fake.orders.lock().unwrap().insert(
        existing.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 500,
            client_order_id: existing.client_order_id.clone(),
            symbol: "SOLUSDC".into(),
            status: "NEW".into(),
            price: dec!(101),
            avg_price: None,
            orig_qty: dec!(1),
            executed_qty: dec!(0),
            side: "SELL".into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url.clone()).await;
    engine.state.ticker.write().await.last_price = dec!(100.5);
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(existing.client_order_id.clone(), existing.clone());
    let mut buy = existing.clone();
    buy.client_order_id = "gb_b_destination_parent".into();
    buy.order_id = None;
    buy.side = OrderSide::Buy;
    buy.price = dec!(100);
    buy.quantity = dec!(0.46);
    buy.amount_usdc = buy.price * buy.quantity;
    assert!(engine.on_order_filled(&mut buy).await);
    let intents = db.load_pair_intents("SOLUSDC", TradingMode::Live).unwrap();
    assert!(intents.is_empty());
    drop(engine);
    let mut restarted = pair_recovery_engine(&config, db.clone(), url).await;
    restarted.state.ticker.write().await.last_price = dec!(100.5);
    assert!(restarted.sync_live_orders().await);
    assert!(fake.posts.lock().unwrap().is_empty());
    assert!(db
        .load_pair_intents("SOLUSDC", TradingMode::Live)
        .unwrap()
        .is_empty());
    let managed = db.load_managed_orders("SOLUSDC").unwrap();
    assert_eq!(managed.len(), 1);
    assert_eq!(managed[0].paired_client_order_id.as_deref(), None);
    assert!(
        !restarted
            .place_grid_order(OrderSide::Buy, dec!(100), "gb_b_repeat".into(), -1, None)
            .await
    );
    server.abort();
}

#[tokio::test]
async fn already_recorded_terminal_counter_order_retires_its_pending_intent() {
    let (config, db, intent) = pending_pair_fixture();
    db.insert_trade(&TradeRecord {
        trade_id: format!("grid:{}", intent.client_order_id),
        client_order_id: intent.client_order_id.clone(),
        symbol: intent.symbol.clone(),
        mode: TradingMode::Live,
        side: OrderSide::Sell,
        price: intent.price,
        quantity: intent.quantity,
        amount_usdc: intent.amount_usdc,
        realized_pnl: dec!(1),
        commission: dec!(0),
        pnl_verified: true,
        is_maker: true,
        timestamp: Utc::now(),
        note: "already reconciled".into(),
    })
    .unwrap();
    let fake = PairExchange::default();
    fake.orders.lock().unwrap().insert(
        intent.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 500,
            client_order_id: intent.client_order_id.clone(),
            symbol: intent.symbol.clone(),
            status: "FILLED".into(),
            price: intent.price,
            avg_price: Some(intent.price),
            orig_qty: intent.quantity,
            executed_qty: intent.quantity,
            side: "SELL".into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    assert!(
        engine
            .drain_pair_intents("SOLUSDC", TradingMode::Live)
            .await
    );
    assert!(db
        .load_pair_intents("SOLUSDC", TradingMode::Live)
        .unwrap()
        .is_empty());
    assert!(fake.posts.lock().unwrap().is_empty());
    assert_eq!(db.get_recent_trades(10).unwrap().len(), 2);
    server.abort();
}

#[tokio::test]
async fn repeated_live_fill_records_one_trade_without_a_pair_intent() {
    let (config, db, _) = pending_pair_fixture();
    db.clear_pair_intents("SOLUSDC", TradingMode::Live).unwrap();
    let fake = PairExchange::default();
    let (url, server) = pair_exchange_server(fake.clone()).await;
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    engine.state.ticker.write().await.last_price = dec!(102.1);
    let price = dec!(102);
    let quantity = engine
        .state
        .rules
        .read()
        .await
        .calculate_quantity(price, config.grid.order_amount_usdc)
        .unwrap();
    let mut order = GridOrder {
        client_order_id: "gb_s_replayed".into(),
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
        purpose: crate::types::OrderPurpose::Legacy,
        merge_sources: Vec::new(),
    };
    assert!(engine.on_order_filled(&mut order).await);
    assert!(engine.on_order_filled(&mut order).await);
    assert!(fake.posts.lock().unwrap().is_empty());
    assert_eq!(
        db.get_recent_trades_for("SOLUSDC", TradingMode::Live, 10)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        db.load_scoped_stats("SOLUSDC", TradingMode::Live)
            .unwrap()
            .unwrap()
            .0,
        2
    );
    let intents = db.load_pair_intents("SOLUSDC", TradingMode::Live).unwrap();
    assert!(intents.is_empty());
    server.abort();
}

#[tokio::test]
async fn unresolved_order_blocks_replenishment() {
    let app = Router::new()
        .route(
            "/fapi/v1/openOrders",
            get(|| async { Json(Vec::<BinanceOrderResponse>::new()) }),
        )
        .route(
            "/fapi/v1/order",
            get(|Query(query): Query<HashMap<String, String>>| async move {
                if query.get("orderId").is_some_and(|id| id == "43") {
                    let order = BinanceOrderResponse {
                        order_id: 43,
                        client_order_id: "gb_b_filled".into(),
                        symbol: "SOLUSDC".into(),
                        status: "FILLED".into(),
                        price: dec!(100),
                        avg_price: Some(dec!(100)),
                        orig_qty: dec!(1),
                        executed_qty: dec!(1),
                        side: "BUY".into(),
                        order_type: "LIMIT".into(),
                        time_in_force: "GTX".into(),
                        update_time: None,
                    };
                    (
                        axum::http::StatusCode::OK,
                        Json(serde_json::to_value(order).unwrap()),
                    )
                } else {
                    (
                        axum::http::StatusCode::SERVICE_UNAVAILABLE,
                        Json(serde_json::json!({})),
                    )
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let mut config = AppConfig::default();
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_ticker_tx, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db.clone(), action_tx);
    let order = GridOrder {
        client_order_id: "gb_b_unresolved".into(),
        order_id: Some(42),
        symbol: "SOLUSDC".into(),
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(1),
        amount_usdc: dec!(100),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: -1,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Legacy,
        merge_sources: Vec::new(),
    };
    let mut filled = order.clone();
    filled.client_order_id = "gb_b_filled".into();
    filled.order_id = Some(43);
    {
        let mut active = state.active_orders.write().await;
        active.insert(order.client_order_id.clone(), order);
        active.insert(filled.client_order_id.clone(), filled);
    }
    let client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);

    assert!(!engine.sync_live_orders().await);
    assert!(engine.last_orders_sync.is_none());
    assert_eq!(state.active_orders.read().await.len(), 2);
    assert!(db.get_recent_trades(10).unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn external_pause_is_not_overridden_by_internal_resume() {
    let config = AppConfig::default();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_ticker_tx, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db.clone(), action_tx);
    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    let active_guard = state.active_orders.write().await;
    let cancellation = tokio::spawn(async move { engine.cancel_all_orders().await });

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while state.pause_epoch() == 0 || db.load_bot_status().unwrap() != Some(BotStatus::Paused) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    state.pause_trading().await.unwrap();
    drop(active_guard);

    assert!(cancellation.await.unwrap());
    assert_eq!(*state.status.read().await, BotStatus::Paused);
    assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
}

#[tokio::test]
async fn cancel_all_only_touches_managed_exchange_orders() {
    #[derive(Clone)]
    struct FakeExchange {
        fetches: Arc<AtomicUsize>,
        canceled: Arc<Mutex<Vec<String>>>,
    }
    let fake = FakeExchange {
        fetches: Arc::new(AtomicUsize::new(0)),
        canceled: Arc::new(Mutex::new(Vec::new())),
    };
    let app =
        Router::new()
            .route(
                "/fapi/v1/openOrders",
                get(|State(fake): State<FakeExchange>| async move {
                    let order = |id, client_id: &str| BinanceOrderResponse {
                        order_id: id,
                        client_order_id: client_id.into(),
                        symbol: "SOLUSDC".into(),
                        status: "NEW".into(),
                        price: dec!(100),
                        avg_price: None,
                        orig_qty: dec!(1),
                        executed_qty: dec!(0),
                        side: "BUY".into(),
                        order_type: "LIMIT".into(),
                        time_in_force: "GTX".into(),
                        update_time: None,
                    };
                    let orders = if fake.fetches.fetch_add(1, Ordering::SeqCst) == 0 {
                        vec![order(99, "manual-order"), order(42, "gb_b_owned")]
                    } else {
                        Vec::new()
                    };
                    Json(orders)
                }),
            )
            .route(
                "/fapi/v1/order",
                delete(
                    |State(fake): State<FakeExchange>,
                     Query(query): Query<HashMap<String, String>>| async move {
                        fake.canceled.lock().unwrap().push(query["orderId"].clone());
                        Json(serde_json::json!({}))
                    },
                ),
            )
            .with_state(fake.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let mut config = AppConfig::default();
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_ticker_tx, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
    let mut engine = GridTradingEngine::new(state, client, action_rx, ticker_rx);

    assert!(engine.cancel_all_orders().await);
    assert_eq!(*fake.canceled.lock().unwrap(), vec!["42"]);
    server.abort();
}

#[tokio::test]
async fn switching_live_to_paper_cancels_live_orders_before_commit() {
    #[derive(Clone)]
    struct FakeExchange {
        fetches: Arc<AtomicUsize>,
        cancellations: Arc<AtomicUsize>,
    }
    let fake = FakeExchange {
        fetches: Arc::new(AtomicUsize::new(0)),
        cancellations: Arc::new(AtomicUsize::new(0)),
    };
    let app = Router::new()
            .route("/fapi/v1/exchangeInfo", get(|| async {
                Json(serde_json::json!({"symbols":[{"symbol":"SOLUSDC","status":"TRADING","pricePrecision":2,"quantityPrecision":2,"filters":[{"filterType":"PRICE_FILTER","minPrice":"0.01","maxPrice":"100000","tickSize":"0.01"},{"filterType":"LOT_SIZE","minQty":"0.01","maxQty":"100000","stepSize":"0.01"},{"filterType":"MIN_NOTIONAL","notional":"5"}]}]}))
            }))
            .route("/fapi/v1/ticker/price", get(|| async {
                Json(serde_json::json!({"symbol":"SOLUSDC","price":"100","time":0}))
            }))
            .route("/fapi/v1/openOrders", get(|State(fake): State<FakeExchange>| async move {
                if fake.fetches.fetch_add(1, Ordering::SeqCst) == 0 {
                    Json(serde_json::json!([{"orderId":42,"clientOrderId":"gb_b_live","symbol":"SOLUSDC","status":"NEW","price":"99","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX","updateTime":0}]))
                } else {
                    Json(serde_json::json!([]))
                }
            }))
            .route("/fapi/v1/order", delete(|State(fake): State<FakeExchange>| async move {
                fake.cancellations.fetch_add(1, Ordering::SeqCst);
                Json(serde_json::json!({}))
            }))
            .with_state(fake.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let mut config = AppConfig::default();
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_ticker_tx, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    let mut paper = config;
    paper.exchange.api_key.clear();
    paper.exchange.api_secret.clear();

    engine.apply_config(paper).await.unwrap();
    assert_eq!(fake.cancellations.load(Ordering::SeqCst), 1);
    assert!(state.config.read().await.exchange.is_paper());
    server.abort();
}

#[tokio::test]
async fn buy_window_respects_position_and_open_buy_exposure() {
    let mut config = AppConfig::default();
    config.grid.grid_interval = dec!(1);
    config.grid.order_amount_usdc = dec!(100);
    config.grid.buy_window = 3;
    config.grid.sell_window = 1;
    config.grid.max_position_usdc = Some(dec!(150));
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_ticker_tx, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    {
        let mut ticker = state.ticker.write().await;
        ticker.last_price = dec!(100);
        ticker.update_time = Utc::now();
        ticker.mark_update_time = Utc::now();
    }
    state.position.write().await.size = dec!(0.5);

    engine.maintain_grid_window().await;
    let orders: Vec<_> = state.active_orders.read().await.values().cloned().collect();
    assert_eq!(
        orders
            .iter()
            .filter(|order| order.side == OrderSide::Buy)
            .count(),
        1
    );
    assert!(buy_exposure_usdc(dec!(0.5), dec!(100), &orders) <= dec!(150));

    engine.handle_control_action(BotControlAction::Pause).await;
    assert_eq!(*state.status.read().await, BotStatus::Paused);
    assert!(state.active_orders.read().await.is_empty());
}

#[test]
fn exchange_fills_include_losses_and_fees() {
    let fills = vec![
        BinanceUserTrade {
            realized_pnl: dec!(1.20),
            commission: dec!(0.03),
            commission_asset: "USDC".into(),
            maker: true,
        },
        BinanceUserTrade {
            realized_pnl: dec!(-2.00),
            commission: dec!(0.04),
            commission_asset: "USDC".into(),
            maker: false,
        },
    ];
    assert_eq!(
        aggregate_execution_pnl("SOLUSDC", &fills).unwrap(),
        (dec!(-0.80), dec!(0.07), false)
    );
    let mut wrong_asset = fills;
    wrong_asset[0].commission_asset = "BNB".into();
    assert!(aggregate_execution_pnl("SOLUSDC", &wrong_asset).is_err());
    assert!(aggregate_execution_pnl("SOLUSDC", &[]).is_err());
}

#[tokio::test]
async fn paper_fill_persistence_failure_preserves_account_and_retry_applies_once() {
    for (direction, side, initial_size, price, expected_size, expected_pnl) in [
        (
            GridDirection::Long,
            OrderSide::Buy,
            dec!(0),
            dec!(100),
            dec!(1),
            dec!(0),
        ),
        (
            GridDirection::Long,
            OrderSide::Sell,
            dec!(2),
            dec!(110),
            dec!(1),
            dec!(10),
        ),
        (
            GridDirection::Short,
            OrderSide::Sell,
            dec!(0),
            dec!(100),
            dec!(-1),
            dec!(0),
        ),
        (
            GridDirection::Short,
            OrderSide::Buy,
            dec!(-2),
            dec!(90),
            dec!(-1),
            dec!(10),
        ),
    ] {
        let path = std::env::temp_dir().join(format!("paper-fill-{}.db", uuid::Uuid::new_v4()));
        let db = Arc::new(Database::open(&path).unwrap());
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TRIGGER reject_paper_stats BEFORE INSERT ON scoped_grid_stats \
                 BEGIN SELECT RAISE(ABORT, 'reject stats'); END;",
            )
            .unwrap();
        let mut config = AppConfig::default();
        config.grid.direction = direction;
        let (action_tx, action_rx) = mpsc::channel(1);
        let (_ticker_tx, ticker_rx) = broadcast::channel(1);
        let state = AppState::new(config.clone(), db.clone(), action_tx);
        let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
        let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
        {
            let mut position = state.position.write().await;
            position.size = initial_size;
            position.entry_price = if initial_size.is_zero() {
                dec!(0)
            } else {
                dec!(100)
            };
            position.mark_price = dec!(100);
            position.leverage = 10;
        }
        state.ticker.write().await.last_price = dec!(100);
        let position_before = serde_json::to_value(&*state.position.read().await).unwrap();
        let account_before = serde_json::to_value(&*state.account.read().await).unwrap();
        let stats_before = serde_json::to_value(&*state.stats.read().await).unwrap();
        let wallet_before = state.account.read().await.total_wallet_balance;
        let mut order = GridOrder {
            client_order_id: "paper-retry".into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side,
            price,
            quantity: dec!(1),
            amount_usdc: price,
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 1,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Legacy,
            merge_sources: Vec::new(),
        };
        state
            .active_orders
            .write()
            .await
            .insert(order.client_order_id.clone(), order.clone());

        assert!(!engine.on_order_filled(&mut order).await);
        assert_eq!(*state.status.read().await, BotStatus::Paused);
        assert!(state
            .active_orders
            .read()
            .await
            .contains_key(&order.client_order_id));
        assert_eq!(
            serde_json::to_value(&*state.position.read().await).unwrap(),
            position_before
        );
        assert_eq!(
            serde_json::to_value(&*state.account.read().await).unwrap(),
            account_before
        );
        assert_eq!(
            serde_json::to_value(&*state.stats.read().await).unwrap(),
            stats_before
        );
        assert!(db.get_recent_trades(10).unwrap().is_empty());

        connection
            .execute_batch("DROP TRIGGER reject_paper_stats")
            .unwrap();
        assert!(engine.on_order_filled(&mut order).await);
        assert_eq!(state.position.read().await.size, expected_size);
        assert_eq!(state.position.read().await.entry_price, dec!(100));
        assert_eq!(
            state.account.read().await.total_wallet_balance,
            wallet_before + expected_pnl
        );
        assert!(!state
            .active_orders
            .read()
            .await
            .contains_key(&order.client_order_id));
        assert_eq!(state.stats.read().await.total_trades, 1);
        assert_eq!(state.stats.read().await.total_realized_pnl, expected_pnl);
        let trades = db.get_recent_trades(10).unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].realized_pnl, expected_pnl);
        let position_after = serde_json::to_value(&*state.position.read().await).unwrap();
        let account_after = serde_json::to_value(&*state.account.read().await).unwrap();
        assert!(engine.on_order_filled(&mut order).await);
        assert_eq!(
            serde_json::to_value(&*state.position.read().await).unwrap(),
            position_after
        );
        assert_eq!(
            serde_json::to_value(&*state.account.read().await).unwrap(),
            account_after
        );
        assert_eq!(state.stats.read().await.total_trades, 1);
        assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);

        drop(engine);
        drop(state);
        drop(connection);
        drop(db);
        std::fs::remove_file(path).unwrap();
    }
}

#[tokio::test]
async fn unpaired_paper_sell_records_realized_pnl() {
    let config = AppConfig::default();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_ticker_tx, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    state.position.write().await.size = dec!(2.63);
    state.position.write().await.entry_price = dec!(114);
    {
        let mut ticker = state.ticker.write().await;
        ticker.last_price = dec!(115);
        ticker.update_time = Utc::now();
        ticker.mark_update_time = Utc::now();
    }
    let mut order = GridOrder {
        client_order_id: "paper-sell".into(),
        order_id: None,
        symbol: "SOLUSDC".into(),
        side: OrderSide::Sell,
        price: dec!(115.1),
        quantity: dec!(1),
        amount_usdc: dec!(115.1),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: 1,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Legacy,
        merge_sources: Vec::new(),
    };
    state
        .active_orders
        .write()
        .await
        .insert(order.client_order_id.clone(), order.clone());

    engine.on_order_filled(&mut order).await;
    let stats = state.stats.read().await;
    assert_eq!(stats.total_realized_pnl, dec!(1.1));
    assert_eq!(stats.completed_cycles, 0);
    drop(stats);
    let trades = state.db.get_recent_trades(10).unwrap();
    assert_eq!(trades[0].realized_pnl, dec!(1.1));
    assert!(trades[0].pnl_verified);
}

#[tokio::test]
async fn sell_window_places_one_remainder_and_replaces_oversized_orders() {
    let mut config = AppConfig::default();
    config.grid.grid_interval = dec!(1);
    config.grid.order_amount_usdc = dec!(2000);
    config.grid.buy_window = 1;
    config.grid.sell_window = 3;
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_ticker_tx, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    {
        let mut ticker = state.ticker.write().await;
        ticker.last_price = dec!(115);
        ticker.update_time = Utc::now();
        ticker.mark_update_time = Utc::now();
    }
    state.position.write().await.size = dec!(19.67);
    engine.maintain_grid_window().await;
    let orders: Vec<_> = state.active_orders.read().await.values().cloned().collect();
    let sell = orders
        .iter()
        .filter(|o| o.side == OrderSide::Sell)
        .min_by_key(|o| o.price)
        .unwrap();
    let expected_qty = state
        .rules
        .read()
        .await
        .calculate_quantity(sell.price, dec!(2000))
        .unwrap();
    assert_eq!(sell.quantity, expected_qty);
    assert_eq!(
        orders.iter().filter(|o| o.side == OrderSide::Sell).count(),
        2
    );
    assert_eq!(reserved_sell_quantity(&orders), dec!(19.67));
    assert_eq!(sell_quantity_available(dec!(19.67), &orders), dec!(0));

    engine.maintain_grid_window().await;
    assert_eq!(state.active_orders.read().await.len(), orders.len());

    let mut oversized = sell.clone();
    oversized.quantity = dec!(20);
    oversized.amount_usdc = oversized.price * oversized.quantity;
    state
        .active_orders
        .write()
        .await
        .insert(oversized.client_order_id.clone(), oversized);

    engine.maintain_grid_window().await;
    let orders: Vec<_> = state.active_orders.read().await.values().cloned().collect();
    assert_eq!(reserved_sell_quantity(&orders), dec!(19.67));
    assert_eq!(
        orders.iter().filter(|o| o.side == OrderSide::Sell).count(),
        2
    );

    state.position.write().await.size = dec!(-1);
    engine.trim_closing_orders_to_position().await;
    let orders: Vec<_> = state.active_orders.read().await.values().cloned().collect();
    assert_eq!(reserved_sell_quantity(&orders), dec!(0));
}

#[tokio::test]
async fn sell_remainder_respects_symbol_minimum_after_step_rounding() {
    let mut config = AppConfig::default();
    config.grid.grid_interval = dec!(1);
    config.grid.order_amount_usdc = dec!(2000);
    config.grid.buy_window = 0;
    config.grid.sell_window = 3;
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_ticker_tx, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    {
        let mut ticker = state.ticker.write().await;
        ticker.last_price = dec!(115);
        ticker.update_time = Utc::now();
        ticker.mark_update_time = Utc::now();
    }
    state.rules.write().await.min_notional = dec!(50);
    state.position.write().await.size = dec!(0.429);

    engine.maintain_grid_window().await;
    assert!(state.active_orders.read().await.is_empty());

    state.position.write().await.size = dec!(0.449);
    engine.maintain_grid_window().await;
    let orders: Vec<_> = state.active_orders.read().await.values().cloned().collect();
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].side, OrderSide::Sell);
    assert_eq!(orders[0].quantity, dec!(0.44));
    assert_eq!(orders[0].amount_usdc, dec!(51.04));

    engine.maintain_grid_window().await;
    assert_eq!(state.active_orders.read().await.len(), 1);
}

#[tokio::test]
async fn sell_window_adds_remainder_after_existing_full_orders() {
    let mut config = AppConfig::default();
    config.grid.grid_interval = dec!(1.2);
    config.grid.order_amount_usdc = dec!(3000);
    config.grid.buy_window = 0;
    config.grid.sell_window = 3;
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_ticker_tx, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    {
        let mut ticker = state.ticker.write().await;
        ticker.last_price = dec!(122.15);
        ticker.update_time = Utc::now();
        ticker.mark_update_time = Utc::now();
    }
    state.position.write().await.size = dec!(50.45);

    let first = GridOrder {
        client_order_id: "gb_s_existing_1".into(),
        order_id: None,
        symbol: "SOLUSDC".into(),
        side: OrderSide::Sell,
        price: dec!(122.4),
        quantity: dec!(24.50),
        amount_usdc: dec!(2998.8),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: 1,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Legacy,
        merge_sources: Vec::new(),
    };
    let second = GridOrder {
        client_order_id: "gb_s_existing_2".into(),
        price: dec!(123.6),
        quantity: dec!(24.27),
        amount_usdc: dec!(2999.772),
        ..first.clone()
    };
    let mut active = state.active_orders.write().await;
    active.insert(first.client_order_id.clone(), first);
    active.insert(second.client_order_id.clone(), second);
    drop(active);

    engine.maintain_grid_window().await;
    let orders: Vec<_> = state.active_orders.read().await.values().cloned().collect();
    assert_eq!(orders.len(), 3);
    assert_eq!(reserved_sell_quantity(&orders), dec!(50.45));
    let remainder = orders.iter().find(|o| o.price == dec!(124.8)).unwrap();
    assert_eq!(remainder.quantity, dec!(1.68));
    assert_eq!(remainder.amount_usdc, dec!(209.664));
}

#[tokio::test]
async fn fills_record_actual_quantities_without_creating_counter_orders() {
    let config = AppConfig::default();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, rx) = mpsc::channel(1);
    let (_, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db.clone(), tx);
    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let mut engine = GridTradingEngine::new(state.clone(), client, rx, ticker_rx);
    state.position.write().await.size = dec!(20);
    for (id, side, price, quantity) in [
        ("gb_b_actual", OrderSide::Buy, dec!(114.24), dec!(17.50)),
        ("gb_s_actual", OrderSide::Sell, dec!(115.24), dec!(0.46)),
    ] {
        let mut order = GridOrder {
            client_order_id: id.into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side,
            price,
            quantity,
            amount_usdc: price * quantity,
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 0,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: OrderPurpose::Grid,
            merge_sources: vec![],
        };
        assert!(engine.on_order_filled(&mut order).await);
        assert_eq!(
            db.get_trade_by_client_id(id, TradingMode::Paper)
                .unwrap()
                .unwrap()
                .quantity,
            quantity
        );
        assert!(state.active_orders.read().await.is_empty());
    }
    assert_eq!(state.position.read().await.size, dec!(37.04));
    assert!(db
        .load_pair_intents("SOLUSDC", TradingMode::Paper)
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn ticker_update_keeps_independent_mark_price() {
    let config = AppConfig::default();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_ticker_tx, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    let mark_updated_at = Utc::now();
    {
        let mut ticker = state.ticker.write().await;
        ticker.mark_price = dec!(114.3);
        ticker.mark_update_time = mark_updated_at;
    }

    engine
        .handle_ticker_update(TickerInfo {
            symbol: config.exchange.symbol,
            last_price: dec!(114.5),
            update_time: Utc::now(),
            ..Default::default()
        })
        .await;

    let ticker = state.ticker.read().await;
    assert_eq!(ticker.last_price, dec!(114.5));
    assert_eq!(ticker.mark_price, dec!(114.3));
    assert_eq!(ticker.mark_update_time, mark_updated_at);
}

#[tokio::test]
async fn mark_update_preserves_ticker_and_does_not_fill_paper_orders() {
    let config = AppConfig::default();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    let now = Utc::now();
    {
        let mut ticker = state.ticker.write().await;
        ticker.last_price = dec!(100);
        ticker.high_24h = dec!(110);
        ticker.update_time = now;
    }
    *state.status.write().await = BotStatus::Running;
    state.active_orders.write().await.insert(
        "gb_b_test".into(),
        GridOrder {
            client_order_id: "gb_b_test".into(),
            order_id: None,
            symbol: config.exchange.symbol.clone(),
            side: OrderSide::Buy,
            price: dec!(99),
            quantity: dec!(1),
            amount_usdc: dec!(99),
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
            grid_level: -1,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: OrderPurpose::Grid,
            merge_sources: Vec::new(),
        },
    );
    {
        let mut position = state.position.write().await;
        position.size = dec!(1);
        position.entry_price = dec!(90);
    }
    engine
        .handle_ticker_update(TickerInfo {
            symbol: config.exchange.symbol.clone(),
            mark_price: dec!(102),
            mark_update_time: Utc::now(),
            ..Default::default()
        })
        .await;
    let ticker = state.ticker.read().await.clone();
    assert_eq!(ticker.last_price, dec!(100));
    assert_eq!(ticker.high_24h, dec!(110));
    assert_eq!(ticker.update_time, now);
    assert_eq!(ticker.mark_price, dec!(102));
    assert_eq!(state.position.read().await.unrealized_pnl, dec!(12));
    assert!(state.active_orders.read().await.contains_key("gb_b_test"));

    engine
        .handle_ticker_update(TickerInfo {
            symbol: config.exchange.symbol,
            mark_price: dec!(50),
            mark_update_time: now - chrono::Duration::seconds(1),
            ..Default::default()
        })
        .await;
    assert_eq!(state.ticker.read().await.mark_price, dec!(102));
}

#[tokio::test]
async fn mark_rest_fallback_only_refreshes_missing_or_stale_values() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let calls = Arc::new(AtomicUsize::new(0));
    let fail = Arc::new(AtomicBool::new(false));
    let app = Router::new().route(
        "/fapi/v1/premiumIndex",
        get({
            let calls = calls.clone();
            let fail = fail.clone();
            move || {
                calls.fetch_add(1, Ordering::SeqCst);
                let failed = fail.load(Ordering::SeqCst);
                async move {
                    if failed {
                        (
                            axum::http::StatusCode::SERVICE_UNAVAILABLE,
                            Json(serde_json::json!({"code": -1000})),
                        )
                    } else {
                        (
                            axum::http::StatusCode::OK,
                            Json(serde_json::json!({"symbol": "SOLUSDC", "markPrice": "105"})),
                        )
                    }
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let config = AppConfig::default();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    let symbol = &config.exchange.symbol;
    {
        let mut ticker = state.ticker.write().await;
        ticker.symbol = symbol.clone();
        ticker.last_price = dec!(100);
        ticker.update_time = Utc::now();
    }
    engine.refresh_mark_price(symbol).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(state.ticker.read().await.mark_price, dec!(105));
    engine.refresh_mark_price(symbol).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let stale = Utc::now() - chrono::Duration::seconds(16);
    state.ticker.write().await.mark_update_time = stale;
    fail.store(true, Ordering::SeqCst);
    engine.refresh_mark_price(symbol).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let ticker = state.ticker.read().await;
    assert_eq!(ticker.mark_update_time, stale);
    assert!(!ticker.prices_are_fresh(Utc::now()));
    drop(ticker);
    fail.store(false, Ordering::SeqCst);
    engine.refresh_mark_price(symbol).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(!state.ticker.read().await.prices_are_fresh(Utc::now()));
    engine.mark_retry.retry_at = Some(Instant::now() - std::time::Duration::from_secs(1));
    engine.refresh_mark_price(symbol).await;
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert!(state.ticker.read().await.prices_are_fresh(Utc::now()));

    state.ticker.write().await.mark_update_time = stale;
    fail.store(true, Ordering::SeqCst);
    engine.refresh_mark_price(symbol).await;
    assert!(!engine.mark_retry.ready());
    engine
        .handle_ticker_update(TickerInfo {
            symbol: symbol.clone(),
            mark_price: dec!(106),
            mark_update_time: Utc::now(),
            ..Default::default()
        })
        .await;
    assert!(engine.mark_retry.ready());
    assert!(state.ticker.read().await.prices_are_fresh(Utc::now()));
    let future = Utc::now() + chrono::Duration::seconds(30);
    state.ticker.write().await.mark_update_time = future;
    fail.store(false, Ordering::SeqCst);
    engine.refresh_mark_price(symbol).await;
    let ticker = state.ticker.read().await;
    assert_eq!(ticker.mark_price, dec!(106));
    assert_eq!(ticker.mark_update_time, future);
    assert!(!ticker.prices_are_fresh(Utc::now()));
    server.abort();
}

#[tokio::test]
async fn failed_ticker_fallback_backs_off_and_recovers_on_push_or_retry() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let calls = Arc::new(AtomicUsize::new(0));
    let available = Arc::new(AtomicBool::new(false));
    let app = Router::new()
            .route(
                "/fapi/v1/ticker/24hr",
                get({
                    let calls = calls.clone();
                    move || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        async { axum::http::StatusCode::SERVICE_UNAVAILABLE }
                    }
                }),
            )
            .route(
                "/fapi/v1/ticker/price",
                get({
                    let calls = calls.clone();
                    let available = available.clone();
                    move || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        let available = available.load(Ordering::SeqCst);
                        async move {
                            if available {
                                (
                                    axum::http::StatusCode::OK,
                                    Json(serde_json::json!({"symbol": "SOLUSDC", "price": "101", "time": 1})),
                                )
                            } else {
                                (
                                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                                    Json(serde_json::json!({})),
                                )
                            }
                        }
                    }
                }),
            );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let config = AppConfig::default();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    let symbol = &config.exchange.symbol;

    engine.refresh_stale_ticker(symbol).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    engine.refresh_stale_ticker(symbol).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(!state.ticker.read().await.prices_are_fresh(Utc::now()));
    engine
        .handle_ticker_update(TickerInfo {
            symbol: symbol.clone(),
            last_price: dec!(100),
            update_time: Utc::now(),
            ..Default::default()
        })
        .await;
    assert!(engine.ticker_retry.ready());
    assert!(state.ticker.read().await.prices_are_fresh(Utc::now()));

    state.ticker.write().await.update_time = Utc::now() - chrono::Duration::seconds(16);
    engine.refresh_stale_ticker(symbol).await;
    assert!(!engine.ticker_retry.ready());
    available.store(true, Ordering::SeqCst);
    engine.ticker_retry.retry_at = Some(Instant::now() - std::time::Duration::from_secs(1));
    engine.refresh_stale_ticker(symbol).await;
    assert_eq!(calls.load(Ordering::SeqCst), 6);
    assert_eq!(state.ticker.read().await.last_price, dec!(101));
    assert!(state.ticker.read().await.prices_are_fresh(Utc::now()));
    server.abort();
}

#[tokio::test]
async fn delayed_mark_rest_response_does_not_overwrite_a_newer_update() {
    let requested = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let app = Router::new().route(
        "/fapi/v1/premiumIndex",
        get({
            let requested = requested.clone();
            let release = release.clone();
            move || {
                let requested = requested.clone();
                let release = release.clone();
                async move {
                    requested.notify_one();
                    release.notified().await;
                    Json(serde_json::json!({"symbol": "SOLUSDC", "markPrice": "90"}))
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let config = AppConfig::default();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    let ((), ()) = tokio::join!(engine.refresh_mark_price(&config.exchange.symbol), async {
        requested.notified().await;
        let mut ticker = state.ticker.write().await;
        ticker.mark_price = dec!(110);
        ticker.mark_update_time = Utc::now();
        drop(ticker);
        release.notify_one();
    });
    assert_eq!(state.ticker.read().await.mark_price, dec!(110));
    server.abort();
}

#[tokio::test]
async fn queued_mark_event_from_during_rest_request_is_not_discarded() {
    let requested = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let app = Router::new().route(
        "/fapi/v1/premiumIndex",
        get({
            let requested = requested.clone();
            let release = release.clone();
            move || {
                let requested = requested.clone();
                let release = release.clone();
                async move {
                    requested.notify_one();
                    release.notified().await;
                    Json(serde_json::json!({"symbol": "SOLUSDC", "markPrice": "90"}))
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let config = AppConfig::default();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    let (_, event_time) = tokio::join!(engine.refresh_mark_price(&config.exchange.symbol), async {
        requested.notified().await;
        let event_time = Utc::now();
        release.notify_one();
        event_time
    });
    engine
        .handle_ticker_update(TickerInfo {
            symbol: config.exchange.symbol,
            mark_price: dec!(110),
            mark_update_time: event_time,
            ..Default::default()
        })
        .await;
    assert_eq!(state.ticker.read().await.mark_price, dec!(110));
    server.abort();
}

#[tokio::test]
async fn market_fallbacks_have_one_request_per_type_and_apply_valid_completions() {
    let started = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
            .route("/fapi/v1/premiumIndex", get({
                let started = started.clone();
                let release = release.clone();
                let calls = calls.clone();
                move || {
                    let started = started.clone();
                    let release = release.clone();
                    let calls = calls.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        started.add_permits(1);
                        release.acquire().await.unwrap().forget();
                        Json(serde_json::json!({"symbol":"SOLUSDC", "markPrice":"101"}))
                    }
                }
            }))
            .route("/fapi/v1/ticker/24hr", get({
                let started = started.clone();
                let release = release.clone();
                let calls = calls.clone();
                move || {
                    let started = started.clone();
                    let release = release.clone();
                    let calls = calls.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        started.add_permits(1);
                        release.acquire().await.unwrap().forget();
                        Json(serde_json::json!({"symbol":"SOLUSDC", "lastPrice":"100", "highPrice":"110", "lowPrice":"90", "priceChange":"1", "priceChangePercent":"1", "volume":"12", "quoteVolume":"1200"}))
                    }
                }
            }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let config = AppConfig::default();
    let mut engine =
        pair_recovery_engine(&config, Arc::new(Database::open(":memory:").unwrap()), url).await;
    *engine.state.status.write().await = BotStatus::Paused;
    *engine.state.ticker.write().await = TickerInfo {
        symbol: config.exchange.symbol.clone(),
        ..Default::default()
    };
    for _ in 0..3 {
        engine
            .start_mark_price_request(&config.exchange.symbol)
            .await;
        engine.start_ticker_request(&config.exchange.symbol).await;
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), started.acquire_many(2))
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(engine.state.ticker.read().await.last_price, Decimal::ZERO);
    release.add_permits(2);
    let mark = engine.mark_price_task.take().unwrap().await.unwrap();
    let ticker = engine.ticker_task.take().unwrap().await.unwrap();
    let mark_requested_at = mark.result.as_ref().unwrap().1;
    let ticker_requested_at = ticker.result.as_ref().unwrap().update_time;
    engine.apply_ticker_completion(ticker).await;
    engine.apply_mark_price_completion(mark).await;
    let ticker = engine.state.ticker.read().await;
    assert_eq!(ticker.mark_price, dec!(101));
    assert_eq!(ticker.last_price, dec!(100));
    assert_eq!(ticker.mark_update_time, mark_requested_at);
    assert_eq!(ticker.update_time, ticker_requested_at);
    assert!(ticker.prices_are_fresh(Utc::now()));
    assert_eq!(engine.state.position.read().await.unrealized_pnl, dec!(101));
    server.abort();
}

#[tokio::test]
async fn market_completions_reject_stale_requests_and_retired_or_changed_scopes() {
    for change in [
        "stale",
        "generation",
        "symbol",
        "mode",
        "account",
        "shutdown",
    ] {
        let config = AppConfig::default();
        let mut engine = pair_recovery_engine(
            &config,
            Arc::new(Database::open(":memory:").unwrap()),
            "http://127.0.0.1:1".into(),
        )
        .await;
        *engine.state.status.write().await = BotStatus::Paused;
        let scope = engine.market_request_scope(&config.exchange.symbol).await;
        let previous = engine.state.ticker.read().await.clone();
        let mut requested_at = Utc::now();
        match change {
            "stale" => requested_at -= chrono::Duration::seconds(16),
            "generation" => engine.retire_market_requests(),
            "symbol" => engine.state.config.write().await.exchange.symbol = "BTCUSDC".into(),
            "mode" => {
                let mut config = engine.state.config.write().await;
                config.exchange.api_key = "different-key".into();
                config.exchange.api_secret = "different-secret".into();
            }
            "account" => {
                let mut changed = config.exchange.clone();
                changed.api_key = "other-account".into();
                changed.api_secret = "other-secret".into();
                engine.client = Arc::new(BinanceFuturesClient::with_base_url(
                    &changed,
                    "http://127.0.0.1:1".into(),
                ));
            }
            "shutdown" => engine.state.request_shutdown(),
            _ => unreachable!(),
        }
        engine
            .apply_mark_price_completion(super::MarkPriceCompletion {
                scope: scope.clone(),
                previous: previous.clone(),
                result: Ok((dec!(70), requested_at)),
            })
            .await;
        engine
            .apply_ticker_completion(super::TickerCompletion {
                scope,
                result: Ok(TickerInfo {
                    symbol: config.exchange.symbol,
                    last_price: dec!(70),
                    update_time: requested_at,
                    ..Default::default()
                }),
            })
            .await;
        let ticker = engine.state.ticker.read().await;
        assert_eq!(ticker.last_price, previous.last_price, "{change}");
        assert_eq!(ticker.mark_price, previous.mark_price, "{change}");
        assert_eq!(ticker.update_time, previous.update_time, "{change}");
        assert_eq!(
            ticker.mark_update_time, previous.mark_update_time,
            "{change}"
        );
    }
}

#[tokio::test]
async fn ticker_fallback_matches_paper_fill_once_when_its_pending_mark_completes() {
    for interrupted_by_pause in [false, true] {
        let config = AppConfig::default();
        let mut engine = pair_recovery_engine(
            &config,
            Arc::new(Database::open(":memory:").unwrap()),
            "http://127.0.0.1:1".into(),
        )
        .await;
        *engine.state.status.write().await = BotStatus::Running;
        *engine.state.position.write().await = PositionInfo::default();
        let now = Utc::now();
        *engine.state.ticker.write().await = TickerInfo {
            symbol: config.exchange.symbol.clone(),
            // A missing mark permits startup fills against the fresh last price.
            // A previously available, stale mark must defer matching instead.
            mark_price: dec!(100),
            mark_update_time: now - chrono::Duration::seconds(16),
            ..Default::default()
        };
        let order = GridOrder {
            client_order_id: "gb_deferred_market_fill".into(),
            order_id: None,
            symbol: config.exchange.symbol.clone(),
            side: OrderSide::Buy,
            price: dec!(99),
            quantity: dec!(1),
            amount_usdc: dec!(99),
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
            grid_level: -1,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(order.client_order_id.clone(), order);
        let scope = engine.market_request_scope(&config.exchange.symbol).await;
        let previous = engine.state.ticker.read().await.clone();
        engine
            .apply_ticker_completion(super::TickerCompletion {
                scope: scope.clone(),
                result: Ok(TickerInfo {
                    symbol: config.exchange.symbol,
                    last_price: dec!(98),
                    update_time: now,
                    ..Default::default()
                }),
            })
            .await;
        assert_eq!(engine.state.stats.read().await.total_trades, 0);
        assert!(engine.pending_paper_ticker.is_some());
        assert!(!engine
            .state
            .ticker
            .read()
            .await
            .prices_are_fresh(Utc::now()));
        if interrupted_by_pause {
            engine.state.pause_trading().await.unwrap();
            engine
                .state
                .resume_trading_in_mode(TradingMode::Paper, engine.state.pause_epoch())
                .await
                .unwrap();
        }
        engine
            .apply_mark_price_completion(super::MarkPriceCompletion {
                scope,
                previous,
                result: Ok((dec!(98), now)),
            })
            .await;
        assert!(engine
            .state
            .ticker
            .read()
            .await
            .prices_are_fresh(Utc::now()));
        let expected_trades = if interrupted_by_pause { 0 } else { 1 };
        assert_eq!(
            engine.state.stats.read().await.total_trades,
            expected_trades
        );
        assert_eq!(
            engine.state.position.read().await.size,
            if interrupted_by_pause {
                dec!(0)
            } else {
                dec!(1)
            }
        );
        assert_eq!(
            engine.state.position.read().await.unrealized_pnl,
            if interrupted_by_pause {
                dec!(0)
            } else {
                dec!(-1)
            }
        );
        assert!(engine.pending_paper_ticker.is_none());
        engine
            .handle_ticker_update(TickerInfo {
                symbol: "SOLUSDC".into(),
                mark_price: dec!(97),
                mark_update_time: Utc::now(),
                ..Default::default()
            })
            .await;
        assert_eq!(
            engine.state.stats.read().await.total_trades,
            expected_trades
        );
    }
}

#[test]
fn grid_client_order_ids_fit_binance_limit() {
    for (side, prefix) in [(OrderSide::Buy, "gb_b_"), (OrderSide::Sell, "gb_s_")] {
        let first = new_grid_client_order_id(side);
        let second = new_grid_client_order_id(side);

        assert!(first.starts_with(prefix));
        assert_eq!(first.len(), 35);
        assert!(first
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_'));
        assert_ne!(first, second);
    }
}

#[tokio::test]
async fn pnl_order_id_cache_survives_failed_user_trades_retry_without_order_lookup() {
    use std::sync::atomic::AtomicBool;
    let order_lookups = Arc::new(AtomicUsize::new(0));
    let fail_trades = Arc::new(AtomicBool::new(true));
    let app = Router::new()
        .route(
            "/fapi/v1/order",
            get({
                let order_lookups = Arc::clone(&order_lookups);
                move || {
                    order_lookups.fetch_add(1, Ordering::SeqCst);
                    async {
                        (
                            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                            Json(serde_json::json!({})),
                        )
                    }
                }
            }),
        )
        .route(
            "/fapi/v1/userTrades",
            get({
                let fail_trades = Arc::clone(&fail_trades);
                move || {
                    let fail = fail_trades.load(Ordering::SeqCst);
                    async move {
                        if fail {
                            (
                                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                                Json(serde_json::json!({"msg": "busy"})),
                            )
                        } else {
                            (
                                axum::http::StatusCode::OK,
                                Json(serde_json::json!([{
                                    "id": 1,
                                    "orderId": 987,
                                    "qty": "1",
                                    "realizedPnl": "1.5",
                                    "commission": "0.02",
                                    "commissionAsset": "USDC",
                                    "maker": true
                                }])),
                            )
                        }
                    }
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let mut config = AppConfig::default();
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
    let db = Arc::new(Database::open(":memory:").unwrap());
    let mut engine = pair_recovery_engine(&config, db.clone(), url).await;
    let mut filled = GridOrder {
        client_order_id: "gb_s_cached_oid".into(),
        order_id: Some(987),
        symbol: "SOLUSDC".into(),
        side: OrderSide::Sell,
        price: dec!(101),
        quantity: dec!(1),
        amount_usdc: dec!(101),
        status: OrderStatus::Filled,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: 1,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: OrderPurpose::Grid,
        merge_sources: Vec::new(),
    };
    assert!(engine.commit_order_fill(&mut filled).await);
    assert_eq!(engine.pnl_order_ids.get("gb_s_cached_oid"), Some(&987));

    engine.reconcile_unverified_trades().await;
    engine.pnl_retry_at = engine.pnl_reconcile_task.take().unwrap().await.unwrap();
    assert!(engine.pnl_retry_at.is_some());
    assert_eq!(engine.pnl_order_ids.get("gb_s_cached_oid"), Some(&987));
    assert_eq!(order_lookups.load(Ordering::SeqCst), 0);

    fail_trades.store(false, Ordering::SeqCst);
    engine.pnl_retry_at = None;
    engine.reconcile_unverified_trades().await;
    assert!(engine
        .pnl_reconcile_task
        .take()
        .unwrap()
        .await
        .unwrap()
        .is_none());
    assert_eq!(order_lookups.load(Ordering::SeqCst), 0);
    assert_eq!(engine.state.stats.read().await.pending_pnl_trades, 0);

    engine.reconcile_unverified_trades().await;
    assert!(engine.pnl_order_ids.is_empty());
    server.abort();
}

#[tokio::test]
async fn config_scope_switch_resets_blocked_flags_and_paper_window_cache() {
    let mut paper = AppConfig::default();
    paper.grid.grid_interval = dec!(1);
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (url, server) = pair_exchange_server(PairExchange::default()).await;
    let mut engine = pair_recovery_engine(&paper, db, url).await;
    engine.state.position.write().await.size = dec!(10);
    engine.maintain_grid_window().await;
    assert!(engine.paper_window_cache.is_some());
    engine.reconciliation_blocked = true;
    engine.market_data_blocked = true;

    let mut switched = paper.clone();
    switched.exchange.api_key = "live-key".into();
    switched.exchange.api_secret = "live-secret".into();
    engine.apply_config(switched).await.unwrap();
    assert!(!engine.reconciliation_blocked);
    assert!(!engine.market_data_blocked);
    assert!(engine.paper_window_cache.is_none());
    server.abort();
}

#[tokio::test]
async fn live_sync_freshness_scales_with_configured_sync_interval() {
    assert_eq!(live_sync_freshness(1), std::time::Duration::from_secs(10));
    assert_eq!(live_sync_freshness(5), std::time::Duration::from_secs(14));
    assert_eq!(live_sync_freshness(20), std::time::Duration::from_secs(44));

    let mut config = AppConfig::default();
    config.exchange.sync_interval_secs = 20;
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let mut engine = GridTradingEngine::new(state, client, action_rx, ticker_rx);
    let recent = std::time::Instant::now() - std::time::Duration::from_secs(25);
    engine.last_account_sync = Some(recent);
    engine.last_orders_sync = Some(recent);
    assert!(engine.sync_timestamps_fresh());

    let expired = std::time::Instant::now() - std::time::Duration::from_secs(45);
    engine.last_account_sync = Some(expired);
    assert!(!engine.sync_timestamps_fresh());
}

#[tokio::test]
async fn paper_single_buy_window_places_closing_sell_immediately_on_fill_tick() {
    let mut config = AppConfig::default();
    config.grid.grid_interval = dec!(1);
    config.grid.order_amount_usdc = dec!(100);
    config.grid.buy_window = 1;
    config.grid.sell_window = 1;
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, action_tx);
    *state.status.write().await = BotStatus::Running;
    {
        let mut ticker = state.ticker.write().await;
        ticker.symbol = config.exchange.symbol.clone();
        ticker.last_price = dec!(100.5);
        ticker.mark_price = dec!(100.5);
        ticker.update_time = Utc::now();
        ticker.mark_update_time = Utc::now();
    }
    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);

    engine.maintain_grid_window().await;
    {
        let active = state.active_orders.read().await;
        assert_eq!(active.len(), 1);
        let only = active.values().next().unwrap();
        assert_eq!(only.side, OrderSide::Buy);
        assert_eq!(only.price, dec!(100));
    }

    // Price crosses the single buy order, temporarily emptying active_orders.
    // That same tick must immediately place the closing sell order at 101.
    let now = Utc::now();
    engine
        .handle_ticker_update(TickerInfo {
            symbol: config.exchange.symbol,
            last_price: dec!(100),
            mark_price: dec!(100),
            update_time: now,
            mark_update_time: now,
            ..Default::default()
        })
        .await;

    assert!(state.position.read().await.size > Decimal::ZERO);
    let active = state.active_orders.read().await;
    assert!(
        active
            .values()
            .any(|o| o.side == OrderSide::Sell && o.price == dec!(101)),
        "expected immediate closing sell at 101, got: {:?}",
        active.values().collect::<Vec<_>>()
    );
}

#[derive(Clone, Default)]
struct DualAccountExchange {
    orders_a: Arc<Mutex<HashMap<String, BinanceOrderResponse>>>,
    orders_b: Arc<Mutex<HashMap<String, BinanceOrderResponse>>>,
    posts_a: Arc<Mutex<Vec<String>>>,
    posts_b: Arc<Mutex<Vec<String>>>,
    b_position_calls: Arc<AtomicUsize>,
    fail_b_second_position: Arc<AtomicUsize>,
}

async fn dual_account_server(fake: DualAccountExchange) -> (String, tokio::task::JoinHandle<()>) {
    let api_key = |headers: &axum::http::HeaderMap| {
        headers
            .get("x-mbx-apikey")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_owned()
    };
    let app = Router::new()
        .route("/fapi/v1/time", get(|| async {
            Json(serde_json::json!({"serverTime": Utc::now().timestamp_millis()}))
        }))
        .route("/fapi/v1/exchangeInfo", get(|| async {
            Json(serde_json::json!({"symbols":[{"symbol":"SOLUSDC","status":"TRADING","contractType":"PERPETUAL","filters":[{"filterType":"PRICE_FILTER","minPrice":"0.01","maxPrice":"100000","tickSize":"0.01"},{"filterType":"LOT_SIZE","minQty":"0.01","maxQty":"100000","stepSize":"0.01"},{"filterType":"MIN_NOTIONAL","notional":"5"}]}]}))
        }))
        .route("/fapi/v1/ticker/price", get(|| async {
            Json(serde_json::json!({"symbol":"SOLUSDC","price":"100.5"}))
        }))
        .route("/fapi/v1/premiumIndex", get(|| async {
            Json(serde_json::json!({"symbol":"SOLUSDC","markPrice":"100.5"}))
        }))
        .route("/fapi/v1/ticker/24hr", get(|| async {
            Json(serde_json::json!({"lastPrice":"100.5","priceChange":"0","priceChangePercent":"0","highPrice":"101","lowPrice":"100","volume":"10"}))
        }))
        .route("/fapi/v2/positionRisk", get(move |headers: axum::http::HeaderMap, State(fake): State<DualAccountExchange>| async move {
            if api_key(&headers) == "account-b-key" {
                let call = fake.b_position_calls.fetch_add(1, Ordering::SeqCst);
                if call >= 1 && consume_failure(&fake.fail_b_second_position) {
                    return (axum::http::StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({})));
                }
                (axum::http::StatusCode::OK, Json(serde_json::json!([{"symbol":"SOLUSDC","positionAmt":"0","entryPrice":"0","markPrice":"100.5","unRealizedProfit":"0","liquidationPrice":"0","leverage":"5"}])))
            } else {
                (axum::http::StatusCode::OK, Json(serde_json::json!([{"symbol":"SOLUSDC","positionAmt":"3","entryPrice":"95","markPrice":"100.5","unRealizedProfit":"16.5","liquidationPrice":"50","leverage":"5"}])))
            }
        }))
        .route("/fapi/v2/account", get(move |headers: axum::http::HeaderMap| async move {
            let bal = if api_key(&headers) == "account-b-key" { "250" } else { "5000" };
            Json(serde_json::json!({"assets":[{"asset":"USDC","walletBalance":bal,"marginBalance":bal,"availableBalance":bal,"unrealizedProfit":"0"}]}))
        }))
        .route("/fapi/v1/openOrders", get(move |headers: axum::http::HeaderMap, State(fake): State<DualAccountExchange>| async move {
            let book = if api_key(&headers) == "account-b-key" { &fake.orders_b } else { &fake.orders_a };
            let open: Vec<_> = book.lock().unwrap().values().filter(|o| o.status == "NEW").cloned().collect();
            Json(serde_json::to_value(open).unwrap())
        }))
        .route("/fapi/v1/order", get(move |headers: axum::http::HeaderMap, State(fake): State<DualAccountExchange>, Query(query): Query<HashMap<String, String>>| async move {
            let book = if api_key(&headers) == "account-b-key" { &fake.orders_b } else { &fake.orders_a };
            let orders = book.lock().unwrap();
            let found = if let Some(cid) = query.get("origClientOrderId") {
                orders.get(cid).cloned()
            } else {
                query.get("orderId").and_then(|id| id.parse::<i64>().ok()).and_then(|id| orders.values().find(|o| o.order_id == id).cloned())
            };
            match found {
                Some(o) => (axum::http::StatusCode::OK, Json(serde_json::to_value(o).unwrap())),
                None => (axum::http::StatusCode::BAD_REQUEST, Json(serde_json::json!({"code": -2013, "msg": "Order does not exist"}))),
            }
        }).delete(move |headers: axum::http::HeaderMap, State(fake): State<DualAccountExchange>, Query(query): Query<HashMap<String, String>>| async move {
            let book = if api_key(&headers) == "account-b-key" { &fake.orders_b } else { &fake.orders_a };
            let mut orders = book.lock().unwrap();
            let id: i64 = query["orderId"].parse().unwrap();
            let order = orders.values_mut().find(|o| o.order_id == id).unwrap();
            order.status = "CANCELED".into();
            Json(serde_json::to_value(order.clone()).unwrap())
        }).post(move |headers: axum::http::HeaderMap, State(fake): State<DualAccountExchange>, Query(query): Query<HashMap<String, String>>| async move {
            let is_b = api_key(&headers) == "account-b-key";
            let (book, posts) = if is_b { (&fake.orders_b, &fake.posts_b) } else { (&fake.orders_a, &fake.posts_a) };
            let cid = query["newClientOrderId"].clone();
            posts.lock().unwrap().push(cid.clone());
            let mut orders = book.lock().unwrap();
            let next_id = orders.values().map(|o| o.order_id).max().unwrap_or(100) + 1;
            let order = BinanceOrderResponse {
                order_id: next_id,
                client_order_id: cid.clone(),
                symbol: query["symbol"].clone(),
                status: "NEW".into(),
                price: query["price"].parse().unwrap(),
                avg_price: None,
                orig_qty: query["quantity"].parse().unwrap(),
                executed_qty: dec!(0),
                side: query["side"].clone(),
                order_type: "LIMIT".into(),
                time_in_force: "GTX".into(),
                update_time: None,
            };
            orders.insert(cid, order.clone());
            Json(serde_json::to_value(order).unwrap())
        }))
        .with_state(fake);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (url, server)
}

#[tokio::test]
async fn live_credential_rotation_cancels_old_account_pauses_and_clears_cached_state() {
    let fake = DualAccountExchange::default();
    fake.orders_b.lock().unwrap().insert(
        "gb_b_preexisting_b".into(),
        BinanceOrderResponse {
            order_id: 900,
            client_order_id: "gb_b_preexisting_b".into(),
            symbol: "SOLUSDC".into(),
            status: "NEW".into(),
            price: dec!(99),
            avg_price: None,
            orig_qty: dec!(1),
            executed_qty: dec!(0),
            side: "BUY".into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    let (url, server) = dual_account_server(fake.clone()).await;
    let mut config_a = AppConfig::default();
    config_a.exchange.api_key = "account-a-key".into();
    config_a.exchange.api_secret = "live-secret".into();
    config_a.grid.grid_interval = dec!(1);
    config_a.grid.buy_window = 1;
    config_a.grid.sell_window = 1;

    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_runtime_status("SOLUSDC", TradingMode::Live, BotStatus::Running, true)
        .unwrap();
    let (action_tx, action_rx) = mpsc::channel(4);
    let (_ticker_tx, ticker_rx) = broadcast::channel(4);
    let state = AppState::new(config_a.clone(), db, action_tx);
    let client = Arc::new(BinanceFuturesClient::with_base_url(
        &config_a.exchange,
        url.clone(),
    ));
    let mut engine = GridTradingEngine::new(state.clone(), client, action_rx, ticker_rx);
    engine.initialize().await.unwrap();
    engine.maintain_grid_window().await;
    assert_eq!(*state.status.read().await, BotStatus::Running);
    assert_eq!(state.position.read().await.size, dec!(3));
    assert_eq!(state.account.read().await.total_wallet_balance, dec!(5000));
    assert!(!fake.posts_a.lock().unwrap().is_empty());

    let mut config_b = config_a.clone();
    config_b.exchange.api_key = "account-b-key".into();
    config_b.exchange.api_secret = "test-secret".into();
    engine.apply_config(config_b.clone()).await.unwrap();

    assert!(fake
        .orders_a
        .lock()
        .unwrap()
        .values()
        .all(|o| o.status == "CANCELED"));
    assert_eq!(
        fake.orders_b.lock().unwrap()["gb_b_preexisting_b"].status,
        "CANCELED"
    );
    assert_eq!(*state.status.read().await, BotStatus::Paused);
    assert_eq!(state.position.read().await.size, Decimal::ZERO);
    assert_eq!(state.account.read().await.total_wallet_balance, dec!(250));
    assert!(fake.posts_b.lock().unwrap().is_empty());

    engine.maintain_grid_window().await;
    assert!(fake.posts_b.lock().unwrap().is_empty());

    let (reply, rx) = tokio::sync::oneshot::channel();
    engine
        .handle_control_action(BotControlAction::Resume {
            expected_mode: TradingMode::Live,
            expected_epoch: state.pause_epoch(),
            reply,
        })
        .await;
    rx.await.unwrap().unwrap();
    assert_eq!(*state.status.read().await, BotStatus::Running);
    assert!(!fake.posts_b.lock().unwrap().is_empty());

    // Rotating back to Account A and then to Account B with a failing post-commit sync
    // must still wipe Account A's cached position and balance rather than leaking them.
    engine.apply_config(config_a).await.unwrap();
    assert_eq!(state.position.read().await.size, dec!(3));
    fake.b_position_calls.store(0, Ordering::SeqCst);
    fake.fail_b_second_position.store(1, Ordering::SeqCst);
    assert!(engine.apply_config(config_b).await.is_err());
    assert_eq!(*state.status.read().await, BotStatus::Paused);
    assert_eq!(state.position.read().await.size, Decimal::ZERO);
    assert_eq!(state.account.read().await.total_wallet_balance, dec!(250));
    server.abort();
}
