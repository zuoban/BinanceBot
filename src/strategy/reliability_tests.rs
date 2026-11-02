use super::*;
use crate::{config::AppConfig, db::Database, lifecycle::stop_engine};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use rust_decimal_macros::dec;
use serde_json::json;
use std::sync::{
    atomic::{AtomicBool, AtomicU8, Ordering},
    Mutex,
};

#[derive(Clone, Default)]
struct Exchange {
    orders: Arc<Mutex<HashMap<String, BinanceOrderResponse>>>,
    posts: Arc<Mutex<Vec<String>>>,
    // 0: accepted, 1: accepted but backend timeout, 2: timeout without acceptance.
    submission: Arc<AtomicU8>,
    lookup_unavailable: Arc<AtomicBool>,
    cancel_fails: Arc<AtomicBool>,
    fill_on_cancel: Arc<AtomicBool>,
    lose_cancel_response: Arc<AtomicBool>,
    hide_open_orders: Arc<AtomicBool>,
    rules_unavailable: Arc<AtomicBool>,
    position_unavailable: Arc<AtomicBool>,
    position_flat: Arc<AtomicBool>,
    fill_on_submit: Arc<AtomicU8>,
    position_large: Arc<AtomicBool>,
    block_submit: Arc<AtomicBool>,
    submit_started: Arc<tokio::sync::Notify>,
    release_submit: Arc<tokio::sync::Notify>,
    lookup_filled_zero: Arc<AtomicBool>,
}

async fn exchange_server(fake: Exchange) -> (String, JoinHandle<()>) {
    let app = Router::new()
        .route("/fapi/v1/exchangeInfo", get(|State(fake): State<Exchange>| async move {
            if fake.rules_unavailable.load(Ordering::SeqCst) {
                return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code": -1007, "msg": "unavailable"})));
            }
            (StatusCode::OK, Json(json!({"symbols":[{"symbol":"SOLUSDC","status":"TRADING","pricePrecision":5,"quantityPrecision":2,"filters":[
                {"filterType":"PRICE_FILTER","minPrice":"0.00001","maxPrice":"100000","tickSize":"0.00001"},
                {"filterType":"LOT_SIZE","minQty":"0.01","maxQty":"100000","stepSize":"0.01"},
                {"filterType":"MIN_NOTIONAL","notional":"5"}
            ]}]})))
        }))
        .route("/fapi/v1/openOrders", get(|State(fake): State<Exchange>| async move {
            if fake.hide_open_orders.load(Ordering::SeqCst) {
                return Json(Vec::<BinanceOrderResponse>::new());
            }
            Json(fake.orders.lock().unwrap().values()
                .filter(|order| matches!(order.status.as_str(), "NEW" | "PARTIALLY_FILLED"))
                .cloned().collect::<Vec<_>>())
        }))
        .route("/fapi/v1/order", get(|State(fake): State<Exchange>, Query(query): Query<HashMap<String, String>>| async move {
            if fake.lookup_unavailable.load(Ordering::SeqCst) {
                return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code": -1007, "msg": "backend timeout"})));
            }
            let found = fake.orders.lock().unwrap().values().find(|order| {
                query.get("origClientOrderId") == Some(&order.client_order_id)
                    || query.get("orderId") == Some(&order.order_id.to_string())
            }).cloned();
            match found {
                Some(mut order) => {
                    if fake.lookup_filled_zero.load(Ordering::SeqCst) {
                        order.status = "FILLED".into();
                        order.executed_qty = Decimal::ZERO;
                    }
                    (StatusCode::OK, Json(serde_json::to_value(order).unwrap()))
                },
                None => (StatusCode::BAD_REQUEST, Json(json!({"code": -2013, "msg": "Order does not exist"}))),
            }
        }).post(|State(fake): State<Exchange>, Query(query): Query<HashMap<String, String>>| async move {
            let id = query["newClientOrderId"].clone();
            let order_id = {
                let mut posts = fake.posts.lock().unwrap();
                posts.push(id.clone());
                posts.len() as i64
            };
            let mut order = BinanceOrderResponse {
                order_id,
                client_order_id: id.clone(), symbol: query["symbol"].clone(), status: "NEW".into(),
                price: query["price"].parse().unwrap(), avg_price: None,
                orig_qty: query["quantity"].parse().unwrap(), executed_qty: dec!(0),
                side: query["side"].clone(), order_type: "LIMIT".into(),
                time_in_force: query["timeInForce"].clone(), update_time: None,
            };
            match fake.fill_on_submit.load(Ordering::SeqCst) {
                1 => { order.status = "PARTIALLY_FILLED".into(); order.executed_qty = order.orig_qty / dec!(2); }
                2 => { order.status = "FILLED".into(); order.executed_qty = order.orig_qty; order.avg_price = Some(order.price); }
                3 => { order.status = "CANCELED".into(); }
                4 => { order.status = "EXPIRED".into(); }
                5 => { order.status = "FILLED".into(); }
                _ => {}
            }
            let behavior = fake.submission.load(Ordering::SeqCst);
            if behavior != 2 { fake.orders.lock().unwrap().insert(id, order.clone()); }
            if fake.block_submit.load(Ordering::SeqCst) {
                fake.submit_started.notify_one();
                fake.release_submit.notified().await;
            }
            if behavior != 0 {
                (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code": -1007, "msg": "Timeout waiting for response from backend server."})))
            } else {
                (StatusCode::OK, Json(serde_json::to_value(order).unwrap()))
            }
        }).delete(|State(fake): State<Exchange>, Query(query): Query<HashMap<String, String>>| async move {
            if fake.cancel_fails.load(Ordering::SeqCst) {
                return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code": -1007, "msg": "unknown"})));
            }
            let mut orders = fake.orders.lock().unwrap();
            let found = orders.values_mut().find(|order| {
                query.get("origClientOrderId") == Some(&order.client_order_id)
                    || query.get("orderId") == Some(&order.order_id.to_string())
            }).unwrap();
            if found.status == "FILLED" && found.executed_qty.is_zero() {
                return (StatusCode::BAD_REQUEST, Json(json!({"code": -2011, "msg": "already filled"})));
            }
            if fake.fill_on_cancel.load(Ordering::SeqCst) {
                found.status = "FILLED".into(); found.executed_qty = found.orig_qty;
                found.avg_price = Some(found.price);
            } else { found.status = "CANCELED".into(); }
            found.update_time = Some(Utc::now().timestamp_millis());
            if fake.lose_cancel_response.load(Ordering::SeqCst) {
                return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code": -1007, "msg": "cancel response lost"})));
            }
            (StatusCode::OK, Json(serde_json::to_value(found).unwrap()))
        }))
        .route("/fapi/v2/positionRisk", get(|State(fake): State<Exchange>| async move {
            if fake.position_unavailable.load(Ordering::SeqCst) {
                return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"msg":"unavailable"})));
            }
            let amount = if fake.position_flat.load(Ordering::SeqCst) { "0" } else if fake.position_large.load(Ordering::SeqCst) { "2" } else { "1" };
            (StatusCode::OK, Json(json!([{"symbol":"SOLUSDC", "positionAmt":amount, "entryPrice":"100", "markPrice":"100.5", "unRealizedProfit":"0.5", "liquidationPrice":"0", "leverage":"1"}])))
        }))
        .route("/fapi/v2/account", get(|| async {
            Json(json!({"totalWalletBalance":"10000", "totalMarginBalance":"10000", "totalUnrealizedProfit":"0", "availableBalance":"10000", "assets":[{"asset":"USDC", "walletBalance":"10000", "marginBalance":"10000", "availableBalance":"10000", "unrealizedProfit":"0"}]}))
        }))
        .route("/fapi/v1/userTrades", get(|State(fake): State<Exchange>, Query(query): Query<HashMap<String, String>>| async move {
            let orders = fake.orders.lock().unwrap();
            let order = orders.values().find(|order| query.get("orderId") == Some(&order.order_id.to_string()));
            Json(order.map(|order| vec![json!({"id":1, "orderId":order.order_id, "qty":order.executed_qty, "realizedPnl":"0", "commission":"0.01", "commissionAsset":"USDC", "maker":true})]).unwrap_or_default())
        }))
        // A market-data outage must not be masked by successful account APIs.
        .route("/fapi/v1/premiumIndex", get(|| async { (StatusCode::SERVICE_UNAVAILABLE, Json(json!({}))) }))
        .route("/fapi/v1/ticker/24hr", get(|| async { (StatusCode::SERVICE_UNAVAILABLE, Json(json!({}))) }))
        .route("/fapi/v1/ticker/price", get(|| async { (StatusCode::SERVICE_UNAVAILABLE, Json(json!({}))) }))
        .with_state(fake);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    (
        url,
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() }),
    )
}

async fn engine(db: Arc<Database>, url: &str) -> GridTradingEngine {
    let mut config = AppConfig::default();
    config.exchange.dry_run = false;
    config.grid.grid_interval = dec!(1);
    config.grid.buy_window = 1;
    config.grid.sell_window = 1;
    let (tx, rx) = mpsc::channel(8);
    let (_, ticks) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db.clone(), tx);
    *state.ticker.write().await = TickerInfo {
        symbol: "SOLUSDC".into(),
        last_price: dec!(100.5),
        mark_price: dec!(100.5),
        update_time: Utc::now(),
        mark_update_time: Utc::now(),
        ..Default::default()
    };
    state.position.write().await.size = dec!(1);
    for order in db.load_managed_orders("SOLUSDC").unwrap() {
        state
            .active_orders
            .write()
            .await
            .insert(order.client_order_id.clone(), order);
    }
    let mut engine = GridTradingEngine::new(
        state,
        Arc::new(BinanceFuturesClient::with_base_url(
            &config.exchange,
            url.into(),
        )),
        rx,
        ticks,
    );
    engine.last_account_sync = Some(Instant::now());
    engine.last_orders_sync = Some(Instant::now());
    engine
}

#[tokio::test]
async fn position_sync_marks_data_valid_only_after_success() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let mut engine = engine(Arc::new(Database::open(":memory:").unwrap()), &url).await;
    assert!(engine.state.position.read().await.update_time.is_none());
    assert!(engine.sync_account_and_position().await);
    assert!(engine.state.position.read().await.update_time.is_some());
    fake.position_unavailable.store(true, Ordering::SeqCst);
    assert!(!engine.sync_account_and_position().await);
    assert!(engine.state.position.read().await.update_time.is_none());
    server.abort();
}

fn database() -> Arc<Database> {
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    db
}

async fn resume(engine: &mut GridTradingEngine, expected_mode: TradingMode) -> Result<(), String> {
    let (reply, response) = tokio::sync::oneshot::channel();
    engine
        .handle_control_action(BotControlAction::Resume {
            expected_mode,
            expected_epoch: engine.state.pause_epoch(),
            reply,
        })
        .await;
    response.await.unwrap()
}

#[tokio::test]
async fn persisted_pause_actions_preserve_queued_resume_epoch() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    for action in [
        BotControlAction::PauseAlreadyPersisted,
        BotControlAction::CancelAllAlreadyPersisted,
    ] {
        let db = database();
        let mut engine = engine(db.clone(), &url).await;
        fake.orders.lock().unwrap().insert(
            "gb_b_test".into(),
            BinanceOrderResponse {
                order_id: 42,
                client_order_id: "gb_b_test".into(),
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
        engine.state.pause_trading().await.unwrap();
        let epoch = engine.state.pause_epoch();
        let (reply, response) = tokio::sync::oneshot::channel();
        let pending = BotControlAction::Resume {
            expected_mode: TradingMode::Live,
            expected_epoch: epoch,
            reply,
        };

        engine.handle_control_action(action).await;
        assert_eq!(engine.state.pause_epoch(), epoch);
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
        assert_eq!(fake.orders.lock().unwrap()["gb_b_test"].status, "CANCELED");
        engine.handle_control_action(pending).await;
        assert!(response.await.unwrap().is_ok());
        assert_eq!(*engine.state.status.read().await, BotStatus::Running);
    }
    server.abort();
}

#[tokio::test]
async fn internal_pause_actions_still_persist_pause() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake).await;
    for action in [BotControlAction::Pause, BotControlAction::CancelAll] {
        let db = database();
        let mut engine = engine(db.clone(), &url).await;
        let epoch = engine.state.pause_epoch();
        engine.handle_control_action(action).await;
        assert!(engine.state.pause_epoch() > epoch);
        assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
    }
    server.abort();
}

#[tokio::test]
async fn resume_rejects_changed_and_unknown_modes_before_exchange_work() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let mut engine = engine(database(), &url).await;
    engine.state.pause_trading().await.unwrap();
    let mut paper = engine.state.config.read().await.clone();
    paper.exchange.dry_run = true;
    engine
        .state
        .save_runtime_config(paper.clone())
        .await
        .unwrap();
    let (reply, response) = tokio::sync::oneshot::channel();
    let pending = BotControlAction::Resume {
        expected_mode: TradingMode::Paper,
        expected_epoch: engine.state.pause_epoch(),
        reply,
    };
    paper.exchange.dry_run = false;
    paper.exchange.is_testnet = false;
    engine.state.save_runtime_config(paper).await.unwrap();
    engine.handle_control_action(pending).await;
    assert!(response.await.unwrap().is_err());
    assert!(resume(&mut engine, TradingMode::Testnet).await.is_err());
    assert!(resume(&mut engine, TradingMode::Unknown).await.is_err());
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert!(!engine.exchange_rules_loaded);
    assert!(fake.posts.lock().unwrap().is_empty());
    *engine.state.ticker.write().await = TickerInfo::default();
    assert!(resume(&mut engine, TradingMode::Live).await.is_ok());
    assert_eq!(*engine.state.status.read().await, BotStatus::Running);
    server.abort();
}

#[tokio::test]
async fn queued_resume_rejects_a_later_pause() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake).await;
    let db = database();
    let mut engine = engine(db.clone(), &url).await;
    engine.state.pause_trading().await.unwrap();
    let (reply, response) = tokio::sync::oneshot::channel();
    let pending = BotControlAction::Resume {
        expected_mode: TradingMode::Live,
        expected_epoch: engine.state.pause_epoch(),
        reply,
    };
    engine.state.pause_trading().await.unwrap();
    engine.handle_control_action(pending).await;
    assert!(response.await.unwrap().is_err());
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
    server.abort();
}

#[tokio::test]
async fn resume_reloads_rules_after_initialization_failure_and_rejects_invalid_spacing() {
    let fake = Exchange::default();
    fake.rules_unavailable.store(true, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let mut engine = engine(database(), &url).await;
    engine.state.config.write().await.grid.grid_interval = dec!(0.00005);
    assert!(engine.initialize().await.is_err());
    engine.state.pause_trading().await.unwrap();

    assert!(resume(&mut engine, TradingMode::Live).await.is_err());
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert!(!engine.exchange_rules_loaded);
    assert_eq!(engine.state.rules.read().await.tick_size, dec!(0.01));
    assert!(fake.posts.lock().unwrap().is_empty());

    fake.rules_unavailable.store(false, Ordering::SeqCst);
    engine.state.config.write().await.grid.grid_interval = dec!(0.000015);
    assert!(resume(&mut engine, TradingMode::Live).await.is_err());
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert_eq!(engine.state.rules.read().await.tick_size, dec!(0.00001));
    assert!(fake.posts.lock().unwrap().is_empty());

    engine.state.config.write().await.grid.grid_interval = dec!(0.00005);
    assert!(resume(&mut engine, TradingMode::Live).await.is_ok());
    assert_eq!(*engine.state.status.read().await, BotStatus::Running);
    server.abort();
}

#[tokio::test]
async fn paper_resume_retries_rules_after_initialization_failure() {
    let fake = Exchange::default();
    fake.rules_unavailable.store(true, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let mut engine = engine(database(), &url).await;
    {
        let mut config = engine.state.config.write().await;
        config.exchange.dry_run = true;
        config.grid.grid_interval = dec!(0.00005);
    }
    assert!(engine.initialize().await.is_err());
    engine.state.pause_trading().await.unwrap();
    *engine.state.ticker.write().await = TickerInfo::default();

    assert!(resume(&mut engine, TradingMode::Paper).await.is_err());
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert!(!engine.exchange_rules_loaded);

    fake.rules_unavailable.store(false, Ordering::SeqCst);
    assert!(resume(&mut engine, TradingMode::Paper).await.is_ok());
    assert_eq!(*engine.state.status.read().await, BotStatus::Running);
    assert!(engine.exchange_rules_loaded);
    assert_eq!(engine.state.rules.read().await.tick_size, dec!(0.00001));
    server.abort();
}

#[tokio::test]
async fn paper_resume_can_use_default_rules_when_exchange_is_unavailable() {
    let fake = Exchange::default();
    fake.rules_unavailable.store(true, Ordering::SeqCst);
    let (url, server) = exchange_server(fake).await;
    let mut engine = engine(database(), &url).await;
    engine.state.config.write().await.exchange.dry_run = true;
    engine.state.pause_trading().await.unwrap();
    *engine.state.ticker.write().await = TickerInfo::default();

    assert!(resume(&mut engine, TradingMode::Paper).await.is_ok());
    assert_eq!(*engine.state.status.read().await, BotStatus::Running);
    assert!(!engine.exchange_rules_loaded);
    assert_eq!(engine.state.rules.read().await.tick_size, dec!(0.01));
    server.abort();
}

async fn submit(engine: &mut GridTradingEngine) -> bool {
    engine
        .place_grid_order(
            OrderSide::Buy,
            dec!(100),
            "gb_b_reliability".into(),
            -1,
            None,
        )
        .await
}

#[tokio::test]
async fn immediate_partial_submission_refreshes_risk_and_retains_order_on_failure() {
    let fake = Exchange::default();
    fake.fill_on_submit.store(1, Ordering::SeqCst);
    fake.position_unavailable.store(true, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut engine = engine(db.clone(), &url).await;
    assert!(!submit(&mut engine).await);
    let response = fake.orders.lock().unwrap()["gb_b_reliability"].clone();
    assert_eq!(
        engine.state.active_orders.read().await["gb_b_reliability"].quantity,
        response.orig_qty
    );
    assert_eq!(
        db.load_managed_orders("SOLUSDC").unwrap()[0].quantity,
        response.orig_qty
    );
    assert!(engine.last_account_sync.is_none());
    assert!(engine.reconciliation_blocked);

    fake.position_unavailable.store(false, Ordering::SeqCst);
    assert!(engine.sync_live_orders().await);
    assert_eq!(
        engine.state.active_orders.read().await["gb_b_reliability"].quantity,
        response.orig_qty - response.executed_qty
    );
    assert_eq!(engine.state.position.read().await.size, dec!(1));
    server.abort();
}

#[tokio::test]
async fn immediate_partial_submission_updates_position_before_publishing_remainder() {
    let fake = Exchange::default();
    fake.fill_on_submit.store(1, Ordering::SeqCst);
    fake.position_flat.store(true, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut engine = engine(db.clone(), &url).await;
    engine
        .position_cache
        .get(&engine.client, "SOLUSDC")
        .await
        .unwrap();
    engine.account_cache.get(&engine.client).await.unwrap();
    engine.state.position.write().await.size = dec!(0);
    fake.position_flat.store(false, Ordering::SeqCst);
    assert!(submit(&mut engine).await);
    let response = fake.orders.lock().unwrap()["gb_b_reliability"].clone();
    let active = engine.state.active_orders.read().await;
    assert_eq!(
        active["gb_b_reliability"].status,
        OrderStatus::PartiallyFilled
    );
    assert_eq!(
        active["gb_b_reliability"].quantity,
        response.orig_qty - response.executed_qty
    );
    assert_eq!(engine.state.position.read().await.size, dec!(1));
    assert_eq!(
        db.load_managed_orders("SOLUSDC").unwrap()[0].quantity,
        response.orig_qty - response.executed_qty
    );
    server.abort();
}

#[tokio::test]
async fn immediate_terminal_submission_records_fill_once() {
    let fake = Exchange::default();
    fake.fill_on_submit.store(2, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut engine = engine(db.clone(), &url).await;
    assert!(submit(&mut engine).await);
    assert!(engine.state.active_orders.read().await.is_empty());
    assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
    assert!(engine.sync_live_orders().await);
    assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
    server.abort();
}

#[tokio::test]
async fn immediate_partial_and_full_fills_pause_when_position_reaches_limit() {
    for response in [1, 2] {
        let fake = Exchange::default();
        fake.fill_on_submit.store(response, Ordering::SeqCst);
        fake.position_large.store(true, Ordering::SeqCst);
        let (url, server) = exchange_server(fake).await;
        let db = database();
        let mut engine = engine(db.clone(), &url).await;
        engine.state.position.write().await.size = dec!(0);
        {
            let mut config = engine.state.config.write().await;
            config.grid.max_position_usdc = Some(dec!(150));
            config.grid.pause_on_max_position = true;
        }
        assert!(!submit(&mut engine).await);
        assert_eq!(engine.state.position.read().await.size, dec!(2));
        assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
        server.abort();
    }
}

#[tokio::test]
async fn partial_submission_persistence_failure_blocks_global_risk() {
    let path = std::env::temp_dir().join(format!("partial-submit-{}.db", uuid::Uuid::new_v4()));
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_partial BEFORE INSERT ON managed_orders \
        WHEN json_extract(NEW.order_json, '$.status') = 'PARTIALLYFILLED' \
        BEGIN SELECT RAISE(ABORT, 'partial write failed'); END;",
        )
        .unwrap();
    let fake = Exchange::default();
    fake.fill_on_submit.store(1, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let mut engine = engine(db.clone(), &url).await;
    engine.state.position.write().await.update_time = Some(Utc::now());
    engine
        .state
        .config
        .write()
        .await
        .global_max_position_notional = Some(dec!(10000));
    let global =
        crate::server::state::GlobalPositionLimit::new(Arc::new(tokio::sync::RwLock::new(vec![
            engine.state.clone(),
        ])));
    global.membership_ready.store(true, Ordering::SeqCst);
    engine
        .state
        .global_position_initialized
        .store(true, Ordering::SeqCst);
    engine
        .state
        .global_position_ready
        .store(true, Ordering::SeqCst);
    let _ = engine
        .state
        .global_position_limit
        .set(Arc::downgrade(&global));
    assert!(!submit(&mut engine).await);
    assert!(!engine.state.global_position_ready.load(Ordering::SeqCst));
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    let response = fake.orders.lock().unwrap()["gb_b_reliability"].clone();
    assert_eq!(
        engine.state.active_orders.read().await["gb_b_reliability"].quantity,
        response.orig_qty
    );
    assert_eq!(
        db.load_managed_orders("SOLUSDC").unwrap()[0].quantity,
        response.orig_qty
    );
    server.abort();
    drop(connection);
    drop(engine);
    drop(db);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn zero_fill_terminal_submission_does_not_leave_a_new_order() {
    for response in [3, 4] {
        let fake = Exchange::default();
        fake.fill_on_submit.store(response, Ordering::SeqCst);
        let (url, server) = exchange_server(fake).await;
        let db = database();
        let mut engine = engine(db.clone(), &url).await;
        assert!(!submit(&mut engine).await);
        assert!(engine.state.active_orders.read().await.is_empty());
        assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
        assert!(db.get_recent_trades(10).unwrap().is_empty());
        server.abort();
    }
}

#[tokio::test]
async fn filled_with_zero_quantity_remains_blocked_until_terminal_details_are_consistent() {
    for (confirmed_fill, submission) in [(false, 0), (true, 0), (false, 1), (true, 1)] {
        let fake = Exchange::default();
        fake.fill_on_submit.store(5, Ordering::SeqCst);
        fake.submission.store(submission, Ordering::SeqCst);
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let mut engine = engine(db.clone(), &url).await;
        assert!(!submit(&mut engine).await);
        let order = engine.state.active_orders.read().await["gb_b_reliability"].clone();
        assert!(!engine.cancel_single_order(&order).await);
        assert!(!engine.sync_live_orders().await);
        assert!(engine.reconciliation_blocked);
        assert!(engine.last_account_sync.is_none());
        assert!(engine
            .state
            .active_orders
            .read()
            .await
            .contains_key("gb_b_reliability"));
        assert_eq!(db.load_managed_orders("SOLUSDC").unwrap().len(), 1);
        assert!(db.get_recent_trades(10).unwrap().is_empty());
        {
            let mut orders = fake.orders.lock().unwrap();
            let found = orders.get_mut("gb_b_reliability").unwrap();
            if confirmed_fill {
                found.executed_qty = found.orig_qty;
                found.avg_price = Some(found.price);
            } else {
                found.status = "CANCELED".into();
            }
        }
        assert!(engine.sync_live_orders().await);
        assert!(!engine
            .state
            .active_orders
            .read()
            .await
            .contains_key("gb_b_reliability"));
        assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
        assert_eq!(
            db.get_recent_trades(10).unwrap().len(),
            usize::from(confirmed_fill)
        );
        server.abort();
    }
}

#[tokio::test]
async fn inconsistent_submission_and_timeout_lookup_block_before_active_order_lock() {
    for submission in [0, 1] {
        let fake = Exchange::default();
        fake.fill_on_submit.store(5, Ordering::SeqCst);
        fake.submission.store(submission, Ordering::SeqCst);
        fake.block_submit.store(true, Ordering::SeqCst);
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let mut engine = engine(db, &url).await;
        engine
            .state
            .global_position_ready
            .store(true, Ordering::SeqCst);
        let state = engine.state.clone();
        let task = tokio::spawn(async move { submit(&mut engine).await });
        tokio::time::timeout(Duration::from_secs(2), fake.submit_started.notified())
            .await
            .unwrap();
        let guard = state.active_orders.write().await;
        fake.release_submit.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            while state.global_position_ready.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(guard);
        assert!(!task.await.unwrap());
        assert!(state
            .active_orders
            .read()
            .await
            .contains_key("gb_b_reliability"));
        server.abort();
    }
}

#[tokio::test]
async fn bulk_cancel_conflicting_fill_response_revokes_global_readiness() {
    let fake = Exchange::default();
    fake.cancel_fails.store(true, Ordering::SeqCst);
    fake.lookup_filled_zero.store(true, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut engine = engine(db.clone(), &url).await;
    let order = GridOrder {
        client_order_id: "gb_cancel_unknown".into(),
        order_id: Some(123),
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
        purpose: crate::types::OrderPurpose::Grid,
        merge_sources: Vec::new(),
    };
    fake.orders.lock().unwrap().insert(
        order.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 123,
            client_order_id: order.client_order_id.clone(),
            symbol: order.symbol.clone(),
            status: "NEW".into(),
            price: order.price,
            avg_price: None,
            orig_qty: order.quantity,
            executed_qty: Decimal::ZERO,
            side: "BUY".into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    engine
        .state
        .global_position_ready
        .store(true, Ordering::SeqCst);
    assert!(!engine.cancel_all_orders().await);
    assert!(!engine.state.global_position_ready.load(Ordering::SeqCst));
    assert!(engine
        .state
        .active_orders
        .read()
        .await
        .contains_key("gb_cancel_unknown"));
    assert_eq!(db.load_managed_orders("SOLUSDC").unwrap().len(), 1);
    assert_eq!(
        db.load_managed_orders("SOLUSDC").unwrap()[0].order_id,
        Some(123)
    );
    fake.orders
        .lock()
        .unwrap()
        .get_mut("gb_cancel_unknown")
        .unwrap()
        .status = "FILLED".into();
    assert!(!engine.sync_live_orders().await);
    assert!(engine
        .state
        .active_orders
        .read()
        .await
        .contains_key("gb_cancel_unknown"));
    assert_eq!(
        db.load_managed_orders("SOLUSDC").unwrap()[0].order_id,
        Some(123)
    );
    server.abort();
}

#[tokio::test]
async fn bulk_cancel_identity_conflict_remains_blocked_after_list_disappears_and_restart() {
    let fake = Exchange::default();
    fake.cancel_fails.store(true, Ordering::SeqCst);
    fake.lookup_filled_zero.store(true, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut first = engine(db.clone(), &url).await;
    let original = GridOrder {
        client_order_id: "gb_bulk_identity".into(),
        order_id: Some(123),
        symbol: "SOLUSDC".into(),
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(1),
        amount_usdc: dec!(100),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: -5,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Grid,
        merge_sources: Vec::new(),
    };
    first
        .state
        .active_orders
        .write()
        .await
        .insert(original.client_order_id.clone(), original.clone());
    fake.orders.lock().unwrap().insert(
        original.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 456,
            client_order_id: original.client_order_id.clone(),
            symbol: original.symbol.clone(),
            status: "NEW".into(),
            price: original.price,
            avg_price: None,
            orig_qty: original.quantity,
            executed_qty: Decimal::ZERO,
            side: "BUY".into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    assert!(!first.cancel_all_orders().await);
    let key = (
        first.client.account_identity_key(),
        original.client_order_id.clone(),
    );
    assert!(first.order_identity_conflicts.contains_key(&key));
    assert_eq!(
        db.load_identity_observations(key.0, "SOLUSDC")
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        db.load_managed_orders("SOLUSDC").unwrap()[0].order_id,
        Some(123)
    );
    fake.orders
        .lock()
        .unwrap()
        .get_mut("gb_bulk_identity")
        .unwrap()
        .status = "FILLED".into();
    assert!(!first.sync_live_orders().await);
    assert!(first.order_identity_conflicts.contains_key(&key));
    assert_eq!(
        first.state.active_orders.read().await["gb_bulk_identity"].order_id,
        Some(123)
    );
    drop(first);
    fake.orders
        .lock()
        .unwrap()
        .get_mut("gb_bulk_identity")
        .unwrap()
        .status = "NEW".into();
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    assert_eq!(
        db.load_identity_observations(key.0, "SOLUSDC")
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        db.load_managed_orders("SOLUSDC").unwrap()[0].order_id,
        Some(123)
    );
    assert_eq!(db.load_managed_orders("SOLUSDC").unwrap()[0].grid_level, -5);
    server.abort();
}

#[tokio::test]
async fn bulk_cancel_keeps_provisional_order_when_managed_read_or_write_fails() {
    for corrupt_row in [true, false] {
        let path =
            std::env::temp_dir().join(format!("cancel-provisional-{}.db", uuid::Uuid::new_v4()));
        let db = Arc::new(Database::open(&path).unwrap());
        db.save_bot_status(BotStatus::Running).unwrap();
        let fake = Exchange::default();
        fake.cancel_fails.store(true, Ordering::SeqCst);
        fake.lookup_filled_zero.store(true, Ordering::SeqCst);
        let (url, server) = exchange_server(fake.clone()).await;
        let mut engine = engine(db.clone(), &url).await;
        let order = GridOrder {
            client_order_id: "gb_cancel_provisional".into(),
            order_id: Some(123),
            symbol: "SOLUSDC".into(),
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(1),
            amount_usdc: dec!(100),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: -4,
            paired_client_order_id: Some("gb_parent".into()),
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        let connection = rusqlite::Connection::open(&path).unwrap();
        if corrupt_row {
            db.save_managed_order(&order).unwrap();
            connection.execute("UPDATE managed_orders SET order_json = 'invalid' WHERE client_order_id = 'gb_cancel_provisional'", []).unwrap();
        } else {
            connection.execute_batch("CREATE TRIGGER reject_managed BEFORE INSERT ON managed_orders BEGIN SELECT RAISE(ABORT, 'write failed'); END;").unwrap();
        }
        fake.orders.lock().unwrap().insert(
            order.client_order_id.clone(),
            BinanceOrderResponse {
                order_id: 123,
                client_order_id: order.client_order_id.clone(),
                symbol: order.symbol.clone(),
                status: "NEW".into(),
                price: order.price,
                avg_price: None,
                orig_qty: order.quantity,
                executed_qty: Decimal::ZERO,
                side: "BUY".into(),
                order_type: "LIMIT".into(),
                time_in_force: "GTX".into(),
                update_time: None,
            },
        );
        assert!(!engine.cancel_all_orders().await);
        let key = (
            engine.client.account_identity_key(),
            order.client_order_id.clone(),
        );
        assert!(engine.provisional_order_ids.contains(&key));
        assert_eq!(
            engine.state.active_orders.read().await[&order.client_order_id].order_id,
            Some(123)
        );
        if corrupt_row {
            let raw: String = connection.query_row("SELECT order_json FROM managed_orders WHERE client_order_id = 'gb_cancel_provisional'", [], |row| row.get(0)).unwrap();
            assert_eq!(raw, "invalid");
            connection.execute("UPDATE managed_orders SET order_json = ?1 WHERE client_order_id = 'gb_cancel_provisional'", rusqlite::params![serde_json::to_string(&order).unwrap()]).unwrap();
        } else {
            connection
                .execute_batch("DROP TRIGGER reject_managed")
                .unwrap();
            db.save_managed_order(&order).unwrap();
        }
        assert!(!engine.cancel_all_orders().await);
        assert_eq!(
            engine.state.active_orders.read().await["gb_cancel_provisional"].purpose,
            crate::types::OrderPurpose::Grid
        );
        let saved = db.load_managed_orders("SOLUSDC").unwrap();
        assert_eq!(saved[0].purpose, crate::types::OrderPurpose::Grid);
        assert_eq!(saved[0].grid_level, -4);
        assert_eq!(
            saved[0].paired_client_order_id.as_deref(),
            Some("gb_parent")
        );
        server.abort();
        drop(connection);
        drop(engine);
        drop(db);
        let _ = std::fs::remove_file(path);
    }
}

#[tokio::test]
async fn inconsistent_open_order_is_retained_in_memory_when_managed_order_loading_fails() {
    for conflicting_active in [false, true] {
        let path = std::env::temp_dir().join(format!("uncertain-load-{}.db", uuid::Uuid::new_v4()));
        let db = Arc::new(Database::open(&path).unwrap());
        db.save_bot_status(BotStatus::Running).unwrap();
        let app = Router::new().route(
            "/fapi/v1/openOrders",
            get(|| async {
                Json(
                    json!([{"orderId":123, "clientOrderId":"gb_unknown_db", "symbol":"SOLUSDC",
                "status":"FILLED", "price":"100", "origQty":"2", "executedQty":"0",
                "side":"BUY", "type":"LIMIT", "timeInForce":"GTX"}]),
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut engine = engine(db.clone(), &url).await;
        if conflicting_active {
            let order = GridOrder {
                client_order_id: "gb_unknown_db".into(),
                order_id: Some(456),
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
            engine
                .state
                .active_orders
                .write()
                .await
                .insert(order.client_order_id.clone(), order);
        }
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch("DROP TABLE managed_orders")
            .unwrap();
        assert!(!engine.sync_live_orders().await);
        let active = engine.state.active_orders.read().await;
        let retained = &active["gb_unknown_db"];
        assert_eq!(
            retained.order_id,
            Some(if conflicting_active { 456 } else { 123 })
        );
        assert_eq!(
            retained.quantity,
            if conflicting_active { dec!(1) } else { dec!(2) }
        );
        assert!(engine.reconciliation_blocked);
        drop(active);
        server.abort();
        drop(connection);
        drop(engine);
        drop(db);
        let _ = std::fs::remove_file(path);
    }
}

#[tokio::test]
async fn recovered_managed_metadata_survives_normal_and_terminal_snapshots() {
    for terminal in [false, true] {
        let path =
            std::env::temp_dir().join(format!("uncertain-metadata-{}.db", uuid::Uuid::new_v4()));
        let db = Arc::new(Database::open(&path).unwrap());
        db.save_bot_status(BotStatus::Running).unwrap();
        let original = GridOrder {
            client_order_id: "gb_metadata".into(),
            order_id: Some(123),
            symbol: "SOLUSDC".into(),
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(2),
            amount_usdc: dec!(200),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: -7,
            paired_client_order_id: Some("gb_parent".into()),
            is_take_profit: true,
            purpose: crate::types::OrderPurpose::Grid,
            merge_sources: vec![crate::types::OrderSource {
                client_order_id: "gb_source".into(),
                price: dec!(99),
                quantity: dec!(1),
            }],
        };
        db.save_managed_order(&original).unwrap();
        let stage = Arc::new(AtomicU8::new(0));
        let app = Router::new().route("/fapi/v1/openOrders", get({
            let stage = stage.clone();
            move || {
                let stage = stage.clone();
                async move {
                    let current = stage.load(Ordering::SeqCst);
                    if current == 2 { return Json(json!([])); }
                    Json(json!([{"orderId":123, "clientOrderId":"gb_metadata", "symbol":"SOLUSDC",
                        "status":if current == 0 { "FILLED" } else { "NEW" }, "price":"100", "origQty":"2", "executedQty":"0",
                        "side":"BUY", "type":"LIMIT", "timeInForce":"GTX"}]))
                }
            }
        })).route("/fapi/v1/order", get(|| async {
            Json(json!({"orderId":123, "clientOrderId":"gb_metadata", "symbol":"SOLUSDC",
                "status":"FILLED", "price":"100", "origQty":"2", "executedQty":"0",
                "side":"BUY", "type":"LIMIT", "timeInForce":"GTX"}))
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut engine = engine(db.clone(), &url).await;
        engine.state.active_orders.write().await.clear();
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection.execute("UPDATE managed_orders SET order_json = 'invalid' WHERE client_order_id = 'gb_metadata'", []).unwrap();
        assert!(!engine.sync_live_orders().await);
        assert_eq!(
            engine.state.active_orders.read().await["gb_metadata"].purpose,
            crate::types::OrderPurpose::Legacy
        );
        connection
            .execute(
                "UPDATE managed_orders SET order_json = ?1 WHERE client_order_id = 'gb_metadata'",
                rusqlite::params![serde_json::to_string(&original).unwrap()],
            )
            .unwrap();
        stage.store(if terminal { 2 } else { 1 }, Ordering::SeqCst);
        assert_eq!(engine.sync_live_orders().await, !terminal);
        let saved = db.load_managed_orders("SOLUSDC").unwrap();
        let active = engine.state.active_orders.read().await;
        for order in [&saved[0], &active["gb_metadata"]] {
            assert_eq!(order.purpose, crate::types::OrderPurpose::Grid);
            assert_eq!(order.grid_level, -7);
            assert_eq!(order.paired_client_order_id.as_deref(), Some("gb_parent"));
            assert!(order.is_take_profit);
            assert_eq!(order.merge_sources.len(), 1);
            assert_eq!(order.merge_sources[0].client_order_id, "gb_source");
            assert_eq!(order.merge_sources[0].price, dec!(99));
            assert_eq!(order.merge_sources[0].quantity, dec!(1));
        }
        drop(active);
        server.abort();
        drop(connection);
        drop(engine);
        drop(db);
        let _ = std::fs::remove_file(path);
    }
}

#[tokio::test]
async fn provisional_identity_conflict_remains_blocked_until_exchange_matches_disk() {
    for already_active in [false, true] {
        let path =
            std::env::temp_dir().join(format!("identity-conflict-{}.db", uuid::Uuid::new_v4()));
        let db = Arc::new(Database::open(&path).unwrap());
        db.save_bot_status(BotStatus::Running).unwrap();
        let original = GridOrder {
            client_order_id: "gb_conflict".into(),
            order_id: Some(123),
            symbol: "SOLUSDC".into(),
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(2),
            amount_usdc: dec!(200),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: -5,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        db.save_managed_order(&original).unwrap();
        let stage = Arc::new(AtomicU8::new(0));
        let app = Router::new().route("/fapi/v1/openOrders", get({
        let stage = stage.clone();
        move || {
            let stage = stage.clone();
            async move {
                let current = stage.load(Ordering::SeqCst);
                Json(json!([{"orderId":if current == 2 {123} else {456}, "clientOrderId":"gb_conflict",
                    "symbol":"SOLUSDC", "status":if current == 0 {"FILLED"} else {"NEW"},
                    "price":"100", "origQty":"2", "executedQty":"0", "side":"BUY", "type":"LIMIT", "timeInForce":"GTX"}]))
            }
        }
    })).route("/fapi/v1/order", get({
        let stage = stage.clone();
        move || { let stage = stage.clone(); async move {
            Json(json!({"orderId":456, "clientOrderId":"gb_conflict", "symbol":"SOLUSDC",
                "status":if stage.load(Ordering::SeqCst) == 2 {"CANCELED"} else {"FILLED"},
                "price":"100", "origQty":"2", "executedQty":"0", "side":"BUY", "type":"LIMIT", "timeInForce":"GTX"}))
        }}
    }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut engine = engine(db.clone(), &url).await;
        if !already_active {
            engine.state.active_orders.write().await.clear();
        }
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection.execute("UPDATE managed_orders SET order_json = 'invalid' WHERE client_order_id = 'gb_conflict'", []).unwrap();
        assert!(!engine.sync_live_orders().await);
        assert_eq!(
            engine.state.active_orders.read().await["gb_conflict"].order_id,
            Some(if already_active { 123 } else { 456 })
        );
        connection
            .execute(
                "UPDATE managed_orders SET order_json = ?1 WHERE client_order_id = 'gb_conflict'",
                rusqlite::params![serde_json::to_string(&original).unwrap()],
            )
            .unwrap();
        stage.store(1, Ordering::SeqCst);
        for _ in 0..2 {
            assert!(!engine.sync_live_orders().await);
            assert_eq!(
                engine.state.active_orders.read().await["gb_conflict"].order_id,
                Some(123)
            );
            assert_eq!(
                db.load_managed_orders("SOLUSDC").unwrap()[0].order_id,
                Some(123)
            );
        }
        stage.store(2, Ordering::SeqCst);
        assert!(engine.sync_live_orders().await);
        let saved = db.load_managed_orders("SOLUSDC").unwrap();
        assert_eq!(saved[0].order_id, Some(123));
        assert_eq!(saved[0].purpose, crate::types::OrderPurpose::Grid);
        assert_eq!(saved[0].grid_level, -5);
        server.abort();
        drop(connection);
        drop(engine);
        drop(db);
        let _ = std::fs::remove_file(path);
    }
}

#[tokio::test]
async fn normal_open_snapshot_cannot_overwrite_disk_identity_after_restart() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let disk = GridOrder {
        client_order_id: "gb_restart_identity".into(),
        order_id: Some(123),
        symbol: "SOLUSDC".into(),
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(1),
        amount_usdc: dec!(100),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: -6,
        paired_client_order_id: Some("gb_parent".into()),
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Grid,
        merge_sources: Vec::new(),
    };
    db.save_managed_order(&disk).unwrap();
    fake.orders.lock().unwrap().insert(
        disk.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 456,
            client_order_id: disk.client_order_id.clone(),
            symbol: disk.symbol.clone(),
            status: "NEW".into(),
            price: disk.price,
            avg_price: None,
            orig_qty: disk.quantity,
            executed_qty: Decimal::ZERO,
            side: "BUY".into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    for _ in 0..2 {
        let mut restarted = engine(db.clone(), &url).await;
        assert!(!restarted.sync_live_orders().await);
        assert_eq!(
            restarted.state.active_orders.read().await["gb_restart_identity"].order_id,
            Some(123)
        );
        assert_eq!(
            db.load_managed_orders("SOLUSDC").unwrap()[0].order_id,
            Some(123)
        );
    }
    {
        let mut orders = fake.orders.lock().unwrap();
        let mut observed = orders.remove("gb_restart_identity").unwrap();
        observed.status = "CANCELED".into();
        let mut trusted = observed.clone();
        trusted.order_id = 123;
        trusted.status = "NEW".into();
        orders.insert("observed_restart".into(), observed);
        orders.insert("gb_restart_identity".into(), trusted);
    }
    let mut restarted = engine(db.clone(), &url).await;
    assert!(restarted.sync_live_orders().await);
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    assert_eq!(saved[0].order_id, Some(123));
    assert_eq!(saved[0].grid_level, -6);
    assert_eq!(
        saved[0].paired_client_order_id.as_deref(),
        Some("gb_parent")
    );
    server.abort();
}

#[tokio::test]
async fn existing_identity_barrier_is_rebased_to_restored_disk_identity() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut engine = engine(db.clone(), &url).await;
    let disk = GridOrder {
        client_order_id: "gb_rebased".into(),
        order_id: Some(123),
        symbol: "SOLUSDC".into(),
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(1),
        amount_usdc: dec!(100),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: -7,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Grid,
        merge_sources: Vec::new(),
    };
    db.save_managed_order(&disk).unwrap();
    let mut memory = disk.clone();
    memory.order_id = Some(456);
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(memory.client_order_id.clone(), memory.clone());
    let key = (
        engine.client.account_identity_key(),
        memory.client_order_id.clone(),
    );
    engine.provisional_order_ids.insert(key.clone());
    engine.order_identity_conflicts.insert(key, memory);
    fake.orders.lock().unwrap().insert(
        disk.client_order_id.clone(),
        BinanceOrderResponse {
            order_id: 456,
            client_order_id: disk.client_order_id.clone(),
            symbol: disk.symbol.clone(),
            status: "NEW".into(),
            price: disk.price,
            avg_price: None,
            orig_qty: disk.quantity,
            executed_qty: Decimal::ZERO,
            side: "BUY".into(),
            order_type: "LIMIT".into(),
            time_in_force: "GTX".into(),
            update_time: None,
        },
    );
    for _ in 0..2 {
        assert!(!engine.sync_live_orders().await);
        assert_eq!(
            engine.state.active_orders.read().await["gb_rebased"].order_id,
            Some(123)
        );
        assert_eq!(
            db.load_managed_orders("SOLUSDC").unwrap()[0].order_id,
            Some(123)
        );
    }
    {
        let mut orders = fake.orders.lock().unwrap();
        let mut observed = orders.remove("gb_rebased").unwrap();
        observed.status = "CANCELED".into();
        let mut trusted = observed.clone();
        trusted.order_id = 123;
        trusted.status = "NEW".into();
        orders.insert("observed_rebased".into(), observed);
        orders.insert("gb_rebased".into(), trusted);
    }
    assert!(engine.sync_live_orders().await);
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    assert_eq!(saved[0].order_id, Some(123));
    assert_eq!(saved[0].grid_level, -7);
    server.abort();
}

#[tokio::test]
async fn all_conflict_groups_record_new_observations_before_any_lookup_can_block() {
    let path = std::env::temp_dir().join(format!("identity-batch-{}.db", uuid::Uuid::new_v4()));
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    let app = Router::new()
        .route("/fapi/v1/openOrders", get(|| async {
            Json(json!([
                {"orderId":789,"clientOrderId":"gb_group_a","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                {"orderId":890,"clientOrderId":"gb_group_b","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}
            ]))
        }))
        .route("/fapi/v1/order", get(|Query(query): Query<HashMap<String, String>>| async move {
            let id = query["orderId"].parse::<i64>().unwrap();
            let client = if id == 456 || id == 789 {"gb_group_a"} else {"gb_group_b"};
            Json(json!({"orderId":id,"clientOrderId":client,"symbol":"SOLUSDC","status":"FILLED",
                "price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}))
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    for (client, expected_id, observed_id) in [("gb_group_a", 123, 456), ("gb_group_b", 234, 567)] {
        let order = GridOrder {
            client_order_id: client.into(),
            order_id: Some(expected_id),
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
            purpose: crate::types::OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        db.save_managed_order(&order).unwrap();
        db.save_identity_observation(
            account,
            &order,
            &crate::types::ObservedOrderIdentity {
                client_order_id: client.into(),
                symbol: "SOLUSDC".into(),
                side: "BUY".into(),
                order_id: observed_id,
            },
        )
        .unwrap();
    }
    connection.execute_batch("CREATE TRIGGER reject_later_observation BEFORE INSERT ON order_identity_observations \
        WHEN NEW.observed_order_id IN (789,890) AND EXISTS (SELECT 1 FROM order_identity_observations WHERE observed_order_id IN (789,890)) \
        BEGIN SELECT RAISE(ABORT, 'batch failure'); END;").unwrap();
    assert!(!first.sync_live_orders().await);
    let observed = db.load_identity_observations(account, "SOLUSDC").unwrap();
    let ids: std::collections::HashSet<_> =
        observed.iter().map(|(_, item)| item.order_id).collect();
    assert_eq!(ids, [456, 567].into_iter().collect());
    assert!(first.reconciliation_blocked);
    assert!(!first.state.global_position_ready.load(Ordering::SeqCst));
    assert_eq!(
        first
            .order_identity_observations
            .values()
            .map(Vec::len)
            .sum::<usize>(),
        4
    );
    connection
        .execute_batch("DROP TRIGGER reject_later_observation")
        .unwrap();
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    assert_eq!(
        db.load_identity_observations(account, "SOLUSDC")
            .unwrap()
            .len(),
        4
    );
    server.abort();
    drop(connection);
    drop(restarted);
    drop(db);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn unknown_order_id_requires_unique_client_lookup_and_survives_restart() {
    let db = database();
    let stage = Arc::new(AtomicU8::new(0));
    let app = Router::new()
        .route("/fapi/v1/openOrders", get(|| async {
            Json(json!([{"orderId":123,"clientOrderId":"gb_unknown_id","symbol":"SOLUSDC",
                "status":"NEW","price":"100","origQty":"2","executedQty":"0",
                "side":"SELL","type":"LIMIT","timeInForce":"GTX"}]))
        }))
        .route("/fapi/v1/order", get({
            let stage = stage.clone();
            move |Query(query): Query<HashMap<String, String>>| {
                let stage = stage.clone();
                async move {
                    let wrong = query.contains_key("origClientOrderId") && stage.load(Ordering::SeqCst) == 0;
                    let queried_id = query.get("orderId").and_then(|s| s.parse::<i64>().ok()).unwrap_or(123);
                    let alternate = wrong || queried_id == 456;
                    Json(json!({"orderId":if alternate {456} else {123},"clientOrderId":"gb_unknown_id","symbol":"SOLUSDC",
                        "status":if queried_id == 456 {"CANCELED"} else {"PARTIALLY_FILLED"},"price":"100","origQty":"2",
                        "executedQty":if queried_id == 456 {"0"} else {"1"},
                        "side":"BUY","type":"LIMIT","timeInForce":"GTX"}))
                }
            }
        }))
        .route("/fapi/v2/positionRisk", get({
            let stage = stage.clone();
            move || { let stage = stage.clone(); async move {
                if stage.load(Ordering::SeqCst) < 2 { return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({}))); }
                (StatusCode::OK, Json(json!([{"symbol":"SOLUSDC","positionAmt":"2","entryPrice":"100","markPrice":"100",
                    "unRealizedProfit":"0","liquidationPrice":"0","leverage":"20"}])))
            }}
        }))
        .route("/fapi/v2/account", get(|| async {
            Json(json!({"totalWalletBalance":"1000","totalMarginBalance":"1000","totalUnrealizedProfit":"0",
                "availableBalance":"1000","assets":[{"asset":"USDC","walletBalance":"1000","marginBalance":"1000",
                "availableBalance":"1000","unrealizedProfit":"0"}]}))
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut first = engine(db.clone(), &url).await;
    let expected = GridOrder {
        client_order_id: "gb_unknown_id".into(),
        order_id: None,
        symbol: "SOLUSDC".into(),
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(2),
        amount_usdc: dec!(200),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: 0,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Grid,
        merge_sources: Vec::new(),
    };
    db.save_managed_order(&expected).unwrap();
    let account = first.client.account_identity_key();
    db.save_identity_observation(
        account,
        &expected,
        &crate::types::ObservedOrderIdentity {
            client_order_id: expected.client_order_id.clone(),
            symbol: expected.symbol.clone(),
            side: "SELL".into(),
            order_id: 123,
        },
    )
    .unwrap();
    assert!(!first.sync_live_orders().await);
    assert_eq!(
        db.load_identity_observations(account, "SOLUSDC").unwrap()[0]
            .0
            .order_id,
        None
    );
    stage.store(1, Ordering::SeqCst);
    assert!(!first.sync_live_orders().await);
    assert_eq!(
        db.load_identity_observations(account, "SOLUSDC").unwrap()[0]
            .0
            .order_id,
        Some(123)
    );
    drop(first);
    stage.store(2, Ordering::SeqCst);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(restarted.sync_live_orders().await);
    let active = restarted.state.active_orders.read().await;
    assert_eq!(active["gb_unknown_id"].order_id, Some(123));
    assert_eq!(active["gb_unknown_id"].status, OrderStatus::PartiallyFilled);
    assert_eq!(active["gb_unknown_id"].quantity, dec!(1));
    assert_eq!(restarted.state.position.read().await.size, dec!(2));
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .is_empty());
    server.abort();
}

#[tokio::test]
async fn corrected_same_order_side_with_fill_uses_normal_reconciliation() {
    for fully_filled in [false, true] {
        let db = database();
        let app = Router::new()
            .route("/fapi/v1/openOrders", get(|| async {
                Json(json!([{"orderId":123,"clientOrderId":"gb_side_corrected","symbol":"SOLUSDC",
                    "status":"NEW","price":"100","origQty":"2","executedQty":"0",
                    "side":"SELL","type":"LIMIT","timeInForce":"GTX"}]))
            }))
            .route("/fapi/v1/order", get(move || async move {
                Json(json!({"orderId":123,"clientOrderId":"gb_side_corrected","symbol":"SOLUSDC",
                    "status":if fully_filled {"FILLED"} else {"PARTIALLY_FILLED"}, "price":"100","origQty":"2",
                    "executedQty":if fully_filled {"2"} else {"1"}, "side":"BUY","type":"LIMIT","timeInForce":"GTX"}))
            }))
            .route("/fapi/v2/positionRisk", get(|| async {
                Json(json!([{"symbol":"SOLUSDC","positionAmt":"2","entryPrice":"100","markPrice":"100",
                    "unRealizedProfit":"0","liquidationPrice":"0","leverage":"20"}]))
            }))
            .route("/fapi/v2/account", get(|| async {
                Json(json!({"totalWalletBalance":"1000","totalMarginBalance":"1000","totalUnrealizedProfit":"0",
                    "availableBalance":"1000","assets":[{"asset":"USDC","walletBalance":"1000","marginBalance":"1000",
                    "availableBalance":"1000","unrealizedProfit":"0"}]}))
            }))
            .route("/fapi/v1/userTrades", get(|| async { Json(json!([])) }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut engine = engine(db.clone(), &url).await;
        let expected = GridOrder {
            client_order_id: "gb_side_corrected".into(),
            order_id: Some(123),
            symbol: "SOLUSDC".into(),
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(2),
            amount_usdc: dec!(200),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 0,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        db.save_managed_order(&expected).unwrap();
        let account = engine.client.account_identity_key();
        db.save_identity_observation(
            account,
            &expected,
            &crate::types::ObservedOrderIdentity {
                client_order_id: expected.client_order_id.clone(),
                symbol: expected.symbol.clone(),
                side: "SELL".into(),
                order_id: 123,
            },
        )
        .unwrap();
        assert!(engine.sync_live_orders().await);
        assert_eq!(engine.state.position.read().await.size, dec!(2));
        if fully_filled {
            assert!(!engine
                .state
                .active_orders
                .read()
                .await
                .contains_key("gb_side_corrected"));
            assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
        } else {
            let active = engine.state.active_orders.read().await;
            assert_eq!(active["gb_side_corrected"].quantity, dec!(1));
            assert_eq!(
                active["gb_side_corrected"].status,
                OrderStatus::PartiallyFilled
            );
        }
        assert!(db
            .load_identity_observations(account, "SOLUSDC")
            .unwrap()
            .is_empty());
        server.abort();
    }
}

#[tokio::test]
async fn changed_window_restores_all_identity_observations_and_accepts_corrected_side_terminal() {
    let db = database();
    let stage = Arc::new(AtomicU8::new(0));
    let app = Router::new()
        .route("/fapi/v1/openOrders", get({
            let stage = stage.clone();
            move || { let stage = stage.clone(); async move {
                if stage.load(Ordering::SeqCst) > 0 { return Json(json!([])); }
                Json(json!([{"orderId":789,"clientOrderId":"gb_many_conflicts","symbol":"SOLUSDC",
                    "status":"NEW","price":"100","origQty":"1","executedQty":"0",
                    "side":"BUY","type":"LIMIT","timeInForce":"GTX"}]))
            }}
        }))
        .route("/fapi/v1/order", get({
            let stage = stage.clone();
            move |Query(query): Query<HashMap<String, String>>| {
                let stage = stage.clone();
                async move {
                    let id = query.get("orderId").and_then(|s| s.parse::<i64>().ok()).unwrap_or(123);
                    let status = if stage.load(Ordering::SeqCst) == 0 { "FILLED" } else { "CANCELED" };
                    Json(json!({"orderId":id,"clientOrderId":"gb_many_conflicts","symbol":"SOLUSDC",
                        "status":status,"price":"100","origQty":"1","executedQty":"0",
                        "side":"BUY","type":"LIMIT","timeInForce":"GTX"}))
                }
            }
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut first = engine(db.clone(), &url).await;
    let expected = GridOrder {
        client_order_id: "gb_many_conflicts".into(),
        order_id: Some(123),
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
        purpose: crate::types::OrderPurpose::Grid,
        merge_sources: Vec::new(),
    };
    db.save_managed_order(&expected).unwrap();
    let account = first.client.account_identity_key();
    db.save_identity_observation(
        account,
        &expected,
        &crate::types::ObservedOrderIdentity {
            client_order_id: expected.client_order_id.clone(),
            symbol: expected.symbol.clone(),
            side: "SELL".into(),
            order_id: 456,
        },
    )
    .unwrap();
    assert!(!first.sync_live_orders().await);
    let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
    assert_eq!(observations.len(), 2);
    assert!(observations.iter().any(|(_, item)| item.order_id == 789));
    drop(first);
    stage.store(1, Ordering::SeqCst);
    let mut restarted = engine(db.clone(), &url).await;
    let mut exchange = restarted.state.config.read().await.exchange.clone();
    exchange.recv_window += 1000;
    restarted.state.config.write().await.exchange.recv_window = exchange.recv_window;
    restarted.client = Arc::new(BinanceFuturesClient::with_base_url(&exchange, url));
    assert_eq!(restarted.client.account_identity_key(), account);
    assert!(restarted.sync_live_orders().await);
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .is_empty());
    server.abort();
}

#[tokio::test]
async fn identity_conflict_can_resolve_through_confirmed_terminal_lookup() {
    for filled in [false, true] {
        let fake = Exchange::default();
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let mut engine = engine(db.clone(), &url).await;
        let order = GridOrder {
            client_order_id: "gb_terminal_identity".into(),
            order_id: Some(123),
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
            purpose: crate::types::OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        db.save_managed_order(&order).unwrap();
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(order.client_order_id.clone(), order.clone());
        let key = (
            engine.client.account_identity_key(),
            order.client_order_id.clone(),
        );
        engine.provisional_order_ids.insert(key.clone());
        engine.order_identity_conflicts.insert(key, order.clone());
        fake.orders.lock().unwrap().insert(
            order.client_order_id.clone(),
            BinanceOrderResponse {
                order_id: 123,
                client_order_id: order.client_order_id.clone(),
                symbol: order.symbol.clone(),
                status: if filled { "FILLED" } else { "CANCELED" }.into(),
                price: order.price,
                avg_price: filled.then_some(order.price),
                orig_qty: order.quantity,
                executed_qty: if filled {
                    order.quantity
                } else {
                    Decimal::ZERO
                },
                side: "BUY".into(),
                order_type: "LIMIT".into(),
                time_in_force: "GTX".into(),
                update_time: None,
            },
        );
        assert!(engine.sync_live_orders().await);
        assert!(engine.state.active_orders.read().await.is_empty());
        assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
        assert_eq!(db.get_recent_trades(10).unwrap().len(), usize::from(filled));
        assert!(engine.order_identity_conflicts.is_empty());
        server.abort();
    }
}

#[tokio::test]
async fn switching_to_exchange_modes_requires_manual_resume_before_orders() {
    for (source, target, previously_running) in [
        (TradingMode::Paper, TradingMode::Testnet, false),
        (TradingMode::Paper, TradingMode::Testnet, true),
        (TradingMode::Paper, TradingMode::Live, false),
        (TradingMode::Paper, TradingMode::Live, true),
        (TradingMode::Testnet, TradingMode::Live, false),
        (TradingMode::Live, TradingMode::Testnet, false),
    ] {
        let fake = Exchange::default();
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let mut engine = engine(db.clone(), &url).await;
        let mut initial = engine.state.config.read().await.clone();
        initial.exchange.dry_run = source == TradingMode::Paper;
        initial.exchange.is_testnet = source == TradingMode::Testnet;
        db.save_config(&initial).unwrap();
        engine
            .state
            .save_runtime_config(initial.clone())
            .await
            .unwrap();
        engine.state.resume_trading().await.unwrap();
        if previously_running {
            db.save_pair_status("SOLUSDC", target, BotStatus::Running)
                .unwrap();
        }
        let epoch = engine.state.pause_epoch();
        let mut exchange = initial;
        exchange.exchange.dry_run = false;
        exchange.exchange.is_testnet = target == TradingMode::Testnet;
        let (committed, _) = tokio::join!(
            engine.state.save_runtime_config(exchange.clone()),
            engine.state.resume_trading_silently(epoch)
        );
        committed.unwrap();
        assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
        assert_eq!(
            db.load_pair_status("SOLUSDC", target).unwrap(),
            Some(BotStatus::Paused)
        );
        assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
        let (tx, _) = mpsc::channel(1);
        let restored = AppState::new(exchange, db.clone(), tx);
        assert_eq!(*restored.status.read().await, BotStatus::Paused);
        assert!(!submit(&mut engine).await);
        assert!(fake.posts.lock().unwrap().is_empty());
        engine.state.resume_trading().await.unwrap();
        assert!(submit(&mut engine).await);
        assert_eq!(fake.posts.lock().unwrap().len(), 1);
        server.abort();
    }
}

#[tokio::test]
async fn backend_timeout_keeps_accepted_identity_and_restart_records_terminal_fill_once() {
    let fake = Exchange::default();
    fake.submission.store(1, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut first = engine(db.clone(), &url).await;
    assert!(!submit(&mut first).await);
    assert!(first.last_orders_sync.is_none());
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].order_id, Some(1));
    // Complete before the next open-orders poll, then restart.
    {
        let mut orders = fake.orders.lock().unwrap();
        let order = orders.get_mut("gb_b_reliability").unwrap();
        order.status = "FILLED".into();
        order.executed_qty = order.orig_qty;
        order.avg_price = Some(order.price);
    }
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    fake.position_unavailable.store(true, Ordering::SeqCst);
    assert!(!restarted.sync_live_orders().await);
    assert!(restarted
        .state
        .active_orders
        .read()
        .await
        .contains_key("gb_b_reliability"));
    assert_eq!(db.load_managed_orders("SOLUSDC").unwrap().len(), 1);
    assert!(db.get_recent_trades(10).unwrap().is_empty());
    fake.position_unavailable.store(false, Ordering::SeqCst);
    // Terminal reconciliation must still refresh both position and account
    // when the ordinary request budget has been exhausted.
    let limiter = Arc::new(crate::exchange::rate_limit::RateLimiter::default());
    limiter
        .reserve(
            &url,
            [0; 32],
            1800,
            crate::exchange::rate_limit::RequestClass::Normal,
        )
        .unwrap();
    restarted.client = Arc::new(BinanceFuturesClient::with_base_url_and_limiter(
        &restarted.state.config.read().await.exchange,
        url.clone(),
        limiter,
    ));
    assert!(restarted.sync_account_and_position().await);
    restarted.client = Arc::new(BinanceFuturesClient::with_base_url(
        &restarted.state.config.read().await.exchange,
        url.clone(),
    ));
    assert!(restarted.sync_live_orders().await);
    // Simulate a recorded terminal order restored alongside a pre-fill snapshot.
    fake.position_flat.store(true, Ordering::SeqCst);
    restarted
        .position_cache
        .get(&restarted.client, "SOLUSDC")
        .await
        .unwrap();
    restarted.state.position.write().await.size = dec!(0);
    let restored = saved[0].clone();
    restarted
        .state
        .active_orders
        .write()
        .await
        .insert(restored.client_order_id.clone(), restored);
    fake.position_flat.store(false, Ordering::SeqCst);
    fake.position_unavailable.store(true, Ordering::SeqCst);
    assert!(!restarted.sync_live_orders().await);
    assert!(restarted
        .state
        .active_orders
        .read()
        .await
        .contains_key("gb_b_reliability"));
    fake.position_unavailable.store(false, Ordering::SeqCst);
    assert!(restarted.sync_live_orders().await);
    assert_eq!(restarted.state.position.read().await.size, dec!(1));
    assert_eq!(
        restarted
            .position_cache
            .get(&restarted.client, "SOLUSDC")
            .await
            .unwrap()
            .0
            .unwrap()
            .position_amt,
        dec!(1)
    );
    assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
    assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
    assert_eq!(fake.posts.lock().unwrap().len(), 1);
    server.abort();
}

#[tokio::test]
async fn absent_unaccepted_intent_waits_then_retires_without_blocking_restart() {
    let fake = Exchange::default();
    fake.submission.store(2, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut first = engine(db.clone(), &url).await;
    assert!(!submit(&mut first).await);
    assert!(!first.sync_live_orders().await);
    let mut intent = db.load_managed_orders("SOLUSDC").unwrap().remove(0);
    assert!(intent.order_id.is_none());
    intent.created_at = Utc::now() - chrono::Duration::seconds(31);
    db.save_managed_order(&intent).unwrap();
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(restarted.sync_live_orders().await);
    assert!(restarted.state.active_orders.read().await.is_empty());
    assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
    fake.submission.store(0, Ordering::SeqCst);
    assert!(
        restarted
            .place_grid_order(
                OrderSide::Buy,
                dec!(100),
                "gb_b_new_attempt".into(),
                -1,
                None
            )
            .await
    );
    assert_eq!(fake.posts.lock().unwrap().len(), 2);
    server.abort();
}

#[tokio::test]
async fn unavailable_lookup_never_releases_an_old_unknown_intent() {
    let fake = Exchange::default();
    fake.submission.store(2, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut engine = engine(db.clone(), &url).await;
    assert!(!submit(&mut engine).await);
    engine
        .state
        .active_orders
        .write()
        .await
        .get_mut("gb_b_reliability")
        .unwrap()
        .created_at = Utc::now() - chrono::Duration::minutes(1);
    fake.lookup_unavailable.store(true, Ordering::SeqCst);
    assert!(!engine.sync_live_orders().await);
    assert_eq!(db.load_managed_orders("SOLUSDC").unwrap().len(), 1);
    assert_eq!(engine.state.active_orders.read().await.len(), 1);
    server.abort();
}

#[tokio::test]
async fn missing_already_accepted_order_is_never_retired_as_an_unsent_intent() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut engine = engine(db.clone(), &url).await;
    assert!(submit(&mut engine).await);
    fake.orders.lock().unwrap().clear();
    engine
        .state
        .active_orders
        .write()
        .await
        .get_mut("gb_b_reliability")
        .unwrap()
        .created_at = Utc::now() - chrono::Duration::minutes(1);
    assert!(!engine.sync_live_orders().await);
    assert_eq!(db.load_managed_orders("SOLUSDC").unwrap().len(), 1);
    server.abort();
}

#[tokio::test]
async fn direct_query_persists_acceptance_when_open_snapshot_omits_the_order() {
    let fake = Exchange::default();
    fake.submission.store(1, Ordering::SeqCst);
    fake.lookup_unavailable.store(true, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut first = engine(db.clone(), &url).await;
    assert!(!submit(&mut first).await);
    assert!(db.load_managed_orders("SOLUSDC").unwrap()[0]
        .order_id
        .is_none());
    fake.lookup_unavailable.store(false, Ordering::SeqCst);
    fake.hide_open_orders.store(true, Ordering::SeqCst);
    assert!(!first.sync_live_orders().await);
    let mut accepted = db.load_managed_orders("SOLUSDC").unwrap().remove(0);
    assert_eq!(accepted.order_id, Some(1));
    accepted.created_at = Utc::now() - chrono::Duration::minutes(1);
    db.save_managed_order(&accepted).unwrap();
    fake.orders.lock().unwrap().clear();
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    assert_eq!(db.load_managed_orders("SOLUSDC").unwrap().len(), 1);
    server.abort();
}

#[tokio::test]
async fn stale_last_or_mark_blocks_orders_even_when_account_reconciliation_succeeds() {
    for stale_mark in [false, true] {
        let fake = Exchange::default();
        let (url, server) = exchange_server(fake.clone()).await;
        let mut engine = engine(database(), &url).await;
        {
            let mut ticker = engine.state.ticker.write().await;
            let stale = Utc::now() - chrono::Duration::seconds(16);
            if stale_mark {
                ticker.mark_update_time = stale;
            } else {
                ticker.update_time = stale;
            }
        }
        assert!(!submit(&mut engine).await);
        engine.sync_cycle().await;
        assert!(engine.last_account_sync.is_some());
        assert!(engine.last_orders_sync.is_some());
        assert!(fake.posts.lock().unwrap().is_empty());
        {
            let mut ticker = engine.state.ticker.write().await;
            ticker.update_time = Utc::now();
            ticker.mark_update_time = Utc::now();
        }
        engine.maintain_grid_window().await;
        assert!(!fake.posts.lock().unwrap().is_empty());
        server.abort();
    }
}

#[tokio::test]
async fn stale_remainder_plan_keeps_identity_until_fresh_prices_return() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut engine = engine(db.clone(), &url).await;
    let now = Utc::now();
    let mut plan = RemainderPlan {
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Live,
        sources: vec![],
        phase: RemainderPhase::Submitting,
        target: GridOrder {
            client_order_id: "gb_s_stale_remainder".into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side: OrderSide::Sell,
            price: dec!(101),
            quantity: dec!(0.05),
            amount_usdc: dec!(5.05),
            status: OrderStatus::New,
            created_at: now,
            updated_at: now,
            grid_level: 1,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: OrderPurpose::Remainder,
            merge_sources: vec![],
        },
    };
    db.save_remainder_plan(&plan).unwrap();
    engine.state.ticker.write().await.mark_update_time = now - chrono::Duration::seconds(16);
    assert!(engine.recover_remainder_plan().await.unwrap());
    assert!(fake.posts.lock().unwrap().is_empty());
    plan = db
        .load_remainder_plan("SOLUSDC", TradingMode::Live)
        .unwrap()
        .unwrap();
    assert_eq!(plan.phase, RemainderPhase::Submitting);
    engine.state.ticker.write().await.mark_update_time = Utc::now();
    engine.recover_remainder_plan().await.unwrap();
    assert_eq!(
        fake.posts.lock().unwrap().as_slice(),
        &["gb_s_stale_remainder"]
    );
    server.abort();
}

#[tokio::test]
async fn delayed_ticker_does_not_become_fresh_when_consumed() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let mut engine = engine(database(), &url).await;
    let previous = Utc::now() - chrono::Duration::seconds(20);
    engine.state.ticker.write().await.update_time = previous;
    let received = Utc::now() - chrono::Duration::seconds(16);
    engine
        .handle_ticker_update(TickerInfo {
            symbol: "SOLUSDC".into(),
            last_price: dec!(100.5),
            update_time: received,
            ..Default::default()
        })
        .await;
    assert_eq!(engine.state.ticker.read().await.update_time, previous);
    assert!(!submit(&mut engine).await);
    assert!(fake.posts.lock().unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn stale_mark_does_not_match_or_prune_existing_paper_orders() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake).await;
    let db = database();
    let mut engine = engine(db.clone(), &url).await;
    engine.state.config.write().await.exchange.dry_run = true;
    assert!(submit(&mut engine).await);
    engine.state.ticker.write().await.mark_update_time = Utc::now() - chrono::Duration::seconds(16);
    engine
        .handle_ticker_update(TickerInfo {
            symbol: "SOLUSDC".into(),
            last_price: dec!(95),
            update_time: Utc::now(),
            ..Default::default()
        })
        .await;
    engine.maintain_grid_window().await;
    assert_eq!(engine.state.active_orders.read().await.len(), 1);
    assert!(db.get_recent_trades(10).unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn shutdown_records_racing_fill_and_restart_stays_paused() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut engine = engine(db.clone(), &url).await;
    assert!(submit(&mut engine).await);
    fake.fill_on_cancel.store(true, Ordering::SeqCst);
    engine.shutdown().await.unwrap();
    assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
    assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
    assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
    assert!(engine.state.resume_trading().await.is_err());
    assert!(!submit(&mut engine).await);
    assert_eq!(fake.posts.lock().unwrap().len(), 1);
    server.abort();
}

#[tokio::test]
async fn failed_shutdown_preserves_orders_and_paused_status() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut engine = engine(db.clone(), &url).await;
    assert!(submit(&mut engine).await);
    fake.cancel_fails.store(true, Ordering::SeqCst);
    assert!(engine.shutdown().await.is_err());
    assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
    assert_eq!(db.load_managed_orders("SOLUSDC").unwrap().len(), 1);
    assert_eq!(
        fake.orders.lock().unwrap()["gb_b_reliability"].status,
        "NEW"
    );
    server.abort();
}

#[tokio::test]
async fn shutdown_confirms_a_cancellation_whose_response_was_lost() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut engine = engine(db.clone(), &url).await;
    assert!(submit(&mut engine).await);
    fake.lose_cancel_response.store(true, Ordering::SeqCst);
    engine.shutdown().await.unwrap();
    assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
    assert_eq!(
        fake.orders.lock().unwrap()["gb_b_reliability"].status,
        "CANCELED"
    );
    assert_eq!(db.load_bot_status().unwrap(), Some(BotStatus::Paused));
    server.abort();
}

#[tokio::test]
async fn interrupted_initialization_still_reconciles_persisted_orders_on_shutdown() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut first = engine(db.clone(), &url).await;
    assert!(submit(&mut first).await);
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    restarted.state.active_orders.write().await.clear();
    fake.fill_on_cancel.store(true, Ordering::SeqCst);
    restarted.shutdown().await.unwrap();
    assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
    assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn shutdown_skips_queued_resume_and_configuration_then_joins_engine() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let mut engine = engine(database(), &url).await;
    let state = engine.state.clone();
    state.request_shutdown();
    let (resume_reply, resume_response) = tokio::sync::oneshot::channel();
    state
        .action_tx
        .send(BotControlAction::Resume {
            expected_mode: TradingMode::Live,
            expected_epoch: state.pause_epoch(),
            reply: resume_reply,
        })
        .await
        .unwrap();
    let (reply, response) = tokio::sync::oneshot::channel();
    let config = state.config.read().await.clone();
    state
        .action_tx
        .send(BotControlAction::UpdateConfig {
            config: Box::new(config),
            reply,
        })
        .await
        .unwrap();
    let mut task = tokio::spawn(async move { engine.run().await });
    stop_engine(&state, &mut task, Duration::from_secs(2))
        .await
        .unwrap();
    assert!(resume_response.await.unwrap().is_err());
    assert!(response.await.unwrap().is_err());
    assert!(task.is_finished());
    assert_eq!(*state.status.read().await, BotStatus::Paused);
    assert!(fake.posts.lock().unwrap().is_empty());
    server.abort();
}
