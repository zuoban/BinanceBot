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
    liquidation_close_after_submission: Arc<AtomicBool>,
    block_submit: Arc<AtomicBool>,
    submit_started: Arc<tokio::sync::Notify>,
    release_submit: Arc<tokio::sync::Notify>,
    lookup_filled_zero: Arc<AtomicBool>,
    block_lookup: Arc<AtomicBool>,
    lookup_started: Arc<tokio::sync::Notify>,
    release_lookup: Arc<tokio::sync::Notify>,
    block_position: Arc<AtomicBool>,
    position_started: Arc<tokio::sync::Notify>,
    release_position: Arc<tokio::sync::Notify>,
    block_account: Arc<AtomicBool>,
    account_unavailable: Arc<AtomicBool>,
    account_started: Arc<tokio::sync::Notify>,
    release_account: Arc<tokio::sync::Notify>,
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
            if fake.block_lookup.load(Ordering::SeqCst) {
                fake.lookup_started.notify_one();
                fake.release_lookup.notified().await;
            }
            if fake.lookup_unavailable.load(Ordering::SeqCst) {
                return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code": -1007, "msg": "backend timeout"})));
            }
            let orders = fake.orders.lock().unwrap();
            let found = if let Some(id) = query.get("orderId") {
                orders.values().find(|order| order.order_id.to_string() == *id)
            } else if let Some(client_id) = query.get("origClientOrderId") {
                orders.values().filter(|order| &order.client_order_id == client_id)
                    .min_by_key(|order| (!matches!(order.status.as_str(), "NEW" | "PARTIALLY_FILLED"), order.order_id))
            } else {
                None
            }.cloned();
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
            if fake.block_position.load(Ordering::SeqCst) {
                fake.position_started.notify_one();
                fake.release_position.notified().await;
            }
            if fake.position_unavailable.load(Ordering::SeqCst) {
                return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"msg":"unavailable"})));
            }
            let amount = if fake.position_flat.load(Ordering::SeqCst) { "0" } else if fake.position_large.load(Ordering::SeqCst) { "2" } else { "1" };
            let liquidation = if fake.liquidation_close_after_submission.load(Ordering::SeqCst)
                && !fake.posts.lock().unwrap().is_empty() { "95" } else { "0" };
            (StatusCode::OK, Json(json!([{"symbol":"SOLUSDC", "positionAmt":amount, "entryPrice":"100", "markPrice":"100.5", "unRealizedProfit":"0.5", "liquidationPrice":liquidation, "leverage":"1"}])))
        }))
        .route("/fapi/v2/account", get(|State(fake): State<Exchange>| async move {
            if fake.block_account.load(Ordering::SeqCst) {
                fake.account_started.notify_one();
                fake.release_account.notified().await;
            }
            if fake.account_unavailable.load(Ordering::SeqCst) {
                return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"msg":"unavailable"})));
            }
            (StatusCode::OK, Json(json!({"totalWalletBalance":"10000", "totalMarginBalance":"10000", "totalUnrealizedProfit":"0", "availableBalance":"10000", "assets":[{"asset":"USDC", "walletBalance":"10000", "marginBalance":"10000", "availableBalance":"10000", "unrealizedProfit":"0"}]})))
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
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
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

fn managed_price_fixture() -> (GridOrder, BinanceOrderResponse) {
    let order = GridOrder {
        client_order_id: "gb_price_update".into(),
        order_id: Some(123),
        symbol: "SOLUSDC".into(),
        side: OrderSide::Buy,
        price: dec!(100.125),
        quantity: dec!(1.25),
        amount_usdc: dec!(125.15625),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: -6,
        paired_client_order_id: Some("gb_parent".into()),
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Grid,
        merge_sources: Vec::new(),
    };
    let response = BinanceOrderResponse {
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
    };
    (order, response)
}

async fn assert_managed_price(engine: &GridTradingEngine, db: &Database, price: Decimal) {
    let active = engine.state.active_orders.read().await;
    let order = &active["gb_price_update"];
    assert_eq!(order.price, price);
    assert_eq!(order.quantity, dec!(1.25));
    assert_eq!(order.amount_usdc, price * dec!(1.25));
    assert_eq!(order.grid_level, -6);
    assert_eq!(order.paired_client_order_id.as_deref(), Some("gb_parent"));
    drop(active);
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].price, price);
    assert_eq!(saved[0].quantity, dec!(1.25));
    assert_eq!(saved[0].amount_usdc, price * dec!(1.25));
    assert_eq!(saved[0].grid_level, -6);
    assert_eq!(
        saved[0].paired_client_order_id.as_deref(),
        Some("gb_parent")
    );
}

#[tokio::test]
async fn managed_open_price_updates_are_durable_and_zero_blocks_until_corrected() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let (order, response) = managed_price_fixture();
    db.save_managed_order(&order).unwrap();
    fake.orders
        .lock()
        .unwrap()
        .insert(order.client_order_id.clone(), response);
    let mut first = engine(db.clone(), &url).await;

    for price in [dec!(102.375), dec!(98.0625)] {
        fake.orders
            .lock()
            .unwrap()
            .get_mut(&order.client_order_id)
            .unwrap()
            .price = price;
        assert!(first.sync_live_orders().await);
        assert_managed_price(&first, &db, price).await;
    }
    drop(first);

    fake.orders
        .lock()
        .unwrap()
        .get_mut(&order.client_order_id)
        .unwrap()
        .price = Decimal::ZERO;
    let mut restarted = engine(db.clone(), &url).await;
    let account = restarted.client.account_identity_key();
    assert_managed_price(&restarted, &db, dec!(98.0625)).await;
    assert!(!restarted.sync_live_orders().await);
    assert!(restarted.reconciliation_blocked);
    assert_managed_price(&restarted, &db, dec!(98.0625)).await;

    fake.orders
        .lock()
        .unwrap()
        .get_mut(&order.client_order_id)
        .unwrap()
        .price = dec!(101.875);
    assert!(restarted.sync_live_orders().await);
    assert_managed_price(&restarted, &db, dec!(101.875)).await;
    assert!(db
        .load_terminal_order_disagreements(account, "SOLUSDC")
        .unwrap()
        .is_empty());
    drop(restarted);
    let mut recovered = engine(db.clone(), &url).await;
    assert_managed_price(&recovered, &db, dec!(101.875)).await;
    assert!(recovered.sync_live_orders().await);
    assert_managed_price(&recovered, &db, dec!(101.875)).await;
    assert!(db
        .load_terminal_order_disagreements(account, "SOLUSDC")
        .unwrap()
        .is_empty());
    server.abort();
}

#[tokio::test]
async fn managed_open_price_write_failure_blocks_and_recovers_on_retry_after_restart() {
    let path = std::env::temp_dir().join(format!("managed-price-{}.db", uuid::Uuid::new_v4()));
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let (order, mut response) = managed_price_fixture();
    db.save_managed_order(&order).unwrap();
    response.price = dec!(102.375);
    let fake = Exchange::default();
    fake.orders
        .lock()
        .unwrap()
        .insert(order.client_order_id.clone(), response);
    let (url, server) = exchange_server(fake).await;
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_managed_price BEFORE INSERT ON managed_orders \
         BEGIN SELECT RAISE(ABORT, 'managed price write failed'); END;",
        )
        .unwrap();

    let mut first = engine(db.clone(), &url).await;
    assert!(!first.sync_live_orders().await);
    assert!(first.reconciliation_blocked);
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    assert_eq!(saved[0].price, order.price);
    assert_eq!(saved[0].amount_usdc, order.amount_usdc);
    drop(first);

    let mut restarted = engine(db.clone(), &url).await;
    assert_managed_price(&restarted, &db, order.price).await;
    assert!(!restarted.sync_live_orders().await);
    assert!(restarted.reconciliation_blocked);
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    assert_eq!(saved[0].price, order.price);
    assert_eq!(saved[0].amount_usdc, order.amount_usdc);
    connection
        .execute_batch("DROP TRIGGER reject_managed_price;")
        .unwrap();
    assert!(restarted.sync_live_orders().await);
    assert_managed_price(&restarted, &db, dec!(102.375)).await;
    drop(restarted);
    let mut recovered = engine(db.clone(), &url).await;
    assert_managed_price(&recovered, &db, dec!(102.375)).await;
    assert!(recovered.sync_live_orders().await);
    assert_managed_price(&recovered, &db, dec!(102.375)).await;
    server.abort();
    drop(recovered);
    drop(connection);
    drop(db);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn managed_open_price_change_affects_opening_order_limit_on_live() {
    for changed in [false, true] {
        let fake = Exchange::default();
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let (mut order, mut response) = managed_price_fixture();
        order.price = dec!(90.125);
        order.amount_usdc = order.price * order.quantity;
        response.price = if changed { dec!(120.375) } else { order.price };
        db.save_managed_order(&order).unwrap();
        fake.orders
            .lock()
            .unwrap()
            .insert(order.client_order_id.clone(), response.clone());
        let mut engine = engine(db.clone(), &url).await;
        {
            let mut config = engine.state.config.write().await;
            config.grid.order_amount_usdc = dec!(10);
            config.grid.max_position_usdc = Some(dec!(245));
            config.grid.pause_on_max_position = false;
        }
        let exchange = engine.state.config.read().await.exchange.clone();
        engine.client = Arc::new(BinanceFuturesClient::with_base_url(&exchange, url.clone()));
        assert!(engine.sync_live_orders().await);
        assert_managed_price(&engine, &db, response.price).await;
        assert_eq!(submit(&mut engine).await, !changed, "changed={changed}");
        assert_eq!(fake.posts.lock().unwrap().len(), usize::from(!changed));
        server.abort();
    }
}

#[tokio::test]
async fn malformed_open_price_blocks_without_replacing_durable_order_and_recovers() {
    let db = database();
    let (order, _) = managed_price_fixture();
    db.save_managed_order(&order).unwrap();
    let corrected = Arc::new(AtomicBool::new(false));
    let app = Router::new().route("/fapi/v1/openOrders", get({
        let corrected = corrected.clone();
        move || {
            let corrected = corrected.clone();
            async move {
                Json(json!([{"orderId":123,"clientOrderId":"gb_price_update",
                    "symbol":"SOLUSDC","status":"NEW",
                    "price":if corrected.load(Ordering::SeqCst) {"102.375"} else {"not-a-price"},
                    "origQty":"1.25","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}]))
            }
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut first = engine(db.clone(), &url).await;
    assert!(!first.sync_live_orders().await);
    assert!(first.reconciliation_blocked);
    assert_managed_price(&first, &db, order.price).await;
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    assert_managed_price(&restarted, &db, order.price).await;
    corrected.store(true, Ordering::SeqCst);
    assert!(restarted.sync_live_orders().await);
    assert_managed_price(&restarted, &db, dec!(102.375)).await;
    server.abort();
}

#[tokio::test]
async fn unchanged_open_price_waits_for_active_order_lock_without_losing_readiness() {
    for partial in [false, true] {
        unchanged_price_lock_contention(partial, false, false, true).await;
    }
}

#[tokio::test]
async fn empty_snapshot_temporarily_blocks_and_disabled_global_protection_does_not() {
    unchanged_price_lock_contention(false, false, true, true).await;
    unchanged_price_lock_contention(true, false, false, false).await;
}

#[tokio::test]
async fn cancelled_snapshot_lock_wait_revokes_global_readiness() {
    for (partial, empty) in [(true, false), (false, false), (false, true)] {
        unchanged_price_lock_contention(partial, true, empty, true).await;
    }
}

async fn unchanged_price_lock_contention(partial: bool, cancel: bool, empty: bool, enabled: bool) {
    let db = database();
    let (mut order, mut response) = managed_price_fixture();
    if partial {
        order.quantity = dec!(0.75);
        order.amount_usdc = order.price * order.quantity;
        order.status = OrderStatus::PartiallyFilled;
        response.status = "PARTIALLY_FILLED".into();
        response.executed_qty = dec!(0.5);
    }
    if !empty {
        db.save_managed_order(&order).unwrap();
    }
    let requested = Arc::new(tokio::sync::Notify::new());
    let release_response = Arc::new(tokio::sync::Notify::new());
    let app = Router::new().route(
        "/fapi/v1/openOrders",
        get({
            let requested = requested.clone();
            let release_response = release_response.clone();
            move || {
                let requested = requested.clone();
                let release_response = release_response.clone();
                let response = response.clone();
                async move {
                    requested.notify_one();
                    release_response.notified().await;
                    Json(if empty { Vec::new() } else { vec![response] })
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let second = engine(database(), &url).await;
    second.state.config.write().await.exchange.symbol = "BTCUSDC".into();
    second.state.ticker.write().await.symbol = "BTCUSDC".into();
    let mut engine = engine(db.clone(), &url).await;
    let state = engine.state.clone();
    for pair in [&state, &second.state] {
        pair.config.write().await.global_max_position_notional = enabled.then_some(dec!(10000));
        pair.position.write().await.update_time = Some(Utc::now());
        pair.global_position_initialized
            .store(true, Ordering::SeqCst);
        pair.global_position_ready.store(true, Ordering::SeqCst);
    }
    let global =
        crate::server::state::GlobalPositionLimit::new(Arc::new(tokio::sync::RwLock::new(vec![
            state.clone(),
            second.state.clone(),
        ])));
    global.membership_ready.store(true, Ordering::SeqCst);
    for pair in [&state, &second.state] {
        let _ = pair.global_position_limit.set(Arc::downgrade(&global));
    }
    assert!(!global.check().await);
    assert!(second.state.try_order_submission().is_some());
    let mut sync = tokio::spawn(async move {
        let result = engine.sync_live_orders().await;
        (engine, result)
    });
    requested.notified().await;
    let active_lock = state.active_orders.write().await;
    release_response.notify_one();
    assert!(tokio::time::timeout(Duration::from_millis(100), &mut sync)
        .await
        .is_err());
    assert!(state.global_position_ready.load(Ordering::SeqCst));
    assert_eq!(*state.status.read().await, BotStatus::Running);
    assert!(second.state.global_position_ready.load(Ordering::SeqCst));
    assert_eq!(*second.state.status.read().await, BotStatus::Running);
    if enabled {
        assert!(second.state.try_order_submission().is_none());
    } else {
        assert!(second.state.try_order_submission().is_some());
    }
    if cancel {
        sync.abort();
        assert!(sync.await.err().unwrap().is_cancelled());
        assert!(!state.global_position_ready.load(Ordering::SeqCst));
        assert!(second.state.global_position_ready.load(Ordering::SeqCst));
        assert!(second.state.try_order_submission().is_none());
        drop(active_lock);
        assert!(global.check().await);
        assert!(second.state.try_order_submission().is_none());
        server.abort();
        return;
    }
    drop(active_lock);
    let (engine, success) = sync.await.unwrap();
    assert!(success);
    assert!(!engine.reconciliation_blocked);
    assert!(state.global_position_ready.load(Ordering::SeqCst));
    assert_eq!(*state.status.read().await, BotStatus::Running);
    assert!(second.state.global_position_ready.load(Ordering::SeqCst));
    assert_eq!(*second.state.status.read().await, BotStatus::Running);
    assert!(second.state.try_order_submission().is_some());
    let active = state.active_orders.read().await;
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    if empty {
        assert!(active.is_empty());
        assert!(saved.is_empty());
    } else {
        let retained = &active[&order.client_order_id];
        assert_eq!(retained.price, order.price);
        assert_eq!(retained.quantity, order.quantity);
        assert_eq!(retained.amount_usdc, order.amount_usdc);
        assert_eq!(saved[0].price, order.price);
        assert_eq!(saved[0].quantity, order.quantity);
        assert_eq!(saved[0].amount_usdc, order.amount_usdc);
    }
    drop(active);
    server.abort();
}

#[tokio::test]
async fn risky_snapshots_publish_pending_before_waiting_on_active_lock_and_survive_enablement() {
    for invalid in [false, true] {
        for initially_enabled in [false, true] {
            let db = database();
            let (order, mut response) = managed_price_fixture();
            db.save_managed_order(&order).unwrap();
            if invalid {
                response.price = Decimal::ZERO;
            } else {
                response.status = "PARTIALLY_FILLED".into();
                response.executed_qty = dec!(0.5);
            }
            let requested = Arc::new(tokio::sync::Notify::new());
            let release_response = Arc::new(tokio::sync::Notify::new());
            let app = Router::new().route(
                "/fapi/v1/openOrders",
                get({
                    let requested = requested.clone();
                    let release_response = release_response.clone();
                    move || {
                        let requested = requested.clone();
                        let release_response = release_response.clone();
                        let response = response.clone();
                        async move {
                            requested.notify_one();
                            release_response.notified().await;
                            Json(vec![response])
                        }
                    }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let mut first = engine(db, &url).await;
            let state = first.state.clone();
            let second = engine(database(), &url).await;
            let global = test_pair_global_limit(&first, &second).await;
            if !initially_enabled {
                for pair in [&state, &second.state] {
                    pair.config.write().await.global_max_position_notional = None;
                }
                assert!(!global.check().await);
            }
            let mut sync = tokio::spawn(async move { first.sync_live_orders().await });
            tokio::time::timeout(Duration::from_secs(5), requested.notified())
                .await
                .unwrap();
            let active_lock = state.active_orders.write().await;
            release_response.notify_one();
            assert!(tokio::time::timeout(Duration::from_millis(100), &mut sync)
                .await
                .is_err());
            if initially_enabled {
                assert!(
                    second.state.try_order_submission().is_none(),
                    "invalid={invalid}"
                );
            } else {
                assert!(second.state.try_order_submission().is_some());
                for pair in [&state, &second.state] {
                    pair.config.write().await.global_max_position_notional = Some(dec!(10000));
                }
                let _ = global.check().await;
                assert!(
                    second.state.try_order_submission().is_none(),
                    "invalid={invalid}"
                );
            }
            sync.abort();
            assert!(sync.await.err().unwrap().is_cancelled());
            assert!(!state.global_position_ready.load(Ordering::SeqCst));
            assert!(second.state.try_order_submission().is_none());
            drop(active_lock);
            server.abort();
        }
    }
}

#[tokio::test]
async fn valid_managed_price_update_preserves_other_pair_global_readiness() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let (order, mut response) = managed_price_fixture();
    db.save_managed_order(&order).unwrap();
    response.price = dec!(102.375);
    fake.orders
        .lock()
        .unwrap()
        .insert(order.client_order_id.clone(), response);
    let mut first = engine(db.clone(), &url).await;
    let second = engine(database(), &url).await;
    second.state.config.write().await.exchange.symbol = "BTCUSDC".into();
    second.state.ticker.write().await.symbol = "BTCUSDC".into();
    for state in [&first.state, &second.state] {
        state.config.write().await.global_max_position_notional = Some(dec!(10000));
        state.position.write().await.update_time = Some(Utc::now());
        state
            .global_position_initialized
            .store(true, Ordering::SeqCst);
        state.global_position_ready.store(true, Ordering::SeqCst);
    }
    let global =
        crate::server::state::GlobalPositionLimit::new(Arc::new(tokio::sync::RwLock::new(vec![
            first.state.clone(),
            second.state.clone(),
        ])));
    global.membership_ready.store(true, Ordering::SeqCst);
    for state in [&first.state, &second.state] {
        let _ = state.global_position_limit.set(Arc::downgrade(&global));
    }
    assert!(!global.check().await);
    assert!(first.sync_live_orders().await);
    assert_managed_price(&first, &db, dec!(102.375)).await;
    for state in [&first.state, &second.state] {
        assert!(state.global_position_ready.load(Ordering::SeqCst));
        assert_eq!(*state.status.read().await, BotStatus::Running);
    }
    assert!(!global.check().await);
    server.abort();
}

#[tokio::test]
async fn invalid_price_snapshot_retains_other_new_order_through_disappearance_and_restart() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let (_, mut valid) = managed_price_fixture();
    valid.client_order_id = "gb_snapshot_valid_price".into();
    valid.order_id = 234;
    valid.price = dec!(99.75);
    valid.orig_qty = dec!(2.5);
    let mut invalid = valid.clone();
    invalid.client_order_id = "gb_snapshot_zero_price".into();
    invalid.order_id = 345;
    invalid.price = Decimal::ZERO;
    invalid.orig_qty = dec!(1.25);
    {
        let mut orders = fake.orders.lock().unwrap();
        orders.insert(valid.client_order_id.clone(), valid.clone());
        orders.insert(invalid.client_order_id.clone(), invalid.clone());
    }
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    assert!(!first.sync_live_orders().await);
    assert!(first.reconciliation_blocked);
    for response in [&valid, &invalid] {
        let saved = db.load_managed_orders("SOLUSDC").unwrap();
        let snapshots = db
            .load_initial_order_responses(account, "SOLUSDC", &response.client_order_id)
            .unwrap();
        let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
        assert!(
            saved
                .iter()
                .any(|order| order.client_order_id == response.client_order_id
                    && order.order_id == Some(response.order_id)
                    && order.quantity >= response.orig_qty)
                || snapshots
                    .iter()
                    .any(|found| found.order_id == response.order_id
                        && found.orig_qty == response.orig_qty
                        && found.price == response.price)
                || observations
                    .iter()
                    .any(
                        |(expected, found)| expected.client_order_id == response.client_order_id
                            && expected.quantity >= response.orig_qty
                            && found.order_id == response.order_id
                    )
        );
        let active = first.state.active_orders.read().await;
        assert!(active[&response.client_order_id].quantity >= response.orig_qty);
        assert_eq!(
            active[&response.client_order_id].order_id,
            Some(response.order_id)
        );
    }
    drop(first);
    {
        let mut orders = fake.orders.lock().unwrap();
        let filled = orders.get_mut(&valid.client_order_id).unwrap();
        filled.status = "FILLED".into();
        filled.executed_qty = filled.orig_qty;
        filled.avg_price = Some(filled.price);
    }
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    assert!(restarted.reconciliation_blocked);
    let active = restarted.state.active_orders.read().await;
    for response in [&valid, &invalid] {
        let retained = &active[&response.client_order_id];
        assert_eq!(retained.order_id, Some(response.order_id));
        assert!(retained.quantity >= response.orig_qty);
    }
    drop(active);
    server.abort();
}

#[tokio::test]
async fn mixed_zero_snapshot_recovers_when_verified_order_price_changes() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let (_, mut valid) = managed_price_fixture();
    valid.client_order_id = "gb_mixed_repriced".into();
    valid.order_id = 234;
    valid.price = dec!(100);
    let mut invalid = valid.clone();
    invalid.client_order_id = "gb_mixed_corrected".into();
    invalid.order_id = 345;
    invalid.price = Decimal::ZERO;
    {
        let mut orders = fake.orders.lock().unwrap();
        orders.insert(valid.client_order_id.clone(), valid.clone());
        orders.insert(invalid.client_order_id.clone(), invalid.clone());
    }
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    assert!(!first.sync_live_orders().await);
    {
        let mut orders = fake.orders.lock().unwrap();
        orders.get_mut(&valid.client_order_id).unwrap().price = dec!(101);
        orders.get_mut(&invalid.client_order_id).unwrap().price = dec!(90);
    }
    assert!(first.sync_live_orders().await);
    assert!(!first.reconciliation_blocked);
    let active = first.state.active_orders.read().await;
    assert_eq!(active[&valid.client_order_id].price, dec!(101));
    assert_eq!(
        active[&valid.client_order_id].amount_usdc,
        dec!(101) * valid.orig_qty
    );
    assert_eq!(active[&invalid.client_order_id].price, dec!(90));
    drop(active);
    assert!(db
        .load_terminal_order_disagreements(account, "SOLUSDC")
        .unwrap()
        .is_empty());
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(restarted.sync_live_orders().await);
    assert!(!restarted.reconciliation_blocked);
    assert!(db
        .load_terminal_order_disagreements(account, "SOLUSDC")
        .unwrap()
        .is_empty());
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    let repriced = saved
        .iter()
        .find(|order| order.client_order_id == valid.client_order_id)
        .unwrap();
    assert_eq!(repriced.price, dec!(101));
    assert_eq!(repriced.amount_usdc, dec!(101) * valid.orig_qty);
    server.abort();
}

#[tokio::test]
async fn mixed_invalid_snapshot_partial_average_correction_survives_restart() {
    for case in [
        "partial",
        "terminal",
        "average_conflict",
        "quantity_regression",
        "time_regression",
    ] {
        let fake = Exchange::default();
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let (_, mut partial) = managed_price_fixture();
        partial.client_order_id = "gb_mixed_partial_average".into();
        partial.order_id = 234;
        partial.status = "PARTIALLY_FILLED".into();
        partial.price = dec!(100);
        partial.orig_qty = dec!(2);
        partial.executed_qty = dec!(0.5);
        partial.avg_price = Some(if case == "average_conflict" {
            dec!(97.5)
        } else {
            Decimal::ZERO
        });
        partial.update_time = Some(1000);
        let mut invalid = partial.clone();
        invalid.client_order_id = "gb_mixed_zero_other".into();
        invalid.order_id = 345;
        invalid.status = "NEW".into();
        invalid.price = Decimal::ZERO;
        invalid.orig_qty = dec!(1.25);
        invalid.executed_qty = Decimal::ZERO;
        invalid.avg_price = None;
        {
            let mut orders = fake.orders.lock().unwrap();
            orders.insert(partial.client_order_id.clone(), partial.clone());
            orders.insert(invalid.client_order_id.clone(), invalid.clone());
        }
        let mut first = engine(db.clone(), &url).await;
        let account = first.client.account_identity_key();
        assert!(!first.sync_live_orders().await, "{case}");
        assert!(db
            .load_initial_order_responses(account, "SOLUSDC", &partial.client_order_id)
            .unwrap()
            .iter()
            .any(|found| found.status == "PARTIALLY_FILLED"
                && found.executed_qty == dec!(0.5)
                && found.avg_price == partial.avg_price));
        drop(first);
        {
            let mut orders = fake.orders.lock().unwrap();
            orders.get_mut(&invalid.client_order_id).unwrap().price = dec!(90);
            let corrected = orders.get_mut(&partial.client_order_id).unwrap();
            corrected.avg_price = Some(dec!(98.5));
            corrected.update_time = Some(if case == "time_regression" { 500 } else { 2000 });
            if case == "quantity_regression" {
                corrected.executed_qty = dec!(0.25);
            }
            if case == "terminal" {
                corrected.status = "CANCELED".into();
            }
        }
        let mut restarted = engine(db.clone(), &url).await;
        let corrected = matches!(case, "partial" | "terminal");
        assert_eq!(restarted.sync_live_orders().await, corrected, "{case}");
        let disagreements = db
            .load_terminal_order_disagreements(account, "SOLUSDC")
            .unwrap();
        if corrected {
            assert!(!restarted.reconciliation_blocked, "{case}");
            assert!(disagreements.is_empty(), "{case}");
        } else {
            assert!(restarted.reconciliation_blocked, "{case}");
            assert!(
                disagreements
                    .iter()
                    .any(|(id, _, _)| id == &partial.client_order_id),
                "{case}"
            );
        }
        drop(restarted);
        let mut again = engine(db.clone(), &url).await;
        assert_eq!(again.sync_live_orders().await, corrected, "{case}");
        assert_eq!(
            db.load_terminal_order_disagreements(account, "SOLUSDC")
                .unwrap()
                .is_empty(),
            corrected,
            "{case}"
        );
        server.abort();
    }
}

#[tokio::test]
async fn filled_zero_price_placeholder_without_average_is_not_booked_at_zero() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let (_, mut response) = managed_price_fixture();
    response.client_order_id = "gb_zero_price_fill".into();
    response.price = Decimal::ZERO;
    response.avg_price = None;
    fake.orders
        .lock()
        .unwrap()
        .insert(response.client_order_id.clone(), response.clone());
    let mut first = engine(db.clone(), &url).await;
    assert!(!first.sync_live_orders().await);
    {
        let mut orders = fake.orders.lock().unwrap();
        let filled = orders.get_mut(&response.client_order_id).unwrap();
        filled.status = "FILLED".into();
        filled.executed_qty = filled.orig_qty;
    }
    assert!(!first.sync_live_orders().await);
    assert!(first.reconciliation_blocked);
    assert!(db.get_recent_trades(10).unwrap().is_empty());
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    assert!(restarted.reconciliation_blocked);
    assert!(db.get_recent_trades(10).unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn terminal_missing_average_never_reprices_prior_partial_fill_after_restart() {
    for terminal_average in [None, Some(Decimal::ZERO), Some(dec!(-1))] {
        for terminal_quantity in [dec!(0.5), dec!(0.75)] {
            let fake = Exchange::default();
            let (url, server) = exchange_server(fake.clone()).await;
            let db = database();
            let (mut order, mut partial) = managed_price_fixture();
            order.price = dec!(100);
            order.quantity = dec!(1.5);
            order.amount_usdc = dec!(150);
            order.status = OrderStatus::PartiallyFilled;
            db.save_managed_order(&order).unwrap();
            partial.status = "PARTIALLY_FILLED".into();
            partial.price = dec!(100);
            partial.orig_qty = dec!(2);
            partial.executed_qty = dec!(0.5);
            partial.avg_price = Some(dec!(98));
            partial.update_time = Some(1000);
            let mut first = engine(db.clone(), &url).await;
            let account = first.client.account_identity_key();
            db.save_identity_observation(
                account,
                &order,
                &crate::types::ObservedOrderIdentity {
                    client_order_id: order.client_order_id.clone(),
                    symbol: order.symbol.clone(),
                    side: "BUY".into(),
                    order_id: partial.order_id,
                },
            )
            .unwrap();
            assert!(
                first
                    .record_initial_order_response(
                        account,
                        "SOLUSDC",
                        &order.client_order_id,
                        &partial
                    )
                    .await
            );
            drop(first);
            let mut terminal = partial.clone();
            terminal.status = "CANCELED".into();
            terminal.executed_qty = terminal_quantity;
            terminal.avg_price = terminal_average;
            terminal.update_time = Some(2000);
            fake.orders
                .lock()
                .unwrap()
                .insert(order.client_order_id.clone(), terminal);
            let mut restarted = engine(db.clone(), &url).await;
            let success = restarted.sync_live_orders().await;
            let trades = db.get_recent_trades(10).unwrap();
            if success {
                assert_eq!(trades.len(), 1);
                assert_eq!(trades[0].quantity, terminal_quantity);
                if terminal_quantity == dec!(0.5) {
                    assert_eq!(trades[0].price, dec!(98));
                } else {
                    assert_ne!(trades[0].price, dec!(98));
                }
            } else {
                assert!(restarted.reconciliation_blocked);
                assert!(trades.is_empty());
            }
            drop(restarted);
            let mut again = engine(db.clone(), &url).await;
            assert_eq!(again.sync_live_orders().await, success);
            assert_eq!(
                db.get_recent_trades(10).unwrap().len(),
                usize::from(success)
            );
            server.abort();
        }
    }
}

#[tokio::test]
async fn terminal_fill_requires_exchange_price_when_average_is_invalid() {
    for terminal_price in [dec!(98.25), Decimal::ZERO] {
        for average in [None, Some(Decimal::ZERO)] {
            let fake = Exchange::default();
            let (url, server) = exchange_server(fake.clone()).await;
            let db = database();
            let (mut order, mut response) = managed_price_fixture();
            order.price = dec!(120.375);
            order.amount_usdc = order.price * order.quantity;
            db.save_managed_order(&order).unwrap();
            response.status = "FILLED".into();
            response.price = terminal_price;
            response.avg_price = average;
            response.executed_qty = response.orig_qty;
            fake.orders
                .lock()
                .unwrap()
                .insert(order.client_order_id.clone(), response);
            let mut engine = engine(db.clone(), &url).await;
            let success = engine.sync_live_orders().await;
            let trades = db.get_recent_trades(10).unwrap();
            if terminal_price > Decimal::ZERO {
                assert!(success);
                assert_eq!(trades.len(), 1);
                assert_eq!(trades[0].price, terminal_price);
                assert_eq!(trades[0].amount_usdc, terminal_price * order.quantity);
            } else {
                assert!(!success);
                assert!(engine.reconciliation_blocked);
                assert!(trades.is_empty());
                assert_eq!(
                    db.load_managed_orders("SOLUSDC").unwrap()[0].price,
                    order.price
                );
            }
            server.abort();
        }
    }
}

#[tokio::test]
async fn terminal_zero_price_with_positive_average_books_at_exchange_average() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let (order, mut response) = managed_price_fixture();
    db.save_managed_order(&order).unwrap();
    response.status = "FILLED".into();
    response.price = Decimal::ZERO;
    response.avg_price = Some(dec!(98.625));
    response.executed_qty = response.orig_qty;
    fake.orders
        .lock()
        .unwrap()
        .insert(order.client_order_id.clone(), response);
    let mut first = engine(db.clone(), &url).await;
    assert!(first.sync_live_orders().await);
    let trades = db.get_recent_trades(10).unwrap();
    assert_eq!(trades.len(), 1);
    assert_eq!(trades[0].price, dec!(98.625));
    assert_eq!(trades[0].amount_usdc, dec!(98.625) * order.quantity);
    server.abort();
}

#[tokio::test]
async fn terminal_average_correction_after_restart_preserves_real_disagreements() {
    for initial_average in [None, Some(Decimal::ZERO), Some(dec!(97.5))] {
        let fake = Exchange::default();
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let (order, mut response) = managed_price_fixture();
        db.save_managed_order(&order).unwrap();
        response.status = "FILLED".into();
        response.price = Decimal::ZERO;
        response.executed_qty = response.orig_qty;
        response.avg_price = initial_average;
        let mut first = engine(db.clone(), &url).await;
        let account = first.client.account_identity_key();
        db.save_identity_observation(
            account,
            &order,
            &crate::types::ObservedOrderIdentity {
                client_order_id: order.client_order_id.clone(),
                symbol: order.symbol.clone(),
                side: "BUY".into(),
                order_id: response.order_id,
            },
        )
        .unwrap();
        assert!(
            first
                .record_initial_order_response(
                    account,
                    "SOLUSDC",
                    &order.client_order_id,
                    &response
                )
                .await
        );
        let initial = db
            .load_initial_order_responses(account, "SOLUSDC", &order.client_order_id)
            .unwrap();
        assert!(initial.iter().any(|found| found.status == "FILLED"
            && found.price == Decimal::ZERO
            && found.executed_qty == order.quantity
            && found.avg_price == initial_average));
        drop(first);
        response.avg_price = Some(dec!(98.625));
        fake.orders
            .lock()
            .unwrap()
            .insert(order.client_order_id.clone(), response);
        let mut restarted = engine(db.clone(), &url).await;
        let corrected = initial_average.is_none_or(|price| price <= Decimal::ZERO);
        assert_eq!(restarted.sync_live_orders().await, corrected);
        let disagreements = db
            .load_terminal_order_disagreements(account, "SOLUSDC")
            .unwrap();
        let trades = db.get_recent_trades(10).unwrap();
        if corrected {
            assert!(disagreements.is_empty());
            assert_eq!(trades.len(), 1);
            assert_eq!(trades[0].price, dec!(98.625));
            assert_eq!(trades[0].amount_usdc, dec!(98.625) * order.quantity);
        } else {
            assert!(restarted.reconciliation_blocked);
            assert!(trades.is_empty());
            assert!(disagreements
                .iter()
                .any(|(id, a, b)| id == &order.client_order_id
                    && a.avg_price == Some(dec!(97.5))
                    && b.avg_price == Some(dec!(98.625))));
        }
        drop(restarted);
        let mut again = engine(db.clone(), &url).await;
        assert_eq!(again.sync_live_orders().await, corrected);
        assert_eq!(
            db.get_recent_trades(10).unwrap().len(),
            usize::from(corrected)
        );
        assert_eq!(
            db.load_terminal_order_disagreements(account, "SOLUSDC")
                .unwrap()
                .is_empty(),
            corrected
        );
        server.abort();
    }
}

#[tokio::test]
async fn terminal_price_correction_after_restart_rejects_time_regression() {
    for later_time in [2000, 500] {
        let fake = Exchange::default();
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let (order, mut response) = managed_price_fixture();
        db.save_managed_order(&order).unwrap();
        response.status = "FILLED".into();
        response.price = Decimal::ZERO;
        response.avg_price = None;
        response.executed_qty = response.orig_qty;
        response.update_time = Some(1000);
        let mut first = engine(db.clone(), &url).await;
        let account = first.client.account_identity_key();
        db.save_identity_observation(
            account,
            &order,
            &crate::types::ObservedOrderIdentity {
                client_order_id: order.client_order_id.clone(),
                symbol: order.symbol.clone(),
                side: "BUY".into(),
                order_id: response.order_id,
            },
        )
        .unwrap();
        assert!(
            first
                .record_initial_order_response(
                    account,
                    "SOLUSDC",
                    &order.client_order_id,
                    &response
                )
                .await
        );
        drop(first);
        response.price = dec!(98);
        response.update_time = Some(later_time);
        fake.orders
            .lock()
            .unwrap()
            .insert(order.client_order_id.clone(), response);
        let mut restarted = engine(db.clone(), &url).await;
        let corrected = later_time > 1000;
        assert_eq!(restarted.sync_live_orders().await, corrected);
        let trades = db.get_recent_trades(10).unwrap();
        let disagreements = db
            .load_terminal_order_disagreements(account, "SOLUSDC")
            .unwrap();
        if corrected {
            assert_eq!(trades.len(), 1);
            assert_eq!(trades[0].price, dec!(98));
            assert_eq!(trades[0].amount_usdc, dec!(98) * order.quantity);
            assert!(disagreements.is_empty());
        } else {
            assert!(restarted.reconciliation_blocked);
            assert!(trades.is_empty());
            assert!(disagreements
                .iter()
                .any(|(id, first, later)| id == &order.client_order_id
                    && first.price == Decimal::ZERO
                    && first.update_time == Some(1000)
                    && later.price == dec!(98)
                    && later.update_time == Some(500)));
        }
        drop(restarted);
        let mut again = engine(db.clone(), &url).await;
        assert_eq!(again.sync_live_orders().await, corrected);
        assert_eq!(
            db.get_recent_trades(10).unwrap().len(),
            usize::from(corrected)
        );
        assert_eq!(
            db.load_terminal_order_disagreements(account, "SOLUSDC")
                .unwrap()
                .is_empty(),
            corrected
        );
        server.abort();
    }
}

#[tokio::test]
async fn terminal_price_and_average_can_be_completed_in_stages_across_restarts() {
    for average_first in [false, true] {
        let fake = Exchange::default();
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let (order, mut response) = managed_price_fixture();
        db.save_managed_order(&order).unwrap();
        response.status = "FILLED".into();
        response.executed_qty = response.orig_qty;
        response.price = Decimal::ZERO;
        response.avg_price = None;
        response.update_time = Some(1000);
        let mut first = engine(db.clone(), &url).await;
        let account = first.client.account_identity_key();
        db.save_identity_observation(
            account,
            &order,
            &crate::types::ObservedOrderIdentity {
                client_order_id: order.client_order_id.clone(),
                symbol: order.symbol.clone(),
                side: "BUY".into(),
                order_id: response.order_id,
            },
        )
        .unwrap();
        assert!(
            first
                .record_initial_order_response(
                    account,
                    "SOLUSDC",
                    &order.client_order_id,
                    &response
                )
                .await
        );
        drop(first);
        if average_first {
            response.avg_price = Some(dec!(98));
        } else {
            response.price = dec!(100);
        }
        response.update_time = Some(2000);
        let mut second = engine(db.clone(), &url).await;
        assert!(
            second
                .record_initial_order_response(
                    account,
                    "SOLUSDC",
                    &order.client_order_id,
                    &response
                )
                .await
        );
        drop(second);
        let evidence = db
            .load_initial_order_responses(account, "SOLUSDC", &order.client_order_id)
            .unwrap();
        assert!(evidence
            .iter()
            .any(|found| found.price == Decimal::ZERO && found.update_time == Some(1000)));
        assert!(evidence.iter().any(|found| found.update_time == Some(2000)
            && if average_first {
                found.price == Decimal::ZERO && found.avg_price == Some(dec!(98))
            } else {
                found.price == dec!(100) && found.avg_price.is_none()
            }));
        response.price = dec!(100);
        response.avg_price = Some(dec!(98));
        response.update_time = Some(3000);
        fake.orders
            .lock()
            .unwrap()
            .insert(order.client_order_id.clone(), response);
        let mut recovered = engine(db.clone(), &url).await;
        assert!(recovered.sync_live_orders().await);
        let trades = db.get_recent_trades(10).unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].price, dec!(98));
        assert_eq!(trades[0].amount_usdc, dec!(98) * order.quantity);
        assert!(db
            .load_terminal_order_disagreements(account, "SOLUSDC")
            .unwrap()
            .is_empty());
        drop(recovered);
        let mut again = engine(db.clone(), &url).await;
        assert!(again.sync_live_orders().await);
        assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
        server.abort();
    }
}

#[test]
fn terminal_fill_evidence_completion_comparison_table() {
    let cases = [
        (
            "complete price",
            dec!(0),
            None,
            dec!(100),
            None,
            Some(1000),
            Some(2000),
            false,
        ),
        (
            "complete average",
            dec!(0),
            None,
            dec!(0),
            Some(dec!(98)),
            Some(1000),
            Some(2000),
            false,
        ),
        (
            "complete both",
            dec!(0),
            None,
            dec!(100),
            Some(dec!(98)),
            Some(1000),
            Some(2000),
            false,
        ),
        (
            "price after average",
            dec!(0),
            Some(dec!(98)),
            dec!(100),
            Some(dec!(98)),
            Some(1000),
            Some(2000),
            false,
        ),
        (
            "average after price",
            dec!(100),
            None,
            dec!(100),
            Some(dec!(98)),
            Some(1000),
            Some(2000),
            false,
        ),
        (
            "zero average completed",
            dec!(100),
            Some(dec!(0)),
            dec!(100),
            Some(dec!(98)),
            Some(1000),
            Some(2000),
            false,
        ),
        (
            "positive price changed",
            dec!(100),
            Some(dec!(98)),
            dec!(101),
            Some(dec!(98)),
            Some(1000),
            Some(2000),
            true,
        ),
        (
            "positive average changed",
            dec!(100),
            Some(dec!(98)),
            dec!(100),
            Some(dec!(99)),
            Some(1000),
            Some(2000),
            true,
        ),
        (
            "price and average changed",
            dec!(100),
            Some(dec!(98)),
            dec!(101),
            Some(dec!(99)),
            Some(1000),
            Some(2000),
            true,
        ),
        (
            "price lost",
            dec!(100),
            Some(dec!(98)),
            dec!(0),
            Some(dec!(98)),
            Some(1000),
            Some(2000),
            true,
        ),
        (
            "average lost",
            dec!(100),
            Some(dec!(98)),
            dec!(100),
            None,
            Some(1000),
            Some(2000),
            true,
        ),
        (
            "same timestamp",
            dec!(0),
            None,
            dec!(100),
            Some(dec!(98)),
            Some(1000),
            Some(1000),
            false,
        ),
        (
            "time reversed",
            dec!(0),
            None,
            dec!(100),
            Some(dec!(98)),
            Some(1000),
            Some(500),
            true,
        ),
        (
            "known time lost",
            dec!(0),
            None,
            dec!(100),
            Some(dec!(98)),
            Some(1000),
            None,
            true,
        ),
        (
            "time supplied",
            dec!(0),
            None,
            dec!(100),
            Some(dec!(98)),
            None,
            Some(2000),
            false,
        ),
        (
            "both times missing",
            dec!(0),
            None,
            dec!(100),
            Some(dec!(98)),
            None,
            None,
            false,
        ),
    ];
    for (name, old_price, old_avg, new_price, new_avg, old_time, new_time, expected) in cases {
        let (_, mut old) = managed_price_fixture();
        old.status = "FILLED".into();
        old.executed_qty = old.orig_qty;
        old.price = old_price;
        old.avg_price = old_avg;
        old.update_time = old_time;
        let mut new = old.clone();
        new.price = new_price;
        new.avg_price = new_avg;
        new.update_time = new_time;
        assert_eq!(
            super::reconciliation::terminal_responses_disagree(&old, &new),
            expected,
            "{name}"
        );
    }
}

#[tokio::test]
async fn missing_terminal_correction_time_cannot_discard_reliable_ordering() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let (order, mut response) = managed_price_fixture();
    db.save_managed_order(&order).unwrap();
    response.status = "FILLED".into();
    response.executed_qty = response.orig_qty;
    response.price = Decimal::ZERO;
    response.avg_price = None;
    response.update_time = Some(1000);
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    db.save_identity_observation(
        account,
        &order,
        &crate::types::ObservedOrderIdentity {
            client_order_id: order.client_order_id.clone(),
            symbol: order.symbol.clone(),
            side: "BUY".into(),
            order_id: response.order_id,
        },
    )
    .unwrap();
    assert!(
        first
            .record_initial_order_response(account, "SOLUSDC", &order.client_order_id, &response)
            .await
    );
    drop(first);
    response.price = dec!(98);
    response.update_time = None;
    fake.orders
        .lock()
        .unwrap()
        .insert(order.client_order_id.clone(), response);
    let mut restarted = engine(db.clone(), &url).await;
    let success = restarted.sync_live_orders().await;
    let trades = db.get_recent_trades(10).unwrap();
    let evidence = db
        .load_initial_order_responses(account, "SOLUSDC", &order.client_order_id)
        .unwrap();
    if success {
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].price, dec!(98));
        assert!(evidence.iter().any(|found| found.update_time == Some(1000)));
    } else {
        assert!(restarted.reconciliation_blocked);
        assert!(trades.is_empty());
        assert!(
            evidence.iter().any(|found| found.update_time == Some(1000))
                || !db
                    .load_terminal_order_disagreements(account, "SOLUSDC")
                    .unwrap()
                    .is_empty()
        );
    }
    drop(restarted);
    let mut again = engine(db.clone(), &url).await;
    assert_eq!(again.sync_live_orders().await, success);
    assert_eq!(
        db.get_recent_trades(10).unwrap().len(),
        usize::from(success)
    );
    server.abort();
}

async fn test_pair_global_limit(
    first: &GridTradingEngine,
    second: &GridTradingEngine,
) -> Arc<crate::server::state::GlobalPositionLimit> {
    second.state.config.write().await.exchange.symbol = "BTCUSDC".into();
    second.state.ticker.write().await.symbol = "BTCUSDC".into();
    for state in [&first.state, &second.state] {
        state.config.write().await.global_max_position_notional = Some(dec!(10000));
        state.position.write().await.update_time = Some(Utc::now());
        state
            .global_position_initialized
            .store(true, Ordering::SeqCst);
        state.global_position_ready.store(true, Ordering::SeqCst);
    }
    let global =
        crate::server::state::GlobalPositionLimit::new(Arc::new(tokio::sync::RwLock::new(vec![
            first.state.clone(),
            second.state.clone(),
        ])));
    global.membership_ready.store(true, Ordering::SeqCst);
    for state in [&first.state, &second.state] {
        let _ = state.global_position_limit.set(Arc::downgrade(&global));
    }
    assert!(!global.check().await);
    global
}

#[tokio::test]
async fn missing_order_blocks_other_pair_during_lookup_and_position_refresh() {
    let fake = Exchange::default();
    fake.block_lookup.store(true, Ordering::SeqCst);
    fake.block_position.store(true, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let (order, mut response) = managed_price_fixture();
    db.save_managed_order(&order).unwrap();
    response.status = "FILLED".into();
    response.executed_qty = response.orig_qty;
    response.avg_price = Some(response.price);
    fake.orders
        .lock()
        .unwrap()
        .insert(order.client_order_id.clone(), response);
    let mut first = engine(db, &url).await;
    let second = engine(database(), &url).await;
    let _global = test_pair_global_limit(&first, &second).await;
    let sync = tokio::spawn(async move { first.sync_live_orders().await });
    tokio::time::timeout(Duration::from_secs(5), fake.lookup_started.notified())
        .await
        .unwrap();
    assert!(second.state.try_order_submission().is_none());
    fake.block_lookup.store(false, Ordering::SeqCst);
    fake.release_lookup.notify_one();
    tokio::time::timeout(Duration::from_secs(5), fake.position_started.notified())
        .await
        .unwrap();
    assert!(second.state.try_order_submission().is_none());
    fake.block_position.store(false, Ordering::SeqCst);
    fake.release_position.notify_one();
    assert!(sync.await.unwrap());
    assert!(second.state.try_order_submission().is_some());
    server.abort();
}

#[tokio::test]
async fn terminal_intent_lookup_blocks_other_pair_until_risk_refresh() {
    for remainder in [false, true] {
        let fake = Exchange::default();
        fake.block_lookup.store(true, Ordering::SeqCst);
        fake.block_position.store(true, Ordering::SeqCst);
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let (mut target, mut response) = managed_price_fixture();
        target.client_order_id = "gb_gate_intent".into();
        target.order_id = Some(123);
        target.side = OrderSide::Sell;
        target.price = dec!(101);
        target.amount_usdc = target.price * target.quantity;
        target.paired_client_order_id = Some("gb_gate_parent".into());
        response.client_order_id = target.client_order_id.clone();
        response.side = "SELL".into();
        response.price = target.price;
        response.status = "FILLED".into();
        response.executed_qty = response.orig_qty;
        response.avg_price = Some(response.price);
        fake.orders
            .lock()
            .unwrap()
            .insert(target.client_order_id.clone(), response);
        if remainder {
            target.purpose = OrderPurpose::Remainder;
            db.save_remainder_plan(&RemainderPlan {
                symbol: "SOLUSDC".into(),
                mode: TradingMode::Live,
                sources: Vec::new(),
                target: target.clone(),
                phase: RemainderPhase::Submitting,
            })
            .unwrap();
        } else {
            let trade = TradeRecord {
                trade_id: "grid:gb_gate_parent".into(),
                client_order_id: "gb_gate_parent".into(),
                symbol: "SOLUSDC".into(),
                mode: TradingMode::Live,
                side: OrderSide::Buy,
                price: dec!(100),
                quantity: dec!(1.25),
                amount_usdc: dec!(125),
                realized_pnl: dec!(0),
                commission: dec!(0),
                pnl_verified: true,
                is_maker: true,
                timestamp: Utc::now(),
                note: "fill".into(),
            };
            db.insert_trade_with_recovery(&trade, &GridStats::default(), Some(&target), None)
                .unwrap();
        }
        let mut first = engine(db, &url).await;
        let second = engine(database(), &url).await;
        let _global = test_pair_global_limit(&first, &second).await;
        let handling = tokio::spawn(async move {
            if remainder {
                first.recover_remainder_plan().await.is_ok()
            } else {
                first.drain_pair_intents("SOLUSDC", TradingMode::Live).await
            }
        });
        tokio::time::timeout(Duration::from_secs(5), fake.lookup_started.notified())
            .await
            .unwrap();
        assert!(
            second.state.try_order_submission().is_none(),
            "remainder={remainder}"
        );
        fake.block_lookup.store(false, Ordering::SeqCst);
        fake.release_lookup.notify_one();
        tokio::time::timeout(Duration::from_secs(5), fake.position_started.notified())
            .await
            .unwrap();
        assert!(
            second.state.try_order_submission().is_none(),
            "remainder={remainder}"
        );
        fake.block_position.store(false, Ordering::SeqCst);
        fake.release_position.notify_one();
        assert!(handling.await.unwrap());
        assert!(second.state.try_order_submission().is_some());
        server.abort();
    }
}

#[tokio::test]
async fn cancelled_inflight_post_immediately_blocks_other_pair() {
    let fake = Exchange::default();
    fake.block_submit.store(true, Ordering::SeqCst);
    let (url, server) = exchange_server(fake.clone()).await;
    let mut first = engine(database(), &url).await;
    let state = first.state.clone();
    let second = engine(database(), &url).await;
    let _global = test_pair_global_limit(&first, &second).await;
    let placement = tokio::spawn(async move { submit(&mut first).await });
    tokio::time::timeout(Duration::from_secs(5), fake.submit_started.notified())
        .await
        .unwrap();
    placement.abort();
    assert!(placement.await.err().unwrap().is_cancelled());
    assert!(!state.global_position_ready.load(Ordering::SeqCst));
    assert!(second.state.try_order_submission().is_none());
    fake.release_submit.notify_one();
    server.abort();
}

#[tokio::test]
async fn empty_intent_drain_skips_writer_but_new_intent_waits_for_submission() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let mut first = engine(db.clone(), &url).await;
    let second = engine(database(), &url).await;
    let _global = test_pair_global_limit(&first, &second).await;
    let exchange_guard = second.state.try_exchange_submission().unwrap();
    assert!(tokio::time::timeout(
        Duration::from_secs(2),
        first.drain_pair_intents("SOLUSDC", TradingMode::Live)
    )
    .await
    .unwrap());
    exchange_guard.complete();

    let (mut target, mut response) = managed_price_fixture();
    target.client_order_id = "gb_concurrent_intent".into();
    target.side = OrderSide::Sell;
    target.paired_client_order_id = Some("gb_concurrent_parent".into());
    response.client_order_id = target.client_order_id.clone();
    response.side = "SELL".into();
    response.status = "FILLED".into();
    response.executed_qty = response.orig_qty;
    response.avg_price = Some(response.price);
    fake.orders
        .lock()
        .unwrap()
        .insert(target.client_order_id.clone(), response);
    let trade = TradeRecord {
        trade_id: "grid:gb_concurrent_parent".into(),
        client_order_id: "gb_concurrent_parent".into(),
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Live,
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(1.25),
        amount_usdc: dec!(125),
        realized_pnl: dec!(0),
        commission: dec!(0),
        pnl_verified: true,
        is_maker: true,
        timestamp: Utc::now(),
        note: "fill".into(),
    };
    db.insert_trade_with_recovery(&trade, &GridStats::default(), Some(&target), None)
        .unwrap();
    let exchange_guard = second.state.try_exchange_submission().unwrap();
    let mut drain =
        tokio::spawn(async move { first.drain_pair_intents("SOLUSDC", TradingMode::Live).await });
    assert!(tokio::time::timeout(Duration::from_millis(100), &mut drain)
        .await
        .is_err());
    assert!(second.state.try_order_submission().is_none());
    let mut concurrent = target.clone();
    concurrent.client_order_id = "gb_concurrent_second".into();
    concurrent.order_id = Some(456);
    concurrent.paired_client_order_id = Some("gb_concurrent_parent_two".into());
    let mut response = fake.orders.lock().unwrap()[&target.client_order_id].clone();
    response.client_order_id = concurrent.client_order_id.clone();
    response.order_id = 456;
    fake.orders
        .lock()
        .unwrap()
        .insert(concurrent.client_order_id.clone(), response);
    let mut second_trade = trade.clone();
    second_trade.trade_id = "grid:gb_concurrent_parent_two".into();
    second_trade.client_order_id = "gb_concurrent_parent_two".into();
    db.insert_trade_with_recovery(
        &second_trade,
        &GridStats::default(),
        Some(&concurrent),
        None,
    )
    .unwrap();
    exchange_guard.complete();
    assert!(drain.await.unwrap());
    assert!(second.state.try_order_submission().is_some());
    assert!(db
        .load_pair_intents("SOLUSDC", TradingMode::Live)
        .unwrap()
        .is_empty());
    server.abort();
}

#[tokio::test]
async fn immediate_fill_response_gates_other_pair_before_order_processing() {
    for remainder in [false, true] {
        let fake = Exchange::default();
        fake.fill_on_submit.store(2, Ordering::SeqCst);
        fake.block_submit.store(true, Ordering::SeqCst);
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let mut first = engine(db.clone(), &url).await;
        let second = engine(database(), &url).await;
        let _global = test_pair_global_limit(&first, &second).await;
        if remainder {
            let (mut target, _) = managed_price_fixture();
            target.client_order_id = "gb_remainder_gate".into();
            target.order_id = None;
            target.side = OrderSide::Sell;
            target.price = dec!(101);
            target.quantity = dec!(0.5);
            target.amount_usdc = target.price * target.quantity;
            target.purpose = OrderPurpose::Remainder;
            db.save_remainder_plan(&RemainderPlan {
                symbol: "SOLUSDC".into(),
                mode: TradingMode::Live,
                sources: Vec::new(),
                target,
                phase: RemainderPhase::Submitting,
            })
            .unwrap();
        }
        let state = first.state.clone();
        let mut placement = tokio::spawn(async move {
            if remainder {
                first.recover_remainder_plan().await.is_ok()
            } else {
                submit(&mut first).await
            }
        });
        tokio::time::timeout(Duration::from_secs(5), fake.submit_started.notified())
            .await
            .unwrap();
        let active_lock = state.active_orders.write().await;
        fake.release_submit.notify_one();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut placement)
                .await
                .is_err()
        );
        assert!(
            second.state.try_order_submission().is_none(),
            "remainder={remainder}"
        );
        assert!(second.state.global_position_ready.load(Ordering::SeqCst));
        assert_eq!(*second.state.status.read().await, BotStatus::Running);
        drop(active_lock);
        assert!(placement.await.unwrap());
        assert!(second.state.try_order_submission().is_some());
        server.abort();
    }
}

#[tokio::test]
async fn fill_gate_prevents_send_after_placement_intent_is_persisted() {
    let path = std::env::temp_dir().join(format!("pre-send-gate-{}.db", uuid::Uuid::new_v4()));
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let other = engine(database(), &url).await;
    let mut placing = engine(db.clone(), &url).await;
    let state = placing.state.clone();
    other.state.config.write().await.exchange.symbol = "BTCUSDC".into();
    other.state.ticker.write().await.symbol = "BTCUSDC".into();
    for pair in [&state, &other.state] {
        pair.config.write().await.global_max_position_notional = Some(dec!(10000));
        pair.position.write().await.update_time = Some(Utc::now());
        pair.global_position_initialized
            .store(true, Ordering::SeqCst);
        pair.global_position_ready.store(true, Ordering::SeqCst);
    }
    let global =
        crate::server::state::GlobalPositionLimit::new(Arc::new(tokio::sync::RwLock::new(vec![
            state.clone(),
            other.state.clone(),
        ])));
    global.membership_ready.store(true, Ordering::SeqCst);
    for pair in [&state, &other.state] {
        let _ = pair.global_position_limit.set(Arc::downgrade(&global));
    }
    assert!(!global.check().await);
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("BEGIN IMMEDIATE").unwrap();
    let placement = tokio::spawn(async move { submit(&mut placing).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!placement.is_finished());
    let ticker_lock = state.ticker.write().await;
    connection.execute_batch("COMMIT").unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let count: i64 = connection.query_row(
                "SELECT count(*) FROM managed_orders WHERE client_order_id = 'gb_b_reliability'",
                [], |row| row.get(0),
            ).unwrap();
            if count == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut gate = other.state.begin_fill_reconciliation().await;
    drop(ticker_lock);
    assert!(!placement.await.unwrap());
    assert!(fake.posts.lock().unwrap().is_empty());
    assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
    gate.resolve();
    drop(gate);
    server.abort();
    drop(connection);
    drop(db);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn recorded_fill_refreshes_account_and_global_risk_before_other_pair_can_send() {
    for account_fails in [false, true] {
        let fake = Exchange::default();
        fake.block_account.store(true, Ordering::SeqCst);
        fake.account_unavailable
            .store(account_fails, Ordering::SeqCst);
        let (url, server) = exchange_server(fake.clone()).await;
        let first_db = database();
        let (order, mut response) = managed_price_fixture();
        first_db.save_managed_order(&order).unwrap();
        let recorded = TradeRecord {
            trade_id: format!("grid:{}", order.client_order_id),
            client_order_id: order.client_order_id.clone(),
            symbol: order.symbol.clone(),
            mode: TradingMode::Live,
            side: order.side,
            price: order.price,
            quantity: order.quantity,
            amount_usdc: order.amount_usdc,
            realized_pnl: dec!(0),
            commission: dec!(0),
            pnl_verified: true,
            is_maker: true,
            timestamp: Utc::now(),
            note: "recorded fill".into(),
        };
        first_db.insert_trade(&recorded).unwrap();
        response.status = "FILLED".into();
        response.executed_qty = response.orig_qty;
        response.avg_price = Some(response.price);
        fake.orders
            .lock()
            .unwrap()
            .insert(order.client_order_id.clone(), response);
        let path = std::env::temp_dir().join(format!("recorded-risk-{}.db", uuid::Uuid::new_v4()));
        let second_db = Arc::new(Database::open(&path).unwrap());
        second_db.save_bot_status(BotStatus::Running).unwrap();
        let mut first = engine(first_db, &url).await;
        let mut second = engine(second_db.clone(), &url).await;
        let second_state = second.state.clone();
        let global = test_pair_global_limit(&first, &second).await;
        for pair in [&first.state, &second.state] {
            pair.config.write().await.global_max_position_notional = Some(dec!(250));
        }
        assert!(!global.check().await);
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection.execute_batch("BEGIN IMMEDIATE").unwrap();
        let placement = tokio::spawn(async move { submit(&mut second).await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!placement.is_finished());
        let ticker_lock = second_state.ticker.write().await;
        connection.execute_batch("COMMIT").unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let count: i64 = connection.query_row("SELECT count(*) FROM managed_orders WHERE client_order_id='gb_b_reliability'", [], |row| row.get(0)).unwrap();
                if count == 1 { break; }
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        fake.position_large.store(true, Ordering::SeqCst);
        let sync = tokio::spawn(async move { first.sync_live_orders().await });
        tokio::time::timeout(Duration::from_secs(5), fake.account_started.notified())
            .await
            .unwrap();
        drop(ticker_lock);
        assert!(!placement.await.unwrap());
        assert!(fake.posts.lock().unwrap().is_empty());
        fake.block_account.store(false, Ordering::SeqCst);
        fake.release_account.notify_one();
        let synced = sync.await.unwrap();
        if account_fails {
            assert!(!synced);
            assert!(second_state.try_order_submission().is_none());
        } else {
            assert!(global.check().await);
            assert_eq!(*second_state.status.read().await, BotStatus::Paused);
        }
        assert!(fake.posts.lock().unwrap().is_empty());
        server.abort();
        drop(connection);
        drop(second_db);
        let _ = std::fs::remove_file(path);
    }
}

#[tokio::test]
async fn invalid_snapshot_preserves_higher_observed_price_and_quantity_after_restart() {
    let fake = Exchange::default();
    let (url, server) = exchange_server(fake.clone()).await;
    let db = database();
    let (order, mut observed) = managed_price_fixture();
    db.save_managed_order(&order).unwrap();
    observed.price = dec!(120.375);
    observed.orig_qty = dec!(2.5);
    let mut invalid = observed.clone();
    invalid.client_order_id = "gb_other_zero".into();
    invalid.order_id = 456;
    invalid.price = Decimal::ZERO;
    {
        let mut orders = fake.orders.lock().unwrap();
        orders.insert(observed.client_order_id.clone(), observed.clone());
        orders.insert(invalid.client_order_id.clone(), invalid);
    }
    let mut first = engine(db.clone(), &url).await;
    assert!(!first.sync_live_orders().await);
    assert!(first.reconciliation_blocked);
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    let active = restarted.state.active_orders.read().await;
    let retained = &active[&order.client_order_id];
    assert!(retained.price >= observed.price);
    assert!(retained.quantity >= observed.orig_qty);
    assert_eq!(retained.amount_usdc, retained.price * retained.quantity);
    drop(active);
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    let retained = saved
        .iter()
        .find(|saved| saved.client_order_id == order.client_order_id)
        .unwrap();
    assert!(retained.price >= observed.price);
    assert!(retained.quantity >= observed.orig_qty);
    assert_eq!(retained.amount_usdc, retained.price * retained.quantity);
    server.abort();
}

#[tokio::test]
async fn duplicate_order_ids_with_zero_price_remain_blocked_after_snapshot_disappears() {
    for reversed in [false, true] {
        let fake = Exchange::default();
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let (_, mut valid) = managed_price_fixture();
        valid.client_order_id = "gb_zero_duplicate".into();
        let mut invalid = valid.clone();
        invalid.order_id = 456;
        invalid.price = Decimal::ZERO;
        {
            let mut orders = fake.orders.lock().unwrap();
            let entries = if reversed {
                [invalid.clone(), valid.clone()]
            } else {
                [valid.clone(), invalid.clone()]
            };
            for (index, response) in entries.into_iter().enumerate() {
                orders.insert(index.to_string(), response);
            }
        }
        let mut first = engine(db.clone(), &url).await;
        let account = first.client.account_identity_key();
        assert!(!first.sync_live_orders().await);
        assert!(first.reconciliation_blocked);
        let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
        for id in [valid.order_id, invalid.order_id] {
            assert!(observations
                .iter()
                .any(
                    |(expected, observed)| expected.client_order_id == valid.client_order_id
                        && observed.order_id == id
                ));
        }
        drop(first);
        fake.hide_open_orders.store(true, Ordering::SeqCst);
        fake.lookup_unavailable.store(true, Ordering::SeqCst);
        let mut restarted = engine(db.clone(), &url).await;
        for _ in 0..2 {
            assert!(!restarted.sync_live_orders().await);
            assert!(restarted.reconciliation_blocked);
            let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
            for id in [valid.order_id, invalid.order_id] {
                assert!(observations
                    .iter()
                    .any(
                        |(expected, observed)| expected.client_order_id == valid.client_order_id
                            && observed.order_id == id
                    ));
            }
        }
        server.abort();
    }
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
    paper.exchange.api_key.clear();
    paper.exchange.api_secret.clear();
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
    paper.exchange.api_key = "live-key".into();
    paper.exchange.api_secret = "live-secret".into();
    engine.state.save_runtime_config(paper).await.unwrap();
    engine.handle_control_action(pending).await;
    assert!(response.await.unwrap().is_err());
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
        config.exchange.api_key.clear();
        config.exchange.api_secret.clear();
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
    engine.state.config.write().await.exchange.api_key.clear();
    engine
        .state
        .config
        .write()
        .await
        .exchange
        .api_secret
        .clear();
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
async fn immediate_fills_suspend_later_openings_and_cancel_existing_window_orders() {
    for response in [1, 2] {
        let fake = Exchange::default();
        fake.fill_on_submit.store(response, Ordering::SeqCst);
        fake.liquidation_close_after_submission
            .store(true, Ordering::SeqCst);
        let db = database();
        for (id, order_id, side, price) in [
            ("gb_guard_opening", 123, OrderSide::Buy, dec!(99)),
            ("gb_guard_exit", 124, OrderSide::Sell, dec!(101)),
        ] {
            let (mut order, mut accepted) = managed_price_fixture();
            order.client_order_id = id.into();
            order.order_id = Some(order_id);
            order.side = side;
            order.price = price;
            order.quantity = dec!(1);
            order.amount_usdc = price;
            order.paired_client_order_id = None;
            accepted.client_order_id = id.into();
            accepted.order_id = order_id;
            accepted.side = side.as_str().into();
            accepted.price = price;
            accepted.orig_qty = order.quantity;
            db.save_managed_order(&order).unwrap();
            fake.orders.lock().unwrap().insert(id.into(), accepted);
        }
        let (url, server) = exchange_server(fake.clone()).await;
        let mut engine = engine(db.clone(), &url).await;
        {
            let mut config = engine.state.config.write().await;
            config.grid.buy_window = 3;
            config.grid.min_liquidation_distance_pct = Some(dec!(10));
        }

        engine.maintain_grid_window().await;

        assert_eq!(*engine.state.status.read().await, BotStatus::Running);
        assert_eq!(
            engine.state.position.read().await.liquidation_price,
            dec!(95)
        );
        let submitted = fake.posts.lock().unwrap().clone();
        assert_eq!(submitted.len(), 1, "response={response}");
        assert_eq!(fake.orders.lock().unwrap()[&submitted[0]].side, "BUY");
        assert_eq!(
            fake.orders.lock().unwrap()["gb_guard_opening"].status,
            "CANCELED"
        );
        assert_eq!(fake.orders.lock().unwrap()["gb_guard_exit"].status, "NEW");
        let active = engine.state.active_orders.read().await;
        assert!(active.contains_key("gb_guard_exit"));
        assert!(!active.contains_key("gb_guard_opening"));
        assert!(!active
            .values()
            .any(|order| order.side == OrderSide::Buy && order.status == OrderStatus::New));
        if response == 1 {
            assert_eq!(active[&submitted[0]].status, OrderStatus::PartiallyFilled);
            assert_eq!(active[&submitted[0]].quantity, dec!(0.5));
            assert_eq!(
                fake.orders.lock().unwrap()[&submitted[0]].status,
                "PARTIALLY_FILLED"
            );
        }
        drop(active);
        assert!(
            !engine
                .place_grid_order(OrderSide::Buy, dec!(98), "gb_guard_later".into(), -1, None)
                .await
        );
        assert_eq!(fake.posts.lock().unwrap().len(), 1);
        assert!(db
            .load_managed_orders("SOLUSDC")
            .unwrap()
            .iter()
            .any(|order| order.client_order_id == "gb_guard_exit"));
        server.abort();
    }
}

#[tokio::test]
async fn closed_submission_gates_do_not_block_protective_opening_cancellation_or_allow_posts() {
    for direction in [GridDirection::Long, GridDirection::Short] {
        for gate in ["pending_fill", "blocked", "configuration"] {
            for entry in ["maintenance", "ticker", "placement", "sync_unready"] {
                // This exchange fixture reports a long position during reconciliation.
                if direction == GridDirection::Short && entry == "sync_unready" {
                    continue;
                }
                let fake = Exchange::default();
                let db = database();
                let (mut opening, mut accepted) = managed_price_fixture();
                opening.side = direction.opening_side();
                opening.quantity = dec!(1);
                opening.amount_usdc = opening.price;
                accepted.side = opening.side.as_str().into();
                accepted.orig_qty = opening.quantity;
                db.save_managed_order(&opening).unwrap();
                fake.orders
                    .lock()
                    .unwrap()
                    .insert(opening.client_order_id.clone(), accepted.clone());
                let mut exit = opening.clone();
                exit.client_order_id = "gb_closed_gate_exit".into();
                exit.order_id = Some(124);
                exit.side = direction.closing_side();
                exit.price = if direction == GridDirection::Long {
                    dec!(120)
                } else {
                    dec!(80)
                };
                exit.amount_usdc = exit.price * exit.quantity;
                accepted.client_order_id = exit.client_order_id.clone();
                accepted.order_id = 124;
                accepted.side = exit.side.as_str().into();
                accepted.price = exit.price;
                db.save_managed_order(&exit).unwrap();
                fake.orders
                    .lock()
                    .unwrap()
                    .insert(exit.client_order_id.clone(), accepted);
                let (url, server) = exchange_server(fake.clone()).await;
                let mut engine = engine(db.clone(), &url).await;
                let second = self::engine(database(), &url).await;
                let global = test_pair_global_limit(&engine, &second).await;
                {
                    let mut config = engine.state.config.write().await;
                    config.grid.direction = direction;
                    config.grid.max_position_usdc = Some(dec!(100));
                    config.grid.cancel_opening_on_max_position = true;
                }
                engine.state.position.write().await.size = direction.sign();
                let _pending =
                    (gate == "pending_fill").then(|| second.state.publish_fill_reconciliation());
                let _configuration = if gate == "configuration" {
                    Some(
                        engine
                            .state
                            .order_submission_lock
                            .clone()
                            .write_owned()
                            .await,
                    )
                } else {
                    None
                };
                if gate == "blocked" {
                    second
                        .state
                        .global_position_initialized
                        .store(false, Ordering::SeqCst);
                    second
                        .state
                        .global_position_ready
                        .store(false, Ordering::SeqCst);
                    global.check().await;
                }
                assert!(engine.state.try_order_submission().is_none());

                match entry {
                    "maintenance" => engine.maintain_grid_window().await,
                    "ticker" => {
                        let mut update = engine.state.ticker.read().await.clone();
                        update.mark_update_time = Utc::now();
                        engine.handle_ticker_update(update).await;
                    }
                    "placement" => {
                        assert!(
                            !engine
                                .place_grid_order(
                                    direction.opening_side(),
                                    dec!(99),
                                    "gb_blocked_new".into(),
                                    -1,
                                    None
                                )
                                .await
                        );
                    }
                    "sync_unready" => {
                        fake.account_unavailable.store(true, Ordering::SeqCst);
                        engine.sync_cycle().await;
                        assert!(engine.last_account_sync.is_none());
                    }
                    _ => unreachable!(),
                }

                assert!(
                    fake.posts.lock().unwrap().is_empty(),
                    "direction={direction:?}, gate={gate}, entry={entry}"
                );
                assert_eq!(
                    fake.orders.lock().unwrap()[&opening.client_order_id].status,
                    "CANCELED"
                );
                assert_eq!(
                    fake.orders.lock().unwrap()[&exit.client_order_id].status,
                    "NEW"
                );
                let active = engine.state.active_orders.read().await;
                assert_eq!(
                    active.len(),
                    1,
                    "direction={direction:?}, gate={gate}, entry={entry}"
                );
                assert_eq!(active[&exit.client_order_id].price, exit.price);
                assert_eq!(active[&exit.client_order_id].quantity, exit.quantity);
                drop(active);
                let saved = db.load_managed_orders("SOLUSDC").unwrap();
                assert_eq!(saved.len(), 1);
                assert_eq!(saved[0].client_order_id, exit.client_order_id);
                assert!(db.get_recent_trades(10).unwrap().is_empty());
                server.abort();
            }
        }
    }
}

#[tokio::test]
async fn pending_or_failing_remainder_recovery_cannot_block_opening_cancellation() {
    for failing in [false, true] {
        let fake = Exchange::default();
        let db = database();
        let (mut opening, mut accepted) = managed_price_fixture();
        opening.price = dec!(100);
        opening.quantity = dec!(1);
        opening.amount_usdc = opening.price;
        accepted.price = opening.price;
        accepted.orig_qty = opening.quantity;
        db.save_managed_order(&opening).unwrap();
        fake.orders
            .lock()
            .unwrap()
            .insert(opening.client_order_id.clone(), accepted.clone());
        let mut exit = opening.clone();
        exit.client_order_id = "gb_recovery_exit".into();
        exit.order_id = Some(124);
        exit.side = OrderSide::Sell;
        exit.price = dec!(101);
        exit.amount_usdc = exit.price;
        accepted.client_order_id = exit.client_order_id.clone();
        accepted.order_id = 124;
        accepted.side = "SELL".into();
        accepted.price = exit.price;
        db.save_managed_order(&exit).unwrap();
        fake.orders
            .lock()
            .unwrap()
            .insert(exit.client_order_id.clone(), accepted);
        let mut target = exit.clone();
        target.client_order_id = "gb_pending_remainder_guard".into();
        target.order_id = None;
        target.purpose = OrderPurpose::Remainder;
        let mut missing_source = exit.clone();
        missing_source.client_order_id = "gb_missing_remainder_source".into();
        missing_source.order_id = Some(125);
        let phase = if failing {
            RemainderPhase::Canceling
        } else {
            RemainderPhase::Submitting
        };
        db.save_remainder_plan(&RemainderPlan {
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Live,
            sources: if failing {
                vec![missing_source]
            } else {
                vec![]
            },
            target,
            phase,
        })
        .unwrap();
        let (url, server) = exchange_server(fake.clone()).await;
        let mut engine = engine(db.clone(), &url).await;
        engine
            .state
            .config
            .write()
            .await
            .grid
            .min_liquidation_distance_pct = Some(dec!(10));
        engine.state.position.write().await.liquidation_price = dec!(95);
        engine.last_account_sync = None;

        engine.maintain_grid_window().await;

        assert_eq!(*engine.state.status.read().await, BotStatus::Running);
        assert!(fake.posts.lock().unwrap().is_empty());
        assert_eq!(
            fake.orders.lock().unwrap()[&opening.client_order_id].status,
            "CANCELED"
        );
        assert_eq!(
            fake.orders.lock().unwrap()[&exit.client_order_id].status,
            "NEW"
        );
        let active = engine.state.active_orders.read().await;
        assert_eq!(active.len(), 1);
        assert!(active.contains_key(&exit.client_order_id));
        drop(active);
        assert_eq!(
            db.load_remainder_plan("SOLUSDC", TradingMode::Live)
                .unwrap()
                .unwrap()
                .phase,
            phase
        );
        assert_eq!(
            db.load_managed_orders("SOLUSDC").unwrap()[0].client_order_id,
            exit.client_order_id
        );
        server.abort();
    }
}

#[tokio::test]
async fn protective_opening_cancellation_retries_unconfirmed_orders_and_preserves_exits() {
    let fake = Exchange::default();
    fake.cancel_fails.store(true, Ordering::SeqCst);
    let db = database();
    let (opening, mut accepted) = managed_price_fixture();
    db.save_managed_order(&opening).unwrap();
    fake.orders
        .lock()
        .unwrap()
        .insert(opening.client_order_id.clone(), accepted.clone());
    let mut exit = opening.clone();
    exit.client_order_id = "gb_stale_guard_exit".into();
    exit.order_id = Some(124);
    exit.side = OrderSide::Sell;
    exit.price = dec!(120);
    exit.amount_usdc = exit.price * exit.quantity;
    accepted.client_order_id = exit.client_order_id.clone();
    accepted.order_id = 124;
    accepted.side = "SELL".into();
    accepted.price = exit.price;
    db.save_managed_order(&exit).unwrap();
    fake.orders
        .lock()
        .unwrap()
        .insert(exit.client_order_id.clone(), accepted);
    let (url, server) = exchange_server(fake.clone()).await;
    let mut engine = engine(db.clone(), &url).await;
    engine
        .state
        .config
        .write()
        .await
        .grid
        .min_liquidation_distance_pct = Some(dec!(10));
    engine.state.position.write().await.liquidation_price = dec!(95);
    engine.state.ticker.write().await.update_time = Utc::now() - chrono::Duration::seconds(16);
    let _configuration = engine
        .state
        .order_submission_lock
        .clone()
        .write_owned()
        .await;

    engine.maintain_grid_window().await;

    assert_eq!(*engine.state.status.read().await, BotStatus::Running);
    assert!(engine.last_orders_sync.is_none());
    assert_eq!(engine.state.active_orders.read().await.len(), 2);
    assert_eq!(db.load_managed_orders("SOLUSDC").unwrap().len(), 2);
    assert!(fake.posts.lock().unwrap().is_empty());
    assert!(db.get_recent_trades(10).unwrap().is_empty());
    fake.cancel_fails.store(false, Ordering::SeqCst);
    fake.fill_on_cancel.store(true, Ordering::SeqCst);

    engine.maintain_grid_window().await;

    assert_eq!(*engine.state.status.read().await, BotStatus::Running);
    let active = engine.state.active_orders.read().await;
    assert_eq!(active.len(), 1);
    assert_eq!(active[&exit.client_order_id].price, exit.price);
    assert_eq!(active[&exit.client_order_id].quantity, exit.quantity);
    drop(active);
    assert_eq!(
        db.load_managed_orders("SOLUSDC").unwrap()[0].client_order_id,
        exit.client_order_id
    );
    assert!(fake.posts.lock().unwrap().is_empty());
    let trades = db.get_recent_trades(10).unwrap();
    assert_eq!(trades.len(), 1);
    assert_eq!(trades[0].client_order_id, opening.client_order_id);
    assert_eq!(trades[0].quantity, opening.quantity);
    assert_eq!(
        fake.orders.lock().unwrap()[&opening.client_order_id].status,
        "FILLED"
    );
    server.abort();
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
            if conflicting_active { Some(456) } else { None }
        );
        assert_eq!(
            retained.purpose,
            if conflicting_active {
                crate::types::OrderPurpose::Legacy
            } else {
                crate::types::OrderPurpose::Unverified
            }
        );
        assert_eq!(
            retained.quantity,
            if conflicting_active { dec!(1) } else { dec!(2) }
        );
        assert!(engine.reconciliation_blocked);
        drop(active);
        let observations = db
            .load_identity_observations(engine.client.account_identity_key(), "SOLUSDC")
            .unwrap();
        assert!(observations
            .iter()
            .any(|(_, observed)| observed.client_order_id == "gb_unknown_db"
                && observed.symbol == "SOLUSDC"
                && observed.side == "BUY"
                && observed.order_id == 123));
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
        })).route("/fapi/v1/order", get({
            let stage = stage.clone();
            move || {
                let stage = stage.clone();
                async move {
                    Json(json!({"orderId":123, "clientOrderId":"gb_metadata", "symbol":"SOLUSDC",
                        "status":if stage.load(Ordering::SeqCst) == 1 {"NEW"} else {"FILLED"},
                        "price":"100", "origQty":"2", "executedQty":"0",
                        "side":"BUY", "type":"LIMIT", "timeInForce":"GTX"}))
                }
            }
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
            crate::types::OrderPurpose::Unverified
        );
        assert_eq!(
            engine.state.active_orders.read().await["gb_metadata"].order_id,
            None
        );
        let observations = db
            .load_identity_observations(engine.client.account_identity_key(), "SOLUSDC")
            .unwrap();
        assert!(observations
            .iter()
            .any(|(_, observed)| observed.client_order_id == "gb_metadata"
                && observed.symbol == "SOLUSDC"
                && observed.side == "BUY"
                && observed.order_id == 123));
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
        move |Query(query): Query<HashMap<String, String>>| { let stage = stage.clone(); async move {
            let current = stage.load(Ordering::SeqCst);
            let id = if current == 2 {
                query.get("orderId").and_then(|id| id.parse::<i64>().ok()).unwrap_or(123)
            } else {456};
            Json(json!({"orderId":id, "clientOrderId":"gb_conflict", "symbol":"SOLUSDC",
                "status":if current != 2 {"FILLED"} else if id == 456 {"CANCELED"} else {"NEW"},
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
            if already_active { Some(123) } else { None }
        );
        assert_eq!(
            engine.state.active_orders.read().await["gb_conflict"].purpose,
            if already_active {
                crate::types::OrderPurpose::Grid
            } else {
                crate::types::OrderPurpose::Unverified
            }
        );
        let observations = db
            .load_identity_observations(engine.client.account_identity_key(), "SOLUSDC")
            .unwrap();
        assert!(observations
            .iter()
            .any(|(_, observed)| observed.client_order_id == "gb_conflict"
                && observed.symbol == "SOLUSDC"
                && observed.side == "BUY"
                && observed.order_id == 456));
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
async fn resolved_identity_groups_cleanup_is_atomic_across_both_tables() {
    for rejected_table in ["initial_order_responses", "order_identity_observations"] {
        let path =
            std::env::temp_dir().join(format!("identity-cleanup-{}.db", uuid::Uuid::new_v4()));
        let db = Arc::new(Database::open(&path).unwrap());
        db.save_bot_status(BotStatus::Running).unwrap();
        let connection = rusqlite::Connection::open(&path).unwrap();
        let queried_a = Arc::new(AtomicU8::new(0));
        let queried_b = Arc::new(AtomicU8::new(0));
        let snapshots = Arc::new(AtomicU8::new(0));
        let app = Router::new()
            .route("/fapi/v1/openOrders", get({
                let snapshots = snapshots.clone();
                move || { let snapshots = snapshots.clone(); async move {
                    snapshots.fetch_add(1, Ordering::SeqCst);
                    Json(json!([
                        {"orderId":123,"clientOrderId":"gb_cleanup_a","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                        {"orderId":234,"clientOrderId":"gb_cleanup_b","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}
                    ]))
                }}
            }))
            .route("/fapi/v1/order", get({
                let queried_a = queried_a.clone();
                let queried_b = queried_b.clone();
                move |Query(query): Query<HashMap<String, String>>| {
                    let queried_a = queried_a.clone();
                    let queried_b = queried_b.clone();
                    async move {
                        let id = match query.get("orderId") {
                            Some(id) => id.parse::<i64>().unwrap(),
                            None => match query.get("origClientOrderId").map(String::as_str) {
                                Some("gb_cleanup_a") => 123,
                                Some("gb_cleanup_b") => 234,
                                other => panic!("unexpected client order ID: {other:?}"),
                            },
                        };
                        if id == 123 { queried_a.fetch_add(1, Ordering::SeqCst); }
                        else if id == 234 { queried_b.fetch_add(1, Ordering::SeqCst); }
                        else { panic!("unexpected order ID: {id}"); }
                        Json(json!({"orderId":id,"clientOrderId":if id == 123 {"gb_cleanup_a"} else {"gb_cleanup_b"},
                            "symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0",
                            "side":"BUY","type":"LIMIT","timeInForce":"GTX"}))
                    }
                }
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut first = engine(db.clone(), &url).await;
        let account = first.client.account_identity_key();
        let mut original_responses = HashMap::new();
        for (client, id) in [("gb_cleanup_a", 123), ("gb_cleanup_b", 234)] {
            let expected = GridOrder {
                client_order_id: client.into(),
                order_id: Some(id),
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
            db.save_identity_observation(
                account,
                &expected,
                &crate::types::ObservedOrderIdentity {
                    client_order_id: client.into(),
                    symbol: "SOLUSDC".into(),
                    side: "SELL".into(),
                    order_id: id,
                },
            )
            .unwrap();
            let response: BinanceOrderResponse =
                serde_json::from_value(json!({"orderId":id,"clientOrderId":client,
                "symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0",
                "side":"BUY","type":"LIMIT","timeInForce":"GTX"}))
                .unwrap();
            original_responses.insert(client.to_string(), serde_json::to_value(&response).unwrap());
            db.save_initial_order_responses(account, "SOLUSDC", client, &[response])
                .unwrap();
        }
        connection.execute_batch(&format!("CREATE TRIGGER reject_group_cleanup BEFORE DELETE ON {rejected_table} \
            WHEN OLD.client_order_id = 'gb_cleanup_b' BEGIN SELECT RAISE(ABORT, 'injected failure'); END;")).unwrap();
        assert!(!first.sync_live_orders().await, "table={rejected_table}");
        assert!(
            queried_a.load(Ordering::SeqCst) > 0,
            "first group was not verified"
        );
        assert!(
            queried_b.load(Ordering::SeqCst) > 0,
            "second group was not verified"
        );
        assert!(snapshots.load(Ordering::SeqCst) > 0);
        assert!(first.reconciliation_blocked);
        assert!(first.last_orders_sync.is_none());
        let mut first_responses = HashMap::new();
        let mut first_observations = HashMap::new();
        for client in ["gb_cleanup_a", "gb_cleanup_b"] {
            let key = (account, client.to_string());
            assert!(first.order_identity_conflicts.contains_key(&key));
            assert!(first.order_identity_observations.contains_key(&key));
            assert!(first.provisional_order_ids.contains(&key));
            let responses = db
                .load_initial_order_responses(account, "SOLUSDC", client)
                .unwrap();
            assert!(!responses.is_empty());
            let values: Vec<_> = responses
                .iter()
                .map(|response| serde_json::to_value(response).unwrap())
                .collect();
            assert_eq!(values[0], original_responses[client]);
            assert!(values
                .iter()
                .all(|value| value == &original_responses[client]));
            first_responses.insert(client.to_string(), values);
            let mut observations: Vec<_> = db
                .load_identity_observations(account, "SOLUSDC")
                .unwrap()
                .into_iter()
                .filter(|(expected, _)| expected.client_order_id == client)
                .collect();
            assert_eq!(observations.len(), 2);
            observations.sort_by(|a, b| a.1.side.cmp(&b.1.side));
            for (index, side) in ["BUY", "SELL"].into_iter().enumerate() {
                assert_eq!(observations[index].1.side, side);
                assert_eq!(observations[index].1.client_order_id, client);
                assert_eq!(observations[index].1.symbol, "SOLUSDC");
                assert_eq!(
                    observations[index].1.order_id,
                    if client == "gb_cleanup_a" { 123 } else { 234 }
                );
            }
            first_observations.insert(
                client.to_string(),
                serde_json::to_value(&observations).unwrap(),
            );
        }
        drop(first);
        let mut restarted = engine(db.clone(), &url).await;
        assert!(!restarted.sync_live_orders().await);
        assert!(restarted.reconciliation_blocked);
        for client in ["gb_cleanup_a", "gb_cleanup_b"] {
            let key = (account, client.to_string());
            assert!(restarted.order_identity_conflicts.contains_key(&key));
            assert!(restarted.order_identity_observations.contains_key(&key));
            let responses = db
                .load_initial_order_responses(account, "SOLUSDC", client)
                .unwrap();
            assert!(!responses.is_empty());
            let values: Vec<_> = responses
                .iter()
                .map(|response| serde_json::to_value(response).unwrap())
                .collect();
            assert!(values.starts_with(&first_responses[client]));
            assert!(values
                .iter()
                .all(|value| value == &original_responses[client]));
            let mut observations: Vec<_> = db
                .load_identity_observations(account, "SOLUSDC")
                .unwrap()
                .into_iter()
                .filter(|(expected, _)| expected.client_order_id == client)
                .collect();
            observations.sort_by(|a, b| a.1.side.cmp(&b.1.side));
            assert_eq!(
                serde_json::to_value(&observations).unwrap(),
                first_observations[client]
            );
        }
        connection
            .execute_batch("DROP TRIGGER reject_group_cleanup")
            .unwrap();
        assert!(restarted.sync_live_orders().await);
        assert!(db
            .load_identity_observations(account, "SOLUSDC")
            .unwrap()
            .is_empty());
        for client in ["gb_cleanup_a", "gb_cleanup_b"] {
            assert!(db
                .load_initial_order_responses(account, "SOLUSDC", client)
                .unwrap()
                .is_empty());
            assert!(!restarted
                .order_identity_conflicts
                .contains_key(&(account, client.to_string())));
        }
        server.abort();
        drop(connection);
        drop(restarted);
        drop(db);
        let _ = std::fs::remove_file(path);
    }
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
                        "status":if alternate {"CANCELED"} else {"PARTIALLY_FILLED"},"price":"100","origQty":"2",
                        "executedQty":if alternate {"0"} else {"1"},
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
async fn cancellation_confirmation_rejects_mismatched_identity() {
    for known_id in [false, true] {
        for mismatch in ["client", "symbol", "side", "order_id"] {
            if !known_id && mismatch == "order_id" {
                continue;
            }
            let db = database();
            let lookups = Arc::new(AtomicU8::new(0));
            let app = Router::new().route("/fapi/v1/order", get({
                let lookups = lookups.clone();
                move |Query(query): Query<HashMap<String, String>>| {
                    let lookups = lookups.clone();
                    async move {
                        if known_id {
                            assert_eq!(query.get("orderId").map(String::as_str), Some("123"));
                        } else {
                            assert_eq!(query.get("origClientOrderId").map(String::as_str), Some("gb_cancel_mismatch"));
                        }
                        lookups.fetch_add(1, Ordering::SeqCst);
                        Json(json!({"orderId":if mismatch == "order_id" {456} else {123},
                            "clientOrderId":if mismatch == "client" {"gb_other"} else {"gb_cancel_mismatch"},
                            "symbol":if mismatch == "symbol" {"BTCUSDC"} else {"SOLUSDC"},
                            "side":if mismatch == "side" {"SELL"} else {"BUY"},
                            "status":"CANCELED","price":"100","origQty":"1","executedQty":"0",
                            "type":"LIMIT","timeInForce":"GTX"}))
                    }
                }
            }).delete(|| async {
                (StatusCode::BAD_REQUEST, Json(json!({"code":-2011,"msg":"unknown order"})))
            }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let order = GridOrder {
                client_order_id: "gb_cancel_mismatch".into(),
                order_id: known_id.then_some(123),
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
            let mut engine = engine(db.clone(), &url).await;
            let account = engine.client.account_identity_key();
            assert!(
                !engine.cancel_single_order(&order).await,
                "known_id={known_id}, mismatch={mismatch}"
            );
            assert!(lookups.load(Ordering::SeqCst) > 0);
            assert!(engine.reconciliation_blocked);
            assert!(engine.last_orders_sync.is_none());
            assert!(!engine.state.global_position_ready.load(Ordering::SeqCst));
            let active = engine.state.active_orders.read().await;
            let retained = &active[&order.client_order_id];
            assert_eq!(retained.order_id, order.order_id);
            assert_eq!(retained.symbol, order.symbol);
            assert_eq!(retained.side, order.side);
            assert_eq!(retained.quantity, order.quantity);
            drop(active);
            let saved = db.load_managed_orders("SOLUSDC").unwrap();
            assert_eq!(saved.len(), 1);
            assert_eq!(saved[0].order_id, order.order_id);
            assert_eq!(saved[0].symbol, order.symbol);
            assert_eq!(saved[0].side, order.side);
            assert!(db.get_recent_trades(10).unwrap().is_empty());
            assert!(!db
                .load_identity_observations(account, "SOLUSDC")
                .unwrap()
                .is_empty());
            server.abort();
        }
    }
}

#[tokio::test]
async fn terminal_lookup_rejects_mismatched_order_identity() {
    for known_id in [false, true] {
        for mismatch in ["client", "symbol", "side", "order_id"] {
            if !known_id && mismatch == "order_id" {
                continue;
            }
            let db = database();
            let lookups = Arc::new(AtomicU8::new(0));
            let app = Router::new()
                .route("/fapi/v1/openOrders", get(|| async { Json(json!([])) }))
                .route("/fapi/v1/order", get({
                    let lookups = lookups.clone();
                    move |Query(query): Query<HashMap<String, String>>| {
                        let lookups = lookups.clone();
                        async move {
                            if known_id {
                                assert_eq!(query.get("orderId").map(String::as_str), Some("123"));
                            } else {
                                assert_eq!(query.get("origClientOrderId").map(String::as_str), Some("gb_terminal_mismatch"));
                            }
                            lookups.fetch_add(1, Ordering::SeqCst);
                            Json(json!({"orderId":if mismatch == "order_id" {456} else {123},
                                "clientOrderId":if mismatch == "client" {"gb_other"} else {"gb_terminal_mismatch"},
                                "symbol":if mismatch == "symbol" {"BTCUSDC"} else {"SOLUSDC"},
                                "side":if mismatch == "side" {"SELL"} else {"BUY"},
                                "status":"CANCELED","price":"100","origQty":"1","executedQty":"0",
                                "type":"LIMIT","timeInForce":"GTX"}))
                        }
                    }
                }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let order = GridOrder {
                client_order_id: "gb_terminal_mismatch".into(),
                order_id: known_id.then_some(123),
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
            let mut engine = engine(db.clone(), &url).await;
            assert!(
                !engine.sync_live_orders().await,
                "known_id={known_id}, mismatch={mismatch}"
            );
            assert!(lookups.load(Ordering::SeqCst) > 0);
            assert!(engine.reconciliation_blocked);
            assert!(engine.last_orders_sync.is_none());
            assert!(!engine.state.global_position_ready.load(Ordering::SeqCst));
            let active = engine.state.active_orders.read().await;
            let retained = &active[&order.client_order_id];
            assert_eq!(retained.order_id, order.order_id);
            assert_eq!(retained.side, order.side);
            assert_eq!(retained.symbol, order.symbol);
            assert_eq!(retained.quantity, order.quantity);
            drop(active);
            let saved = db.load_managed_orders("SOLUSDC").unwrap();
            assert_eq!(saved.len(), 1);
            assert_eq!(saved[0].order_id, order.order_id);
            assert_eq!(saved[0].side, order.side);
            assert_eq!(saved[0].symbol, order.symbol);
            assert!(db.get_recent_trades(10).unwrap().is_empty());
            server.abort();
        }
    }
}

#[tokio::test]
async fn conflicting_duplicate_open_orders_block_in_both_snapshot_orders_and_after_restart() {
    for known_id in [false, true] {
        for reversed in [false, true] {
            for mismatch in ["order_id", "side", "symbol"] {
                let db = database();
                let good = json!({"orderId":123,"clientOrderId":"gb_duplicate_snapshot","symbol":"SOLUSDC",
                    "status":"NEW","price":"100","origQty":"1","executedQty":"0",
                    "side":"BUY","type":"LIMIT","timeInForce":"GTX"});
                let bad = json!({"orderId":if mismatch == "order_id" {456} else {123},
                    "clientOrderId":"gb_duplicate_snapshot",
                    "symbol":if mismatch == "symbol" {"BTCUSDC"} else {"SOLUSDC"},
                    "status":"NEW","price":"100","origQty":"1","executedQty":"0",
                    "side":if mismatch == "side" {"SELL"} else {"BUY"},
                    "type":"LIMIT","timeInForce":"GTX"});
                let snapshot = if reversed {
                    vec![bad, good]
                } else {
                    vec![good, bad]
                };
                let app = Router::new()
                    .route(
                        "/fapi/v1/openOrders",
                        get(move || {
                            let snapshot = snapshot.clone();
                            async move { Json(snapshot) }
                        }),
                    )
                    .route(
                        "/fapi/v1/order",
                        get(|| async {
                            (
                                StatusCode::SERVICE_UNAVAILABLE,
                                Json(json!({"code":-1007,"msg":"unavailable"})),
                            )
                        }),
                    );
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let url = format!("http://{}", listener.local_addr().unwrap());
                let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
                let order = GridOrder {
                    client_order_id: "gb_duplicate_snapshot".into(),
                    order_id: known_id.then_some(123),
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
                let mut first = engine(db.clone(), &url).await;
                let account = first.client.account_identity_key();
                assert!(
                    !first.sync_live_orders().await,
                    "known_id={known_id}, reversed={reversed}, mismatch={mismatch}"
                );
                for mut current in [first, engine(db.clone(), &url).await] {
                    assert!(!current.sync_live_orders().await);
                    assert!(current.reconciliation_blocked);
                    assert!(current.last_orders_sync.is_none());
                    assert!(!current.state.global_position_ready.load(Ordering::SeqCst));
                    let active = current.state.active_orders.read().await;
                    assert_eq!(active[&order.client_order_id].order_id, order.order_id);
                    assert_eq!(active[&order.client_order_id].side, order.side);
                    assert_eq!(active[&order.client_order_id].symbol, order.symbol);
                    drop(active);
                    let saved = db.load_managed_orders("SOLUSDC").unwrap();
                    assert_eq!(saved.len(), 1);
                    assert_eq!(saved[0].order_id, order.order_id);
                    let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
                    assert!(observations.iter().any(|(_, observed)| {
                        observed.client_order_id == order.client_order_id
                            && observed.order_id == if mismatch == "order_id" { 456 } else { 123 }
                            && observed.symbol
                                == if mismatch == "symbol" {
                                    "BTCUSDC"
                                } else {
                                    "SOLUSDC"
                                }
                            && observed.side == if mismatch == "side" { "SELL" } else { "BUY" }
                    }));
                }
                server.abort();
            }
        }
    }
}

#[tokio::test]
async fn partial_snapshot_cannot_be_overwritten_by_older_single_order_queries() {
    let db = database();
    let stage = Arc::new(AtomicU8::new(0));
    let queries = Arc::new(AtomicU8::new(0));
    let app = Router::new()
        .route("/fapi/v1/openOrders", get({
            let stage = stage.clone();
            move || { let stage = stage.clone(); async move {
                if stage.load(Ordering::SeqCst) != 0 { return Json(json!([])); }
                Json(json!([{"orderId":123,"clientOrderId":"gb_partial_snapshot","symbol":"SOLUSDC",
                    "status":"PARTIALLY_FILLED","price":"100","avgPrice":"100","origQty":"2","executedQty":"0.5",
                    "updateTime":2000,"side":"BUY","type":"LIMIT","timeInForce":"GTX"}]))
            }}
        }))
        .route("/fapi/v1/order", get({
            let queries = queries.clone();
            move || { let queries = queries.clone(); async move {
                queries.fetch_add(1, Ordering::SeqCst);
                Json(json!({"orderId":123,"clientOrderId":"gb_partial_snapshot","symbol":"SOLUSDC",
                    "status":"NEW","price":"100","avgPrice":"100","origQty":"2","executedQty":"0",
                    "updateTime":1000,"side":"BUY","type":"LIMIT","timeInForce":"GTX"}))
            }}
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let expected = GridOrder {
        client_order_id: "gb_partial_snapshot".into(),
        order_id: None,
        symbol: "SOLUSDC".into(),
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(2),
        amount_usdc: dec!(200),
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
    let mut first = engine(db.clone(), &url).await;
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
    assert!(first.reconciliation_blocked);
    let responses = db
        .load_initial_order_responses(account, "SOLUSDC", &expected.client_order_id)
        .unwrap();
    assert!(responses.iter().any(|response| response.order_id == 123
        && response.status == "PARTIALLY_FILLED"
        && response.executed_qty == dec!(0.5)
        && response.update_time == Some(2000)));
    drop(first);
    stage.store(1, Ordering::SeqCst);
    let mut restarted = engine(db.clone(), &url).await;
    for _ in 0..3 {
        assert!(!restarted.sync_live_orders().await);
        assert!(restarted.reconciliation_blocked);
        assert!(restarted
            .state
            .active_orders
            .read()
            .await
            .contains_key(&expected.client_order_id));
        assert!(db.get_recent_trades(10).unwrap().is_empty());
    }
    assert!(queries.load(Ordering::SeqCst) > 0);
    assert!(db
        .load_initial_order_responses(account, "SOLUSDC", &expected.client_order_id)
        .unwrap()
        .iter()
        .any(
            |response| response.status == "PARTIALLY_FILLED" && response.executed_qty == dec!(0.5)
        ));
    assert!(!db
        .load_terminal_order_disagreements(account, "SOLUSDC")
        .unwrap()
        .is_empty());
    server.abort();
}

#[tokio::test]
async fn duplicate_snapshot_responses_are_written_as_one_transaction() {
    let path = std::env::temp_dir().join(format!(
        "snapshot-evidence-batch-{}.db",
        uuid::Uuid::new_v4()
    ));
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_second_snapshot BEFORE INSERT ON initial_order_responses WHEN NEW.response_json LIKE '%PARTIALLY_FILLED%' BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
    let app = Router::new().route("/fapi/v1/openOrders", get(|| async {
        Json(json!([
            {"orderId":123,"clientOrderId":"gb_snapshot_batch","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"2","executedQty":"0","updateTime":1000,"side":"BUY","type":"LIMIT","timeInForce":"GTX"},
            {"orderId":123,"clientOrderId":"gb_snapshot_batch","symbol":"SOLUSDC","status":"PARTIALLY_FILLED","price":"100","origQty":"2","executedQty":"0.5","updateTime":2000,"side":"BUY","type":"LIMIT","timeInForce":"GTX"}
        ]))
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    let key = (account, "gb_snapshot_batch".to_string());
    assert!(!first.sync_live_orders().await);
    assert!(first.reconciliation_blocked);
    assert!(db
        .load_initial_order_responses(account, "SOLUSDC", "gb_snapshot_batch")
        .unwrap()
        .is_empty());
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .is_empty());
    assert!(first.order_identity_conflicts.contains_key(&key));
    assert!(first
        .order_identity_observations
        .get(&key)
        .unwrap()
        .iter()
        .any(|observed| observed.order_id == 123));
    assert_eq!(
        first.state.active_orders.read().await["gb_snapshot_batch"].quantity,
        dec!(2)
    );
    assert!(!first.sync_live_orders().await);
    assert!(db
        .load_initial_order_responses(account, "SOLUSDC", "gb_snapshot_batch")
        .unwrap()
        .is_empty());
    assert!(first.reconciliation_blocked);
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .is_empty());
    assert!(first.order_identity_conflicts.contains_key(&key));
    assert!(first
        .order_identity_observations
        .get(&key)
        .unwrap()
        .iter()
        .any(|observed| observed.order_id == 123));
    connection
        .execute_batch("DROP TRIGGER reject_second_snapshot")
        .unwrap();
    assert!(!first.sync_live_orders().await);
    let responses = db
        .load_initial_order_responses(account, "SOLUSDC", "gb_snapshot_batch")
        .unwrap();
    assert!(responses
        .iter()
        .any(|response| response.status == "NEW" && response.executed_qty == dec!(0)));
    assert!(responses.iter().any(
        |response| response.status == "PARTIALLY_FILLED" && response.executed_qty == dec!(0.5)
    ));
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .iter()
        .any(
            |(expected, observed)| expected.client_order_id == "gb_snapshot_batch"
                && observed.order_id == 123
        ));
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    assert!(restarted.reconciliation_blocked);
    assert_eq!(
        restarted.state.active_orders.read().await["gb_snapshot_batch"].quantity,
        dec!(2)
    );
    let responses = db
        .load_initial_order_responses(account, "SOLUSDC", "gb_snapshot_batch")
        .unwrap();
    assert!(responses.iter().any(|response| response.status == "NEW"));
    assert!(responses.iter().any(
        |response| response.status == "PARTIALLY_FILLED" && response.executed_qty == dec!(0.5)
    ));
    assert!(restarted.order_identity_conflicts.contains_key(&key));
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .iter()
        .any(
            |(expected, observed)| expected.client_order_id == "gb_snapshot_batch"
                && observed.order_id == 123
        ));
    server.abort();
    drop(connection);
    drop(restarted);
    drop(db);
    let _ = std::fs::remove_file(path);
}

fn evidence_test_response(
    status: &str,
    executed_qty: Decimal,
    update_time: i64,
) -> BinanceOrderResponse {
    BinanceOrderResponse {
        order_id: 123,
        client_order_id: "gb_evidence_timeline".into(),
        symbol: "SOLUSDC".into(),
        status: status.into(),
        price: dec!(100),
        avg_price: None,
        orig_qty: dec!(2),
        executed_qty,
        side: "BUY".into(),
        order_type: "LIMIT".into(),
        time_in_force: "GTX".into(),
        update_time: Some(update_time),
    }
}

#[tokio::test]
async fn consecutive_snapshot_evidence_is_deduplicated_without_losing_timeline() {
    let db = database();
    let (url, server) = exchange_server(Exchange::default()).await;
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    let client = "gb_evidence_timeline";
    let new = evidence_test_response("NEW", dec!(0), 1000);
    let partial = evidence_test_response("PARTIALLY_FILLED", dec!(0.5), 2000);
    let snapshot = vec![
        (client.to_string(), new.clone()),
        (client.to_string(), partial.clone()),
    ];
    let changed = vec![
        (client.to_string(), partial.clone()),
        (client.to_string(), new.clone()),
    ];

    assert!(
        first
            .record_initial_order_response_snapshot(
                account,
                "SOLUSDC",
                "snapshot:first",
                &snapshot,
                &[]
            )
            .await
    );
    for id in ["snapshot:repeat1", "snapshot:repeat2"] {
        assert!(
            first
                .record_initial_order_response_snapshot(account, "SOLUSDC", id, &snapshot, &[])
                .await
        );
    }
    let timeline = db
        .load_initial_order_response_timeline(account, "SOLUSDC", client)
        .unwrap();
    assert_eq!(timeline.len(), 2);
    assert!(timeline.iter().all(|(batch, _)| batch == "snapshot:first"));
    drop(first);

    let mut restarted = engine(db.clone(), &url).await;
    assert!(
        restarted
            .record_initial_order_response_snapshot(
                account,
                "SOLUSDC",
                "snapshot:restart",
                &snapshot,
                &[]
            )
            .await
    );
    assert_eq!(
        db.load_initial_order_response_timeline(account, "SOLUSDC", client)
            .unwrap()
            .len(),
        2
    );
    assert!(
        restarted
            .record_initial_order_response(account, "SOLUSDC", client, &new)
            .await
    );
    assert!(
        restarted
            .record_initial_order_response_snapshot(
                account,
                "SOLUSDC",
                "snapshot:after-query",
                &snapshot,
                &[]
            )
            .await
    );
    assert!(
        restarted
            .record_initial_order_response(account, "SOLUSDC", client, &new)
            .await
    );
    assert!(
        restarted
            .record_initial_order_response(account, "SOLUSDC", client, &partial)
            .await
    );
    assert!(
        restarted
            .record_initial_order_response_snapshot(
                account,
                "SOLUSDC",
                "snapshot:after-second-query",
                &snapshot,
                &[]
            )
            .await
    );
    drop(restarted);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(
        restarted
            .record_initial_order_response_snapshot(
                account,
                "SOLUSDC",
                "snapshot:second-restart",
                &snapshot,
                &[]
            )
            .await
    );
    assert!(
        restarted
            .record_initial_order_response(account, "SOLUSDC", client, &partial)
            .await
    );
    let timeline = db
        .load_initial_order_response_timeline(account, "SOLUSDC", client)
        .unwrap();
    let statuses: Vec<_> = timeline
        .iter()
        .map(|(_, response)| response.status.as_str())
        .collect();
    assert_eq!(
        statuses,
        ["NEW", "PARTIALLY_FILLED", "NEW", "PARTIALLY_FILLED"]
    );
    assert_eq!(timeline[0].0, "snapshot:first");
    assert_eq!(timeline[1].0, "snapshot:first");
    assert!(!timeline[2].0.starts_with("snapshot:"));
    assert!(!timeline[3].0.starts_with("snapshot:"));
    assert_ne!(timeline[2].0, timeline[3].0);
    assert!(
        restarted
            .record_initial_order_response(account, "SOLUSDC", client, &new)
            .await
    );
    assert!(
        restarted
            .record_initial_order_response_snapshot(
                account,
                "SOLUSDC",
                "snapshot:changed",
                &changed,
                &[]
            )
            .await
    );
    assert!(
        restarted
            .record_initial_order_response(account, "SOLUSDC", client, &new)
            .await
    );
    assert!(
        restarted
            .record_initial_order_response(account, "SOLUSDC", client, &new)
            .await
    );
    assert!(
        restarted
            .record_initial_order_response_snapshot(
                account,
                "SOLUSDC",
                "snapshot:after-change",
                &snapshot,
                &[]
            )
            .await
    );
    let timeline = db
        .load_initial_order_response_timeline(account, "SOLUSDC", client)
        .unwrap();
    let statuses: Vec<_> = timeline
        .iter()
        .map(|(_, response)| response.status.as_str())
        .collect();
    assert_eq!(
        statuses,
        [
            "NEW",
            "PARTIALLY_FILLED",
            "NEW",
            "PARTIALLY_FILLED",
            "NEW",
            "PARTIALLY_FILLED",
            "NEW",
            "NEW",
            "NEW",
            "PARTIALLY_FILLED"
        ]
    );
    assert!(!timeline[4].0.starts_with("snapshot:"));
    assert!(!timeline[7].0.starts_with("snapshot:"));
    for (index, batch) in [(5, "snapshot:changed"), (8, "snapshot:after-change")] {
        assert_eq!(timeline[index].0, batch);
        assert_eq!(timeline[index + 1].0, batch);
    }
    server.abort();
}

#[tokio::test]
async fn interleaved_lookup_evidence_is_deduplicated_per_observed_identity() {
    let db = database();
    let (url, server) = exchange_server(Exchange::default()).await;
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    let client = "gb_evidence_timeline";
    let a = evidence_test_response("NEW", dec!(0), 1000);
    let mut b = a.clone();
    b.order_id = 456;
    let mut changed_a = a.clone();
    changed_a.status = "PARTIALLY_FILLED".into();
    changed_a.executed_qty = dec!(0.5);
    changed_a.update_time = Some(2000);
    let snapshot = vec![
        (client.to_string(), a.clone()),
        (client.to_string(), b.clone()),
    ];

    assert!(
        first
            .record_initial_order_response_snapshot(
                account,
                "SOLUSDC",
                "snapshot:identities",
                &snapshot,
                &[]
            )
            .await
    );
    for (index, response) in [&a, &b, &a, &b].iter().enumerate() {
        assert!(
            first
                .record_initial_order_response(account, "SOLUSDC", client, response)
                .await
        );
        assert!(
            first
                .record_initial_order_response_snapshot(
                    account,
                    "SOLUSDC",
                    &format!("snapshot:identity-repeat-{index}"),
                    &snapshot,
                    &[]
                )
                .await
        );
    }
    let timeline = db
        .load_initial_order_response_timeline(account, "SOLUSDC", client)
        .unwrap();
    assert_eq!(timeline.len(), 4);
    assert_eq!(
        timeline
            .iter()
            .map(|(_, response)| response.order_id)
            .collect::<Vec<_>>(),
        [123, 456, 123, 456]
    );
    assert!(!timeline[2].0.starts_with("snapshot:"));
    assert!(!timeline[3].0.starts_with("snapshot:"));
    drop(first);

    let mut restarted = engine(db.clone(), &url).await;
    assert!(
        restarted
            .record_initial_order_response_snapshot(
                account,
                "SOLUSDC",
                "snapshot:identity-restart",
                &snapshot,
                &[]
            )
            .await
    );
    for response in [&a, &b] {
        assert!(
            restarted
                .record_initial_order_response(account, "SOLUSDC", client, response)
                .await
        );
    }
    assert_eq!(
        db.load_initial_order_response_timeline(account, "SOLUSDC", client)
            .unwrap()
            .len(),
        4
    );
    assert!(
        restarted
            .record_initial_order_response(account, "SOLUSDC", client, &changed_a)
            .await
    );
    assert!(
        restarted
            .record_initial_order_response(account, "SOLUSDC", client, &b)
            .await
    );
    assert!(
        restarted
            .record_initial_order_response(account, "SOLUSDC", client, &a)
            .await
    );
    let timeline = db
        .load_initial_order_response_timeline(account, "SOLUSDC", client)
        .unwrap();
    assert_eq!(timeline.len(), 6);
    assert_eq!(timeline[4].1.order_id, 123);
    assert_eq!(timeline[4].1.status, "PARTIALLY_FILLED");
    assert_eq!(timeline[5].1.order_id, 123);
    assert_eq!(timeline[5].1.status, "NEW");
    assert!(!timeline[4].0.starts_with("snapshot:"));
    assert!(!timeline[5].0.starts_with("snapshot:"));
    server.abort();
}

#[tokio::test]
async fn failed_snapshot_batch_retries_atomically_and_only_once() {
    let path = std::env::temp_dir().join(format!("snapshot-retry-{}.db", uuid::Uuid::new_v4()));
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_partial_retry BEFORE INSERT ON initial_order_responses WHEN NEW.response_json LIKE '%PARTIALLY_FILLED%' BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
    let (url, server) = exchange_server(Exchange::default()).await;
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    let client = "gb_evidence_timeline";
    let snapshot = vec![
        (
            client.to_string(),
            evidence_test_response("NEW", dec!(0), 1000),
        ),
        (
            client.to_string(),
            evidence_test_response("PARTIALLY_FILLED", dec!(0.5), 2000),
        ),
    ];
    for _ in 0..2 {
        assert!(
            !first
                .record_initial_order_response_snapshot(
                    account,
                    "SOLUSDC",
                    "snapshot:retry",
                    &snapshot,
                    &[]
                )
                .await
        );
        assert_eq!(
            first.initial_order_responses[&(account, "SOLUSDC".to_string())].len(),
            1
        );
        assert!(db
            .load_initial_order_response_timeline(account, "SOLUSDC", client)
            .unwrap()
            .is_empty());
    }
    connection
        .execute_batch("DROP TRIGGER reject_partial_retry")
        .unwrap();
    assert!(
        first
            .flush_initial_order_responses(account, "SOLUSDC")
            .await
    );
    assert!(!first
        .initial_order_responses
        .contains_key(&(account, "SOLUSDC".to_string())));
    assert!(
        first
            .record_initial_order_response_snapshot(
                account,
                "SOLUSDC",
                "snapshot:next",
                &snapshot,
                &[]
            )
            .await
    );
    let timeline = db
        .load_initial_order_response_timeline(account, "SOLUSDC", client)
        .unwrap();
    assert_eq!(timeline.len(), 2);
    assert_eq!(timeline[0].1.status, "NEW");
    assert_eq!(timeline[1].1.status, "PARTIALLY_FILLED");
    server.abort();
    drop(first);
    drop(connection);
    drop(db);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn terminal_disagreement_queue_retries_failures_and_removes_successes() {
    let path = std::env::temp_dir().join(format!("terminal-retry-{}.db", uuid::Uuid::new_v4()));
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_terminal_retry BEFORE INSERT ON terminal_order_disagreements BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
    let (url, server) = exchange_server(Exchange::default()).await;
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    let client = "gb_evidence_timeline";
    let key = (account, "SOLUSDC".to_string(), client.to_string());
    let filled = evidence_test_response("FILLED", dec!(2), 2000);
    let canceled = evidence_test_response("CANCELED", dec!(0), 3000);
    let expired = evidence_test_response("EXPIRED", dec!(0), 4000);
    assert!(
        !first
            .record_terminal_disagreement(account, "SOLUSDC", client, &filled, &canceled)
            .await
    );
    assert!(
        !first
            .record_terminal_disagreement(account, "SOLUSDC", client, &filled, &expired)
            .await
    );
    assert_eq!(first.terminal_order_disagreements[&key].len(), 2);
    assert!(
        first
            .terminal_disagreement_blocked(account, "SOLUSDC")
            .await
    );
    assert_eq!(first.terminal_order_disagreements[&key].len(), 2);
    assert!(db
        .load_terminal_order_disagreements(account, "SOLUSDC")
        .unwrap()
        .is_empty());

    connection.execute_batch("DROP TRIGGER reject_terminal_retry; CREATE TRIGGER reject_expired_retry BEFORE INSERT ON terminal_order_disagreements WHEN NEW.conflicting_json LIKE '%EXPIRED%' BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
    assert!(
        first
            .terminal_disagreement_blocked(account, "SOLUSDC")
            .await
    );
    assert_eq!(first.terminal_order_disagreements[&key].len(), 1);
    assert_eq!(
        first.terminal_order_disagreements[&key][0].1.status,
        "EXPIRED"
    );
    assert_eq!(
        db.load_terminal_order_disagreements(account, "SOLUSDC")
            .unwrap()
            .len(),
        1
    );
    connection
        .execute_batch("DROP TRIGGER reject_expired_retry")
        .unwrap();
    assert!(
        first
            .terminal_disagreement_blocked(account, "SOLUSDC")
            .await
    );
    assert!(!first.terminal_order_disagreements.contains_key(&key));
    assert_eq!(
        db.load_terminal_order_disagreements(account, "SOLUSDC")
            .unwrap()
            .len(),
        2
    );
    assert!(
        first
            .terminal_disagreement_blocked(account, "SOLUSDC")
            .await
    );
    assert!(!first.terminal_order_disagreements.contains_key(&key));
    assert!(
        first
            .record_terminal_disagreement(account, "SOLUSDC", client, &canceled, &expired)
            .await
    );
    assert!(!first.terminal_order_disagreements.contains_key(&key));
    assert_eq!(
        db.load_terminal_order_disagreements(account, "SOLUSDC")
            .unwrap()
            .len(),
        3
    );
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(
        restarted
            .terminal_disagreement_blocked(account, "SOLUSDC")
            .await
    );
    assert!(restarted.reconciliation_blocked);
    assert_eq!(
        db.load_terminal_order_disagreements(account, "SOLUSDC")
            .unwrap()
            .len(),
        3
    );
    server.abort();
    drop(restarted);
    drop(connection);
    drop(db);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn duplicate_snapshot_fill_evidence_survives_restart_and_preserves_order() {
    for case in ["partial", "filled_zero", "repeated_new"] {
        let db = database();
        let stage = Arc::new(AtomicU8::new(0));
        let snapshot = match case {
            "partial" => vec![("NEW", "0", 1000), ("PARTIALLY_FILLED", "0.5", 2000)],
            "filled_zero" => vec![("PARTIALLY_FILLED", "0.5", 2000), ("FILLED", "0", 3000)],
            _ => vec![("NEW", "0", 1000), ("PARTIALLY_FILLED", "0.5", 2000), ("NEW", "0", 1000)],
        }.into_iter().map(|(status, quantity, time)| json!({"orderId":456,"clientOrderId":"gb_snapshot_evidence",
            "symbol":"SOLUSDC","status":status,"price":"100","avgPrice":"100","origQty":"2",
            "executedQty":quantity,"updateTime":time,"side":"BUY","type":"LIMIT","timeInForce":"GTX"})).collect::<Vec<_>>();
        let app = Router::new()
            .route("/fapi/v1/openOrders", get({
                let stage = stage.clone();
                move || { let stage = stage.clone(); let snapshot = snapshot.clone(); async move {
                    Json(if stage.load(Ordering::SeqCst) == 0 {snapshot} else {Vec::new()})
                }}
            }))
            .route("/fapi/v1/order", get(|| async {
                Json(json!({"orderId":456,"clientOrderId":"gb_snapshot_evidence","symbol":"SOLUSDC",
                    "status":"CANCELED","price":"100","avgPrice":"100","origQty":"2",
                    "executedQty":"0","updateTime":3000,"side":"BUY","type":"LIMIT","timeInForce":"GTX"}))
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut first = engine(db.clone(), &url).await;
        let account = first.client.account_identity_key();
        assert!(!first.sync_live_orders().await, "case={case}");
        assert!(first.reconciliation_blocked);
        let responses = db
            .load_initial_order_responses(account, "SOLUSDC", "gb_snapshot_evidence")
            .unwrap();
        assert!(responses.iter().any(|response| response.order_id == 456
            && response.status == "PARTIALLY_FILLED"
            && response.executed_qty == dec!(0.5)
            && response.update_time == Some(2000)));
        if case == "partial" {
            assert!(responses
                .iter()
                .any(|response| response.status == "NEW" && response.executed_qty == dec!(0)));
        } else if case == "repeated_new" {
            let statuses: Vec<_> = responses
                .iter()
                .map(|response| response.status.as_str())
                .collect();
            assert_eq!(statuses, ["NEW", "PARTIALLY_FILLED", "NEW"]);
        } else {
            assert!(db
                .load_terminal_order_disagreements(account, "SOLUSDC")
                .unwrap()
                .is_empty());
        }
        drop(first);
        stage.store(1, Ordering::SeqCst);
        let mut restarted = engine(db.clone(), &url).await;
        for _ in 0..3 {
            assert!(!restarted.sync_live_orders().await, "case={case}");
            assert!(restarted.reconciliation_blocked);
            assert!(restarted
                .state
                .active_orders
                .read()
                .await
                .contains_key("gb_snapshot_evidence"));
            assert!(db.get_recent_trades(10).unwrap().is_empty());
        }
        let responses = db
            .load_initial_order_responses(account, "SOLUSDC", "gb_snapshot_evidence")
            .unwrap();
        assert!(responses
            .iter()
            .any(|response| response.status == "PARTIALLY_FILLED"
                && response.executed_qty == dec!(0.5)));
        assert!(!db
            .load_terminal_order_disagreements(account, "SOLUSDC")
            .unwrap()
            .is_empty());
        server.abort();
    }
}

#[tokio::test]
async fn duplicate_snapshot_details_must_agree_before_reconciliation() {
    for reversed in [false, true] {
        for mismatch in ["none", "status", "executed_qty", "orig_qty", "price"] {
            let db = database();
            let good = json!({"orderId":123,"clientOrderId":"gb_duplicate_details","symbol":"SOLUSDC",
                "status":"NEW","price":"100","origQty":"2","executedQty":"0",
                "side":"BUY","type":"LIMIT","timeInForce":"GTX"});
            let bad = json!({"orderId":123,"clientOrderId":"gb_duplicate_details","symbol":"SOLUSDC",
                "status":if mismatch == "status" {"PARTIALLY_FILLED"} else {"NEW"},
                "price":if mismatch == "price" {"101"} else {"100"},
                "origQty":if mismatch == "orig_qty" {"3"} else {"2"},
                "executedQty":if mismatch == "status" || mismatch == "executed_qty" {"1"} else {"0"},
                "side":"BUY","type":"LIMIT","timeInForce":"GTX"});
            let snapshot = if reversed {
                vec![bad, good]
            } else {
                vec![good, bad]
            };
            let app = Router::new()
                .route("/fapi/v1/openOrders", get(move || {
                    let snapshot = snapshot.clone();
                    async move { Json(snapshot) }
                }))
                .route("/fapi/v2/positionRisk", get(|| async {
                    Json(json!([{"symbol":"SOLUSDC","positionAmt":"2","entryPrice":"100","markPrice":"100",
                        "unRealizedProfit":"0","liquidationPrice":"0","leverage":"1"}]))
                }))
                .route("/fapi/v2/account", get(|| async {
                    Json(json!({"totalWalletBalance":"1000","totalMarginBalance":"1000","totalUnrealizedProfit":"0",
                        "availableBalance":"1000","assets":[{"asset":"USDC","walletBalance":"1000","marginBalance":"1000",
                        "unrealizedProfit":"0","availableBalance":"1000"}]}))
                }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let order = GridOrder {
                client_order_id: "gb_duplicate_details".into(),
                order_id: Some(123),
                symbol: "SOLUSDC".into(),
                side: OrderSide::Buy,
                price: dec!(100),
                quantity: dec!(2),
                amount_usdc: dec!(200),
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
            let mut engine = engine(db.clone(), &url).await;
            assert_eq!(
                engine.sync_live_orders().await,
                mismatch == "none",
                "reversed={reversed}, mismatch={mismatch}"
            );
            assert_eq!(engine.reconciliation_blocked, mismatch != "none");
            if mismatch != "none" {
                assert!(engine.last_orders_sync.is_none());
                assert!(!engine.state.global_position_ready.load(Ordering::SeqCst));
            }
            let active = engine.state.active_orders.read().await;
            assert_eq!(active.len(), 1);
            let retained = &active[&order.client_order_id];
            assert_eq!(retained.order_id, order.order_id);
            assert_eq!(retained.quantity, order.quantity);
            assert_eq!(retained.price, order.price);
            assert_eq!(retained.status, order.status);
            drop(active);
            let saved = db.load_managed_orders("SOLUSDC").unwrap();
            assert_eq!(saved.len(), 1);
            assert_eq!(saved[0].order_id, order.order_id);
            assert_eq!(saved[0].quantity, order.quantity);
            assert_eq!(saved[0].price, order.price);
            assert_eq!(saved[0].status, order.status);
            server.abort();
        }
    }
}

#[tokio::test]
async fn failed_managed_write_is_durably_recovered_when_next_snapshot_conflicts() {
    let path = std::env::temp_dir().join(format!("mixed-memory-{}.db", uuid::Uuid::new_v4()));
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_discovered_order BEFORE INSERT ON managed_orders BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
    let stage = Arc::new(AtomicU8::new(0));
    let app = Router::new().route("/fapi/v1/openOrders", get({
        let stage = stage.clone();
        move || { let stage = stage.clone(); async move {
            let mut orders = Vec::new();
            if stage.load(Ordering::SeqCst) == 0 {
                orders.push(json!({"orderId":99,"clientOrderId":"gb_memory_only","symbol":"SOLUSDC",
                    "status":"NEW","price":"95","origQty":"2","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}));
            } else {
                for id in [11, 12] {
                    orders.push(json!({"orderId":id,"clientOrderId":"gb_memory_conflict","symbol":"SOLUSDC",
                        "status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}));
                }
            }
            Json(orders)
        }}
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    assert!(!first.sync_live_orders().await);
    assert_eq!(
        first.state.active_orders.read().await["gb_memory_only"].quantity,
        dec!(2)
    );
    assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
    connection
        .execute_batch("DROP TRIGGER reject_discovered_order")
        .unwrap();
    stage.store(1, Ordering::SeqCst);
    assert!(!first.sync_live_orders().await);
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
    assert!(
        saved
            .iter()
            .any(|order| order.client_order_id == "gb_memory_only"
                && order.order_id == Some(99)
                && order.quantity == dec!(2)
                && order.price == dec!(95))
            || observations
                .iter()
                .any(
                    |(expected, found)| expected.client_order_id == "gb_memory_only"
                        && expected.quantity == dec!(2)
                        && expected.price == dec!(95)
                        && found.order_id == 99
                )
    );
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    let active = restarted.state.active_orders.read().await;
    let restored = &active["gb_memory_only"];
    assert_eq!(restored.order_id, Some(99));
    assert_eq!(restored.quantity, dec!(2));
    assert_eq!(restored.price, dec!(95));
    assert_eq!(restored.amount_usdc, dec!(190));
    drop(active);
    server.abort();
    drop(connection);
    drop(restarted);
    drop(db);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn managed_placeholder_does_not_hide_confirmed_memory_identity_on_restart() {
    let db = database();
    let app = Router::new()
        .route("/fapi/v1/openOrders", get(|| async {
            Json(json!([
                {"orderId":11,"clientOrderId":"gb_placeholder_conflict","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                {"orderId":12,"clientOrderId":"gb_placeholder_conflict","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}
            ]))
        }))
        .route("/fapi/v1/order", get(|| async {
            (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code":-1007,"msg":"unavailable"})))
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let order = GridOrder {
        client_order_id: "gb_placeholder_known".into(),
        order_id: None,
        symbol: "SOLUSDC".into(),
        side: OrderSide::Buy,
        price: dec!(95),
        quantity: dec!(2),
        amount_usdc: dec!(190),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: -3,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Grid,
        merge_sources: Vec::new(),
    };
    db.save_managed_order(&order).unwrap();
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    let mut confirmed = order.clone();
    confirmed.order_id = Some(99);
    first
        .state
        .active_orders
        .write()
        .await
        .insert(order.client_order_id.clone(), confirmed);
    assert!(!first.sync_live_orders().await);
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
    assert!(
        saved
            .iter()
            .any(|item| item.client_order_id == order.client_order_id && item.order_id == Some(99))
            || observations
                .iter()
                .any(
                    |(expected, found)| expected.client_order_id == order.client_order_id
                        && (expected.order_id == Some(99) || found.order_id == 99)
                )
    );
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    let active = restarted.state.active_orders.read().await;
    assert_eq!(active["gb_placeholder_known"].order_id, Some(99));
    assert_eq!(active["gb_placeholder_known"].quantity, dec!(2));
    server.abort();
}

#[tokio::test]
async fn memory_order_cannot_overwrite_existing_managed_identity_on_insert_if_absent() {
    for mismatch in ["symbol", "side", "order_id"] {
        let db = database();
        let app = Router::new().route("/fapi/v1/openOrders", get(|| async {
            Json(json!([
                {"orderId":11,"clientOrderId":"gb_collision_conflict","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                {"orderId":12,"clientOrderId":"gb_collision_conflict","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}
            ]))
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let durable = GridOrder {
            client_order_id: "gb_collision_memory".into(),
            order_id: Some(123),
            symbol: if mismatch == "symbol" {
                "BTCUSDC"
            } else {
                "SOLUSDC"
            }
            .into(),
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(1),
            amount_usdc: dec!(100),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: -2,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        db.save_managed_order(&durable).unwrap();
        let mut engine = engine(db.clone(), &url).await;
        let mut memory = durable.clone();
        memory.symbol = "SOLUSDC".into();
        if mismatch == "side" {
            memory.side = OrderSide::Sell;
        }
        if mismatch == "order_id" {
            memory.order_id = Some(456);
        }
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(memory.client_order_id.clone(), memory);
        assert!(!engine.sync_live_orders().await);
        assert!(engine.reconciliation_blocked);
        assert!(engine.last_orders_sync.is_none());
        let saved = db.load_managed_orders(&durable.symbol).unwrap();
        let saved = saved
            .iter()
            .find(|item| item.client_order_id == durable.client_order_id)
            .unwrap();
        assert_eq!(saved.order_id, durable.order_id);
        assert_eq!(saved.symbol, durable.symbol);
        assert_eq!(saved.side, durable.side);
        assert_eq!(saved.grid_level, durable.grid_level);
        server.abort();
    }
}

#[tokio::test]
async fn second_lookup_partial_fill_survives_retry_and_restart() {
    for write_fails in [false, true] {
        let path =
            std::env::temp_dir().join(format!("second-response-write-{}.db", uuid::Uuid::new_v4()));
        let db = Arc::new(Database::open(&path).unwrap());
        db.save_bot_status(BotStatus::Running).unwrap();
        let connection = rusqlite::Connection::open(&path).unwrap();
        let stage = Arc::new(AtomicU8::new(0));
        let partial_queries = Arc::new(AtomicU8::new(0));
        let partial_seen = Arc::new(AtomicBool::new(false));
        let partial_client_queries = Arc::new(AtomicU8::new(0));
        let app = Router::new()
            .route("/fapi/v1/openOrders", get(|| async { Json(json!([])) }))
            .route("/fapi/v1/order", get({
                let stage = stage.clone();
                let partial_queries = partial_queries.clone();
                let partial_seen = partial_seen.clone();
                let partial_client_queries = partial_client_queries.clone();
                move |Query(query): Query<HashMap<String, String>>| {
                    let stage = stage.clone();
                    let partial_queries = partial_queries.clone();
                    let partial_seen = partial_seen.clone();
                    let partial_client_queries = partial_client_queries.clone();
                    async move {
                        let by_client = query.contains_key("origClientOrderId");
                        let id = if by_client {456} else {query["orderId"].parse::<i64>().unwrap()};
                        let partial = stage.load(Ordering::SeqCst) == 0 && id == 456
                            && (!by_client || partial_seen.load(Ordering::SeqCst));
                        if partial && by_client {
                            partial_client_queries.fetch_add(1, Ordering::SeqCst);
                        } else if partial {
                            partial_seen.store(true, Ordering::SeqCst);
                            partial_queries.fetch_add(1, Ordering::SeqCst);
                        }
                        let status = if id == 123 {"CANCELED"} else if partial {"PARTIALLY_FILLED"}
                            else if stage.load(Ordering::SeqCst) == 0 {"NEW"} else {"CANCELED"};
                        Json(json!({"orderId":id,"clientOrderId":"gb_second_partial","symbol":"SOLUSDC",
                            "status":status,"price":"101","avgPrice":"100","origQty":"2",
                            "executedQty":if partial {"0.5"} else {"0"},
                            "updateTime":if partial {2000} else if status == "NEW" {1000} else {3000},
                            "side":"SELL","type":"LIMIT","timeInForce":"GTX"}))
                    }
                }
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let intent = GridOrder {
            client_order_id: "gb_second_partial".into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side: OrderSide::Sell,
            price: dec!(101),
            quantity: dec!(2),
            amount_usdc: dec!(202),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 1,
            paired_client_order_id: Some("gb_second_partial_parent".into()),
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        let trade = TradeRecord {
            trade_id: "grid:gb_second_partial_parent".into(),
            client_order_id: "gb_second_partial_parent".into(),
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Live,
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(2),
            amount_usdc: dec!(200),
            realized_pnl: dec!(0),
            commission: dec!(0),
            pnl_verified: true,
            is_maker: true,
            timestamp: Utc::now(),
            note: "fill".into(),
        };
        db.insert_trade_with_recovery(&trade, &GridStats::default(), Some(&intent), None)
            .unwrap();
        let mut first = engine(db.clone(), &url).await;
        let account = first.client.account_identity_key();
        db.save_identity_observation(
            account,
            &intent,
            &crate::types::ObservedOrderIdentity {
                client_order_id: intent.client_order_id.clone(),
                symbol: intent.symbol.clone(),
                side: "SELL".into(),
                order_id: 123,
            },
        )
        .unwrap();
        if write_fails {
            connection.execute_batch("CREATE TRIGGER reject_second_response BEFORE INSERT ON initial_order_responses WHEN NEW.response_json LIKE '%PARTIALLY_FILLED%' BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
        }
        assert!(!first.sync_live_orders().await);
        assert!(partial_queries.load(Ordering::SeqCst) > 0);
        let client_response = first
            .client
            .lookup_order_by_client_id("SOLUSDC", "gb_second_partial")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(client_response.status, "PARTIALLY_FILLED");
        assert_eq!(client_response.executed_qty, dec!(0.5));
        assert!(partial_client_queries.load(Ordering::SeqCst) > 0);
        assert!(first.reconciliation_blocked);
        assert_eq!(
            db.load_pair_intents("SOLUSDC", TradingMode::Live)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
        if write_fails {
            assert!(!db
                .load_initial_order_responses(account, "SOLUSDC", &intent.client_order_id)
                .unwrap()
                .iter()
                .any(|response| response.status == "PARTIALLY_FILLED"));
            assert!(!first.sync_live_orders().await);
            drop(first);
            let mut failed_restart = engine(db.clone(), &url).await;
            assert!(!failed_restart.sync_live_orders().await);
            assert!(failed_restart.reconciliation_blocked);
            assert_eq!(
                db.load_pair_intents("SOLUSDC", TradingMode::Live)
                    .unwrap()
                    .len(),
                1
            );
            connection
                .execute_batch("DROP TRIGGER reject_second_response")
                .unwrap();
            assert!(!failed_restart.sync_live_orders().await);
            drop(failed_restart);
        } else {
            drop(first);
        }
        let responses = db
            .load_initial_order_responses(account, "SOLUSDC", &intent.client_order_id)
            .unwrap();
        assert!(responses.iter().any(|response| response.order_id == 456
            && response.status == "NEW"
            && response.executed_qty == dec!(0)
            && response.update_time == Some(1000)));
        assert!(responses.iter().any(|response| response.order_id == 456
            && response.status == "PARTIALLY_FILLED"
            && response.executed_qty == dec!(0.5)
            && response.update_time == Some(2000)));
        stage.store(1, Ordering::SeqCst);
        let mut restarted = engine(db.clone(), &url).await;
        for _ in 0..3 {
            assert!(!restarted.sync_live_orders().await);
            assert!(restarted.reconciliation_blocked);
            assert_eq!(
                db.load_pair_intents("SOLUSDC", TradingMode::Live)
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
        }
        let disagreements = db
            .load_terminal_order_disagreements(account, "SOLUSDC")
            .unwrap();
        assert!(disagreements.iter().any(|(client, earlier, later)| client
            == &intent.client_order_id
            && earlier.order_id == 456
            && earlier.status == "PARTIALLY_FILLED"
            && earlier.executed_qty == dec!(0.5)
            && later.order_id == 456
            && later.status == "CANCELED"
            && later.executed_qty == dec!(0)));
        server.abort();
        drop(restarted);
        drop(connection);
        drop(db);
        let _ = std::fs::remove_file(path);
    }
}

#[tokio::test]
async fn partial_client_response_survives_failed_id_lookup_and_restart() {
    for write_fails in [false, true] {
        let path = std::env::temp_dir().join(format!(
            "partial-response-write-{}.db",
            uuid::Uuid::new_v4()
        ));
        let db = Arc::new(Database::open(&path).unwrap());
        db.save_bot_status(BotStatus::Running).unwrap();
        let connection = rusqlite::Connection::open(&path).unwrap();
        let stage = Arc::new(AtomicU8::new(0));
        let failed_id_queries = Arc::new(AtomicU8::new(0));
        let saved_before_query = Arc::new(AtomicBool::new(false));
        let app = Router::new()
        .route("/fapi/v1/openOrders", get(|| async { Json(json!([])) }))
        .route("/fapi/v1/order", get({
            let stage = stage.clone();
            let failed_id_queries = failed_id_queries.clone();
            let saved_before_query = saved_before_query.clone();
            let path = path.clone();
            move |Query(query): Query<HashMap<String, String>>| {
                let stage = stage.clone();
                let failed_id_queries = failed_id_queries.clone();
                let saved_before_query = saved_before_query.clone();
                let path = path.clone();
                async move {
                    let by_client = query.contains_key("origClientOrderId");
                    let id = if by_client {456} else {query["orderId"].parse::<i64>().unwrap()};
                    let initial = stage.load(Ordering::SeqCst) == 0;
                    if initial && !by_client && id == 456 {
                        failed_id_queries.fetch_add(1, Ordering::SeqCst);
                        let conn = rusqlite::Connection::open(&path).unwrap();
                        let count: i64 = conn.query_row("SELECT COUNT(*) FROM initial_order_responses", [], |row| row.get(0)).unwrap();
                        saved_before_query.store(count > 0, Ordering::SeqCst);
                        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code":-1007,"msg":"unavailable"})));
                    }
                    let partial = initial && by_client;
                    (StatusCode::OK, Json(json!({"orderId":id,"clientOrderId":"gb_partial_interrupted",
                        "symbol":"SOLUSDC","status":if partial {"PARTIALLY_FILLED"} else {"CANCELED"},
                        "price":"101","avgPrice":"100","origQty":"2","executedQty":if partial {"0.5"} else {"0"},
                        "updateTime":if partial {1000} else {2000},"side":"SELL","type":"LIMIT","timeInForce":"GTX"})))
                }
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let intent = GridOrder {
            client_order_id: "gb_partial_interrupted".into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side: OrderSide::Sell,
            price: dec!(101),
            quantity: dec!(2),
            amount_usdc: dec!(202),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 1,
            paired_client_order_id: Some("gb_partial_interrupted_parent".into()),
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        let trade = TradeRecord {
            trade_id: "grid:gb_partial_interrupted_parent".into(),
            client_order_id: "gb_partial_interrupted_parent".into(),
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Live,
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(2),
            amount_usdc: dec!(200),
            realized_pnl: dec!(0),
            commission: dec!(0),
            pnl_verified: true,
            is_maker: true,
            timestamp: Utc::now(),
            note: "fill".into(),
        };
        db.insert_trade_with_recovery(&trade, &GridStats::default(), Some(&intent), None)
            .unwrap();
        let mut first = engine(db.clone(), &url).await;
        let account = first.client.account_identity_key();
        db.save_identity_observation(
            account,
            &intent,
            &crate::types::ObservedOrderIdentity {
                client_order_id: intent.client_order_id.clone(),
                symbol: intent.symbol.clone(),
                side: "SELL".into(),
                order_id: 123,
            },
        )
        .unwrap();
        if write_fails {
            connection.execute_batch("CREATE TRIGGER reject_initial_response BEFORE INSERT ON initial_order_responses BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
        }
        assert!(!first.sync_live_orders().await);
        if write_fails {
            assert_eq!(failed_id_queries.load(Ordering::SeqCst), 0);
            assert!(first.reconciliation_blocked);
            assert!(!first.sync_live_orders().await);
            assert_eq!(
                db.load_pair_intents("SOLUSDC", TradingMode::Live)
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
            drop(first);
            let mut restarted = engine(db.clone(), &url).await;
            assert!(!restarted.sync_live_orders().await);
            assert!(restarted.reconciliation_blocked);
            assert_eq!(failed_id_queries.load(Ordering::SeqCst), 0);
            assert_eq!(
                db.load_pair_intents("SOLUSDC", TradingMode::Live)
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
            server.abort();
            drop(restarted);
            drop(connection);
            drop(db);
            let _ = std::fs::remove_file(path);
            continue;
        }
        assert!(failed_id_queries.load(Ordering::SeqCst) > 0);
        assert!(saved_before_query.load(Ordering::SeqCst));
        assert!(first.reconciliation_blocked);
        assert_eq!(
            db.load_pair_intents("SOLUSDC", TradingMode::Live)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
        stage.store(1, Ordering::SeqCst);
        for _ in 0..2 {
            assert!(!first.sync_live_orders().await);
            assert!(first.reconciliation_blocked);
            assert_eq!(
                db.load_pair_intents("SOLUSDC", TradingMode::Live)
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
        }
        let disagreements = db
            .load_terminal_order_disagreements(account, "SOLUSDC")
            .unwrap();
        assert!(disagreements.iter().any(|(client, initial, later)| client
            == &intent.client_order_id
            && initial.order_id == 456
            && initial.status == "PARTIALLY_FILLED"
            && initial.executed_qty == dec!(0.5)
            && initial.update_time == Some(1000)
            && later.order_id == 456
            && later.status == "CANCELED"
            && later.executed_qty == dec!(0)
            && later.update_time == Some(2000)));
        drop(first);
        let mut restarted = engine(db.clone(), &url).await;
        assert!(!restarted.sync_live_orders().await);
        assert!(restarted.reconciliation_blocked);
        assert_eq!(
            db.load_pair_intents("SOLUSDC", TradingMode::Live)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
        assert!(!db
            .load_terminal_order_disagreements(account, "SOLUSDC")
            .unwrap()
            .is_empty());
        server.abort();
        drop(restarted);
        drop(connection);
        drop(db);
        let _ = std::fs::remove_file(path);
    }
}

#[tokio::test]
async fn repeated_lookup_of_same_observed_id_preserves_disagreeing_terminal_responses() {
    let db = database();
    let queries = Arc::new(AtomicU8::new(0));
    let app = Router::new()
        .route("/fapi/v1/openOrders", get(|| async { Json(json!([])) }))
        .route("/fapi/v1/order", get({
            let queries = queries.clone();
            move |Query(query): Query<HashMap<String, String>>| {
                let queries = queries.clone();
                async move {
                    assert_eq!(query.get("orderId").map(String::as_str), Some("456"));
                    let count = queries.fetch_add(1, Ordering::SeqCst);
                    Json(json!({"orderId":456,"clientOrderId":"gb_repeated_observed_id","symbol":"SOLUSDC",
                        "status":"CANCELED","price":"100","avgPrice":"100","origQty":"2",
                        "executedQty":if count.is_multiple_of(2) {"0"} else {"0.5"},
                        "updateTime":1000,"side":"BUY","type":"LIMIT","timeInForce":"GTX"}))
                }
            }
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let expected = GridOrder {
        client_order_id: "gb_repeated_observed_id".into(),
        order_id: Some(123),
        symbol: "SOLUSDC".into(),
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(2),
        amount_usdc: dec!(200),
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
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    for side in ["BUY", "SELL"] {
        db.save_identity_observation(
            account,
            &expected,
            &crate::types::ObservedOrderIdentity {
                client_order_id: expected.client_order_id.clone(),
                symbol: expected.symbol.clone(),
                side: side.into(),
                order_id: 456,
            },
        )
        .unwrap();
    }
    for _ in 0..3 {
        assert!(!first.sync_live_orders().await);
        assert!(first.reconciliation_blocked);
        assert_eq!(
            first.state.active_orders.read().await[&expected.client_order_id].order_id,
            Some(123)
        );
        assert_eq!(
            db.load_managed_orders("SOLUSDC").unwrap()[0].order_id,
            Some(123)
        );
        assert!(db.get_recent_trades(10).unwrap().is_empty());
    }
    assert!(queries.load(Ordering::SeqCst) >= 2);
    let disagreements = db
        .load_terminal_order_disagreements(account, "SOLUSDC")
        .unwrap();
    assert!(disagreements.iter().any(|(client, a, b)| {
        client == &expected.client_order_id
            && a.order_id == 456
            && b.order_id == 456
            && a.status == "CANCELED"
            && b.status == "CANCELED"
            && a.side == "BUY"
            && b.side == "BUY"
            && ((a.executed_qty == dec!(0) && b.executed_qty == dec!(0.5))
                || (a.executed_qty == dec!(0.5) && b.executed_qty == dec!(0)))
    }));
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    assert!(restarted.reconciliation_blocked);
    assert_eq!(
        restarted.state.active_orders.read().await[&expected.client_order_id].order_id,
        Some(123)
    );
    assert!(!db
        .load_terminal_order_disagreements(account, "SOLUSDC")
        .unwrap()
        .is_empty());
    server.abort();
}

#[tokio::test]
async fn pair_order_forward_fill_transition_settles_but_regressions_remain_blocked() {
    for case in [
        "forward",
        "time_regression",
        "quantity_regression",
        "terminal_reversal",
        "avg_price_change",
        "partial_to_new",
        "partial_time_back",
    ] {
        let db = database();
        let client_queries = Arc::new(AtomicU8::new(0));
        let app = Router::new()
            .route("/fapi/v1/openOrders", get(|| async { Json(json!([])) }))
            .route("/fapi/v1/order", get({
                let client_queries = client_queries.clone();
                move |Query(query): Query<HashMap<String, String>>| {
                    let client_queries = client_queries.clone();
                    async move {
                        let by_client = query.contains_key("origClientOrderId");
                        let first = by_client && client_queries.fetch_add(1, Ordering::SeqCst) == 0;
                        let old_identity = query.get("orderId").map(String::as_str) == Some("123");
                        let initial_terminal = first && case == "terminal_reversal";
                        let status = if old_identity {"CANCELED"} else if initial_terminal {"FILLED"}
                            else if first || case == "partial_time_back" {"PARTIALLY_FILLED"}
                            else if case == "partial_to_new" {"NEW"}
                            else if case == "terminal_reversal" || case == "avg_price_change" {"CANCELED"} else {"FILLED"};
                        let executed = if old_identity {"0"} else if initial_terminal {"2"}
                            else if case == "partial_to_new" {if first {"0.5"} else {"0"}}
                            else if case == "partial_time_back" {"0.5"}
                            else if first {if case == "quantity_regression" {"1.5"} else {"1"}}
                            else if case == "terminal_reversal" {"0"} else if case == "quantity_regression" || case == "avg_price_change" {"1"} else {"2"};
                        let time = if old_identity {500} else if first {if case == "time_regression" || case == "partial_time_back" {2000} else {1000}}
                            else if case == "time_regression" || case == "partial_time_back" {1000} else {3000};
                        Json(json!({"orderId":if old_identity {123} else {456},"clientOrderId":"gb_pair_forward_fill",
                            "symbol":"SOLUSDC","status":status,"price":"101",
                            "avgPrice":if case == "avg_price_change" {if first {"100"} else {"200"}} else {"101"},
                            "origQty":"2","executedQty":executed,"updateTime":time,
                            "side":"SELL","type":"LIMIT","timeInForce":"GTX"}))
                    }
                }
            }))
            .route("/fapi/v2/positionRisk", get(|| async {
                Json(json!([{"symbol":"SOLUSDC","positionAmt":"0","entryPrice":"0","markPrice":"100",
                    "unRealizedProfit":"0","liquidationPrice":"0","leverage":"1"}]))
            }))
            .route("/fapi/v2/account", get(|| async {
                Json(json!({"totalWalletBalance":"1000","totalMarginBalance":"1000","totalUnrealizedProfit":"0",
                    "availableBalance":"1000","assets":[{"asset":"USDC","walletBalance":"1000","marginBalance":"1000",
                    "unrealizedProfit":"0","availableBalance":"1000"}]}))
            }))
            .route("/fapi/v1/userTrades", get(|| async {
                Json(json!([{"id":1,"orderId":456,"qty":"2","realizedPnl":"0","commission":"0.01",
                    "commissionAsset":"USDC","maker":true}]))
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let intent = GridOrder {
            client_order_id: "gb_pair_forward_fill".into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side: OrderSide::Sell,
            price: dec!(101),
            quantity: dec!(2),
            amount_usdc: dec!(202),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 1,
            paired_client_order_id: Some("gb_pair_forward_parent".into()),
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        let trade = TradeRecord {
            trade_id: "grid:gb_pair_forward_parent".into(),
            client_order_id: "gb_pair_forward_parent".into(),
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Live,
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(2),
            amount_usdc: dec!(200),
            realized_pnl: dec!(0),
            commission: dec!(0),
            pnl_verified: true,
            is_maker: true,
            timestamp: Utc::now(),
            note: "fill".into(),
        };
        db.insert_trade_with_recovery(&trade, &GridStats::default(), Some(&intent), None)
            .unwrap();
        let mut engine = engine(db.clone(), &url).await;
        let account = engine.client.account_identity_key();
        db.save_identity_observation(
            account,
            &intent,
            &crate::types::ObservedOrderIdentity {
                client_order_id: intent.client_order_id.clone(),
                symbol: intent.symbol.clone(),
                side: "SELL".into(),
                order_id: 123,
            },
        )
        .unwrap();
        let mut settled = false;
        for _ in 0..3 {
            let ready = engine.sync_live_orders().await;
            if case == "forward" && ready {
                settled = true;
                break;
            }
            assert!(!ready, "case={case}");
            assert!(engine.reconciliation_blocked);
            assert_eq!(
                db.load_pair_intents("SOLUSDC", TradingMode::Live)
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
        }
        if case == "forward" {
            assert!(settled);
            assert!(!engine.reconciliation_blocked);
            assert!(!engine
                .state
                .active_orders
                .read()
                .await
                .contains_key(&intent.client_order_id));
            assert!(db
                .load_identity_observations(account, "SOLUSDC")
                .unwrap()
                .is_empty());
            assert_eq!(db.get_recent_trades(10).unwrap().len(), 2);
        } else {
            assert!(!settled);
            assert!(!db
                .load_identity_observations(account, "SOLUSDC")
                .unwrap()
                .is_empty());
            if matches!(
                case,
                "avg_price_change" | "partial_to_new" | "partial_time_back"
            ) {
                let disagreements = db
                    .load_terminal_order_disagreements(account, "SOLUSDC")
                    .unwrap();
                assert!(disagreements.iter().any(|(client, first, conflicting)| {
                    if client != &intent.client_order_id {
                        return false;
                    }
                    match case {
                        "avg_price_change" => {
                            first.executed_qty == dec!(1)
                                && conflicting.executed_qty == dec!(1)
                                && first.avg_price == Some(dec!(100))
                                && conflicting.avg_price == Some(dec!(200))
                        }
                        "partial_to_new" => {
                            first.status == "PARTIALLY_FILLED"
                                && first.executed_qty == dec!(0.5)
                                && conflicting.status == "NEW"
                                && conflicting.executed_qty == dec!(0)
                                && first.update_time == Some(1000)
                                && conflicting.update_time == Some(3000)
                        }
                        "partial_time_back" => {
                            first.status == "PARTIALLY_FILLED"
                                && first.executed_qty == dec!(0.5)
                                && conflicting.status == "PARTIALLY_FILLED"
                                && conflicting.executed_qty == dec!(0.5)
                                && first.update_time == Some(2000)
                                && conflicting.update_time == Some(1000)
                        }
                        _ => false,
                    }
                }));
                drop(engine);
                let mut restarted = self::engine(db.clone(), &url).await;
                assert!(!restarted.sync_live_orders().await);
                assert!(restarted.reconciliation_blocked);
                assert_eq!(
                    db.load_pair_intents("SOLUSDC", TradingMode::Live)
                        .unwrap()
                        .len(),
                    1
                );
                assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
                assert!(!db
                    .load_terminal_order_disagreements(account, "SOLUSDC")
                    .unwrap()
                    .is_empty());
            }
        }
        server.abort();
    }
}

#[tokio::test]
async fn pair_terminal_verification_remains_blocked_on_conflicting_results_or_later_id() {
    for case in [
        "status",
        "executed_qty",
        "price",
        "update_time",
        "first_status",
        "first_qty",
        "first_price",
        "first_update_time",
        "first_filled",
        "later_id",
    ] {
        let db = database();
        let client_queries = Arc::new(AtomicU8::new(0));
        let app = Router::new()
            .route("/fapi/v1/openOrders", get(|| async { Json(json!([])) }))
            .route("/fapi/v1/order", get({
                let client_queries = client_queries.clone();
                move |Query(query): Query<HashMap<String, String>>| {
                    let client_queries = client_queries.clone();
                    async move {
                        let by_client = query.contains_key("origClientOrderId");
                        let count = if by_client { client_queries.fetch_add(1, Ordering::SeqCst) } else {0};
                        let id = if by_client {
                            if case == "later_id" && count >= 2 {789} else {456}
                        } else { query["orderId"].parse::<i64>().unwrap() };
                        let inconsistent_order = !by_client && id == 456;
                        let initial_client = by_client && count == 0;
                        Json(json!({"orderId":id,"clientOrderId":"gb_pair_terminal_disagreement","symbol":"SOLUSDC",
                            "status":if initial_client && case == "first_filled" {"FILLED"}
                                else if (inconsistent_order && case == "status") || (initial_client && case == "first_status") {"EXPIRED"}
                                else {"CANCELED"},
                            "price":if (inconsistent_order && case == "price") || (initial_client && case == "first_price") {"102"} else {"101"},
                            "origQty":"2","executedQty":if initial_client && case == "first_filled" {"2"}
                                else if (inconsistent_order && case == "executed_qty") || (initial_client && case == "first_qty") {"1"} else {"0"},
                            "updateTime":if (inconsistent_order && case == "update_time") || (initial_client && case == "first_update_time") {1000} else {2000},
                            "side":"SELL","type":"LIMIT","timeInForce":"GTX"}))
                    }
                }
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let intent = GridOrder {
            client_order_id: "gb_pair_terminal_disagreement".into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side: OrderSide::Sell,
            price: dec!(101),
            quantity: dec!(2),
            amount_usdc: dec!(202),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 1,
            paired_client_order_id: Some("gb_pair_disagreement_parent".into()),
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        let trade = TradeRecord {
            trade_id: "grid:gb_pair_disagreement_parent".into(),
            client_order_id: "gb_pair_disagreement_parent".into(),
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Live,
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(2),
            amount_usdc: dec!(200),
            realized_pnl: dec!(0),
            commission: dec!(0),
            pnl_verified: true,
            is_maker: true,
            timestamp: Utc::now(),
            note: "fill".into(),
        };
        db.insert_trade_with_recovery(&trade, &GridStats::default(), Some(&intent), None)
            .unwrap();
        let mut first = engine(db.clone(), &url).await;
        let account = first.client.account_identity_key();
        db.save_identity_observation(
            account,
            &intent,
            &crate::types::ObservedOrderIdentity {
                client_order_id: intent.client_order_id.clone(),
                symbol: intent.symbol.clone(),
                side: "SELL".into(),
                order_id: 123,
            },
        )
        .unwrap();
        for round in 0..3 {
            assert!(
                !first.sync_live_orders().await,
                "case={case}, round={round}, client_queries={}",
                client_queries.load(Ordering::SeqCst)
            );
            assert!(first.reconciliation_blocked);
            assert_eq!(
                db.load_pair_intents("SOLUSDC", TradingMode::Live)
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
            assert!(db
                .load_identity_observations(account, "SOLUSDC")
                .unwrap()
                .iter()
                .any(
                    |(expected, found)| expected.client_order_id == intent.client_order_id
                        && (found.order_id == 456
                            || (case == "later_id" && expected.order_id == Some(456)))
                ));
        }
        if case == "later_id" {
            assert!(client_queries.load(Ordering::SeqCst) >= 3);
            assert!(db
                .load_identity_observations(account, "SOLUSDC")
                .unwrap()
                .iter()
                .any(
                    |(expected, found)| expected.client_order_id == intent.client_order_id
                        && found.order_id == 789
                ));
        } else {
            assert!(first
                .state
                .active_orders
                .read()
                .await
                .contains_key(&intent.client_order_id));
        }
        drop(first);
        let mut restarted = engine(db.clone(), &url).await;
        assert!(!restarted.sync_live_orders().await);
        assert!(restarted.reconciliation_blocked);
        assert_eq!(
            db.load_pair_intents("SOLUSDC", TradingMode::Live)
                .unwrap()
                .len(),
            1
        );
        server.abort();
    }
}

#[tokio::test]
async fn pair_terminal_fallback_persists_new_identity_before_unique_verification() {
    for case in [
        "valid",
        "wrong_order_id",
        "wrong_client_side",
        "write_failure",
    ] {
        let path = std::env::temp_dir().join(format!(
            "pair-terminal-fallback-{}.db",
            uuid::Uuid::new_v4()
        ));
        let db = Arc::new(Database::open(&path).unwrap());
        db.save_bot_status(BotStatus::Running).unwrap();
        let connection = rusqlite::Connection::open(&path).unwrap();
        let account_slot = Arc::new(Mutex::new(None::<[u8; 32]>));
        let client_queries = Arc::new(AtomicU8::new(0));
        let order_queries = Arc::new(AtomicU8::new(0));
        let durable_before_query = Arc::new(AtomicBool::new(false));
        let app = Router::new()
            .route("/fapi/v1/openOrders", get(|| async { Json(json!([])) }))
            .route("/fapi/v1/order", get({
                let db = db.clone();
                let account_slot = account_slot.clone();
                let client_queries = client_queries.clone();
                let order_queries = order_queries.clone();
                let durable_before_query = durable_before_query.clone();
                move |Query(query): Query<HashMap<String, String>>| {
                    let db = db.clone();
                    let account_slot = account_slot.clone();
                    let client_queries = client_queries.clone();
                    let order_queries = order_queries.clone();
                    let durable_before_query = durable_before_query.clone();
                    async move {
                        let client_query = query.contains_key("origClientOrderId");
                        let client_count = if client_query { client_queries.fetch_add(1, Ordering::SeqCst) } else {0};
                        let id = if client_query {456} else {query["orderId"].parse::<i64>().unwrap()};
                        if !client_query && id == 456 {
                            order_queries.fetch_add(1, Ordering::SeqCst);
                            let account = account_slot.lock().unwrap().unwrap();
                            let recorded = db.load_identity_observations(account, "SOLUSDC").unwrap().iter()
                                .any(|(expected, found)| expected.client_order_id == "gb_pair_fallback" && found.order_id == 456);
                            durable_before_query.store(recorded, Ordering::SeqCst);
                        }
                        let wrong_id = case == "wrong_order_id" && !client_query && id == 456;
                        let wrong_side = case == "wrong_client_side" && client_query && client_count > 0;
                        Json(json!({"orderId":if wrong_id {789} else {id},"clientOrderId":"gb_pair_fallback",
                            "symbol":"SOLUSDC","status":"CANCELED","price":"101","origQty":"2","executedQty":"0",
                            "side":if wrong_side {"BUY"} else {"SELL"},"type":"LIMIT","timeInForce":"GTX"}))
                    }
                }
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let intent = GridOrder {
            client_order_id: "gb_pair_fallback".into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side: OrderSide::Sell,
            price: dec!(101),
            quantity: dec!(2),
            amount_usdc: dec!(202),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 1,
            paired_client_order_id: Some("gb_pair_fallback_parent".into()),
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Grid,
            merge_sources: Vec::new(),
        };
        let trade = TradeRecord {
            trade_id: "grid:gb_pair_fallback_parent".into(),
            client_order_id: "gb_pair_fallback_parent".into(),
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Live,
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(2),
            amount_usdc: dec!(200),
            realized_pnl: dec!(0),
            commission: dec!(0),
            pnl_verified: true,
            is_maker: true,
            timestamp: Utc::now(),
            note: "fill".into(),
        };
        db.insert_trade_with_recovery(&trade, &GridStats::default(), Some(&intent), None)
            .unwrap();
        let mut first = engine(db.clone(), &url).await;
        let account = first.client.account_identity_key();
        *account_slot.lock().unwrap() = Some(account);
        db.save_identity_observation(
            account,
            &intent,
            &crate::types::ObservedOrderIdentity {
                client_order_id: intent.client_order_id.clone(),
                symbol: intent.symbol.clone(),
                side: "SELL".into(),
                order_id: 123,
            },
        )
        .unwrap();
        if case == "write_failure" {
            connection.execute_batch("CREATE TRIGGER reject_fallback_identity BEFORE INSERT ON order_identity_observations WHEN NEW.observed_order_id = 456 BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
        }
        let completed = first.sync_live_orders().await;
        assert!(client_queries.load(Ordering::SeqCst) > 0);
        if case == "valid" {
            if !completed {
                drop(first);
                let mut restarted = engine(db.clone(), &url).await;
                assert!(restarted.sync_live_orders().await);
            }
            assert!(order_queries.load(Ordering::SeqCst) > 0);
            assert!(client_queries.load(Ordering::SeqCst) >= 2);
            assert!(durable_before_query.load(Ordering::SeqCst));
            assert!(db
                .load_identity_observations(account, "SOLUSDC")
                .unwrap()
                .is_empty());
        } else {
            assert!(!completed, "case={case}");
            assert!(first.reconciliation_blocked);
            assert_eq!(
                db.load_pair_intents("SOLUSDC", TradingMode::Live)
                    .unwrap()
                    .len(),
                1
            );
            let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
            assert!(observations.iter().any(|(_, found)| found.order_id == 123));
            if case == "write_failure" {
                assert!(!observations.iter().any(|(_, found)| found.order_id == 456));
                assert_eq!(order_queries.load(Ordering::SeqCst), 0);
            } else {
                assert!(observations.iter().any(|(_, found)| found.order_id == 456));
                assert!(durable_before_query.load(Ordering::SeqCst));
            }
            drop(first);
            let mut restarted = engine(db.clone(), &url).await;
            assert!(!restarted.sync_live_orders().await);
            assert!(restarted.reconciliation_blocked);
            assert_eq!(
                db.load_pair_intents("SOLUSDC", TradingMode::Live)
                    .unwrap()
                    .len(),
                1
            );
            assert!(restarted
                .state
                .active_orders
                .read()
                .await
                .contains_key("gb_pair_fallback"));
            drop(restarted);
        }
        server.abort();
        drop(connection);
        drop(db);
        let _ = std::fs::remove_file(path);
    }
}

#[tokio::test]
async fn pair_intent_rejects_different_id_on_second_lookup() {
    let db = database();
    let stage = Arc::new(AtomicU8::new(0));
    let app = Router::new().route("/fapi/v1/order", get({
        let stage = stage.clone();
        move || { let stage = stage.clone(); async move {
            let first = stage.load(Ordering::SeqCst) == 0;
            Json(json!({"orderId":if first {123} else {456},"clientOrderId":"gb_pair_twice","symbol":"SOLUSDC",
                "status":if first {"FILLED"} else {"CANCELED"},"price":"101","origQty":"2","executedQty":"0",
                "side":"SELL","type":"LIMIT","timeInForce":"GTX"}))
        }}
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let intent = GridOrder {
        client_order_id: "gb_pair_twice".into(),
        order_id: None,
        symbol: "SOLUSDC".into(),
        side: OrderSide::Sell,
        price: dec!(101),
        quantity: dec!(2),
        amount_usdc: dec!(202),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: 1,
        paired_client_order_id: Some("gb_pair_twice_parent".into()),
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Grid,
        merge_sources: Vec::new(),
    };
    let trade = TradeRecord {
        trade_id: "grid:gb_pair_twice_parent".into(),
        client_order_id: "gb_pair_twice_parent".into(),
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Live,
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(2),
        amount_usdc: dec!(200),
        realized_pnl: dec!(0),
        commission: dec!(0),
        pnl_verified: true,
        is_maker: true,
        timestamp: Utc::now(),
        note: "fill".into(),
    };
    db.insert_trade_with_recovery(&trade, &GridStats::default(), Some(&intent), None)
        .unwrap();
    let mut engine = engine(db.clone(), &url).await;
    let account = engine.client.account_identity_key();
    assert!(
        !engine
            .drain_pair_intents("SOLUSDC", TradingMode::Live)
            .await
    );
    assert_eq!(
        engine.state.active_orders.read().await["gb_pair_twice"].order_id,
        Some(123)
    );
    stage.store(1, Ordering::SeqCst);
    assert!(
        !engine
            .drain_pair_intents("SOLUSDC", TradingMode::Live)
            .await
    );
    assert!(engine.reconciliation_blocked);
    assert_eq!(
        engine.state.active_orders.read().await["gb_pair_twice"].order_id,
        Some(123)
    );
    assert_eq!(
        db.load_pair_intents("SOLUSDC", TradingMode::Live)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        db.load_managed_orders("SOLUSDC").unwrap()[0].order_id,
        Some(123)
    );
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .iter()
        .any(
            |(expected, observed)| expected.client_order_id == "gb_pair_twice"
                && expected.order_id == Some(123)
                && observed.order_id == 456
        ));
    server.abort();
}

#[tokio::test]
async fn failed_remainder_confirmation_survives_cancel_recovery_and_restart() {
    let path = std::env::temp_dir().join(format!(
        "remainder-cancel-restart-{}.db",
        uuid::Uuid::new_v4()
    ));
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    let stage = Arc::new(AtomicU8::new(0));
    let app = Router::new()
        .route("/fapi/v1/openOrders", get(|| async {
            (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code":-1007,"msg":"unavailable"})))
        }))
        .route("/fapi/v1/order", get({
            let stage = stage.clone();
            move || { let stage = stage.clone(); async move {
                let current = stage.load(Ordering::SeqCst);
                if current == 1 {
                    return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code":-1007,"msg":"unavailable"})));
                }
                (StatusCode::OK, Json(json!({"orderId":if current == 0 {123} else {456},
                    "clientOrderId":"gb_remainder_cancel_restart","symbol":"SOLUSDC","status":"CANCELED",
                    "price":"101","origQty":"2","executedQty":"0","side":"SELL","type":"LIMIT","timeInForce":"GTX"})))
            }}
        }).delete(|| async {
            (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code":-1007,"msg":"unavailable"})))
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let plan = RemainderPlan {
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Live,
        sources: Vec::new(),
        phase: RemainderPhase::Submitting,
        target: GridOrder {
            client_order_id: "gb_remainder_cancel_restart".into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side: OrderSide::Sell,
            price: dec!(101),
            quantity: dec!(2),
            amount_usdc: dec!(202),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 1,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Remainder,
            merge_sources: Vec::new(),
        },
    };
    db.save_remainder_plan(&plan).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_cancel_plan_confirmation BEFORE UPDATE ON remainder_plans BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    let mut copy = plan.clone();
    assert!(first.adopt_remainder_target(&mut copy).await.is_err());
    drop(copy);
    assert_eq!(
        db.load_remainder_plan("SOLUSDC", TradingMode::Live)
            .unwrap()
            .unwrap()
            .target
            .order_id,
        None
    );
    assert_eq!(
        first.state.active_orders.read().await["gb_remainder_cancel_restart"].order_id,
        Some(123)
    );
    connection
        .execute_batch("DROP TRIGGER reject_cancel_plan_confirmation")
        .unwrap();
    stage.store(1, Ordering::SeqCst);
    assert!(!first.cancel_all_orders().await);
    assert_eq!(
        first.state.active_orders.read().await["gb_remainder_cancel_restart"].order_id,
        Some(123)
    );
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .iter()
        .any(
            |(expected, observed)| expected.client_order_id == "gb_remainder_cancel_restart"
                && observed.order_id == 123
        ));
    drop(first);
    stage.store(2, Ordering::SeqCst);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    assert!(restarted.reconciliation_blocked);
    assert_eq!(
        restarted.state.active_orders.read().await["gb_remainder_cancel_restart"].order_id,
        Some(123)
    );
    let mut recovered = db
        .load_remainder_plan("SOLUSDC", TradingMode::Live)
        .unwrap()
        .unwrap();
    assert!(restarted
        .adopt_remainder_target(&mut recovered)
        .await
        .is_err());
    assert_eq!(
        restarted.state.active_orders.read().await["gb_remainder_cancel_restart"].order_id,
        Some(123)
    );
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .iter()
        .any(
            |(expected, observed)| expected.client_order_id == "gb_remainder_cancel_restart"
                && observed.order_id == 456
        ));
    server.abort();
    drop(connection);
    drop(restarted);
    drop(db);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn remainder_confirmation_write_failure_retains_identity_barrier_after_restart() {
    let path = std::env::temp_dir().join(format!(
        "remainder-confirm-fail-{}.db",
        uuid::Uuid::new_v4()
    ));
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    let stage = Arc::new(AtomicU8::new(0));
    let app = Router::new()
        .route("/fapi/v1/openOrders", get(|| async { Json(json!([])) }))
        .route("/fapi/v1/order", get({
            let stage = stage.clone();
            move || { let stage = stage.clone(); async move {
                Json(json!({"orderId":if stage.load(Ordering::SeqCst) == 0 {123} else {456},
                    "clientOrderId":"gb_remainder_write_fail","symbol":"SOLUSDC","status":"CANCELED",
                    "price":"101","origQty":"2","executedQty":"0","side":"SELL","type":"LIMIT","timeInForce":"GTX"}))
            }}
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let plan = RemainderPlan {
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Live,
        sources: Vec::new(),
        phase: RemainderPhase::Submitting,
        target: GridOrder {
            client_order_id: "gb_remainder_write_fail".into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side: OrderSide::Sell,
            price: dec!(101),
            quantity: dec!(2),
            amount_usdc: dec!(202),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 1,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Remainder,
            merge_sources: Vec::new(),
        },
    };
    db.save_remainder_plan(&plan).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_plan_confirmation BEFORE UPDATE ON remainder_plans BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    let mut copy = plan.clone();
    assert!(first.adopt_remainder_target(&mut copy).await.is_err());
    assert!(first.reconciliation_blocked);
    assert_eq!(
        first.state.active_orders.read().await["gb_remainder_write_fail"].order_id,
        Some(123)
    );
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .iter()
        .any(
            |(expected, observed)| expected.client_order_id == "gb_remainder_write_fail"
                && observed.order_id == 123
        ));
    drop(copy);
    connection
        .execute_batch("DROP TRIGGER reject_plan_confirmation")
        .unwrap();
    let mut recovered = db
        .load_remainder_plan("SOLUSDC", TradingMode::Live)
        .unwrap()
        .unwrap();
    stage.store(1, Ordering::SeqCst);
    assert!(first.adopt_remainder_target(&mut recovered).await.is_err());
    assert_eq!(
        first.state.active_orders.read().await["gb_remainder_write_fail"].order_id,
        Some(123)
    );
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .iter()
        .any(
            |(expected, observed)| expected.client_order_id == "gb_remainder_write_fail"
                && observed.order_id == 456
        ));
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.sync_live_orders().await);
    assert!(restarted.reconciliation_blocked);
    assert_eq!(
        restarted.state.active_orders.read().await["gb_remainder_write_fail"].order_id,
        Some(123)
    );
    server.abort();
    drop(connection);
    drop(restarted);
    drop(db);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn cancellation_recovery_preserves_all_same_exchange_id_observations() {
    let db = database();
    let app = Router::new()
        .route(
            "/fapi/v1/openOrders",
            get(|| async {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"code":-1007,"msg":"unavailable"})),
                )
            }),
        )
        .route(
            "/fapi/v1/order",
            axum::routing::delete(|| async {
                (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"code":-2011,"msg":"unknown order"})),
                )
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let expected = GridOrder {
        client_order_id: "gb_cancel_merge".into(),
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
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    let key = (account, expected.client_order_id.clone());
    let existing = crate::types::ObservedOrderIdentity {
        client_order_id: "gb_observed_a".into(),
        symbol: "SOLUSDC".into(),
        side: "BUY".into(),
        order_id: 456,
    };
    let pending = crate::types::ObservedOrderIdentity {
        client_order_id: "gb_observed_b".into(),
        symbol: "SOLUSDC".into(),
        side: "SELL".into(),
        order_id: 456,
    };
    db.save_identity_observation(account, &expected, &existing)
        .unwrap();
    first
        .order_identity_conflicts
        .insert(key.clone(), expected.clone());
    first.provisional_order_ids.insert(key.clone());
    first
        .provisional_order_observations
        .insert(key, vec![pending]);
    assert!(!first.cancel_all_orders().await);
    let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
    for (client, side) in [("gb_observed_a", "BUY"), ("gb_observed_b", "SELL")] {
        assert!(observations
            .iter()
            .any(|(_, observed)| observed.client_order_id == client
                && observed.symbol == "SOLUSDC"
                && observed.side == side
                && observed.order_id == 456));
    }
    drop(first);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(!restarted.cancel_all_orders().await);
    let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
    for (client, side) in [("gb_observed_a", "BUY"), ("gb_observed_b", "SELL")] {
        assert!(observations
            .iter()
            .any(|(_, observed)| observed.client_order_id == client
                && observed.symbol == "SOLUSDC"
                && observed.side == side
                && observed.order_id == 456));
    }
    server.abort();
}

#[tokio::test]
async fn terminal_remainder_preserves_confirmed_id_against_later_mismatch() {
    let path =
        std::env::temp_dir().join(format!("remainder-terminal-id-{}.db", uuid::Uuid::new_v4()));
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    let stage = Arc::new(AtomicU8::new(0));
    let app = Router::new().route("/fapi/v1/order", get({
        let stage = stage.clone();
        move || { let stage = stage.clone(); async move {
            Json(json!({"orderId":if stage.load(Ordering::SeqCst) == 0 {123} else {456},
                "clientOrderId":"gb_remainder_twice","symbol":"SOLUSDC","status":"CANCELED",
                "price":"101","origQty":"2","executedQty":"0","side":"SELL","type":"LIMIT","timeInForce":"GTX"}))
        }}
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut plan = RemainderPlan {
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Live,
        sources: Vec::new(),
        phase: RemainderPhase::Submitting,
        target: GridOrder {
            client_order_id: "gb_remainder_twice".into(),
            order_id: None,
            symbol: "SOLUSDC".into(),
            side: OrderSide::Sell,
            price: dec!(101),
            quantity: dec!(2),
            amount_usdc: dec!(202),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 1,
            paired_client_order_id: None,
            is_take_profit: false,
            purpose: crate::types::OrderPurpose::Remainder,
            merge_sources: Vec::new(),
        },
    };
    db.save_remainder_plan(&plan).unwrap();
    let mut engine = engine(db.clone(), &url).await;
    let account = engine.client.account_identity_key();
    assert!(engine.adopt_remainder_target(&mut plan).await.unwrap());
    assert!(!engine
        .state
        .active_orders
        .read()
        .await
        .contains_key("gb_remainder_twice"));
    assert_eq!(plan.target.order_id, Some(123));
    let persisted_json: String = connection
        .query_row(
            "SELECT plan_json FROM remainder_plans WHERE target_client_order_id = ?1",
            ["gb_remainder_twice"],
            |row| row.get(0),
        )
        .unwrap();
    let persisted: RemainderPlan = serde_json::from_str(&persisted_json).unwrap();
    assert_eq!(persisted.target.order_id, Some(123));
    stage.store(1, Ordering::SeqCst);
    assert!(engine.adopt_remainder_target(&mut plan).await.is_err());
    assert!(engine.reconciliation_blocked);
    assert_eq!(plan.target.order_id, Some(123));
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .iter()
        .any(
            |(expected, observed)| expected.client_order_id == "gb_remainder_twice"
                && expected.order_id == Some(123)
                && observed.order_id == 456
        ));
    server.abort();
    drop(connection);
    drop(engine);
    drop(db);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn accepted_intent_without_order_id_survives_missing_snapshot_and_lookup() {
    for remainder in [false, true] {
        for write_fails in [false, true] {
            let path =
                std::env::temp_dir().join(format!("intent-acceptance-{}.db", uuid::Uuid::new_v4()));
            let db = Arc::new(Database::open(&path).unwrap());
            db.save_bot_status(BotStatus::Running).unwrap();
            let connection = rusqlite::Connection::open(&path).unwrap();
            if write_fails {
                connection.execute_batch("CREATE TRIGGER reject_intent_observation BEFORE INSERT ON order_identity_observations BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
            }
            let stage = Arc::new(AtomicU8::new(0));
            let missing_queries = Arc::new(AtomicU8::new(0));
            let app = Router::new()
                .route("/fapi/v1/openOrders", get({
                    let stage = stage.clone();
                    move || { let stage = stage.clone(); async move {
                        if stage.load(Ordering::SeqCst) != 0 { return Json(json!([])); }
                        Json(json!([
                            {"orderId":11,"clientOrderId":"gb_intent_accept_conflict","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                            {"orderId":12,"clientOrderId":"gb_intent_accept_conflict","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                            {"orderId":99,"clientOrderId":"gb_intent_accepted","symbol":"SOLUSDC","status":"NEW","price":"101","origQty":"2","executedQty":"0","side":"SELL","type":"LIMIT","timeInForce":"GTX"}
                        ]))
                    }}
                }))
                .route("/fapi/v1/order", get({
                    let missing_queries = missing_queries.clone();
                    move |Query(query): Query<HashMap<String, String>>| {
                        let missing_queries = missing_queries.clone();
                        async move {
                            if query.get("origClientOrderId").map(String::as_str) == Some("gb_intent_accepted") {
                                missing_queries.fetch_add(1, Ordering::SeqCst);
                                return (StatusCode::BAD_REQUEST, Json(json!({"code":-2013,"msg":"Order does not exist"})));
                            }
                            let id = query.get("orderId").and_then(|id| id.parse::<i64>().ok()).unwrap_or(11);
                            (StatusCode::OK, Json(json!({"orderId":id,
                                "clientOrderId":if id == 99 {"gb_intent_accepted"} else {"gb_intent_accept_conflict"},
                                "symbol":"SOLUSDC","status":if id == 12 {"CANCELED"} else {"NEW"},
                                "price":if id == 99 {"101"} else {"100"},"origQty":if id == 99 {"2"} else {"1"},
                                "executedQty":"0","side":if id == 99 {"SELL"} else {"BUY"},"type":"LIMIT","timeInForce":"GTX"})))
                        }
                    }
                }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let intent = GridOrder {
                client_order_id: "gb_intent_accepted".into(),
                order_id: None,
                symbol: "SOLUSDC".into(),
                side: OrderSide::Sell,
                price: dec!(101),
                quantity: dec!(2),
                amount_usdc: dec!(202),
                status: OrderStatus::New,
                created_at: Utc::now() - chrono::Duration::hours(1),
                updated_at: Utc::now(),
                grid_level: 3,
                paired_client_order_id: Some("gb_accept_parent".into()),
                is_take_profit: false,
                purpose: if remainder {
                    crate::types::OrderPurpose::Remainder
                } else {
                    crate::types::OrderPurpose::Grid
                },
                merge_sources: Vec::new(),
            };
            if remainder {
                db.save_remainder_plan(&RemainderPlan {
                    symbol: "SOLUSDC".into(),
                    mode: TradingMode::Live,
                    sources: Vec::new(),
                    target: intent.clone(),
                    phase: RemainderPhase::Submitting,
                })
                .unwrap();
            } else {
                let trade = TradeRecord {
                    trade_id: "grid:gb_accept_parent".into(),
                    client_order_id: "gb_accept_parent".into(),
                    symbol: "SOLUSDC".into(),
                    mode: TradingMode::Live,
                    side: OrderSide::Buy,
                    price: dec!(100),
                    quantity: dec!(2),
                    amount_usdc: dec!(200),
                    realized_pnl: dec!(0),
                    commission: dec!(0),
                    pnl_verified: true,
                    is_maker: true,
                    timestamp: Utc::now(),
                    note: "fill".into(),
                };
                db.insert_trade_with_recovery(&trade, &GridStats::default(), Some(&intent), None)
                    .unwrap();
            }
            let mut first = engine(db.clone(), &url).await;
            let account = first.client.account_identity_key();
            assert!(!first.sync_live_orders().await);
            assert!(first.reconciliation_blocked);
            assert_eq!(
                first.state.active_orders.read().await["gb_intent_accepted"].quantity,
                dec!(2)
            );
            if !write_fails {
                let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
                assert!(observations
                    .iter()
                    .any(
                        |(expected, found)| expected.client_order_id == "gb_intent_accepted"
                            && found.order_id == 99
                            && found.side == "SELL"
                    ));
                drop(first);
                stage.store(1, Ordering::SeqCst);
                let mut restarted = engine(db.clone(), &url).await;
                assert!(!restarted.sync_live_orders().await);
                assert!(missing_queries.load(Ordering::SeqCst) > 0);
                assert!(restarted.reconciliation_blocked);
                assert_eq!(
                    restarted.state.active_orders.read().await["gb_intent_accepted"].quantity,
                    dec!(2)
                );
                assert!(db
                    .load_identity_observations(account, "SOLUSDC")
                    .unwrap()
                    .iter()
                    .any(
                        |(expected, found)| expected.client_order_id == "gb_intent_accepted"
                            && found.order_id == 99
                    ));
                drop(restarted);
            } else {
                assert!(first.last_orders_sync.is_none());
                drop(first);
            }
            if remainder {
                assert!(db
                    .load_remainder_plan("SOLUSDC", TradingMode::Live)
                    .unwrap()
                    .is_some());
            } else {
                assert_eq!(
                    db.load_pair_intents("SOLUSDC", TradingMode::Live)
                        .unwrap()
                        .len(),
                    1
                );
            }
            server.abort();
            drop(connection);
            drop(db);
            let _ = std::fs::remove_file(path);
        }
    }
}

#[tokio::test]
async fn recovery_intent_in_conflicting_snapshot_is_available_to_cancel_fallback() {
    for remainder in [false, true] {
        let db = database();
        let stage = Arc::new(AtomicU8::new(0));
        let listing_failed = Arc::new(AtomicBool::new(false));
        let fallback_ids = Arc::new(Mutex::new(Vec::<String>::new()));
        let app = Router::new()
            .route("/fapi/v1/openOrders", get({
                let stage = stage.clone();
                let listing_failed = listing_failed.clone();
                move || { let stage = stage.clone(); let listing_failed = listing_failed.clone(); async move {
                    if stage.load(Ordering::SeqCst) != 0 {
                        listing_failed.store(true, Ordering::SeqCst);
                        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code":-1007,"msg":"unavailable"})));
                    }
                    (StatusCode::OK, Json(json!([
                        {"orderId":11,"clientOrderId":"gb_intent_conflict","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                        {"orderId":12,"clientOrderId":"gb_intent_conflict","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                        {"orderId":99,"clientOrderId":"gb_intent_known","symbol":"SOLUSDC","status":"NEW","price":"101","origQty":"2","executedQty":"0","side":"SELL","type":"LIMIT","timeInForce":"GTX"}
                    ])))
                }}
            }))
            .route("/fapi/v1/order", get(|| async {
                (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code":-1007,"msg":"unavailable"})))
            }).delete({
                let listing_failed = listing_failed.clone();
                let fallback_ids = fallback_ids.clone();
                move |Query(query): Query<HashMap<String, String>>| {
                    let listing_failed = listing_failed.clone();
                    let fallback_ids = fallback_ids.clone();
                    async move {
                        if listing_failed.load(Ordering::SeqCst) {
                            if let Some(id) = query.get("orderId") { fallback_ids.lock().unwrap().push(id.clone()); }
                        }
                        (StatusCode::BAD_REQUEST, Json(json!({"code":-2011,"msg":"unknown order"})))
                    }
                }
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let intent = GridOrder {
            client_order_id: "gb_intent_known".into(),
            order_id: Some(99),
            symbol: "SOLUSDC".into(),
            side: OrderSide::Sell,
            price: dec!(101),
            quantity: dec!(2),
            amount_usdc: dec!(202),
            status: OrderStatus::New,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            grid_level: 3,
            paired_client_order_id: Some("gb_intent_parent".into()),
            is_take_profit: false,
            purpose: if remainder {
                crate::types::OrderPurpose::Remainder
            } else {
                crate::types::OrderPurpose::Grid
            },
            merge_sources: Vec::new(),
        };
        if remainder {
            db.save_remainder_plan(&RemainderPlan {
                symbol: "SOLUSDC".into(),
                mode: TradingMode::Live,
                sources: Vec::new(),
                target: intent.clone(),
                phase: RemainderPhase::Submitting,
            })
            .unwrap();
        } else {
            let trade = TradeRecord {
                trade_id: "grid:gb_intent_parent".into(),
                client_order_id: "gb_intent_parent".into(),
                symbol: "SOLUSDC".into(),
                mode: TradingMode::Live,
                side: OrderSide::Buy,
                price: dec!(100),
                quantity: dec!(2),
                amount_usdc: dec!(200),
                realized_pnl: dec!(0),
                commission: dec!(0),
                pnl_verified: true,
                is_maker: true,
                timestamp: Utc::now(),
                note: "fill".into(),
            };
            db.insert_trade_with_recovery(&trade, &GridStats::default(), Some(&intent), None)
                .unwrap();
        }
        let mut engine = engine(db.clone(), &url).await;
        assert!(!engine
            .state
            .active_orders
            .read()
            .await
            .contains_key("gb_intent_known"));
        assert!(!engine.sync_live_orders().await);
        let active = engine.state.active_orders.read().await;
        let restored = &active["gb_intent_known"];
        assert_eq!(restored.order_id, Some(99));
        assert_eq!(restored.quantity, dec!(2));
        assert_eq!(restored.price, dec!(101));
        assert_eq!(restored.grid_level, 3);
        assert_eq!(
            restored.paired_client_order_id.as_deref(),
            Some("gb_intent_parent")
        );
        drop(active);
        stage.store(1, Ordering::SeqCst);
        assert!(!engine.cancel_all_orders().await);
        assert!(listing_failed.load(Ordering::SeqCst));
        assert!(
            fallback_ids.lock().unwrap().iter().any(|id| id == "99"),
            "remainder={remainder}"
        );
        server.abort();
    }
}

#[tokio::test]
async fn ordinary_order_in_conflicting_snapshot_survives_restart_and_later_fill() {
    let db = database();
    let stage = Arc::new(AtomicU8::new(0));
    let normal_queries = Arc::new(AtomicU8::new(0));
    let app = Router::new()
        .route("/fapi/v1/openOrders", get({
            let stage = stage.clone();
            move || { let stage = stage.clone(); async move {
                if stage.load(Ordering::SeqCst) != 0 { return Json(json!([])); }
                Json(json!([
                    {"orderId":11,"clientOrderId":"gb_batch_conflict","symbol":"SOLUSDC","status":"NEW",
                        "price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                    {"orderId":12,"clientOrderId":"gb_batch_conflict","symbol":"SOLUSDC","status":"NEW",
                        "price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                    {"orderId":99,"clientOrderId":"gb_batch_normal","symbol":"SOLUSDC","status":"NEW",
                        "price":"95","origQty":"2","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}
                ]))
            }}
        }))
        .route("/fapi/v1/order", get({
            let normal_queries = normal_queries.clone();
            move |Query(query): Query<HashMap<String, String>>| {
                let normal_queries = normal_queries.clone();
                async move {
                    let client = query.get("origClientOrderId").map(String::as_str);
                    let id = query.get("orderId").and_then(|id| id.parse::<i64>().ok())
                        .unwrap_or(if client == Some("gb_batch_normal") {99} else {11});
                    let normal = id == 99;
                    if normal { normal_queries.fetch_add(1, Ordering::SeqCst); }
                    Json(json!({"orderId":id,"clientOrderId":if normal {"gb_batch_normal"} else {"gb_batch_conflict"},
                        "symbol":"SOLUSDC","status":if normal {"FILLED"} else if id == 12 {"CANCELED"} else {"NEW"},
                        "price":if normal {"95"} else {"100"},"avgPrice":if normal {"95"} else {"0"},
                        "origQty":if normal {"2"} else {"1"},"executedQty":if normal {"2"} else {"0"},
                        "side":"BUY","type":"LIMIT","timeInForce":"GTX"}))
                }
            }
        }))
        .route("/fapi/v2/positionRisk", get(|| async {
            Json(json!([{"symbol":"SOLUSDC","positionAmt":"2","entryPrice":"95","markPrice":"100",
                "unRealizedProfit":"10","liquidationPrice":"0","leverage":"1"}]))
        }))
        .route("/fapi/v2/account", get(|| async {
            Json(json!({"totalWalletBalance":"1000","totalMarginBalance":"1000","totalUnrealizedProfit":"0",
                "availableBalance":"1000","assets":[{"asset":"USDC","walletBalance":"1000","marginBalance":"1000",
                "unrealizedProfit":"0","availableBalance":"1000"}]}))
        }))
        .route("/fapi/v1/userTrades", get(|| async {
            Json(json!([{"id":1,"orderId":99,"qty":"2","realizedPnl":"0","commission":"0.01",
                "commissionAsset":"USDC","maker":true}]))
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    assert!(!first.sync_live_orders().await);
    assert!(first.reconciliation_blocked);
    let active = first.state.active_orders.read().await;
    let normal = &active["gb_batch_normal"];
    assert_eq!(normal.quantity, dec!(2));
    assert_eq!(normal.price, dec!(95));
    assert_eq!(normal.amount_usdc, dec!(190));
    drop(active);
    let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
    assert!(observations
        .iter()
        .any(
            |(expected, observed)| expected.client_order_id == "gb_batch_normal"
                && expected.quantity == dec!(2)
                && expected.price == dec!(95)
                && observed.order_id == 99
        ));
    drop(first);
    stage.store(1, Ordering::SeqCst);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(restarted.sync_live_orders().await);
    assert!(normal_queries.load(Ordering::SeqCst) > 0);
    assert!(!restarted
        .state
        .active_orders
        .read()
        .await
        .contains_key("gb_batch_normal"));
    let trades = db.get_recent_trades(10).unwrap();
    assert_eq!(trades.len(), 1);
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .iter()
        .all(|(expected, _)| expected.client_order_id != "gb_batch_normal"));
    server.abort();
}

#[tokio::test]
async fn mixed_snapshot_persistence_failure_retains_all_observed_reservations_in_memory() {
    let path = std::env::temp_dir().join(format!("mixed-snapshot-{}.db", uuid::Uuid::new_v4()));
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_mixed_observations BEFORE INSERT ON order_identity_observations BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
    let app = Router::new().route("/fapi/v1/openOrders", get(|| async {
        Json(json!([
            {"orderId":11,"clientOrderId":"gb_failed_conflict","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
            {"orderId":12,"clientOrderId":"gb_failed_conflict","symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
            {"orderId":99,"clientOrderId":"gb_failed_normal","symbol":"SOLUSDC","status":"NEW","price":"95","origQty":"2","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}
        ]))
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut engine = engine(db.clone(), &url).await;
    assert!(!engine.sync_live_orders().await);
    assert!(engine.reconciliation_blocked);
    assert!(engine.last_orders_sync.is_none());
    let active = engine.state.active_orders.read().await;
    assert_eq!(active["gb_failed_normal"].quantity, dec!(2));
    assert_eq!(active["gb_failed_normal"].price, dec!(95));
    assert_eq!(active["gb_failed_normal"].amount_usdc, dec!(190));
    assert!(active.contains_key("gb_failed_conflict"));
    drop(active);
    server.abort();
    drop(connection);
    drop(engine);
    drop(db);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn known_id_does_not_bypass_unique_lookup_for_unverified_order() {
    let db = database();
    let stage = Arc::new(AtomicU8::new(0));
    let client_lookups = Arc::new(AtomicU8::new(0));
    let app = Router::new()
        .route("/fapi/v1/openOrders", get(|| async { Json(json!([])) }))
        .route("/fapi/v1/order", get({
            let stage = stage.clone();
            let client_lookups = client_lookups.clone();
            move |Query(query): Query<HashMap<String, String>>| {
                let stage = stage.clone();
                let client_lookups = client_lookups.clone();
                async move {
                    if query.contains_key("origClientOrderId") {
                        client_lookups.fetch_add(1, Ordering::SeqCst);
                        if stage.load(Ordering::SeqCst) == 0 {
                            return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code":-1007,"msg":"unavailable"})));
                        }
                    }
                    (StatusCode::OK, Json(json!({"orderId":123,"clientOrderId":"gb_known_unverified",
                        "symbol":"SOLUSDC","status":"NEW","price":"100","origQty":"1","executedQty":"0",
                        "side":"BUY","type":"LIMIT","timeInForce":"GTX"})))
                }
            }
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut engine = engine(db.clone(), &url).await;
    let account = engine.client.account_identity_key();
    let expected = GridOrder {
        client_order_id: "gb_known_unverified".into(),
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
        purpose: crate::types::OrderPurpose::Unverified,
        merge_sources: Vec::new(),
    };
    db.save_identity_observation(
        account,
        &expected,
        &crate::types::ObservedOrderIdentity {
            client_order_id: expected.client_order_id.clone(),
            symbol: expected.symbol.clone(),
            side: "BUY".into(),
            order_id: 123,
        },
    )
    .unwrap();
    assert!(!engine.sync_live_orders().await);
    assert!(client_lookups.load(Ordering::SeqCst) > 0);
    assert!(engine.reconciliation_blocked);
    assert!(!db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .is_empty());
    stage.store(1, Ordering::SeqCst);
    assert!(engine.sync_live_orders().await);
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .is_empty());
    let active = engine.state.active_orders.read().await;
    assert_eq!(active["gb_known_unverified"].order_id, Some(123));
    assert_eq!(
        active["gb_known_unverified"].purpose,
        crate::types::OrderPurpose::Legacy
    );
    server.abort();
}

#[tokio::test]
async fn persistent_duplicate_snapshot_resolves_through_unique_query_and_real_price() {
    for canceled_other in [false, true] {
        for reversed in [false, true] {
            let db = database();
            let lookups = Arc::new(AtomicU8::new(0));
            let first = json!({"orderId":123,"clientOrderId":"gb_query_duplicate","symbol":"SOLUSDC",
                "status":"NEW","price":if canceled_other {"95"} else {"150"},"origQty":if canceled_other {"2"} else {"3"},
                "executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"});
            let second = json!({"orderId":if canceled_other {456} else {123},"clientOrderId":"gb_query_duplicate","symbol":"SOLUSDC",
                "status":"NEW","price":if canceled_other {"150"} else {"120"},"origQty":if canceled_other {"3"} else {"2"},
                "executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"});
            let snapshot = if reversed {
                vec![second, first]
            } else {
                vec![first, second]
            };
            let app = Router::new()
                .route("/fapi/v1/openOrders", get(move || {
                    let snapshot = snapshot.clone();
                    async move { Json(snapshot) }
                }))
                .route("/fapi/v1/order", get({
                    let lookups = lookups.clone();
                    move |Query(query): Query<HashMap<String, String>>| {
                        let lookups = lookups.clone();
                        async move {
                            if query.contains_key("origClientOrderId") { lookups.fetch_add(1, Ordering::SeqCst); }
                            let canceled = query.get("orderId").map(String::as_str) == Some("456");
                            Json(json!({"orderId":if canceled {456} else {123},"clientOrderId":"gb_query_duplicate",
                                "symbol":"SOLUSDC","status":if canceled {"CANCELED"} else {"NEW"},
                                "price":if canceled {"150"} else {"95"},"origQty":if canceled {"3"} else {"2"},"executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}))
                        }
                    }
                }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let mut engine = engine(db.clone(), &url).await;
            let account = engine.client.account_identity_key();
            assert!(!engine.sync_live_orders().await);
            assert!(!db
                .load_identity_observations(account, "SOLUSDC")
                .unwrap()
                .is_empty());
            assert_eq!(
                engine.state.active_orders.read().await["gb_query_duplicate"].price,
                dec!(150)
            );
            assert_eq!(
                engine.state.active_orders.read().await["gb_query_duplicate"].quantity,
                dec!(3)
            );
            assert!(db.get_recent_trades(10).unwrap().is_empty());
            if !canceled_other {
                for _ in 0..2 {
                    assert!(!engine.sync_live_orders().await, "reversed={reversed}");
                    assert!(engine.reconciliation_blocked);
                    assert!(db.get_recent_trades(10).unwrap().is_empty());
                }
                let responses = db
                    .load_initial_order_responses(account, "SOLUSDC", "gb_query_duplicate")
                    .unwrap();
                for (price, quantity) in [(dec!(150), dec!(3)), (dec!(120), dec!(2))] {
                    assert!(responses.iter().any(|response| response.order_id == 123
                        && response.price == price
                        && response.orig_qty == quantity));
                }
                let disagreements = db
                    .load_terminal_order_disagreements(account, "SOLUSDC")
                    .unwrap();
                assert!(disagreements.iter().any(|(client, first, second)| client
                    == "gb_query_duplicate"
                    && first.order_id == 123
                    && second.order_id == 123
                    && (first.price != second.price || first.orig_qty != second.orig_qty)));
                drop(engine);
                let mut restarted = self::engine(db.clone(), &url).await;
                assert!(!restarted.sync_live_orders().await);
                assert!(restarted.reconciliation_blocked);
                assert!(db.get_recent_trades(10).unwrap().is_empty());
                let responses = db
                    .load_initial_order_responses(account, "SOLUSDC", "gb_query_duplicate")
                    .unwrap();
                for (price, quantity) in [(dec!(150), dec!(3)), (dec!(120), dec!(2))] {
                    assert!(responses.iter().any(|response| response.order_id == 123
                        && response.price == price
                        && response.orig_qty == quantity));
                }
                assert!(!db
                    .load_terminal_order_disagreements(account, "SOLUSDC")
                    .unwrap()
                    .is_empty());
                server.abort();
                continue;
            }
            assert!(
                engine.sync_live_orders().await,
                "canceled_other={canceled_other}, reversed={reversed}"
            );
            assert!(lookups.load(Ordering::SeqCst) > 0);
            assert!(!engine.reconciliation_blocked);
            assert!(db
                .load_identity_observations(account, "SOLUSDC")
                .unwrap()
                .is_empty());
            let active = engine.state.active_orders.read().await;
            let order = &active["gb_query_duplicate"];
            assert_eq!(order.order_id, Some(123));
            assert_eq!(order.price, dec!(95));
            assert_eq!(order.quantity, dec!(2));
            assert_eq!(order.amount_usdc, dec!(190));
            drop(active);
            let saved = db.load_managed_orders("SOLUSDC").unwrap();
            assert_eq!(saved.len(), 1);
            assert_eq!(saved[0].price, dec!(95));
            assert_eq!(saved[0].amount_usdc, dec!(190));
            assert!(db.get_recent_trades(10).unwrap().is_empty());
            server.abort();
        }
    }
}

#[tokio::test]
async fn suspicious_and_ordinary_conflicts_in_same_snapshot_are_all_recorded() {
    for invalid in [false, true] {
        let db = database();
        let mut snapshot = vec![
            json!({"orderId":456,"clientOrderId":"gb_batch_known","symbol":"SOLUSDC",
            "status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}),
            json!({"orderId":11,"clientOrderId":"gb_batch_suspicious","symbol":"SOLUSDC",
            "status":"NEW","price":"100","origQty":"1","executedQty":"0",
            "side":if invalid {"INVALID"} else {"BUY"},"type":"LIMIT","timeInForce":"GTX"}),
        ];
        if !invalid {
            snapshot.push(json!({"orderId":12,"clientOrderId":"gb_batch_suspicious","symbol":"SOLUSDC",
                "status":"NEW","price":"100","origQty":"1","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}));
        }
        let app = Router::new().route(
            "/fapi/v1/openOrders",
            get(move || {
                let snapshot = snapshot.clone();
                async move { Json(snapshot) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let order = GridOrder {
            client_order_id: "gb_batch_known".into(),
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
        let mut engine = engine(db.clone(), &url).await;
        let account = engine.client.account_identity_key();
        assert!(!engine.sync_live_orders().await);
        assert!(engine.reconciliation_blocked);
        let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
        assert!(observations
            .iter()
            .any(
                |(expected, found)| expected.client_order_id == "gb_batch_known"
                    && found.order_id == 456
            ));
        assert!(observations
            .iter()
            .any(
                |(expected, found)| expected.client_order_id == "gb_batch_suspicious"
                    && found.order_id == 11
            ));
        if !invalid {
            assert!(observations
                .iter()
                .any(
                    |(expected, found)| expected.client_order_id == "gb_batch_suspicious"
                        && found.order_id == 12
                ));
        }
        assert_eq!(
            db.load_managed_orders("SOLUSDC").unwrap()[0].order_id,
            Some(123)
        );
        server.abort();
    }
}

#[tokio::test]
async fn untrusted_invalid_side_can_resolve_to_uniquely_confirmed_sell() {
    let db = database();
    let stage = Arc::new(AtomicU8::new(0));
    let client_lookups = Arc::new(AtomicU8::new(0));
    let app = Router::new()
        .route("/fapi/v1/openOrders", get({
            let stage = stage.clone();
            move || { let stage = stage.clone(); async move {
                if stage.load(Ordering::SeqCst) != 0 { return Json(json!([])); }
                Json(json!([{"orderId":123,"clientOrderId":"gb_invalid_side","symbol":"SOLUSDC",
                    "status":"NEW","price":"100","origQty":"1","executedQty":"0",
                    "side":"INVALID","type":"LIMIT","timeInForce":"GTX"}]))
            }}
        }))
        .route("/fapi/v1/order", get({
            let client_lookups = client_lookups.clone();
            move |Query(query): Query<HashMap<String, String>>| {
                let client_lookups = client_lookups.clone();
                async move {
                    if query.contains_key("origClientOrderId") { client_lookups.fetch_add(1, Ordering::SeqCst); }
                    Json(json!({"orderId":123,"clientOrderId":"gb_invalid_side","symbol":"SOLUSDC",
                        "status":"NEW","price":"100","origQty":"1","executedQty":"0",
                        "side":"SELL","type":"LIMIT","timeInForce":"GTX"}))
                }
            }
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut first = engine(db.clone(), &url).await;
    let account = first.client.account_identity_key();
    assert!(!first.sync_live_orders().await);
    drop(first);
    stage.store(1, Ordering::SeqCst);
    let mut restarted = engine(db.clone(), &url).await;
    assert!(restarted.sync_live_orders().await);
    assert!(client_lookups.load(Ordering::SeqCst) > 0);
    let active = restarted.state.active_orders.read().await;
    assert_eq!(active["gb_invalid_side"].order_id, Some(123));
    assert_eq!(active["gb_invalid_side"].side, OrderSide::Sell);
    drop(active);
    assert!(db
        .load_identity_observations(account, "SOLUSDC")
        .unwrap()
        .is_empty());
    server.abort();
}

#[tokio::test]
async fn provisional_reservation_grows_without_changing_managed_metadata() {
    let db = database();
    let stage = Arc::new(AtomicU8::new(0));
    let app = Router::new().route("/fapi/v1/openOrders", get({
        let stage = stage.clone();
        move || { let stage = stage.clone(); async move {
            let later = stage.load(Ordering::SeqCst) != 0;
            Json(json!([
                {"orderId":11,"clientOrderId":"gb_growing","symbol":"SOLUSDC","status":"NEW",
                    "price":if later {"120"} else {"100"},"origQty":if later {"4"} else {"1"},
                    "executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                {"orderId":12,"clientOrderId":"gb_growing","symbol":"SOLUSDC","status":"NEW",
                    "price":if later {"130"} else {"101"},"origQty":if later {"5"} else {"2"},
                    "executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                {"orderId":123,"clientOrderId":"gb_preserved","symbol":"SOLUSDC","status":"NEW",
                    "price":"140","origQty":"6","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"},
                {"orderId":456,"clientOrderId":"gb_preserved","symbol":"SOLUSDC","status":"NEW",
                    "price":"150","origQty":"7","executedQty":"0","side":"BUY","type":"LIMIT","timeInForce":"GTX"}
            ]))
        }}
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let order = GridOrder {
        client_order_id: "gb_preserved".into(),
        order_id: Some(123),
        symbol: "SOLUSDC".into(),
        side: OrderSide::Buy,
        price: dec!(90),
        quantity: dec!(2),
        amount_usdc: dec!(180),
        status: OrderStatus::New,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        grid_level: -8,
        paired_client_order_id: Some("gb_parent".into()),
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Grid,
        merge_sources: Vec::new(),
    };
    db.save_managed_order(&order).unwrap();
    let mut engine = engine(db.clone(), &url).await;
    assert!(!engine.sync_live_orders().await);
    stage.store(1, Ordering::SeqCst);
    assert!(!engine.sync_live_orders().await);
    assert!(engine.reconciliation_blocked);
    let active = engine.state.active_orders.read().await;
    let provisional = &active["gb_growing"];
    assert_eq!(provisional.order_id, None);
    assert_eq!(provisional.price, dec!(130));
    assert_eq!(provisional.quantity, dec!(5));
    assert_eq!(provisional.amount_usdc, dec!(650));
    let managed = &active["gb_preserved"];
    assert_eq!(managed.order_id, order.order_id);
    assert_eq!(managed.price, order.price);
    assert_eq!(managed.quantity, order.quantity);
    assert_eq!(managed.grid_level, order.grid_level);
    assert_eq!(managed.paired_client_order_id, order.paired_client_order_id);
    drop(active);
    let saved = db.load_managed_orders("SOLUSDC").unwrap();
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].price, order.price);
    assert_eq!(saved[0].quantity, order.quantity);
    assert_eq!(saved[0].grid_level, order.grid_level);
    assert_eq!(
        saved[0].paired_client_order_id,
        order.paired_client_order_id
    );
    server.abort();
}

#[tokio::test]
async fn untrusted_snapshot_identities_survive_restart_and_missing_snapshot() {
    for case in ["status", "quantity", "price", "symbol", "side"] {
        for reversed in [false, true] {
            let db = database();
            let stage = Arc::new(AtomicU8::new(0));
            let lookups = Arc::new(AtomicU8::new(0));
            let good = json!({"orderId":123,"clientOrderId":"gb_untrusted_snapshot","symbol":"SOLUSDC",
                "status":"NEW","price":"100","origQty":"2","executedQty":"0",
                "side":"BUY","type":"LIMIT","timeInForce":"GTX"});
            let bad = json!({"orderId":123,"clientOrderId":"gb_untrusted_snapshot",
                "symbol":if case == "symbol" {"BTCUSDC"} else {"SOLUSDC"},
                "status":if case == "status" {"PARTIALLY_FILLED"} else {"NEW"},
                "price":if case == "price" {"110"} else {"100"},
                "origQty":if case == "quantity" {"3"} else {"2"},
                "executedQty":if case == "status" {"1"} else {"0"},
                "side":if case == "side" {"INVALID"} else {"BUY"},"type":"LIMIT","timeInForce":"GTX"});
            let snapshot = if case == "symbol" || case == "side" {
                vec![bad]
            } else if reversed {
                vec![bad, good]
            } else {
                vec![good, bad]
            };
            let app = Router::new()
                .route(
                    "/fapi/v1/openOrders",
                    get({
                        let stage = stage.clone();
                        move || {
                            let snapshot = snapshot.clone();
                            let stage = stage.clone();
                            async move {
                                Json(if stage.load(Ordering::SeqCst) == 0 {
                                    snapshot
                                } else {
                                    vec![]
                                })
                            }
                        }
                    }),
                )
                .route(
                    "/fapi/v1/order",
                    get({
                        let lookups = lookups.clone();
                        move || {
                            let lookups = lookups.clone();
                            async move {
                                lookups.fetch_add(1, Ordering::SeqCst);
                                (
                                    StatusCode::SERVICE_UNAVAILABLE,
                                    Json(json!({"code":-1007,"msg":"unavailable"})),
                                )
                            }
                        }
                    }),
                );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let mut first = engine(db.clone(), &url).await;
            let account = first.client.account_identity_key();
            assert!(
                !first.sync_live_orders().await,
                "case={case}, reversed={reversed}"
            );
            assert!(first.reconciliation_blocked);
            let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
            assert!(!observations.is_empty(), "case={case}, reversed={reversed}");
            assert!(observations
                .iter()
                .any(
                    |(_, observed)| observed.client_order_id == "gb_untrusted_snapshot"
                        && observed.order_id == 123
                        && observed.symbol
                            == if case == "symbol" {
                                "BTCUSDC"
                            } else {
                                "SOLUSDC"
                            }
                        && observed.side == if case == "side" { "INVALID" } else { "BUY" }
                ));
            drop(first);
            let mut restarted = engine(db.clone(), &url).await;
            assert!(!restarted.sync_live_orders().await);
            assert!(restarted.reconciliation_blocked);
            lookups.store(0, Ordering::SeqCst);
            stage.store(1, Ordering::SeqCst);
            assert!(!restarted.sync_live_orders().await);
            assert!(lookups.load(Ordering::SeqCst) > 0);
            assert!(restarted.reconciliation_blocked);
            assert!(restarted.last_orders_sync.is_none());
            assert!(!restarted.state.global_position_ready.load(Ordering::SeqCst));
            assert!(!db
                .load_identity_observations(account, "SOLUSDC")
                .unwrap()
                .is_empty());
            assert_eq!(
                restarted.state.active_orders.read().await["gb_untrusted_snapshot"].order_id,
                None
            );
            server.abort();
        }
    }
}

#[tokio::test]
async fn unknown_duplicate_reservation_is_conservative_and_independent_of_input_order() {
    for indices in [[0, 1, 2, 3], [3, 2, 1, 0], [2, 0, 3, 1], [1, 3, 0, 2]] {
        let db = database();
        let records: Vec<_> = [(123, "90", "1"), (123, "100", "2"), (123, "110", "3"), (456, "105", "4")]
            .into_iter().map(|(id, price, quantity)| json!({"orderId":id,"clientOrderId":"gb_reservation_order",
                "symbol":"SOLUSDC","status":"NEW","price":price,"origQty":quantity,"executedQty":"0",
                "side":"BUY","type":"LIMIT","timeInForce":"GTX"})).collect();
        let snapshot: Vec<_> = indices
            .into_iter()
            .map(|index| records[index].clone())
            .collect();
        let app = Router::new().route(
            "/fapi/v1/openOrders",
            get(move || {
                let snapshot = snapshot.clone();
                async move { Json(snapshot) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut engine = engine(db.clone(), &url).await;
        assert!(!engine.sync_live_orders().await);
        assert!(engine.reconciliation_blocked);
        let active = engine.state.active_orders.read().await;
        let retained = &active["gb_reservation_order"];
        assert_eq!(retained.order_id, None);
        assert_eq!(retained.price, dec!(110));
        assert_eq!(retained.quantity, dec!(4));
        assert_eq!(retained.amount_usdc, dec!(440));
        drop(active);
        let observations = db
            .load_identity_observations(engine.client.account_identity_key(), "SOLUSDC")
            .unwrap();
        let ids: std::collections::HashSet<_> = observations
            .iter()
            .map(|(_, observed)| observed.order_id)
            .collect();
        assert_eq!(ids, [123, 456].into_iter().collect());
        server.abort();
    }
}

#[tokio::test]
async fn conflicting_duplicate_snapshot_does_not_adopt_unknown_order() {
    for reversed in [false, true] {
        let db = database();
        let first = json!({"orderId":123,"clientOrderId":"gb_duplicate_unknown","symbol":"SOLUSDC",
            "status":"NEW","price":"100","origQty":"1","executedQty":"0",
            "side":"BUY","type":"LIMIT","timeInForce":"GTX"});
        let second = json!({"orderId":456,"clientOrderId":"gb_duplicate_unknown","symbol":"SOLUSDC",
            "status":"NEW","price":"100","origQty":"1","executedQty":"0",
            "side":"BUY","type":"LIMIT","timeInForce":"GTX"});
        let snapshot = if reversed {
            vec![second, first]
        } else {
            vec![first, second]
        };
        let app = Router::new()
            .route(
                "/fapi/v1/openOrders",
                get(move || {
                    let snapshot = snapshot.clone();
                    async move { Json(snapshot) }
                }),
            )
            .route(
                "/fapi/v1/order",
                get(|| async {
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json(json!({"code":-1007,"msg":"unavailable"})),
                    )
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut first = engine(db.clone(), &url).await;
        let account = first.client.account_identity_key();
        assert!(!first.sync_live_orders().await);
        drop(first);
        let mut restarted = engine(db.clone(), &url).await;
        assert!(!restarted.sync_live_orders().await);
        assert!(restarted.reconciliation_blocked);
        assert!(restarted.last_orders_sync.is_none());
        assert!(!restarted.state.global_position_ready.load(Ordering::SeqCst));
        assert_eq!(
            restarted.state.active_orders.read().await["gb_duplicate_unknown"].order_id,
            None
        );
        assert!(db.load_managed_orders("SOLUSDC").unwrap().is_empty());
        let observations = db.load_identity_observations(account, "SOLUSDC").unwrap();
        let ids: std::collections::HashSet<_> = observations
            .iter()
            .map(|(_, observed)| observed.order_id)
            .collect();
        assert_eq!(ids, [123, 456].into_iter().collect());
        server.abort();
    }
}

#[tokio::test]
async fn switching_to_exchange_modes_requires_manual_resume_before_orders() {
    for (source, target, previously_running) in [
        (TradingMode::Paper, TradingMode::Live, false),
        (TradingMode::Paper, TradingMode::Live, true),
    ] {
        let fake = Exchange::default();
        let (url, server) = exchange_server(fake.clone()).await;
        let db = database();
        let mut engine = engine(db.clone(), &url).await;
        let mut initial = engine.state.config.read().await.clone();
        if source == TradingMode::Paper {
            initial.exchange.api_key.clear();
            initial.exchange.api_secret.clear();
        } else {
            initial.exchange.api_key = "live-key".into();
            initial.exchange.api_secret = "live-secret".into();
        }
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
        exchange.exchange.api_key = "live-key".into();
        exchange.exchange.api_secret = "live-secret".into();
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
    engine.state.config.write().await.exchange.api_key.clear();
    engine
        .state
        .config
        .write()
        .await
        .exchange
        .api_secret
        .clear();
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
