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
    atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
    Mutex,
};

#[derive(Clone, Default)]
struct Exchange {
    orders: Arc<Mutex<HashMap<String, BinanceOrderResponse>>>,
    posts: Arc<Mutex<Vec<String>>>,
    // 0: accepted, 1: accepted+timeout, 2: timeout unaccepted, 3: GTX rejected, 4: order rejected.
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
    position_calls: Arc<AtomicUsize>,
    account_calls: Arc<AtomicUsize>,
    pnl_calls: Arc<AtomicUsize>,
    block_pnl: Arc<AtomicBool>,
    pnl_unavailable: Arc<AtomicBool>,
    pnl_started: Arc<tokio::sync::Notify>,
    release_pnl: Arc<tokio::sync::Notify>,
    realized_pnl: Arc<Mutex<Decimal>>,
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
            if behavior <= 1 { fake.orders.lock().unwrap().insert(id, order.clone()); }
            if fake.block_submit.load(Ordering::SeqCst) {
                fake.submit_started.notify_one();
                fake.release_submit.notified().await;
            }
            match behavior {
                0 => (StatusCode::OK, Json(serde_json::to_value(order).unwrap())),
                3 => (StatusCode::BAD_REQUEST, Json(json!({"code": -5022, "msg": "Post Only order rejected."}))),
                4 => (StatusCode::BAD_REQUEST, Json(json!({"code": -2019, "msg": "Margin is insufficient."}))),
                _ => (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code": -1007, "msg": "Timeout waiting for response from backend server."}))),
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
            fake.position_calls.fetch_add(1, Ordering::SeqCst);
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
            fake.account_calls.fetch_add(1, Ordering::SeqCst);
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
            fake.pnl_calls.fetch_add(1, Ordering::SeqCst);
            if fake.block_pnl.load(Ordering::SeqCst) {
                fake.pnl_started.notify_one();
                fake.release_pnl.notified().await;
            }
            if fake.pnl_unavailable.load(Ordering::SeqCst) {
                return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"msg":"PnL unavailable"})));
            }
            let orders = fake.orders.lock().unwrap();
            let order = orders.values().find(|order| query.get("orderId") == Some(&order.order_id.to_string()));
            (StatusCode::OK, Json(json!(order.map(|order| vec![json!({"id":1, "orderId":order.order_id, "qty":order.executed_qty, "realizedPnl":*fake.realized_pnl.lock().unwrap(), "commission":"0.01", "commissionAsset":"USDC", "maker":true})]).unwrap_or_default())))
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

mod account_sync;
mod batch_and_pnl;
mod cancel_and_provisional;
mod conflicting_snapshot_recovery;
mod control_and_submission;
mod evidence_batches_and_dedup;
mod global_gates;
mod identity_barriers_and_cleanup;
mod identity_resolution_and_mismatches;
mod managed_and_partial_evidence;
mod pair_intent_recovery;
mod pnl_identity;
mod remainder_identity_recovery;
mod snapshot_open_prices;
mod submission_staleness_and_shutdown;
mod terminal_price_completion;
mod untrusted_snapshots;
