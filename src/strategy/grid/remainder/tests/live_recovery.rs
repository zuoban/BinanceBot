use super::*;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

#[derive(Clone, Default)]
struct Exchange {
    orders: Arc<Mutex<HashMap<String, BinanceOrderResponse>>>,
    posts: Arc<Mutex<Vec<HashMap<String, String>>>>,
    position: Arc<Mutex<Decimal>>,
    lose_post: Arc<AtomicUsize>,
    lose_cancel: Arc<AtomicUsize>,
    fail_lookup: Arc<AtomicUsize>,
    reject_post: Arc<AtomicUsize>,
    hide_open: Arc<AtomicUsize>,
    cancel_fill: Arc<Mutex<Decimal>>,
    fail_position: Arc<AtomicUsize>,
    fail_cancel: Arc<AtomicUsize>,
    lookup_calls: Arc<AtomicUsize>,
    filled_zero_after: Arc<AtomicUsize>,
}

fn consume(counter: &AtomicUsize) -> bool {
    let mut count = counter.load(Ordering::SeqCst);
    while let Some(next) = count.checked_sub(1) {
        match counter.compare_exchange(count, next, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return true,
            Err(actual) => count = actual,
        }
    }
    false
}

fn response(order: &GridOrder, id: i64) -> BinanceOrderResponse {
    BinanceOrderResponse {
        order_id: id,
        client_order_id: order.client_order_id.clone(),
        symbol: order.symbol.clone(),
        status: "NEW".into(),
        price: order.price,
        avg_price: None,
        orig_qty: order.quantity,
        executed_qty: Decimal::ZERO,
        side: "SELL".into(),
        order_type: "LIMIT".into(),
        time_in_force: "GTX".into(),
        update_time: None,
    }
}

fn lookup(
    orders: &HashMap<String, BinanceOrderResponse>,
    query: &HashMap<String, String>,
) -> Option<BinanceOrderResponse> {
    if let Some(id) = query.get("origClientOrderId") {
        return orders.get(id).cloned();
    }
    orders
        .values()
        .find(|o| Some(o.order_id.to_string()).as_ref() == query.get("orderId"))
        .cloned()
}

async fn exchange_server(fake: Exchange) -> (String, tokio::task::JoinHandle<()>) {
    let app = Router::new()
        .route("/fapi/v1/openOrders", get(|State(fake): State<Exchange>| async move {
            if consume(&fake.hide_open) { return Json(Vec::<BinanceOrderResponse>::new()) }
            Json(fake.orders.lock().unwrap().values().filter(|o| !o.is_terminal()).cloned().collect::<Vec<_>>())
        }))
        .route("/fapi/v1/order", get(|State(fake): State<Exchange>, Query(query): Query<HashMap<String,String>>| async move {
            if consume(&fake.fail_lookup) { return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code": -1000, "msg": "uncertain"}))) }
            let call = fake.lookup_calls.fetch_add(1, Ordering::SeqCst) + 1;
            match lookup(&fake.orders.lock().unwrap(), &query) {
                Some(mut order) => {
                    let threshold = fake.filled_zero_after.load(Ordering::SeqCst);
                    if threshold > 0 && call >= threshold { order.status = "FILLED".into(); order.executed_qty = Decimal::ZERO; }
                    (StatusCode::OK, Json(serde_json::to_value(order).unwrap()))
                },
                None => (StatusCode::BAD_REQUEST, Json(json!({"code": -2013, "msg": "Order does not exist"}))),
            }
        }).delete(|State(fake): State<Exchange>, Query(query): Query<HashMap<String,String>>| async move {
            if consume(&fake.fail_cancel) { return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code": -1000}))); }
            let mut orders = fake.orders.lock().unwrap();
            let mut order = lookup(&orders, &query).unwrap();
            let fill = std::mem::take(&mut *fake.cancel_fill.lock().unwrap());
            order.executed_qty += fill;
            *fake.position.lock().unwrap() -= fill;
            order.status = "CANCELED".into();
            let canceled = order.clone();
            orders.insert(order.client_order_id.clone(), order);
            if consume(&fake.lose_cancel) {
                (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code": -1000})))
            } else { (StatusCode::OK, Json(serde_json::to_value(canceled).unwrap())) }
        }).post(|State(fake): State<Exchange>, Query(query): Query<HashMap<String,String>>| async move {
            fake.posts.lock().unwrap().push(query.clone());
            if consume(&fake.reject_post) { return (StatusCode::BAD_REQUEST, Json(json!({"code": -5022, "msg": "would take liquidity"}))) }
            let mut order = response(&source("new", query["price"].parse().unwrap(), query["quantity"].parse().unwrap()), 999);
            order.client_order_id = query["newClientOrderId"].clone();
            fake.orders.lock().unwrap().insert(order.client_order_id.clone(), order.clone());
            if consume(&fake.lose_post) {
                (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code": -1000, "msg": "response lost"})))
            } else { (StatusCode::OK, Json(serde_json::to_value(order).unwrap())) }
        }))
        .route("/fapi/v2/positionRisk", get(|State(fake): State<Exchange>| async move {
            if consume(&fake.fail_position) {
                return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({})));
            }
            (StatusCode::OK, Json(json!([{"symbol": "SOLUSDC", "positionAmt": fake.position.lock().unwrap().to_string(), "entryPrice":"120", "markPrice":"122.31", "unRealizedProfit":"0", "liquidationPrice":"0", "leverage":"1"}])))
        }))
        .route("/fapi/v2/account", get(|| async { Json(json!({
            "totalWalletBalance":"10000", "totalMarginBalance":"10000", "totalUnrealizedProfit":"0", "availableBalance":"10000",
            "assets":[{"asset":"USDC", "walletBalance":"10000", "unrealizedProfit":"0", "marginBalance":"10000", "availableBalance":"10000"}]
        })) }))
        .route("/fapi/v1/userTrades", get(|| async { Json(Vec::<Value>::new()) }))
        .with_state(fake);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (url, server)
}

async fn live_fixture() -> (
    GridTradingEngine,
    Exchange,
    String,
    tokio::task::JoinHandle<()>,
) {
    let fake = Exchange::default();
    *fake.position.lock().unwrap() = dec!(0.48);
    let (url, server) = exchange_server(fake.clone()).await;
    let engine = engine(database(), Some(url.clone())).await;
    let sources = two_sources();
    for (i, order) in sources.iter().enumerate() {
        fake.orders
            .lock()
            .unwrap()
            .insert(order.client_order_id.clone(), response(order, i as i64 + 1));
    }
    seed(&engine, &sources).await;
    (engine, fake, url, server)
}

#[tokio::test]
async fn lost_submit_response_is_adopted_after_restart_with_provenance() {
    let (mut first, fake, url, server) = live_fixture().await;
    fake.lose_post.store(1, Ordering::SeqCst);
    first.maintain_grid_window().await;
    let plan = first.load_remainder_plan().await.unwrap().unwrap();
    assert_eq!(plan.phase, RemainderPhase::Submitting);
    let db = first.state.db.clone();
    drop(first);
    let mut restarted = engine(db, Some(url)).await;
    assert!(restarted.sync_live_orders().await);
    restarted.maintain_grid_window().await;
    assert!(restarted.load_remainder_plan().await.unwrap().is_none());
    let active = restarted.state.active_orders.read().await;
    assert_eq!(active.len(), 1);
    assert_eq!(
        active[&plan.target.client_order_id].purpose,
        OrderPurpose::Remainder
    );
    assert_eq!(active[&plan.target.client_order_id].merge_sources.len(), 2);
    let posts = fake.posts.lock().unwrap();
    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0]["quantity"], "0.48");
    assert_eq!(posts[0]["reduceOnly"], "true");
    assert_eq!(posts[0]["timeInForce"], "GTX");
    server.abort();
}

#[tokio::test]
async fn provisional_remainder_uses_recovery_plan_metadata_without_managed_row() {
    let (mut engine, fake, _, server) = live_fixture().await;
    fake.lose_post.store(1, Ordering::SeqCst);
    engine.maintain_grid_window().await;
    let plan = engine.load_remainder_plan().await.unwrap().unwrap();
    engine
        .state
        .db
        .delete_managed_order(&plan.target.client_order_id)
        .unwrap();
    let mut placeholder = plan.target.clone();
    placeholder.order_id = Some(999);
    placeholder.purpose = OrderPurpose::Legacy;
    placeholder.merge_sources.clear();
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(placeholder.client_order_id.clone(), placeholder);
    engine.evidence.mark_provisional(
        engine.client.account_identity_key(),
        &plan.target.client_order_id,
    );
    assert!(engine.sync_live_orders().await);
    let active = engine.state.active_orders.read().await;
    let target = &active[&plan.target.client_order_id];
    assert_eq!(target.purpose, OrderPurpose::Remainder);
    assert_eq!(target.merge_sources.len(), 2);
    let saved = engine.state.db.load_managed_orders("SOLUSDC").unwrap();
    let stored = saved
        .iter()
        .find(|order| order.client_order_id == plan.target.client_order_id)
        .unwrap();
    assert_eq!(stored.purpose, OrderPurpose::Remainder);
    assert_eq!(stored.merge_sources.len(), 2);
    server.abort();
}

#[tokio::test]
async fn bulk_cancel_unknown_remainder_uses_plan_metadata_on_first_discovery() {
    let (mut engine, fake, _, server) = live_fixture().await;
    fake.lose_post.store(1, Ordering::SeqCst);
    engine.maintain_grid_window().await;
    let plan = engine.load_remainder_plan().await.unwrap().unwrap();
    engine
        .state
        .db
        .delete_managed_order(&plan.target.client_order_id)
        .unwrap();
    engine
        .state
        .active_orders
        .write()
        .await
        .remove(&plan.target.client_order_id);
    fake.fail_cancel.store(2, Ordering::SeqCst);
    fake.lookup_calls.store(0, Ordering::SeqCst);
    fake.filled_zero_after.store(2, Ordering::SeqCst);
    assert!(!engine.cancel_all_orders().await);
    let active = engine.state.active_orders.read().await;
    let target = &active[&plan.target.client_order_id];
    assert_eq!(target.purpose, OrderPurpose::Remainder);
    assert_eq!(target.merge_sources.len(), 2);
    let saved = engine.state.db.load_managed_orders("SOLUSDC").unwrap();
    let target = saved
        .iter()
        .find(|order| order.client_order_id == plan.target.client_order_id)
        .unwrap();
    assert_eq!(target.purpose, OrderPurpose::Remainder);
    assert_eq!(target.merge_sources.len(), 2);
    server.abort();
}

#[tokio::test]
async fn cancel_retry_restores_provisional_remainder_from_plan() {
    let (mut engine, fake, _, server) = live_fixture().await;
    fake.lose_post.store(1, Ordering::SeqCst);
    engine.maintain_grid_window().await;
    let plan = engine.load_remainder_plan().await.unwrap().unwrap();
    engine
        .state
        .db
        .delete_managed_order(&plan.target.client_order_id)
        .unwrap();
    let mut placeholder = plan.target.clone();
    placeholder.order_id = Some(999);
    placeholder.purpose = OrderPurpose::Legacy;
    placeholder.merge_sources.clear();
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(placeholder.client_order_id.clone(), placeholder);
    engine.evidence.mark_provisional(
        engine.client.account_identity_key(),
        &plan.target.client_order_id,
    );
    fake.lose_cancel.store(1, Ordering::SeqCst);
    assert!(!engine.cancel_all_orders().await);
    let active = engine.state.active_orders.read().await;
    assert_eq!(
        active[&plan.target.client_order_id].purpose,
        OrderPurpose::Remainder
    );
    assert_eq!(active[&plan.target.client_order_id].merge_sources.len(), 2);
    server.abort();
}

#[tokio::test]
async fn cancel_response_loss_recovers_and_subtracts_fills_before_merging() {
    let (mut first, fake, url, server) = live_fixture().await;
    fake.lose_cancel.store(1, Ordering::SeqCst);
    *fake.cancel_fill.lock().unwrap() = dec!(0.10);
    first.maintain_grid_window().await;
    assert_eq!(
        first.load_remainder_plan().await.unwrap().unwrap().phase,
        RemainderPhase::Canceling
    );
    assert!(fake.posts.lock().unwrap().is_empty());
    let db = first.state.db.clone();
    drop(first);
    let mut restarted = engine(db.clone(), Some(url)).await;
    assert!(restarted.sync_live_orders().await);
    restarted.maintain_grid_window().await;
    let active: Vec<_> = restarted
        .state
        .active_orders
        .read()
        .await
        .values()
        .cloned()
        .collect();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].quantity, dec!(0.38));
    assert_eq!(
        reserved_sell_quantity(&active),
        *fake.position.lock().unwrap()
    );
    assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
    assert_eq!(fake.posts.lock().unwrap().len(), 1);
    server.abort();
}

#[tokio::test]
async fn target_filled_before_restart_is_recorded_once_without_replacement() {
    let (mut first, fake, url, server) = live_fixture().await;
    fake.lose_post.store(1, Ordering::SeqCst);
    first.maintain_grid_window().await;
    let plan = first.load_remainder_plan().await.unwrap().unwrap();
    {
        let mut orders = fake.orders.lock().unwrap();
        let order = orders.get_mut(&plan.target.client_order_id).unwrap();
        order.status = "FILLED".into();
        order.executed_qty = order.orig_qty;
    }
    *fake.position.lock().unwrap() = Decimal::ZERO;
    let db = first.state.db.clone();
    drop(first);
    let mut restarted = engine(db.clone(), Some(url)).await;
    assert!(restarted.sync_live_orders().await);
    restarted.maintain_grid_window().await;
    restarted.maintain_grid_window().await;
    assert!(restarted.state.active_orders.read().await.is_empty());
    assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
    assert_eq!(fake.posts.lock().unwrap().len(), 1);
    assert!(db
        .load_pair_intents("SOLUSDC", TradingMode::Live)
        .unwrap()
        .is_empty());
    server.abort();
}

#[tokio::test]
async fn unresolved_lookup_blocks_all_new_window_orders_and_pause_cancels_target() {
    let (mut engine, fake, _, server) = live_fixture().await;
    fake.lose_post.store(1, Ordering::SeqCst);
    engine.maintain_grid_window().await;
    fake.fail_lookup.store(1, Ordering::SeqCst);
    engine.state.config.write().await.grid.buy_window = 3;
    engine.maintain_grid_window().await;
    assert_eq!(fake.posts.lock().unwrap().len(), 1);
    assert!(engine.load_remainder_plan().await.unwrap().is_some());
    engine.handle_control_action(BotControlAction::Pause).await;
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert!(engine.load_remainder_plan().await.unwrap().is_none());
    assert!(fake
        .orders
        .lock()
        .unwrap()
        .values()
        .all(|o| o.is_terminal()));
    assert!(engine.state.active_orders.read().await.is_empty());
    server.abort();
}

#[tokio::test]
async fn post_only_rejection_keeps_intent_and_never_changes_price_or_order_type() {
    let (mut engine, fake, _, server) = live_fixture().await;
    fake.reject_post.store(1, Ordering::SeqCst);
    engine.maintain_grid_window().await;
    let plan = engine.load_remainder_plan().await.unwrap().unwrap();
    engine.maintain_grid_window().await;
    assert!(engine.load_remainder_plan().await.unwrap().is_none());
    let posts = fake.posts.lock().unwrap();
    assert_eq!(posts.len(), 2);
    for post in posts.iter() {
        assert_eq!(post["newClientOrderId"], plan.target.client_order_id);
        assert_eq!(post["price"], "122.40");
        assert_eq!(post["timeInForce"], "GTX");
    }
    server.abort();
}

#[tokio::test]
async fn missing_open_snapshot_does_not_release_an_uncertain_targets_inventory() {
    let (mut engine, fake, _, server) = live_fixture().await;
    fake.lose_post.store(1, Ordering::SeqCst);
    engine.maintain_grid_window().await;
    fake.hide_open.store(2, Ordering::SeqCst);
    fake.fail_lookup.store(1, Ordering::SeqCst);
    assert!(!engine.sync_live_orders().await);
    assert!(engine.last_orders_sync.is_none());
    assert_eq!(fake.posts.lock().unwrap().len(), 1);
    // A successful lookup, despite another empty snapshot, reserves the accepted target.
    assert!(engine.sync_live_orders().await);
    let orders: Vec<_> = engine
        .state
        .active_orders
        .read()
        .await
        .values()
        .cloned()
        .collect();
    assert_eq!(reserved_sell_quantity(&orders), dec!(0.48));
    assert_eq!(orders[0].purpose, OrderPurpose::Remainder);
    assert!(engine.load_remainder_plan().await.unwrap().is_none());
    server.abort();
}

#[tokio::test]
async fn partially_filled_remainder_lookup_refreshes_risk_before_reducing_reservation() {
    let (mut engine, fake, _, server) = live_fixture().await;
    fake.lose_post.store(1, Ordering::SeqCst);
    engine.maintain_grid_window().await;
    let mut plan = engine.load_remainder_plan().await.unwrap().unwrap();
    engine
        .position_cache
        .get(&engine.client, "SOLUSDC")
        .await
        .unwrap();
    engine.account_cache.get(&engine.client).await.unwrap();
    {
        let mut orders = fake.orders.lock().unwrap();
        let target = orders.get_mut(&plan.target.client_order_id).unwrap();
        target.status = "PARTIALLY_FILLED".into();
        target.executed_qty = dec!(0.24);
    }
    *fake.position.lock().unwrap() = dec!(0.24);
    fake.fail_position.store(1, Ordering::SeqCst);
    assert!(engine.adopt_remainder_target(&mut plan).await.is_err());
    assert_eq!(
        engine.state.active_orders.read().await[&plan.target.client_order_id].quantity,
        dec!(0.48)
    );
    assert_eq!(plan.phase, RemainderPhase::Submitting);
    assert!(engine.last_account_sync.is_none());
    assert!(engine
        .state
        .db
        .load_managed_orders("SOLUSDC")
        .unwrap()
        .iter()
        .any(|order| order.client_order_id == plan.target.client_order_id
            && order.quantity == dec!(0.48)));

    assert!(engine.adopt_remainder_target(&mut plan).await.unwrap());
    assert_eq!(
        engine.state.active_orders.read().await[&plan.target.client_order_id].quantity,
        dec!(0.24)
    );
    assert_eq!(engine.state.position.read().await.size, dec!(0.24));
    assert_eq!(plan.phase, RemainderPhase::Complete);
    server.abort();
}

#[tokio::test]
async fn remainder_zero_fill_conflicting_order_id_keeps_trusted_identity_and_persists_observation()
{
    let (mut engine, fake, _, server) = live_fixture().await;
    fake.lose_post.store(1, Ordering::SeqCst);
    engine.maintain_grid_window().await;
    let mut plan = engine.load_remainder_plan().await.unwrap().unwrap();
    plan.target.order_id = Some(123);
    {
        let mut orders = fake.orders.lock().unwrap();
        let found = orders.get_mut(&plan.target.client_order_id).unwrap();
        found.order_id = 456;
        found.status = "FILLED".into();
        found.executed_qty = Decimal::ZERO;
    }
    engine
        .state
        .active_orders
        .write()
        .await
        .remove(&plan.target.client_order_id);
    assert!(engine.adopt_remainder_target(&mut plan).await.is_err());
    assert_eq!(plan.phase, RemainderPhase::Submitting);
    assert_eq!(
        engine.state.active_orders.read().await[&plan.target.client_order_id].order_id,
        Some(123)
    );
    let observations = engine
        .state
        .db
        .load_identity_observations(engine.client.account_identity_key(), "SOLUSDC")
        .unwrap();
    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0].0.order_id, Some(123));
    assert_eq!(observations[0].1.order_id, 456);
    assert!(!engine.state.global_position_ready.load(Ordering::SeqCst));
    server.abort();
}

#[tokio::test]
async fn filled_remainder_with_zero_quantity_stays_recoverable_until_cancel_confirmed() {
    let (mut engine, fake, _, server) = live_fixture().await;
    fake.lose_post.store(1, Ordering::SeqCst);
    engine.maintain_grid_window().await;
    let mut plan = engine.load_remainder_plan().await.unwrap().unwrap();
    fake.orders
        .lock()
        .unwrap()
        .get_mut(&plan.target.client_order_id)
        .unwrap()
        .status = "FILLED".into();
    let state = engine.state.clone();
    state.global_position_ready.store(true, Ordering::SeqCst);
    let guard = state.active_orders.write().await;
    let task = tokio::spawn(async move {
        let result = engine.adopt_remainder_target(&mut plan).await;
        (engine, plan, result)
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.global_position_ready.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(guard);
    let (mut engine, mut plan, result) = task.await.unwrap();
    assert!(result.is_err());
    assert_eq!(plan.phase, RemainderPhase::Submitting);
    assert_eq!(
        engine.state.active_orders.read().await[&plan.target.client_order_id].quantity,
        dec!(0.48)
    );
    assert!(engine
        .state
        .db
        .load_managed_orders("SOLUSDC")
        .unwrap()
        .iter()
        .any(|order| order.client_order_id == plan.target.client_order_id));
    fake.orders
        .lock()
        .unwrap()
        .get_mut(&plan.target.client_order_id)
        .unwrap()
        .status = "CANCELED".into();
    assert!(engine.adopt_remainder_target(&mut plan).await.unwrap());
    assert_eq!(plan.phase, RemainderPhase::Complete);
    assert!(!engine
        .state
        .active_orders
        .read()
        .await
        .contains_key(&plan.target.client_order_id));
    server.abort();
}

#[tokio::test]
async fn remainder_partial_persistence_failure_blocks_global_and_keeps_reservation() {
    let dir = crate::test_support::TempTestDir::new("remainder-partial");
    let path = dir.join("remainder-partial.db");
    let db = Arc::new(Database::open(&path).unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    let fake = Exchange::default();
    *fake.position.lock().unwrap() = dec!(0.48);
    let (url, server) = exchange_server(fake.clone()).await;
    let mut engine = engine(db.clone(), Some(url)).await;
    let sources = two_sources();
    for (i, order) in sources.iter().enumerate() {
        fake.orders
            .lock()
            .unwrap()
            .insert(order.client_order_id.clone(), response(order, i as i64 + 1));
    }
    seed(&engine, &sources).await;
    fake.lose_post.store(1, Ordering::SeqCst);
    engine.maintain_grid_window().await;
    let mut plan = engine.load_remainder_plan().await.unwrap().unwrap();
    {
        let mut orders = fake.orders.lock().unwrap();
        let target = orders.get_mut(&plan.target.client_order_id).unwrap();
        target.status = "PARTIALLY_FILLED".into();
        target.executed_qty = dec!(0.24);
    }
    *fake.position.lock().unwrap() = dec!(0.24);
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_partial BEFORE INSERT ON managed_orders \
        WHEN json_extract(NEW.order_json, '$.status') = 'PARTIALLYFILLED' \
        BEGIN SELECT RAISE(ABORT, 'partial write failed'); END;",
        )
        .unwrap();
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
    assert!(engine.adopt_remainder_target(&mut plan).await.is_err());
    assert!(!engine.state.global_position_ready.load(Ordering::SeqCst));
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert_eq!(
        engine.state.active_orders.read().await[&plan.target.client_order_id].quantity,
        dec!(0.48)
    );
    assert_eq!(plan.phase, RemainderPhase::Submitting);
    server.abort();
    drop(connection);
    drop(engine);
    drop(db);
}

#[tokio::test]
async fn canceled_sources_that_fall_below_minimum_leave_no_invalid_replacement() {
    let (mut engine, fake, _, server) = live_fixture().await;
    *fake.cancel_fill.lock().unwrap() = dec!(0.24);
    engine.state.rules.write().await.min_notional = dec!(50);
    engine.maintain_grid_window().await;
    assert!(fake.posts.lock().unwrap().is_empty());
    assert!(engine.state.active_orders.read().await.is_empty());
    assert!(engine.load_remainder_plan().await.unwrap().is_none());
    assert_eq!(*fake.position.lock().unwrap(), dec!(0.24));
    server.abort();
}

#[tokio::test]
async fn crossed_gtx_rejected_plan_recovers_after_restart_and_replenishes_sells() {
    let (mut first, fake, url, server) = live_fixture().await;
    fake.reject_post.store(1, Ordering::SeqCst);
    first.maintain_grid_window().await;
    let plan = first.load_remainder_plan().await.unwrap().unwrap();
    assert_eq!(plan.phase, RemainderPhase::Submitting);
    assert!(!fake
        .orders
        .lock()
        .unwrap()
        .contains_key(&plan.target.client_order_id));
    let db = first.state.db.clone();
    drop(first);

    let mut restarted = engine(db.clone(), Some(url)).await;
    {
        let mut config = restarted.state.config.write().await;
        config.grid.grid_interval = dec!(0.1);
        config.grid.order_amount_usdc = dec!(200);
        config.grid.sell_window = 3;
    }
    *fake.position.lock().unwrap() = dec!(107.33);
    restarted.state.ticker.write().await.last_price = dec!(127.35);
    assert!(restarted.sync_live_orders().await);
    assert!(restarted.sync_account_and_position().await);
    restarted.maintain_grid_window().await;

    assert!(restarted.load_remainder_plan().await.unwrap().is_none());
    let active: Vec<_> = restarted
        .state
        .active_orders
        .read()
        .await
        .values()
        .cloned()
        .collect();
    let mut sells: Vec<_> = active
        .iter()
        .filter(|o| o.side == OrderSide::Sell)
        .map(|o| o.price)
        .collect();
    sells.sort();
    assert_eq!(sells, vec![dec!(127.4), dec!(127.5), dec!(127.6)]);
    assert_eq!(reserved_sell_quantity(&active), dec!(4.68));
    {
        let posts = fake.posts.lock().unwrap();
        assert_eq!(posts.len(), 4); // One rejected old target, then three current grid sells.
        assert!(posts[1..]
            .iter()
            .all(|p| p["newClientOrderId"] != plan.target.client_order_id));
        assert!(posts
            .iter()
            .all(|p| p["timeInForce"] == "GTX" && p["reduceOnly"] == "true"));
    }
    // Once retired, the old target cannot reappear or be resubmitted on another cycle.
    restarted.maintain_grid_window().await;
    assert_eq!(fake.posts.lock().unwrap().len(), 4);
    assert!(restarted
        .state
        .recent_logs
        .read()
        .await
        .iter()
        .any(|log| log.message.contains("normal grid replenishment resumed")));
    server.abort();
}

#[tokio::test]
async fn crossed_target_with_uncertain_lookup_stays_reserved_until_adopted() {
    let (mut engine, fake, _, server) = live_fixture().await;
    fake.lose_post.store(1, Ordering::SeqCst);
    engine.maintain_grid_window().await;
    let plan = engine.load_remainder_plan().await.unwrap().unwrap();
    engine.state.ticker.write().await.last_price = dec!(127.35);
    fake.fail_lookup.store(1, Ordering::SeqCst);
    engine.maintain_grid_window().await;
    assert!(engine.load_remainder_plan().await.unwrap().is_some());
    assert_eq!(fake.posts.lock().unwrap().len(), 1);

    assert!(engine.recover_remainder_plan().await.unwrap());
    assert!(engine.load_remainder_plan().await.unwrap().is_none());
    let active = engine.state.active_orders.read().await;
    assert_eq!(active.len(), 1);
    assert_eq!(active[&plan.target.client_order_id].quantity, dec!(0.48));
    assert_eq!(
        active[&plan.target.client_order_id].price,
        plan.target.price
    );
    assert_eq!(fake.posts.lock().unwrap().len(), 1);
    server.abort();
}

#[tokio::test]
async fn remainder_submitting_identity_mismatch_blocks_reconciliation_and_persists_observation() {
    for mismatch in ["side", "symbol", "zero_price"] {
        let (mut engine, fake, _, server) = live_fixture().await;
        fake.lose_post.store(1, Ordering::SeqCst);
        engine.maintain_grid_window().await;
        let mut plan = engine.load_remainder_plan().await.unwrap().unwrap();
        assert_eq!(plan.phase, RemainderPhase::Submitting);
        assert_eq!(plan.target.order_id, None);
        {
            let mut orders = fake.orders.lock().unwrap();
            let found = orders.get_mut(&plan.target.client_order_id).unwrap();
            match mismatch {
                "side" => found.side = "BUY".into(),
                "symbol" => found.symbol = "BTCUSDC".into(),
                _ => found.price = Decimal::ZERO,
            }
        }
        engine
            .state
            .active_orders
            .write()
            .await
            .remove(&plan.target.client_order_id);
        engine
            .state
            .global_position_ready
            .store(true, Ordering::SeqCst);

        assert!(
            engine.adopt_remainder_target(&mut plan).await.is_err(),
            "mismatch={mismatch}"
        );
        assert_eq!(plan.phase, RemainderPhase::Submitting);
        assert_eq!(plan.target.order_id, None);
        assert!(engine.reconciliation_blocked);
        assert!(engine.last_orders_sync.is_none());
        assert!(engine.last_account_sync.is_none());
        assert!(!engine.state.global_position_ready.load(Ordering::SeqCst));

        let active = engine.state.active_orders.read().await;
        let staged = &active[&plan.target.client_order_id];
        assert_eq!(staged.order_id, None);
        assert_eq!(staged.symbol, "SOLUSDC");
        assert_eq!(staged.side, OrderSide::Sell);
        assert!(staged.price > Decimal::ZERO);
        assert_eq!(staged.purpose, OrderPurpose::Remainder);
        assert_eq!(staged.merge_sources.len(), 2);
        drop(active);

        let observations = engine
            .state
            .db
            .load_identity_observations(engine.client.account_identity_key(), "SOLUSDC")
            .unwrap();
        if mismatch == "zero_price" {
            assert!(observations.is_empty());
        } else {
            assert_eq!(observations.len(), 1);
            assert_eq!(
                observations[0].0.client_order_id,
                plan.target.client_order_id
            );
            assert_eq!(observations[0].0.order_id, None);
            assert_eq!(observations[0].1.order_id, 999);
            assert_eq!(
                observations[0].1.side,
                if mismatch == "side" { "BUY" } else { "SELL" }
            );
            assert_eq!(
                observations[0].1.symbol,
                if mismatch == "symbol" {
                    "BTCUSDC"
                } else {
                    "SOLUSDC"
                }
            );
        }
        server.abort();
    }
}
