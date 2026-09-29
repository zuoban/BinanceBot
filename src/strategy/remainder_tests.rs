use super::*;
use crate::{config::AppConfig, db::Database};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use rust_decimal_macros::dec;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

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
    config.exchange.dry_run = url.is_none();
    config.grid.grid_interval = dec!(1.2);
    config.grid.order_amount_usdc = dec!(3000);
    config.grid.buy_window = 0;
    config.grid.sell_window = 4;
    let (action_tx, action_rx) = mpsc::channel(1);
    let (_, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db.clone(), action_tx);
    state.ticker.write().await.last_price = dec!(122.31);
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
        source("a", dec!(125.66), dec!(0.24)),
        source("b", dec!(126.87), dec!(0.24)),
    ]
}

#[tokio::test]
async fn merges_screenshot_remainders_at_highest_price_and_is_idempotent() {
    let mut engine = engine(database(), None).await;
    seed(&engine, &two_sources()).await;
    engine.maintain_grid_window().await;
    let merged = engine
        .state
        .active_orders
        .read()
        .await
        .values()
        .next()
        .unwrap()
        .clone();
    assert_eq!(engine.state.active_orders.read().await.len(), 1);
    assert_eq!(merged.price, dec!(126.87));
    assert_eq!(merged.quantity, dec!(0.48));
    assert_eq!(merged.amount_usdc, dec!(60.8976));
    assert_eq!(merged.purpose, OrderPurpose::Remainder);
    assert_eq!(merged.merge_sources.len(), 2);
    for _ in 0..3 {
        engine.maintain_grid_window().await;
    }
    assert!(engine
        .state
        .active_orders
        .read()
        .await
        .contains_key(&merged.client_order_id));
    assert!(engine.load_remainder_plan().await.unwrap().is_none());
}

#[tokio::test]
async fn legacy_partial_grid_and_take_profit_orders_are_not_merge_candidates() {
    let mut engine = engine(database(), None).await;
    let mut legacy = source("legacy", dec!(125.66), dec!(0.24));
    legacy.purpose = OrderPurpose::Legacy;
    let mut partial = source("partial", dec!(126.87), dec!(0.24));
    partial.purpose = OrderPurpose::Grid;
    partial.status = OrderStatus::PartiallyFilled;
    let mut tp = source("tp", dec!(124.38), dec!(24.11));
    tp.purpose = OrderPurpose::TakeProfit;
    tp.is_take_profit = true;
    tp.paired_client_order_id = Some("gb_b_parent".into());
    engine.state.position.write().await.size = dec!(25.07);
    let mut orders = two_sources();
    orders.extend([legacy.clone(), partial.clone(), tp.clone()]);
    seed(&engine, &orders).await;
    engine.reconcile_remainder(&[]).await.unwrap();
    let active = engine.state.active_orders.read().await;
    assert_eq!(active.len(), 4);
    for preserved in [legacy, partial, tp] {
        assert_eq!(
            active[&preserved.client_order_id].quantity,
            preserved.quantity
        );
    }
    assert_eq!(
        active
            .values()
            .find(|o| o.purpose == OrderPurpose::Remainder)
            .unwrap()
            .quantity,
        dec!(0.48)
    );
}

#[tokio::test]
async fn tops_up_one_remainder_after_coalescing_delay_without_creating_another() {
    let mut engine = engine(database(), None).await;
    seed(&engine, &two_sources()).await;
    engine.maintain_grid_window().await;
    engine.state.position.write().await.size = dec!(0.72);
    engine.maintain_grid_window().await;
    assert_eq!(engine.state.active_orders.read().await.len(), 1);
    assert_eq!(
        engine
            .state
            .active_orders
            .read()
            .await
            .values()
            .next()
            .unwrap()
            .quantity,
        dec!(0.48)
    );
    engine.last_remainder_change = Some(Instant::now() - Duration::from_secs(31));
    engine.maintain_grid_window().await;
    let orders = engine.state.active_orders.read().await;
    assert_eq!(orders.len(), 1);
    assert_eq!(orders.values().next().unwrap().quantity, dec!(0.72));
    assert_eq!(orders.values().next().unwrap().price, dec!(126.87));
}

#[tokio::test]
async fn remainder_fill_never_places_full_buy_even_when_configured_amount_matches() {
    let mut engine = engine(database(), None).await;
    let mut order = source("filled", dec!(126.87), dec!(0.48));
    engine.state.config.write().await.grid.order_amount_usdc = dec!(60.8976);
    seed(&engine, &[order.clone()]).await;
    assert!(engine.on_order_filled(&mut order).await);
    assert!(engine.state.active_orders.read().await.is_empty());
    assert!(engine
        .state
        .db
        .load_pair_intents("SOLUSDC", TradingMode::Paper)
        .unwrap()
        .is_empty());
    assert_eq!(engine.state.position.read().await.size, Decimal::ZERO);
}

#[tokio::test]
async fn promotes_whole_grid_then_allocates_only_one_final_remainder() {
    let mut engine = engine(database(), None).await;
    let sources = vec![
        source("a", dec!(125.66), dec!(12)),
        source("b", dec!(126.87), dec!(12)),
    ];
    engine.state.position.write().await.size = dec!(24);
    seed(&engine, &sources).await;
    engine.maintain_grid_window().await;
    let promoted = engine
        .state
        .active_orders
        .read()
        .await
        .values()
        .next()
        .unwrap()
        .clone();
    assert_eq!(promoted.purpose, OrderPurpose::Grid);
    assert_eq!(promoted.price, dec!(126.87));
    assert_eq!(promoted.quantity, dec!(23.64));
    engine.maintain_grid_window().await;
    let orders: Vec<_> = engine
        .state
        .active_orders
        .read()
        .await
        .values()
        .cloned()
        .collect();
    assert_eq!(
        orders
            .iter()
            .filter(|o| o.purpose == OrderPurpose::Remainder)
            .count(),
        1
    );
    assert_eq!(reserved_sell_quantity(&orders), dec!(24));
}

#[test]
fn legacy_json_retains_unknown_provenance() {
    let mut value = serde_json::to_value(source("legacy", dec!(126), dec!(0.24))).unwrap();
    value.as_object_mut().unwrap().remove("purpose");
    value.as_object_mut().unwrap().remove("merge_sources");
    let old: GridOrder = serde_json::from_value(value).unwrap();
    assert_eq!(old.purpose, OrderPurpose::Legacy);
    assert!(old.merge_sources.is_empty());
}

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
}

fn consume(counter: &AtomicUsize) -> bool {
    counter
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
        .is_ok()
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
            Json(fake.orders.lock().unwrap().values().filter(|o| !terminal(&o.status)).cloned().collect::<Vec<_>>())
        }))
        .route("/fapi/v1/order", get(|State(fake): State<Exchange>, Query(query): Query<HashMap<String,String>>| async move {
            if consume(&fake.fail_lookup) { return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code": -1000, "msg": "uncertain"}))) }
            match lookup(&fake.orders.lock().unwrap(), &query) {
                Some(order) => (StatusCode::OK, Json(serde_json::to_value(order).unwrap())),
                None => (StatusCode::BAD_REQUEST, Json(json!({"code": -2013, "msg": "Order does not exist"}))),
            }
        }).delete(|State(fake): State<Exchange>, Query(query): Query<HashMap<String,String>>| async move {
            let mut orders = fake.orders.lock().unwrap();
            let mut order = lookup(&orders, &query).unwrap();
            let fill = std::mem::take(&mut *fake.cancel_fill.lock().unwrap());
            order.executed_qty += fill;
            *fake.position.lock().unwrap() -= fill;
            order.status = "CANCELED".into();
            orders.insert(order.client_order_id.clone(), order);
            if consume(&fake.lose_cancel) {
                (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"code": -1000})))
            } else { (StatusCode::OK, Json(json!({}))) }
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
            Json(json!([{"symbol": "SOLUSDC", "positionAmt": fake.position.lock().unwrap().to_string(), "entryPrice":"120", "markPrice":"122.31", "unRealizedProfit":"0", "liquidationPrice":"0", "leverage":"1"}]))
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
        .all(|o| terminal(&o.status)));
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
        assert_eq!(post["price"], "126.87");
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

#[test]
fn plans_survive_database_reopen_and_are_scoped_and_unique() {
    let directory = std::env::temp_dir().join(format!("remainder-test-{}", Uuid::new_v4()));
    let path = directory.join("test.db");
    let mut plan = RemainderPlan {
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Live,
        sources: two_sources(),
        target: source("target", dec!(126.87), dec!(0.48)),
        phase: RemainderPhase::Canceling,
    };
    {
        let db = Database::open(&path).unwrap();
        db.save_remainder_plan(&plan).unwrap();
        let mut conflicting = plan.clone();
        conflicting.target.client_order_id = "gb_s_other_target".into();
        assert!(db.save_remainder_plan(&conflicting).is_err());
    }
    {
        let db = Database::open(&path).unwrap();
        let restored = db
            .load_remainder_plan("SOLUSDC", TradingMode::Live)
            .unwrap()
            .unwrap();
        assert_eq!(restored.sources.len(), 2);
        assert_eq!(restored.target.quantity, dec!(0.48));
        assert_eq!(restored.phase, RemainderPhase::Canceling);
        assert!(db
            .load_remainder_plan("SOLUSDC", TradingMode::Testnet)
            .unwrap()
            .is_none());
        assert!(db
            .load_remainder_plan("BTCUSDC", TradingMode::Live)
            .unwrap()
            .is_none());
        plan.phase = RemainderPhase::Complete;
        db.save_remainder_plan(&plan).unwrap();
        assert!(db
            .load_remainder_plan("SOLUSDC", TradingMode::Live)
            .unwrap()
            .is_none());
    }
    std::fs::remove_dir_all(directory).unwrap();
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

    engine.maintain_grid_window().await;
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
