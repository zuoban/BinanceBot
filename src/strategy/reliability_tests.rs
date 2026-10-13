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
}

async fn exchange_server(fake: Exchange) -> (String, JoinHandle<()>) {
    let app = Router::new()
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
                Some(order) => (StatusCode::OK, Json(serde_json::to_value(order).unwrap())),
                None => (StatusCode::BAD_REQUEST, Json(json!({"code": -2013, "msg": "Order does not exist"}))),
            }
        }).post(|State(fake): State<Exchange>, Query(query): Query<HashMap<String, String>>| async move {
            let id = query["newClientOrderId"].clone();
            let mut posts = fake.posts.lock().unwrap();
            posts.push(id.clone());
            let order = BinanceOrderResponse {
                order_id: posts.len() as i64,
                client_order_id: id.clone(), symbol: query["symbol"].clone(), status: "NEW".into(),
                price: query["price"].parse().unwrap(), avg_price: None,
                orig_qty: query["quantity"].parse().unwrap(), executed_qty: dec!(0),
                side: query["side"].clone(), order_type: "LIMIT".into(),
                time_in_force: query["timeInForce"].clone(), update_time: None,
            };
            let behavior = fake.submission.load(Ordering::SeqCst);
            if behavior != 2 { fake.orders.lock().unwrap().insert(id, order.clone()); }
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
        .route("/fapi/v2/positionRisk", get(|| async {
            Json(json!([{"symbol":"SOLUSDC", "positionAmt":"1", "entryPrice":"100", "markPrice":"100.5", "unRealizedProfit":"0.5", "liquidationPrice":"0", "leverage":"1"}]))
        }))
        .route("/fapi/v2/account", get(|| async {
            Json(json!({"totalWalletBalance":"10000", "totalMarginBalance":"10000", "totalUnrealizedProfit":"0", "availableBalance":"10000", "assets":[{"asset":"USDC", "walletBalance":"10000", "marginBalance":"10000", "availableBalance":"10000", "unrealizedProfit":"0"}]}))
        }))
        .route("/fapi/v1/userTrades", get(|| async {
            Json(json!([{"realizedPnl":"0", "commission":"0.01", "commissionAsset":"USDC", "maker":true}]))
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

fn database() -> Arc<Database> {
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_bot_status(BotStatus::Running).unwrap();
    db
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
    assert!(restarted.sync_live_orders().await);
    assert!(restarted.sync_live_orders().await);
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
    let received = Utc::now() - chrono::Duration::seconds(16);
    engine
        .handle_ticker_update(TickerInfo {
            symbol: "SOLUSDC".into(),
            last_price: dec!(100.5),
            update_time: received,
            ..Default::default()
        })
        .await;
    assert_eq!(engine.state.ticker.read().await.update_time, received);
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
    state
        .action_tx
        .send(BotControlAction::Resume)
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
    assert!(response.await.unwrap().is_err());
    assert!(task.is_finished());
    assert_eq!(*state.status.read().await, BotStatus::Paused);
    assert!(fake.posts.lock().unwrap().is_empty());
    server.abort();
}
