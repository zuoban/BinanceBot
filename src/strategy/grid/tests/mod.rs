use super::{
    aggregate_execution_pnl, buy_exposure_usdc, grid_level_occupied, has_nearby_grid_order,
    is_grid_order, live_sync_freshness, new_grid_client_order_id, new_pair_intent,
    paired_grid_client_order_id, reserved_sell_quantity, sell_quantity_available,
    GridTradingEngine, MarkPriceCompletion, PairPlacementDecision, TickerCompletion,
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
    batch_cancel_calls: Arc<AtomicUsize>,
    single_cancel_calls: Arc<AtomicUsize>,
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
                    fake.single_cancel_calls.fetch_add(1, Ordering::SeqCst);
                    if fake.block_cancel.load(Ordering::SeqCst) != 0 {
                        fake.cancel_started.notify_one();
                        fake.release_cancel.notified().await;
                    }
                    if consume_failure(&fake.fail_cancel) {
                        return (axum::http::StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"code": -1000})));
                    }
                    let mut orders = fake.orders.lock().unwrap();
                    let found = if let Some(id_raw) = query.get("orderId") {
                        let id: i64 = id_raw.parse().unwrap();
                        orders.values_mut().find(|order| order.order_id == id)
                    } else {
                        let cid = &query["origClientOrderId"];
                        orders.values_mut().find(|order| &order.client_order_id == cid)
                    };
                    let Some(order) = found else {
                        return (axum::http::StatusCode::BAD_REQUEST, Json(serde_json::json!({"code": -2011, "msg": "Unknown order sent."})));
                    };
                    if order.is_terminal() {
                        return (axum::http::StatusCode::BAD_REQUEST, Json(serde_json::json!({"code": -2011, "msg": "Unknown order sent."})));
                    }
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
            .route(
                "/fapi/v1/batchOrders",
                delete(|State(fake): State<PairExchange>, Query(query): Query<HashMap<String, String>>| async move {
                    fake.batch_cancel_calls.fetch_add(1, Ordering::SeqCst);
                    if fake.block_cancel.load(Ordering::SeqCst) != 0 {
                        fake.cancel_started.notify_one();
                        fake.release_cancel.notified().await;
                    }
                    let id_targets = query
                        .get("orderIdList")
                        .and_then(|raw| serde_json::from_str::<Vec<i64>>(raw).ok());
                    let cid_targets = query
                        .get("origClientOrderIdList")
                        .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok());
                    let count = id_targets
                        .as_ref()
                        .map_or_else(|| cid_targets.as_ref().map_or(0, Vec::len), Vec::len);
                    let mut orders = fake.orders.lock().unwrap();
                    let mut items = Vec::with_capacity(count);
                    for idx in 0..count {
                        if consume_failure(&fake.fail_cancel) {
                            items.push(serde_json::json!({"code": -1000, "msg": "cancel failed"}));
                            continue;
                        }
                        let found = if let Some(ref ids) = id_targets {
                            let target_id = ids[idx];
                            orders.values_mut().find(|order| order.order_id == target_id)
                        } else {
                            let target_cid = &cid_targets.as_ref().unwrap()[idx];
                            orders.values_mut().find(|order| &order.client_order_id == target_cid)
                        };
                        match found {
                            Some(order) if !order.is_terminal() => {
                                order.status = "CANCELED".into();
                                items.push(serde_json::to_value(order.clone()).unwrap());
                            }
                            _ => {
                                items.push(serde_json::json!({"code": -2011, "msg": "Unknown order sent."}));
                            }
                        }
                    }
                    (axum::http::StatusCode::OK, Json(serde_json::Value::Array(items)))
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
        commission_asset: None,
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

mod credentials_and_scope_transitions;
mod lifecycle_and_cancel;
mod market_feed_and_fallbacks;
mod market_identity;
mod pair_intent_lifecycle;
mod paper_and_sizing;
mod reconcile_and_mode_cancel;
