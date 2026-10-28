use super::*;
use crate::{config::AppConfig, db::Database};
use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use rust_decimal_macros::dec;
use serde_json::json;
use std::sync::Mutex;

async fn engine(direction: GridDirection) -> GridTradingEngine {
    let mut config = AppConfig::default();
    config.grid.direction = direction;
    config.grid.grid_interval = dec!(1);
    config.grid.order_amount_usdc = dec!(100);
    config.grid.buy_window = 3;
    config.grid.sell_window = 3;
    let (tx, rx) = mpsc::channel(1);
    let (_, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(
        config.clone(),
        Arc::new(Database::open(":memory:").unwrap()),
        tx,
    );
    *state.ticker.write().await = TickerInfo {
        symbol: "SOLUSDC".into(),
        last_price: dec!(100.5),
        mark_price: dec!(100.5),
        update_time: Utc::now(),
        mark_update_time: Utc::now(),
        ..Default::default()
    };
    GridTradingEngine::new(
        state,
        Arc::new(BinanceFuturesClient::new(&config.exchange)),
        rx,
        ticker_rx,
    )
}

fn order(id: &str, side: OrderSide, price: Decimal, quantity: Decimal) -> GridOrder {
    GridOrder {
        client_order_id: format!("gb_{id}"),
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
    }
}

async fn active(engine: &GridTradingEngine) -> Vec<GridOrder> {
    engine
        .state
        .active_orders
        .read()
        .await
        .values()
        .cloned()
        .collect()
}

#[tokio::test]
async fn empty_short_starts_with_only_opening_sells_then_reserves_closing_buys() {
    let mut engine = engine(GridDirection::Short).await;
    engine.maintain_grid_window().await;
    let sells = active(&engine).await;
    assert_eq!(sells.len(), 3);
    assert!(sells.iter().all(|o| o.side == OrderSide::Sell));
    let mut fill = sells.iter().find(|o| o.price == dec!(101)).unwrap().clone();
    assert!(engine.on_order_filled(&mut fill).await);
    engine.maintain_grid_window().await;
    let orders = active(&engine).await;
    let size = engine.state.position.read().await.size;
    assert_eq!(size, -fill.quantity);
    assert!(orders.iter().any(|o| o.side == OrderSide::Buy));
    assert!(reserved_closing_quantity(GridDirection::Short, &orders) <= -size);
}

#[tokio::test]
async fn paper_short_average_entry_and_partial_closes_record_profit_and_loss() {
    let mut engine = engine(GridDirection::Short).await;
    let wallet = engine.state.account.read().await.total_wallet_balance;
    for (id, price) in [("entry_a", dec!(110)), ("entry_b", dec!(120))] {
        assert!(
            engine
                .on_order_filled(&mut order(id, OrderSide::Sell, price, dec!(1)))
                .await
        );
    }
    let pos = engine.state.position.read().await.clone();
    assert_eq!(pos.size, dec!(-2));
    assert_eq!(pos.entry_price, dec!(115));
    assert_eq!(pos.unrealized_pnl, dec!(29));
    let mut profit = order("profit", OrderSide::Buy, dec!(100), dec!(0.5));
    assert!(engine.on_order_filled(&mut profit).await);
    assert!(engine.on_order_filled(&mut profit).await); // Reconciliation is idempotent.
    assert_eq!(engine.state.position.read().await.size, dec!(-1.5));
    assert_eq!(engine.state.position.read().await.entry_price, dec!(115));
    assert_eq!(
        engine.state.account.read().await.total_wallet_balance,
        wallet + dec!(7.5)
    );
    assert!(
        engine
            .on_order_filled(&mut order("loss", OrderSide::Buy, dec!(120), dec!(1.5)))
            .await
    );
    let pos = engine.state.position.read().await.clone();
    assert_eq!(pos.size, Decimal::ZERO);
    assert_eq!(pos.entry_price, Decimal::ZERO);
    assert_eq!(pos.unrealized_pnl, Decimal::ZERO);
    assert_eq!(
        engine.state.account.read().await.total_wallet_balance,
        wallet
    );
    assert_eq!(engine.state.stats.read().await.total_trades, 4);
    assert_eq!(
        engine.state.stats.read().await.total_realized_pnl,
        Decimal::ZERO
    );
    let trade = engine
        .state
        .db
        .get_trade_by_client_id("gb_profit", TradingMode::Paper)
        .unwrap()
        .unwrap();
    assert_eq!(trade.realized_pnl, dec!(7.5));
    let trade = engine
        .state
        .db
        .get_trade_by_client_id("gb_loss", TradingMode::Paper)
        .unwrap()
        .unwrap();
    assert_eq!(trade.realized_pnl, dec!(-7.5));
}

#[tokio::test]
async fn short_cap_counts_pending_sells_and_does_not_block_closing_buys() {
    let mut engine = engine(GridDirection::Short).await;
    engine.state.position.write().await.size = dec!(-4);
    engine.state.config.write().await.grid.max_position_usdc = Some(dec!(550));
    assert!(
        engine
            .place_grid_order(OrderSide::Sell, dec!(101), "gb_open_a".into(), 1, None)
            .await
    );
    assert!(
        !engine
            .place_grid_order(OrderSide::Sell, dec!(102), "gb_open_b".into(), 1, None)
            .await
    );
    assert!(
        engine
            .place_grid_order(OrderSide::Buy, dec!(100), "gb_close".into(), -1, None)
            .await
    );
    assert!(
        opening_exposure_usdc(
            GridDirection::Short,
            dec!(-4),
            dec!(101),
            &active(&engine).await
        ) <= dec!(550)
    );
    assert_eq!(*engine.state.status.read().await, BotStatus::Running);
}

#[tokio::test]
async fn next_short_opening_order_over_cap_pauses_and_cancels_both_sides() {
    let mut engine = engine(GridDirection::Short).await;
    engine.state.position.write().await.size = dec!(-9.1);
    {
        let mut config = engine.state.config.write().await;
        config.grid.max_position_usdc = Some(dec!(1000));
        config.grid.pause_on_max_position = true;
    }
    for pending in [
        order("pending_buy", OrderSide::Buy, dec!(99), dec!(1)),
        order("pending_sell", OrderSide::Sell, dec!(103), dec!(1)),
    ] {
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(pending.client_order_id.clone(), pending);
    }
    assert!(
        !engine
            .place_grid_order(OrderSide::Sell, dec!(101), "gb_over_limit".into(), 1, None)
            .await
    );
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert!(active(&engine).await.is_empty());
    assert_eq!(engine.state.position.read().await.size, dec!(-9.1));
    assert_eq!(
        engine.state.db.load_bot_status().unwrap(),
        Some(BotStatus::Paused)
    );
}

#[tokio::test]
async fn short_closing_buys_trim_farthest_first_without_canceling_opening_sells() {
    let mut engine = engine(GridDirection::Short).await;
    engine.state.position.write().await.size = dec!(-1.5);
    for pending in [
        order("nearest", OrderSide::Buy, dec!(100), dec!(1)),
        order("far", OrderSide::Buy, dec!(99), dec!(1)),
        order("opening", OrderSide::Sell, dec!(101), dec!(1)),
    ] {
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(pending.client_order_id.clone(), pending);
    }
    engine.trim_closing_orders_to_position().await;
    let orders = active(&engine).await;
    assert_eq!(orders.len(), 2);
    assert!(orders.iter().any(|o| o.client_order_id == "gb_nearest"));
    assert!(orders.iter().any(|o| o.client_order_id == "gb_opening"));
}

#[tokio::test]
async fn paper_closing_buy_rechecks_inventory_at_execution_and_never_opens_long() {
    let mut engine = engine(GridDirection::Short).await;
    engine.state.position.write().await.size = dec!(-0.5);
    engine.state.position.write().await.entry_price = dec!(110);
    let pending = order("oversized_close", OrderSide::Buy, dec!(100), dec!(2));
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(pending.client_order_id.clone(), pending);
    engine.simulate_order_fills(dec!(100)).await;
    assert_eq!(engine.state.position.read().await.size, Decimal::ZERO);
    let trade = engine
        .state
        .db
        .get_trade_by_client_id("gb_oversized_close", TradingMode::Paper)
        .unwrap()
        .unwrap();
    assert_eq!(trade.quantity, dec!(0.5));
    assert_eq!(trade.realized_pnl, dec!(5));
}

#[tokio::test]
async fn short_remainders_coalesce_into_one_nearest_buy_and_keep_identity() {
    let mut engine = engine(GridDirection::Short).await;
    engine.state.config.write().await.grid.order_amount_usdc = dec!(3000);
    engine.state.position.write().await.size = dec!(-0.48);
    for mut source in [
        order("tail_a", OrderSide::Buy, dec!(99), dec!(0.2)),
        order("tail_b", OrderSide::Buy, dec!(98), dec!(0.28)),
    ] {
        source.purpose = OrderPurpose::Remainder;
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(source.client_order_id.clone(), source);
    }
    engine.maintain_grid_window().await;
    let orders = active(&engine).await;
    assert_eq!(orders.len(), 1);
    let tail = &orders[0];
    assert_eq!(tail.side, OrderSide::Buy);
    assert_eq!(tail.price, dec!(100));
    assert_eq!(tail.quantity, dec!(0.48));
    assert_eq!(tail.purpose, OrderPurpose::Remainder);
    assert_eq!(tail.merge_sources.len(), 2);
    engine.maintain_grid_window().await;
    assert!(engine
        .state
        .active_orders
        .read()
        .await
        .contains_key(&tail.client_order_id));
}

#[tokio::test]
async fn direction_changes_require_flat_position_and_cancel_old_orders_before_saving() {
    let mut engine = engine(GridDirection::Long).await;
    engine.state.position.write().await.size = dec!(1);
    let mut next = engine.state.config.read().await.clone();
    next.grid.direction = GridDirection::Short;
    assert!(engine
        .apply_config(next.clone())
        .await
        .unwrap_err()
        .to_string()
        .contains("平掉"));
    assert_eq!(
        engine.state.config.read().await.grid.direction,
        GridDirection::Long
    );
    engine.state.position.write().await.size = Decimal::ZERO;
    let pending = order("old_buy", OrderSide::Buy, dec!(100), dec!(1));
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(pending.client_order_id.clone(), pending);
    engine.apply_config(next).await.unwrap();
    assert_eq!(
        engine.state.config.read().await.grid.direction,
        GridDirection::Short
    );
    assert_eq!(
        engine
            .state
            .db
            .load_config()
            .unwrap()
            .unwrap()
            .grid
            .direction,
        GridDirection::Short
    );
    assert!(!engine
        .state
        .active_orders
        .read()
        .await
        .contains_key("gb_old_buy"));
    assert!(active(&engine)
        .await
        .iter()
        .all(|o| o.side == OrderSide::Sell));
    let mut next = engine.state.config.read().await.clone();
    next.grid.direction = GridDirection::Long;
    engine.apply_config(next).await.unwrap();
    assert!(active(&engine)
        .await
        .iter()
        .all(|o| o.side == OrderSide::Buy));
}

#[tokio::test]
async fn incompatible_manual_position_pauses_instead_of_opening_orders() {
    for (direction, size) in [
        (GridDirection::Short, dec!(1)),
        (GridDirection::Long, dec!(-1)),
    ] {
        let mut engine = engine(direction).await;
        engine.state.position.write().await.size = size;
        engine.maintain_grid_window().await;
        assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
        assert!(active(&engine).await.is_empty());
        assert_eq!(engine.state.position.read().await.size, size);
    }
}

#[derive(Clone, Default)]
struct FakeExchange {
    position: Arc<Mutex<Decimal>>,
    orders: Arc<Mutex<HashMap<String, BinanceOrderResponse>>>,
    posts: Arc<Mutex<Vec<HashMap<String, String>>>>,
    fill_on_cancel: bool,
}

async fn fake_server(fake: FakeExchange) -> (String, JoinHandle<()>) {
    let app = Router::new()
        .route("/fapi/v1/time", get(|| async { Json(json!({"serverTime": Utc::now().timestamp_millis()})) }))
        .route("/fapi/v2/positionRisk", get(|State(fake): State<FakeExchange>| async move {
            Json(json!([{"symbol": "SOLUSDC", "positionSide": "BOTH", "positionAmt": fake.position.lock().unwrap().to_string(),
                "entryPrice": "100", "markPrice": "100.5", "unRealizedProfit": "0", "liquidationPrice": "0", "leverage": "20"}]))
        }))
        .route("/fapi/v2/account", get(|| async { Json(json!({
            "totalWalletBalance": "10000", "totalMarginBalance": "10000", "totalUnrealizedProfit": "0", "availableBalance": "10000",
            "assets": [{"asset": "USDC", "walletBalance": "10000", "marginBalance": "10000", "availableBalance": "10000", "unrealizedProfit": "0"}]
        })) }))
        .route("/fapi/v1/openOrders", get(|State(fake): State<FakeExchange>| async move {
            Json(fake.orders.lock().unwrap().values().filter(|o| o.status == "NEW").cloned().collect::<Vec<_>>())
        }))
        .route("/fapi/v1/userTrades", get(|| async { Json(json!([{"realizedPnl": "0", "commission": "0", "commissionAsset": "USDC", "maker": true}])) }))
        .route("/fapi/v1/order", get(|State(fake): State<FakeExchange>, Query(query): Query<HashMap<String, String>>| async move {
            let orders = fake.orders.lock().unwrap();
            let found = orders.values().find(|o| query.get("origClientOrderId") == Some(&o.client_order_id)
                || query.get("orderId") == Some(&o.order_id.to_string()));
            match found {
                Some(found) => (reqwest::StatusCode::OK, Json(serde_json::to_value(found).unwrap())),
                None => (reqwest::StatusCode::BAD_REQUEST, Json(json!({"code": -2013, "msg": "Order does not exist"}))),
            }
        }).post(|State(fake): State<FakeExchange>, Query(query): Query<HashMap<String, String>>| async move {
            fake.posts.lock().unwrap().push(query.clone());
            let mut found = order("placeholder", if query["side"] == "BUY" { OrderSide::Buy } else { OrderSide::Sell }, query["price"].parse().unwrap(), query["quantity"].parse().unwrap());
            found.client_order_id = query["newClientOrderId"].clone();
            let response = BinanceOrderResponse {
                order_id: fake.posts.lock().unwrap().len() as i64,
                client_order_id: found.client_order_id.clone(), symbol: found.symbol, status: "NEW".into(), price: found.price,
                avg_price: None, orig_qty: found.quantity, executed_qty: Decimal::ZERO, side: query["side"].clone(),
                order_type: query["type"].clone(), time_in_force: query["timeInForce"].clone(), update_time: None,
            };
            fake.orders.lock().unwrap().insert(response.client_order_id.clone(), response.clone());
            Json(response)
        }).delete(|State(fake): State<FakeExchange>, Query(query): Query<HashMap<String, String>>| async move {
            let mut orders = fake.orders.lock().unwrap();
            let found = orders.values_mut().find(|o| query.get("origClientOrderId") == Some(&o.client_order_id)
                || query.get("orderId") == Some(&o.order_id.to_string())).unwrap();
            found.status = if fake.fill_on_cancel { "FILLED" } else { "CANCELED" }.into();
            if fake.fill_on_cancel {
                found.executed_qty = found.orig_qty;
                found.avg_price = Some(found.price);
                *fake.position.lock().unwrap() += if found.side == "BUY" { found.orig_qty } else { -found.orig_qty };
            }
            Json(json!({}))
        }))
        .with_state(fake);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (url, server)
}

async fn live_engine(direction: GridDirection, url: String) -> GridTradingEngine {
    let mut engine = engine(direction).await;
    let mut config = engine.state.config.write().await;
    config.exchange.dry_run = false;
    config.exchange.api_key = "test-key".into();
    config.exchange.api_secret = "test-secret".into();
    engine.client = Arc::new(BinanceFuturesClient::with_base_url(&config.exchange, url));
    drop(config);
    // This fixture starts after exchange rules have already been installed.
    engine.exchange_rules_loaded = true;
    assert!(engine.sync_account_and_position().await);
    assert!(engine.sync_live_orders().await);
    engine
}

#[tokio::test]
async fn live_opposite_position_cancels_opening_orders_even_with_stale_prices() {
    for (direction, opening_side, price, manual_position) in [
        (GridDirection::Short, OrderSide::Sell, dec!(101), dec!(1)),
        (GridDirection::Long, OrderSide::Buy, dec!(100), dec!(-1)),
    ] {
        let fake = FakeExchange::default();
        let (url, server) = fake_server(fake.clone()).await;
        let mut engine = live_engine(direction, url).await;
        assert!(
            engine
                .place_grid_order(opening_side, price, "gb_stale_open".into(), 0, None)
                .await
        );
        *fake.position.lock().unwrap() = manual_position;
        {
            let mut ticker = engine.state.ticker.write().await;
            ticker.update_time = Utc::now() - chrono::Duration::seconds(20);
            ticker.mark_update_time = ticker.update_time;
        }
        // This exchange still serves account/order requests, but price endpoints
        // are unavailable, so the sync cannot refresh the expired market data.
        engine.sync_cycle().await;

        assert!(!engine
            .state
            .ticker
            .read()
            .await
            .prices_are_fresh(Utc::now()));
        assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
        assert!(active(&engine).await.is_empty());
        assert_eq!(engine.state.position.read().await.size, manual_position);
        assert_eq!(*fake.position.lock().unwrap(), manual_position);
        assert_eq!(
            fake.orders.lock().unwrap()["gb_stale_open"].status,
            "CANCELED"
        );
        assert_eq!(fake.posts.lock().unwrap().len(), 1);
        assert!(engine
            .state
            .db
            .load_managed_orders("SOLUSDC")
            .unwrap()
            .is_empty());
        assert_eq!(
            engine.state.db.load_bot_status().unwrap(),
            Some(BotStatus::Paused)
        );
        server.abort();
    }
}

#[tokio::test]
async fn live_short_buys_are_reduce_only_and_opening_sells_are_not() {
    let fake = FakeExchange::default();
    *fake.position.lock().unwrap() = dec!(-2);
    let (url, server) = fake_server(fake.clone()).await;
    let mut engine = live_engine(GridDirection::Short, url).await;
    assert!(
        engine
            .place_grid_order(OrderSide::Sell, dec!(101), "gb_live_open".into(), 1, None)
            .await
    );
    assert!(
        engine
            .place_grid_order(OrderSide::Buy, dec!(100), "gb_live_close".into(), -1, None)
            .await
    );
    let posts = fake.posts.lock().unwrap();
    assert_eq!(posts.len(), 2);
    assert_eq!(posts[0]["side"], "SELL");
    assert!(!posts[0].contains_key("reduceOnly"));
    assert_eq!(posts[1]["side"], "BUY");
    assert_eq!(posts[1]["reduceOnly"], "true");
    assert!(posts
        .iter()
        .all(|p| p["type"] == "LIMIT" && p["timeInForce"] == "GTX"));
    server.abort();
}

#[tokio::test]
async fn live_short_tail_is_submitted_as_reduce_only_buy_and_restored() {
    let fake = FakeExchange::default();
    *fake.position.lock().unwrap() = dec!(-0.48);
    let (url, server) = fake_server(fake.clone()).await;
    let mut engine = live_engine(GridDirection::Short, url).await;
    engine.state.config.write().await.grid.order_amount_usdc = dec!(3000);
    engine.reconcile_remainder(&[dec!(100)]).await.unwrap();
    {
        let posts = fake.posts.lock().unwrap();
        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0]["side"], "BUY");
        assert_eq!(posts[0]["reduceOnly"], "true");
        assert_eq!(posts[0]["quantity"], "0.48");
    }
    let saved = engine.state.db.load_managed_orders("SOLUSDC").unwrap();
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].side, OrderSide::Buy);
    assert_eq!(saved[0].purpose, OrderPurpose::Remainder);
    // Simulate restarting the workflow after the exchange accepted its target.
    let plan = RemainderPlan {
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Live,
        sources: vec![],
        target: saved[0].clone(),
        phase: RemainderPhase::Submitting,
    };
    engine.state.db.save_remainder_plan(&plan).unwrap();
    engine.state.active_orders.write().await.clear();
    assert!(engine.recover_remainder_plan().await.unwrap());
    assert_eq!(active(&engine).await[0].side, OrderSide::Buy);
    assert_eq!(fake.posts.lock().unwrap().len(), 1);
    server.abort();
}

#[tokio::test]
async fn direction_switch_rechecks_live_position_when_cancellation_races_with_fill() {
    let fake = FakeExchange {
        fill_on_cancel: true,
        ..Default::default()
    };
    let (url, server) = fake_server(fake.clone()).await;
    let mut engine = live_engine(GridDirection::Long, url).await;
    assert!(
        engine
            .place_grid_order(OrderSide::Buy, dec!(100), "gb_racing_buy".into(), -1, None)
            .await
    );
    let mut next = engine.state.config.read().await.clone();
    next.grid.direction = GridDirection::Short;
    assert!(engine.apply_config(next).await.is_err());
    assert_eq!(
        engine.state.config.read().await.grid.direction,
        GridDirection::Long
    );
    assert_eq!(engine.state.position.read().await.size, dec!(1));
    assert_eq!(*engine.state.status.read().await, BotStatus::Paused);
    assert!(active(&engine).await.is_empty());
    assert_eq!(fake.posts.lock().unwrap().len(), 1);
    server.abort();
}
