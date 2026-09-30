use super::*;
use crate::{config::AppConfig, db::Database};
use rust_decimal_macros::dec;

async fn paper_engine() -> GridTradingEngine {
    let mut config = AppConfig::default();
    config.exchange.dry_run = true;
    config.grid.order_amount_usdc = dec!(200);
    config.grid.buy_window = 3;
    config.grid.sell_window = 3;
    config.grid.max_position_usdc = Some(dec!(2000));
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, rx) = mpsc::channel(1);
    let (_, ticker_rx) = broadcast::channel(1);
    let state = AppState::new(config.clone(), db, tx);
    state.ticker.write().await.last_price = dec!(118.84);
    let client = Arc::new(BinanceFuturesClient::new(&config.exchange));
    GridTradingEngine::new(state, client, rx, ticker_rx)
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

async fn orders(engine: &GridTradingEngine) -> Vec<GridOrder> {
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
async fn sell_window_ignores_sold_levels_counts_nearby_exit_and_prunes_old_buys() {
    let mut engine = paper_engine().await;
    engine.state.config.write().await.grid.order_amount_usdc = dec!(300);
    engine.state.config.write().await.grid.max_position_usdc = None;
    engine.state.ticker.write().await.last_price = dec!(119.21);
    engine.state.position.write().await.size = dec!(20);
    for (id, price) in [dec!(119.3), dec!(119.5)].into_iter().enumerate() {
        engine
            .state
            .db
            .insert_trade(&journal_trade(id, OrderSide::Sell, price, dec!(2.51)))
            .unwrap();
    }
    let parent = order("b_parent", OrderSide::Buy, dec!(119.3), dec!(2.51));
    let exit = new_pair_intent(&parent, OrderSide::Sell, dec!(119.4), parent.quantity);
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(exit.client_order_id.clone(), exit.clone());
    for price in [
        dec!(119.2),
        dec!(119.1),
        dec!(119),
        dec!(118.9),
        dec!(118.8),
        dec!(118.7),
        dec!(118.6),
        dec!(118.5),
        dec!(118.4),
        dec!(118.3),
        dec!(118.2),
    ] {
        let buy = order(&format!("b_{price}"), OrderSide::Buy, price, dec!(2.51));
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(buy.client_order_id.clone(), buy);
    }
    let retained = orders(&engine).await;
    assert_eq!(retained.len(), 12);
    // Pruning is followed by a separate reconciliation/placement cycle.
    engine.maintain_grid_window().await;
    engine.maintain_grid_window().await;
    let active = orders(&engine).await;
    for (side, expected) in [
        (OrderSide::Buy, vec![dec!(119), dec!(119.1), dec!(119.2)]),
        (OrderSide::Sell, vec![dec!(119.3), dec!(119.4), dec!(119.5)]),
    ] {
        let mut prices: Vec<_> = active
            .iter()
            .filter(|o| o.side == side)
            .map(|o| o.price)
            .collect();
        prices.sort();
        assert_eq!(prices, expected);
    }
    assert!(active
        .iter()
        .any(|o| o.client_order_id == exit.client_order_id));
    for buy in retained
        .iter()
        .filter(|o| o.side == OrderSide::Buy && o.price >= dec!(119))
    {
        assert!(active
            .iter()
            .any(|o| o.client_order_id == buy.client_order_id));
    }
    engine.maintain_grid_window().await;
    let stable = orders(&engine).await;
    assert_eq!(stable.len(), active.len());
    assert!(active.iter().all(|o| stable
        .iter()
        .any(|s| s.client_order_id == o.client_order_id)));
    assert!(reserved_sell_quantity(&active) <= engine.state.position.read().await.size);
}

#[tokio::test]
async fn window_cleanup_prunes_stale_buys_immediately_and_keeps_queue_priority() {
    for (buy_window, sell_window) in [(3, 3), (2, 1)] {
        let mut engine = paper_engine().await;
        {
            let mut config = engine.state.config.write().await;
            config.grid.buy_window = buy_window;
            config.grid.sell_window = sell_window;
            config.grid.max_position_usdc = None;
        }
        engine.state.position.write().await.size = dec!(100);
        engine.maintain_grid_window().await;
        let original = orders(&engine).await;
        let stale = order("b_far", OrderSide::Buy, dec!(118), dec!(1.69));
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(stale.client_order_id.clone(), stale);
        engine.maintain_grid_window().await;
        let active = orders(&engine).await;
        assert_eq!(active.len(), buy_window + sell_window);
        assert!(original.iter().all(|o| active
            .iter()
            .any(|a| a.client_order_id == o.client_order_id)));
    }
}

#[tokio::test]
async fn cleanup_releases_stale_buy_funds_before_replenishment() {
    let mut engine = paper_engine().await;
    engine.state.config.write().await.grid.max_position_usdc = None;
    engine.state.position.write().await.size = dec!(100);
    // Stale buys must release their funds before the missing near buy is placed.
    for (side, prices) in [
        (
            OrderSide::Buy,
            vec![
                dec!(118.8),
                dec!(118.7),
                dec!(118.5),
                dec!(118.4),
                dec!(118.3),
                dec!(118.2),
                dec!(118.1),
                dec!(118),
            ],
        ),
        (OrderSide::Sell, vec![dec!(118.9), dec!(119), dec!(119.1)]),
    ] {
        for price in prices {
            let o = order(
                &format!("{}_{}", side.as_str(), price),
                side,
                price,
                dec!(1.68),
            );
            engine
                .state
                .active_orders
                .write()
                .await
                .insert(o.client_order_id.clone(), o);
        }
    }
    assert_eq!(orders(&engine).await.len(), 11);
    engine.maintain_grid_window().await;
    let active = orders(&engine).await;
    assert_eq!(active.len(), 5);
    engine.maintain_grid_window().await;
    let active = orders(&engine).await;
    assert_eq!(active.len(), 6);
    assert_eq!(
        active.iter().filter(|o| o.side == OrderSide::Buy).count(),
        3
    );
    assert_eq!(
        active.iter().filter(|o| o.side == OrderSide::Sell).count(),
        3
    );
    assert!(active
        .iter()
        .any(|o| o.side == OrderSide::Buy && o.price == dec!(118.6)));
}

#[tokio::test]
async fn buy_window_ignores_take_profit_reservation_and_respects_price_bounds() {
    let mut engine = paper_engine().await;
    {
        let mut config = engine.state.config.write().await;
        config.grid.min_price = Some(dec!(118.5));
        config.grid.max_price = Some(dec!(119));
        config.grid.max_position_usdc = None;
    }
    engine.state.position.write().await.size = dec!(20);
    let parent = order("b_parent", OrderSide::Buy, dec!(118.8), dec!(1.68));
    let exit = new_pair_intent(&parent, OrderSide::Sell, dec!(118.9), parent.quantity);
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(exit.client_order_id.clone(), exit);
    engine.maintain_grid_window().await;
    let active = orders(&engine).await;
    let mut buys: Vec<_> = active
        .iter()
        .filter(|o| o.side == OrderSide::Buy)
        .map(|o| o.price)
        .collect();
    buys.sort();
    assert_eq!(buys, vec![dec!(118.6), dec!(118.7), dec!(118.8)]);
    assert_eq!(
        active.iter().filter(|o| o.side == OrderSide::Sell).count(),
        2
    );
    assert!(active
        .iter()
        .all(|o| o.price >= dec!(118.5) && o.price <= dec!(119)));
}

#[tokio::test]
async fn price_jitter_and_engine_restart_preserve_grid_and_queue_priority() {
    let mut engine = paper_engine().await;
    engine.state.position.write().await.size = dec!(5.04);
    engine.maintain_grid_window().await;
    let original = orders(&engine).await;
    assert_eq!(original.len(), 6);
    for price in [dec!(118.81), dec!(118.87), dec!(118.89)] {
        engine.state.ticker.write().await.last_price = price;
        engine.maintain_grid_window().await;
        let active = orders(&engine).await;
        assert_eq!(active.len(), original.len());
        for expected in &original {
            let actual = active
                .iter()
                .find(|o| o.client_order_id == expected.client_order_id)
                .unwrap();
            assert_eq!(actual.price, expected.price);
            assert_eq!(actual.price % dec!(0.1), Decimal::ZERO);
            assert!(actual.amount_usdc <= dec!(200));
            assert!(dec!(200) - actual.amount_usdc < actual.price * dec!(0.01));
        }
    }
    let (_, rx) = mpsc::channel(1);
    let (_, ticker_rx) = broadcast::channel(1);
    let mut restarted =
        GridTradingEngine::new(engine.state.clone(), engine.client.clone(), rx, ticker_rx);
    restarted.maintain_grid_window().await;
    let active = orders(&restarted).await;
    assert_eq!(active.len(), original.len());
    assert!(original.iter().all(|o| active
        .iter()
        .any(|a| a.client_order_id == o.client_order_id)));
}

#[tokio::test]
async fn resizing_window_preserves_exits_and_partial_fills_and_releases_inventory() {
    let mut engine = paper_engine().await;
    engine.state.config.write().await.grid.max_position_usdc = None;
    engine.state.position.write().await.size = dec!(20);
    engine.maintain_grid_window().await;
    let parent = order("b_high", OrderSide::Buy, dec!(120), dec!(1.66));
    let exit = new_pair_intent(&parent, OrderSide::Sell, dec!(120.1), parent.quantity);
    let mut partial = order("b_partial_far", OrderSide::Buy, dec!(118), dec!(0.4));
    partial.status = OrderStatus::PartiallyFilled;
    for protected in [&exit, &partial] {
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(protected.client_order_id.clone(), protected.clone());
    }
    let mut config = engine.state.config.read().await.clone();
    config.grid.buy_window = 1;
    config.grid.sell_window = 1;
    engine.apply_config(config).await.unwrap();
    engine.maintain_grid_window().await;
    let active = orders(&engine).await;
    assert_eq!(active.len(), 4);
    assert!(active
        .iter()
        .any(|o| o.side == OrderSide::Buy && o.price == dec!(118.8)));
    assert!(active
        .iter()
        .any(|o| o.side == OrderSide::Sell && o.price == dec!(118.9)));
    for protected in [&exit, &partial] {
        assert!(active
            .iter()
            .any(|o| o.client_order_id == protected.client_order_id));
    }
    // A larger window reuses the freed inventory at the nearest eligible prices.
    let mut config = engine.state.config.read().await.clone();
    config.grid.sell_window = 3;
    engine.apply_config(config).await.unwrap();
    let active = orders(&engine).await;
    let mut sells: Vec<_> = active
        .iter()
        .filter(|o| o.side == OrderSide::Sell)
        .map(|o| o.price)
        .collect();
    sells.sort();
    assert_eq!(
        sells,
        vec![dec!(118.9), dec!(119), dec!(119.1), dec!(120.1)]
    );
    assert!(active
        .iter()
        .any(|o| o.client_order_id == exit.client_order_id));
}

#[tokio::test]
async fn far_paired_exits_reserve_inventory_without_displacing_nearby_sells() {
    let mut engine = paper_engine().await;
    engine.state.config.write().await.grid.sell_window = 1;
    engine.state.config.write().await.grid.max_position_usdc = None;
    engine.state.position.write().await.size = dec!(20);
    for price in [dec!(119), dec!(120), dec!(121)] {
        let parent = order(&format!("b_{price}"), OrderSide::Buy, price, dec!(1.6));
        let exit = new_pair_intent(&parent, OrderSide::Sell, price + dec!(0.1), parent.quantity);
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(exit.client_order_id.clone(), exit);
    }
    engine.maintain_grid_window().await;
    let active = orders(&engine).await;
    let sells: Vec<_> = active
        .iter()
        .filter(|o| o.side == OrderSide::Sell)
        .collect();
    assert_eq!(sells.len(), 4);
    assert_eq!(sells.iter().filter(|o| o.is_take_profit).count(), 3);
    assert!(sells
        .iter()
        .any(|o| !o.is_take_profit && o.price == dec!(118.9)));
    assert!(reserved_sell_quantity(&active) <= engine.state.position.read().await.size);
}

#[tokio::test]
async fn full_cycle_preserves_quantity_and_rebuys_nearest_target_notional() {
    let mut engine = paper_engine().await;
    {
        let mut config = engine.state.config.write().await;
        config.grid.grid_interval = dec!(1);
        config.grid.order_amount_usdc = dec!(2000);
        config.grid.buy_window = 1;
        config.grid.sell_window = 1;
        config.grid.max_position_usdc = Some(dec!(3000));
    }
    engine.state.ticker.write().await.last_price = dec!(114.5);
    let mut buy = order("b_cycle", OrderSide::Buy, dec!(114), dec!(17.54));
    assert!(engine.on_order_filled(&mut buy).await);
    let mut sell = orders(&engine).await.pop().unwrap();
    assert_eq!(sell.side, OrderSide::Sell);
    assert_eq!(sell.price, dec!(115));
    assert_eq!(sell.quantity, buy.quantity);
    assert_eq!(sell.amount_usdc, dec!(2017.10));
    assert!(sell.is_take_profit);

    engine.maintain_grid_window().await;
    assert_eq!(
        orders(&engine).await.len(),
        1,
        "position limit blocks further full-size buys"
    );
    engine.state.ticker.write().await.last_price = dec!(115.1);
    assert!(engine.on_order_filled(&mut sell).await);
    assert_eq!(engine.state.position.read().await.size, Decimal::ZERO);
    assert!(
        orders(&engine).await.is_empty(),
        "historical 114 rebuy is outside the nearest window"
    );
    engine.maintain_grid_window().await;
    let active = orders(&engine).await;
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].side, OrderSide::Buy);
    assert_eq!(active[0].price, dec!(115));
    assert_eq!(active[0].quantity, dec!(17.39));
    assert_eq!(engine.state.stats.read().await.completed_cycles, 1);
    let trades = engine.state.db.get_recent_trades(10).unwrap();
    assert_eq!(trades.len(), 2);
    let exit = trades.iter().find(|t| t.side == OrderSide::Sell).unwrap();
    assert!(exit.note.contains("Paired Buy: 114"));
    assert_eq!(exit.realized_pnl, dec!(17.54));
}

#[tokio::test]
async fn small_buy_gets_equal_exit_without_expanding_into_full_size_rebuy() {
    let mut engine = paper_engine().await;
    let mut buy = order("b_partial", OrderSide::Buy, dec!(118.8), dec!(0.46));
    assert!(engine.on_order_filled(&mut buy).await);
    let mut sell = orders(&engine).await.pop().unwrap();
    assert_eq!(sell.quantity, dec!(0.46));
    assert_eq!(sell.price, dec!(118.9));
    engine.state.ticker.write().await.last_price = dec!(118.95);
    assert!(engine.on_order_filled(&mut sell).await);
    assert!(orders(&engine).await.is_empty());
    assert_eq!(engine.state.position.read().await.size, Decimal::ZERO);
}

#[tokio::test]
async fn migration_keeps_exits_and_partial_fills_then_adds_only_aligned_orders() {
    let mut engine = paper_engine().await;
    engine.state.position.write().await.size = dec!(1.68);
    let old = order("b_old", OrderSide::Buy, dec!(118.71), dec!(1.68));
    let mut partial = order("b_partial", OrderSide::Buy, dec!(118.59), dec!(0.5));
    partial.status = OrderStatus::PartiallyFilled;
    let parent = order("b_parent", OrderSide::Buy, dec!(118.84), dec!(1.68));
    let exit = new_pair_intent(&parent, OrderSide::Sell, dec!(118.94), parent.quantity);
    for o in [&old, &partial, &exit] {
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(o.client_order_id.clone(), o.clone());
    }
    engine.maintain_grid_window().await;
    let active = orders(&engine).await;
    assert_eq!(active.len(), 2);
    assert!(!active
        .iter()
        .any(|o| o.client_order_id == old.client_order_id));
    assert!(active
        .iter()
        .any(|o| o.client_order_id == partial.client_order_id));
    assert!(active
        .iter()
        .any(|o| o.client_order_id == exit.client_order_id));
    engine.maintain_grid_window().await;
    let active = orders(&engine).await;
    assert!(active.iter().any(|o| o.price == dec!(118.7)));
    assert!(active
        .iter()
        .filter(|o| o.client_order_id != partial.client_order_id
            && o.client_order_id != exit.client_order_id)
        .all(|o| o.price % dec!(0.1) == Decimal::ZERO));
}

#[tokio::test]
async fn duplicate_buy_and_position_limit_apply_to_replenishment_and_pairs() {
    let mut engine = paper_engine().await;
    engine.state.config.write().await.grid.max_position_usdc = Some(dec!(400));
    engine.maintain_grid_window().await;
    let active = orders(&engine).await;
    assert_eq!(active.len(), 2);
    assert!(buy_exposure_usdc(Decimal::ZERO, dec!(118.84), &active) <= dec!(400));
    let existing = &active[0];
    assert!(
        !engine
            .place_grid_order(
                OrderSide::Buy,
                existing.price,
                "gb_b_duplicate".into(),
                -1,
                Some("parent".into())
            )
            .await
    );
    assert!(
        !engine
            .place_grid_order(
                OrderSide::Buy,
                dec!(118.5),
                "gb_b_over_limit".into(),
                -1,
                Some("parent".into())
            )
            .await
    );
    assert_eq!(orders(&engine).await.len(), 2);
}

#[tokio::test]
async fn invalid_spacing_is_rejected_before_canceling_or_saving_configuration() {
    let mut engine = paper_engine().await;
    engine.maintain_grid_window().await;
    let original = orders(&engine).await;
    let mut config = engine.state.config.read().await.clone();
    config.grid.grid_interval = dec!(0.015);
    assert!(engine.apply_config(config).await.is_err());
    assert_eq!(
        engine.state.config.read().await.grid.grid_interval,
        dec!(0.1)
    );
    let active = orders(&engine).await;
    assert_eq!(active.len(), original.len());
    assert!(original.iter().all(|o| active
        .iter()
        .any(|a| a.client_order_id == o.client_order_id)));
}

#[tokio::test]
async fn moving_window_keeps_fixed_levels_through_multiple_fills() {
    let mut engine = paper_engine().await;
    engine.maintain_grid_window().await;
    for price in [
        dec!(118.74),
        dec!(118.64),
        dec!(118.94),
        dec!(119.14),
        dec!(119.04),
        dec!(118.94),
    ] {
        engine
            .handle_ticker_update(TickerInfo {
                symbol: "SOLUSDC".into(),
                last_price: price,
                ..Default::default()
            })
            .await;
        engine.maintain_grid_window().await;
        let active = orders(&engine).await;
        for side in [OrderSide::Buy, OrderSide::Sell] {
            let mut prices: Vec<_> = active
                .iter()
                .filter(|o| o.side == side)
                .map(|o| o.price)
                .collect();
            prices.sort();
            assert!(prices.iter().all(|p| *p % dec!(0.1) == Decimal::ZERO));
            assert!(
                prices.windows(2).all(|p| p[1] - p[0] >= dec!(0.1)),
                "duplicate levels at market {price}: {prices:?}"
            );
        }
        assert!(reserved_sell_quantity(&active) <= engine.state.position.read().await.size);
    }
    assert!(engine.state.stats.read().await.completed_cycles > 0);
    assert!(orders(&engine).await.iter().any(|o| o.price == dec!(118.9)));
}

#[tokio::test]
async fn paired_exit_takes_priority_over_existing_ordinary_sell_at_same_level() {
    let mut engine = paper_engine().await;
    engine.state.position.write().await.size = dec!(1.68);
    let ordinary = order("s_background", OrderSide::Sell, dec!(118.9), dec!(1.68));
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(ordinary.client_order_id.clone(), ordinary.clone());
    let mut buy = order("b_fill", OrderSide::Buy, dec!(118.8), dec!(1.68));
    engine.on_order_filled(&mut buy).await;
    engine.maintain_grid_window().await;
    let active = orders(&engine).await;
    assert_eq!(active.len(), 1);
    assert!(active[0].is_take_profit);
    assert_eq!(active[0].quantity, buy.quantity);
    assert_ne!(active[0].client_order_id, ordinary.client_order_id);
}

#[tokio::test]
async fn migration_and_window_pruning_require_terminal_cancellation_and_recover_racing_fill() {
    use axum::{routing::get, Json, Router};
    for (price, terminal) in [
        (dec!(118.81), false),
        (dec!(118.81), true),
        (dec!(118.4), false),
        (dec!(118.4), true),
    ] {
        let mut engine = paper_engine().await;
        engine.state.config.write().await.exchange.dry_run = false;
        let old = order("b_migration", OrderSide::Buy, price, dec!(1.68));
        let response = serde_json::json!({
            "orderId": 42, "clientOrderId": old.client_order_id,
            "symbol": "SOLUSDC", "status": if terminal { "CANCELED" } else { "PARTIALLY_FILLED" },
            "price": price.to_string(), "avgPrice": price.to_string(), "origQty": "1.68", "executedQty": "0.46",
            "side": "BUY", "type": "LIMIT", "timeInForce": "GTX"
        });
        let app = Router::new().route(
            "/fapi/v1/order",
            get(move || {
                let response = response.clone();
                async move { Json(response) }
            })
            .delete(|| async { Json(serde_json::json!({})) }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        engine.client = Arc::new(BinanceFuturesClient::with_base_url(
            &engine.state.config.read().await.exchange,
            url,
        ));
        engine.last_orders_sync = Some(Instant::now());
        engine.last_account_sync = Some(Instant::now());
        engine.state.db.save_managed_order(&old).unwrap();
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(old.client_order_id.clone(), old);
        if price == dec!(118.4) {
            // Exercise ordinary window cleanup with a pending cancellation.
            assert!(
                engine
                    .prune_grid_window(&[dec!(118.8), dec!(118.7), dec!(118.6)], &[])
                    .await
            );
        } else {
            engine.maintain_grid_window().await;
        }
        let intents = engine
            .state
            .db
            .load_pair_intents("SOLUSDC", TradingMode::Live)
            .unwrap();
        if terminal {
            assert!(
                orders(&engine).await.is_empty(),
                "no replenishment before paired intent is reconciled"
            );
            assert_eq!(intents.len(), 1);
            assert_eq!(intents[0].quantity, dec!(0.46));
            assert_eq!(intents[0].price, price + dec!(0.1));
            assert_eq!(engine.state.db.get_recent_trades(10).unwrap().len(), 1);
        } else {
            assert_eq!(orders(&engine).await.len(), 1);
            assert!(intents.is_empty());
            assert!(engine.state.db.get_recent_trades(10).unwrap().is_empty());
            assert!(engine.last_orders_sync.is_none());
        }
        server.abort();
    }
}

#[tokio::test]
async fn interrupted_window_cleanup_retries_remaining_orders_after_partial_success() {
    use axum::{extract::Query, routing::get, Json, Router};
    use std::sync::atomic::{AtomicBool, Ordering};

    let mut engine = paper_engine().await;
    engine.state.config.write().await.exchange.dry_run = false;
    let allow_second_cancel = Arc::new(AtomicBool::new(false));
    let gate = allow_second_cancel.clone();
    let app = Router::new().route(
        "/fapi/v1/order",
        get(move |Query(query): Query<HashMap<String, String>>| {
            let gate = gate.clone();
            async move {
                let id = &query["origClientOrderId"];
                Json(serde_json::json!({
                    "orderId": 42, "clientOrderId": id, "symbol": "SOLUSDC",
                    "status": if id == "gb_a" || gate.load(Ordering::SeqCst) { "CANCELED" } else { "NEW" },
                    "price": "118.4", "origQty": "1.68", "executedQty": "0",
                    "side": "BUY", "type": "LIMIT", "timeInForce": "GTX"
                }))
            }
        }).delete(|| async { Json(serde_json::json!({})) }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    engine.client = Arc::new(BinanceFuturesClient::with_base_url(
        &engine.state.config.read().await.exchange,
        url,
    ));
    for (id, price) in [("a", dec!(118.4)), ("b", dec!(118.3))] {
        let o = order(id, OrderSide::Buy, price, dec!(1.68));
        engine.state.db.save_managed_order(&o).unwrap();
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(o.client_order_id.clone(), o);
    }
    assert!(engine.prune_grid_window(&[dec!(118.8)], &[]).await);
    assert_eq!(orders(&engine).await.len(), 1);
    allow_second_cancel.store(true, Ordering::SeqCst);
    assert!(engine.prune_grid_window(&[dec!(118.8)], &[]).await);
    assert!(orders(&engine).await.is_empty());
    assert!(engine
        .state
        .db
        .load_managed_orders("SOLUSDC")
        .unwrap()
        .is_empty());
    server.abort();
}

#[tokio::test]
async fn crossed_unsubmitted_remainder_does_not_starve_sells_with_large_position() {
    let mut engine = paper_engine().await;
    engine.state.ticker.write().await.last_price = dec!(119.35);
    engine.state.position.write().await.size = dec!(107.33);
    for price in [
        dec!(119.0),
        dec!(118.9),
        dec!(118.8),
        dec!(118.7),
        dec!(118.6),
        dec!(118.5),
    ] {
        let buy = order(&format!("b_{price}"), OrderSide::Buy, price, dec!(1.68));
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(buy.client_order_id.clone(), buy);
    }
    let mut target = order(
        "s_stale_remainder",
        OrderSide::Sell,
        dec!(119.1),
        dec!(0.24),
    );
    target.purpose = OrderPurpose::Remainder;
    engine
        .state
        .db
        .save_remainder_plan(&RemainderPlan {
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Paper,
            sources: vec![],
            target,
            phase: RemainderPhase::Submitting,
        })
        .unwrap();

    engine.maintain_grid_window().await;
    assert!(
        engine.load_remainder_plan().await.unwrap().is_none(),
        "crossed unsubmitted target must not block the whole grid forever"
    );
    // Confirm stale buys before replenishing either side of the new window.
    engine.maintain_grid_window().await;
    let active = orders(&engine).await;
    let mut sells: Vec<_> = active
        .iter()
        .filter(|o| o.side == OrderSide::Sell)
        .collect();
    sells.sort_by_key(|o| o.price);
    assert_eq!(
        sells.iter().map(|o| o.price).collect::<Vec<_>>(),
        vec![dec!(119.4), dec!(119.5), dec!(119.6)]
    );
    assert_eq!(reserved_sell_quantity(&active), dec!(5.01));
    assert!(sells.iter().all(|o| o.quantity == dec!(1.67)));
}

#[tokio::test]
async fn sold_levels_and_restart_do_not_push_inventory_sells_away_from_market() {
    let engine = paper_engine().await;
    engine.state.config.write().await.grid.order_amount_usdc = dec!(300);
    engine.state.config.write().await.grid.max_position_usdc = None;
    engine.state.position.write().await.size = dec!(100);
    engine.state.ticker.write().await.last_price = dec!(119.19);
    for (id, price) in [
        dec!(119.2),
        dec!(119.3),
        dec!(119.4),
        dec!(119.5),
        dec!(119.6),
        dec!(119.7),
        dec!(119.8),
        dec!(120.1),
    ]
    .into_iter()
    .enumerate()
    {
        engine
            .state
            .db
            .insert_trade(&journal_trade(id, OrderSide::Sell, price, dec!(2.51)))
            .unwrap();
    }
    for price in [dec!(119.9), dec!(120), dec!(120.2)] {
        let sell = order(&format!("s_old_{price}"), OrderSide::Sell, price, dec!(2.5));
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(sell.client_order_id.clone(), sell);
    }
    let (_, rx) = mpsc::channel(1);
    let (_, ticker_rx) = broadcast::channel(1);
    let mut restarted =
        GridTradingEngine::new(engine.state.clone(), engine.client.clone(), rx, ticker_rx);
    // Cancel and confirm the old window before reusing its inventory.
    restarted.maintain_grid_window().await;
    assert!(orders(&restarted)
        .await
        .iter()
        .all(|o| o.side != OrderSide::Sell));
    restarted.maintain_grid_window().await;
    let mut sells: Vec<_> = orders(&restarted)
        .await
        .into_iter()
        .filter(|o| o.side == OrderSide::Sell)
        .map(|o| o.price)
        .collect();
    sells.sort();
    assert_eq!(sells, vec![dec!(119.2), dec!(119.3), dec!(119.4)]);
}

#[tokio::test]
async fn sell_window_tracks_rising_falling_and_exact_market_levels_with_limited_inventory() {
    let mut engine = paper_engine().await;
    engine.state.config.write().await.grid.order_amount_usdc = dec!(300);
    engine.state.config.write().await.grid.max_position_usdc = None;
    engine.state.position.write().await.size = dec!(7.6);
    for (market, expected) in [
        (dec!(119.19), vec![dec!(119.2), dec!(119.3), dec!(119.4)]),
        (dec!(119.25), vec![dec!(119.3), dec!(119.4), dec!(119.5)]),
        (dec!(118.95), vec![dec!(119), dec!(119.1), dec!(119.2)]),
        (dec!(120), vec![dec!(120.1), dec!(120.2), dec!(120.3)]),
    ] {
        engine.state.ticker.write().await.last_price = market;
        let previous = orders(&engine).await;
        for _ in 0..4 {
            engine.maintain_grid_window().await;
        }
        let active = orders(&engine).await;
        let mut sells: Vec<_> = active
            .iter()
            .filter(|o| o.side == OrderSide::Sell)
            .map(|o| o.price)
            .collect();
        sells.sort();
        assert_eq!(sells, expected);
        assert!(reserved_sell_quantity(&active) <= dec!(7.6));
        // Orders still inside the band retain their queue position.
        for old in previous
            .iter()
            .filter(|o| o.side == OrderSide::Sell && expected.contains(&o.price))
        {
            assert!(active
                .iter()
                .any(|o| o.client_order_id == old.client_order_id));
        }
    }
}

#[tokio::test]
async fn stale_orders_require_confirmed_cancel_and_record_racing_fills() {
    use axum::{routing::get, Json, Router};
    for side in [OrderSide::Buy, OrderSide::Sell] {
        for terminal in [false, true] {
            let mut engine = paper_engine().await;
            engine.state.config.write().await.exchange.dry_run = false;
            engine.state.position.write().await.size = dec!(100);
            let price = if side == OrderSide::Buy {
                dec!(118.4)
            } else {
                dec!(120)
            };
            let old = order("stale", side, price, dec!(1.66));
            let response = serde_json::json!({
                "orderId": 42, "clientOrderId": old.client_order_id,
                "symbol": "SOLUSDC", "status": if terminal { "CANCELED" } else { "PARTIALLY_FILLED" },
                "price": price.to_string(), "avgPrice": price.to_string(), "origQty": "1.66", "executedQty": "0.46",
                "side": side.as_str(), "type": "LIMIT", "timeInForce": "GTX"
            });
            let app = Router::new().route(
                "/fapi/v1/order",
                get(move || {
                    let response = response.clone();
                    async move { Json(response) }
                })
                .delete(|| async { Json(serde_json::json!({})) }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            engine.client = Arc::new(BinanceFuturesClient::with_base_url(
                &engine.state.config.read().await.exchange,
                url,
            ));
            engine.last_orders_sync = Some(Instant::now());
            engine.last_account_sync = Some(Instant::now());
            engine.state.db.save_managed_order(&old).unwrap();
            engine
                .state
                .active_orders
                .write()
                .await
                .insert(old.client_order_id.clone(), old.clone());
            engine.maintain_grid_window().await;
            let active = orders(&engine).await;
            let fills = engine.state.db.get_recent_trades(10).unwrap();
            if terminal {
                assert!(
                    active.is_empty(),
                    "replenish only after the next reconciliation cycle"
                );
                assert_eq!(fills.len(), 1);
                assert_eq!(fills[0].side, side);
                let intents = engine
                    .state
                    .db
                    .load_pair_intents("SOLUSDC", TradingMode::Live)
                    .unwrap();
                if side == OrderSide::Buy {
                    assert_eq!(intents.len(), 1);
                    assert_eq!(intents[0].quantity, dec!(0.46));
                    assert_eq!(intents[0].price, dec!(118.5));
                } else {
                    assert!(intents.is_empty());
                }
                assert_eq!(fills[0].quantity, dec!(0.46));
            } else {
                assert_eq!(active.len(), 1);
                assert_eq!(active[0].client_order_id, old.client_order_id);
                assert!(fills.is_empty());
            }
            server.abort();
        }
    }
}

#[tokio::test]
async fn sold_level_can_be_reused_without_lower_buy_and_remainder_can_recover() {
    let mut engine = paper_engine().await;
    engine.state.position.write().await.size = dec!(100);
    engine.state.ticker.write().await.last_price = dec!(119.51);
    let mut sell = order("s_initial", OrderSide::Sell, dec!(119.5), dec!(1.67));
    assert!(engine.on_order_filled(&mut sell).await);
    engine.state.ticker.write().await.last_price = dec!(119.45);
    assert!(
        engine
            .place_grid_order(OrderSide::Sell, dec!(119.5), "gb_s_again".into(), 1, None)
            .await
    );
    // A recovered remainder remains subject to position and exchange checks,
    // rather than being retired merely because the same price sold previously.
    let mut target = order("s_remainder", OrderSide::Sell, dec!(119.6), dec!(0.1));
    target.purpose = OrderPurpose::Remainder;
    engine
        .state
        .db
        .insert_trade(&journal_trade(
            100,
            OrderSide::Sell,
            dec!(119.6),
            dec!(1.67),
        ))
        .unwrap();
    engine
        .state
        .db
        .save_remainder_plan(&RemainderPlan {
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Paper,
            sources: vec![],
            target: target.clone(),
            phase: RemainderPhase::Submitting,
        })
        .unwrap();
    engine.maintain_grid_window().await;
    assert!(engine.load_remainder_plan().await.unwrap().is_none());
    assert!(orders(&engine)
        .await
        .iter()
        .any(|o| o.client_order_id == target.client_order_id));
}

fn journal_trade(id: usize, side: OrderSide, price: Decimal, quantity: Decimal) -> TradeRecord {
    TradeRecord {
        trade_id: format!("journal-{id}"),
        client_order_id: format!("gb_journal_{id}"),
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Paper,
        side,
        price,
        quantity,
        amount_usdc: price * quantity,
        realized_pnl: Decimal::ZERO,
        commission: Decimal::ZERO,
        pnl_verified: true,
        is_maker: true,
        timestamp: Utc::now(),
        note: String::new(),
    }
}

#[tokio::test]
async fn historical_buys_and_existing_exits_do_not_push_buy_window_away_after_restart() {
    let engine = paper_engine().await;
    engine.state.config.write().await.grid.order_amount_usdc = dec!(300);
    engine.state.config.write().await.grid.max_position_usdc = None;
    engine.state.ticker.write().await.last_price = dec!(119.4);
    engine.state.position.write().await.size = dec!(20);
    for id in 0..1002 {
        let price = dec!(119.1) + Decimal::from(id % 3) * dec!(0.1);
        engine
            .state
            .db
            .insert_trade(&journal_trade(id, OrderSide::Buy, price, dec!(2.51)))
            .unwrap();
    }
    let parent = order("b_parent_near", OrderSide::Buy, dec!(119.3), dec!(2.51));
    let exit = new_pair_intent(&parent, OrderSide::Sell, dec!(119.4), parent.quantity);
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(exit.client_order_id.clone(), exit.clone());
    for price in [dec!(118.9), dec!(118.8), dec!(118.7)] {
        let buy = order(&format!("b_old_{price}"), OrderSide::Buy, price, dec!(2.52));
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(buy.client_order_id.clone(), buy);
    }
    let (_, rx) = mpsc::channel(1);
    let (_, ticker_rx) = broadcast::channel(1);
    let mut restarted =
        GridTradingEngine::new(engine.state.clone(), engine.client.clone(), rx, ticker_rx);
    restarted.maintain_grid_window().await;
    assert!(orders(&restarted)
        .await
        .iter()
        .all(|o| o.side != OrderSide::Buy));
    restarted.maintain_grid_window().await;
    let active = orders(&restarted).await;
    let mut buys: Vec<_> = active
        .iter()
        .filter(|o| o.side == OrderSide::Buy)
        .map(|o| o.price)
        .collect();
    buys.sort();
    assert_eq!(buys, vec![dec!(119.1), dec!(119.2), dec!(119.3)]);
    assert!(active
        .iter()
        .any(|o| o.client_order_id == exit.client_order_id));
}

#[tokio::test]
async fn buy_window_tracks_market_and_keeps_partial_orders_without_extending_band() {
    let mut engine = paper_engine().await;
    engine.state.config.write().await.grid.max_position_usdc = None;
    let mut far = order("b_partial_far", OrderSide::Buy, dec!(118), dec!(0.2));
    far.status = OrderStatus::PartiallyFilled;
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(far.client_order_id.clone(), far.clone());
    for (market, expected) in [
        (dec!(119.4), vec![dec!(119.1), dec!(119.2), dec!(119.3)]),
        (dec!(119.45), vec![dec!(119.2), dec!(119.3), dec!(119.4)]),
        (dec!(118.95), vec![dec!(118.7), dec!(118.8), dec!(118.9)]),
    ] {
        engine.state.ticker.write().await.last_price = market;
        let previous = orders(&engine).await;
        engine.maintain_grid_window().await;
        engine.maintain_grid_window().await;
        let active = orders(&engine).await;
        let mut buys: Vec<_> = active
            .iter()
            .filter(|o| o.side == OrderSide::Buy && o.status == OrderStatus::New)
            .map(|o| o.price)
            .collect();
        buys.sort();
        assert_eq!(buys, expected);
        assert!(active
            .iter()
            .any(|o| o.client_order_id == far.client_order_id));
        for old in previous
            .iter()
            .filter(|o| o.side == OrderSide::Buy && expected.contains(&o.price))
        {
            assert!(active
                .iter()
                .any(|o| o.client_order_id == old.client_order_id));
        }
    }
    let mut near = order("b_partial_near", OrderSide::Buy, dec!(119.3), dec!(0.2));
    near.status = OrderStatus::PartiallyFilled;
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(near.client_order_id.clone(), near.clone());
    engine.state.ticker.write().await.last_price = dec!(119.4);
    engine.maintain_grid_window().await;
    engine.maintain_grid_window().await;
    let active = orders(&engine).await;
    let mut buys: Vec<_> = active
        .iter()
        .filter(|o| o.side == OrderSide::Buy && o.status == OrderStatus::New)
        .map(|o| o.price)
        .collect();
    buys.sort();
    assert_eq!(buys, vec![dec!(119.1), dec!(119.2)]);
    assert!(active
        .iter()
        .any(|o| o.client_order_id == near.client_order_id));
}

#[tokio::test]
async fn repeated_buy_at_same_price_keeps_independent_equal_quantity_exits() {
    let mut engine = paper_engine().await;
    engine.state.config.write().await.grid.max_position_usdc = None;
    for id in ["b_first", "b_second"] {
        let mut buy = order(id, OrderSide::Buy, dec!(118.8), dec!(1.68));
        assert!(engine.on_order_filled(&mut buy).await);
    }
    let active = orders(&engine).await;
    assert_eq!(active.len(), 2);
    assert!(active
        .iter()
        .all(|o| o.is_take_profit && o.price == dec!(118.9) && o.quantity == dec!(1.68)));
    engine.maintain_grid_window().await;
    assert!(orders(&engine)
        .await
        .iter()
        .any(|o| o.side == OrderSide::Buy && o.price == dec!(118.8)));
    // A recovered live exit for a different purchase can share the sell price.
    engine.state.position.write().await.size = dec!(5.04);
    engine.last_orders_sync = Some(Instant::now());
    engine.last_account_sync = Some(Instant::now());
    let parent = order("b_third", OrderSide::Buy, dec!(118.8), dec!(1.68));
    let exit = new_pair_intent(&parent, OrderSide::Sell, dec!(118.9), parent.quantity);
    assert!(matches!(
        engine.pair_submission_decision(&exit).await,
        PairPlacementDecision::Ready
    ));
    let duplicate = orders(&engine)
        .await
        .into_iter()
        .find(|o| o.is_take_profit)
        .unwrap();
    assert!(
        !engine
            .place_grid_order_with_quantity(
                OrderSide::Sell,
                duplicate.price,
                "gb_s_duplicate_parent".into(),
                1,
                duplicate.paired_client_order_id,
                Some(duplicate.quantity)
            )
            .await
    );
}

#[tokio::test]
async fn paired_rebuys_outside_nearest_band_are_skipped_without_blocking_window() {
    let mut engine = paper_engine().await;
    engine.state.config.write().await.grid.max_position_usdc = None;
    engine.state.ticker.write().await.last_price = dec!(119.4);
    engine.last_orders_sync = Some(Instant::now());
    engine.last_account_sync = Some(Instant::now());
    let parent = order("s_far", OrderSide::Sell, dec!(119), dec!(1.68));
    let rebuy = new_pair_intent(&parent, OrderSide::Buy, dec!(118.9), dec!(1.68));
    assert!(matches!(
        engine.pair_submission_decision(&rebuy).await,
        PairPlacementDecision::Skip(_)
    ));
    assert!(
        !engine
            .place_grid_order(
                OrderSide::Buy,
                rebuy.price,
                rebuy.client_order_id,
                -1,
                rebuy.paired_client_order_id
            )
            .await
    );
    let near = new_pair_intent(&parent, OrderSide::Buy, dec!(119.3), dec!(1.67));
    assert!(matches!(
        engine.pair_submission_decision(&near).await,
        PairPlacementDecision::Ready
    ));
}

#[tokio::test]
async fn nearest_buy_window_reuses_canceled_funds_and_still_enforces_position_limit() {
    let mut engine = paper_engine().await;
    {
        let mut config = engine.state.config.write().await;
        config.grid.order_amount_usdc = dec!(300);
        config.grid.max_position_usdc = Some(dec!(900));
    }
    engine.state.ticker.write().await.last_price = dec!(119.4);
    // Existing far-away orders would prevent funding the nearest orders unless
    // their cancellation is confirmed first.
    for price in [dec!(118.9), dec!(118.8), dec!(118.7)] {
        let buy = order(&format!("b_far_{price}"), OrderSide::Buy, price, dec!(2.52));
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(buy.client_order_id.clone(), buy);
    }
    engine.maintain_grid_window().await;
    assert!(orders(&engine).await.is_empty());
    engine.maintain_grid_window().await;
    let active = orders(&engine).await;
    let mut buys: Vec<_> = active
        .iter()
        .filter(|o| o.side == OrderSide::Buy)
        .map(|o| o.price)
        .collect();
    buys.sort();
    assert_eq!(buys, vec![dec!(119.1), dec!(119.2), dec!(119.3)]);
    assert!(buy_exposure_usdc(Decimal::ZERO, dec!(119.4), &active) <= dec!(900));
    engine.state.active_orders.write().await.clear();
    engine.state.position.write().await.size = dec!(6);
    engine.maintain_grid_window().await;
    assert!(orders(&engine)
        .await
        .iter()
        .all(|o| o.side != OrderSide::Buy));
    engine.last_orders_sync = Some(Instant::now());
    engine.last_account_sync = Some(Instant::now());
    let parent = order("s_for_rebuy", OrderSide::Sell, dec!(119.4), dec!(2.51));
    let rebuy = new_pair_intent(&parent, OrderSide::Buy, dec!(119.3), dec!(2.51));
    assert!(matches!(
        engine.pair_submission_decision(&rebuy).await,
        PairPlacementDecision::Skip(_)
    ));
}

#[tokio::test]
async fn ordinary_sell_placement_cannot_duplicate_an_active_sell() {
    let mut engine = paper_engine().await;
    engine.state.position.write().await.size = dec!(100);
    assert!(
        engine
            .place_grid_order(OrderSide::Sell, dec!(119), "gb_s_first".into(), 1, None)
            .await
    );
    assert!(
        !engine
            .place_grid_order(
                OrderSide::Sell,
                dec!(119.0000),
                "gb_s_second".into(),
                1,
                None
            )
            .await
    );
    assert_eq!(orders(&engine).await.len(), 1);
}
