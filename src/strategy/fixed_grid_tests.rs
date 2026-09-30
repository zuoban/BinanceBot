use super::*;
use crate::{config::AppConfig, db::Database};
use rust_decimal_macros::dec;

async fn paper_engine() -> GridTradingEngine {
    paper_engine_with_db(Arc::new(Database::open(":memory:").unwrap())).await
}

async fn paper_engine_with_db(db: Arc<Database>) -> GridTradingEngine {
    let mut config = AppConfig::default();
    config.exchange.dry_run = true;
    config.grid.order_amount_usdc = dec!(200);
    config.grid.buy_window = 3;
    config.grid.sell_window = 3;
    config.grid.max_position_usdc = Some(dec!(2000));
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
async fn sell_window_skips_last_sell_and_prunes_wrong_direction_and_stale_orders() {
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
    engine.maintain_grid_window().await;
    let active = orders(&engine).await;
    for (side, expected) in [
        (OrderSide::Buy, vec![dec!(119), dec!(119.1), dec!(119.2)]),
        (OrderSide::Sell, vec![dec!(119.6), dec!(119.7), dec!(119.8)]),
    ] {
        let mut prices: Vec<_> = active
            .iter()
            .filter(|o| o.side == side)
            .map(|o| o.price)
            .collect();
        prices.sort();
        assert_eq!(prices, expected);
    }
    assert!(!active
        .iter()
        .any(|o| o.client_order_id == exit.client_order_id));
    for buy in retained.iter().filter(|o| {
        o.side == OrderSide::Buy && [dec!(119), dec!(119.1), dec!(119.2)].contains(&o.price)
    }) {
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
        (OrderSide::Sell, vec![dec!(118.9), dec!(119.1), dec!(119.3)]),
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
    assert_eq!(active.len(), 4);
    engine.maintain_grid_window().await;
    assert_eq!(orders(&engine).await.len(), 6);
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
async fn directions_are_independent_and_respect_price_bounds() {
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
async fn resizing_window_moves_all_idle_orders_and_keeps_partial_fills() {
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
    assert_eq!(active.len(), 3);
    assert!(active
        .iter()
        .any(|o| o.side == OrderSide::Buy && o.price == dec!(118.8)));
    assert!(active
        .iter()
        .any(|o| o.side == OrderSide::Sell && o.price == dec!(118.9)));
    assert!(active
        .iter()
        .any(|o| o.client_order_id == partial.client_order_id));
    assert!(!active
        .iter()
        .any(|o| o.client_order_id == exit.client_order_id));
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
    assert_eq!(sells, vec![dec!(118.9), dec!(119), dec!(119.1)]);
    assert!(!active
        .iter()
        .any(|o| o.client_order_id == exit.client_order_id));
}

#[tokio::test]
async fn far_counter_orders_follow_the_same_window_as_other_orders() {
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
    engine.maintain_grid_window().await;
    let active = orders(&engine).await;
    let sells: Vec<_> = active
        .iter()
        .filter(|o| o.side == OrderSide::Sell)
        .collect();
    assert_eq!(sells.len(), 1);
    assert!(sells.iter().all(|o| !o.is_take_profit));
    assert!(sells
        .iter()
        .any(|o| !o.is_take_profit && o.price == dec!(118.9)));
    assert!(reserved_sell_quantity(&active) <= engine.state.position.read().await.size);
}

#[tokio::test]
async fn fills_replenish_only_the_window_without_creating_pairs() {
    let mut engine = paper_engine().await;
    engine.state.config.write().await.grid.buy_window = 0;
    engine.state.config.write().await.grid.sell_window = 1;
    let mut buy = order("b_cycle", OrderSide::Buy, dec!(118.8), dec!(0.46));
    assert!(engine.on_order_filled(&mut buy).await);
    assert!(orders(&engine).await.is_empty());
    engine.maintain_grid_window().await;
    let mut sell = orders(&engine).await.pop().unwrap();
    assert_eq!(sell.price, dec!(118.9));
    assert_eq!(sell.quantity, dec!(0.46));
    assert!(sell.paired_client_order_id.is_none());
    engine.state.ticker.write().await.last_price = dec!(118.95);
    assert!(engine.on_order_filled(&mut sell).await);
    assert_eq!(engine.state.position.read().await.size, Decimal::ZERO);
    assert!(orders(&engine).await.is_empty());
    assert_eq!(engine.state.stats.read().await.completed_cycles, 0);
    assert!(engine
        .state
        .db
        .load_pair_intents("SOLUSDC", TradingMode::Paper)
        .unwrap()
        .is_empty());
    let trades = engine.state.db.get_recent_trades(10).unwrap();
    assert_eq!(trades.len(), 2);
    assert_eq!(
        trades
            .iter()
            .find(|t| t.side == OrderSide::Sell)
            .unwrap()
            .realized_pnl,
        dec!(0.046)
    );
}

#[tokio::test]
async fn consecutive_fills_step_one_level_and_opposite_fills_unlock_previous_levels() {
    let mut engine = paper_engine().await;
    engine.state.config.write().await.grid.max_position_usdc = None;
    engine.state.position.write().await.size = dec!(10);
    engine.state.ticker.write().await.last_price = dec!(119.25);
    let mut buy = order("first_buy", OrderSide::Buy, dec!(119.2), dec!(0.1));
    assert!(engine.on_order_filled(&mut buy).await);
    let config = engine.state.config.read().await.clone();
    let rules = engine.state.rules.read().await.clone();
    assert_eq!(
        engine
            .window_prices(&config, &rules, dec!(119.25))
            .await
            .unwrap(),
        (
            vec![dec!(119.1), dec!(119), dec!(118.9)],
            vec![dec!(119.3), dec!(119.4), dec!(119.5)]
        )
    );
    assert!(
        !engine
            .place_grid_order(
                OrderSide::Buy,
                dec!(119.2),
                "gb_buy_repeat".into(),
                -1,
                None
            )
            .await
    );
    // A rising market alone cannot allow buying above the last buy either.
    engine.state.ticker.write().await.last_price = dec!(119.45);
    assert!(
        !engine
            .place_grid_order(
                OrderSide::Buy,
                dec!(119.3),
                "gb_buy_higher".into(),
                -1,
                None
            )
            .await
    );
    engine.state.ticker.write().await.last_price = dec!(119.25);
    assert!(
        engine
            .place_grid_order(OrderSide::Buy, dec!(119.1), "gb_buy_lower".into(), -1, None)
            .await
    );
    let mut sell = order("opposite_sell", OrderSide::Sell, dec!(119.3), dec!(0.1));
    assert!(engine.on_order_filled(&mut sell).await);
    assert_eq!(
        engine
            .window_prices(&config, &rules, dec!(119.25))
            .await
            .unwrap(),
        (
            vec![dec!(119.2), dec!(119.1), dec!(119)],
            vec![dec!(119.4), dec!(119.5), dec!(119.6)]
        )
    );
    assert!(
        engine
            .place_grid_order(
                OrderSide::Buy,
                dec!(119.2),
                "gb_buy_reopened".into(),
                -1,
                None
            )
            .await
    );
    assert!(
        !engine
            .place_grid_order(
                OrderSide::Sell,
                dec!(119.3),
                "gb_sell_repeat".into(),
                1,
                None
            )
            .await
    );
    engine.state.ticker.write().await.last_price = dec!(119.15);
    assert!(
        !engine
            .place_grid_order(
                OrderSide::Sell,
                dec!(119.2),
                "gb_sell_lower".into(),
                1,
                None
            )
            .await
    );
    engine.state.ticker.write().await.last_price = dec!(119.25);
    assert!(
        engine
            .place_grid_order(
                OrderSide::Sell,
                dec!(119.4),
                "gb_sell_higher".into(),
                1,
                None
            )
            .await
    );
    let mut rebuy = order("rebuy_fill", OrderSide::Buy, dec!(119.2), dec!(0.1));
    assert!(engine.on_order_filled(&mut rebuy).await);
    assert!(
        engine
            .place_grid_order(
                OrderSide::Sell,
                dec!(119.3),
                "gb_sell_reopened".into(),
                1,
                None
            )
            .await
    );
}

#[tokio::test]
async fn migration_aligns_all_idle_orders_and_preserves_partial_executions() {
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
    assert_eq!(active.len(), 1);
    assert!(!active
        .iter()
        .any(|o| o.client_order_id == old.client_order_id));
    assert!(active
        .iter()
        .any(|o| o.client_order_id == partial.client_order_id));
    assert!(!active
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
async fn duplicate_buy_and_position_limit_apply_to_window_replenishment() {
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
    assert!(engine.state.stats.read().await.total_trades > 0);
    assert_eq!(engine.state.stats.read().await.completed_cycles, 0);
    let mut trades = engine.state.db.get_recent_trades(100).unwrap();
    trades.reverse();
    assert_execution_steps(&trades, dec!(0.1));
}

#[tokio::test]
async fn counter_order_reuses_an_occupied_level_without_replacing_it() {
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
    let at_level: Vec<_> = active
        .iter()
        .filter(|o| o.side == OrderSide::Sell && o.price == dec!(118.9))
        .collect();
    assert_eq!(at_level.len(), 1);
    assert_eq!(at_level[0].quantity, buy.quantity);
    assert_eq!(at_level[0].client_order_id, ordinary.client_order_id);
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
            assert!(intents.is_empty());
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
    assert_eq!(
        sells.iter().map(|o| o.quantity).collect::<Vec<_>>(),
        vec![dec!(1.67), dec!(1.67), dec!(1.67)]
    );
}

#[tokio::test]
async fn consecutive_sells_keep_advancing_after_restart_until_a_buy_fills() {
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
    restarted.maintain_grid_window().await;
    restarted.maintain_grid_window().await;
    let mut sells: Vec<_> = orders(&restarted)
        .await
        .into_iter()
        .filter(|o| o.side == OrderSide::Sell)
        .map(|o| o.price)
        .collect();
    sells.sort();
    assert_eq!(sells, vec![dec!(120.2), dec!(120.3), dec!(120.4)]);
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
                dec!(118.2)
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
                assert!(intents.is_empty());
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
async fn unsubmitted_remainder_at_last_sell_price_is_retired() {
    let mut engine = paper_engine().await;
    engine.state.position.write().await.size = dec!(100);
    engine.state.ticker.write().await.last_price = dec!(119.45);
    let mut sell = order("s_initial", OrderSide::Sell, dec!(119.5), dec!(1.67));
    assert!(engine.on_order_filled(&mut sell).await);
    assert!(
        !engine
            .place_grid_order(OrderSide::Sell, dec!(119.5), "gb_s_again".into(), 1, None)
            .await
    );
    let mut target = order("s_remainder", OrderSide::Sell, dec!(119.5), dec!(0.1));
    target.purpose = OrderPurpose::Remainder;
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
    let active = orders(&engine).await;
    assert!(active
        .iter()
        .all(|o| o.client_order_id != target.client_order_id));
    assert!(active
        .iter()
        .filter(|o| o.side == OrderSide::Sell)
        .all(|o| o.price != dec!(119.5)));
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
async fn restart_restores_the_buy_frontier_without_locking_all_historical_levels() {
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
    restarted.maintain_grid_window().await;
    restarted.maintain_grid_window().await;
    let active = orders(&restarted).await;
    let mut buys: Vec<_> = active
        .iter()
        .filter(|o| o.side == OrderSide::Buy)
        .map(|o| o.price)
        .collect();
    buys.sort();
    assert_eq!(buys, vec![dec!(119), dec!(119.1), dec!(119.2)]);
    assert!(!active
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
    let mut near = order("b_partial_near", OrderSide::Buy, dec!(119.2), dec!(0.2));
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
    assert_eq!(buys, vec![dec!(119.1), dec!(119.3)]);
    assert!(active
        .iter()
        .any(|o| o.client_order_id == near.client_order_id));
}

#[tokio::test]
async fn last_buy_and_active_sell_are_both_protected_from_duplicate_placement() {
    let mut engine = paper_engine().await;
    engine.state.config.write().await.grid.max_position_usdc = None;
    let mut buy = order("b_first", OrderSide::Buy, dec!(118.8), dec!(1.68));
    assert!(engine.on_order_filled(&mut buy).await);
    engine.maintain_grid_window().await;
    let active = orders(&engine).await;
    assert_eq!(
        active.iter().filter(|o| o.side == OrderSide::Sell).count(),
        1
    );
    assert!(
        !engine
            .place_grid_order(OrderSide::Buy, buy.price, "gb_b_duplicate".into(), -1, None)
            .await
    );
    assert!(
        !engine
            .place_grid_order_with_quantity(
                OrderSide::Sell,
                dec!(118.9),
                "gb_s_duplicate".into(),
                1,
                None,
                Some(buy.quantity)
            )
            .await
    );
}

#[tokio::test]
async fn legacy_pair_replenishment_is_retired_even_inside_the_window() {
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
        PairPlacementDecision::Skip(_)
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
            .place_grid_order(OrderSide::Sell, dec!(119.1), "gb_s_first".into(), 1, None)
            .await
    );
    assert!(
        !engine
            .place_grid_order(
                OrderSide::Sell,
                dec!(119.1000),
                "gb_s_second".into(),
                1,
                None
            )
            .await
    );
    assert_eq!(orders(&engine).await.len(), 1);
}

#[tokio::test]
async fn exact_market_window_skips_current_price_and_reuses_levels_after_opposite_fills() {
    let mut engine = paper_engine().await;
    engine.state.config.write().await.grid.order_amount_usdc = dec!(300);
    engine.state.config.write().await.grid.max_position_usdc = None;
    engine.state.position.write().await.size = dec!(30);
    engine.state.ticker.write().await.last_price = dec!(100);
    engine
        .state
        .db
        .insert_trade(&journal_trade(0, OrderSide::Buy, dec!(99.8), dec!(1)))
        .unwrap();
    engine
        .state
        .db
        .insert_trade(&journal_trade(1, OrderSide::Sell, dec!(100.1), dec!(1)))
        .unwrap();
    engine.maintain_grid_window().await;
    for (side, expected) in [
        (OrderSide::Buy, vec![dec!(99.7), dec!(99.8), dec!(99.9)]),
        (OrderSide::Sell, vec![dec!(100.2), dec!(100.3), dec!(100.4)]),
    ] {
        let mut prices: Vec<_> = orders(&engine)
            .await
            .iter()
            .filter(|o| o.side == side)
            .map(|o| o.price)
            .collect();
        prices.sort();
        assert_eq!(prices, expected);
    }
    for price in [
        dec!(100.05),
        dec!(100.11),
        dec!(100.05),
        dec!(100.11),
        dec!(100),
        dec!(100.11),
        dec!(100),
        dec!(100.11),
    ] {
        engine
            .handle_ticker_update(TickerInfo {
                symbol: "SOLUSDC".into(),
                last_price: price,
                ..Default::default()
            })
            .await;
        engine.maintain_grid_window().await;
    }
    let trades = engine.state.db.get_recent_trades(100).unwrap();
    let mut executions = trades.clone();
    executions.reverse();
    assert!(
        executions
            .iter()
            .filter(|t| t.side == OrderSide::Sell && t.price == dec!(100.1))
            .count()
            >= 2
    );
    assert_execution_steps(&executions, dec!(0.1));
    assert!(orders(&engine)
        .await
        .iter()
        .all(|o| o.paired_client_order_id.is_none()));
}

#[tokio::test]
async fn execution_average_is_recorded_without_creating_a_counter_order() {
    for (side, average) in [
        (OrderSide::Sell, dec!(100.12)),
        (OrderSide::Buy, dec!(99.88)),
    ] {
        let mut engine = paper_engine().await;
        engine.state.position.write().await.size = dec!(10);
        let mut fill = order("execution", side, average, dec!(1));
        assert!(engine.on_order_filled(&mut fill).await);
        assert!(orders(&engine).await.is_empty());
        assert_eq!(
            engine.state.db.get_recent_trades(10).unwrap()[0].price,
            average
        );
        let last = engine
            .state
            .db
            .last_fill_for("SOLUSDC", TradingMode::Paper)
            .unwrap();
        assert_eq!(last, Some((side, average)));
    }
}

#[tokio::test]
async fn legacy_exit_label_does_not_change_pruning_or_duplicate_priority() {
    let mut engine = paper_engine().await;
    engine.state.config.write().await.grid.max_position_usdc = None;
    engine.state.ticker.write().await.last_price = dec!(100);
    engine.state.position.write().await.size = dec!(30);
    let mut first = order("first", OrderSide::Sell, dec!(100.1), dec!(2));
    first.created_at -= chrono::Duration::seconds(60);
    let mut duplicate = order("duplicate", OrderSide::Sell, dec!(100.1), dec!(2));
    duplicate.is_take_profit = true;
    duplicate.purpose = OrderPurpose::TakeProfit;
    let mut far = order("far_legacy_exit", OrderSide::Sell, dec!(101), dec!(2));
    far.is_take_profit = true;
    far.purpose = OrderPurpose::TakeProfit;
    for o in [&first, &duplicate, &far] {
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(o.client_order_id.clone(), o.clone());
    }
    engine.maintain_grid_window().await;
    engine.maintain_grid_window().await;
    let active = orders(&engine).await;
    assert!(active
        .iter()
        .any(|o| o.client_order_id == first.client_order_id));
    assert!(!active
        .iter()
        .any(|o| o.client_order_id == duplicate.client_order_id
            || o.client_order_id == far.client_order_id));
    assert_eq!(
        active
            .iter()
            .filter(|o| o.side == OrderSide::Sell && o.price == first.price)
            .count(),
        1
    );
}

#[tokio::test]
async fn restart_uses_the_latest_opposite_fill_to_unlock_the_previous_buy_level() {
    let directory = std::env::temp_dir().join(format!("window-restart-{}", Uuid::new_v4()));
    let path = directory.join("bot.db");
    {
        let db = Database::open(&path).unwrap();
        db.insert_trade(&journal_trade(0, OrderSide::Buy, dec!(118.8), dec!(1)))
            .unwrap();
        for id in 1..=205 {
            db.insert_trade(&journal_trade(id, OrderSide::Sell, dec!(118.9), dec!(1)))
                .unwrap();
        }
    }
    {
        let mut restarted = paper_engine_with_db(Arc::new(Database::open(&path).unwrap())).await;
        assert!(restarted
            .state
            .recent_trades
            .read()
            .await
            .iter()
            .all(|t| t.side == OrderSide::Sell));
        restarted.state.position.write().await.size = dec!(10);
        restarted.maintain_grid_window().await;
        let active = orders(&restarted).await;
        assert!(active.iter().all(|o| o.price != dec!(118.9)));
        assert!(active
            .iter()
            .any(|o| o.side == OrderSide::Buy && o.price == dec!(118.8)));
        assert!(active
            .iter()
            .any(|o| o.side == OrderSide::Sell && o.price == dec!(119)));
    }
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn last_prices_are_isolated_by_symbol_and_mode_and_never_force_out_of_bounds_orders() {
    let mut engine = paper_engine().await;
    engine
        .state
        .db
        .insert_trade(&journal_trade(0, OrderSide::Buy, dec!(118.8), dec!(1)))
        .unwrap();
    engine
        .state
        .db
        .insert_trade(&journal_trade(1, OrderSide::Sell, dec!(118.9), dec!(1)))
        .unwrap();
    let mut config = engine.state.config.read().await.clone();
    let rules = engine.state.rules.read().await.clone();
    config.grid.min_price = Some(dec!(118.8));
    config.grid.max_price = Some(dec!(118.9));
    assert_eq!(
        engine
            .window_prices(&config, &rules, dec!(118.84))
            .await
            .unwrap(),
        (vec![dec!(118.8)], vec![])
    );
    config.exchange.dry_run = false;
    config.exchange.is_testnet = true;
    assert_eq!(
        engine
            .window_prices(&config, &rules, dec!(118.84))
            .await
            .unwrap(),
        (vec![dec!(118.8)], vec![dec!(118.9)])
    );
    config.exchange.dry_run = true;
    config.exchange.symbol = "ETHUSDC".into();
    assert_eq!(
        engine
            .window_prices(&config, &rules, dec!(118.84))
            .await
            .unwrap(),
        (vec![dec!(118.8)], vec![dec!(118.9)])
    );
    config.exchange.symbol = "SOLUSDC".into();
    *engine.state.config.write().await = config;
    engine.state.position.write().await.size = dec!(10);
    engine.maintain_grid_window().await;
    assert_eq!(orders(&engine).await.len(), 1);
    assert_eq!(orders(&engine).await[0].side, OrderSide::Buy);
    assert_eq!(*engine.state.status.read().await, BotStatus::Running);
}

#[tokio::test]
async fn replaying_an_old_fill_does_not_unlock_the_latest_same_side_price() {
    let mut engine = paper_engine().await;
    let mut first = order("old_fill", OrderSide::Buy, dec!(118.6), dec!(0.1));
    let mut latest = order("latest_fill", OrderSide::Buy, dec!(118.8), dec!(0.1));
    assert!(engine.on_order_filled(&mut first).await);
    assert!(engine.on_order_filled(&mut latest).await);
    assert!(engine.on_order_filled(&mut first).await);
    assert_eq!(
        engine
            .state
            .db
            .last_fill_for("SOLUSDC", TradingMode::Paper)
            .unwrap(),
        Some((OrderSide::Buy, dec!(118.8)))
    );
    assert_eq!(engine.state.db.get_recent_trades(10).unwrap().len(), 2);
    assert_eq!(engine.state.position.read().await.size, dec!(0.2));
    assert!(
        !engine
            .place_grid_order(
                OrderSide::Buy,
                dec!(118.8),
                "gb_b_replay_repeat".into(),
                -1,
                None
            )
            .await
    );
}

#[tokio::test]
async fn remainder_at_latest_buy_price_is_retired_and_replaced_one_level_above() {
    let mut engine = paper_engine().await;
    engine.state.ticker.write().await.last_price = dec!(118.75);
    engine.state.position.write().await.size = dec!(0.1);
    engine
        .state
        .db
        .insert_trade(&journal_trade(0, OrderSide::Buy, dec!(118.8), dec!(0.1)))
        .unwrap();
    let mut target = order(
        "wrong_side_remainder",
        OrderSide::Sell,
        dec!(118.8),
        dec!(0.1),
    );
    target.purpose = OrderPurpose::Remainder;
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
    let active = orders(&engine).await;
    assert!(active
        .iter()
        .all(|o| o.client_order_id != target.client_order_id));
    let sells: Vec<_> = active
        .iter()
        .filter(|o| o.side == OrderSide::Sell)
        .collect();
    assert_eq!(sells.len(), 1);
    assert_eq!(sells[0].price, dec!(118.9));
    assert_eq!(sells[0].quantity, dec!(0.1));
}

#[tokio::test]
async fn multiple_crossed_levels_have_deterministic_fill_order_and_no_same_side_repeats() {
    let mut engine = paper_engine().await;
    engine.state.config.write().await.grid.max_position_usdc = None;
    engine.state.position.write().await.size = dec!(30);
    engine.maintain_grid_window().await;
    for price in [dec!(118.39), dec!(119.31), dec!(118.39), dec!(119.31)] {
        engine
            .handle_ticker_update(TickerInfo {
                symbol: "SOLUSDC".into(),
                last_price: price,
                ..Default::default()
            })
            .await;
        for _ in 0..3 {
            engine.maintain_grid_window().await;
        }
    }
    let mut trades = engine.state.db.get_recent_trades(100).unwrap();
    trades.reverse();
    for side in [OrderSide::Buy, OrderSide::Sell] {
        let prices: Vec<_> = trades
            .iter()
            .filter(|t| t.side == side)
            .map(|t| t.price)
            .collect();
        assert!(prices.len() >= 3);
    }
    assert_execution_steps(&trades, dec!(0.1));
    assert_eq!(
        trades
            .iter()
            .find(|t| t.side == OrderSide::Buy)
            .unwrap()
            .price,
        dec!(118.8)
    );
    assert!(orders(&engine)
        .await
        .iter()
        .all(|o| o.paired_client_order_id.is_none()));
}

fn assert_execution_steps(trades: &[TradeRecord], interval: Decimal) {
    for pair in trades.windows(2) {
        match pair[1].side {
            OrderSide::Buy => assert!(
                pair[1].price <= pair[0].price - interval,
                "buy frontier violated: {pair:?}"
            ),
            OrderSide::Sell => assert!(
                pair[1].price >= pair[0].price + interval,
                "sell frontier violated: {pair:?}"
            ),
        }
    }
}

#[tokio::test]
async fn repeated_buy_sell_cycles_reuse_the_same_two_levels() {
    let mut engine = paper_engine().await;
    {
        let mut config = engine.state.config.write().await;
        config.grid.buy_window = 1;
        config.grid.sell_window = 1;
        config.grid.max_position_usdc = None;
    }
    engine.state.position.write().await.size = dec!(10);
    engine.state.ticker.write().await.last_price = dec!(119.25);
    engine.maintain_grid_window().await;
    for price in [
        dec!(119.19),
        dec!(119.25),
        dec!(119.31),
        dec!(119.25),
        dec!(119.19),
        dec!(119.31),
    ] {
        engine
            .handle_ticker_update(TickerInfo {
                symbol: "SOLUSDC".into(),
                last_price: price,
                ..Default::default()
            })
            .await;
        for _ in 0..3 {
            engine.maintain_grid_window().await;
        }
    }
    let mut trades = engine.state.db.get_recent_trades(10).unwrap();
    trades.reverse();
    assert_eq!(
        trades.iter().map(|t| (t.side, t.price)).collect::<Vec<_>>(),
        vec![
            (OrderSide::Buy, dec!(119.2)),
            (OrderSide::Sell, dec!(119.3)),
            (OrderSide::Buy, dec!(119.2)),
            (OrderSide::Sell, dec!(119.3)),
        ]
    );
    assert_execution_steps(&trades, dec!(0.1));
}
