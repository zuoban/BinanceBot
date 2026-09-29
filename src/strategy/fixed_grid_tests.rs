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
async fn full_cycle_preserves_quantity_reserves_buy_level_and_rebuys_target_notional() {
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
        "pending exit reserves its buy level"
    );
    engine.state.ticker.write().await.last_price = dec!(115.1);
    assert!(engine.on_order_filled(&mut sell).await);
    assert_eq!(engine.state.position.read().await.size, Decimal::ZERO);
    let active = orders(&engine).await;
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].side, OrderSide::Buy);
    assert_eq!(active[0].price, dec!(114));
    assert_eq!(active[0].quantity, dec!(17.54));
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
async fn migration_requires_terminal_cancellation_and_recovers_racing_buy_fill() {
    use axum::{routing::get, Json, Router};
    for terminal in [false, true] {
        let mut engine = paper_engine().await;
        engine.state.config.write().await.exchange.dry_run = false;
        let old = order("b_migration", OrderSide::Buy, dec!(118.81), dec!(1.68));
        let response = serde_json::json!({
            "orderId": 42, "clientOrderId": old.client_order_id,
            "symbol": "SOLUSDC", "status": if terminal { "CANCELED" } else { "PARTIALLY_FILLED" },
            "price": "118.81", "avgPrice": "118.81", "origQty": "1.68", "executedQty": "0.46",
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
        engine.maintain_grid_window().await;
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
            assert_eq!(intents[0].price, dec!(118.91));
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
async fn sold_level_waits_for_lower_buy_even_if_inventory_and_price_recover() {
    let mut engine = paper_engine().await;
    engine.state.position.write().await.size = dec!(107.33);
    engine.state.ticker.write().await.last_price = dec!(119.51);
    let mut sell = order("s_initial", OrderSide::Sell, dec!(119.5), dec!(1.67));
    assert!(engine.on_order_filled(&mut sell).await);
    // The paired buy is blocked by the position limit, as in the user's logs.
    assert!(orders(&engine).await.is_empty());
    assert!(engine
        .waiting_sell_levels()
        .await
        .unwrap()
        .contains(&dec!(119.5)));
    for market in [dec!(119.49), dec!(119.45), dec!(119.48), dec!(119.44)] {
        engine.state.ticker.write().await.last_price = market;
        engine.maintain_grid_window().await;
        assert!(orders(&engine)
            .await
            .iter()
            .all(|o| o.side != OrderSide::Sell || o.price != dec!(119.5)));
        assert!(
            !engine
                .place_grid_order(
                    OrderSide::Sell,
                    dec!(119.5),
                    "gb_s_duplicate".into(),
                    1,
                    None
                )
                .await
        );
    }
    // The remainder path must not refill the omitted ordinary level either.
    engine.reconcile_remainder(&[dec!(119.5)]).await.unwrap();
    assert!(orders(&engine).await.iter().all(|o| o.price != dec!(119.5)));

    let mut buy = order("b_rearm", OrderSide::Buy, dec!(119.4), dec!(1.67));
    assert!(engine.on_order_filled(&mut buy).await);
    assert!(!engine
        .waiting_sell_levels()
        .await
        .unwrap()
        .contains(&dec!(119.5)));
    engine.maintain_grid_window().await;
    let at_level: Vec<_> = orders(&engine)
        .await
        .into_iter()
        .filter(|o| o.side == OrderSide::Sell && o.price == dec!(119.5))
        .collect();
    assert_eq!(at_level.len(), 1);
    assert!(at_level[0].is_take_profit);
    let mut exit = at_level[0].clone();
    engine.state.ticker.write().await.last_price = dec!(119.51);
    engine.on_order_filled(&mut exit).await;
    assert!(engine
        .waiting_sell_levels()
        .await
        .unwrap()
        .contains(&dec!(119.5)));
}

#[tokio::test]
async fn sold_level_survives_restart_and_stops_old_remainder_plan_and_duplicate() {
    let mut first = paper_engine().await;
    first.state.position.write().await.size = dec!(107.33);
    first.state.ticker.write().await.last_price = dec!(119.51);
    let mut sell = order("s_before_restart", OrderSide::Sell, dec!(119.5), dec!(1.67));
    first.on_order_filled(&mut sell).await;
    first.state.ticker.write().await.last_price = dec!(119.45);
    let mut target = order("s_pending_retry", OrderSide::Sell, dec!(119.5), dec!(1.67));
    target.purpose = OrderPurpose::Remainder;
    first
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
    let duplicate = order(
        "s_existing_duplicate",
        OrderSide::Sell,
        dec!(119.5),
        dec!(1.67),
    );
    first
        .state
        .active_orders
        .write()
        .await
        .insert(duplicate.client_order_id.clone(), duplicate);
    let (_, rx) = mpsc::channel(1);
    let (_, ticker_rx) = broadcast::channel(1);
    let mut restarted =
        GridTradingEngine::new(first.state.clone(), first.client.clone(), rx, ticker_rx);
    drop(first);
    restarted.maintain_grid_window().await;
    restarted.maintain_grid_window().await;
    assert!(restarted.load_remainder_plan().await.unwrap().is_none());
    let active = orders(&restarted).await;
    assert!(active
        .iter()
        .all(|o| o.side != OrderSide::Sell || o.price != dec!(119.5)));
    assert!(active
        .iter()
        .any(|o| o.side == OrderSide::Sell && o.price == dec!(119.6)));
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
async fn ledger_reads_full_history_incrementally_and_isolates_mode_and_symbol() {
    let mut engine = paper_engine().await;
    let sold = journal_trade(0, OrderSide::Sell, dec!(119.500000), dec!(1.67));
    engine
        .state
        .db
        .insert_trade_with_stats(&sold, &GridStats::default())
        .unwrap();
    for i in 1..=1001 {
        let trade = journal_trade(i, OrderSide::Buy, dec!(120), dec!(1.66));
        engine
            .state
            .db
            .insert_trade_with_stats(&trade, &GridStats::default())
            .unwrap();
    }
    assert!(engine
        .waiting_sell_levels()
        .await
        .unwrap()
        .contains(&dec!(119.5)));
    // Neither another mode nor another symbol can rearm this sell level.
    let mut other = journal_trade(1002, OrderSide::Buy, dec!(119.4), dec!(1.67));
    other.mode = TradingMode::Live;
    engine
        .state
        .db
        .insert_trade_with_stats(&other, &GridStats::default())
        .unwrap();
    let mut other_symbol = journal_trade(1003, OrderSide::Buy, dec!(119.4), dec!(1.67));
    other_symbol.symbol = "ETHUSDC".into();
    engine
        .state
        .db
        .insert_trade_with_stats(&other_symbol, &GridStats::default())
        .unwrap();
    let partial = journal_trade(1004, OrderSide::Buy, dec!(119.4), dec!(0.1));
    engine
        .state
        .db
        .insert_trade_with_stats(&partial, &GridStats::default())
        .unwrap();
    assert!(engine
        .waiting_sell_levels()
        .await
        .unwrap()
        .contains(&dec!(119.5)));
    let buy = journal_trade(1005, OrderSide::Buy, dec!(119.4), dec!(1.67));
    engine
        .state
        .db
        .insert_trade_with_stats(&buy, &GridStats::default())
        .unwrap();
    assert!(!engine
        .waiting_sell_levels()
        .await
        .unwrap()
        .contains(&dec!(119.5)));
    // A duplicate fill must not reset the buy/sell alternation.
    assert!(!engine
        .state
        .db
        .insert_trade_with_stats(&sold, &GridStats::default())
        .unwrap());
    assert!(!engine
        .waiting_sell_levels()
        .await
        .unwrap()
        .contains(&dec!(119.5)));
    engine.state.config.write().await.exchange.dry_run = false;
    assert!(!engine
        .waiting_sell_levels()
        .await
        .unwrap()
        .contains(&dec!(119.5)));
    engine.state.config.write().await.exchange.dry_run = true;
    let sold_again = journal_trade(1006, OrderSide::Sell, dec!(119.5), dec!(1.67));
    engine
        .state
        .db
        .insert_trade_with_stats(&sold_again, &GridStats::default())
        .unwrap();
    assert!(engine
        .waiting_sell_levels()
        .await
        .unwrap()
        .contains(&dec!(119.5)));
}
