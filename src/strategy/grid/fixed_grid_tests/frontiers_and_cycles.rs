use super::*;

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
            .place_grid_order(
                OrderSide::Sell,
                dec!(118.9),
                "gb_s_duplicate".into(),
                1,
                None,
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
        engine.pair_submission_decision(),
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
    assert!(matches!(
        engine.pair_submission_decision(),
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
    assert!(matches!(
        engine.pair_submission_decision(),
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
                update_time: Utc::now(),
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
    let directory = crate::test_support::TempTestDir::new("window-restart");
    let path = directory.join("bot.db");
    {
        let db = Arc::new(Database::open(&path).unwrap());
        let initial = paper_engine_with_db(db.clone()).await;
        initial.state.position.write().await.size = dec!(10);
        let paper_state = (
            initial.state.position.read().await.clone(),
            initial.state.account.read().await.clone(),
        );
        db.insert_trade(&journal_trade(0, OrderSide::Buy, dec!(118.8), dec!(1)))
            .unwrap();
        for id in 1..205 {
            db.insert_trade(&journal_trade(id, OrderSide::Sell, dec!(118.9), dec!(1)))
                .unwrap();
        }
        db.insert_trade_with_paper_recovery(
            &journal_trade(205, OrderSide::Sell, dec!(118.9), dec!(1)),
            &GridStats::default(),
            None,
            None,
            Some(&paper_state),
        )
        .unwrap();
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
        assert_eq!(*restarted.state.status.read().await, BotStatus::Running);
        assert_eq!(restarted.state.position.read().await.size, dec!(10));
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
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
    assert_eq!(
        engine
            .window_prices(&config, &rules, dec!(118.84))
            .await
            .unwrap(),
        (vec![dec!(118.8)], vec![dec!(118.9)])
    );
    config.exchange.api_key.clear();
    config.exchange.api_secret.clear();
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
                update_time: Utc::now(),
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

#[tokio::test]
async fn buys_follow_mark_price_above_old_fills_and_keep_queue_priority_within_a_level() {
    let mut engine = paper_engine().await;
    engine.state.config.write().await.grid.max_position_usdc = None;
    engine
        .state
        .db
        .insert_trade(&journal_trade(0, OrderSide::Buy, dec!(110.2), dec!(1)))
        .unwrap();
    {
        let mut ticker = engine.state.ticker.write().await;
        ticker.last_price = dec!(112.35);
        ticker.mark_price = dec!(111.86);
    }
    for price in [dec!(110.1), dec!(110), dec!(109.9)] {
        let stale = order(&format!("old_buy_{price}"), OrderSide::Buy, price, dec!(1));
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(stale.client_order_id.clone(), stale);
    }
    for (mark, expected) in [
        (dec!(111.86), vec![dec!(111.6), dec!(111.7), dec!(111.8)]),
        (dec!(112.16), vec![dec!(111.9), dec!(112), dec!(112.1)]),
        (dec!(111.96), vec![dec!(111.7), dec!(111.8), dec!(111.9)]),
    ] {
        engine.state.ticker.write().await.mark_price = mark;
        // Confirm cancellation before allocating the released funds.
        engine.maintain_grid_window().await;
        engine.maintain_grid_window().await;
        let active = orders(&engine).await;
        let mut prices: Vec<_> = active.iter().map(|o| o.price).collect();
        prices.sort();
        assert_eq!(prices, expected);
        assert!(active.iter().all(|o| o.side == OrderSide::Buy));
        engine.state.ticker.write().await.mark_price = mark + dec!(0.01);
        engine.maintain_grid_window().await;
        let stable = orders(&engine).await;
        assert!(active.iter().all(|o| stable
            .iter()
            .any(|s| s.client_order_id == o.client_order_id)));
    }
    assert_eq!(engine.state.db.get_recent_trades(10).unwrap().len(), 1);
}

#[tokio::test]
async fn sells_follow_falling_mark_price_below_old_fills() {
    let mut engine = paper_engine().await;
    engine.state.config.write().await.grid.buy_window = 0;
    engine.state.position.write().await.size = dec!(100);
    engine
        .state
        .db
        .insert_trade(&journal_trade(0, OrderSide::Sell, dec!(114.5), dec!(1)))
        .unwrap();
    {
        let mut ticker = engine.state.ticker.write().await;
        ticker.last_price = dec!(110.95);
        ticker.mark_price = dec!(111.86);
    }
    engine.maintain_grid_window().await;
    for (mark, expected) in [
        (dec!(111.86), vec![dec!(111.9), dec!(112), dec!(112.1)]),
        (dec!(111.16), vec![dec!(111.2), dec!(111.3), dec!(111.4)]),
        (dec!(111.2), vec![dec!(111.3), dec!(111.4), dec!(111.5)]),
    ] {
        engine.state.ticker.write().await.mark_price = mark;
        engine.maintain_grid_window().await;
        engine.maintain_grid_window().await;
        let active = orders(&engine).await;
        let mut prices: Vec<_> = active.iter().map(|o| o.price).collect();
        prices.sort();
        assert_eq!(prices, expected);
        assert!(active.iter().all(|o| o.side == OrderSide::Sell));
        assert!(reserved_sell_quantity(&active) <= dec!(100));
    }
}

#[tokio::test]
async fn mark_centered_orders_still_respect_maker_prices_and_last_price_fallback() {
    let mut engine = paper_engine().await;
    engine.state.config.write().await.grid.max_position_usdc = None;
    engine.state.position.write().await.size = dec!(100);
    {
        let mut ticker = engine.state.ticker.write().await;
        ticker.last_price = dec!(111.95);
        ticker.mark_price = dec!(112.16);
    }
    assert!(
        !engine
            .place_grid_order(
                OrderSide::Buy,
                dec!(112.1),
                "gb_crossed_buy".into(),
                -1,
                None
            )
            .await
    );
    assert!(
        engine
            .place_grid_order(OrderSide::Buy, dec!(111.9), "gb_safe_buy".into(), -1, None)
            .await
    );
    {
        let mut ticker = engine.state.ticker.write().await;
        ticker.last_price = dec!(112.05);
        ticker.mark_price = dec!(111.86);
    }
    assert!(
        !engine
            .place_grid_order(
                OrderSide::Sell,
                dec!(111.9),
                "gb_crossed_sell".into(),
                1,
                None
            )
            .await
    );
    assert!(
        engine
            .place_grid_order(OrderSide::Sell, dec!(112.1), "gb_safe_sell".into(), 1, None)
            .await
    );
    engine.state.ticker.write().await.mark_price = Decimal::ZERO;
    engine.maintain_grid_window().await;
    engine.maintain_grid_window().await;
    let active = orders(&engine).await;
    assert!(active
        .iter()
        .any(|o| o.side == OrderSide::Buy && o.price == dec!(112)));
    assert!(active
        .iter()
        .any(|o| o.side == OrderSide::Sell && o.price == dec!(112.1)));
}

#[tokio::test]
async fn remainder_recovery_uses_mark_window_when_last_price_has_not_moved() {
    let mut engine = paper_engine().await;
    engine.state.config.write().await.grid.buy_window = 0;
    engine.state.position.write().await.size = dec!(0.1);
    {
        let mut ticker = engine.state.ticker.write().await;
        ticker.last_price = dec!(111.74);
        ticker.mark_price = dec!(112.16);
    }
    let mut target = order(
        "old_mark_remainder",
        OrderSide::Sell,
        dec!(111.8),
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
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].price, dec!(112.2));
    assert_eq!(active[0].quantity, dec!(0.1));
    assert_eq!(active[0].purpose, OrderPurpose::Remainder);
    assert_ne!(active[0].client_order_id, target.client_order_id);
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
                update_time: Utc::now(),
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
