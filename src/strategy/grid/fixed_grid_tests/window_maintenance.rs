use super::*;

#[tokio::test]
async fn history_cleanup_keeps_the_window_boundary_and_does_not_reapply_fills() {
    let mut engine = paper_engine().await;
    engine.state.ticker.write().await.last_price = dec!(119.25);
    let mut filled = order("b_cleanup_filled", OrderSide::Buy, dec!(119.2), dec!(1));
    assert!(engine.on_order_filled(&mut filled).await);
    let position = engine.state.position.read().await.size;
    let balance = engine.state.account.read().await.total_wallet_balance;
    let config = engine.state.config.read().await.clone();
    let rules = engine.state.rules.read().await.clone();
    let expected_window = engine
        .window_prices(&config, &rules, dec!(119.25))
        .await
        .unwrap();
    let pending = order("b_cleanup_pending", OrderSide::Buy, dec!(119.1), dec!(1));
    engine
        .state
        .active_orders
        .write()
        .await
        .insert(pending.client_order_id.clone(), pending.clone());
    engine.state.db.save_managed_order(&pending).unwrap();
    let pair = new_pair_intent(&filled, OrderSide::Sell, dec!(119.3), dec!(1));
    let plan = RemainderPlan {
        symbol: "SOLUSDC".into(),
        mode: TradingMode::Paper,
        sources: vec![pending],
        target: pair,
        phase: RemainderPhase::Canceling,
    };
    engine.state.db.save_remainder_plan(&plan).unwrap();

    assert_eq!(engine.state.clear_trade_history().await.unwrap(), 1);
    assert_eq!(
        engine
            .window_prices(&config, &rules, dec!(119.25))
            .await
            .unwrap(),
        expected_window
    );
    assert_eq!(orders(&engine).await.len(), 1);
    assert_eq!(
        engine
            .state
            .db
            .load_managed_orders("SOLUSDC")
            .unwrap()
            .len(),
        1
    );
    assert!(engine
        .state
        .db
        .load_remainder_plan("SOLUSDC", TradingMode::Paper)
        .unwrap()
        .is_some());
    assert!(engine.on_order_filled(&mut filled).await);
    assert_eq!(engine.state.position.read().await.size, position);
    assert_eq!(
        engine.state.account.read().await.total_wallet_balance,
        balance
    );
    assert_eq!(engine.state.stats.read().await.total_trades, 0);
    assert!(engine.state.db.get_recent_trades(10).unwrap().is_empty());

    let mut next = order("b_cleanup_next", OrderSide::Buy, dec!(119.1), dec!(1));
    assert!(engine.on_order_filled(&mut next).await);
    assert_eq!(engine.state.stats.read().await.total_trades, 1);
    assert_eq!(engine.state.position.read().await.size, position + dec!(1));
}

#[tokio::test]
async fn paired_paper_close_after_history_cleanup_records_the_cycle_spread() {
    for symbol_only in [true, false] {
        for (direction, opening_side, closing_side, opening_price) in [
            (
                GridDirection::Long,
                OrderSide::Buy,
                OrderSide::Sell,
                dec!(100),
            ),
            (
                GridDirection::Short,
                OrderSide::Sell,
                OrderSide::Buy,
                dec!(110),
            ),
        ] {
            let mut engine = paper_engine().await;
            engine.state.config.write().await.grid.direction = direction;
            let mut opening = order("paired_cleanup_open", opening_side, opening_price, dec!(2));
            assert!(engine.on_order_filled(&mut opening).await);

            let mut closing = new_pair_intent(&opening, closing_side, dec!(105), dec!(2));
            engine
                .state
                .active_orders
                .write()
                .await
                .insert(closing.client_order_id.clone(), closing.clone());

            // Paper orders exist in memory; they are not saved as managed live orders.
            assert!(engine
                .state
                .db
                .load_managed_orders("SOLUSDC")
                .unwrap()
                .is_empty());
            let mut other_trade = engine
                .state
                .db
                .get_trade_by_client_id(&opening.client_order_id, TradingMode::Paper)
                .unwrap()
                .unwrap();
            other_trade.trade_id = "other_symbol_trade".into();
            other_trade.client_order_id = "other_symbol_order".into();
            other_trade.symbol = "BTCUSDC".into();
            assert!(engine.state.db.insert_trade(&other_trade).unwrap());

            let deleted = if symbol_only {
                engine
                    .state
                    .clear_symbol_trade_history("SOLUSDC".into())
                    .await
                    .unwrap()
                    .0
            } else {
                engine.state.clear_trade_history().await.unwrap()
            };
            assert_eq!(deleted, if symbol_only { 1 } else { 2 });
            assert!(engine
                .state
                .db
                .get_recent_trades_for("SOLUSDC", TradingMode::Paper, 10)
                .unwrap()
                .is_empty());
            assert_eq!(
                engine.state.db.get_recent_trades(10).unwrap().len(),
                if symbol_only { 1 } else { 0 }
            );
            assert!(engine.state.recent_trades.read().await.is_empty());
            assert_eq!(engine.state.stats.read().await.total_trades, 0);
            assert_eq!(orders(&engine).await.len(), 1);

            assert!(engine.on_order_filled(&mut closing).await);
            assert!(orders(&engine).await.is_empty());
            assert_eq!(engine.state.position.read().await.size, Decimal::ZERO);
            let stats = engine.state.stats.read().await.clone();
            assert_eq!(stats.total_trades, 1);
            assert_eq!(stats.completed_cycles, 1);
            assert_eq!(stats.total_realized_profit, dec!(10));
            assert_eq!(stats.total_realized_pnl, dec!(10));
            let trades = engine
                .state
                .db
                .get_recent_trades_for("SOLUSDC", TradingMode::Paper, 10)
                .unwrap();
            assert_eq!(trades.len(), 1);
            assert_eq!(trades[0].client_order_id, closing.client_order_id);
            assert_eq!(trades[0].realized_pnl, dec!(10));
            assert_eq!(
                engine
                    .state
                    .db
                    .load_scoped_stats("SOLUSDC", TradingMode::Paper)
                    .unwrap()
                    .unwrap()
                    .1,
                1
            );
            assert_eq!(
                engine
                    .state
                    .db
                    .load_scoped_stats("SOLUSDC", TradingMode::Paper)
                    .unwrap()
                    .unwrap()
                    .2,
                dec!(10)
            );
        }
    }
}

#[tokio::test]
async fn sell_window_follows_market_and_prunes_stale_orders_without_a_historical_floor() {
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
        (OrderSide::Sell, vec![dec!(119.3), dec!(119.4), dec!(119.6)]),
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
async fn unchanged_paper_ticks_skip_database_work_but_configuration_and_orders_invalidate() {
    let mut engine = paper_engine().await;
    engine.state.config.write().await.grid.max_position_usdc = None;
    engine.state.position.write().await.size = dec!(5.04);
    let samples = engine.state.db.enable_benchmark_wait_samples();
    engine.maintain_grid_window().await;
    assert!(engine.paper_window_cache.is_some());
    let initial = orders(&engine).await;
    let queries = samples.queue.lock().unwrap().len();
    for price in [dec!(118.81), dec!(118.87), dec!(118.89)] {
        engine.state.ticker.write().await.last_price = price;
        engine.simulate_order_fills(price).await;
    }
    assert_eq!(samples.queue.lock().unwrap().len(), queries);
    let unchanged = orders(&engine).await;
    assert!(initial.iter().all(|order| unchanged
        .iter()
        .any(|current| { current.client_order_id == order.client_order_id })));

    // An in-place order change keeps the count and ID but still requires cleanup.
    let id = initial
        .iter()
        .find(|order| order.side == OrderSide::Buy)
        .unwrap()
        .client_order_id
        .clone();
    engine
        .state
        .active_orders
        .write()
        .await
        .get_mut(&id)
        .unwrap()
        .price = dec!(117);
    engine.simulate_order_fills(dec!(118.84)).await;
    assert!(samples.queue.lock().unwrap().len() > queries);
    assert!(!engine.state.active_orders.read().await.contains_key(&id));
    engine.maintain_grid_window().await;
    assert!(engine.paper_window_cache.is_some());
    let queries = samples.queue.lock().unwrap().len();
    engine.state.config.write().await.grid.buy_window = 2;
    engine.simulate_order_fills(dec!(118.84)).await;
    assert!(samples.queue.lock().unwrap().len() > queries);
    assert_eq!(
        orders(&engine)
            .await
            .iter()
            .filter(|order| order.side == OrderSide::Buy)
            .count(),
        2
    );
    assert_eq!(*engine.state.status.read().await, BotStatus::Running);
    assert!(engine.state.db.get_recent_trades(10).unwrap().is_empty());
}

#[tokio::test]
async fn paper_window_cache_does_not_bypass_limits_or_failed_maintenance_retries() {
    let mut engine = paper_engine().await;
    engine.state.position.write().await.size = dec!(5.04);
    let state = engine.state.clone();
    let gate = state.order_submission_lock.clone().write_owned().await;
    engine.maintain_grid_window().await;
    assert!(engine.paper_window_cache.is_none());
    drop(gate);
    engine.maintain_grid_window().await;
    assert!(engine.paper_window_cache.is_some());
    {
        let mut config = state.config.write().await;
        config.grid.max_position_usdc = Some(dec!(500));
        config.grid.pause_on_max_position = true;
    }
    engine.simulate_order_fills(dec!(118.84)).await;
    assert_eq!(*state.status.read().await, BotStatus::Paused);
    assert!(orders(&engine).await.is_empty());
}

#[tokio::test]
async fn last_fill_context_is_invalidated_by_a_fill_before_the_next_order() {
    let mut engine = paper_engine().await;
    let config = engine.state.config.read().await.clone();
    let cached = engine.load_last_fill(&config).await.unwrap();
    assert!(cached.fill.is_none());
    let mut fill = order("cached_fill", OrderSide::Buy, dec!(118.8), dec!(0.1));
    assert!(engine.on_order_filled(&mut fill).await);
    assert!(
        !engine
            .place_grid_order_with_last_fill(
                OrderSide::Buy,
                dec!(118.8),
                "gb_stale_fill_context".into(),
                -1,
                None,
                Some(cached),
            )
            .await
    );
    assert!(!engine
        .state
        .active_orders
        .read()
        .await
        .contains_key("gb_stale_fill_context"));
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
async fn recent_fill_reserves_one_level_without_pinning_a_moving_window() {
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
    // A rising market may place higher buys while the last filled level stays reserved.
    engine.state.ticker.write().await.last_price = dec!(119.45);
    assert!(
        engine
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
        engine
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
                update_time: Utc::now(),
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
        engine.state.config.write().await.exchange.api_key = "live-key".into();
        engine.state.config.write().await.exchange.api_secret = "live-secret".into();
        let old = order("b_migration", OrderSide::Buy, price, dec!(1.68));
        let response = serde_json::json!({
            "orderId": 42, "clientOrderId": old.client_order_id,
            "symbol": "SOLUSDC", "status": if terminal { "CANCELED" } else { "PARTIALLY_FILLED" },
            "price": price.to_string(), "avgPrice": price.to_string(), "origQty": "1.68", "executedQty": "0.46",
            "side": "BUY", "type": "LIMIT", "timeInForce": "GTX"
        });
        let app = Router::new()
            .route(
                "/fapi/v1/order",
                get(move || {
                    let response = response.clone();
                    async move { Json(response) }
                })
                .delete(|| async { Json(serde_json::json!({})) }),
            )
            .route("/fapi/v2/positionRisk", get(|| async {
                Json(serde_json::json!([{"symbol":"SOLUSDC", "positionSide":"BOTH",
                    "positionAmt":"0.46", "entryPrice":"118.81", "markPrice":"118.84",
                    "unRealizedProfit":"0", "liquidationPrice":"0", "leverage":"20"}]))
            }))
            .route("/fapi/v2/account", get(|| async {
                Json(serde_json::json!({"totalWalletBalance":"10000", "totalMarginBalance":"10000",
                    "totalUnrealizedProfit":"0", "availableBalance":"10000", "assets":[
                        {"asset":"USDC", "walletBalance":"10000", "marginBalance":"10000",
                         "availableBalance":"10000", "unrealizedProfit":"0"}]}))
            }));
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
    engine.state.config.write().await.exchange.api_key = "live-key".into();
    engine.state.config.write().await.exchange.api_secret = "live-secret".into();
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
async fn restart_recenters_sells_near_market_even_after_higher_sell_fills() {
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
            engine.state.config.write().await.exchange.api_key = "live-key".into();
            engine.state.config.write().await.exchange.api_secret = "live-secret".into();
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
            let app = Router::new()
                .route(
                    "/fapi/v1/order",
                    get(move || {
                        let response = response.clone();
                        async move { Json(response) }
                    })
                    .delete(|| async { Json(serde_json::json!({})) }),
                )
                .route("/fapi/v2/positionRisk", get(|| async {
                    Json(serde_json::json!([{"symbol":"SOLUSDC", "positionSide":"BOTH",
                        "positionAmt":"100", "entryPrice":"118.81", "markPrice":"118.84",
                        "unRealizedProfit":"0", "liquidationPrice":"0", "leverage":"20"}]))
                }))
                .route("/fapi/v2/account", get(|| async {
                    Json(serde_json::json!({"totalWalletBalance":"10000", "totalMarginBalance":"10000",
                        "totalUnrealizedProfit":"0", "availableBalance":"10000", "assets":[
                            {"asset":"USDC", "walletBalance":"10000", "marginBalance":"10000",
                             "availableBalance":"10000", "unrealizedProfit":"0"}]}))
                }));
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
