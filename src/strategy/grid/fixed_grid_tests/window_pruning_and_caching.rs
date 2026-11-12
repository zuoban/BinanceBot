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
                    .clear_symbol_trade_history("SOLUSDC".into(), None)
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
async fn paper_window_cache_populates_with_flat_position_and_invalidates_when_position_grows() {
    let mut engine = paper_engine().await;
    engine.state.config.write().await.grid.max_position_usdc = None;
    engine.state.position.write().await.size = Decimal::ZERO;
    let samples = engine.state.db.enable_benchmark_wait_samples();
    engine.maintain_grid_window().await;
    assert!(engine.paper_window_cache.is_some());
    let queries = samples.queue.lock().unwrap().len();
    for price in [dec!(118.81), dec!(118.87), dec!(118.89)] {
        engine.state.ticker.write().await.last_price = price;
        engine.simulate_order_fills(price).await;
    }
    assert_eq!(samples.queue.lock().unwrap().len(), queries);
    assert!(orders(&engine)
        .await
        .iter()
        .all(|order| order.side == OrderSide::Buy));

    engine.state.position.write().await.size = dec!(2);
    engine.simulate_order_fills(dec!(118.85)).await;
    assert!(orders(&engine)
        .await
        .iter()
        .any(|order| order.side == OrderSide::Sell));
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
