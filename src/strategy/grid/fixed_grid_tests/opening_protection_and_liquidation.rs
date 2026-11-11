use super::*;

#[tokio::test]
async fn empty_paper_grid_recovers_from_either_market_completion_order_without_pushes() {
    for mark_first in [false, true] {
        for blocked in [
            "ready",
            "gate",
            "paused",
            "shutdown",
            "live_account",
            "live_orders",
            "unreconciled",
        ] {
            let mut engine = paper_engine().await;
            let state = engine.state.clone();
            state.config.write().await.exchange.sync_interval_secs = 20;
            *state.position.write().await = PositionInfo::default();
            let now = Utc::now();
            *state.ticker.write().await = TickerInfo {
                symbol: "SOLUSDC".into(),
                last_price: dec!(100),
                mark_price: dec!(100),
                update_time: now - chrono::Duration::seconds(20),
                mark_update_time: now - chrono::Duration::seconds(20),
                ..Default::default()
            };
            if blocked.starts_with("live_") || blocked == "unreconciled" {
                let mut config = state.config.write().await;
                config.exchange.api_key = "live-key".into();
                config.exchange.api_secret = "live-secret".into();
                engine.last_account_sync = Some(Instant::now());
                engine.last_orders_sync = Some(Instant::now());
                if blocked == "live_account" {
                    engine.last_account_sync = None;
                }
                if blocked == "live_orders" {
                    engine.last_orders_sync = None;
                }
                engine.reconciliation_blocked = blocked == "unreconciled";
            }
            let gate = if blocked == "gate" {
                Some(state.order_submission_lock.clone().write_owned().await)
            } else {
                None
            };
            if blocked == "paused" {
                state.pause_trading().await.unwrap();
            }
            let scope = engine.market_request_scope("SOLUSDC").await;
            let mark = MarkPriceCompletion {
                scope: scope.clone(),
                previous: state.ticker.read().await.clone(),
                result: Ok((dec!(100), now)),
            };
            let ticker = TickerCompletion {
                scope,
                result: Ok(TickerInfo {
                    symbol: "SOLUSDC".into(),
                    last_price: dec!(100),
                    update_time: now,
                    ..Default::default()
                }),
            };
            if blocked == "shutdown" {
                state.request_shutdown();
            }
            if mark_first {
                engine.apply_mark_price_completion(mark).await;
                assert!(orders(&engine).await.is_empty());
                engine.apply_ticker_completion(ticker).await;
            } else {
                engine.apply_ticker_completion(ticker).await;
                assert!(orders(&engine).await.is_empty());
                engine.apply_mark_price_completion(mark).await;
            }
            let recovered = orders(&engine).await;
            if blocked == "ready" {
                assert!(
                    recovered.iter().any(|order| order.side == OrderSide::Buy),
                    "mark_first={mark_first}"
                );
            } else {
                assert!(recovered.is_empty(), "{blocked}, mark_first={mark_first}");
            }
            assert!(state.db.get_recent_trades(10).unwrap().is_empty());
            drop(gate);
        }
    }
}

#[tokio::test]
async fn mark_only_completion_runs_paper_position_cap_and_opening_protection() {
    for pause in [false, true] {
        let mut engine = paper_engine().await;
        let state = engine.state.clone();
        {
            let mut config = state.config.write().await;
            config.grid.max_position_usdc = Some(dec!(1000));
            config.grid.pause_on_max_position = pause;
            config.grid.cancel_opening_on_max_position = !pause;
        }
        state.position.write().await.size = dec!(10);
        let now = Utc::now();
        *state.ticker.write().await = TickerInfo {
            symbol: "SOLUSDC".into(),
            last_price: dec!(99),
            mark_price: dec!(99),
            update_time: now - chrono::Duration::seconds(20),
            mark_update_time: now - chrono::Duration::seconds(20),
            ..Default::default()
        };
        for pending in [
            order("mark_cap_buy", OrderSide::Buy, dec!(98), dec!(1)),
            order("mark_cap_sell", OrderSide::Sell, dec!(102), dec!(1)),
        ] {
            state
                .active_orders
                .write()
                .await
                .insert(pending.client_order_id.clone(), pending);
        }
        assert!(engine.pending_paper_ticker.is_none());
        let completion = MarkPriceCompletion {
            scope: engine.market_request_scope("SOLUSDC").await,
            previous: state.ticker.read().await.clone(),
            result: Ok((dec!(101), now)),
        };
        engine.apply_mark_price_completion(completion).await;
        let remaining = orders(&engine).await;
        if pause {
            assert_eq!(*state.status.read().await, BotStatus::Paused);
            assert!(remaining.is_empty());
        } else {
            assert_eq!(*state.status.read().await, BotStatus::Running);
            assert_eq!(remaining.len(), 1);
            assert_eq!(remaining[0].client_order_id, "gb_mark_cap_sell");
            assert_eq!(remaining[0].price, dec!(102));
        }
        assert!(state.db.get_recent_trades(10).unwrap().is_empty());
    }
}

#[tokio::test]
async fn remainder_submission_waits_for_the_global_configuration_gate() {
    let mut engine = paper_engine().await;
    let state = engine.state.clone();
    state.position.write().await.size = dec!(0.6);
    let gate = state.order_submission_lock.clone().write_owned().await;
    engine.reconcile_remainder(&[dec!(118.9)]).await.unwrap();
    let pending = engine.load_remainder_plan().await.unwrap().unwrap();
    assert_eq!(pending.phase, RemainderPhase::Submitting);
    assert!(orders(&engine).await.is_empty());
    assert!(engine.recover_remainder_plan().await.unwrap());
    assert!(orders(&engine).await.is_empty());
    drop(gate);
    assert!(engine.recover_remainder_plan().await.unwrap());
    let submitted = orders(&engine).await;
    assert_eq!(submitted.len(), 1);
    assert_eq!(submitted[0].client_order_id, pending.target.client_order_id);
    assert_eq!(submitted[0].quantity, dec!(0.6));
    assert_eq!(submitted[0].purpose, OrderPurpose::Remainder);
}

#[test]
fn liquidation_distance_is_measured_on_the_losing_side_only() {
    // Long: liquidation below mark. 100 -> 92 is an 8% gap.
    assert!(liquidation_too_close(
        dec!(1),
        dec!(100),
        dec!(92),
        Some(dec!(8))
    ));
    assert!(!liquidation_too_close(
        dec!(1),
        dec!(100),
        dec!(91.9),
        Some(dec!(8))
    ));
    // Short: liquidation above mark.
    assert!(liquidation_too_close(
        dec!(-1),
        dec!(100),
        dec!(108),
        Some(dec!(8))
    ));
    assert!(!liquidation_too_close(
        dec!(-1),
        dec!(100),
        dec!(108.1),
        Some(dec!(8))
    ));
    // A long whose reported liquidation is already above mark is past liquidation.
    assert!(liquidation_too_close(
        dec!(1),
        dec!(100),
        dec!(105),
        Some(dec!(8))
    ));
    // Flat, unreported (0) and disabled never trigger.
    assert!(!liquidation_too_close(
        dec!(0),
        dec!(100),
        dec!(99),
        Some(dec!(8))
    ));
    assert!(!liquidation_too_close(
        dec!(1),
        dec!(100),
        dec!(0),
        Some(dec!(8))
    ));
    assert!(!liquidation_too_close(dec!(1), dec!(100), dec!(99), None));
    // Extreme values fail closed instead of panicking.
    assert!(liquidation_too_close(
        dec!(1),
        Decimal::MAX,
        dec!(1),
        Some(dec!(8))
    ));
    assert_eq!(buy_exposure_usdc(dec!(2), Decimal::MAX, &[]), Decimal::MAX);
    let huge_order = order("gb_b_max", OrderSide::Buy, Decimal::MAX, dec!(2));
    assert_eq!(
        buy_exposure_usdc(Decimal::ZERO, dec!(100), &[huge_order]),
        Decimal::MAX
    );

    // Paper valuation and shared ledger survive extreme WS ticks for both long and short
    // positions and recover exact balances once normal prices return.
    let db = crate::db::Database::open(":memory:").unwrap();
    let now = Utc::now();
    let mut short_pos = PositionInfo {
        symbol: "SOLUSDC".into(),
        size: dec!(-2),
        entry_price: dec!(100),
        mark_price: dec!(100),
        leverage: 10,
        update_time: Some(now),
        ..Default::default()
    };
    let mut account = AccountInfo {
        asset: "USDC".into(),
        total_wallet_balance: PAPER_INITIAL_BALANCE,
        available_balance: PAPER_INITIAL_BALANCE,
        margin_balance: PAPER_INITIAL_BALANCE,
        update_time: now,
        ..Default::default()
    };
    let extreme_ticker = TickerInfo {
        symbol: "SOLUSDC".into(),
        mark_price: Decimal::MAX,
        mark_update_time: now,
        update_time: now,
        ..Default::default()
    };
    update_paper_account(&mut short_pos, &mut account, &extreme_ticker);
    assert_eq!(short_pos.unrealized_pnl, Decimal::MIN);
    assert!(account.available_balance < Decimal::ZERO);
    let shared_extreme = db
        .shared_paper_account("SOLUSDC", &short_pos, &account)
        .unwrap();
    assert!(shared_extreme.available_balance < Decimal::ZERO);
    let normal_ticker = TickerInfo {
        symbol: "SOLUSDC".into(),
        mark_price: dec!(110),
        mark_update_time: now + chrono::Duration::seconds(1),
        update_time: now + chrono::Duration::seconds(1),
        ..Default::default()
    };
    update_paper_account(&mut short_pos, &mut account, &normal_ticker);
    let shared_recovered = db
        .shared_paper_account("SOLUSDC", &short_pos, &account)
        .unwrap();
    assert_eq!(short_pos.unrealized_pnl, dec!(-20));
    assert_eq!(shared_recovered.unrealized_profit, dec!(-20));
    assert_eq!(
        shared_recovered.margin_balance,
        PAPER_INITIAL_BALANCE - dec!(20)
    );
}

#[tokio::test]
async fn liquidation_proximity_cancels_openings_and_keeps_exits_running() {
    let mut engine = paper_engine().await;
    {
        let mut config = engine.state.config.write().await;
        config.grid.min_liquidation_distance_pct = Some(dec!(10));
    }
    {
        let mut ticker = engine.state.ticker.write().await;
        ticker.last_price = dec!(100.05);
        ticker.mark_price = dec!(100);
    }
    {
        let mut position = engine.state.position.write().await;
        position.size = dec!(2);
        position.liquidation_price = dec!(95);
    }
    for pending in [
        order("limit_buy", OrderSide::Buy, dec!(99.9), dec!(1)),
        order("limit_sell", OrderSide::Sell, dec!(100.1), dec!(1)),
    ] {
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(pending.client_order_id.clone(), pending);
    }

    engine.maintain_grid_window().await;
    assert_eq!(*engine.state.status.read().await, BotStatus::Running);
    let remaining = orders(&engine).await;
    assert!(remaining.iter().all(|o| o.side == OrderSide::Sell));
    assert!(!remaining.is_empty());

    // Price recovers: 100 -> liquidation 80 is a 20% gap, openings resume.
    engine.state.position.write().await.liquidation_price = dec!(80);
    engine.maintain_grid_window().await;
    assert!(orders(&engine)
        .await
        .iter()
        .any(|o| o.side == OrderSide::Buy));
}

#[tokio::test]
async fn fresh_mark_cancels_openings_with_stale_last_trade_without_repricing_exits() {
    for direction in [GridDirection::Long, GridDirection::Short] {
        for at_cap in [false, true] {
            let mut engine = paper_engine().await;
            {
                let mut config = engine.state.config.write().await;
                config.grid.direction = direction;
                config.grid.cancel_opening_on_max_position = at_cap;
                config.grid.max_position_usdc = Some(dec!(1000));
                config.grid.min_liquidation_distance_pct = Some(dec!(10));
            }
            {
                let mut ticker = engine.state.ticker.write().await;
                ticker.last_price = dec!(100.05);
                ticker.mark_price = dec!(100);
                ticker.update_time = Utc::now() - chrono::Duration::seconds(16);
                ticker.mark_update_time = Utc::now();
            }
            {
                let mut position = engine.state.position.write().await;
                position.size = direction.sign() * if at_cap { dec!(10) } else { dec!(2) };
                position.liquidation_price = if at_cap {
                    Decimal::ZERO
                } else if direction == GridDirection::Long {
                    dec!(95)
                } else {
                    dec!(105)
                };
            }
            let opening_price = if direction == GridDirection::Long {
                dec!(99)
            } else {
                dec!(101)
            };
            let partial_price = if direction == GridDirection::Long {
                dec!(98)
            } else {
                dec!(102)
            };
            let opening = order(
                "stale_opening",
                direction.opening_side(),
                opening_price,
                dec!(1),
            );
            let mut partial = order(
                "stale_partial",
                direction.opening_side(),
                partial_price,
                dec!(0.5),
            );
            partial.status = OrderStatus::PartiallyFilled;
            let exit_price = if direction == GridDirection::Long {
                dec!(120)
            } else {
                dec!(80)
            };
            let exit = order("stale_exit", direction.closing_side(), exit_price, dec!(1));
            for pending in [opening.clone(), partial.clone(), exit.clone()] {
                engine
                    .state
                    .active_orders
                    .write()
                    .await
                    .insert(pending.client_order_id.clone(), pending);
            }

            engine.maintain_grid_window().await;

            assert_eq!(*engine.state.status.read().await, BotStatus::Running);
            let remaining = engine.state.active_orders.read().await;
            assert_eq!(
                remaining.len(),
                2,
                "direction={direction:?}, at_cap={at_cap}"
            );
            assert!(!remaining.contains_key(&opening.client_order_id));
            assert_eq!(
                remaining[&partial.client_order_id].status,
                OrderStatus::PartiallyFilled
            );
            assert_eq!(
                remaining[&partial.client_order_id].quantity,
                partial.quantity
            );
            assert_eq!(remaining[&exit.client_order_id].price, exit.price);
            assert_eq!(remaining[&exit.client_order_id].quantity, exit.quantity);
        }
    }
}

#[tokio::test]
async fn opening_protection_uses_fresh_position_marks_and_suspends_without_fresh_marks() {
    for direction in [GridDirection::Long, GridDirection::Short] {
        for source in [
            "position_close",
            "position_safe",
            "stale",
            "future",
            "missing",
        ] {
            let mut engine = paper_engine().await;
            {
                let mut config = engine.state.config.write().await;
                config.grid.direction = direction;
                config.grid.min_liquidation_distance_pct = Some(dec!(10));
            }
            let now = Utc::now();
            let safe_mark = if direction == GridDirection::Long {
                dec!(140)
            } else {
                dec!(60)
            };
            {
                let mut ticker = engine.state.ticker.write().await;
                ticker.last_price = safe_mark;
                ticker.update_time = now;
                ticker.mark_price = if source == "missing" {
                    Decimal::ZERO
                } else {
                    safe_mark
                };
                ticker.mark_update_time = if source == "future" {
                    now + chrono::Duration::seconds(60)
                } else {
                    now - chrono::Duration::seconds(16)
                };
            }
            {
                let mut position = engine.state.position.write().await;
                position.size = direction.sign() * dec!(2);
                position.liquidation_price = dec!(100);
                position.mark_price = if source == "position_close" {
                    if direction == GridDirection::Long {
                        dec!(105)
                    } else {
                        dec!(95)
                    }
                } else if source == "missing" {
                    Decimal::ZERO
                } else {
                    safe_mark
                };
                position.update_time = Some(match source {
                    "stale" => now - chrono::Duration::seconds(31),
                    "future" => now + chrono::Duration::seconds(60),
                    _ => now,
                });
            }
            let opening = order("mark_opening", direction.opening_side(), dec!(99), dec!(1));
            let exit = order("mark_exit", direction.closing_side(), dec!(150), dec!(1));
            for pending in [opening.clone(), exit.clone()] {
                engine
                    .state
                    .active_orders
                    .write()
                    .await
                    .insert(pending.client_order_id.clone(), pending);
            }
            let _configuration = engine
                .state
                .order_submission_lock
                .clone()
                .write_owned()
                .await;

            engine.maintain_grid_window().await;

            assert_eq!(*engine.state.status.read().await, BotStatus::Running);
            let active = engine.state.active_orders.read().await;
            let suspended = source != "position_safe";
            assert_eq!(
                active.contains_key(&opening.client_order_id),
                !suspended,
                "direction={direction:?}, source={source}"
            );
            assert_eq!(active.len(), if suspended { 1 } else { 2 });
            assert_eq!(active[&exit.client_order_id].price, exit.price);
            assert_eq!(active[&exit.client_order_id].quantity, exit.quantity);
        }
    }
}

#[tokio::test]
async fn cancel_opening_at_limit_keeps_running_and_keeps_exits() {
    let mut engine = paper_engine().await;
    {
        let mut config = engine.state.config.write().await;
        config.grid.max_position_usdc = Some(dec!(1000));
        config.grid.cancel_opening_on_max_position = true;
    }
    {
        let mut ticker = engine.state.ticker.write().await;
        ticker.last_price = dec!(100.05);
        ticker.mark_price = dec!(100);
    }
    engine.state.position.write().await.size = dec!(10);
    for pending in [
        order("limit_buy", OrderSide::Buy, dec!(99.9), dec!(1)),
        order("limit_sell", OrderSide::Sell, dec!(100.1), dec!(1)),
    ] {
        engine
            .state
            .active_orders
            .write()
            .await
            .insert(pending.client_order_id.clone(), pending);
    }

    engine.maintain_grid_window().await;
    assert_eq!(*engine.state.status.read().await, BotStatus::Running);
    let remaining = orders(&engine).await;
    assert!(remaining.iter().all(|o| o.side == OrderSide::Sell));
    assert!(remaining
        .iter()
        .any(|o| o.client_order_id == "gb_limit_sell"));

    // Below the cap, opening orders are placed again.
    engine.state.position.write().await.size = dec!(1);
    engine.maintain_grid_window().await;
    assert!(orders(&engine)
        .await
        .iter()
        .any(|o| o.side == OrderSide::Buy));
}
