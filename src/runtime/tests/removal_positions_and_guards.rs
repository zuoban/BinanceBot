use super::super::*;
use super::{
    assert_paper_account_and_position_preserved, fixture, seed_paper_account_and_position,
};
use crate::config::GlobalConfig;
use crate::types::{BotControlAction, BotStatus, TradingMode};
use axum::http::StatusCode;
use axum::Router;
use serde_json::{json, Value};

#[tokio::test]
async fn paper_removal_rejects_open_positions_and_rebuilds_member() {
    for size in [
        rust_decimal_macros::dec!(2.5),
        rust_decimal_macros::dec!(-2.5),
    ] {
        let mut runtime = fixture();
        let state = runtime.states()[0].clone();
        let other = runtime.states()[1].clone();
        let (account, mut position) = seed_paper_account_and_position(&state).await;
        position.size = size;
        state.position.write().await.size = size;
        state.pause_trading().await.unwrap();
        let saved =
            serde_json::to_value(runtime.handle.db.load_config().unwrap().unwrap()).unwrap();

        let error = runtime.remove_pair("SOLUSDC").await.unwrap_err();
        assert!(error.to_string().contains("持仓"));
        assert_eq!(
            serde_json::to_value(runtime.handle.db.load_config().unwrap().unwrap()).unwrap(),
            saved
        );
        let members = runtime.handle.states.read().await.clone();
        assert_eq!(members.len(), 2);
        let replacement = runtime.states()[0].clone();
        assert!(!Arc::ptr_eq(&state, &replacement));
        assert!(Arc::ptr_eq(&members[0], &replacement));
        assert!(Arc::ptr_eq(&members[1], &other));
        assert!(state.is_shutting_down());
        assert!(!replacement.is_shutting_down());
        assert_eq!(*replacement.status.read().await, BotStatus::Paused);
        assert!(!runtime.pairs[0].engine.is_finished());
        assert_paper_account_and_position_preserved(&replacement, &account, &position).await;
    }
}

fn position_risk(symbol: &str, amount: &str) -> Value {
    json!({
        "symbol": symbol, "positionSide": "BOTH", "positionAmt": amount,
        "entryPrice": "100", "markPrice": "99", "unRealizedProfit": "0",
        "liquidationPrice": "0", "leverage": "20"
    })
}

#[tokio::test]
async fn live_removal_requires_confirmed_zero_exchange_position_after_shutdown() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    for (time_status, status, body, allowed) in [
        (
            StatusCode::OK,
            StatusCode::OK,
            json!([position_risk("SOLUSDC", "1")]),
            false,
        ),
        (
            StatusCode::OK,
            StatusCode::OK,
            json!([position_risk("SOLUSDC", "-1")]),
            false,
        ),
        (
            StatusCode::OK,
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"msg": "exchange error"}),
            false,
        ),
        (StatusCode::OK, StatusCode::OK, json!([]), false),
        (
            StatusCode::OK,
            StatusCode::OK,
            json!([position_risk("BTCUSDT", "0")]),
            false,
        ),
        (
            StatusCode::OK,
            StatusCode::OK,
            json!([{"symbol": "SOLUSDC"}]),
            false,
        ),
        (
            StatusCode::OK,
            StatusCode::OK,
            json!([position_risk("SOLUSDC", "0")]),
            true,
        ),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::OK,
            json!([position_risk("SOLUSDC", "0")]),
            false,
        ),
    ] {
        let confirmed_open = time_status == StatusCode::OK
            && status == StatusCode::OK
            && body.as_array().is_some_and(|positions| {
                positions.first().is_some_and(|position| {
                    position["symbol"] == "SOLUSDC"
                        && (position["positionAmt"] == "1" || position["positionAmt"] == "-1")
                })
            });
        let stopped = Arc::new(AtomicBool::new(false));
        let queried_early = Arc::new(AtomicBool::new(false));
        let queries = Arc::new(AtomicUsize::new(0));
        let time_queries = Arc::new(AtomicUsize::new(0));
        let sync_completed = Arc::new(AtomicBool::new(false));
        let app = Router::new().route(
            "/fapi/v1/time",
            axum::routing::get({
                let time_queries = time_queries.clone();
                let sync_completed = sync_completed.clone();
                move || {
                    let time_queries = time_queries.clone();
                    let sync_completed = sync_completed.clone();
                    async move {
                        if time_queries.fetch_add(1, Ordering::SeqCst) > 0 {
                            std::future::pending::<()>().await;
                        }
                        sync_completed.store(true, Ordering::SeqCst);
                        (time_status, axum::Json(json!({"serverTime": chrono::Utc::now().timestamp_millis() + 60_000})))
                    }
                }
            }),
        ).route(
            "/fapi/v2/positionRisk",
            axum::routing::get({
                let stopped = stopped.clone();
                let queried_early = queried_early.clone();
                let queries = queries.clone();
                let sync_completed = sync_completed.clone();
                move |axum::extract::RawQuery(query): axum::extract::RawQuery| {
                    let body = body.clone();
                    let stopped = stopped.clone();
                    let queried_early = queried_early.clone();
                    let queries = queries.clone();
                    let sync_completed = sync_completed.clone();
                    async move {
                        assert!(sync_completed.load(Ordering::SeqCst));
                        let timestamp: i64 = query.unwrap().split('&')
                            .find_map(|part| part.strip_prefix("timestamp="))
                            .unwrap().parse().unwrap();
                        assert!(timestamp > chrono::Utc::now().timestamp_millis() + 30_000);
                        if !stopped.load(Ordering::SeqCst) {
                            queried_early.store(true, Ordering::SeqCst);
                        }
                        queries.fetch_add(1, Ordering::SeqCst);
                        (status, axum::Json(body))
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let mut runtime = fixture();
        runtime.test_exchange_url = Some(url);
        let old = runtime.pairs.remove(0);
        old.engine.abort();
        let mut config = old.state.config.read().await.clone();
        config.exchange.api_key = "live-key".into();
        config.exchange.api_secret = "live-secret".into();
        let (tx, mut rx) = mpsc::channel(8);
        let state = AppState::new_for_pair(
            config,
            runtime.handle.db.clone(),
            tx,
            runtime.trade_lock.clone(),
        );
        // The cached value deliberately disagrees with the exchange response.
        state.position.write().await.size = if allowed {
            rust_decimal_macros::dec!(3)
        } else {
            rust_decimal::Decimal::ZERO
        };
        state.position.write().await.mark_price = rust_decimal_macros::dec!(99);
        state.position.write().await.update_time = Some(chrono::Utc::now());
        {
            let mut account = state.account.write().await;
            account.asset = "USDC".into();
            account.available_balance = rust_decimal::Decimal::ZERO;
            account.update_time = chrono::Utc::now();
        }
        state.pause_trading().await.unwrap();
        let stopped_on_exit = stopped.clone();
        let engine = tokio::spawn(async move {
            if let Some(BotControlAction::Shutdown { reply, .. }) = rx.recv().await {
                let _ = reply.send(Ok(()));
                stopped_on_exit.store(true, Ordering::SeqCst);
            }
        });
        let (shutdown_tx, _) = watch::channel(false);
        runtime.pairs.insert(
            0,
            PairTask {
                state: state.clone(),
                engine,
                engine_joined: false,
                recovery_failed: false,
                shutdown_tx,
                background: vec![],
            },
        );
        runtime.handle.register_engine(&runtime.pairs[0]);
        *runtime.handle.states.write().await = runtime.states();
        let other = runtime.states()[1].clone();
        {
            let mut account = other.account.write().await;
            account.asset = "USDT".into();
            account.available_balance = rust_decimal_macros::dec!(100);
            account.update_time = chrono::Utc::now();
        }
        let mut global = GlobalConfig::from_app(&AppConfig::default());
        global.global_max_position_notional = Some(rust_decimal_macros::dec!(1000));
        global.pause_on_zero_available_margin = false;
        runtime.global_position_limit.initialize_config(global);
        runtime
            .global_position_limit
            .membership_ready
            .store(true, Ordering::SeqCst);
        for member in [&state, &other] {
            member
                .global_position_initialized
                .store(true, Ordering::SeqCst);
            member.global_position_ready.store(true, Ordering::SeqCst);
            let _ = member
                .global_position_limit
                .set(Arc::downgrade(&runtime.global_position_limit));
        }
        other.resume_trading().await.unwrap();
        let saved =
            serde_json::to_value(runtime.handle.db.load_config().unwrap().unwrap()).unwrap();

        let result = runtime.remove_pair("SOLUSDC").await;
        assert_eq!(
            result.is_ok(),
            allowed,
            "unexpected removal result: {result:?}"
        );
        assert!(time_queries.load(Ordering::SeqCst) > 0);
        assert_eq!(
            queries.load(Ordering::SeqCst) > 0,
            time_status == StatusCode::OK
        );
        if time_status != StatusCode::OK {
            assert!(format!("{:#}", result.as_ref().unwrap_err()).contains("同步交易所时间失败"));
        }
        if confirmed_open {
            assert!(state.global_position_ready.load(Ordering::SeqCst));
            assert_eq!(*other.status.read().await, BotStatus::Running);
            assert_eq!(
                state.position.read().await.size.abs(),
                rust_decimal_macros::dec!(1)
            );
        } else if !allowed {
            assert!(!state.global_position_ready.load(Ordering::SeqCst));
            assert_eq!(*other.status.read().await, BotStatus::Paused);
        }
        assert!(!queried_early.load(Ordering::SeqCst));
        let members = runtime.handle.states.read().await.clone();
        if allowed {
            assert_eq!(members.len(), 1);
            assert!(Arc::ptr_eq(&members[0], &other));
            let saved = runtime.handle.db.load_config().unwrap().unwrap();
            assert_eq!(saved.pairs.len(), 1);
            assert_eq!(saved.pairs[0].symbol, "BTCUSDT");
        } else {
            assert_eq!(
                serde_json::to_value(runtime.handle.db.load_config().unwrap().unwrap()).unwrap(),
                saved
            );
            assert_eq!(members.len(), 2);
            let replacement = runtime.states()[0].clone();
            assert!(!Arc::ptr_eq(&state, &replacement));
            assert!(Arc::ptr_eq(&members[0], &replacement));
            assert!(Arc::ptr_eq(&members[1], &other));
            assert!(state.is_shutting_down());
            assert!(!replacement.is_shutting_down());
            assert_eq!(*replacement.status.read().await, BotStatus::Paused);
            assert!(!runtime.pairs[0].engine.is_finished());
            if confirmed_open {
                // The replacement's time request is held by the mock exchange, so
                // its initialization cannot refresh the inherited snapshots yet.
                assert!(!replacement
                    .global_position_initialized
                    .load(Ordering::SeqCst));
                assert_eq!(*other.status.read().await, BotStatus::Running);
                let mut risk = GlobalConfig::from_app(&*state.config.read().await);
                risk.global_max_position_notional = Some(rust_decimal_macros::dec!(50));
                risk.pause_on_zero_available_margin = false;
                runtime
                    .global_position_limit
                    .initialize_config(risk.clone());
                assert!(runtime.global_position_limit.check().await);
                assert_eq!(*other.status.read().await, BotStatus::Paused);
                assert!(other.recent_logs.read().await.iter().any(|entry| {
                    entry.message.contains("全局持仓名义价值") && entry.message.contains("99")
                }));

                // Clear the position limit and resume the other member, then
                // exercise the inherited account snapshot through margin risk.
                risk.global_max_position_notional = None;
                runtime
                    .global_position_limit
                    .initialize_config(risk.clone());
                assert!(!runtime.global_position_limit.check().await);
                // The fixture engine ignores risk cancellations, so acknowledge
                // the cancellation before manually resuming the strategy.
                other.global_cancel_pending.store(false, Ordering::SeqCst);
                other.resume_trading().await.unwrap();
                risk.pause_on_zero_available_margin = true;
                runtime.global_position_limit.initialize_config(risk);
                assert!(runtime.global_position_limit.check().await);
                assert_eq!(*other.status.read().await, BotStatus::Paused);
                assert!(other
                    .recent_logs
                    .read()
                    .await
                    .iter()
                    .any(|entry| { entry.message.contains("可用保证金已达到零") }));
                assert!(!replacement
                    .global_position_initialized
                    .load(Ordering::SeqCst));
            }
        }
        server.abort();
        let _ = server.await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn pair_state_restore_does_not_block_runtime_while_database_is_busy() {
    let mut runtime = fixture();
    runtime.test_exchange_url = Some("http://127.0.0.1:1".into());
    let db = runtime.handle.db.clone();
    let mut config = AppConfig::default();
    config.exchange.symbol = "ETHUSDT".into();
    let (reached_tx, reached_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    db.set_membership_commit_hook("add", reached_tx, resume_rx);
    let write_db = db.clone();
    let write_config = config.clone();
    let writer = tokio::task::spawn_blocking(move || write_db.add_pair_config(&write_config));
    reached_rx.await.unwrap();

    // An independent thread always releases the SQLite lock, including when
    // a synchronous restore blocks this runtime's only async worker.
    let (progress_tx, progress_rx) = std::sync::mpsc::channel();
    let releaser = std::thread::spawn(move || {
        let progressed = progress_rx.recv_timeout(Duration::from_secs(3)).is_ok();
        resume_tx.send(()).unwrap();
        progressed
    });
    let (start_tx, start_rx) = oneshot::channel();
    let pair = {
        let preparing = runtime.prepare_pair_with_state(
            config,
            PreparePairOptions {
                multi: true,
                prearmed: Some(false),
                start: Some(start_rx),
                ..Default::default()
            },
        );
        tokio::pin!(preparing);
        let completed = tokio::select! {
            biased;
            result = &mut preparing => Some(result),
            _ = tokio::task::yield_now() => None,
        };
        let _ = progress_tx.send(());
        match completed {
            Some(result) => result.unwrap(),
            None => preparing.await.unwrap(),
        }
    };
    writer.await.unwrap().unwrap();
    let progressed_while_locked = releaser.join().unwrap();
    pair.engine.abort();
    for task in &pair.background {
        task.abort();
    }
    let status = *pair.state.status.read().await;
    drop(start_tx);
    assert!(
        progressed_while_locked,
        "state restoration blocked the runtime while another transaction held SQLite"
    );
    assert_eq!(status, BotStatus::Paused);
}

#[tokio::test]
async fn removal_preserves_at_least_one_pair_and_requires_confirmed_empty_orders() {
    let mut runtime = fixture();
    let state = runtime.states()[0].clone();
    state.pause_trading().await.unwrap();
    let order = crate::types::GridOrder {
        client_order_id: "gb_b_removal".into(),
        symbol: "SOLUSDC".into(),
        order_id: None,
        side: crate::types::OrderSide::Buy,
        price: rust_decimal_macros::dec!(100),
        quantity: rust_decimal_macros::dec!(1),
        amount_usdc: rust_decimal_macros::dec!(100),
        status: crate::types::OrderStatus::New,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        grid_level: 1,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: crate::types::OrderPurpose::Grid,
        merge_sources: vec![],
    };
    state
        .active_orders
        .write()
        .await
        .insert(order.client_order_id.clone(), order);
    assert!(runtime
        .remove_pair("SOLUSDC")
        .await
        .unwrap_err()
        .to_string()
        .contains("仍有挂单"));
    assert!(!state.is_shutting_down());
    state.active_orders.write().await.clear();
    runtime.remove_pair("SOLUSDC").await.unwrap();
    assert!(runtime
        .remove_pair("BTCUSDT")
        .await
        .unwrap_err()
        .to_string()
        .contains("至少保留"));
    assert_eq!(
        runtime
            .handle
            .db
            .load_config()
            .unwrap()
            .unwrap()
            .pairs
            .len(),
        1
    );
}

#[tokio::test]
async fn paper_removal_rejects_durable_saved_position_when_memory_was_flat_and_restores_memory() {
    for size in [
        rust_decimal_macros::dec!(2.5),
        rust_decimal_macros::dec!(-2.5),
    ] {
        let mut runtime = fixture();
        let state = runtime.states()[0].clone();
        let (account, mut position) = seed_paper_account_and_position(&state).await;
        position.size = size;
        let fill = crate::types::TradeRecord {
            trade_id: format!("durable-paper-{size}"),
            client_order_id: format!("gb_b_durable_{size}"),
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Paper,
            side: crate::types::OrderSide::Buy,
            price: rust_decimal_macros::dec!(100),
            quantity: size.abs(),
            amount_usdc: rust_decimal_macros::dec!(250),
            realized_pnl: rust_decimal::Decimal::ZERO,
            commission: rust_decimal::Decimal::ZERO,
            pnl_verified: true,
            is_maker: true,
            timestamp: chrono::Utc::now(),
            note: String::new(),
        };
        assert!(
            state
                .record_trade_with_paper_state(
                    fill,
                    None,
                    None,
                    None,
                    Some((position.clone(), account.clone())),
                )
                .await
        );
        *state.position.write().await = crate::types::PositionInfo::default();
        *state.account.write().await = crate::types::AccountInfo::initial_paper("SOLUSDC", None);
        assert!(state.position.read().await.size.is_zero());
        state.pause_trading().await.unwrap();

        let error = runtime.remove_pair("SOLUSDC").await.unwrap_err();
        assert!(error.to_string().contains("仍有持仓"));
        assert_eq!(runtime.states().len(), 2);
        assert_paper_account_and_position_preserved(&state, &account, &position).await;
        let replacement = runtime.states()[0].clone();
        assert!(!Arc::ptr_eq(&state, &replacement));
        assert_paper_account_and_position_preserved(&replacement, &account, &position).await;
    }
}
