use super::super::*;
use super::{fixture, fixture_with_database, fixture_with_shutdown_failure, request};
use crate::config::GlobalConfig;
use crate::db::Database;
use crate::server::create_managed_router;
use crate::types::{BotControlAction, BotStatus, TradingMode};
use axum::http::StatusCode;
use axum::Router;
use serde_json::{json, Value};

async fn seed_paper_account_and_position(
    state: &AppState,
) -> (crate::types::AccountInfo, crate::types::PositionInfo) {
    assert!(state.config.read().await.exchange.is_paper());
    let account = {
        let mut account = state.account.write().await;
        account.asset = "USDC".into();
        account.total_wallet_balance = rust_decimal_macros::dec!(9123.45);
        account.available_balance = rust_decimal_macros::dec!(8765.43);
        account.margin_balance = rust_decimal_macros::dec!(9135.45);
        account.unrealized_profit = rust_decimal_macros::dec!(12);
        account.clone()
    };
    let position = {
        let mut position = state.position.write().await;
        position.symbol = "SOLUSDC".into();
        position.size = rust_decimal_macros::dec!(2.5);
        position.entry_price = rust_decimal_macros::dec!(100);
        position.mark_price = rust_decimal_macros::dec!(104.8);
        position.unrealized_pnl = rust_decimal_macros::dec!(12);
        position.liquidation_price = rust_decimal_macros::dec!(70);
        position.leverage = 10;
        position.clone()
    };
    (account, position)
}

async fn assert_paper_account_and_position_preserved(
    state: &AppState,
    account: &crate::types::AccountInfo,
    position: &crate::types::PositionInfo,
) {
    assert_eq!(
        serde_json::to_value(&*state.account.read().await).unwrap(),
        serde_json::to_value(account).unwrap()
    );
    assert_eq!(
        serde_json::to_value(&*state.position.read().await).unwrap(),
        serde_json::to_value(position).unwrap()
    );
}
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
async fn failed_removal_keeps_configuration_and_restarts_paused_engine() {
    let mut runtime = fixture_with_shutdown_failure(true);
    let state = runtime.states()[0].clone();
    let (account, position) = seed_paper_account_and_position(&state).await;
    let mut retirement = state.subscribe_retirement();
    assert!(!*retirement.borrow());
    let other_state = runtime.states()[1].clone();
    let other_retirement = other_state.subscribe_retirement();
    state.pause_trading().await.unwrap();
    assert!(runtime
        .remove_pair("SOLUSDC")
        .await
        .unwrap_err()
        .to_string()
        .contains("撤单结果仍未确认"));
    assert_eq!(
        runtime
            .handle
            .db
            .load_config()
            .unwrap()
            .unwrap()
            .pairs
            .len(),
        2
    );
    assert_eq!(runtime.handle.states.read().await.len(), 2);
    assert!(state.is_shutting_down());
    let replacement = runtime.states()[0].clone();
    assert!(!Arc::ptr_eq(&state, &replacement));
    tokio::time::timeout(Duration::from_secs(2), retirement.changed())
        .await
        .expect("旧 state 应通知订阅者退休")
        .unwrap();
    assert!(*retirement.borrow());
    assert!(!*replacement.subscribe_retirement().borrow());
    assert!(!*other_retirement.borrow());
    assert_paper_account_and_position_preserved(&replacement, &account, &position).await;
    assert!(!replacement.is_shutting_down());
    assert_eq!(*replacement.status.read().await, BotStatus::Paused);
    assert!(!runtime.pairs[0].engine.is_finished());
}

#[tokio::test]
async fn failed_removal_drains_old_verification_before_restoring_replacement() {
    use crate::types::{OrderSide, TradeRecord};
    use rust_decimal_macros::dec;

    for shared_lock in [true, false] {
        let mut runtime = fixture();
        runtime.test_exchange_url = Some("http://127.0.0.1:1".into());
        let old = runtime.pairs.remove(0);
        let config = old.state.config.read().await.clone();
        old.engine.abort();
        let _ = old.engine.await;
        let old_lock = if shared_lock {
            runtime.trade_lock.clone()
        } else {
            Arc::new(Mutex::new(()))
        };
        assert_eq!(Arc::ptr_eq(&old_lock, &runtime.trade_lock), shared_lock);
        let db = runtime.handle.db.clone();
        let (tx, mut rx) = mpsc::channel(8);
        let state = AppState::new_for_pair(config, db.clone(), tx, old_lock.clone());
        state.pause_trading().await.unwrap();
        let engine = tokio::spawn(async move {
            if let Some(BotControlAction::Shutdown { reply, .. }) = rx.recv().await {
                let _ = reply.send(Err("模拟停机失败".into()));
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

        let mut trade = TradeRecord {
            trade_id: "retiring-verification".into(),
            client_order_id: "gb_b_retiring_verification".into(),
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Paper,
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(1),
            amount_usdc: dec!(100),
            realized_pnl: dec!(0),
            commission: dec!(0),
            pnl_verified: false,
            is_maker: true,
            timestamp: chrono::Utc::now(),
            note: String::new(),
        };
        assert!(state.record_trade(trade.clone(), None, None, None).await);
        let clear_revision = db.symbol_trade_clear_revision(&trade.symbol);
        trade.realized_pnl = dec!(3.25);
        trade.commission = dec!(0.02);
        trade.pnl_verified = true;

        // Keep publication in flight after SQLite commits. Removal must cancel
        // this outer worker, then await the detached publication on the old lock.
        let stats_guard = state.stats.read().await;
        let (outcome_tx, outcome_rx) = oneshot::channel();
        runtime.pairs[0].background.push(tokio::spawn({
            let state = state.clone();
            let trade = trade.clone();
            async move {
                let result = state.verify_trade_pnl(trade, clear_revision).await;
                let _ = outcome_tx.send(result);
            }
        }));
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let totals = db
                    .run_blocking(|db| db.get_pnl_totals("SOLUSDC", TradingMode::Paper))
                    .await
                    .unwrap();
                if totals == (dec!(3.25), dec!(0.02), 0) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("旧实例的验证应已持锁并提交数据库");
        assert!(old_lock.try_lock().is_err());
        assert_eq!(stats_guard.pending_pnl_trades, 1);
        let mut removal = tokio::spawn(async move {
            let result = runtime.remove_pair("SOLUSDC").await;
            (runtime, result)
        });
        assert!(tokio::time::timeout(Duration::from_secs(3), outcome_rx)
            .await
            .expect("删除应先停止旧引擎并取消外层 PnL worker")
            .is_err());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut removal)
                .await
                .is_err(),
            "替换实例必须等旧实例的验证发布结束"
        );
        drop(stats_guard);

        let (runtime, result) = tokio::time::timeout(Duration::from_secs(3), removal)
            .await
            .expect("发布完成后应释放旧锁并完成替换初始化")
            .unwrap();
        assert!(result.unwrap_err().to_string().contains("模拟停机失败"));
        let replacement = runtime.states()[0].clone();
        assert!(!Arc::ptr_eq(&state, &replacement));
        let stats = replacement.stats.read().await;
        assert_eq!(stats.total_realized_pnl, dec!(3.25));
        assert_eq!(stats.total_commission, dec!(0.02));
        assert_eq!(stats.pending_pnl_trades, 0);
        let recent = replacement.recent_trades.read().await;
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].trade_id, trade.trade_id);
        assert!(recent[0].pnl_verified);
        assert_eq!(recent[0].realized_pnl, trade.realized_pnl);
        assert_eq!(recent[0].commission, trade.commission);
        assert_eq!(recent[0].timestamp, trade.timestamp);
        assert_eq!(
            db.get_pnl_totals("SOLUSDC", TradingMode::Paper).unwrap(),
            (dec!(3.25), dec!(0.02), 0)
        );
    }
}

#[tokio::test]
async fn failed_removal_persistence_restarts_paused_engine() {
    let path =
        std::env::temp_dir().join(format!("remove-commit-failure-{}.db", uuid::Uuid::new_v4()));
    let db = Arc::new(Database::open(&path).unwrap());
    let mut runtime = fixture_with_database(db.clone(), false, false);
    let state = runtime.states()[0].clone();
    let (account, mut position) = seed_paper_account_and_position(&state).await;
    position.size = rust_decimal::Decimal::ZERO;
    state.position.write().await.size = position.size;
    let mut retirement = state.subscribe_retirement();
    assert!(!*retirement.borrow());
    state.pause_trading().await.unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_remove BEFORE UPDATE ON app_config BEGIN SELECT RAISE(ABORT, 'simulated removal failure'); END;").unwrap();
    drop(connection);
    assert!(runtime.remove_pair("SOLUSDC").await.is_err());
    assert_eq!(db.load_config().unwrap().unwrap().pairs.len(), 2);
    let replacement = runtime.states()[0].clone();
    assert!(!Arc::ptr_eq(&state, &replacement));
    tokio::time::timeout(Duration::from_secs(2), retirement.changed())
        .await
        .expect("旧 state 应通知订阅者退休")
        .unwrap();
    assert!(*retirement.borrow());
    assert!(!*replacement.subscribe_retirement().borrow());
    assert_paper_account_and_position_preserved(&replacement, &account, &position).await;
    assert!(!replacement.is_shutting_down());
    assert_eq!(*replacement.status.read().await, BotStatus::Paused);
    assert!(!runtime.pairs[0].engine.is_finished());
    drop(runtime);
    drop(replacement);
    drop(state);
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn failed_removal_rebuilds_with_configuration_committed_during_shutdown() {
    let mut runtime = fixture();
    let old = runtime.pairs.remove(0);
    old.engine.abort();
    let config = old.state.config.read().await.clone();
    let (tx, mut rx) = mpsc::channel(8);
    let state = AppState::new_for_pair(
        config,
        runtime.handle.db.clone(),
        tx,
        runtime.trade_lock.clone(),
    );
    state.pause_trading().await.unwrap();
    let engine = tokio::spawn(async move {
        if let Some(BotControlAction::Shutdown { reply, .. }) = rx.recv().await {
            let _ = reply.send(Err("模拟停机失败".into()));
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

    let mut updated = state.config.read().await.clone();
    updated.grid.grid_interval = rust_decimal_macros::dec!(12.5);
    // The writer has already acquired the status lock and committed to SQLite,
    // but cannot publish its configuration until this read guard is released.
    let config_guard = state.config.read().await;
    let writer_state = state.clone();
    let writer = tokio::spawn(async move { writer_state.save_runtime_config(updated).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let saved = runtime.handle.db.load_config().unwrap().unwrap();
            if saved.strategy_configs().unwrap()[0].grid.grid_interval
                == rust_decimal_macros::dec!(12.5)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("配置写入应在退休前提交");
    let mut retirement = state.subscribe_retirement();
    let removal = async move {
        let result = runtime.remove_pair("SOLUSDC").await;
        (runtime, result)
    };
    let removal = tokio::spawn(removal);
    tokio::time::timeout(Duration::from_secs(2), retirement.changed())
        .await
        .expect("删除应在配置发布前退休旧实例")
        .unwrap();
    drop(config_guard);
    writer.await.unwrap().unwrap();
    let (runtime, result) = removal.await.unwrap();
    assert!(result.is_err());
    let replacement = runtime.states()[0].clone();
    assert!(!Arc::ptr_eq(&state, &replacement));
    assert_eq!(
        replacement.config.read().await.grid.grid_interval,
        rust_decimal_macros::dec!(12.5)
    );
    assert_eq!(
        runtime
            .handle
            .db
            .load_config()
            .unwrap()
            .unwrap()
            .strategy_configs()
            .unwrap()[0]
            .grid
            .grid_interval,
        rust_decimal_macros::dec!(12.5)
    );
    assert_eq!(*replacement.status.read().await, BotStatus::Paused);
}

#[tokio::test]
async fn failed_removal_with_unpersisted_pause_rebuilds_paused() {
    let path =
        std::env::temp_dir().join(format!("remove-pause-failure-{}.db", uuid::Uuid::new_v4()));
    let db = Arc::new(Database::open(&path).unwrap());
    let mut runtime = fixture_with_database(db.clone(), false, false);
    let state = runtime.states()[0].clone();
    state.pause_trading().await.unwrap();
    db.save_pair_status("SOLUSDC", TradingMode::Paper, BotStatus::Running)
        .unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_pause_insert BEFORE INSERT ON pair_runtime WHEN NEW.symbol = 'SOLUSDC' BEGIN SELECT RAISE(ABORT, 'simulated pause failure'); END; CREATE TRIGGER reject_pause_update BEFORE UPDATE ON pair_runtime WHEN NEW.symbol = 'SOLUSDC' BEGIN SELECT RAISE(ABORT, 'simulated pause failure'); END;").unwrap();
    drop(connection);

    assert!(runtime.remove_pair("SOLUSDC").await.is_err());
    let replacement = runtime.states()[0].clone();
    assert!(!Arc::ptr_eq(&state, &replacement));
    assert_eq!(
        db.load_pair_status("SOLUSDC", TradingMode::Paper).unwrap(),
        Some(BotStatus::Running)
    );
    assert_eq!(*replacement.status.read().await, BotStatus::Paused);
    assert!(!replacement.is_shutting_down());
    assert!(!runtime.pairs[0].engine.is_finished());
    drop(runtime);
    let config = db.load_config().unwrap().unwrap();
    let reopened = Arc::new(Database::open(&path).unwrap());
    let restarted =
        TradingRuntime::start_with_test_exchange(&config, reopened, "http://127.0.0.1:1".into())
            .await
            .unwrap();
    assert_eq!(
        *restarted.states()[0].status.read().await,
        BotStatus::Paused
    );
    drop(restarted);
    drop(replacement);
    drop(state);
    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn failed_removal_and_rebuild_keeps_member_in_global_risk() {
    let path = std::env::temp_dir().join(format!(
        "remove-rebuild-failure-{}.db",
        uuid::Uuid::new_v4()
    ));
    let db = Arc::new(Database::open(&path).unwrap());
    let mut runtime = fixture_with_database(db.clone(), true, false);
    let state = runtime.states()[0].clone();
    let other = runtime.states()[1].clone();
    let mut global = GlobalConfig::from_app(&*state.config.read().await);
    global.global_max_position_notional = Some(rust_decimal_macros::dec!(100));
    runtime.global_position_limit.initialize_config(global);
    runtime
        .global_position_limit
        .membership_ready
        .store(true, std::sync::atomic::Ordering::SeqCst);
    for member in [&state, &other] {
        member
            .global_position_initialized
            .store(true, std::sync::atomic::Ordering::SeqCst);
        member
            .global_position_ready
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = member
            .global_position_limit
            .set(Arc::downgrade(&runtime.global_position_limit));
    }
    state.position.write().await.size = rust_decimal_macros::dec!(1);
    state.ticker.write().await.mark_price = rust_decimal_macros::dec!(200);
    state.ticker.write().await.mark_update_time = chrono::Utc::now();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_guard_insert BEFORE INSERT ON pair_execution_guard \
             BEGIN SELECT RAISE(ABORT, 'simulated guard failure'); END; \
             CREATE TRIGGER reject_guard_update BEFORE UPDATE ON pair_execution_guard \
             BEGIN SELECT RAISE(ABORT, 'simulated guard failure'); END;",
        )
        .unwrap();
    drop(connection);
    state.pause_trading().await.unwrap();

    let handle = runtime.handle();
    let global_limit = runtime.global_position_limit.clone();
    let other_engine = runtime.pairs[1].engine.abort_handle();
    let router = create_managed_router(handle.clone()).await;
    let manager = tokio::spawn(async move { runtime.run().await });
    let error = tokio::time::timeout(Duration::from_secs(2), handle.remove_pair("SOLUSDC".into()))
        .await
        .unwrap()
        .unwrap_err();
    assert!(format!("{error:#}").contains("删除失败后无法重启策略"));
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch("DROP TRIGGER reject_guard_insert; DROP TRIGGER reject_guard_update;")
        .unwrap();
    drop(connection);
    assert_eq!(db.load_config().unwrap().unwrap().pairs.len(), 2);
    assert!(state.is_shutting_down());
    assert!(*state.subscribe_retirement().borrow());
    let published = handle.states.read().await.clone();
    assert_eq!(published.len(), 2);
    assert!(Arc::ptr_eq(&published[0], &state));
    assert!(Arc::ptr_eq(&published[1], &other));
    assert!(!state
        .global_position_ready
        .load(std::sync::atomic::Ordering::SeqCst));
    assert!(global_limit.check().await);
    assert!(other.try_order_submission().is_none());
    assert!(other.resume_trading().await.is_err());
    assert!(!other.is_shutting_down());
    assert!(!*other.subscribe_retirement().borrow());
    let updated = tokio::time::timeout(
        Duration::from_secs(2),
        handle.update_global_config(crate::types::UpdateGlobalConfigPayload {
            recv_window: Some(9000),
            global_max_position_notional: Some(Some(rust_decimal_macros::dec!(100))),
            ..Default::default()
        }),
    )
    .await
    .expect("隔离成员不能阻止全局配置更新")
    .unwrap();
    assert_eq!(updated.recv_window, 9000);
    assert_eq!(
        db.load_config().unwrap().unwrap().exchange.recv_window,
        9000
    );
    assert_eq!(other.config.read().await.exchange.recv_window, 9000);
    assert_eq!(state.config.read().await.exchange.recv_window, 5000);
    let members = handle.states.read().await.clone();
    assert_eq!(members.len(), 2);
    assert!(Arc::ptr_eq(&members[0], &state));
    assert!(global_limit.check().await);
    assert!(other.try_order_submission().is_none());
    assert!(other.resume_trading().await.is_err());
    drop(members);
    let (status, body) = request(
        &router,
        "/api/pairs/BTCUSDT/status",
        "GET",
        Value::Null,
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["success"], true);
    let error = tokio::time::timeout(Duration::from_secs(2), handle.remove_pair("UNKNOWN".into()))
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("交易对不存在"));
    let error = tokio::time::timeout(Duration::from_secs(2), handle.remove_pair("SOLUSDC".into()))
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("策略重建失败"));
    other.pause_trading().await.unwrap();
    assert_eq!(*other.status.read().await, BotStatus::Paused);
    other_engine.abort();
    let reason = tokio::time::timeout(Duration::from_secs(2), manager)
        .await
        .unwrap()
        .unwrap();
    assert!(reason.contains("BTCUSDT"));
    drop(published);
    drop(router);
    drop(handle);
    drop(other);
    drop(state);
    drop(db);
    std::fs::remove_file(path).unwrap();
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
