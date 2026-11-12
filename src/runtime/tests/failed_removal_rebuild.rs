use super::super::*;
use super::{
    assert_paper_account_and_position_preserved, fixture, fixture_with_database,
    fixture_with_shutdown_failure, request, seed_paper_account_and_position,
};
use crate::config::GlobalConfig;
use crate::db::Database;
use crate::server::create_managed_router;
use crate::types::{BotControlAction, BotStatus, TradingMode};
use axum::http::StatusCode;
use serde_json::Value;

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
            commission_asset: None,
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
    let dir = crate::test_support::TempTestDir::new("remove-commit-failure");
    let path = dir.join("bot.db");
    let db = Arc::new(Database::open(&path).unwrap());
    let mut runtime = fixture_with_database(db.clone(), false, false);
    runtime.test_exchange_url = Some("http://127.0.0.1:1".into());
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
    runtime.shutdown(Duration::from_secs(5)).await.unwrap();
    drop(runtime);
    drop(replacement);
    drop(state);
    drop(db);
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
    let dir = crate::test_support::TempTestDir::new("remove-pause-failure");
    let path = dir.join("bot.db");
    let db = Arc::new(Database::open(&path).unwrap());
    let mut runtime = fixture_with_database(db.clone(), false, false);
    runtime.test_exchange_url = Some("http://127.0.0.1:1".into());
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
    let _ = runtime.shutdown(Duration::from_secs(5)).await;
    drop(runtime);
    let config = db.load_config().unwrap().unwrap();
    let reopened = Arc::new(Database::open(&path).unwrap());
    let mut restarted = TradingRuntime::start_with_test_exchange(
        &config,
        reopened.clone(),
        "http://127.0.0.1:1".into(),
    )
    .await
    .unwrap();
    assert_eq!(
        *restarted.states()[0].status.read().await,
        BotStatus::Paused
    );
    let _ = restarted.shutdown(Duration::from_secs(5)).await;
    drop(restarted);
    drop(reopened);
    drop(replacement);
    drop(state);
    drop(db);
}

#[tokio::test]
async fn failed_removal_and_rebuild_keeps_member_in_global_risk() {
    let dir = crate::test_support::TempTestDir::new("remove-rebuild-failure");
    let path = dir.join("bot.db");
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
    let router = create_managed_router(handle.clone());
    let manager = tokio::spawn(async move { runtime.run().await });
    let error = tokio::time::timeout(Duration::from_secs(2), handle.remove_pair("SOLUSDC".into()))
        .await
        .unwrap()
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("删除失败后无法重启策略")
            && !format!("{error:#}").contains("simulated guard failure"),
        "{error:#}"
    );
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
    let (unscoped_status, unscoped_body) =
        request(&router, "/api/status", "GET", Value::Null, true).await;
    assert_eq!(unscoped_status, StatusCode::OK);
    assert_eq!(unscoped_body["data"]["symbol"], "BTCUSDT");
    let (retired_cfg_status, _) = request(
        &router,
        "/api/pairs/SOLUSDC/config",
        "POST",
        super::payload("SOLUSDC"),
        true,
    )
    .await;
    assert_eq!(retired_cfg_status, StatusCode::CONFLICT);
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
}
