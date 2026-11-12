use super::super::*;
use super::cache_test_trade;
use crate::types::{OrderSide, TradingMode};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::time::Duration;

#[tokio::test]
async fn cancelled_paper_fill_finishes_publishing_before_retry_checks_history() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, _rx) = mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db.clone(), tx);
    let trade = cache_test_trade("cancel-paper-fill", 1, TradingMode::Paper);
    let position = PositionInfo {
        size: dec!(1),
        entry_price: dec!(100),
        ..Default::default()
    };
    let mut account = state.account.read().await.clone();
    account.available_balance = dec!(9900);
    let position_guard = state.position.write().await;
    let revision = db.trade_data_revision();
    let committing_state = state.clone();
    let committing_trade = trade.clone();
    let commit = tokio::spawn(async move {
        committing_state
            .record_trade_with_paper_state(
                committing_trade,
                None,
                None,
                None,
                Some((position, account)),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while db.trade_data_revision() == revision {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    commit.abort();
    assert!(commit.await.unwrap_err().is_cancelled());
    assert!(state.trade_commit_lock.try_lock().is_err());
    let retry_state = state.clone();
    let retry_id = trade.client_order_id.clone();
    let retry = tokio::spawn(async move {
        retry_state
            .has_recorded_paper_order(retry_id, TradingMode::Paper)
            .await
            .unwrap()
    });
    drop(position_guard);
    assert!(tokio::time::timeout(Duration::from_secs(5), retry)
        .await
        .unwrap()
        .unwrap());
    assert_eq!(state.position.read().await.size, dec!(1));
    assert_eq!(state.position.read().await.entry_price, dec!(100));
    assert_eq!(state.account.read().await.available_balance, dec!(9900));
    assert_eq!(state.stats.read().await.total_trades, 1);
    assert_eq!(db.get_recent_trades(10).unwrap().len(), 1);
}

#[tokio::test]
async fn cancelled_cleanup_finishes_all_cache_resets_before_releasing_commit_lock() {
    for cleanup_kind in 0..3 {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let lock = Arc::new(Mutex::new(()));
        let mut config = AppConfig::default();
        config.exchange.symbol = "SOLUSDC".into();
        let (tx, _rx) = mpsc::channel(1);
        let first = AppState::new_for_pair(config.clone(), db.clone(), tx.clone(), lock.clone());
        config.exchange.symbol = "BTCUSDC".into();
        let second = AppState::new_for_pair(config, db.clone(), tx, lock);
        let trade = cache_test_trade("cancel-cleanup", 1, TradingMode::Paper);
        assert!(db.insert_trade(&trade).unwrap());
        first.stats.write().await.total_trades = 1;
        first.recent_trades.write().await.push_back(trade);
        second.stats.write().await.total_trades = 2;
        let stats_guard = first.stats.write().await;
        let revision = db.trade_data_revision();
        let states = vec![first.clone(), second.clone()];
        let cleanup = tokio::spawn(async move {
            match cleanup_kind {
                0 => {
                    states[0]
                        .clear_symbol_trade_history("SOLUSDC".into(), None)
                        .await
                        .unwrap();
                }
                1 => {
                    states[0].clear_trade_history().await.unwrap();
                }
                _ => {
                    AppState::clear_selected_database_for(&states, true, false, None)
                        .await
                        .unwrap();
                }
            }
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while db.trade_data_revision() == revision {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        cleanup.abort();
        assert!(cleanup.await.unwrap_err().is_cancelled());
        assert!(first.trade_commit_lock.try_lock().is_err());
        drop(stats_guard);
        let _commit = tokio::time::timeout(Duration::from_secs(5), first.trade_commit_lock.lock())
            .await
            .unwrap();
        assert!(db.get_recent_trades(10).unwrap().is_empty());
        assert_eq!(first.stats.read().await.total_trades, 0);
        assert!(first.recent_trades.read().await.is_empty());
        assert_eq!(
            second.stats.read().await.total_trades,
            if cleanup_kind == 2 { 0 } else { 2 }
        );
    }
}

#[tokio::test]
async fn clearing_previous_symbol_preserves_current_symbol_cache() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let mut config = AppConfig::default();
    config.exchange.symbol = "BTCUSDC".into();
    let (tx, _rx) = mpsc::channel(1);
    let state = AppState::new(config, db.clone(), tx);
    let old = cache_test_trade("old-symbol", 1, TradingMode::Paper);
    let mut current = cache_test_trade("current-symbol", 2, TradingMode::Paper);
    current.symbol = "BTCUSDC".into();
    assert!(db.insert_trade(&old).unwrap());
    assert!(db.insert_trade(&current).unwrap());
    state.stats.write().await.total_trades = 1;
    state.recent_trades.write().await.push_back(current);
    assert_eq!(
        state
            .clear_symbol_trade_history("SOLUSDC".into(), None)
            .await
            .unwrap()
            .0,
        1
    );
    assert_eq!(state.stats.read().await.total_trades, 1);
    assert_eq!(
        state.recent_trades.read().await[0].trade_id,
        "current-symbol"
    );
    assert_eq!(db.get_recent_trades(10).unwrap()[0].symbol, "BTCUSDC");
}

#[tokio::test]
async fn recent_trade_cache_keeps_latest_after_out_of_order_backfill_and_reload() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (action_tx, _) = mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db.clone(), action_tx);
    // A permutation of 0..260 forces old and new fills to arrive in mixed order.
    for i in 0..260 {
        let timestamp = (i * 73) % 260;
        let trade = cache_test_trade(&format!("paper-{timestamp}"), timestamp, TradingMode::Paper);
        assert!(state.record_trade(trade, None, None, None).await);
    }
    // Equal timestamps use insertion order, including at the retention boundary.
    for id in ["tie-first", "tie-second"] {
        assert!(
            state
                .record_trade(
                    cache_test_trade(id, 200, TradingMode::Paper),
                    None,
                    None,
                    None
                )
                .await
        );
    }
    let cached = state.recent_trades.read().await.clone();
    assert_eq!(cached.len(), 200);
    assert_eq!(
        cached.front().unwrap().timestamp,
        cache_test_trade("x", 62, TradingMode::Paper).timestamp
    );
    assert!(cached
        .iter()
        .zip(cached.iter().skip(1))
        .all(|(a, b)| a.timestamp <= b.timestamp));
    let ids: Vec<_> = cached.iter().map(|t| t.trade_id.as_str()).collect();
    assert!(
        ids.iter().position(|id| *id == "tie-first").unwrap()
            < ids.iter().position(|id| *id == "tie-second").unwrap()
    );
    let snapshot = state.snapshot().await;
    let expected: Vec<_> = cached
        .iter()
        .rev()
        .take(50)
        .map(|t| t.trade_id.clone())
        .collect();
    assert_eq!(
        snapshot
            .recent_trades
            .iter()
            .map(|t| t.trade_id.clone())
            .collect::<Vec<_>>(),
        expected
    );

    let (tx, _) = mpsc::channel(1);
    let restarted = AppState::new(AppConfig::default(), db.clone(), tx);
    assert_eq!(
        restarted
            .recent_trades
            .read()
            .await
            .iter()
            .map(|t| &t.trade_id)
            .collect::<Vec<_>>(),
        cached.iter().map(|t| &t.trade_id).collect::<Vec<_>>()
    );
    for i in 0..205 {
        db.insert_trade(&cache_test_trade(
            &format!("live-{i}"),
            i,
            TradingMode::Live,
        ))
        .unwrap();
    }
    state.config.write().await.exchange.api_key = "live-key".into();
    state.config.write().await.exchange.api_secret = "live-secret".into();
    state.refresh_current_scope().await;
    let live = state.recent_trades.read().await;
    assert_eq!(live.len(), 200);
    assert_eq!(live.front().unwrap().trade_id, "live-5");
    assert_eq!(live.back().unwrap().trade_id, "live-204");
    drop(live);
    state.config.write().await.exchange.api_key.clear();
    state.config.write().await.exchange.api_secret.clear();
    state.refresh_current_scope().await;
    assert_eq!(
        state
            .recent_trades
            .read()
            .await
            .iter()
            .map(|t| t.trade_id.clone())
            .collect::<Vec<_>>(),
        cached
            .iter()
            .map(|t| t.trade_id.clone())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn canceled_verification_request_still_publishes_its_committed_result() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_config(&AppConfig::default()).unwrap();
    let (tx, _) = mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db.clone(), tx);
    let mut trade = cache_test_trade("cancel-verification", 1, TradingMode::Paper);
    trade.pnl_verified = false;
    trade.realized_pnl = Decimal::ZERO;
    trade.commission = Decimal::ZERO;
    assert!(state.record_trade(trade.clone(), None, None, None).await);
    let clear_revision = db.symbol_trade_clear_revision(&trade.symbol);
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    db.set_membership_commit_hook("add", reached_tx, resume_rx);
    let writer = tokio::task::spawn_blocking({
        let db = db.clone();
        move || {
            let mut config = AppConfig::default();
            config.exchange.symbol = "ETHUSDT".into();
            db.add_pair_config(&config)
        }
    });
    reached_rx.await.unwrap();
    trade.realized_pnl = dec!(3.25);
    trade.commission = dec!(0.02);
    trade.pnl_verified = true;
    let request = tokio::spawn({
        let state = state.clone();
        async move { state.verify_trade_pnl(trade, clear_revision).await }
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while state.trade_commit_lock.try_lock().is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    resume_tx.send(()).unwrap();
    writer.await.unwrap().unwrap();
    let _commit = tokio::time::timeout(Duration::from_secs(3), state.trade_commit_lock.lock())
        .await
        .unwrap();
    let stats = state.stats.read().await;
    assert_eq!(stats.total_realized_pnl, dec!(3.25));
    assert_eq!(stats.total_commission, dec!(0.02));
    assert_eq!(stats.pending_pnl_trades, 0);
    let recent = state.recent_trades.read().await;
    assert!(recent[0].pnl_verified);
    assert_eq!(recent[0].realized_pnl, dec!(3.25));
    assert_eq!(
        db.get_pnl_totals("SOLUSDC", TradingMode::Paper).unwrap(),
        (dec!(3.25), dec!(0.02), 0)
    );
}

#[tokio::test]
async fn canceled_verification_waiter_never_commits_after_the_lock_is_released() {
    use std::future::Future;
    use std::task::Poll;

    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, _) = mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db.clone(), tx);
    let mut trade = cache_test_trade("cancel-waiting-verification", 1, TradingMode::Paper);
    assert!(state.record_trade(trade.clone(), None, None, None).await);
    let clear_revision = db.symbol_trade_clear_revision(&trade.symbol);
    trade.realized_pnl = dec!(3.25);
    trade.commission = dec!(0.02);
    trade.pnl_verified = true;

    let commit = state.trade_commit_lock.lock().await;
    let (waiting_tx, waiting_rx) = tokio::sync::oneshot::channel();
    let request = tokio::spawn({
        let state = state.clone();
        async move {
            let verification = state.verify_trade_pnl(trade, clear_revision);
            tokio::pin!(verification);
            std::future::poll_fn(|cx| {
                assert!(verification.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            waiting_tx.send(()).unwrap();
            verification.await
        }
    });
    waiting_rx.await.unwrap();
    tokio::task::yield_now().await;
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    drop(commit);

    let _commit = tokio::time::timeout(Duration::from_secs(3), state.trade_commit_lock.lock())
        .await
        .unwrap();
    let stats = state.stats.read().await;
    assert_eq!(stats.total_realized_pnl, Decimal::ZERO);
    assert_eq!(stats.total_commission, Decimal::ZERO);
    assert_eq!(stats.pending_pnl_trades, 1);
    let recent = state.recent_trades.read().await;
    assert!(!recent[0].pnl_verified);
    assert_eq!(recent[0].realized_pnl, Decimal::ZERO);
    assert_eq!(recent[0].commission, Decimal::ZERO);
    assert_eq!(
        db.get_pnl_totals("SOLUSDC", TradingMode::Paper).unwrap(),
        (Decimal::ZERO, Decimal::ZERO, 1)
    );
}

#[tokio::test]
async fn recent_trade_verification_preserves_cached_timestamp() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx, _) = mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db, tx);
    let original = cache_test_trade("verify", 1, TradingMode::Paper);
    assert!(state.record_trade(original.clone(), None, None, None).await);
    let mut verified = original.clone();
    verified.timestamp += chrono::Duration::seconds(100);
    verified.realized_pnl = dec!(3);
    let clear_revision = state.db.symbol_trade_clear_revision(&verified.symbol);
    state
        .verify_trade_pnl(verified, clear_revision)
        .await
        .unwrap();
    let cached = state.recent_trades.read().await;
    assert_eq!(cached[0].timestamp, original.timestamp);
    assert_eq!(cached[0].realized_pnl, dec!(3));
    assert!(cached[0].pnl_verified);
}

#[tokio::test]
async fn switching_mode_reloads_only_that_modes_trades() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    for (id, mode, pnl) in [
        ("paper", TradingMode::Paper, dec!(10)),
        ("live", TradingMode::Live, dec!(-2)),
    ] {
        db.insert_trade(&TradeRecord {
            trade_id: id.into(),
            client_order_id: format!("gb_b_{id}"),
            symbol: "SOLUSDC".into(),
            mode,
            side: OrderSide::Buy,
            price: dec!(100),
            quantity: dec!(1),
            amount_usdc: dec!(100),
            realized_pnl: pnl,
            commission: Decimal::ZERO,
            pnl_verified: true,
            is_maker: true,
            timestamp: Utc::now(),
            note: String::new(),
        })
        .unwrap();
    }
    let (action_tx, _) = mpsc::channel(1);
    let state = AppState::new(AppConfig::default(), db, action_tx);
    assert_eq!(state.stats.read().await.total_realized_pnl, dec!(10));
    assert_eq!(state.recent_trades.read().await.len(), 1);
    let snapshot = state.snapshot().await;
    let hourly = snapshot.hourly_trade_stats.unwrap();
    assert_eq!(hourly.buckets.iter().map(|b| b.buy_count).sum::<u64>(), 1);

    state.config.write().await.exchange.api_key = "live-key".into();
    state.config.write().await.exchange.api_secret = "live-secret".into();
    state.refresh_current_scope().await;
    assert_eq!(state.stats.read().await.total_realized_pnl, dec!(-2));
    let trades = state.recent_trades.read().await;
    assert_eq!(trades.len(), 1);
    assert_eq!(trades[0].mode, TradingMode::Live);
    drop(trades);
    let snapshot = state.snapshot().await;
    let hourly = snapshot.hourly_trade_stats.unwrap();
    assert_eq!(hourly.buckets.iter().map(|b| b.buy_count).sum::<u64>(), 1);
}

#[tokio::test]
async fn retired_verification_is_left_for_the_replacement_state() {
    let config = AppConfig::default();
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_config(&config).unwrap();
    let commit = Arc::new(Mutex::new(()));
    let (tx, _) = mpsc::channel(1);
    let retired = AppState::new_for_pair(config.clone(), db.clone(), tx, commit.clone());
    let mut trade = cache_test_trade("retired-verification", 1, TradingMode::Paper);
    trade.pnl_verified = false;
    trade.realized_pnl = Decimal::ZERO;
    trade.commission = Decimal::ZERO;
    assert!(retired.record_trade(trade.clone(), None, None, None).await);
    let revision = db.symbol_trade_clear_revision(&trade.symbol);
    trade.realized_pnl = dec!(3.25);
    trade.commission = dec!(0.02);
    trade.pnl_verified = true;

    retired.retire();
    retired.drain_trade_commits().await;
    retired
        .verify_trade_pnl(trade.clone(), revision)
        .await
        .unwrap();
    assert_eq!(
        db.get_pnl_totals("SOLUSDC", TradingMode::Paper).unwrap(),
        (Decimal::ZERO, Decimal::ZERO, 1)
    );
    assert!(!retired.recent_trades.read().await[0].pnl_verified);

    let (tx, _) = mpsc::channel(1);
    let replacement = AppState::new_for_pair(config, db.clone(), tx, commit);
    replacement.verify_trade_pnl(trade, revision).await.unwrap();
    let stats = replacement.stats.read().await;
    assert_eq!(stats.total_realized_pnl, dec!(3.25));
    assert_eq!(stats.total_commission, dec!(0.02));
    assert_eq!(stats.pending_pnl_trades, 0);
    assert!(replacement.recent_trades.read().await[0].pnl_verified);
    assert_eq!(
        db.get_pnl_totals("SOLUSDC", TradingMode::Paper).unwrap(),
        (dec!(3.25), dec!(0.02), 0)
    );
}

#[tokio::test]
async fn record_trade_saturates_counters_and_volume_on_extreme_stats() {
    let config = AppConfig::default();
    let db = Arc::new(Database::open(":memory:").unwrap());
    db.save_config(&config).unwrap();
    let (tx, _rx) = mpsc::channel(1);
    let state = AppState::new(config, db, tx);
    {
        let mut stats = state.stats.write().await;
        stats.total_trades = usize::MAX;
        stats.completed_cycles = usize::MAX;
        stats.pending_pnl_trades = usize::MAX;
        stats.total_volume_usdc = Decimal::MAX;
    }
    let mut trade = cache_test_trade("saturating-trade", 1, TradingMode::Paper);
    trade.amount_usdc = dec!(100);
    trade.pnl_verified = false;
    assert!(state.record_trade(trade, Some(dec!(1)), None, None).await);
    let stats = state.stats.read().await;
    assert_eq!(stats.total_trades, usize::MAX);
    assert_eq!(stats.completed_cycles, usize::MAX);
    assert_eq!(stats.pending_pnl_trades, usize::MAX);
    assert_eq!(stats.total_volume_usdc, Decimal::MAX);
}

#[tokio::test]
async fn refresh_current_scope_wipes_old_mode_on_sqlite_error_and_recovers_durable_stats_on_next_fill(
) {
    let dir = crate::test_support::TempTestDir::new("state-dirty-stats");
    let db_path = dir.join("bot.db");
    let db = Arc::new(Database::open(&db_path).unwrap());
    let config = AppConfig::default();
    db.save_config(&config).unwrap();
    let (tx, _rx) = mpsc::channel(4);
    let state = AppState::new(config, db.clone(), tx);

    let mut p1 = cache_test_trade("paper-1", 1, TradingMode::Paper);
    p1.amount_usdc = dec!(100);
    p1.realized_pnl = dec!(1.5);
    p1.pnl_verified = true;
    assert!(state.record_trade(p1, None, None, None).await);

    let mut p2 = cache_test_trade("paper-2", 2, TradingMode::Paper);
    p2.side = OrderSide::Sell;
    p2.amount_usdc = dec!(100);
    p2.realized_pnl = dec!(2.5);
    p2.pnl_verified = true;
    assert!(state.record_trade(p2, Some(dec!(5)), None, None).await);
    assert_eq!(state.stats.read().await.total_trades, 2);
    assert!(!state.is_stats_dirty());

    {
        let mut cfg = state.config.write().await;
        cfg.exchange.api_key = "live-key".into();
        cfg.exchange.api_secret = "live-secret".into();
    }
    state.refresh_current_scope().await;
    assert_eq!(state.stats.read().await.total_trades, 0);
    let mut live_trade = cache_test_trade("live-1", 3, TradingMode::Live);
    live_trade.amount_usdc = dec!(500);
    live_trade.realized_pnl = dec!(99);
    assert!(
        state
            .record_trade(live_trade, Some(dec!(99)), None, None)
            .await
    );
    assert_eq!(state.stats.read().await.total_trades, 1);
    assert_eq!(state.recent_trades.read().await.len(), 1);

    {
        let mut cfg = state.config.write().await;
        cfg.exchange.api_key.clear();
        cfg.exchange.api_secret.clear();
    }
    let external = rusqlite::Connection::open(&db_path).unwrap();
    external
        .execute_batch("ALTER TABLE scoped_grid_stats RENAME TO scoped_grid_stats_bak;")
        .unwrap();

    state.refresh_current_scope().await;
    assert!(state.is_stats_dirty());
    assert!(state.recent_trades.read().await.is_empty());
    assert_eq!(state.stats.read().await.total_trades, 0);
    assert_eq!(state.stats.read().await.total_realized_pnl, Decimal::ZERO);
    assert!(state.snapshot().await.recent_trades.is_empty());

    external
        .execute_batch("ALTER TABLE scoped_grid_stats_bak RENAME TO scoped_grid_stats;")
        .unwrap();
    drop(external);

    let mut p3 = cache_test_trade("paper-3", 4, TradingMode::Paper);
    p3.side = OrderSide::Sell;
    p3.amount_usdc = dec!(100);
    p3.realized_pnl = dec!(2);
    p3.pnl_verified = true;
    assert!(state.record_trade(p3, Some(dec!(3)), None, None).await);
    assert!(!state.is_stats_dirty());

    let recovered = state.stats.read().await.clone();
    assert_eq!(recovered.total_trades, 3);
    assert_eq!(recovered.completed_cycles, 2);
    assert_eq!(recovered.total_realized_profit, dec!(8));
    assert_eq!(recovered.total_volume_usdc, dec!(300));
    assert_eq!(recovered.total_realized_pnl, dec!(6));
    assert_eq!(state.recent_trades.read().await.len(), 3);
    assert_eq!(
        db.load_scoped_stats("SOLUSDC", TradingMode::Paper).unwrap(),
        Some((3, 2, dec!(8), dec!(300)))
    );
}

#[tokio::test]
async fn post_commit_recent_trades_reload_failure_keeps_paper_state_and_stats_consistent() {
    let dir = crate::test_support::TempTestDir::new("state-post-commit-reload");
    let db_path = dir.join("bot.db");
    let db = Arc::new(Database::open(&db_path).unwrap());
    let config = AppConfig::default();
    db.save_config(&config).unwrap();
    let (tx, _rx) = mpsc::channel(4);
    let state = AppState::new(config, db.clone(), tx);

    let external = rusqlite::Connection::open(&db_path).unwrap();
    external
        .execute(
            "INSERT INTO trades (trade_id, client_order_id, symbol, mode, side, price, quantity, \
             amount_usdc, realized_pnl, commission, pnl_verified, is_maker, timestamp, note) \
             VALUES ('corrupt-1', 'gb_bad', 'SOLUSDC', 'PAPER', 'INVALID', '100', '1', '100', \
             '0', '0', 1, 1, '2026-10-01T00:00:00Z', '')",
            [],
        )
        .unwrap();

    state.refresh_current_scope().await;
    assert!(state.is_stats_dirty());

    let mut fill = cache_test_trade("paper-commit-ok", 10, TradingMode::Paper);
    fill.pnl_verified = true;
    fill.realized_pnl = dec!(4.5);
    let committed_pos = PositionInfo {
        symbol: "SOLUSDC".into(),
        size: dec!(1),
        entry_price: dec!(100),
        mark_price: dec!(100),
        leverage: 1,
        update_time: Some(Utc::now()),
        ..Default::default()
    };
    let mut committed_acc = AccountInfo::initial_paper("SOLUSDC", Some(Utc::now()));
    committed_acc.total_wallet_balance += dec!(4.5);
    committed_acc.margin_balance = committed_acc.total_wallet_balance;
    committed_acc.available_balance = committed_acc.total_wallet_balance - dec!(100);

    assert!(
        state
            .record_trade_with_paper_state(
                fill.clone(),
                Some(dec!(4.5)),
                None,
                None,
                Some((committed_pos.clone(), committed_acc.clone())),
            )
            .await
    );
    assert_eq!(*state.status.read().await, BotStatus::Running);
    assert_eq!(state.position.read().await.size, dec!(1));
    assert_eq!(
        state.account.read().await.total_wallet_balance,
        committed_acc.total_wallet_balance
    );
    assert_eq!(
        state.current_account().await,
        db.shared_paper_account("SOLUSDC", &committed_pos, &committed_acc)
            .unwrap()
    );
    let stats = state.stats.read().await.clone();
    assert_eq!(stats.total_trades, 1);
    assert_eq!(stats.completed_cycles, 1);
    assert_eq!(stats.total_realized_profit, dec!(4.5));
    assert_eq!(
        state
            .recent_trades
            .read()
            .await
            .back()
            .map(|t| t.trade_id.as_str()),
        Some("paper-commit-ok")
    );
    assert!(state.is_stats_dirty());

    external
        .execute("DELETE FROM trades WHERE trade_id = 'corrupt-1'", [])
        .unwrap();
    drop(external);
    state.refresh_current_scope().await;
    assert!(!state.is_stats_dirty());
    assert_eq!(state.recent_trades.read().await.len(), 1);
}

#[tokio::test]
async fn clear_selected_database_waits_for_distinct_state_commit_locks() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let (tx1, _rx1) = mpsc::channel(1);
    let (tx2, _rx2) = mpsc::channel(1);
    let first = AppState::new(AppConfig::default(), db.clone(), tx1);
    let mut second_cfg = AppConfig::default();
    second_cfg.exchange.symbol = "BTCUSDC".into();
    let second = AppState::new(second_cfg, db.clone(), tx2);
    assert!(!Arc::ptr_eq(
        &first.trade_commit_lock,
        &second.trade_commit_lock
    ));
    assert!(db
        .insert_trade(&cache_test_trade("distinct-lock-1", 1, TradingMode::Paper))
        .unwrap());
    second.stats.write().await.total_trades = 3;

    let second_guard = second.trade_commit_lock.lock().await;
    let states = vec![first.clone(), second.clone()];
    let mut cleanup = tokio::spawn(async move {
        AppState::clear_selected_database_for(&states, true, false, None)
            .await
            .unwrap()
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut cleanup)
            .await
            .is_err()
    );
    drop(second_guard);
    let (deleted_trades, _) = tokio::time::timeout(Duration::from_secs(3), cleanup)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(deleted_trades, 1);
    assert_eq!(second.stats.read().await.total_trades, 0);
}
