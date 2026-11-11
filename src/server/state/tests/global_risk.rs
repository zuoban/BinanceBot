use super::super::*;
use super::notification_fixture;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::atomic::Ordering;
use std::time::Duration;

#[tokio::test]
async fn global_position_limit_aggregates_and_blocks_resume_until_below_limit() {
    let first = notification_fixture(false, false).await;
    let second = notification_fixture(false, false).await;
    first
        .state
        .config
        .write()
        .await
        .global_max_position_notional = Some(dec!(100));
    second
        .state
        .config
        .write()
        .await
        .global_max_position_notional = Some(dec!(100));
    first.state.position.write().await.size = dec!(-2);
    first.state.position.write().await.mark_price = dec!(20);
    first.state.ticker.write().await.mark_price = dec!(30);
    first.state.ticker.write().await.mark_update_time = Utc::now();
    second.state.position.write().await.size = dec!(2);
    second.state.ticker.write().await.last_price = dec!(20);
    second.state.ticker.write().await.update_time = Utc::now();
    let states = Arc::new(RwLock::new(vec![first.state.clone(), second.state.clone()]));
    let global = GlobalPositionLimit::new(states);
    global.membership_ready.store(true, Ordering::SeqCst);
    for state in [&first.state, &second.state] {
        state
            .global_position_initialized
            .store(true, Ordering::SeqCst);
        state.global_position_ready.store(true, Ordering::SeqCst);
        let _ = state.global_position_limit.set(Arc::downgrade(&global));
    }
    assert!(global.check().await);
    assert_eq!(*first.state.status.read().await, BotStatus::Paused);
    assert_eq!(*second.state.status.read().await, BotStatus::Paused);
    assert!(first.state.resume_trading().await.is_err());
    second.state.position.write().await.size = Decimal::ZERO;
    assert!(!global.check().await);
    assert!(first.state.resume_trading().await.is_err());
    first
        .state
        .global_cancel_pending
        .store(false, Ordering::SeqCst);
    first.state.resume_trading().await.unwrap();
    first.server.abort();
    second.server.abort();
}

#[tokio::test]
async fn global_position_limit_blocks_until_data_is_ready() {
    let fixture = notification_fixture(false, false).await;
    fixture
        .state
        .config
        .write()
        .await
        .global_max_position_notional = Some(Decimal::ONE);
    let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![fixture.state.clone()])));
    global.membership_ready.store(true, Ordering::SeqCst);
    let _ = fixture
        .state
        .global_position_limit
        .set(Arc::downgrade(&global));
    assert!(global.check().await);
    assert!(fixture.state.resume_trading().await.is_err());
    fixture
        .state
        .global_position_ready
        .store(true, Ordering::SeqCst);
    fixture.state.position.write().await.size = Decimal::ONE;
    assert!(global.check().await);
    fixture.state.position.write().await.mark_price = Decimal::ONE;
    fixture.state.position.write().await.update_time = Some(Utc::now());
    assert!(global.check().await);
    assert_eq!(*fixture.state.status.read().await, BotStatus::Paused);
    fixture.server.abort();
}

#[tokio::test]
async fn initialized_member_failure_pauses_even_while_another_initializes() {
    let mut first = notification_fixture(false, false).await;
    let second = notification_fixture(false, false).await;
    first
        .state
        .config
        .write()
        .await
        .global_max_position_notional = Some(Decimal::ONE);
    first
        .state
        .global_position_initialized
        .store(true, Ordering::SeqCst);
    first
        .state
        .global_position_ready
        .store(false, Ordering::SeqCst);
    let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![
        first.state.clone(),
        second.state.clone(),
    ])));
    global.membership_ready.store(true, Ordering::SeqCst);
    assert!(global.check().await);
    assert_eq!(*first.state.status.read().await, BotStatus::Paused);
    assert_eq!(*second.state.status.read().await, BotStatus::Paused);
    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(1), first.actions.recv())
            .await
            .unwrap(),
        Some(BotControlAction::GlobalRiskCancel)
    ));
    first.server.abort();
    second.server.abort();
}

#[tokio::test]
async fn disabled_committed_guard_skips_check_and_membership_locks() {
    let states = Arc::new(RwLock::new(Vec::new()));
    let global = GlobalPositionLimit::new(states.clone());
    global.initialize_config(crate::config::GlobalConfig::from_app(&AppConfig::default()));
    let _check = global.lock.lock().await;
    let _members = states.write().await;
    assert!(
        !tokio::time::timeout(Duration::from_secs(1), global.check())
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn risk_checks_use_committed_config_during_propagation() {
    let fixture = notification_fixture(false, false).await;
    let old = crate::config::GlobalConfig::from_app(&*fixture.state.config.read().await);
    let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![fixture.state.clone()])));
    global.initialize_config(old);
    global.membership_ready.store(true, Ordering::SeqCst);
    fixture
        .state
        .global_position_initialized
        .store(true, Ordering::SeqCst);
    fixture
        .state
        .config
        .write()
        .await
        .pause_on_zero_available_margin = true;
    fixture.state.account.write().await.total_wallet_balance = Decimal::ZERO;
    assert!(!global.check().await);
    assert_eq!(*fixture.state.status.read().await, BotStatus::Running);
    let new = crate::config::GlobalConfig::from_app(&*fixture.state.config.read().await);
    global.commit_config(new).await;
    assert_eq!(*fixture.state.status.read().await, BotStatus::Paused);
    fixture.server.abort();
}

#[tokio::test]
async fn zero_margin_guard_blocks_failed_initialization_until_full_reconciliation() {
    let fixture = notification_fixture(false, false).await;
    fixture
        .state
        .config
        .write()
        .await
        .pause_on_zero_available_margin = true;
    fixture
        .state
        .global_position_initialized
        .store(true, Ordering::SeqCst);
    let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![fixture.state.clone()])));
    global.membership_ready.store(true, Ordering::SeqCst);
    assert!(global.check().await);
    assert_eq!(*fixture.state.status.read().await, BotStatus::Paused);
    fixture
        .state
        .global_position_ready
        .store(true, Ordering::SeqCst);
    assert!(!global.check().await);
    fixture.server.abort();
}

#[tokio::test]
async fn margin_ratio_guard_pauses_only_at_or_below_threshold() {
    let fixture = notification_fixture(false, false).await;
    {
        let mut config = fixture.state.config.write().await;
        config.min_available_margin_pct = Some(dec!(10));
        // Live accounts are read as stored; paper accounts come from the shared ledger.
        config.exchange.api_key = "key".into();
        config.exchange.api_secret = "secret".into();
    }
    fixture
        .state
        .global_position_initialized
        .store(true, Ordering::SeqCst);
    fixture
        .state
        .global_position_ready
        .store(true, Ordering::SeqCst);
    let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![fixture.state.clone()])));
    global.membership_ready.store(true, Ordering::SeqCst);
    {
        let mut account = fixture.state.account.write().await;
        account.asset = "USDC".into();
        account.margin_balance = dec!(1000);
        account.available_balance = dec!(101);
        account.update_time = Utc::now();
    }
    assert!(!global.check().await);
    assert_eq!(*fixture.state.status.read().await, BotStatus::Running);
    fixture.state.account.write().await.available_balance = dec!(100);
    assert!(global.check().await);
    assert_eq!(*fixture.state.status.read().await, BotStatus::Paused);
    fixture.server.abort();
}

#[tokio::test]
async fn zero_margin_pauses_all_and_requires_confirmed_cancellation() {
    let first = notification_fixture(false, false).await;
    let mut second = notification_fixture(false, false).await;
    for state in [&first.state, &second.state] {
        state.config.write().await.pause_on_zero_available_margin = true;
        state
            .global_position_initialized
            .store(true, Ordering::SeqCst);
        state.global_position_ready.store(true, Ordering::SeqCst);
    }
    second.state.account.write().await.total_wallet_balance = Decimal::ZERO;
    let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![
        first.state.clone(),
        second.state.clone(),
    ])));
    global.membership_ready.store(true, Ordering::SeqCst);
    for state in [&first.state, &second.state] {
        let _ = state.global_position_limit.set(Arc::downgrade(&global));
    }
    assert!(global.check().await);
    assert_eq!(*first.state.status.read().await, BotStatus::Paused);
    assert_eq!(*second.state.status.read().await, BotStatus::Paused);
    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(1), second.actions.recv())
            .await
            .unwrap(),
        Some(BotControlAction::GlobalRiskCancel)
    ));
    second.state.account.write().await.total_wallet_balance = dec!(1);
    assert!(second.state.resume_trading().await.is_err());
    second
        .state
        .global_cancel_pending
        .store(false, Ordering::SeqCst);
    second.state.resume_trading().await.unwrap();
    first.server.abort();
    second.server.abort();
}

#[tokio::test]
async fn zero_margin_guard_ignores_uninitialized_default_balance() {
    let fixture = notification_fixture(false, false).await;
    fixture
        .state
        .config
        .write()
        .await
        .pause_on_zero_available_margin = true;
    {
        let mut config = fixture.state.config.write().await;
        config.exchange.api_key = "live-key".into();
        config.exchange.api_secret = "live-secret".into();
    }
    *fixture.state.account.write().await = AccountInfo::default();
    let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![fixture.state.clone()])));
    global.membership_ready.store(true, Ordering::SeqCst);
    assert!(global.check().await);
    assert!(!fixture.state.global_cancel_pending.load(Ordering::SeqCst));
    fixture.server.abort();
}

#[tokio::test]
async fn zero_margin_guard_requires_enabled_switch_and_detects_negative_balance() {
    let fixture = notification_fixture(false, false).await;
    fixture
        .state
        .global_position_initialized
        .store(true, Ordering::SeqCst);
    fixture.state.account.write().await.total_wallet_balance = -Decimal::ONE;
    fixture
        .state
        .global_position_ready
        .store(true, Ordering::SeqCst);
    let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![fixture.state.clone()])));
    global.membership_ready.store(true, Ordering::SeqCst);
    assert!(!global.check().await);
    assert_eq!(*fixture.state.status.read().await, BotStatus::Running);
    fixture
        .state
        .config
        .write()
        .await
        .pause_on_zero_available_margin = true;
    assert!(global.check().await);
    assert_eq!(*fixture.state.status.read().await, BotStatus::Paused);
    fixture.server.abort();
}

#[tokio::test]
async fn zero_margin_guard_uses_combined_paper_positions() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let commit = Arc::new(Mutex::new(()));
    let mut states = Vec::new();
    let mut receivers = Vec::new();
    for (symbol, size, price) in [
        ("SOLUSDC", dec!(60), dec!(100)),
        ("BNBUSDC", dec!(-20), dec!(200)),
    ] {
        let mut config = AppConfig::default();
        config.exchange.symbol = symbol.into();
        config.pause_on_zero_available_margin = true;
        let (tx, rx) = mpsc::channel(8);
        let state = AppState::new_for_pair(config, db.clone(), tx, commit.clone());
        *state.position.write().await = PositionInfo {
            symbol: symbol.into(),
            size,
            entry_price: price,
            mark_price: price,
            ..Default::default()
        };
        state.account.write().await.available_balance = dec!(10000) - size.abs() * price;
        state
            .global_position_initialized
            .store(true, Ordering::SeqCst);
        state.global_position_ready.store(true, Ordering::SeqCst);
        states.push(state);
        receivers.push(rx);
    }
    let global = GlobalPositionLimit::new(Arc::new(RwLock::new(states.clone())));
    global.membership_ready.store(true, Ordering::SeqCst);
    assert!(global.check().await);
    for state in states {
        assert_eq!(*state.status.read().await, BotStatus::Paused);
        assert_eq!(
            state.current_account().await.available_balance,
            Decimal::ZERO
        );
    }
}

#[tokio::test]
async fn global_margin_guard_fails_closed_on_decimal_overflow_without_panicking() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let mut config = AppConfig::default();
    config.exchange.api_key = "live-key".into();
    config.exchange.api_secret = "live-secret".into();
    config.pause_on_zero_available_margin = true;
    config.min_available_margin_pct = Some(dec!(50));
    let (tx, _rx) = mpsc::channel(4);
    let state = AppState::new(config.clone(), db, tx);
    state
        .global_position_initialized
        .store(true, Ordering::SeqCst);
    state.global_position_ready.store(true, Ordering::SeqCst);
    {
        let mut account = state.account.write().await;
        account.asset = "USDC".into();
        account.available_balance = Decimal::MAX;
        account.margin_balance = Decimal::MAX;
        account.update_time = Utc::now();
    }
    let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![state.clone()])));
    let _ = state.global_position_limit.set(Arc::downgrade(&global));
    global.initialize_config(crate::config::GlobalConfig::from_app(&config));
    global.membership_ready.store(true, Ordering::SeqCst);
    assert!(global.check().await);
}

#[tokio::test]
async fn margin_ratio_guard_triggers_when_margin_balance_is_non_positive_even_with_positive_available(
) {
    let fixture = notification_fixture(false, false).await;
    {
        let mut config = fixture.state.config.write().await;
        config.min_available_margin_pct = Some(dec!(10));
        config.exchange.api_key = "key".into();
        config.exchange.api_secret = "secret".into();
    }
    fixture
        .state
        .global_position_initialized
        .store(true, Ordering::SeqCst);
    fixture
        .state
        .global_position_ready
        .store(true, Ordering::SeqCst);
    let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![fixture.state.clone()])));
    global.membership_ready.store(true, Ordering::SeqCst);
    for margin_balance in [Decimal::ZERO, dec!(-50)] {
        *fixture.state.status.write().await = BotStatus::Running;
        fixture
            .state
            .global_cancel_pending
            .store(false, Ordering::SeqCst);
        {
            let mut account = fixture.state.account.write().await;
            account.asset = "USDC".into();
            account.margin_balance = margin_balance;
            account.available_balance = dec!(10);
            account.update_time = Utc::now();
        }
        assert!(global.check().await);
        assert_eq!(*fixture.state.status.read().await, BotStatus::Paused);
    }
    fixture.server.abort();
}

#[tokio::test]
async fn retired_recovery_failed_member_stays_fail_closed_without_queueing_cancel_or_logging_to_retired_state(
) {
    let retired = notification_fixture(false, false).await;
    let mut active = notification_fixture(true, true).await;
    for state in [&retired.state, &active.state] {
        state.config.write().await.global_max_position_notional = Some(dec!(500));
    }
    retired.state.pause_trading().await.unwrap();
    retired.state.retire();
    retired.state.recent_logs.write().await.clear();
    drop(retired.actions);
    retired
        .state
        .global_position_initialized
        .store(true, Ordering::SeqCst);
    retired
        .state
        .global_position_ready
        .store(false, Ordering::SeqCst);
    active
        .state
        .global_position_initialized
        .store(true, Ordering::SeqCst);
    active
        .state
        .global_position_ready
        .store(true, Ordering::SeqCst);

    let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![
        retired.state.clone(),
        active.state.clone(),
    ])));
    global.membership_ready.store(true, Ordering::SeqCst);
    let _ = active
        .state
        .global_position_limit
        .set(Arc::downgrade(&global));

    assert!(global.check().await);
    assert!(global.check().await);
    assert!(!retired.state.global_cancel_queued.load(Ordering::SeqCst));
    assert!(!retired.state.global_cancel_pending.load(Ordering::SeqCst));
    assert!(retired.state.recent_logs.read().await.is_empty());
    assert_eq!(*active.state.status.read().await, BotStatus::Paused);
    assert!(active.state.try_order_submission().is_none());
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), active.actions.recv())
            .await
            .unwrap(),
        Some(BotControlAction::GlobalRiskCancel)
    ));
    let payload = tokio::time::timeout(Duration::from_secs(1), active.requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(payload["text"].as_str().unwrap().contains("全局风控已触发"));

    retired.server.abort();
    active.server.abort();
}

#[tokio::test]
async fn disabling_global_risk_retries_pending_cancel_until_cleared_then_restores_fast_path() {
    let mut fixture = notification_fixture(false, false).await;
    {
        let mut config = fixture.state.config.write().await;
        config.pause_on_zero_available_margin = true;
    }
    fixture
        .state
        .global_position_initialized
        .store(true, Ordering::SeqCst);
    fixture
        .state
        .global_position_ready
        .store(true, Ordering::SeqCst);
    fixture.state.account.write().await.total_wallet_balance = Decimal::ZERO;

    let states = Arc::new(RwLock::new(vec![fixture.state.clone()]));
    let global = GlobalPositionLimit::new(states.clone());
    global.membership_ready.store(true, Ordering::SeqCst);
    let _ = fixture
        .state
        .global_position_limit
        .set(Arc::downgrade(&global));
    let enabled_cfg = crate::config::GlobalConfig::from_app(&*fixture.state.config.read().await);
    global.initialize_config(enabled_cfg);

    assert!(global.check().await);
    assert!(fixture.state.global_cancel_pending.load(Ordering::SeqCst));
    assert!(global.has_pending_cancels.load(Ordering::SeqCst));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), fixture.actions.recv())
            .await
            .unwrap(),
        Some(BotControlAction::GlobalRiskCancel)
    ));
    // Simulate a transient cancel failure before disabling global risk.
    fixture
        .state
        .global_cancel_queued
        .store(false, Ordering::SeqCst);

    fixture
        .state
        .config
        .write()
        .await
        .pause_on_zero_available_margin = false;
    let disabled_cfg = crate::config::GlobalConfig::from_app(&*fixture.state.config.read().await);
    global.commit_config(disabled_cfg).await;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), fixture.actions.recv())
            .await
            .unwrap(),
        Some(BotControlAction::GlobalRiskCancel)
    ));

    // Simulate another transient cancel failure after global risk is disabled.
    fixture
        .state
        .global_cancel_queued
        .store(false, Ordering::SeqCst);
    assert!(fixture.state.global_cancel_pending.load(Ordering::SeqCst));
    assert!(!fixture.state.global_cancel_queued.load(Ordering::SeqCst));
    assert!(!global.enabled.load(Ordering::SeqCst));
    assert!(global.has_pending_cancels.load(Ordering::SeqCst));

    // Subsequent tick must not short-circuit on the disabled fast path while cancel is pending.
    assert!(!global.check().await);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), fixture.actions.recv())
            .await
            .unwrap(),
        Some(BotControlAction::GlobalRiskCancel)
    ));

    // Once the engine finishes cancelling and clears global_cancel_pending, has_pending_cancels clears.
    fixture
        .state
        .global_cancel_pending
        .store(false, Ordering::SeqCst);
    fixture
        .state
        .global_cancel_queued
        .store(false, Ordering::SeqCst);
    assert!(!global.check().await);
    assert!(!global.has_pending_cancels.load(Ordering::SeqCst));

    // Verify the lock-free fast path is active again.
    let _lock = global.lock.lock().await;
    let _members = states.write().await;
    assert!(
        !tokio::time::timeout(Duration::from_secs(1), global.check())
            .await
            .unwrap()
    );
    drop(_members);
    drop(_lock);
    fixture.state.resume_trading().await.unwrap();
    fixture.server.abort();
}
