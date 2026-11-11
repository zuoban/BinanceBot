use super::super::*;
use super::cache_test_trade;
use crate::types::{GridDirection, OrderSide, TradingMode};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::atomic::Ordering;
use std::time::Duration;

struct NotificationFixture {
    state: Arc<AppState>,
    requests: mpsc::UnboundedReceiver<serde_json::Value>,
    actions: mpsc::Receiver<BotControlAction>,
    url: String,
    server: tokio::task::JoinHandle<()>,
}

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

async fn notification_fixture(enabled: bool, reject: bool) -> NotificationFixture {
    use axum::{routing::post, Json, Router};
    let (request_tx, requests) = mpsc::unbounded_channel();
    let app = Router::new().route(
        "/botTEST/sendMessage",
        post(move |Json(payload): Json<serde_json::Value>| {
            let tx = request_tx.clone();
            async move {
                tx.send(payload).unwrap();
                Json(serde_json::json!({"ok": !reject, "description": "test rejection"}))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut config = AppConfig::default();
    config.telegram = crate::config::TelegramConfig {
        enabled,
        bot_token: "TEST".into(),
        chat_id: "12345".into(),
    };
    let (action_tx, actions) = mpsc::channel(8);
    let mut state = AppState::new(
        config,
        Arc::new(Database::open(":memory:").unwrap()),
        action_tx,
    );
    Arc::get_mut(&mut state).unwrap().telegram_notifier =
        TelegramNotifier::with_base_url(reqwest::Client::new(), url.clone());
    NotificationFixture {
        state,
        requests,
        actions,
        url,
        server,
    }
}

async fn next_notification(fixture: &mut NotificationFixture) -> serde_json::Value {
    let request = tokio::time::timeout(Duration::from_secs(1), fixture.requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(request["chat_id"], "12345");
    assert_eq!(request["parse_mode"], "HTML");
    request
}

async fn no_more_notifications(fixture: &mut NotificationFixture) {
    assert!(
        tokio::time::timeout(Duration::from_millis(100), fixture.requests.recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn global_risk_sends_one_notification_per_trigger_across_pairs() {
    let mut first = notification_fixture(true, false).await;
    let mut second = notification_fixture(true, false).await;
    first
        .state
        .config
        .write()
        .await
        .global_max_position_notional = Some(dec!(100));
    first.state.position.write().await.size = dec!(2);
    {
        let mut ticker = first.state.ticker.write().await;
        ticker.mark_price = dec!(50);
        ticker.mark_update_time = Utc::now();
    }
    let states = Arc::new(RwLock::new(vec![first.state.clone(), second.state.clone()]));
    let global = GlobalPositionLimit::new(states);
    global.membership_ready.store(true, Ordering::SeqCst);
    for state in [&first.state, &second.state] {
        state
            .global_position_initialized
            .store(true, Ordering::SeqCst);
        state.global_position_ready.store(true, Ordering::SeqCst);
    }
    assert!(global.check().await);
    let message = next_notification(&mut first).await;
    assert!(message["text"]
        .as_str()
        .unwrap()
        .contains("全局持仓名义价值 100 已达到上限 100"));
    assert_eq!(*first.state.status.read().await, BotStatus::Paused);
    assert_eq!(*second.state.status.read().await, BotStatus::Paused);
    assert!(global.check().await);
    no_more_notifications(&mut first).await;
    no_more_notifications(&mut second).await;
    first.state.position.write().await.size = Decimal::ZERO;
    assert!(!global.check().await);
    first.state.position.write().await.size = dec!(2);
    assert!(global.check().await);
    next_notification(&mut first).await;
    no_more_notifications(&mut second).await;
    first.server.abort();
    second.server.abort();
}

#[tokio::test]
async fn global_risk_notification_reports_limit_when_another_pair_is_initializing() {
    let mut first = notification_fixture(true, false).await;
    let second = notification_fixture(true, false).await;
    first
        .state
        .config
        .write()
        .await
        .global_max_position_notional = Some(dec!(100));
    first.state.position.write().await.size = dec!(2);
    {
        let mut ticker = first.state.ticker.write().await;
        ticker.mark_price = dec!(50);
        ticker.mark_update_time = Utc::now();
    }
    first
        .state
        .global_position_initialized
        .store(true, Ordering::SeqCst);
    first
        .state
        .global_position_ready
        .store(true, Ordering::SeqCst);
    let global = GlobalPositionLimit::new(Arc::new(RwLock::new(vec![
        first.state.clone(),
        second.state.clone(),
    ])));
    global.membership_ready.store(true, Ordering::SeqCst);
    assert!(global.check().await);
    let message = next_notification(&mut first).await;
    let text = message["text"].as_str().unwrap();
    assert!(text.contains("全局持仓名义价值 100 已达到上限 100"));
    assert!(text.contains("全局风控数据不可用或已过期"));
    first.server.abort();
    second.server.abort();
}

#[tokio::test]
async fn global_risk_notification_failure_does_not_prevent_pause() {
    let mut fixture = notification_fixture(true, true).await;
    fixture
        .state
        .config
        .write()
        .await
        .pause_on_zero_available_margin = true;
    fixture.state.account.write().await.asset = "USDC".into();
    fixture.state.account.write().await.total_wallet_balance = Decimal::ZERO;
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
    let mut logs = fixture.state.ws_broadcast_tx.subscribe();
    assert!(global.check().await);
    let message = next_notification(&mut fixture).await;
    assert!(message["text"]
        .as_str()
        .unwrap()
        .contains("可用保证金已达到零"));
    assert_eq!(*fixture.state.status.read().await, BotStatus::Paused);
    assert!(fixture.state.global_cancel_pending.load(Ordering::SeqCst));
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let log = logs.recv().await.unwrap();
            if log.contains("Telegram 全局风控通知发送失败") {
                break;
            }
        }
    })
    .await
    .unwrap();
    fixture.server.abort();
}

#[tokio::test]
async fn strategy_status_changes_do_not_send_notifications() {
    let mut fixture = notification_fixture(true, false).await;
    fixture.state.pause_trading().await.unwrap();
    assert_eq!(
        fixture.state.db.load_bot_status().unwrap(),
        Some(BotStatus::Paused)
    );
    fixture.state.resume_trading().await.unwrap();
    fixture.state.pause_trading_silently().await.unwrap();
    fixture.state.pause_trading().await.unwrap();
    assert_eq!(*fixture.state.status.read().await, BotStatus::Paused);
    no_more_notifications(&mut fixture).await;
    fixture.server.abort();
}

#[tokio::test]
async fn engine_start_and_position_limit_pause_do_not_notify() {
    for at_limit in [false, true] {
        let mut fixture = notification_fixture(true, false).await;
        {
            let mut config = fixture.state.config.write().await;
            config.grid.pause_on_max_position = at_limit;
            config.grid.max_position_usdc = Some(dec!(1000));
        }
        fixture.state.position.write().await.size = dec!(10);
        {
            let mut ticker = fixture.state.ticker.write().await;
            ticker.last_price = dec!(100);
            ticker.mark_price = dec!(100);
            ticker.mark_update_time = Utc::now();
            ticker.update_time = Utc::now();
        }
        let config = fixture.state.config.read().await.clone();
        let client = Arc::new(crate::exchange::BinanceFuturesClient::with_base_url(
            &config.exchange,
            fixture.url.clone(),
        ));
        let (_, ticker_rx) = broadcast::channel(1);
        // Keep the fixture usable while moving the real action receiver into the engine.
        let (_, unused_actions) = mpsc::channel(1);
        let action_rx = std::mem::replace(&mut fixture.actions, unused_actions);
        let mut engine = crate::strategy::GridTradingEngine::new(
            fixture.state.clone(),
            client,
            action_rx,
            ticker_rx,
        );
        let task = tokio::spawn(async move { engine.run().await });
        if at_limit {
            tokio::time::timeout(Duration::from_secs(1), async {
                while *fixture.state.status.read().await != BotStatus::Paused {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        } else {
            assert_eq!(*fixture.state.status.read().await, BotStatus::Running);
        }
        no_more_notifications(&mut fixture).await;
        task.abort();
        no_more_notifications(&mut fixture).await;
        fixture.server.abort();
    }
}

async fn live_notification_fixture(direction: GridDirection) -> NotificationFixture {
    let fixture = notification_fixture(true, false).await;
    {
        let mut config = fixture.state.config.write().await;
        config.exchange.api_key = "live-key".into();
        config.exchange.api_secret = "live-secret".into();
        config.grid.direction = direction;
    }
    fixture
}

#[tokio::test]
async fn trade_notifications_wait_for_closing_pnl_and_send_each_verified_trade_once() {
    for (direction, pnl, expected) in [
        (GridDirection::Long, dec!(3.25), "+3.2500"),
        (GridDirection::Short, dec!(-2.5), "-2.5000"),
    ] {
        let mut fixture = live_notification_fixture(direction).await;
        let mut closing = cache_test_trade("closing", 1, TradingMode::Live);
        closing.side = direction.closing_side();
        let mut later = cache_test_trade("closing-later", 2, TradingMode::Live);
        later.side = direction.closing_side();
        assert!(
            fixture
                .state
                .record_trade(closing.clone(), None, None, None)
                .await
        );
        assert!(
            fixture
                .state
                .record_trade(later.clone(), None, None, None)
                .await
        );
        no_more_notifications(&mut fixture).await;

        let revision = fixture
            .state
            .db
            .symbol_trade_clear_revision(&closing.symbol);
        closing.realized_pnl = pnl;
        closing.commission = dec!(0.02);
        closing.pnl_verified = true;
        fixture
            .state
            .verify_trade_pnl(closing.clone(), revision)
            .await
            .unwrap();
        let message = next_notification(&mut fixture).await;
        let text = message["text"].as_str().unwrap();
        assert!(text.contains("｜1D｜1H</b>"));
        assert!(text.ends_with(&format!("已实现盈亏：{expected} USDC")));

        fixture
            .state
            .verify_trade_pnl(closing, revision)
            .await
            .unwrap();
        later.pnl_verified = true;
        fixture
            .state
            .verify_trade_pnl(later, revision)
            .await
            .unwrap();
        let message = next_notification(&mut fixture).await;
        let text = message["text"].as_str().unwrap();
        assert!(text.contains("｜2D｜2H</b>"));
        assert!(text.ends_with("已实现盈亏：+0.0000 USDC"));
        no_more_notifications(&mut fixture).await;
        assert_eq!(fixture.state.stats.read().await.pending_pnl_trades, 0);
        assert_eq!(fixture.state.stats.read().await.total_realized_pnl, pnl);
        fixture.server.abort();
    }
}

#[tokio::test]
async fn trade_notifications_keep_openings_immediate_without_repeating_on_pnl_verification() {
    for direction in [GridDirection::Long, GridDirection::Short] {
        let mut fixture = live_notification_fixture(direction).await;
        let mut opening = cache_test_trade("opening", 1, TradingMode::Live);
        opening.side = direction.opening_side();
        assert!(
            fixture
                .state
                .record_trade(opening.clone(), None, None, None)
                .await
        );
        let message = next_notification(&mut fixture).await;
        assert!(!message["text"].as_str().unwrap().contains("已实现盈亏"));

        let revision = fixture
            .state
            .db
            .symbol_trade_clear_revision(&opening.symbol);
        opening.pnl_verified = true;
        fixture
            .state
            .verify_trade_pnl(opening, revision)
            .await
            .unwrap();
        no_more_notifications(&mut fixture).await;
        assert_eq!(fixture.state.stats.read().await.pending_pnl_trades, 0);
        fixture.server.abort();
    }
}

#[tokio::test]
async fn trade_notifications_skip_disabled_or_cleared_pnl_verifications() {
    for clear_history in [false, true] {
        let mut fixture = live_notification_fixture(GridDirection::Long).await;
        let mut closing = cache_test_trade("closing", 1, TradingMode::Live);
        closing.side = OrderSide::Sell;
        assert!(
            fixture
                .state
                .record_trade(closing.clone(), None, None, None)
                .await
        );
        let revision = fixture
            .state
            .db
            .symbol_trade_clear_revision(&closing.symbol);
        if clear_history {
            assert_eq!(
                AppState::clear_trade_history_for(&[fixture.state.clone()])
                    .await
                    .unwrap(),
                1
            );
        } else {
            fixture.state.config.write().await.telegram.enabled = false;
        }

        closing.realized_pnl = dec!(3.25);
        closing.pnl_verified = true;
        fixture
            .state
            .verify_trade_pnl(closing, revision)
            .await
            .unwrap();
        no_more_notifications(&mut fixture).await;
        assert_eq!(fixture.state.stats.read().await.pending_pnl_trades, 0);
        let saved = fixture.state.db.get_recent_trades(1).unwrap();
        if clear_history {
            assert!(saved.is_empty());
        } else {
            assert!(saved[0].pnl_verified);
            assert_eq!(saved[0].realized_pnl, dec!(3.25));
        }
        fixture.server.abort();
    }
}

#[tokio::test]
async fn trade_notifications_capture_position_amount_after_buy_and_sell_fills() {
    let mut fixture = notification_fixture(true, false).await;
    fixture.state.position.write().await.size = dec!(9.5);
    {
        let mut ticker = fixture.state.ticker.write().await;
        ticker.last_price = dec!(100);
        ticker.mark_price = dec!(100);
        ticker.mark_update_time = Utc::now();
        ticker.update_time = Utc::now();
    }
    let config = fixture.state.config.read().await.clone();
    let client = Arc::new(crate::exchange::BinanceFuturesClient::with_base_url(
        &config.exchange,
        fixture.url.clone(),
    ));
    let (ticker_tx, ticker_rx) = broadcast::channel(4);
    let (_, unused_actions) = mpsc::channel(1);
    let action_rx = std::mem::replace(&mut fixture.actions, unused_actions);
    let mut engine = crate::strategy::GridTradingEngine::new(
        fixture.state.clone(),
        client,
        action_rx,
        ticker_rx,
    );
    let task = tokio::spawn(async move { engine.run().await });
    // The engine constructs its initial window asynchronously. Start the
    // notification deadline only after both fillable orders exist.
    let ready = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let orders = fixture.state.active_orders.read().await;
            let buy = orders
                .values()
                .any(|order| order.side == OrderSide::Buy && order.price == dec!(99.9));
            let sell = orders
                .values()
                .any(|order| order.side == OrderSide::Sell && order.price == dec!(100.1));
            if buy && sell {
                break;
            }
            drop(orders);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        ready.is_ok(),
        "initial orders missing; engine_finished={}; status={:?}; orders={:?}; logs={:?}",
        task.is_finished(),
        fixture.state.status.try_read(),
        fixture.state.active_orders.try_read(),
        fixture.state.recent_logs.try_read(),
    );
    for (price, side, size, amount) in [
        (dec!(99.9), "BUY", dec!(10.5), "1050.00"),
        (dec!(100.1), "SELL", dec!(9.51), "951.00"),
    ] {
        ticker_tx
            .send(TickerInfo {
                symbol: config.exchange.symbol.clone(),
                last_price: price,
                mark_price: dec!(100),
                mark_update_time: Utc::now(),
                update_time: Utc::now(),
                ..Default::default()
            })
            .unwrap();
        let message = match tokio::time::timeout(Duration::from_secs(1), fixture.requests.recv())
            .await
        {
            Ok(Some(message)) => message,
            result => panic!(
                "{side} notification missing: {result:?}; engine_finished={}; status={:?}; orders={:?}; trades={:?}; logs={:?}",
                task.is_finished(),
                fixture.state.status.try_read(),
                fixture.state.active_orders.try_read(),
                fixture.state.recent_trades.try_read(),
                fixture.state.recent_logs.try_read(),
            ),
        };
        assert_eq!(message["chat_id"], "12345");
        assert_eq!(message["parse_mode"], "HTML");
        let text = message["text"].as_str().unwrap();
        assert!(text.contains(side));
        assert!(text.contains(&format!("持仓金额：{amount} USDC")));
        assert_eq!(fixture.state.position.read().await.size, size);
    }
    task.abort();
    fixture.server.abort();
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
async fn live_trade_notification_uses_configured_sync_interval_for_account_freshness() {
    let mut fixture = live_notification_fixture(GridDirection::Long).await;
    fixture
        .state
        .config
        .write()
        .await
        .exchange
        .sync_interval_secs = 30;
    {
        let mut account = fixture.state.account.write().await;
        account.asset = "USDC".into();
        account.total_wallet_balance = dec!(10000);
        account.margin_balance = dec!(10123.45);
        account.available_balance = dec!(8000);
        account.update_time = Utc::now() - chrono::Duration::seconds(45);
    }
    let mut buy = cache_test_trade("long-sync-interval-buy", 1, TradingMode::Live);
    buy.side = OrderSide::Buy;
    buy.pnl_verified = true;
    assert!(fixture.state.record_trade(buy, None, None, None).await);
    let message = next_notification(&mut fixture).await;
    let text = message["text"].as_str().unwrap();
    assert!(text.contains("账户权益：10123.45 USDC"), "{text}");
    assert!(text.contains("可用余额：8000.00 USDC｜80.00%"), "{text}");
    fixture.server.abort();
}
