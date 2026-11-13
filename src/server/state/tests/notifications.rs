use super::super::*;
use super::{cache_test_trade, notification_fixture, NotificationFixture};
use crate::types::{GridDirection, OrderSide, TradingMode};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::atomic::Ordering;
use std::time::Duration;

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
        assert!(text.contains("\n成交笔数：1D｜1H\n"));
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
        assert!(text.contains("\n成交笔数：2D｜2H\n"));
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
                AppState::clear_trade_history_for(&[fixture.state.clone()], None)
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
async fn live_trade_notification_omits_account_balances() {
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
    assert!(!text.contains("账户权益"), "{text}");
    assert!(!text.contains("可用余额"), "{text}");
    assert!(!text.contains("≈ ¥"), "{text}");
    fixture.server.abort();
}

#[tokio::test]
async fn ntfy_notifications_deliver_independently_and_alongside_failing_telegram() {
    use axum::{routing::post, Json, Router};

    let (ntfy_tx, mut ntfy_rx) = mpsc::unbounded_channel();
    let app = Router::new().route(
        "/",
        post(move |Json(payload): Json<serde_json::Value>| {
            let tx = ntfy_tx.clone();
            async move {
                let topic = payload["topic"].as_str().unwrap_or("").to_string();
                tx.send(payload).unwrap();
                Json(serde_json::json!({"id": "ntfy-msg-1", "event": "message", "topic": topic}))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ntfy_url = format!("http://{}", listener.local_addr().unwrap());
    let ntfy_server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // Telegram enabled and rejecting, ntfy enabled and succeeding.
    let mut fixture = notification_fixture(true, true).await;
    Arc::get_mut(&mut fixture.state).unwrap().ntfy_notifier =
        crate::ntfy::NtfyNotifier::with_client(reqwest::Client::new());
    {
        let mut config = fixture.state.config.write().await;
        config.ntfy = crate::config::NtfyConfig {
            enabled: true,
            server_url: ntfy_url,
            topic: "grid_alerts".into(),
            token: String::new(),
        };
        config.pause_on_zero_available_margin = true;
    }

    let mut buy = cache_test_trade("ntfy-buy-1", 1, TradingMode::Paper);
    buy.side = OrderSide::Buy;
    buy.pnl_verified = true;
    assert!(fixture.state.record_trade(buy, None, None, None).await);
    let _ = next_notification(&mut fixture).await;
    let ntfy_trade = tokio::time::timeout(Duration::from_secs(1), ntfy_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ntfy_trade["topic"], "grid_alerts");
    assert!(ntfy_trade["title"]
        .as_str()
        .unwrap()
        .starts_with("🟢 BUY｜SOLUSDC｜"));
    let body = ntfy_trade["message"].as_str().unwrap();
    assert!(body.starts_with("成交笔数：1D｜1H\n"), "{body}");
    assert!(!body.contains("账户权益"), "{body}");
    assert!(!body.contains("可用余额"), "{body}");

    // Global risk notifies ntfy with priority 4 even when Telegram fails.
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
    assert!(global.check().await);
    let _ = next_notification(&mut fixture).await;
    let ntfy_risk = tokio::time::timeout(Duration::from_secs(1), ntfy_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ntfy_risk["title"], "⚠️ 全局风控已触发");
    assert_eq!(ntfy_risk["priority"], 4);

    fixture.server.abort();
    ntfy_server.abort();
}

#[tokio::test]
async fn reverse_trades_suppress_trade_alerts_and_deliver_synchronized_reverse_notifications() {
    use crate::telegram::ReverseNotification;
    use axum::{routing::post, Json, Router};

    let (ntfy_tx, mut ntfy_rx) = mpsc::unbounded_channel();
    let app = Router::new().route(
        "/",
        post(move |Json(payload): Json<serde_json::Value>| {
            let tx = ntfy_tx.clone();
            async move {
                let topic = payload["topic"].as_str().unwrap_or("").to_string();
                tx.send(payload).unwrap();
                Json(serde_json::json!({"id": "ntfy-rev-1", "event": "message", "topic": topic}))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ntfy_url = format!("http://{}", listener.local_addr().unwrap());
    let ntfy_server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let mut fixture = notification_fixture(true, false).await;
    Arc::get_mut(&mut fixture.state).unwrap().ntfy_notifier =
        crate::ntfy::NtfyNotifier::with_client(reqwest::Client::new());
    {
        let mut config = fixture.state.config.write().await;
        config.exchange.api_key = "live-key".into();
        config.exchange.api_secret = "live-secret".into();
        config.ntfy = crate::config::NtfyConfig {
            enabled: true,
            server_url: ntfy_url,
            topic: "grid_reverse".into(),
            token: String::new(),
        };
    }

    let mut reverse_open = cache_test_trade("reverse:open-1", 1, TradingMode::Live);
    reverse_open.side = OrderSide::Buy;
    reverse_open.pnl_verified = true;
    reverse_open.is_maker = false;
    assert!(
        fixture
            .state
            .record_trade(reverse_open, None, None, None)
            .await
    );

    let mut reverse_close = cache_test_trade("reverse:close-1", 2, TradingMode::Live);
    reverse_close.side = OrderSide::Sell;
    reverse_close.pnl_verified = false;
    reverse_close.is_maker = false;
    assert!(
        fixture
            .state
            .record_trade(reverse_close.clone(), None, None, None)
            .await
    );
    let mut verified_close = reverse_close;
    verified_close.realized_pnl = dec!(4.2);
    verified_close.commission = dec!(0.1);
    verified_close.pnl_verified = true;
    let clear_rev = fixture.state.db.symbol_trade_clear_revision("SOLUSDC");
    fixture
        .state
        .verify_trade_pnl(verified_close, clear_rev)
        .await
        .unwrap();

    no_more_notifications(&mut fixture).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), ntfy_rx.recv())
            .await
            .is_err()
    );

    let now = Utc::now();
    fixture
        .state
        .notify_reverse_result(ReverseNotification {
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Live,
            from_direction: GridDirection::Long,
            to_direction: GridDirection::Short,
            effective_direction: GridDirection::Short,
            closed_qty: dec!(1.5),
            close_avg_price: Some(dec!(120)),
            opened_qty: dec!(1.5),
            open_avg_price: Some(dec!(119.9)),
            status: BotStatus::Running,
            failure_reason: None,
            timestamp: now,
        })
        .await;

    let tg_ok = next_notification(&mut fixture).await;
    assert!(tg_ok["text"]
        .as_str()
        .unwrap()
        .starts_with("<b>🔄 反手交易已完成｜SOLUSDC｜实盘</b>"));
    let ntfy_ok = tokio::time::timeout(Duration::from_secs(1), ntfy_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ntfy_ok["title"], "🔄 反手交易已完成｜SOLUSDC｜实盘");
    assert_eq!(ntfy_ok["priority"], 3);

    fixture
        .state
        .notify_reverse_result(ReverseNotification {
            symbol: "SOLUSDC".into(),
            mode: TradingMode::Live,
            from_direction: GridDirection::Short,
            to_direction: GridDirection::Long,
            effective_direction: GridDirection::Short,
            closed_qty: Decimal::ZERO,
            close_avg_price: None,
            opened_qty: Decimal::ZERO,
            open_avg_price: None,
            status: BotStatus::Paused,
            failure_reason: Some("市价平仓未完成".into()),
            timestamp: now,
        })
        .await;

    let tg_err = next_notification(&mut fixture).await;
    assert!(tg_err["text"]
        .as_str()
        .unwrap()
        .starts_with("<b>⚠️ 反手交易异常暂停｜SOLUSDC｜实盘</b>"));
    let ntfy_err = tokio::time::timeout(Duration::from_secs(1), ntfy_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ntfy_err["title"], "⚠️ 反手交易异常暂停｜SOLUSDC｜实盘");
    assert_eq!(ntfy_err["priority"], 4);

    fixture.server.abort();
    ntfy_server.abort();
}
