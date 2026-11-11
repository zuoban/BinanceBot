use super::*;
use rust_decimal_macros::dec;

#[test]
fn price_freshness_checks_both_clocks_and_keeps_the_startup_fallback() {
    let now = Utc::now();
    let mut ticker = TickerInfo {
        last_price: dec!(100),
        update_time: now,
        ..Default::default()
    };
    assert!(ticker.prices_are_fresh(now));
    ticker.mark_price = dec!(101);
    assert!(!ticker.prices_are_fresh(now));
    ticker.mark_update_time = now - chrono::Duration::milliseconds(14_999);
    assert!(ticker.prices_are_fresh(now));
    ticker.mark_update_time = now - chrono::Duration::seconds(15);
    assert!(!ticker.prices_are_fresh(now));
    ticker.mark_update_time = now;
    ticker.update_time = now + chrono::Duration::milliseconds(1);
    assert!(!ticker.prices_are_fresh(now));
    ticker.update_time = now;
    ticker.last_price = Decimal::ZERO;
    assert!(!ticker.prices_are_fresh(now));
}

#[test]
fn global_config_payload_allows_clearing_chat_id_while_preserving_omitted_fields() {
    let mut global = crate::config::GlobalConfig::from_app(&crate::config::AppConfig::default());
    global.telegram.bot_token = "saved-bot-token".into();
    global.telegram.chat_id = "123456".into();

    UpdateGlobalConfigPayload {
        telegram_chat_id: None,
        ..Default::default()
    }
    .apply_to(&mut global);
    assert_eq!(global.telegram.chat_id, "123456");

    UpdateGlobalConfigPayload {
        telegram_enabled: Some(false),
        telegram_chat_id: Some("   ".into()),
        ..Default::default()
    }
    .apply_to(&mut global);
    assert!(global.telegram.chat_id.is_empty());
    assert_eq!(global.telegram.bot_token, "saved-bot-token");

    global.telegram.chat_id = "654321".into();
    UpdateGlobalConfigPayload {
        clear_telegram_credentials: true,
        ..Default::default()
    }
    .apply_to(&mut global);
    assert!(global.telegram.bot_token.is_empty());
    assert!(global.telegram.chat_id.is_empty());
}

#[test]
fn initial_paper_account_falls_back_to_symbol_when_quote_suffix_is_unknown() {
    assert_eq!(AccountInfo::initial_paper("SOLUSDC", None).asset, "USDC");
    assert_eq!(AccountInfo::initial_paper("BTCETH", None).asset, "BTCETH");
}

#[test]
fn terminal_fill_prefers_positive_avg_price_falls_back_to_response_price_and_updates_notional() {
    let created_at = Utc::now() - chrono::Duration::seconds(10);
    let updated_at = Utc::now();
    let base_order = GridOrder {
        client_order_id: "gb_fill_1".into(),
        order_id: Some(42),
        symbol: "SOLUSDC".into(),
        side: OrderSide::Buy,
        price: dec!(100),
        quantity: dec!(2),
        amount_usdc: dec!(200),
        status: OrderStatus::New,
        created_at,
        updated_at: created_at,
        grid_level: 1000,
        paired_client_order_id: None,
        is_take_profit: false,
        purpose: OrderPurpose::Grid,
        merge_sources: Vec::new(),
    };
    let mut response = crate::exchange::BinanceOrderResponse {
        order_id: 42,
        client_order_id: "gb_fill_1".into(),
        symbol: "SOLUSDC".into(),
        status: "FILLED".into(),
        price: dec!(101),
        avg_price: Some(dec!(99.5)),
        orig_qty: dec!(2),
        executed_qty: dec!(1.5),
        side: "BUY".into(),
        order_type: "LIMIT".into(),
        time_in_force: "GTX".into(),
        update_time: None,
    };

    let mut order = base_order.clone();
    order.apply_terminal_fill(&response, updated_at);
    assert_eq!(order.updated_at, updated_at);
    assert_eq!(order.quantity, dec!(1.5));
    assert_eq!(order.price, dec!(99.5));
    assert_eq!(order.amount_usdc, dec!(149.25));

    response.avg_price = Some(Decimal::ZERO);
    let mut fallback_order = base_order.clone();
    fallback_order.apply_terminal_fill(&response, updated_at);
    assert_eq!(fallback_order.price, dec!(101));
    assert_eq!(fallback_order.amount_usdc, dec!(151.5));

    response.price = Decimal::ZERO;
    let mut keep_order = base_order;
    keep_order.apply_terminal_fill(&response, updated_at);
    assert_eq!(keep_order.price, dec!(100));
    assert_eq!(keep_order.amount_usdc, dec!(150));
}

#[test]
fn config_payload_debug_redacts_secrets() {
    let global = UpdateGlobalConfigPayload {
        api_key: Some("payload-api-key".into()),
        api_secret: Some("payload-api-secret".into()),
        telegram_bot_token: Some("payload-bot-token".into()),
        telegram_chat_id: Some("12345".into()),
        ..Default::default()
    };
    let pair = UpdateConfigPayload {
        baseline_version: None,
        symbol: "SOLUSDC".into(),
        direction: None,
        grid_interval: dec!(0.1),
        order_amount_usdc: dec!(100),
        buy_window: 5,
        sell_window: 5,
        post_only: Some(true),
        dry_run: None,
        is_testnet: None,
        recv_window: None,
        sync_interval_secs: None,
        api_key: Some("pair-payload-key".into()),
        api_secret: Some("pair-payload-secret".into()),
        clear_api_credentials: false,
        clear_telegram_credentials: false,
        telegram_enabled: None,
        telegram_bot_token: Some("pair-payload-token".into()),
        telegram_chat_id: None,
        min_price: None,
        max_price: None,
        max_position_usdc: None,
        pause_on_max_position: None,
        cancel_opening_on_max_position: None,
        min_liquidation_distance_pct: None,
    };
    let login = AuthLoginPayload {
        password: "login-secret-password".into(),
    };
    let token_response = AuthTokenResponse {
        token: "session-bearer-secret-token".into(),
    };
    let rendered = format!("{global:?} {pair:?} {login:?} {token_response:?}");
    for secret in [
        "payload-api-key",
        "payload-api-secret",
        "payload-bot-token",
        "pair-payload-key",
        "pair-payload-secret",
        "pair-payload-token",
        "login-secret-password",
        "session-bearer-secret-token",
    ] {
        assert!(!rendered.contains(secret), "{rendered}");
    }
    assert!(rendered.contains("[REDACTED]"));
}

#[test]
fn apply_fill_to_position_handles_extreme_quantities_and_prices_without_panicking() {
    assert!(OrderSide::Buy.is_maker_price(dec!(99.9), dec!(100)));
    assert!(!OrderSide::Buy.is_maker_price(dec!(100), dec!(100)));
    assert!(OrderSide::Sell.is_maker_price(dec!(100.1), dec!(100)));
    assert!(!OrderSide::Sell.is_maker_price(dec!(100), dec!(100)));

    // Weighted entry fallback when entry_price * abs_size overflows Decimal::MAX.
    let huge_qty = Decimal::MAX / dec!(4);
    let (new_size, new_entry, realized) =
        apply_fill_to_position(huge_qty, dec!(100), OrderSide::Buy, dec!(200), huge_qty);
    assert_eq!(new_size, huge_qty + huge_qty);
    assert_eq!(new_entry, dec!(150));
    assert_eq!(realized, Decimal::ZERO);

    // Saturation when adding to position exceeds Decimal::MAX / Decimal::MIN.
    let (long_sat, long_entry, long_pnl) = apply_fill_to_position(
        Decimal::MAX,
        Decimal::MAX,
        OrderSide::Buy,
        Decimal::MAX,
        Decimal::MAX,
    );
    assert_eq!(long_sat, Decimal::MAX);
    assert!(long_entry > Decimal::ZERO);
    assert_eq!(long_pnl, Decimal::ZERO);

    let (short_sat, short_entry, short_pnl) = apply_fill_to_position(
        Decimal::MIN,
        Decimal::MAX,
        OrderSide::Sell,
        Decimal::MAX,
        Decimal::MAX,
    );
    assert_eq!(short_sat, Decimal::MIN);
    assert!(short_entry > Decimal::ZERO);
    assert_eq!(short_pnl, Decimal::ZERO);

    // Opposite-direction closing fills with extreme price differences and quantities.
    let (closed_long, entry_after_long, max_gain) = apply_fill_to_position(
        Decimal::MAX,
        Decimal::MIN,
        OrderSide::Sell,
        Decimal::MAX,
        Decimal::MAX,
    );
    assert_eq!(closed_long, Decimal::ZERO);
    assert_eq!(entry_after_long, Decimal::ZERO);
    assert_eq!(max_gain, Decimal::MAX);

    let (_, _, max_loss) = apply_fill_to_position(
        Decimal::MAX,
        Decimal::MAX,
        OrderSide::Sell,
        Decimal::MIN,
        Decimal::MAX,
    );
    assert_eq!(max_loss, Decimal::MIN);

    // Short closing extreme gain & loss.
    let (_, _, short_gain) = apply_fill_to_position(
        Decimal::MIN,
        Decimal::MAX,
        OrderSide::Buy,
        Decimal::MIN,
        Decimal::MAX,
    );
    assert_eq!(short_gain, Decimal::MAX);

    let stats = HourlyTradeStats {
        window_start: Utc::now(),
        window_end: Utc::now(),
        buckets: vec![
            HourlyTradeCount {
                hour_start: Utc::now(),
                buy_count: u64::MAX,
                sell_count: u64::MAX - 2,
            },
            HourlyTradeCount {
                hour_start: Utc::now(),
                buy_count: 10,
                sell_count: 10,
            },
        ],
        realized_pnl: Decimal::ZERO,
        pending_pnl_count: 0,
    };
    assert_eq!(stats.total_counts(), (u64::MAX, u64::MAX));
}

#[test]
fn decimal_formatters_handle_rounding_truncation_negative_zero_and_extreme_digits() {
    assert_eq!(format_decimal_dp(dec!(1.236), 2, false), "1.24");
    assert_eq!(format_truncated_decimal_dp(dec!(1.236), 2, false), "1.23");
    assert_eq!(format_decimal_dp(dec!(1.236), 2, true), "+1.24");
    assert_eq!(format_truncated_decimal_dp(dec!(-1.239), 2, true), "-1.23");
    assert_eq!(format_decimal_dp(dec!(-0.001), 2, false), "0.00");
    assert_eq!(format_decimal_dp(dec!(-0.001), 2, true), "+0.00");
    assert_eq!(format_truncated_decimal_dp(dec!(-0.009), 2, false), "0.00");
    assert_eq!(format_truncated_decimal_dp(dec!(-0.9), 0, false), "0");

    for extreme in [Decimal::MAX, Decimal::MIN] {
        for dp in [0, 2, 8, 28, 30] {
            let rounded = format_decimal_dp(extreme, dp, true);
            let truncated = format_truncated_decimal_dp(extreme, dp, false);
            if dp == 0 {
                assert!(!rounded.contains('.'));
                assert!(!truncated.contains('.'));
            } else {
                assert_eq!(rounded.split_once('.').unwrap().1.len(), dp as usize);
                assert_eq!(truncated.split_once('.').unwrap().1.len(), dp as usize);
            }
        }
    }
}

#[test]
fn bot_status_db_round_trip_and_const_order_side_helpers() {
    for (status, text) in [
        (BotStatus::Running, "RUNNING"),
        (BotStatus::Paused, "PAUSED"),
        (BotStatus::Stopped, "STOPPED"),
        (BotStatus::Error, "ERROR"),
    ] {
        assert_eq!(status.as_str(), text);
        assert_eq!(BotStatus::from_db(text).unwrap(), status);
    }
    assert!(BotStatus::from_db("running").is_err());
    assert!(BotStatus::from_db("\"RUNNING\"").is_err());
    assert!(BotStatus::from_db("UNKNOWN").is_err());

    const OPP_BUY: OrderSide = OrderSide::Buy.opposite();
    const OPP_SELL: OrderSide = OrderSide::Sell.opposite();
    const BUY_STR: &str = OrderSide::Buy.as_str();
    const SELL_STR: &str = OrderSide::Sell.as_str();
    assert_eq!(OPP_BUY, OrderSide::Sell);
    assert_eq!(OPP_SELL, OrderSide::Buy);
    assert_eq!(BUY_STR, "BUY");
    assert_eq!(SELL_STR, "SELL");
}

#[test]
fn validate_credential_updates_rejects_contradictory_or_half_configured_payloads() {
    assert!(UpdateGlobalConfigPayload::default()
        .validate_credential_updates()
        .is_ok());
    assert!(UpdateGlobalConfigPayload {
        clear_api_credentials: true,
        api_key: Some("   ".into()),
        ..Default::default()
    }
    .validate_credential_updates()
    .is_ok());
    assert!(UpdateGlobalConfigPayload {
        api_key: Some("new-key".into()),
        api_secret: Some("new-secret".into()),
        ..Default::default()
    }
    .validate_credential_updates()
    .is_ok());

    let clear_api_conflict = UpdateGlobalConfigPayload {
        clear_api_credentials: true,
        api_key: Some("new-key".into()),
        ..Default::default()
    }
    .validate_credential_updates()
    .unwrap_err()
    .to_string();
    assert_eq!(
        clear_api_conflict,
        "清除 API 凭证时请勿同时提交新的 API Key 或 Secret"
    );

    let clear_tg_conflict = UpdateGlobalConfigPayload {
        clear_telegram_credentials: true,
        telegram_chat_id: Some("12345".into()),
        ..Default::default()
    }
    .validate_credential_updates()
    .unwrap_err()
    .to_string();
    assert_eq!(
        clear_tg_conflict,
        "清除 Telegram 凭证时请勿同时提交新的 Bot Token 或 Chat ID"
    );

    let partial_api = UpdateGlobalConfigPayload {
        api_key: Some("only-key".into()),
        api_secret: Some("   ".into()),
        ..Default::default()
    }
    .validate_credential_updates()
    .unwrap_err()
    .to_string();
    assert_eq!(
        partial_api,
        "更换或配置 API 凭证时请同时填写 API Key 和 Secret"
    );
}
