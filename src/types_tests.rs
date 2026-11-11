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
fn global_config_payload_preserves_existing_chat_id_when_blank() {
    let mut global = crate::config::GlobalConfig::from_app(&crate::config::AppConfig::default());
    global.telegram.chat_id = "123456".into();
    UpdateGlobalConfigPayload {
        telegram_chat_id: Some("   ".into()),
        ..Default::default()
    }
    .apply_to(&mut global);
    assert_eq!(global.telegram.chat_id, "123456");

    UpdateGlobalConfigPayload {
        clear_telegram_credentials: true,
        ..Default::default()
    }
    .apply_to(&mut global);
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
    let mut response = crate::exchange::model::BinanceOrderResponse {
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
    let rendered = format!("{global:?} {pair:?}");
    for secret in [
        "payload-api-key",
        "payload-api-secret",
        "payload-bot-token",
        "pair-payload-key",
        "pair-payload-secret",
        "pair-payload-token",
    ] {
        assert!(!rendered.contains(secret), "{rendered}");
    }
    assert!(rendered.contains("[REDACTED]"));
}
