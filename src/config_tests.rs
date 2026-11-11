use super::AppConfig;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[test]
fn server_host_defaults_to_loopback_when_missing() {
    let config: super::ServerConfig = serde_json::from_value(serde_json::json!({})).unwrap();
    assert_eq!(config.host, "127.0.0.1");
    assert_eq!(config.port, 8080);
}

#[test]
fn validation_checks_grid_exchange_and_notification_boundaries() {
    let mut config = AppConfig::default();
    config.grid.buy_window = 1;
    config.grid.sell_window = 50;
    config.exchange.recv_window = 60_000;
    assert!(config.validate().is_ok());
    config.grid.buy_window = 0;
    assert!(config.validate().unwrap_err().to_string().contains("窗口"));
    config.grid.buy_window = 1;
    config.grid.min_price = Some(dec!(10));
    config.grid.max_price = Some(dec!(10));
    assert!(config
        .validate()
        .unwrap_err()
        .to_string()
        .contains("价格下限必须小于上限"));
    config.grid.max_price = Some(dec!(11));
    config.exchange.recv_window = 60_001;
    assert!(config
        .validate()
        .unwrap_err()
        .to_string()
        .contains("请求有效窗口"));
    config.exchange.recv_window = 1;
    config.exchange.api_key = "key".into();
    assert!(config
        .validate()
        .unwrap_err()
        .to_string()
        .contains("API Key"));
    config.exchange.api_key = "key".into();
    config.exchange.api_secret = "secret".into();
    config.telegram.enabled = true;
    assert!(config
        .validate()
        .unwrap_err()
        .to_string()
        .contains("Telegram"));
    config.telegram.bot_token = "token".into();
    config.telegram.chat_id = "chat".into();
    assert!(config.validate().is_ok());
}

#[test]
fn validation_rejects_extreme_decimal_bounds() {
    let limit = dec!(1_000_000_000_000);
    let over = dec!(1_000_000_000_001);

    let mut base = AppConfig::default();
    base.grid.grid_interval = limit;
    base.grid.order_amount_usdc = limit;
    base.grid.min_price = Some(dec!(1));
    base.grid.max_price = Some(limit);
    base.grid.max_position_usdc = Some(limit);
    base.global_max_position_notional = Some(limit);
    assert!(base.validate().is_ok());

    for value in [over, Decimal::MAX] {
        let mut c = AppConfig::default();
        c.grid.grid_interval = value;
        assert!(c.validate().is_err());

        let mut c = AppConfig::default();
        c.grid.order_amount_usdc = value;
        assert!(c.validate().is_err());

        let mut c = AppConfig::default();
        c.grid.min_price = Some(value);
        assert!(c.validate().is_err());

        let mut c = AppConfig::default();
        c.grid.max_price = Some(value);
        assert!(c.validate().is_err());

        let mut c = AppConfig::default();
        c.grid.max_position_usdc = Some(value);
        assert!(c.validate().is_err());

        let c = AppConfig {
            global_max_position_notional: Some(value),
            ..Default::default()
        };
        assert!(c.validate().is_err());
    }
}

#[test]
fn legacy_configs_default_long_and_pairs_keep_independent_directions() {
    use crate::types::GridDirection;
    let mut serialized = serde_json::to_value(AppConfig::default()).unwrap();
    serialized["grid"]
        .as_object_mut()
        .unwrap()
        .remove("direction");
    let mut config: AppConfig = serde_json::from_value(serialized).unwrap();
    assert_eq!(config.grid.direction, GridDirection::Long);
    config.pairs = ["SOLUSDC", "BTCUSDT"]
        .map(|symbol| super::TradingPairConfig::new(symbol.into()))
        .to_vec();
    let mut short = config.grid.clone();
    short.direction = GridDirection::Short;
    config.pairs[1].grid = Some(short);
    let configs = config.strategy_configs().unwrap();
    assert_eq!(configs[0].grid.direction, GridDirection::Long);
    assert_eq!(configs[1].grid.direction, GridDirection::Short);
    config.ensure_pair_list().unwrap();
    assert_eq!(
        config.pairs[1].grid.as_ref().unwrap().direction,
        GridDirection::Short
    );
}

#[test]
fn shared_credentials_stay_global_when_pair_settings_are_saved() {
    let mut config = AppConfig::default();
    config.exchange.api_key = "legacy-key".into();
    config.exchange.api_secret = "legacy-secret".into();
    config.telegram.enabled = true;
    config.telegram.bot_token = "legacy-token".into();
    config.telegram.chat_id = "1234".into();
    config.pairs = ["SOLUSDC", "BTCUSDT"]
        .map(|symbol| super::TradingPairConfig::new(symbol.into()))
        .to_vec();
    config.ensure_pair_list().unwrap();
    assert_eq!(config.exchange.api_key, "legacy-key");
    assert_eq!(config.telegram.bot_token, "legacy-token");
    assert!(config
        .pairs
        .iter()
        .all(|pair| pair.exchange.api_key.is_none() && pair.telegram.is_none()));
    let strategies = config.strategy_configs().unwrap();
    assert!(strategies
        .iter()
        .all(|strategy| strategy.exchange.api_key == "legacy-key"
            && strategy.telegram.bot_token == "legacy-token"));
    let mut cleared = strategies[0].clone();
    cleared.exchange.api_key.clear();
    cleared.exchange.api_secret.clear();
    cleared.telegram = Default::default();
    config.pairs[0].update_from(&cleared);
    let strategies = config.strategy_configs().unwrap();
    assert_eq!(strategies[0].exchange.api_key, "legacy-key");
    assert_eq!(strategies[1].exchange.api_key, "legacy-key");
}

#[test]
fn pair_overrides_migrate_once_to_first_pair_globals() {
    let mut config: AppConfig = serde_json::from_value(serde_json::json!({
        "exchange": {}, "server": {}, "pairs": [
            {"symbol":"SOLUSDC", "exchange":{"api_key":"first-key", "api_secret":"first-secret", "recv_window":8000, "sync_interval_secs":7},
             "telegram":{"enabled":true,"bot_token":"first-token","chat_id":"123"}},
            {"symbol":"BTCUSDT", "grid":{"grid_interval":"10"},
             "exchange":{"api_key":"second-key","api_secret":"second-secret"},
             "telegram":{"enabled":false}}
        ]
    })).unwrap();
    config.ensure_pair_list().unwrap();
    config.validate().unwrap();
    let configs = config.strategy_configs().unwrap();
    assert!(configs
        .iter()
        .all(|strategy| strategy.exchange.api_key == "first-key"
            && strategy.exchange.recv_window == 8000
            && strategy.exchange.sync_interval_secs == 7
            && strategy.telegram.bot_token == "first-token"));
    assert!(configs
        .iter()
        .all(|c| c.exchange.trading_mode() == crate::types::TradingMode::Live));
    assert_eq!(configs[1].grid.grid_interval, dec!(10));
    let serialized = serde_json::to_value(&config).unwrap();
    config.ensure_pair_list().unwrap();
    assert_eq!(serialized, serde_json::to_value(&config).unwrap());
    config.exchange.api_key = "new-global-key".into();
    config.ensure_pair_list().unwrap();
    assert!(config
        .strategy_configs()
        .unwrap()
        .iter()
        .all(|c| c.exchange.api_key == "new-global-key"));
}

#[test]
fn global_position_limit_defaults_and_validates() {
    let mut legacy = serde_json::to_value(AppConfig::default()).unwrap();
    legacy
        .as_object_mut()
        .unwrap()
        .remove("global_max_position_notional");
    let mut config: AppConfig = serde_json::from_value(legacy).unwrap();
    assert_eq!(config.global_max_position_notional, None);
    assert!(config.validate().is_ok());
    for value in [Decimal::ZERO, Decimal::NEGATIVE_ONE] {
        config.global_max_position_notional = Some(value);
        assert!(config.validate().is_err());
    }
    config.global_max_position_notional = Some(Decimal::ONE);
    assert!(config.validate().is_ok());
    let restored: AppConfig =
        serde_json::from_str(&serde_json::to_string(&config).unwrap()).unwrap();
    assert_eq!(restored.global_max_position_notional, Some(Decimal::ONE));
}

#[test]
fn zero_available_margin_pause_defaults_and_round_trips() {
    let mut legacy = serde_json::to_value(AppConfig::default()).unwrap();
    legacy
        .as_object_mut()
        .unwrap()
        .remove("pause_on_zero_available_margin");
    let mut config: AppConfig = serde_json::from_value(legacy).unwrap();
    assert!(!config.pause_on_zero_available_margin);
    config.pause_on_zero_available_margin = true;
    let global = super::GlobalConfig::from_app(&config);
    let mut restored = AppConfig::default();
    global.apply_to(&mut restored);
    assert!(restored.pause_on_zero_available_margin);
    let restored: AppConfig =
        serde_json::from_str(&serde_json::to_string(&restored).unwrap()).unwrap();
    assert!(restored.pause_on_zero_available_margin);
}

#[test]
fn existing_config_without_telegram_still_loads() {
    let mut legacy = serde_json::to_value(AppConfig::default()).unwrap();
    legacy.as_object_mut().unwrap().remove("telegram");
    let config: AppConfig = serde_json::from_value(legacy).unwrap();
    assert!(!config.telegram.enabled);
    assert!(config.telegram.bot_token.is_empty());
    assert!(config.telegram.chat_id.is_empty());
}

#[test]
fn legacy_configs_keep_position_pause_disabled() {
    let config = AppConfig::default();
    assert!(!config.grid.pause_on_max_position);
    let mut legacy = serde_json::to_value(config).unwrap();
    legacy["grid"]
        .as_object_mut()
        .unwrap()
        .remove("pause_on_max_position");
    let restored: AppConfig = serde_json::from_value(legacy).unwrap();
    assert!(!restored.grid.pause_on_max_position);
}

#[test]
fn position_pause_requires_a_valid_limit() {
    let mut config = AppConfig::default();
    config.grid.pause_on_max_position = true;
    config.grid.max_position_usdc = None;
    assert!(config.validate().is_err());
    config.grid.max_position_usdc = Some(Decimal::ZERO);
    assert!(config.validate().is_err());
    config.grid.max_position_usdc = Some(dec!(1000));
    config.validate().unwrap();
    assert!(config.summary().pause_on_max_position);
}

#[test]
fn cancel_opening_requires_a_limit_and_excludes_pause() {
    let mut config = AppConfig::default();
    config.grid.cancel_opening_on_max_position = true;
    config.grid.max_position_usdc = None;
    assert!(config.validate().is_err());
    config.grid.max_position_usdc = Some(dec!(1000));
    config.validate().unwrap();
    config.grid.pause_on_max_position = true;
    assert!(config.validate().is_err());
}

#[test]
fn multi_pair_json_inherits_defaults_and_round_trips() {
    let config: AppConfig = serde_json::from_value(serde_json::json!({
        "exchange": {"api_key": "shared-key", "api_secret": "shared-secret"},
        "grid": {"grid_interval": "1", "order_amount_usdc": "50"},
        "server": {},
        "pairs": [
            {"symbol": "SOLUSDC"},
            {"symbol": "BTCUSDT", "grid": {"grid_interval": "10", "order_amount_usdc": "200", "buy_window": 3},
             "exchange": {}}
        ]
    })).unwrap();
    config.validate().unwrap();
    let configs = config.strategy_configs().unwrap();
    assert_eq!(configs.len(), 2);
    assert_eq!(configs[0].grid.grid_interval, dec!(1));
    assert_eq!(configs[0].grid.order_amount_usdc, dec!(50));
    assert_eq!(
        configs[0].exchange.trading_mode(),
        crate::types::TradingMode::Live
    );
    assert_eq!(configs[1].grid.grid_interval, dec!(10));
    assert_eq!(configs[1].grid.buy_window, 3);
    assert_eq!(configs[1].exchange.api_key, "shared-key");
    assert_eq!(
        configs[1].exchange.trading_mode(),
        crate::types::TradingMode::Live
    );
    assert!(configs.iter().all(|c| c.pairs.is_empty()));
    let json = serde_json::to_value(config).unwrap();
    let restored: AppConfig = serde_json::from_value(json.clone()).unwrap();
    assert_eq!(json, serde_json::to_value(restored).unwrap());
}

#[test]
fn duplicate_symbols_and_invalid_individual_strategies_are_rejected() {
    let mut config = AppConfig {
        pairs: vec![super::TradingPairConfig::new("SOLUSDC".into()); 2],
        ..Default::default()
    };
    assert!(config
        .strategy_configs()
        .unwrap_err()
        .to_string()
        .contains("重复"));
    config.pairs[1].symbol = "btcusdt".into();
    assert!(config.validate().is_err());
    config.pairs[1].symbol = "BTCUSDT".into();
    config.pairs[1].grid = Some(super::GridConfig {
        grid_interval: Decimal::ZERO,
        ..Default::default()
    });
    assert!(config
        .validate()
        .unwrap_err()
        .to_string()
        .contains("BTCUSDT"));
    config.pairs[1].grid = None;
    config.exchange.api_key = "key".into();
    config.exchange.api_secret.clear();
    assert!(config.validate().is_err());
}

#[test]
fn debug_redacts_secrets_and_bot_token_rejects_url_injection_chars() {
    let mut config = AppConfig::default();
    config.exchange.api_key = "live-api-key-value".into();
    config.exchange.api_secret = "live-api-secret-value".into();
    config.telegram.enabled = true;
    config.telegram.bot_token = "123456:valid-bot_token".into();
    config.telegram.chat_id = "987654".into();
    let mut pair = super::TradingPairConfig::new("SOLUSDC".into());
    pair.exchange.api_key = Some("legacy-pair-key".into());
    pair.exchange.api_secret = Some("legacy-pair-secret".into());
    let global = super::GlobalConfig::from_app(&config);

    let rendered = format!("{config:?} {pair:?} {global:?}");
    for secret in [
        "live-api-key-value",
        "live-api-secret-value",
        "123456:valid-bot_token",
        "legacy-pair-key",
        "legacy-pair-secret",
    ] {
        assert!(!rendered.contains(secret), "{rendered}");
    }
    assert!(rendered.contains("[REDACTED]"));
    assert!(config.validate().is_ok());

    for invalid in [
        "123:abc/../../evil",
        "123:abc?chat_id=1",
        "123:abc#frag",
        "123:abc\\path",
        "123:abc space",
        "123:abc\r\n",
        "123:abc\x00",
    ] {
        config.telegram.bot_token = invalid.into();
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("Telegram Bot Token 格式无效"),
            "{invalid:?}"
        );
        config.telegram.enabled = false;
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("Telegram Bot Token 格式无效"),
            "{invalid:?}"
        );
        config.telegram.enabled = true;
    }
}

#[test]
fn api_key_validation_rejects_whitespace_control_and_non_ascii_chars() {
    let mut config = AppConfig::default();
    config.exchange.api_secret = "valid-secret".into();
    for invalid in [
        "key with space",
        "key\nnewline",
        "key\x00null",
        "key\u{200B}zero-width",
        "中文key",
    ] {
        config.exchange.api_key = invalid.into();
        config.ensure_pair_list().unwrap();
        let err = format!("{:#}", config.validate().unwrap_err());
        assert!(err.contains("API Key 格式无效"), "{invalid:?}: {err}");
    }
    config.exchange.api_key = "valid-ascii-api-key-123".into();
    config.ensure_pair_list().unwrap();
    assert!(config.validate().is_ok());
}
