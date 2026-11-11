use super::*;
use rust_decimal_macros::dec;

#[test]
fn startup_preserves_saved_settings_and_upgrades_single_pair() {
    let db = Database::open(":memory:").unwrap();
    let mut saved = AppConfig::default();
    saved.exchange.symbol = "BTCUSDT".into();
    saved.exchange.api_key = "saved-key".into();
    saved.grid.grid_interval = dec!(10);
    saved.server.host = "0.0.0.0".into();
    db.save_config(&saved).unwrap();
    let config = load_saved_config(&db).unwrap();
    let strategies = config.strategy_configs().unwrap();
    assert_eq!(strategies.len(), 1);
    assert_eq!(strategies[0].exchange.symbol, "BTCUSDT");
    assert_eq!(strategies[0].exchange.api_key, "saved-key");
    assert_eq!(config.server.host, "0.0.0.0");
    assert_eq!(strategies[0].grid.grid_interval, dec!(10));
    assert_eq!(
        serde_json::to_value(&config).unwrap(),
        serde_json::to_value(load_saved_config(&db).unwrap()).unwrap()
    );
}

#[test]
fn fresh_database_uses_defaults_without_credentials() {
    let db = Database::open(":memory:").unwrap();
    let config = load_saved_config(&db).unwrap();
    config.validate().unwrap();
    assert_eq!(config.server.host, "127.0.0.1");
    assert_eq!(load_saved_config(&db).unwrap().server.host, "127.0.0.1");
    assert_eq!(config.pairs.len(), 1);
    let strategy = &config.strategy_configs().unwrap()[0];
    assert_eq!(
        strategy.exchange.trading_mode(),
        binance_grid_bot::types::TradingMode::Paper
    );
    assert!(strategy.exchange.api_key.is_empty());
    assert!(strategy.exchange.api_secret.is_empty());
}

#[test]
fn file_and_strategy_cli_options_are_no_longer_supported() {
    for option in [
        "--config",
        "--symbol",
        "--interval",
        "--amount",
        "--live",
        "--testnet",
        "--buy-window",
        "--sell-window",
    ] {
        assert!(CliArgs::try_parse_from(["bot", option, "value"]).is_err());
    }
}
