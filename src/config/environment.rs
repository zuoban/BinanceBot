use super::{AppConfig, TradingPairConfig};
use anyhow::{anyhow, Result};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;

struct EnvReader<'a> {
    values: &'a HashMap<String, String>,
    prefix: String,
}

impl EnvReader<'_> {
    fn raw(&self, name: &str) -> Option<&str> {
        self.values
            .get(&format!("{}{}", self.prefix, name))
            .map(|value| value.trim())
    }

    fn parsed<T: FromStr>(&self, name: &str) -> Result<Option<T>> {
        match self.raw(name).filter(|value| !value.is_empty()) {
            Some(value) => value.parse().map(Some).map_err(|_| {
                // Values may contain sensitive input; report the variable name only.
                anyhow!("环境变量 {}{} 格式无效", self.prefix, name)
            }),
            None => Ok(None),
        }
    }

    fn boolean(&self, name: &str) -> Result<Option<bool>> {
        match self
            .raw(name)
            .filter(|value| !value.is_empty())
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("true" | "1" | "yes" | "on") => Ok(Some(true)),
            Some("false" | "0" | "no" | "off") => Ok(Some(false)),
            Some(_) => Err(anyhow!(
                "环境变量 {}{} 必须为 true 或 false",
                self.prefix,
                name
            )),
            None => Ok(None),
        }
    }

    fn text(&self, name: &str, target: &mut String) -> bool {
        if let Some(value) = self.raw(name) {
            *target = value.to_string();
            true
        } else {
            false
        }
    }

    fn optional_decimal(&self, name: &str, target: &mut Option<Decimal>) -> Result<bool> {
        if self.raw(name).is_none() {
            return Ok(false);
        }
        *target = self.parsed(name)?;
        Ok(true)
    }
}

fn apply_strategy(config: &mut AppConfig, env: &EnvReader<'_>) -> Result<bool> {
    let mut changed = false;
    macro_rules! number {
        ($name:literal, $target:expr) => {
            if let Some(value) = env.parsed($name)? {
                $target = value;
                changed = true;
            }
        };
    }
    macro_rules! boolean {
        ($name:literal, $target:expr) => {
            if let Some(value) = env.boolean($name)? {
                $target = value;
                changed = true;
            }
        };
    }
    number!("GRID_INTERVAL", config.grid.grid_interval);
    number!("GRID_ORDER_AMOUNT_USDC", config.grid.order_amount_usdc);
    number!("GRID_BUY_WINDOW", config.grid.buy_window);
    number!("GRID_SELL_WINDOW", config.grid.sell_window);
    boolean!("GRID_POST_ONLY", config.grid.post_only);
    boolean!(
        "GRID_PAUSE_ON_MAX_POSITION",
        config.grid.pause_on_max_position
    );
    changed |= env.optional_decimal("GRID_MIN_PRICE", &mut config.grid.min_price)?;
    changed |= env.optional_decimal("GRID_MAX_PRICE", &mut config.grid.max_price)?;
    changed |=
        env.optional_decimal("GRID_MAX_POSITION_USDC", &mut config.grid.max_position_usdc)?;

    changed |= env.text("BINANCE_API_KEY", &mut config.exchange.api_key);
    changed |= env.text("BINANCE_API_SECRET", &mut config.exchange.api_secret);
    boolean!("BINANCE_DRY_RUN", config.exchange.dry_run);
    boolean!("BINANCE_TESTNET", config.exchange.is_testnet);
    number!("BINANCE_RECV_WINDOW", config.exchange.recv_window);
    number!(
        "BINANCE_SYNC_INTERVAL_SECS",
        config.exchange.sync_interval_secs
    );

    boolean!("TELEGRAM_ENABLED", config.telegram.enabled);
    changed |= env.text("TELEGRAM_BOT_TOKEN", &mut config.telegram.bot_token);
    changed |= env.text("TELEGRAM_CHAT_ID", &mut config.telegram.chat_id);
    Ok(changed)
}

impl AppConfig {
    /// Docker injects .env through env_file / --env-file; the application reads process variables.
    pub fn apply_env(&mut self) -> Result<bool> {
        let values = std::env::vars_os()
            .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)))
            .collect();
        self.apply_environment(&values)
    }

    /// Explicit input keeps precedence testable without mutating process-wide environment variables.
    /// Parsing is atomic; validation runs after the final CLI overrides at startup.
    pub fn apply_environment(&mut self, values: &HashMap<String, String>) -> Result<bool> {
        let mut candidate = self.clone();
        let global = EnvReader {
            values,
            prefix: String::new(),
        };
        let mut changed = false;
        if let Some(symbols) = global.raw("TRADING_PAIRS") {
            anyhow::ensure!(!symbols.is_empty(), "环境变量 TRADING_PAIRS 不能为空");
            candidate.pairs = symbols
                .split(',')
                .map(|symbol| {
                    let symbol = symbol.trim().to_uppercase();
                    candidate
                        .pairs
                        .iter()
                        .find(|pair| pair.symbol == symbol)
                        .cloned()
                        .unwrap_or_else(|| TradingPairConfig::new(symbol))
                })
                .collect();
            candidate
                .strategy_configs()
                .map_err(|_| anyhow!("环境变量 TRADING_PAIRS 包含格式无效或重复的交易对"))?;
            changed = true;
        }
        changed |= global.text("SERVER_HOST", &mut candidate.server.host);
        if let Some(port) = global.parsed("SERVER_PORT")? {
            candidate.server.port = port;
            changed = true;
        }
        let global_strategy_changed = apply_strategy(&mut candidate, &global)?;
        changed |= global_strategy_changed;
        let strategies = candidate.strategy_configs()?;
        if candidate.pairs.is_empty() {
            let pair = EnvReader {
                values,
                prefix: format!("PAIR_{}_", candidate.exchange.symbol),
            };
            changed |= apply_strategy(&mut candidate, &pair)?;
        } else {
            for (pair_config, mut strategy) in candidate.pairs.iter_mut().zip(strategies) {
                // Explicit global variables override saved per-pair values as well.
                apply_strategy(&mut strategy, &global)?;
                let pair = EnvReader {
                    values,
                    prefix: format!("PAIR_{}_", pair_config.symbol),
                };
                let pair_changed = apply_strategy(&mut strategy, &pair)?;
                changed |= pair_changed;
                if global_strategy_changed || pair_changed {
                    pair_config.update_from(&strategy);
                }
            }
        }
        if changed {
            *self = candidate;
        }
        Ok(changed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn values(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn env_list_and_individual_grids_support_multi_pair_docker_configuration() {
        let mut config = AppConfig::default();
        config
            .apply_environment(&values(&[
                ("TRADING_PAIRS", " solusdc, BTCUSDT "),
                ("GRID_ORDER_AMOUNT_USDC", "50"),
                ("GRID_BUY_WINDOW", "2"),
                ("PAIR_SOLUSDC_GRID_INTERVAL", "0.2"),
                ("PAIR_BTCUSDT_GRID_INTERVAL", "10"),
                ("PAIR_BTCUSDT_GRID_ORDER_AMOUNT_USDC", "150"),
                ("SERVER_HOST", "127.0.0.1"),
                ("SERVER_PORT", "9090"),
            ]))
            .unwrap();
        config.validate().unwrap();
        let strategies = config.strategy_configs().unwrap();
        assert_eq!(strategies.len(), 2);
        assert_eq!(strategies[0].grid.grid_interval, dec!(0.2));
        assert_eq!(strategies[0].grid.order_amount_usdc, dec!(50));
        assert_eq!(strategies[1].grid.grid_interval, dec!(10));
        assert_eq!(strategies[1].grid.order_amount_usdc, dec!(150));
        assert_eq!(strategies[1].grid.buy_window, 2);
        assert_eq!(config.server.host, "127.0.0.1");
        assert_eq!(config.server.port, 9090);
    }

    #[test]
    fn shipped_env_example_round_trips_through_sqlite_and_reapplies_overrides() {
        let environment = include_str!("../../.env.example")
            .lines()
            .filter(|line| !line.trim().is_empty() && !line.trim_start().starts_with('#'))
            .map(|line| {
                let (key, value) = line.split_once('=').unwrap();
                (key.to_string(), value.to_string())
            })
            .collect();
        let mut config = AppConfig::default();
        config.apply_environment(&environment).unwrap();
        config.validate().unwrap();
        let db = crate::db::Database::open(":memory:").unwrap();
        db.save_config(&config).unwrap();
        let mut restored = db.load_config().unwrap().unwrap();
        let mut web_config = restored.strategy_configs().unwrap().remove(1);
        web_config.grid.buy_window = 9;
        db.save_pair_config("BTCUSDT", &web_config).unwrap();
        restored = db.load_config().unwrap().unwrap();
        restored.apply_environment(&environment).unwrap();
        let strategies = restored.strategy_configs().unwrap();
        assert_eq!(strategies[0].grid.grid_interval, dec!(0.1));
        assert_eq!(strategies[1].grid.grid_interval, dec!(10));
        assert_eq!(strategies[1].grid.buy_window, 3);
        assert!(strategies.iter().all(|config| config.exchange.dry_run));
        assert!(strategies
            .iter()
            .all(|config| config.exchange.api_key.is_empty()));
    }

    #[test]
    fn globals_override_saved_pairs_and_pair_env_wins_over_globals() {
        let mut config = AppConfig::default();
        config.exchange.api_key = "saved-key".into();
        let mut pair = TradingPairConfig::new("BTCUSDT".into());
        pair.update_from(&config);
        config.pairs.push(pair);
        config
            .apply_environment(&values(&[
                ("BINANCE_API_KEY", "new-key"),
                ("BINANCE_API_SECRET", "new-secret"),
                ("BINANCE_DRY_RUN", "false"),
                ("BINANCE_SYNC_INTERVAL_SECS", "6"),
                ("PAIR_BTCUSDT_BINANCE_DRY_RUN", "true"),
                ("PAIR_BTCUSDT_BINANCE_SYNC_INTERVAL_SECS", "9"),
            ]))
            .unwrap();
        let strategy = config.strategy_configs().unwrap().remove(0);
        assert_eq!(strategy.exchange.api_key, "new-key");
        assert_eq!(strategy.exchange.api_secret, "new-secret");
        assert!(strategy.exchange.dry_run);
        assert_eq!(strategy.exchange.sync_interval_secs, 9);
    }

    #[test]
    fn unspecified_fields_and_saved_pairs_survive_env_overrides() {
        let mut config = AppConfig::default();
        config.grid.buy_window = 7;
        let mut pair = TradingPairConfig::new("BTCUSDT".into());
        pair.grid = Some(super::super::GridConfig {
            grid_interval: dec!(10),
            ..Default::default()
        });
        config.pairs.push(pair);
        assert!(!config.apply_environment(&HashMap::new()).unwrap());
        config
            .apply_environment(&values(&[("GRID_ORDER_AMOUNT_USDC", "80")]))
            .unwrap();
        let strategy = config.strategy_configs().unwrap().remove(0);
        assert_eq!(strategy.exchange.symbol, "BTCUSDT");
        assert_eq!(strategy.grid.grid_interval, dec!(10));
        assert_eq!(strategy.grid.buy_window, 5);
        assert_eq!(strategy.grid.order_amount_usdc, dec!(80));
        assert_eq!(config.grid.buy_window, 7);
    }

    #[test]
    fn blanks_clear_credentials_and_optional_limits_but_skip_other_scalars() {
        let mut config = AppConfig::default();
        config.exchange.api_key = "saved".into();
        config.grid.min_price = Some(dec!(50));
        config
            .apply_environment(&values(&[
                ("BINANCE_API_KEY", ""),
                ("GRID_MIN_PRICE", " "),
                ("GRID_INTERVAL", ""),
                ("BINANCE_DRY_RUN", ""),
                ("GRID_MAX_POSITION_USDC", ""),
            ]))
            .unwrap();
        assert!(config.exchange.api_key.is_empty());
        assert!(config.grid.min_price.is_none());
        assert!(config.grid.max_position_usdc.is_none());
        assert_eq!(config.grid.grid_interval, dec!(0.1));
        assert!(config.exchange.dry_run);
    }

    #[test]
    fn malformed_env_is_atomic_and_does_not_echo_values() {
        let mut config = AppConfig::default();
        let original = serde_json::to_value(&config).unwrap();
        let error = config
            .apply_environment(&values(&[
                ("GRID_BUY_WINDOW", "2"),
                ("GRID_INTERVAL", "sensitive-malformed-value"),
            ]))
            .unwrap_err()
            .to_string();
        assert!(error.contains("GRID_INTERVAL"));
        assert!(!error.contains("sensitive-malformed-value"));
        assert_eq!(serde_json::to_value(config).unwrap(), original);
        for list in ["", "SOLUSDC,SOLUSDC", "SOLUSDC,"] {
            assert!(AppConfig::default()
                .apply_environment(&values(&[("TRADING_PAIRS", list)]))
                .is_err());
        }
        let error = AppConfig::default()
            .apply_environment(&values(&[("TRADING_PAIRS", "sensitive-malformed-value")]))
            .unwrap_err()
            .to_string();
        assert!(error.contains("TRADING_PAIRS"));
        assert!(!error.to_lowercase().contains("sensitive-malformed-value"));
        assert!(AppConfig::default()
            .apply_environment(&values(&[("BINANCE_DRY_RUN", "maybe")]))
            .is_err());
    }
}
