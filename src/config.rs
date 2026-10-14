use anyhow::Context;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub exchange: ExchangeConfig,
    #[serde(default)]
    pub grid: GridConfig,
    /// Empty preserves the legacy single-pair configuration.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pairs: Vec<TradingPairConfig>,
    pub server: ServerConfig,
    #[serde(default)]
    pub telegram: TelegramConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingPairConfig {
    pub symbol: String,
    /// Omitted grids inherit the saved global defaults.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grid: Option<GridConfig>,
    #[serde(default)]
    pub exchange: PairExchangeConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub telegram: Option<TelegramConfig>,
}

/// Credentials and execution settings inherit saved defaults unless overridden.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PairExchangeConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_testnet: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dry_run: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recv_window: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sync_interval_secs: Option<u64>,
}

impl PairExchangeConfig {
    fn apply(&self, exchange: &mut ExchangeConfig) {
        if let Some(value) = &self.api_key {
            exchange.api_key = value.clone();
        }
        if let Some(value) = &self.api_secret {
            exchange.api_secret = value.clone();
        }
        if let Some(value) = self.is_testnet {
            exchange.is_testnet = value;
        }
        if let Some(value) = self.dry_run {
            exchange.dry_run = value;
        }
        if let Some(value) = self.recv_window {
            exchange.recv_window = value;
        }
        if let Some(value) = self.sync_interval_secs {
            exchange.sync_interval_secs = value;
        }
    }
}

impl TradingPairConfig {
    pub fn new(symbol: String) -> Self {
        Self {
            symbol,
            grid: None,
            exchange: PairExchangeConfig::default(),
            telegram: None,
        }
    }

    pub fn update_from(&mut self, config: &AppConfig) {
        self.grid = Some(config.grid.clone());
        self.exchange = PairExchangeConfig {
            api_key: Some(config.exchange.api_key.clone()),
            api_secret: Some(config.exchange.api_secret.clone()),
            is_testnet: Some(config.exchange.is_testnet),
            dry_run: Some(config.exchange.dry_run),
            recv_window: Some(config.exchange.recv_window),
            sync_interval_secs: Some(config.exchange.sync_interval_secs),
        };
        self.telegram = Some(config.telegram.clone());
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TelegramConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub bot_token: String,
    #[serde(default)]
    pub chat_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExchangeConfig {
    #[serde(default = "default_symbol")]
    pub symbol: String,
    #[serde(default)]
    pub api_key: String,
    #[serde(default)]
    pub api_secret: String,
    #[serde(default = "default_false")]
    pub is_testnet: bool,
    #[serde(default = "default_true")]
    pub dry_run: bool,
    #[serde(default = "default_recv_window")]
    pub recv_window: u64,
    #[serde(default = "default_sync_interval")]
    pub sync_interval_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GridConfig {
    /// Grid spacing / interval (e.g. 0.1 USDC)
    #[serde(default = "default_grid_interval")]
    pub grid_interval: Decimal,

    /// Order notional amount per grid (e.g. 100 USDC)
    #[serde(default = "default_order_amount")]
    pub order_amount_usdc: Decimal,

    /// Number of pre-placed buy orders below current price
    #[serde(default = "default_window_size")]
    pub buy_window: usize,

    /// Number of pre-placed sell orders above current price
    #[serde(default = "default_window_size")]
    pub sell_window: usize,

    /// Always place Maker orders (Post-Only, timeInForce=GTX)
    #[serde(default = "default_true")]
    pub post_only: bool,

    /// Minimum price limit for grid execution
    pub min_price: Option<Decimal>,

    /// Maximum price limit for grid execution
    pub max_price: Option<Decimal>,

    /// Maximum position limit in USDC value
    pub max_position_usdc: Option<Decimal>,

    /// Pause and cancel managed orders at the position limit or before a buy would exceed it.
    #[serde(default)]
    pub pause_on_max_position: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
}

impl ServerConfig {
    pub fn socket_addr(&self) -> anyhow::Result<std::net::SocketAddr> {
        anyhow::ensure!(self.port > 0, "监听端口必须在 1-65535 之间");
        let host = self
            .host
            .parse::<std::net::IpAddr>()
            .context("监听地址必须是有效的 IPv4 或 IPv6 地址")?;
        Ok(std::net::SocketAddr::new(host, self.port))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridConfigSummary {
    pub symbol: String,
    pub grid_interval: Decimal,
    pub order_amount_usdc: Decimal,
    pub buy_window: usize,
    pub sell_window: usize,
    pub post_only: bool,
    pub min_price: Option<Decimal>,
    pub max_price: Option<Decimal>,
    pub max_position_usdc: Option<Decimal>,
    pub pause_on_max_position: bool,
    pub is_testnet: bool,
    pub dry_run: bool,
}

fn default_true() -> bool {
    true
}

fn default_symbol() -> String {
    "SOLUSDC".to_string()
}

fn default_false() -> bool {
    false
}

fn default_recv_window() -> u64 {
    5000
}

fn default_sync_interval() -> u64 {
    3
}

fn default_grid_interval() -> Decimal {
    dec!(0.1)
}

fn default_order_amount() -> Decimal {
    dec!(100.0)
}

fn default_window_size() -> usize {
    5
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    8080
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            exchange: ExchangeConfig {
                symbol: "SOLUSDC".to_string(),
                api_key: String::new(),
                api_secret: String::new(),
                is_testnet: false,
                dry_run: true,
                recv_window: 5000,
                sync_interval_secs: 3,
            },
            grid: GridConfig::default(),
            pairs: Vec::new(),
            server: ServerConfig {
                host: "0.0.0.0".to_string(),
                port: 8080,
            },
            telegram: TelegramConfig::default(),
        }
    }
}

impl Default for GridConfig {
    fn default() -> Self {
        Self {
            grid_interval: dec!(0.1),
            order_amount_usdc: dec!(100.0),
            buy_window: 5,
            sell_window: 5,
            post_only: true,
            min_price: None,
            max_price: None,
            max_position_usdc: Some(dec!(2000.0)),
            pause_on_max_position: false,
        }
    }
}

impl AppConfig {
    /// Upgrade legacy single-pair SQLite settings without changing strategy parameters.
    pub fn ensure_pair_list(&mut self) -> anyhow::Result<()> {
        // Materialize inherited legacy credentials before clearing the obsolete shared copy.
        self.pairs = self
            .strategy_configs()?
            .iter()
            .map(|config| {
                let mut pair = TradingPairConfig::new(config.exchange.symbol.clone());
                pair.update_from(config);
                pair
            })
            .collect();
        self.exchange.api_key.clear();
        self.exchange.api_secret.clear();
        self.telegram = TelegramConfig::default();
        Ok(())
    }

    /// Resolve independent engine configs; the global symbol is ignored when pairs are explicit.
    pub fn strategy_configs(&self) -> anyhow::Result<Vec<Self>> {
        if self.pairs.is_empty() {
            return Ok(vec![self.clone()]);
        }
        let mut symbols = std::collections::HashSet::new();
        let mut configs = Vec::with_capacity(self.pairs.len());
        for pair in &self.pairs {
            validate_symbol(&pair.symbol)?;
            anyhow::ensure!(
                symbols.insert(&pair.symbol),
                "重复配置交易对: {}",
                pair.symbol
            );
            let mut config = self.clone();
            config.pairs.clear();
            config.exchange.symbol = pair.symbol.clone();
            pair.exchange.apply(&mut config.exchange);
            config.grid = pair.grid.clone().unwrap_or_else(|| self.grid.clone());
            config.telegram = pair
                .telegram
                .clone()
                .unwrap_or_else(|| self.telegram.clone());
            configs.push(config);
        }
        Ok(configs)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if !self.pairs.is_empty() {
            for config in self.strategy_configs()? {
                config
                    .validate()
                    .with_context(|| format!("交易对 {} 配置无效", config.exchange.symbol))?;
            }
            return Ok(());
        }
        validate_symbol(&self.exchange.symbol)?;
        anyhow::ensure!(
            self.grid.grid_interval > Decimal::ZERO,
            "网格间距必须大于 0"
        );
        anyhow::ensure!(
            self.grid.order_amount_usdc > Decimal::ZERO,
            "每格金额必须大于 0"
        );
        anyhow::ensure!(
            self.grid.post_only,
            "滑动网格策略必须开启 Post-Only Maker 挂单"
        );
        anyhow::ensure!(
            (1..=50).contains(&self.grid.buy_window) && (1..=50).contains(&self.grid.sell_window),
            "买入和卖出窗口必须在 1-50 之间"
        );
        if let Some(minimum) = self.grid.min_price {
            anyhow::ensure!(minimum > Decimal::ZERO, "价格下限必须大于 0");
        }
        if let Some(maximum) = self.grid.max_price {
            anyhow::ensure!(maximum > Decimal::ZERO, "价格上限必须大于 0");
        }
        if let (Some(minimum), Some(maximum)) = (self.grid.min_price, self.grid.max_price) {
            anyhow::ensure!(minimum < maximum, "价格下限必须小于上限");
        }
        if let Some(limit) = self.grid.max_position_usdc {
            anyhow::ensure!(limit > Decimal::ZERO, "最大持仓金额必须大于 0");
        }
        anyhow::ensure!(
            !self.grid.pause_on_max_position || self.grid.max_position_usdc.is_some(),
            "启用达到上限时暂停策略前，请设置最大持仓名义价值"
        );
        anyhow::ensure!(self.exchange.sync_interval_secs > 0, "同步间隔必须大于 0");
        anyhow::ensure!(
            (1..=60_000).contains(&self.exchange.recv_window),
            "请求有效窗口必须在 1-60000 毫秒之间"
        );
        if !self.exchange.dry_run {
            anyhow::ensure!(
                !self.exchange.api_key.trim().is_empty()
                    && !self.exchange.api_secret.trim().is_empty(),
                "实盘或测试网需要配置 API Key 和 Secret"
            );
        }
        anyhow::ensure!(
            !self.telegram.enabled
                || (!self.telegram.bot_token.trim().is_empty()
                    && !self.telegram.chat_id.trim().is_empty()),
            "启用 Telegram 通知前请填写 Bot Token 和 Chat ID"
        );
        Ok(())
    }

    pub fn summary(&self) -> GridConfigSummary {
        GridConfigSummary {
            symbol: self.exchange.symbol.clone(),
            grid_interval: self.grid.grid_interval,
            order_amount_usdc: self.grid.order_amount_usdc,
            buy_window: self.grid.buy_window,
            sell_window: self.grid.sell_window,
            post_only: self.grid.post_only,
            min_price: self.grid.min_price,
            max_price: self.grid.max_price,
            max_position_usdc: self.grid.max_position_usdc,
            pause_on_max_position: self.grid.pause_on_max_position,
            is_testnet: self.exchange.is_testnet,
            dry_run: self.exchange.dry_run,
        }
    }
}

fn validate_symbol(symbol: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        (3..=32).contains(&symbol.len())
            && symbol
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit()),
        "交易对只能包含 3-32 位大写字母和数字: {}",
        symbol
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::AppConfig;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    #[test]
    fn legacy_shared_credentials_become_independent_pair_settings() {
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
        assert!(config.exchange.api_key.is_empty());
        assert!(config.telegram.bot_token.is_empty());
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
        assert!(strategies[0].exchange.api_key.is_empty());
        assert_eq!(strategies[1].exchange.api_key, "legacy-key");
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
    fn multi_pair_json_inherits_defaults_and_round_trips() {
        let config: AppConfig = serde_json::from_value(serde_json::json!({
            "exchange": {"api_key": "shared-key", "api_secret": "shared-secret"},
            "grid": {"grid_interval": "1", "order_amount_usdc": "50"},
            "server": {},
            "pairs": [
                {"symbol": "SOLUSDC"},
                {"symbol": "BTCUSDT", "grid": {"grid_interval": "10", "order_amount_usdc": "200", "buy_window": 3},
                 "exchange": {"is_testnet": true, "dry_run": false}}
            ]
        })).unwrap();
        config.validate().unwrap();
        let configs = config.strategy_configs().unwrap();
        assert_eq!(configs.len(), 2);
        assert_eq!(configs[0].grid.grid_interval, dec!(1));
        assert_eq!(configs[0].grid.order_amount_usdc, dec!(50));
        assert!(configs[0].exchange.dry_run);
        assert_eq!(configs[1].grid.grid_interval, dec!(10));
        assert_eq!(configs[1].grid.buy_window, 3);
        assert_eq!(configs[1].exchange.api_key, "shared-key");
        assert!(configs[1].exchange.is_testnet);
        assert!(!configs[1].exchange.dry_run);
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
        config.pairs[1].exchange.dry_run = Some(false);
        config.exchange.api_key.clear();
        config.exchange.api_secret.clear();
        assert!(config.validate().is_err());
    }
}
