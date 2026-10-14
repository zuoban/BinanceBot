use anyhow::Context;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::path::Path;

mod environment;

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
    /// Omitted grids inherit the global [grid] defaults.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grid: Option<GridConfig>,
    #[serde(default)]
    pub exchange: PairExchangeConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub telegram: Option<TelegramConfig>,
}

/// Credentials and execution settings inherit [exchange] unless overridden.
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
                api_key: std::env::var("BINANCE_API_KEY").unwrap_or_default(),
                api_secret: std::env::var("BINANCE_API_SECRET").unwrap_or_default(),
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

    pub fn load_from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path.as_ref())
            .with_context(|| format!("Failed to read config file: {:?}", path.as_ref()))?;
        let config: AppConfig =
            toml::from_str(&content).with_context(|| "Failed to parse config TOML format")?;
        Ok(config)
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()> {
        let content = toml::to_string_pretty(self)
            .with_context(|| "Failed to serialize configuration to TOML")?;
        std::fs::write(path.as_ref(), content)
            .with_context(|| format!("Failed to write configuration to {:?}", path.as_ref()))?;
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
        let mut legacy = toml::Value::try_from(&config).unwrap();
        legacy["grid"]
            .as_table_mut()
            .unwrap()
            .remove("pause_on_max_position");
        let restored: AppConfig = toml::from_str(&toml::to_string(&legacy).unwrap()).unwrap();
        assert!(!restored.grid.pause_on_max_position);

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
    fn multi_pair_toml_inherits_defaults_and_round_trips() {
        let source = r#"
            [exchange]
            api_key = "shared-key"
            api_secret = "shared-secret"
            [grid]
            grid_interval = "1"
            order_amount_usdc = "50"
            [server]
            [[pairs]]
            symbol = "SOLUSDC"
            [[pairs]]
            symbol = "BTCUSDT"
            [pairs.grid]
            grid_interval = "10"
            order_amount_usdc = "200"
            buy_window = 3
            [pairs.exchange]
            is_testnet = true
            dry_run = false
        "#;
        let config: AppConfig = toml::from_str(source).unwrap();
        config.validate().unwrap();
        let configs = config.strategy_configs().unwrap();
        assert_eq!(configs.len(), 2);
        assert_eq!(configs[0].exchange.symbol, "SOLUSDC");
        assert_eq!(configs[0].grid.grid_interval, dec!(1));
        assert_eq!(configs[0].grid.order_amount_usdc, dec!(50));
        assert!(configs[0].exchange.dry_run);
        assert_eq!(configs[1].exchange.symbol, "BTCUSDT");
        assert_eq!(configs[1].grid.grid_interval, dec!(10));
        assert_eq!(configs[1].grid.buy_window, 3);
        assert_eq!(configs[1].exchange.api_key, "shared-key");
        assert!(configs[1].exchange.is_testnet);
        assert!(!configs[1].exchange.dry_run);
        assert!(configs.iter().all(|c| c.pairs.is_empty()));
        let restored: AppConfig =
            toml::from_str(&toml::to_string_pretty(&config).unwrap()).unwrap();
        assert_eq!(
            serde_json::to_value(config).unwrap(),
            serde_json::to_value(restored).unwrap()
        );
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
