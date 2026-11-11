use anyhow::Context;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(crate) struct ConfigVersionConflict(pub &'static str);
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
    #[serde(default)]
    pub global_max_position_notional: Option<Decimal>,
    #[serde(default)]
    pub pause_on_zero_available_margin: bool,
    /// Pause all strategies when available balance falls to this percentage of margin balance.
    #[serde(default)]
    pub min_available_margin_pct: Option<Decimal>,
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

mod internal {
    use super::{redact_optional_secret, redact_secret, AppConfig, TelegramConfig};
    use rust_decimal::Decimal;
    use serde::{Deserialize, Serialize};

    /// Legacy per-pair connection fields, read only to migrate older configurations.
    #[derive(Clone, Default, Serialize, Deserialize)]
    pub struct PairExchangeConfig {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) api_key: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) api_secret: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) recv_window: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub(crate) sync_interval_secs: Option<u64>,
    }

    impl std::fmt::Debug for PairExchangeConfig {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let Self {
                api_key,
                api_secret,
                recv_window,
                sync_interval_secs,
            } = self;
            f.debug_struct("PairExchangeConfig")
                .field("api_key", &redact_optional_secret(api_key))
                .field("api_secret", &redact_optional_secret(api_secret))
                .field("recv_window", recv_window)
                .field("sync_interval_secs", sync_interval_secs)
                .finish()
        }
    }

    /// Shared connection and notification settings, without a pair's symbol or mode.
    #[derive(Clone, PartialEq, Serialize)]
    pub struct GlobalConfig {
        pub(crate) api_key: String,
        pub(crate) api_secret: String,
        pub(crate) recv_window: u64,
        pub(crate) sync_interval_secs: u64,
        pub(crate) telegram: TelegramConfig,
        pub(crate) global_max_position_notional: Option<Decimal>,
        pub(crate) pause_on_zero_available_margin: bool,
        pub(crate) min_available_margin_pct: Option<Decimal>,
    }

    impl GlobalConfig {
        pub(crate) fn from_app(config: &AppConfig) -> Self {
            Self {
                api_key: config.exchange.api_key.clone(),
                api_secret: config.exchange.api_secret.clone(),
                recv_window: config.exchange.recv_window,
                sync_interval_secs: config.exchange.sync_interval_secs,
                telegram: config.telegram.clone(),
                global_max_position_notional: config.global_max_position_notional,
                pause_on_zero_available_margin: config.pause_on_zero_available_margin,
                min_available_margin_pct: config.min_available_margin_pct,
            }
        }

        pub(crate) fn apply_to(&self, config: &mut AppConfig) {
            config.exchange.api_key = self.api_key.clone();
            config.exchange.api_secret = self.api_secret.clone();
            config.exchange.recv_window = self.recv_window;
            config.exchange.sync_interval_secs = self.sync_interval_secs;
            config.telegram = self.telegram.clone();
            config.global_max_position_notional = self.global_max_position_notional;
            config.pause_on_zero_available_margin = self.pause_on_zero_available_margin;
            config.min_available_margin_pct = self.min_available_margin_pct;
        }
    }

    impl std::fmt::Debug for GlobalConfig {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let Self {
                api_key,
                api_secret,
                recv_window,
                sync_interval_secs,
                telegram,
                global_max_position_notional,
                pause_on_zero_available_margin,
                min_available_margin_pct,
            } = self;
            f.debug_struct("GlobalConfig")
                .field("api_key", &redact_secret(api_key))
                .field("api_secret", &redact_secret(api_secret))
                .field("recv_window", recv_window)
                .field("sync_interval_secs", sync_interval_secs)
                .field("telegram", telegram)
                .field("global_max_position_notional", global_max_position_notional)
                .field(
                    "pause_on_zero_available_margin",
                    pause_on_zero_available_margin,
                )
                .field("min_available_margin_pct", min_available_margin_pct)
                .finish()
        }
    }
}

pub(crate) use internal::{GlobalConfig, PairExchangeConfig};
impl TradingPairConfig {
    pub fn new(symbol: String) -> Self {
        Self {
            symbol,
            grid: None,
            exchange: PairExchangeConfig::default(),
            telegram: None,
        }
    }

    pub(crate) fn update_from(&mut self, config: &AppConfig) {
        self.grid = Some(config.grid);
        self.exchange = PairExchangeConfig::default();
        self.telegram = None;
    }
}

#[derive(Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TelegramConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub bot_token: String,
    #[serde(default)]
    pub chat_id: String,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct ExchangeConfig {
    #[serde(default = "default_symbol")]
    pub symbol: String,
    #[serde(default)]
    pub api_key: String,
    #[serde(default)]
    pub api_secret: String,
    #[serde(default = "default_recv_window")]
    pub recv_window: u64,
    #[serde(default = "default_sync_interval")]
    pub sync_interval_secs: u64,
}

pub(crate) fn redact_secret(value: &str) -> &'static str {
    if value.is_empty() {
        ""
    } else {
        "[REDACTED]"
    }
}

pub(crate) fn redact_optional_secret(value: &Option<String>) -> Option<&'static str> {
    value.as_deref().map(redact_secret)
}

impl std::fmt::Debug for TelegramConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            enabled,
            bot_token,
            chat_id,
        } = self;
        f.debug_struct("TelegramConfig")
            .field("enabled", enabled)
            .field("bot_token", &redact_secret(bot_token))
            .field("chat_id", chat_id)
            .finish()
    }
}

impl std::fmt::Debug for ExchangeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            symbol,
            api_key,
            api_secret,
            recv_window,
            sync_interval_secs,
        } = self;
        f.debug_struct("ExchangeConfig")
            .field("symbol", symbol)
            .field("api_key", &redact_secret(api_key))
            .field("api_secret", &redact_secret(api_secret))
            .field("recv_window", recv_window)
            .field("sync_interval_secs", sync_interval_secs)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct GridConfig {
    /// Long buys to open; short sells to open. Exits are always reduce-only.
    #[serde(default)]
    pub direction: crate::types::GridDirection,
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
    /// Pause and cancel managed orders at the limit or before an opening order would exceed it.
    #[serde(default)]
    pub pause_on_max_position: bool,
    /// At the limit, cancel only opening-side orders and keep the strategy running so
    /// reduce-only exits stay on the book. Mutually exclusive with `pause_on_max_position`.
    #[serde(default)]
    pub cancel_opening_on_max_position: bool,
    /// Stop opening (cancel opening-side orders, keep exits) while the mark price is within
    /// this percentage of the position's liquidation price. Inactive without a position or
    /// a reported liquidation price.
    #[serde(default)]
    pub min_liquidation_distance_pct: Option<Decimal>,
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
    pub direction: crate::types::GridDirection,
    pub grid_interval: Decimal,
    pub order_amount_usdc: Decimal,
    pub buy_window: usize,
    pub sell_window: usize,
    pub post_only: bool,
    pub min_price: Option<Decimal>,
    pub max_price: Option<Decimal>,
    pub max_position_usdc: Option<Decimal>,
    pub pause_on_max_position: bool,
    pub cancel_opening_on_max_position: bool,
    pub min_liquidation_distance_pct: Option<Decimal>,
}

fn default_symbol() -> String {
    "SOLUSDC".to_string()
}

fn default_recv_window() -> u64 {
    5000
}

fn default_sync_interval() -> u64 {
    3
}

fn default_true() -> bool {
    true
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
    "127.0.0.1".to_string()
}

fn default_port() -> u16 {
    8080
}

impl Default for ExchangeConfig {
    fn default() -> Self {
        Self {
            symbol: default_symbol(),
            api_key: String::new(),
            api_secret: String::new(),
            recv_window: default_recv_window(),
            sync_interval_secs: default_sync_interval(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
        }
    }
}

impl Default for GridConfig {
    fn default() -> Self {
        Self {
            direction: crate::types::GridDirection::Long,
            grid_interval: default_grid_interval(),
            order_amount_usdc: default_order_amount(),
            buy_window: default_window_size(),
            sell_window: default_window_size(),
            post_only: default_true(),
            min_price: None,
            max_price: None,
            max_position_usdc: Some(dec!(2000.0)),
            pause_on_max_position: false,
            cancel_opening_on_max_position: false,
            min_liquidation_distance_pct: None,
        }
    }
}

impl AppConfig {
    /// Normalize pair settings and promote legacy connection/notification overrides to globals.
    pub fn ensure_pair_list(&mut self) -> anyhow::Result<()> {
        // Previous versions stored globals in each pair and cleared the shared copy.
        // The first pair supplies the migration baseline; subsequent saves remove overrides.
        if let Some(first) = self.pairs.first() {
            if let Some(value) = &first.exchange.api_key {
                self.exchange.api_key = value.clone();
            }
            if let Some(value) = &first.exchange.api_secret {
                self.exchange.api_secret = value.clone();
            }
            if let Some(value) = first.exchange.recv_window {
                self.exchange.recv_window = value;
            }
            if let Some(value) = first.exchange.sync_interval_secs {
                self.exchange.sync_interval_secs = value;
            }
            if let Some(value) = &first.telegram {
                self.telegram = value.clone();
            }
        }
        self.pairs = self
            .strategy_configs()?
            .iter()
            .map(|config| {
                let mut pair = TradingPairConfig::new(config.exchange.symbol.clone());
                pair.update_from(config);
                pair
            })
            .collect();
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
            config.grid = pair.grid.unwrap_or(self.grid);
            configs.push(config);
        }
        Ok(configs)
    }

    pub(crate) fn summary(&self) -> GridConfigSummary {
        GridConfigSummary {
            symbol: self.exchange.symbol.clone(),
            direction: self.grid.direction,
            grid_interval: self.grid.grid_interval,
            order_amount_usdc: self.grid.order_amount_usdc,
            buy_window: self.grid.buy_window,
            sell_window: self.grid.sell_window,
            post_only: self.grid.post_only,
            min_price: self.grid.min_price,
            max_price: self.grid.max_price,
            max_position_usdc: self.grid.max_position_usdc,
            pause_on_max_position: self.grid.pause_on_max_position,
            cancel_opening_on_max_position: self.grid.cancel_opening_on_max_position,
            min_liquidation_distance_pct: self.grid.min_liquidation_distance_pct,
        }
    }
}

impl ExchangeConfig {
    /// Paper mode is the absence of API credentials; configured credentials run live.
    pub fn trading_mode(&self) -> crate::types::TradingMode {
        if self.api_key.trim().is_empty() && self.api_secret.trim().is_empty() {
            crate::types::TradingMode::Paper
        } else {
            crate::types::TradingMode::Live
        }
    }

    pub(crate) fn is_paper(&self) -> bool {
        self.trading_mode() == crate::types::TradingMode::Paper
    }
}

#[path = "config_validation.rs"]
mod config_validation;
pub use config_validation::validate_symbol;
pub(crate) use config_validation::{
    validate_global_position_limit, validate_min_available_margin_pct, MAX_SYNC_INTERVAL_SECS,
};

#[cfg(test)]
#[path = "config_tests.rs"]
mod tests;
