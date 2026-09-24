use anyhow::Context;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub exchange: ExchangeConfig,
    pub grid: GridConfig,
    pub server: ServerConfig,
    #[serde(default)]
    pub telegram: TelegramConfig,
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
    pub is_testnet: bool,
    pub dry_run: bool,
}

fn default_true() -> bool {
    true
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
            grid: GridConfig {
                grid_interval: dec!(0.1),
                order_amount_usdc: dec!(100.0),
                buy_window: 5,
                sell_window: 5,
                post_only: true,
                min_price: None,
                max_price: None,
                max_position_usdc: Some(dec!(2000.0)),
            },
            server: ServerConfig {
                host: "0.0.0.0".to_string(),
                port: 8080,
            },
            telegram: TelegramConfig::default(),
        }
    }
}

impl AppConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        let symbol = &self.exchange.symbol;
        anyhow::ensure!(
            symbol.len() >= 3
                && symbol.len() <= 32
                && symbol
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit()),
            "交易对只能包含 3-32 位大写字母和数字"
        );
        anyhow::ensure!(
            self.grid.grid_interval > Decimal::ZERO,
            "网格间距必须大于 0"
        );
        anyhow::ensure!(
            self.grid.order_amount_usdc > Decimal::ZERO,
            "每格金额必须大于 0"
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
        anyhow::ensure!(self.exchange.sync_interval_secs > 0, "同步间隔必须大于 0");
        if !self.exchange.dry_run {
            anyhow::ensure!(
                !self.exchange.api_key.trim().is_empty()
                    && !self.exchange.api_secret.trim().is_empty(),
                "实盘或测试网需要配置 API Key 和 Secret"
            );
        }
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
            is_testnet: self.exchange.is_testnet,
            dry_run: self.exchange.dry_run,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AppConfig;

    #[test]
    fn existing_config_without_telegram_still_loads() {
        let mut legacy = serde_json::to_value(AppConfig::default()).unwrap();
        legacy.as_object_mut().unwrap().remove("telegram");
        let config: AppConfig = serde_json::from_value(legacy).unwrap();
        assert!(!config.telegram.enabled);
        assert!(config.telegram.bot_token.is_empty());
        assert!(config.telegram.chat_id.is_empty());
    }
}
