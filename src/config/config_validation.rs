use super::{AppConfig, ExchangeConfig, GridConfig, TelegramConfig};
use anyhow::Context;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

const MAX_CONFIG_NOTIONAL_OR_PRICE: Decimal = dec!(1_000_000_000_000);
pub(crate) const MAX_SYNC_INTERVAL_SECS: u64 = 300;

impl AppConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        validate_global_position_limit(self.global_max_position_notional)?;
        validate_min_available_margin_pct(self.min_available_margin_pct)?;
        if !self.pairs.is_empty() {
            for config in self.strategy_configs()? {
                config
                    .validate()
                    .with_context(|| format!("交易对 {} 配置无效", config.exchange.symbol))?;
            }
            return Ok(());
        }
        validate_symbol(&self.exchange.symbol)?;
        self.grid.validate()?;
        self.exchange.validate()?;
        self.telegram.validate()?;
        Ok(())
    }
}

impl GridConfig {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.grid_interval > Decimal::ZERO, "网格间距必须大于 0");
        anyhow::ensure!(
            self.grid_interval <= MAX_CONFIG_NOTIONAL_OR_PRICE,
            "网格间距超出允许上限"
        );
        anyhow::ensure!(self.order_amount_usdc > Decimal::ZERO, "每格金额必须大于 0");
        anyhow::ensure!(
            self.order_amount_usdc <= MAX_CONFIG_NOTIONAL_OR_PRICE,
            "每格金额超出允许上限"
        );
        anyhow::ensure!(self.post_only, "滑动网格策略必须开启 Post-Only Maker 挂单");
        anyhow::ensure!(
            (1..=50).contains(&self.buy_window) && (1..=50).contains(&self.sell_window),
            "买入和卖出窗口必须在 1-50 之间"
        );
        if let Some(minimum) = self.min_price {
            anyhow::ensure!(minimum > Decimal::ZERO, "价格下限必须大于 0");
            anyhow::ensure!(
                minimum <= MAX_CONFIG_NOTIONAL_OR_PRICE,
                "价格下限超出允许上限"
            );
        }
        if let Some(maximum) = self.max_price {
            anyhow::ensure!(maximum > Decimal::ZERO, "价格上限必须大于 0");
            anyhow::ensure!(
                maximum <= MAX_CONFIG_NOTIONAL_OR_PRICE,
                "价格上限超出允许上限"
            );
        }
        if let (Some(minimum), Some(maximum)) = (self.min_price, self.max_price) {
            anyhow::ensure!(minimum < maximum, "价格下限必须小于上限");
        }
        if let Some(limit) = self.max_position_usdc {
            anyhow::ensure!(limit > Decimal::ZERO, "最大持仓金额必须大于 0");
            anyhow::ensure!(
                limit <= MAX_CONFIG_NOTIONAL_OR_PRICE,
                "最大持仓金额超出允许上限"
            );
        }
        anyhow::ensure!(
            !self.pause_on_max_position || self.max_position_usdc.is_some(),
            "启用达到上限时暂停策略前，请设置最大持仓名义价值"
        );
        anyhow::ensure!(
            !self.cancel_opening_on_max_position || self.max_position_usdc.is_some(),
            "启用达到上限时撤销开仓单前，请设置最大持仓名义价值"
        );
        anyhow::ensure!(
            !(self.pause_on_max_position && self.cancel_opening_on_max_position),
            "达到上限时暂停策略与仅撤销开仓单不能同时启用"
        );
        if let Some(distance) = self.min_liquidation_distance_pct {
            anyhow::ensure!(
                distance > Decimal::ZERO && distance < dec!(100),
                "强平距离阈值必须大于 0 且小于 100（百分比）"
            );
        }
        Ok(())
    }
}

impl ExchangeConfig {
    pub(crate) fn validate_api_key(api_key: &str) -> anyhow::Result<()> {
        anyhow::ensure!(
            !api_key.is_empty()
                && api_key.chars().all(|ch| ch.is_ascii_graphic())
                && reqwest::header::HeaderValue::from_str(api_key).is_ok(),
            "API Key 格式无效（仅允许可见 ASCII 字符，不能包含空白或控制字符）"
        );
        Ok(())
    }

    pub(super) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (1..=MAX_SYNC_INTERVAL_SECS).contains(&self.sync_interval_secs),
            "同步间隔必须在 1-{MAX_SYNC_INTERVAL_SECS} 秒之间"
        );
        anyhow::ensure!(
            (1..=60_000).contains(&self.recv_window),
            "请求有效窗口必须在 1-60000 毫秒之间"
        );
        anyhow::ensure!(
            self.api_key.trim().is_empty() == self.api_secret.trim().is_empty(),
            "API Key 和 Secret 必须同时配置：配置后以实盘运行，全部清空后以模拟盘运行"
        );
        if !self.api_key.trim().is_empty() {
            Self::validate_api_key(&self.api_key)?;
        }
        Ok(())
    }
}

impl TelegramConfig {
    pub(crate) fn validate_bot_token(token: &str) -> anyhow::Result<()> {
        anyhow::ensure!(
            !token.is_empty()
                && !token.chars().any(|ch| {
                    matches!(ch, '/' | '?' | '#' | '\\')
                        || ch.is_whitespace()
                        || ch.is_ascii_control()
                }),
            "Telegram Bot Token 格式无效"
        );
        Ok(())
    }

    pub(super) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.enabled || (!self.bot_token.trim().is_empty() && !self.chat_id.trim().is_empty()),
            "启用 Telegram 通知前请填写 Bot Token 和 Chat ID"
        );
        if self.enabled || !self.bot_token.is_empty() {
            Self::validate_bot_token(&self.bot_token)?;
        }
        Ok(())
    }
}

pub(crate) fn validate_global_position_limit(limit: Option<Decimal>) -> anyhow::Result<()> {
    if let Some(limit) = limit {
        anyhow::ensure!(limit > Decimal::ZERO, "全局最大持仓金额必须大于 0");
        anyhow::ensure!(
            limit <= MAX_CONFIG_NOTIONAL_OR_PRICE,
            "全局最大持仓金额超出允许上限"
        );
    }
    Ok(())
}

pub(crate) fn validate_min_available_margin_pct(pct: Option<Decimal>) -> anyhow::Result<()> {
    if let Some(pct) = pct {
        anyhow::ensure!(
            pct > Decimal::ZERO && pct < dec!(100),
            "可用保证金比例阈值必须大于 0 且小于 100（百分比）"
        );
    }
    Ok(())
}

pub fn validate_symbol(symbol: &str) -> anyhow::Result<()> {
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
