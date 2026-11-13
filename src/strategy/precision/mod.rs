use crate::exchange::model::{BinanceExchangeInfo, BinanceFilter, BinanceSymbolInfo};
use crate::types::OrderSide;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::{debug, warn};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SymbolRules {
    pub(crate) symbol: String,
    pub(crate) tick_size: Decimal,
    pub(crate) price_scale: u32,
    pub(crate) min_price: Decimal,
    pub(crate) max_price: Decimal,

    pub(crate) step_size: Decimal,
    pub(crate) quantity_scale: u32,
    pub(crate) min_qty: Decimal,
    pub(crate) max_qty: Decimal,

    pub(crate) market_step_size: Decimal,
    pub(crate) market_quantity_scale: u32,
    pub(crate) market_min_qty: Decimal,
    pub(crate) market_max_qty: Decimal,

    pub(crate) min_notional: Decimal,
}

impl Default for SymbolRules {
    fn default() -> Self {
        Self {
            symbol: String::new(),
            tick_size: dec!(0.01),
            price_scale: 2,
            min_price: dec!(0.1),
            max_price: dec!(100000.0),
            step_size: dec!(0.01),
            quantity_scale: 2,
            min_qty: dec!(0.01),
            max_qty: dec!(800000.0),
            market_step_size: dec!(0.01),
            market_quantity_scale: 2,
            market_min_qty: dec!(0.01),
            market_max_qty: dec!(800000.0),
            min_notional: dec!(5.0),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SymbolRulesError {
    NotFound,
    NotTrading,
    MissingFilters,
}

impl SymbolRulesError {
    #[must_use]
    #[inline]
    pub(crate) fn into_chinese_error(self, symbol: &str) -> anyhow::Error {
        match self {
            Self::NotFound => anyhow::anyhow!("币安合约不存在交易对 {symbol}"),
            Self::NotTrading => anyhow::anyhow!("交易对 {symbol} 当前未处于可交易状态"),
            Self::MissingFilters => anyhow::anyhow!("无法解析交易对 {symbol} 的价格或数量精度规则"),
        }
    }

    #[must_use]
    #[inline]
    pub(crate) fn preview_status_and_message(self) -> (axum::http::StatusCode, &'static str) {
        use axum::http::StatusCode;
        match self {
            Self::NotFound => (StatusCode::NOT_FOUND, "交易对不存在"),
            Self::NotTrading => (StatusCode::BAD_REQUEST, "该交易对当前未处于交易状态"),
            Self::MissingFilters => (StatusCode::BAD_GATEWAY, "交易对精度规则缺失"),
        }
    }
}

impl SymbolRules {
    #[must_use]
    pub(crate) fn from_exchange_info(
        info: &BinanceExchangeInfo,
        symbol_name: &str,
    ) -> Option<Self> {
        let sym = info
            .symbols
            .iter()
            .find(|s| s.symbol.eq_ignore_ascii_case(symbol_name))?;
        Self::from_symbol_info(sym)
    }

    pub(crate) fn from_trading_exchange_info(
        info: &BinanceExchangeInfo,
        symbol_name: &str,
    ) -> std::result::Result<Self, SymbolRulesError> {
        let sym = info
            .symbols
            .iter()
            .find(|s| s.symbol.eq_ignore_ascii_case(symbol_name))
            .ok_or(SymbolRulesError::NotFound)?;
        if sym.status != "TRADING" {
            return Err(SymbolRulesError::NotTrading);
        }
        Self::from_symbol_info(sym).ok_or(SymbolRulesError::MissingFilters)
    }

    pub(crate) fn parse_and_validate_interval(
        info: &crate::exchange::BinanceExchangeInfo,
        symbol: &str,
        grid_interval: Decimal,
    ) -> anyhow::Result<Self> {
        let rules = Self::from_trading_exchange_info(info, symbol)
            .map_err(|error| error.into_chinese_error(symbol))?;
        rules.validate_grid_interval(grid_interval)?;
        Ok(rules)
    }

    fn has_required_perpetual_filters(sym: &BinanceSymbolInfo) -> bool {
        (sym.contract_type.is_empty() || sym.contract_type == "PERPETUAL")
            && sym.filters.iter().any(|filter| {
                matches!(filter, BinanceFilter::PriceFilter { tick_size, .. } if *tick_size > Decimal::ZERO)
            })
            && sym.filters.iter().any(|filter| {
                matches!(filter, BinanceFilter::LotSize { step_size, .. } if *step_size > Decimal::ZERO)
            })
    }

    fn from_symbol_info(sym: &BinanceSymbolInfo) -> Option<Self> {
        if !Self::has_required_perpetual_filters(sym) {
            return None;
        }
        let (mut tick_size, mut min_price, mut max_price) =
            (dec!(0.01), dec!(0.0001), dec!(1000000.0));
        let (mut step_size, mut min_qty, mut max_qty) = (dec!(0.01), dec!(0.01), dec!(1000000.0));
        let mut market_lot: Option<(Decimal, Decimal, Decimal)> = None;
        let mut min_notional = dec!(5.0);
        for filter in &sym.filters {
            match filter {
                BinanceFilter::PriceFilter {
                    min_price: min_p,
                    max_price: max_p,
                    tick_size: tick,
                } => {
                    tick_size = *tick;
                    if *min_p >= Decimal::ZERO {
                        min_price = *min_p;
                    }
                    if *max_p > Decimal::ZERO && *max_p >= min_price {
                        max_price = *max_p;
                    }
                }
                BinanceFilter::LotSize {
                    min_qty: min_q,
                    max_qty: max_q,
                    step_size: step,
                } => {
                    step_size = *step;
                    if *min_q > Decimal::ZERO {
                        min_qty = *min_q;
                    }
                    if *max_q > Decimal::ZERO && *max_q >= min_qty {
                        max_qty = *max_q;
                    }
                }
                BinanceFilter::MarketLotSize {
                    min_qty: min_q,
                    max_qty: max_q,
                    step_size: step,
                } => {
                    market_lot = Some((*min_q, *max_q, *step));
                }
                BinanceFilter::MinNotional { notional: Some(n) } if *n > Decimal::ZERO => {
                    min_notional = *n;
                }
                _ => {}
            }
        }

        let price_scale = calculate_scale(tick_size);
        let quantity_scale = calculate_scale(step_size);
        let mut market_step_size = step_size;
        let mut market_min_qty = min_qty;
        let mut market_max_qty = max_qty;
        if let Some((m_min, m_max, m_step)) = market_lot {
            if m_step > Decimal::ZERO {
                market_step_size = m_step.max(step_size);
            }
            if m_min > Decimal::ZERO {
                market_min_qty = m_min.max(min_qty);
            }
            if m_max > Decimal::ZERO && m_max >= market_min_qty {
                market_max_qty = m_max.min(max_qty);
            }
        }
        market_max_qty = market_max_qty.max(market_min_qty);
        let market_quantity_scale = calculate_scale(market_step_size);

        debug!(
            "Loaded symbol rules for {}: tick_size={}, step_size={}, min_notional={}",
            sym.symbol, tick_size, step_size, min_notional
        );

        Some(Self {
            symbol: sym.symbol.clone(),
            tick_size,
            price_scale,
            min_price,
            max_price,
            step_size,
            quantity_scale,
            min_qty,
            max_qty,
            market_step_size,
            market_quantity_scale,
            market_min_qty,
            market_max_qty,
            min_notional,
        })
    }

    pub(crate) fn set_tick_size(&mut self, tick_size: Decimal) {
        self.tick_size = tick_size;
        self.price_scale = calculate_scale(tick_size);
    }

    pub(crate) fn set_step_size(&mut self, step_size: Decimal) {
        self.step_size = step_size;
        self.quantity_scale = calculate_scale(step_size);
        self.market_step_size = step_size;
        self.market_quantity_scale = self.quantity_scale;
    }

    /// A fixed lattice must be exactly representable by the exchange price filter.
    pub(crate) fn validate_grid_interval(&self, interval: Decimal) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.tick_size > Decimal::ZERO
                && interval >= self.tick_size
                && interval
                    .checked_div(self.tick_size)
                    .filter(|ticks| ticks.fract().is_zero())
                    .and_then(|ticks| ticks.checked_mul(self.tick_size))
                    == Some(interval),
            "网格间距 {} 必须是价格步长 {} 的正整数倍",
            interval,
            self.tick_size
        );
        Ok(())
    }

    /// Both directions share the same zero-anchored price lattice.
    #[must_use]
    #[inline]
    pub(crate) fn is_grid_price(&self, price: Decimal, interval: Decimal) -> bool {
        interval > Decimal::ZERO
            && price > Decimal::ZERO
            && price
                .checked_div(interval)
                .filter(|steps| steps.fract().is_zero())
                .and_then(|steps| steps.checked_mul(interval))
                == Some(price)
    }

    #[must_use]
    pub(crate) fn is_valid_maker_grid_price(
        &self,
        grid: &crate::config::GridConfig,
        side: OrderSide,
        price: Decimal,
        market: Decimal,
    ) -> bool {
        if price <= Decimal::ZERO || market <= Decimal::ZERO || grid.grid_interval <= Decimal::ZERO
        {
            return false;
        }
        let min_price = grid
            .min_price
            .unwrap_or(self.min_price)
            .max(self.min_price)
            .max(grid.grid_interval);
        let max_price = grid.max_price.unwrap_or(self.max_price).min(self.max_price);
        side.is_maker_price(price, market)
            && price >= min_price
            && price <= max_price
            && self.checked_round_price(price) == Some(price)
            && self.is_grid_price(price, grid.grid_interval)
    }

    /// Zero-anchored levels survive market moves and restarts.
    /// Count eligible levels, skipping reservations without consuming window slots.
    /// At an exact level, neither side places an order at the current market price.
    #[must_use]
    pub(crate) fn grid_window_prices(
        &self,
        market: Decimal,
        interval: Decimal,
        windows: (usize, usize),
        bounds: (Option<Decimal>, Option<Decimal>),
        eligible: impl Fn(OrderSide, Decimal) -> bool,
    ) -> (Vec<Decimal>, Vec<Decimal>) {
        if market <= Decimal::ZERO || interval <= Decimal::ZERO {
            return (Vec::new(), Vec::new());
        }
        let min = bounds
            .0
            .unwrap_or(self.min_price)
            .max(self.min_price)
            .max(interval);
        let max = bounds.1.unwrap_or(self.max_price).min(self.max_price);
        let (Some(market_steps), Some(min_steps), Some(max_steps)) = (
            market.checked_div(interval),
            min.checked_div(interval),
            max.checked_div(interval),
        ) else {
            return (Vec::new(), Vec::new());
        };
        let (Some(below), Some(above)) = (
            market_steps
                .ceil()
                .checked_sub(Decimal::ONE)
                .map(|steps| steps.min(max_steps.floor()))
                .and_then(|steps| steps.checked_mul(interval)),
            market_steps
                .floor()
                .checked_add(Decimal::ONE)
                .map(|steps| steps.max(min_steps.ceil()))
                .and_then(|steps| steps.checked_mul(interval)),
        ) else {
            return (Vec::new(), Vec::new());
        };
        let collect = |side, mut price, count: usize, step| {
            let mut prices = Vec::with_capacity(count.min(1024));
            while prices.len() < count && price >= min && price <= max {
                if self.is_grid_price(price, interval) && eligible(side, price) {
                    prices.push(price);
                }
                let Some(next) = price.checked_add(step) else {
                    break;
                };
                price = next;
            }
            prices
        };
        (
            collect(OrderSide::Buy, below, windows.0, -interval),
            collect(OrderSide::Sell, above, windows.1, interval),
        )
    }

    fn checked_round_price(&self, price: Decimal) -> Option<Decimal> {
        if self.tick_size.is_zero() {
            return Some(price);
        }
        if self.tick_size < Decimal::ZERO {
            return None;
        }
        let ticks = price.checked_div(self.tick_size)?.round();
        let rounded = ticks.checked_mul(self.tick_size)?;
        Some(rounded.trunc_with_scale(self.price_scale))
    }

    /// Round price to the nearest tick size
    #[must_use]
    #[inline]
    pub(crate) fn round_price(&self, price: Decimal) -> Decimal {
        self.checked_round_price(price).unwrap_or(Decimal::ZERO)
    }

    fn checked_round_quantity(&self, qty: Decimal) -> Option<Decimal> {
        if self.step_size.is_zero() {
            return Some(qty);
        }
        if self.step_size < Decimal::ZERO {
            return None;
        }
        let steps = qty.checked_div(self.step_size)?.floor();
        let rounded = steps.checked_mul(self.step_size)?;
        Some(rounded.trunc_with_scale(self.quantity_scale))
    }

    /// Truncate quantity down to the nearest lot step size
    #[must_use]
    #[inline]
    pub(crate) fn round_quantity(&self, qty: Decimal) -> Decimal {
        self.checked_round_quantity(qty).unwrap_or(Decimal::ZERO)
    }

    /// Calculate compliant order quantity given target USDC notional amount and price
    #[must_use]
    pub(crate) fn calculate_quantity(
        &self,
        price: Decimal,
        amount_usdc: Decimal,
    ) -> Option<Decimal> {
        if price <= Decimal::ZERO || amount_usdc <= Decimal::ZERO {
            return None;
        }
        let raw_qty = amount_usdc.checked_div(price)?;
        let qty = self.checked_round_quantity(raw_qty)?;

        let notional = qty.checked_mul(price)?;
        if qty < self.min_qty {
            warn!(
                "Calculated quantity {} is below min_qty {}",
                qty, self.min_qty
            );
            return None;
        }
        if qty > self.max_qty {
            warn!(
                "Calculated quantity {} exceeds max_qty {}",
                qty, self.max_qty
            );
            return None;
        }
        if notional < self.min_notional {
            warn!(
                "Calculated notional {} USDC is below min_notional {}",
                notional, self.min_notional
            );
            return None;
        }

        Some(qty)
    }

    /// Plan a remainder/closing order quantity bounded by `available` and a single grid order's
    /// full quantity. Returns `(quantity, OrderPurpose)` when `quantity` meets `min_qty` and
    /// `min_notional`.
    #[must_use]
    pub(crate) fn plan_remainder_quantity(
        &self,
        price: Decimal,
        order_amount_usdc: Decimal,
        available: Decimal,
    ) -> Option<(Decimal, crate::types::OrderPurpose)> {
        if available <= Decimal::ZERO {
            return None;
        }
        let full_quantity = self.calculate_quantity(price, order_amount_usdc)?;
        let quantity = self.checked_round_quantity(available)?.min(full_quantity);
        let notional = quantity.checked_mul(price)?;
        if quantity <= Decimal::ZERO || quantity < self.min_qty || notional < self.min_notional {
            return None;
        }
        let purpose = if quantity == full_quantity {
            crate::types::OrderPurpose::Grid
        } else {
            crate::types::OrderPurpose::Remainder
        };
        Some((quantity, purpose))
    }

    #[must_use]
    #[inline]
    pub(crate) fn format_price(&self, price: Decimal) -> String {
        crate::types::format_decimal_dp(self.round_price(price), self.price_scale, false)
    }

    #[must_use]
    #[inline]
    pub(crate) fn format_quantity(&self, qty: Decimal) -> String {
        crate::types::format_truncated_decimal_dp(
            self.round_quantity(qty),
            self.quantity_scale,
            false,
        )
    }

    fn checked_round_market_quantity(&self, qty: Decimal) -> Option<Decimal> {
        if self.market_step_size.is_zero() {
            return Some(qty);
        }
        if self.market_step_size < Decimal::ZERO {
            return None;
        }
        let steps = qty.checked_div(self.market_step_size)?.floor();
        let rounded = steps.checked_mul(self.market_step_size)?;
        Some(rounded.trunc_with_scale(self.market_quantity_scale))
    }

    #[must_use]
    #[inline]
    pub(crate) fn round_market_quantity(&self, qty: Decimal) -> Decimal {
        self.checked_round_market_quantity(qty)
            .unwrap_or(Decimal::ZERO)
    }

    #[must_use]
    #[inline]
    pub(crate) fn format_market_quantity(&self, qty: Decimal) -> String {
        crate::types::format_truncated_decimal_dp(
            self.round_market_quantity(qty),
            self.market_quantity_scale,
            false,
        )
    }

    fn min_open_market_chunk_qty(&self, reference_price: Decimal) -> Option<Decimal> {
        let mut min_chunk = self.market_min_qty.max(self.min_qty);
        if reference_price > Decimal::ZERO
            && self.min_notional > Decimal::ZERO
            && self.market_step_size > Decimal::ZERO
        {
            let raw = self.min_notional.checked_div(reference_price)?;
            let steps = raw.checked_div(self.market_step_size)?.ceil();
            let notional_qty = steps
                .checked_mul(self.market_step_size)?
                .trunc_with_scale(self.market_quantity_scale);
            min_chunk = min_chunk.max(notional_qty);
        }
        Some(min_chunk)
    }

    pub(crate) fn plan_market_chunks(
        &self,
        total_qty: Decimal,
        reduce_only: bool,
        reference_price: Decimal,
    ) -> anyhow::Result<Vec<Decimal>> {
        let rounded = self
            .checked_round_market_quantity(total_qty)
            .unwrap_or(Decimal::ZERO);
        if reduce_only {
            let lot_rounded = self
                .checked_round_quantity(total_qty)
                .unwrap_or(Decimal::ZERO);
            anyhow::ensure!(
                rounded > Decimal::ZERO && rounded == lot_rounded,
                "当前持仓数量 {} 不满足市价平仓步长规则，请先手动处理残余仓位",
                total_qty.normalize()
            );
        } else {
            let min_open = self
                .min_open_market_chunk_qty(reference_price)
                .ok_or_else(|| anyhow::anyhow!("无法计算市价开仓数量限制"))?;
            let notional_ok = reference_price <= Decimal::ZERO
                || rounded
                    .checked_mul(reference_price)
                    .is_some_and(|notional| notional >= self.min_notional);
            anyhow::ensure!(
                rounded >= min_open && notional_ok,
                "当前持仓数量 {} 低于交易所最小市价开仓门槛，请先手动处理残余仓位",
                total_qty.normalize()
            );
        }
        let max_chunk = self
            .checked_round_market_quantity(self.market_max_qty)
            .filter(|qty| *qty > Decimal::ZERO)
            .ok_or_else(|| anyhow::anyhow!("交易对市价最大下单数量规则无效"))?;
        let min_chunk = if reduce_only {
            self.market_min_qty.max(self.min_qty)
        } else {
            self.min_open_market_chunk_qty(reference_price)
                .ok_or_else(|| anyhow::anyhow!("无法计算市价开仓数量限制"))?
        };
        if !reduce_only {
            anyhow::ensure!(
                max_chunk >= min_chunk,
                "当前价格下最小开仓门槛高于单笔市价数量上限，无法执行反手开仓"
            );
        }
        let mut chunks = Vec::new();
        let mut remaining = rounded;
        while remaining > max_chunk {
            chunks.push(max_chunk);
            remaining = remaining
                .checked_sub(max_chunk)
                .ok_or_else(|| anyhow::anyhow!("市价分片数量计算溢出"))?;
        }
        if remaining > Decimal::ZERO {
            chunks.push(remaining);
        }
        if chunks.len() >= 2 {
            let last_idx = chunks.len() - 1;
            if chunks[last_idx] < min_chunk {
                let combined = chunks[last_idx - 1]
                    .checked_add(chunks[last_idx])
                    .ok_or_else(|| anyhow::anyhow!("市价分片数量计算溢出"))?;
                let first = self.round_market_quantity(combined / Decimal::TWO);
                let second = combined.saturating_sub(first);
                if first >= min_chunk && second >= min_chunk && second <= max_chunk {
                    chunks[last_idx - 1] = first;
                    chunks[last_idx] = second;
                } else if !reduce_only {
                    anyhow::bail!(
                        "当前持仓数量 {} 无法拆分为满足交易所最小门槛的市价订单",
                        total_qty.normalize()
                    );
                }
            }
        }
        Ok(chunks)
    }
}

#[must_use]
#[inline]
fn calculate_scale(d: Decimal) -> u32 {
    d.normalize().scale()
}

#[cfg(test)]
mod tests;
