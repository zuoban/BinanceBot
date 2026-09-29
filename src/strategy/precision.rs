use crate::exchange::model::{BinanceExchangeInfo, BinanceFilter};
use crate::types::OrderSide;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::{debug, warn};

#[derive(Debug, Clone)]
pub struct SymbolRules {
    pub symbol: String,
    pub tick_size: Decimal,
    pub price_scale: u32,
    pub min_price: Decimal,
    pub max_price: Decimal,

    pub step_size: Decimal,
    pub quantity_scale: u32,
    pub min_qty: Decimal,
    pub max_qty: Decimal,

    pub min_notional: Decimal,
}

impl Default for SymbolRules {
    fn default() -> Self {
        Self {
            symbol: "SOLUSDC".to_string(),
            tick_size: dec!(0.01),
            price_scale: 2,
            min_price: dec!(0.1),
            max_price: dec!(100000.0),
            step_size: dec!(0.01),
            quantity_scale: 2,
            min_qty: dec!(0.01),
            max_qty: dec!(800000.0),
            min_notional: dec!(5.0),
        }
    }
}

impl SymbolRules {
    pub fn from_exchange_info(info: &BinanceExchangeInfo, symbol_name: &str) -> Option<Self> {
        let sym = info
            .symbols
            .iter()
            .find(|s| s.symbol.eq_ignore_ascii_case(symbol_name))?;

        let mut tick_size = dec!(0.01);
        let mut min_price = dec!(0.0001);
        let mut max_price = dec!(1000000.0);

        let mut step_size = dec!(0.01);
        let mut min_qty = dec!(0.01);
        let mut max_qty = dec!(1000000.0);

        let mut min_notional = dec!(5.0);

        for filter in &sym.filters {
            match filter {
                BinanceFilter::PriceFilter {
                    min_price: min_p,
                    max_price: max_p,
                    tick_size: tick,
                } => {
                    tick_size = *tick;
                    min_price = *min_p;
                    max_price = *max_p;
                }
                BinanceFilter::LotSize {
                    min_qty: min_q,
                    max_qty: max_q,
                    step_size: step,
                } => {
                    step_size = *step;
                    min_qty = *min_q;
                    max_qty = *max_q;
                }
                BinanceFilter::MinNotional { notional: Some(n) } => min_notional = *n,
                _ => {}
            }
        }

        let price_scale = calculate_scale(tick_size);
        let quantity_scale = calculate_scale(step_size);

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
            min_notional,
        })
    }

    /// A fixed lattice must be exactly representable by the exchange price filter.
    pub fn validate_grid_interval(&self, interval: Decimal) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.tick_size > Decimal::ZERO
                && interval >= self.tick_size
                && interval % self.tick_size == Decimal::ZERO,
            "网格间距 {} 必须是价格步长 {} 的正整数倍",
            interval,
            self.tick_size
        );
        Ok(())
    }

    /// Zero-anchored levels stay identical across market moves and process restarts.
    /// Count eligible levels, skipping reservations without consuming window slots.
    /// At an exact level, neither side places an order at the current market price.
    pub fn grid_window_prices(
        &self,
        market: Decimal,
        interval: Decimal,
        windows: (usize, usize),
        bounds: (Option<Decimal>, Option<Decimal>),
        eligible: impl Fn(OrderSide, Decimal) -> bool,
    ) -> (Vec<Decimal>, Vec<Decimal>) {
        let min = bounds
            .0
            .unwrap_or(self.min_price)
            .max(self.min_price)
            .max(interval);
        let max = bounds.1.unwrap_or(self.max_price).min(self.max_price);
        let below =
            ((market / interval).ceil() - Decimal::ONE).min((max / interval).floor()) * interval;
        let above =
            ((market / interval).floor() + Decimal::ONE).max((min / interval).ceil()) * interval;
        let collect = |side, mut price, count, step| {
            let mut prices = Vec::new();
            while prices.len() < count && price >= min && price <= max {
                if eligible(side, price) {
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

    /// Round price to the nearest tick size
    pub fn round_price(&self, price: Decimal) -> Decimal {
        if self.tick_size.is_zero() {
            return price;
        }
        let ticks = (price / self.tick_size).round();
        let rounded = ticks * self.tick_size;
        rounded.trunc_with_scale(self.price_scale)
    }

    /// Truncate quantity down to the nearest lot step size
    pub fn round_quantity(&self, qty: Decimal) -> Decimal {
        if self.step_size.is_zero() {
            return qty;
        }
        let steps = (qty / self.step_size).floor();
        let rounded = steps * self.step_size;
        rounded.trunc_with_scale(self.quantity_scale)
    }

    /// Calculate compliant order quantity given target USDC notional amount and price
    pub fn calculate_quantity(&self, price: Decimal, amount_usdc: Decimal) -> Option<Decimal> {
        if price.is_zero() || amount_usdc.is_zero() {
            return None;
        }
        let raw_qty = amount_usdc / price;
        let qty = self.round_quantity(raw_qty);

        let notional = qty * price;
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

    pub fn format_price(&self, price: Decimal) -> String {
        format!("{:.*}", self.price_scale as usize, self.round_price(price))
    }

    pub fn format_quantity(&self, qty: Decimal) -> String {
        format!(
            "{:.*}",
            self.quantity_scale as usize,
            self.round_quantity(qty)
        )
    }
}

fn calculate_scale(d: Decimal) -> u32 {
    let s = d.normalize().to_string();
    if let Some(pos) = s.find('.') {
        (s.len() - 1 - pos) as u32
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_symbol_rules_rounding() {
        let rules = SymbolRules {
            symbol: "SOLUSDC".to_string(),
            tick_size: dec!(0.01),
            price_scale: 2,
            min_price: dec!(0.1),
            max_price: dec!(10000.0),
            step_size: dec!(0.01),
            quantity_scale: 2,
            min_qty: dec!(0.01),
            max_qty: dec!(1000.0),
            min_notional: dec!(5.0),
        };

        assert_eq!(rules.round_price(dec!(115.4849)), dec!(115.48));
        assert_eq!(rules.round_price(dec!(115.486)), dec!(115.49));
        assert_eq!(rules.round_quantity(dec!(0.8674)), dec!(0.86));

        let qty = rules.calculate_quantity(dec!(115.48), dec!(100.0)).unwrap();
        assert_eq!(qty, dec!(0.86));
        assert_eq!(rules.format_price(dec!(115.48)), "115.48");
        assert_eq!(rules.format_quantity(qty), "0.86");
    }

    #[test]
    fn grid_window_uses_fixed_levels_and_excludes_market_price() {
        let rules = SymbolRules::default();
        for market in [dec!(118.81), dec!(118.84), dec!(118.89)] {
            let (buys, sells) =
                rules.grid_window_prices(market, dec!(0.1), (3, 3), (None, None), |_, _| true);
            assert_eq!(buys, vec![dec!(118.8), dec!(118.7), dec!(118.6)]);
            assert_eq!(sells, vec![dec!(118.9), dec!(119.0), dec!(119.1)]);
        }
        let (buys, sells) =
            rules.grid_window_prices(dec!(118.9), dec!(0.1), (2, 2), (None, None), |_, _| true);
        assert_eq!(buys, vec![dec!(118.8), dec!(118.7)]);
        assert_eq!(sells, vec![dec!(119.0), dec!(119.1)]);
        let (buys, sells) =
            rules.grid_window_prices(dec!(118.91), dec!(0.1), (2, 2), (None, None), |_, _| true);
        assert_eq!(buys, vec![dec!(118.9), dec!(118.8)]);
        assert_eq!(sells, vec![dec!(119.0), dec!(119.1)]);
        let (buys, _) =
            rules.grid_window_prices(dec!(0.11), dec!(0.1), (5, 0), (None, None), |_, _| true);
        assert_eq!(buys, vec![dec!(0.1)]);
    }

    #[test]
    fn eligible_window_stops_at_bounds_and_jumps_to_allowed_range() {
        let rules = SymbolRules::default();
        let bounds = (Some(dec!(119.3)), Some(dec!(119.75)));
        let (buys, sells) =
            rules.grid_window_prices(dec!(119.21), dec!(0.1), (3, 3), bounds, |side, price| {
                side != OrderSide::Sell || price == dec!(119.6)
            });
        assert!(buys.is_empty());
        assert_eq!(sells, vec![dec!(119.6)]);
        let (buys, sells) =
            rules.grid_window_prices(dec!(200), dec!(0.1), (3, 3), bounds, |_, _| true);
        assert_eq!(buys, vec![dec!(119.7), dec!(119.6), dec!(119.5)]);
        assert!(sells.is_empty());
        let (buys, sells) =
            rules.grid_window_prices(dec!(100), dec!(0.1), (3, 3), bounds, |_, _| false);
        assert!(buys.is_empty() && sells.is_empty());
    }

    #[test]
    fn fixed_spacing_must_be_representable_by_price_ticks() {
        let mut rules = SymbolRules::default();
        assert!(rules.validate_grid_interval(dec!(0.1)).is_ok());
        assert!(rules.validate_grid_interval(dec!(0.015)).is_err());
        assert!(rules.validate_grid_interval(dec!(0.001)).is_err());
        assert!(rules.validate_grid_interval(dec!(0)).is_err());
        rules.tick_size = dec!(0.05);
        assert!(rules.validate_grid_interval(dec!(0.1)).is_ok());
        assert!(rules.validate_grid_interval(dec!(0.12)).is_err());
    }
}
