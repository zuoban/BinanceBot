use crate::exchange::model::{BinanceExchangeInfo, BinanceFilter};
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
    fn test_grid_window_pricing() {
        let rules = SymbolRules::default();
        let current_price = dec!(115.48);
        let interval = dec!(0.1);
        let buy_window = 5;
        let sell_window = 5;

        let buy_prices: Vec<Decimal> = (1..=buy_window)
            .map(|i| rules.round_price(current_price - interval * Decimal::from(i)))
            .collect();

        let sell_prices: Vec<Decimal> = (1..=sell_window)
            .map(|i| rules.round_price(current_price + interval * Decimal::from(i)))
            .collect();

        assert_eq!(
            buy_prices,
            vec![
                dec!(115.38),
                dec!(115.28),
                dec!(115.18),
                dec!(115.08),
                dec!(114.98)
            ]
        );
        assert_eq!(
            sell_prices,
            vec![
                dec!(115.58),
                dec!(115.68),
                dec!(115.78),
                dec!(115.88),
                dec!(115.98)
            ]
        );

        for bp in &buy_prices {
            assert!(*bp < current_price);
        }
        for sp in &sell_prices {
            assert!(*sp > current_price);
        }
    }
}
