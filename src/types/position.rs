use super::{OrderSide, TickerInfo, TradingMode};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    pub trade_id: String,
    pub client_order_id: String,
    pub symbol: String,
    #[serde(default)]
    pub mode: TradingMode,
    pub side: OrderSide,
    pub price: Decimal,
    pub quantity: Decimal,
    pub amount_usdc: Decimal,
    pub realized_pnl: Decimal,
    pub commission: Decimal,
    #[serde(default)]
    pub pnl_verified: bool,
    pub is_maker: bool,
    pub timestamp: DateTime<Utc>,
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PositionInfo {
    pub symbol: String,
    pub size: Decimal,
    pub entry_price: Decimal,
    pub mark_price: Decimal,
    pub unrealized_pnl: Decimal,
    pub liquidation_price: Decimal,
    pub leverage: u32,
    #[serde(default)]
    pub update_time: Option<DateTime<Utc>>,
}

impl PositionInfo {
    #[must_use]
    #[inline]
    pub fn position_margin(&self) -> Decimal {
        let valuation_price = if self.mark_price > Decimal::ZERO {
            self.mark_price
        } else {
            self.entry_price
        };
        let leverage = Decimal::from(self.leverage.max(1));
        self.size
            .abs()
            .checked_mul(valuation_price)
            .and_then(|notional| notional.checked_div(leverage))
            .unwrap_or_else(|| {
                (self.size.abs() / leverage)
                    .checked_mul(valuation_price)
                    .unwrap_or(Decimal::MAX)
            })
    }

    /// Best-effort market valuation price using an optional ticker (`mark_price`,
    /// then `self.mark_price`, then `ticker.last_price`), without falling back to `entry_price`.
    #[must_use]
    #[inline]
    pub fn market_valuation_price(&self, ticker: Option<&TickerInfo>) -> Decimal {
        if let Some(price) = ticker.map(|t| t.mark_price).filter(|p| *p > Decimal::ZERO) {
            price
        } else if self.mark_price > Decimal::ZERO {
            self.mark_price
        } else if let Some(price) = ticker.map(|t| t.last_price).filter(|p| *p > Decimal::ZERO) {
            price
        } else {
            Decimal::ZERO
        }
    }

    /// Strictly fresh market valuation price (`ticker.mark_price`, `self.mark_price`, or `ticker.last_price`).
    #[must_use]
    #[inline]
    pub fn fresh_market_valuation_price(
        &self,
        ticker: &TickerInfo,
        mut is_fresh: impl FnMut(DateTime<Utc>) -> bool,
    ) -> Decimal {
        if ticker.mark_price > Decimal::ZERO && is_fresh(ticker.mark_update_time) {
            ticker.mark_price
        } else if self.mark_price > Decimal::ZERO && self.update_time.is_some_and(&mut is_fresh) {
            self.mark_price
        } else if ticker.last_price > Decimal::ZERO && is_fresh(ticker.update_time) {
            ticker.last_price
        } else {
            Decimal::ZERO
        }
    }

    /// Valuation price preferring fresh mark prices, falling back to best-effort market price or `entry_price`.
    #[must_use]
    #[inline]
    pub fn valuation_price_with_freshness(
        &self,
        ticker: &TickerInfo,
        ticker_fresh: bool,
        position_fresh: bool,
    ) -> Decimal {
        if ticker_fresh && position_fresh {
            if self
                .update_time
                .is_some_and(|time| time > ticker.mark_update_time)
            {
                self.mark_price
            } else {
                ticker.mark_price
            }
        } else if ticker_fresh {
            ticker.mark_price
        } else if position_fresh {
            self.mark_price
        } else {
            let market_price = self.market_valuation_price(Some(ticker));
            if market_price > Decimal::ZERO {
                market_price
            } else {
                self.entry_price
            }
        }
    }
}

fn increase_position_entry(
    size: Decimal,
    entry_price: Decimal,
    new_size: Decimal,
    price: Decimal,
    qty: Decimal,
) -> Decimal {
    let abs_new = new_size.abs();
    if abs_new.is_zero() {
        return Decimal::ZERO;
    }
    let abs_size = size.abs();
    entry_price
        .checked_mul(abs_size)
        .zip(price.checked_mul(qty))
        .and_then(|(a, b)| a.checked_add(b))
        .and_then(|sum| sum.checked_div(abs_new))
        .or_else(|| {
            abs_size
                .checked_div(abs_new)
                .and_then(|w1| w1.checked_mul(entry_price))
                .zip(
                    qty.checked_div(abs_new)
                        .and_then(|w2| w2.checked_mul(price)),
                )
                .and_then(|(a, b)| a.checked_add(b))
        })
        .unwrap_or(price)
}

fn reduce_or_flip_position(
    size: Decimal,
    entry_price: Decimal,
    new_size: Decimal,
    price: Decimal,
    qty: Decimal,
) -> (Decimal, Decimal, Decimal) {
    let closed_qty = qty.min(size.abs());
    let (exit_price, open_price) = if size > Decimal::ZERO {
        (price, entry_price)
    } else {
        (entry_price, price)
    };
    let realized = exit_price
        .checked_sub(open_price)
        .and_then(|diff| diff.checked_mul(closed_qty))
        .or_else(|| {
            exit_price
                .checked_mul(closed_qty)
                .zip(open_price.checked_mul(closed_qty))
                .and_then(|(a, b)| a.checked_sub(b))
        })
        .unwrap_or_else(|| {
            if closed_qty.is_zero() || exit_price == open_price {
                Decimal::ZERO
            } else if (exit_price > open_price) == (closed_qty > Decimal::ZERO) {
                Decimal::MAX
            } else {
                Decimal::MIN
            }
        });
    let new_entry = if new_size.is_zero() {
        Decimal::ZERO
    } else if new_size.is_sign_positive() != size.is_sign_positive() {
        price
    } else {
        entry_price
    };
    (new_size, new_entry, realized)
}

#[must_use]
pub(crate) fn apply_fill_to_position(
    size: Decimal,
    entry_price: Decimal,
    side: OrderSide,
    price: Decimal,
    qty: Decimal,
) -> (Decimal, Decimal, Decimal) {
    let signed_qty = match side {
        OrderSide::Buy => qty,
        OrderSide::Sell => -qty,
    };
    let new_size = size.saturating_add(signed_qty);
    if size.is_zero() || size.is_sign_positive() == signed_qty.is_sign_positive() {
        let new_entry = increase_position_entry(size, entry_price, new_size, price, qty);
        (new_size, new_entry, Decimal::ZERO)
    } else {
        reduce_or_flip_position(size, entry_price, new_size, price, qty)
    }
}

pub(crate) const PAPER_INITIAL_BALANCE: Decimal = rust_decimal_macros::dec!(10000);

#[must_use]
#[inline]
pub(crate) fn quote_asset(symbol: &str) -> Option<&'static str> {
    ["FDUSD", "USDC", "USDT", "BUSD"]
        .into_iter()
        .find(|asset| symbol.ends_with(asset))
}

#[must_use]
#[inline]
pub(crate) fn resolve_quote_asset<'a>(symbol: &'a str, fallback_asset: &'a str) -> &'a str {
    quote_asset(symbol).unwrap_or(if fallback_asset.is_empty() {
        symbol
    } else {
        fallback_asset
    })
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountInfo {
    pub asset: String,
    pub total_wallet_balance: Decimal,
    pub available_balance: Decimal,
    pub margin_balance: Decimal,
    pub unrealized_profit: Decimal,
    pub update_time: DateTime<Utc>,
}

impl AccountInfo {
    #[must_use]
    pub fn initial_paper(symbol: &str, update_time: Option<DateTime<Utc>>) -> Self {
        Self {
            asset: quote_asset(symbol).unwrap_or(symbol).to_string(),
            total_wallet_balance: PAPER_INITIAL_BALANCE,
            available_balance: PAPER_INITIAL_BALANCE,
            margin_balance: PAPER_INITIAL_BALANCE,
            unrealized_profit: Decimal::ZERO,
            update_time: update_time.unwrap_or_else(Utc::now),
        }
    }
}

impl Default for AccountInfo {
    fn default() -> Self {
        Self {
            asset: String::new(),
            total_wallet_balance: Decimal::ZERO,
            available_balance: Decimal::ZERO,
            margin_balance: Decimal::ZERO,
            unrealized_profit: Decimal::ZERO,
            update_time: DateTime::<Utc>::default(),
        }
    }
}

fn format_scaled_decimal(mut scaled: Decimal, dp: u32, force_sign: bool) -> String {
    if scaled.is_zero() {
        scaled = Decimal::ZERO;
    }
    let raw = scaled.to_string();
    let (sign, unsigned) = match raw.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None if force_sign => ("+", raw.as_str()),
        None => ("", raw.as_str()),
    };
    let dp_usize = dp as usize;
    let (whole, frac) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if dp_usize == 0 {
        return format!("{sign}{whole}");
    }
    if frac.len() >= dp_usize {
        format!("{sign}{whole}.{}", &frac[..dp_usize])
    } else {
        format!("{sign}{whole}.{frac:0<dp_usize$}")
    }
}

#[must_use]
#[inline]
pub(crate) fn format_decimal_dp(value: Decimal, dp: u32, force_sign: bool) -> String {
    let rounded = if dp <= Decimal::MAX_SCALE {
        value.round_dp(dp)
    } else {
        value
    };
    format_scaled_decimal(rounded, dp, force_sign)
}

#[must_use]
#[inline]
pub(crate) fn format_truncated_decimal_dp(value: Decimal, dp: u32, force_sign: bool) -> String {
    let truncated = if dp <= Decimal::MAX_SCALE {
        value.trunc_with_scale(dp)
    } else {
        value
    };
    format_scaled_decimal(truncated, dp, force_sign)
}
