//! Offline grid replay over historical candles or ticks.
//!
//! The simulator reuses [`SymbolRules::grid_window_prices`], so the price lattice, the
//! one-grid distance from the last fill, price bounds and quantity rounding match the live
//! engine. Everything else is a simplified model, see the notes printed with each report:
//!
//! * Price moves along `open -> low -> high -> close` for rising candles and
//!   `open -> high -> low -> close` for falling ones. Orders fill at their own price as the
//!   path crosses them and the window is re-maintained after every fill, so a fast move can
//!   keep filling like it would live. Intra-candle order is unknowable from OHLC.
//! * A resting Maker order fills as soon as the path touches its price: no queue position and
//!   no partial fills. This makes results optimistic, especially for tight spacing.
//! * Fees are a flat Maker rate. Funding is a flat 8-hour rate on position notional.
//! * No leverage, margin or liquidation is modelled, so the liquidation guard and the margin
//!   ratio guard are not exercised.

use crate::config::GridConfig;
use crate::strategy::precision::SymbolRules;
use crate::types::{GridDirection, OrderSide};
use anyhow::{bail, ensure, Context, Result};
use rust_decimal::Decimal;
use serde::Serialize;

#[path = "backtest/cli.rs"]
mod cli;
#[path = "backtest/data.rs"]
mod data;

#[cfg(test)]
#[path = "backtest_tests.rs"]
mod tests;

pub use cli::{run_cli, BacktestArgs, DirectionArg};

#[cfg(test)]
use cli::{format_funding_impact, load_rules};
#[cfg(test)]
use data::{fetch_klines, next_kline_page_start, parse_csv, MAX_BACKTEST_DAYS};

const FUNDING_PERIOD_MS: i64 = 8 * 60 * 60 * 1000;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Candle {
    pub(crate) time_ms: i64,
    pub(crate) open: Decimal,
    pub(crate) high: Decimal,
    pub(crate) low: Decimal,
    pub(crate) close: Decimal,
}

impl Candle {
    pub(crate) fn tick(time_ms: i64, price: Decimal) -> Self {
        Self {
            time_ms,
            open: price,
            high: price,
            low: price,
            close: price,
        }
    }

    fn path(&self) -> [Decimal; 4] {
        if self.close >= self.open {
            [self.open, self.low, self.high, self.close]
        } else {
            [self.open, self.high, self.low, self.close]
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BacktestParams {
    pub(crate) grid: GridConfig,
    pub(crate) rules: SymbolRules,
    /// Fraction of notional charged per fill, e.g. 0.0002.
    pub(crate) maker_fee_rate: Decimal,
    /// Fraction of position notional paid by longs (received by shorts) every 8 hours.
    pub(crate) funding_rate_8h: Decimal,
    /// Reference capital for percentage figures only; it does not limit trading.
    pub(crate) capital: Decimal,
    /// Experimental: only place exits at least this many grid spacings in profit versus the
    /// average entry (long: above, short: below). Replay-only, the live engine does not do this.
    pub(crate) exit_floor_spacings: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct BacktestReport {
    pub(crate) candles: usize,
    pub(crate) fills: u64,
    pub(crate) buys: u64,
    pub(crate) sells: u64,
    pub(crate) realized_pnl: Decimal,
    pub(crate) fees: Decimal,
    pub(crate) funding: Decimal,
    /// realized - fees - funding
    pub(crate) net_realized: Decimal,
    pub(crate) unrealized_pnl: Decimal,
    pub(crate) net_total: Decimal,
    pub(crate) return_pct: Decimal,
    pub(crate) max_drawdown: Decimal,
    pub(crate) max_drawdown_pct: Decimal,
    pub(crate) peak_position_notional: Decimal,
    pub(crate) avg_position_notional: Decimal,
    pub(crate) final_position: Decimal,
    pub(crate) final_entry_price: Decimal,
    pub(crate) final_price: Decimal,
    pub(crate) paused_at_ms: Option<i64>,
    pub(crate) pause_reason: Option<String>,
}

struct BtOrder {
    side: OrderSide,
    price: Decimal,
    qty: Decimal,
    remainder: bool,
}

struct Sim<'a> {
    p: &'a BacktestParams,
    orders: Vec<BtOrder>,
    size: Decimal,
    entry: Decimal,
    last_fill: Option<Decimal>,
    market: Decimal,
    now_ms: i64,
    realized: Decimal,
    /// Sum of signed fill cash flows, kept to cross-check the average-cost accounting.
    cash_flow: Decimal,
    fees: Decimal,
    funding: Decimal,
    fills: u64,
    buys: u64,
    sells: u64,
    peak_equity: Decimal,
    max_drawdown: Decimal,
    peak_notional: Decimal,
    paused: Option<(i64, String)>,
}

impl<'a> Sim<'a> {
    fn new(p: &'a BacktestParams, start_price: Decimal, now_ms: i64) -> Self {
        Self {
            p,
            orders: Vec::new(),
            size: Decimal::ZERO,
            entry: Decimal::ZERO,
            last_fill: None,
            market: start_price,
            now_ms,
            realized: Decimal::ZERO,
            cash_flow: Decimal::ZERO,
            fees: Decimal::ZERO,
            funding: Decimal::ZERO,
            fills: 0,
            buys: 0,
            sells: 0,
            peak_equity: Decimal::ZERO,
            max_drawdown: Decimal::ZERO,
            peak_notional: Decimal::ZERO,
            paused: None,
        }
    }

    fn limit(&self) -> Option<Decimal> {
        self.p
            .grid
            .max_position_usdc
            .filter(|limit| *limit > Decimal::ZERO)
    }

    fn unrealized(&self) -> Decimal {
        if self.size.is_zero() {
            Decimal::ZERO
        } else {
            self.market
                .checked_sub(self.entry)
                .and_then(|diff| diff.checked_mul(self.size))
                .unwrap_or(Decimal::ZERO)
        }
    }

    fn equity(&self) -> Decimal {
        self.realized
            .checked_sub(self.fees)
            .and_then(|v| v.checked_sub(self.funding))
            .and_then(|v| v.checked_add(self.unrealized()))
            .unwrap_or(Decimal::ZERO)
    }

    fn record(&mut self) {
        let equity = self.equity();
        self.peak_equity = self.peak_equity.max(equity);
        self.max_drawdown = self
            .max_drawdown
            .max(self.peak_equity.checked_sub(equity).unwrap_or(Decimal::MAX));
        self.peak_notional = self.peak_notional.max(
            self.size
                .abs()
                .checked_mul(self.market)
                .unwrap_or(Decimal::MAX),
        );
    }

    fn pause(&mut self, reason: &str) {
        if self.paused.is_none() {
            self.paused = Some((self.now_ms, reason.to_string()));
        }
        self.orders.clear();
    }

    fn qty_at(&self, price: Decimal) -> Option<Decimal> {
        if price <= Decimal::ZERO {
            return None;
        }
        self.p
            .rules
            .calculate_quantity(price, self.p.grid.order_amount_usdc)
            .filter(|qty| *qty > Decimal::ZERO)
    }

    fn pause_at_position_limit(&mut self) -> bool {
        if self.p.grid.pause_on_max_position
            && self.limit().is_some_and(|limit| {
                self.size
                    .abs()
                    .checked_mul(self.market)
                    .is_none_or(|notional| notional >= limit)
            })
        {
            self.pause("持仓名义价值达到最大持仓名义价值");
            return true;
        }
        false
    }

    fn move_to(&mut self, target: Decimal) {
        loop {
            let current = self.market;
            if target == current {
                break;
            }
            if self.paused.is_some() {
                self.market = target;
                self.record();
                break;
            }
            let down = target < current;
            let next = if down {
                self.orders
                    .iter()
                    .filter(|o| o.side == OrderSide::Buy && o.price < current && o.price >= target)
                    .map(|o| o.price)
                    .max()
            } else {
                self.orders
                    .iter()
                    .filter(|o| o.side == OrderSide::Sell && o.price > current && o.price <= target)
                    .map(|o| o.price)
                    .min()
            };
            match next {
                Some(price) => {
                    self.market = price;
                    // The engine marks the position and checks its cap before paper matching.
                    // Do not maintain the window here: it would prune the touched order.
                    self.record();
                    if self.pause_at_position_limit() {
                        continue;
                    }
                    let side = if down {
                        OrderSide::Buy
                    } else {
                        OrderSide::Sell
                    };
                    self.fill_at(side, price);
                    self.record();
                    self.maintain();
                    self.record();
                }
                None => {
                    let interval = self.p.grid.grid_interval;
                    let same_cell = !self.orders.is_empty()
                        && self.limit().is_none()
                        && interval > Decimal::ZERO
                        && current
                            .checked_div(interval)
                            .zip(target.checked_div(interval))
                            .is_some_and(|(a, b)| a.floor() == b.floor() && a.ceil() == b.ceil());
                    self.market = target;
                    self.record();
                    if !same_cell {
                        self.maintain();
                        self.record();
                    }
                    break;
                }
            }
        }
    }

    fn fill_at(&mut self, side: OrderSide, price: Decimal) {
        let Some(index) = self
            .orders
            .iter()
            .position(|o| o.side == side && o.price == price)
        else {
            return;
        };
        let order = self.orders.remove(index);
        let direction = self.p.grid.direction;
        let mut qty = order.qty;
        if side == direction.closing_side() {
            qty = qty.min(direction.position_quantity(self.size));
        }
        if qty <= Decimal::ZERO {
            return;
        }
        let (new_size, new_entry, realized_delta) =
            crate::types::apply_fill_to_position(self.size, self.entry, side, price, qty);
        self.size = new_size;
        self.entry = new_entry;
        self.realized = self.realized.saturating_add(realized_delta);
        let fill_notional = price.checked_mul(qty).unwrap_or(Decimal::MAX);
        let signed_flow = if side == OrderSide::Buy {
            fill_notional
        } else {
            -fill_notional
        };
        self.cash_flow = self.cash_flow.saturating_sub(signed_flow);
        let fee = fill_notional
            .checked_mul(self.p.maker_fee_rate)
            .unwrap_or(Decimal::MAX);
        self.fees = self.fees.saturating_add(fee);
        self.last_fill = Some(price);
        self.fills += 1;
        if side == OrderSide::Buy {
            self.buys += 1;
        } else {
            self.sells += 1;
        }
    }

    /// Mirrors `GridTradingEngine::maintain_grid_window` for a single symbol.
    fn maintain(&mut self) {
        if self.paused.is_some() || self.pause_at_position_limit() {
            return;
        }
        let opening = self.p.grid.direction.opening_side();
        let (buys, sells) = self.compute_window_prices();
        self.prune_and_trim_orders(&buys, &sells);
        let (opening_prices, closing_prices) = if opening == OrderSide::Buy {
            (buys.as_slice(), sells.as_slice())
        } else {
            (sells.as_slice(), buys.as_slice())
        };
        if !self.replenish_opening_orders(opening_prices) {
            return;
        }
        self.replenish_closing_and_remainder_orders(closing_prices);
    }

    fn compute_window_prices(&self) -> (Vec<Decimal>, Vec<Decimal>) {
        let grid = &self.p.grid;
        let direction = grid.direction;
        let opening = direction.opening_side();
        let market = self.market;
        let position_value = self.size.abs().checked_mul(market).unwrap_or(Decimal::MAX);
        let interval = grid.grid_interval;
        let last_fill = self.last_fill;
        let (mut buys, mut sells) = self.p.rules.grid_window_prices(
            market,
            interval,
            (grid.buy_window, grid.sell_window),
            (grid.min_price, grid.max_price),
            |_, price| {
                last_fill.is_none_or(|fill| {
                    price
                        .checked_sub(fill)
                        .is_none_or(|diff| diff.abs() >= interval)
                })
            },
        );
        if let Some(spacings) = self.p.exit_floor_spacings.filter(|_| !self.size.is_zero()) {
            if let Some(offset) = spacings.checked_mul(interval) {
                if direction == GridDirection::Long {
                    let floor = self.entry.checked_add(offset).unwrap_or(Decimal::MAX);
                    sells.retain(|price| *price >= floor);
                } else {
                    let ceiling = self.entry.checked_sub(offset).unwrap_or(Decimal::MIN);
                    buys.retain(|price| *price <= ceiling);
                }
            } else if direction == GridDirection::Long {
                sells.clear();
            } else {
                buys.clear();
            }
        }
        let suspended = grid.cancel_opening_on_max_position
            && !grid.pause_on_max_position
            && self.limit().is_some_and(|limit| position_value >= limit);
        if suspended {
            if opening == OrderSide::Buy {
                buys.clear();
            } else {
                sells.clear();
            }
        }
        (buys, sells)
    }

    fn prune_and_trim_orders(&mut self, buys: &[Decimal], sells: &[Decimal]) {
        let direction = self.p.grid.direction;
        let closing = direction.closing_side();
        let mut position_qty = direction.position_quantity(self.size);
        self.orders.retain(|order| {
            if order.side == closing && position_qty.is_zero() {
                return false;
            }
            if order.side == OrderSide::Buy {
                buys.contains(&order.price)
            } else {
                sells.contains(&order.price)
            }
        });
        if position_qty.is_zero() {
            return;
        }
        // Keep closing orders within the position, nearest first.
        let mut closing_orders: Vec<usize> = (0..self.orders.len())
            .filter(|&i| self.orders[i].side == closing)
            .collect();
        closing_orders.sort_by(|&a, &b| {
            direction.closing_price_cmp(self.orders[a].price, self.orders[b].price)
        });
        let mut drop = Vec::new();
        for index in closing_orders {
            if self.orders[index].qty <= position_qty {
                position_qty = position_qty.saturating_sub(self.orders[index].qty);
            } else {
                drop.push(index);
            }
        }
        drop.sort_unstable_by(|a, b| b.cmp(a));
        for index in drop {
            self.orders.remove(index);
        }
    }

    fn replenish_opening_orders(&mut self, opening_prices: &[Decimal]) -> bool {
        let direction = self.p.grid.direction;
        let opening = direction.opening_side();
        let market = self.market;
        let limit = self.limit();
        let position_value = self.size.abs().checked_mul(market).unwrap_or(Decimal::MAX);
        let mut opening_notional: Decimal = limit.map_or(Decimal::ZERO, |_| {
            self.orders
                .iter()
                .filter(|o| o.side == opening)
                .fold(Decimal::ZERO, |acc, o| {
                    o.price
                        .checked_mul(o.qty)
                        .and_then(|n| acc.checked_add(n))
                        .unwrap_or(Decimal::MAX)
                })
        });
        for &price in opening_prices {
            if self
                .orders
                .iter()
                .any(|o| o.side == opening && o.price == price)
            {
                continue;
            }
            let Some(qty) = self
                .qty_at(price)
                .filter(|_| opening.is_maker_price(price, market))
            else {
                continue;
            };
            let Some(order_notional) = price.checked_mul(qty) else {
                continue;
            };
            if let Some(limit) = limit {
                if self.p.grid.pause_on_max_position {
                    let projected = direction
                        .sign()
                        .checked_mul(qty)
                        .and_then(|delta| self.size.checked_add(delta))
                        .and_then(|s| s.abs().checked_mul(market))
                        .unwrap_or(Decimal::MAX);
                    if projected > limit
                        && (projected > position_value || projected == Decimal::MAX)
                    {
                        self.pause("下一笔开仓单成交后预计持仓名义价值超过上限");
                        return false;
                    }
                }
                let valuation = market.max(price);
                let exposure = direction
                    .position_quantity(self.size)
                    .checked_mul(valuation)
                    .and_then(|pos| pos.checked_add(opening_notional))
                    .and_then(|sum| sum.checked_add(order_notional))
                    .unwrap_or(Decimal::MAX);
                if exposure > limit || exposure == Decimal::MAX {
                    continue;
                }
            }
            opening_notional = opening_notional
                .checked_add(order_notional)
                .unwrap_or(Decimal::MAX);
            self.orders.push(BtOrder {
                side: opening,
                price,
                qty,
                remainder: false,
            });
        }
        true
    }

    fn replenish_closing_and_remainder_orders(&mut self, closing_prices: &[Decimal]) {
        let direction = self.p.grid.direction;
        let closing = direction.closing_side();
        let market = self.market;
        let position_qty = direction.position_quantity(self.size);
        if position_qty.is_zero() {
            return;
        }
        let mut reserved_qty: Decimal = self
            .orders
            .iter()
            .filter(|o| o.side == closing)
            .fold(Decimal::ZERO, |acc, o| {
                acc.checked_add(o.qty).unwrap_or(Decimal::MAX)
            });
        for &price in closing_prices {
            if self
                .orders
                .iter()
                .any(|o| o.side == closing && o.price == price)
            {
                continue;
            }
            let Some(qty) = self
                .qty_at(price)
                .filter(|_| closing.is_maker_price(price, market))
            else {
                continue;
            };
            let available = position_qty
                .checked_sub(reserved_qty)
                .unwrap_or(Decimal::ZERO)
                .max(Decimal::ZERO);
            if available >= qty {
                reserved_qty = reserved_qty.checked_add(qty).unwrap_or(Decimal::MAX);
                self.orders.push(BtOrder {
                    side: closing,
                    price,
                    qty,
                    remainder: false,
                });
            }
        }

        // Ordinary allocation must honor existing remainder reservations. Only replace
        // their queue after a compliant target can reuse their reserved inventory.
        let remainder_qty: Decimal = self
            .orders
            .iter()
            .filter(|o| o.side == closing && o.remainder)
            .fold(Decimal::ZERO, |acc, o| {
                acc.checked_add(o.qty).unwrap_or(Decimal::MAX)
            });
        let available = position_qty
            .checked_sub(reserved_qty)
            .and_then(|v| v.checked_add(remainder_qty))
            .unwrap_or(Decimal::ZERO)
            .max(Decimal::ZERO);
        if let Some(&price) = closing_prices.iter().find(|&&price| {
            !self
                .orders
                .iter()
                .any(|o| o.side == closing && o.price == price && !o.remainder)
                && closing.is_maker_price(price, market)
        }) {
            let Some((qty, purpose)) = self.p.rules.plan_remainder_quantity(
                price,
                self.p.grid.order_amount_usdc,
                available,
            ) else {
                return;
            };
            let remainder = purpose == crate::types::OrderPurpose::Remainder;
            let mut sources = self
                .orders
                .iter()
                .filter(|o| o.side == closing && o.remainder);
            if sources
                .next()
                .is_some_and(|o| o.price == price && o.qty == qty && o.remainder == remainder)
                && sources.next().is_none()
            {
                return;
            }
            self.orders.retain(|o| o.side != closing || !o.remainder);
            self.orders.push(BtOrder {
                side: closing,
                price,
                qty,
                remainder,
            });
        }
    }
}

fn validate_backtest_inputs(params: &BacktestParams, candles: &[Candle]) -> Result<()> {
    let grid = &params.grid;
    ensure!(!candles.is_empty(), "没有可回放的行情数据");
    grid.validate()?;
    if params.maker_fee_rate < Decimal::ZERO || params.maker_fee_rate >= Decimal::ONE {
        bail!("maker_fee_rate 必须在 [0, 1) 范围内");
    }
    ensure!(params.capital > Decimal::ZERO, "参考资金必须大于 0");
    ensure!(
        params
            .exit_floor_spacings
            .is_none_or(|spacings| spacings >= Decimal::ZERO),
        "平仓价下限不能为负"
    );
    params.rules.validate_grid_interval(grid.grid_interval)?;
    for pair in candles.windows(2) {
        ensure!(pair[0].time_ms <= pair[1].time_ms, "行情时间必须按升序排列");
    }
    let rules = &params.rules;
    for candle in candles {
        ensure!(
            candle.high <= rules.max_price && candle.low >= rules.min_price,
            "行情价格 {}–{} 超出交易规则的价格范围 {}–{}，网格档位会被全部过滤；请用 --symbol 读取交易所规则",
            candle.low,
            candle.high,
            rules.min_price,
            rules.max_price
        );
        ensure!(
            candle.low > Decimal::ZERO
                && candle.low <= candle.open.min(candle.close)
                && candle.high >= candle.open.max(candle.close),
            "无效的 K 线（价格必须为正且 low ≤ open/close ≤ high）：时间 {}",
            candle.time_ms
        );
    }
    Ok(())
}

pub(crate) fn run(params: &BacktestParams, candles: &[Candle]) -> Result<BacktestReport> {
    validate_backtest_inputs(params, candles)?;
    let first = candles[0];
    let mut sim = Sim::new(params, first.open, first.time_ms);
    sim.maintain();
    sim.record();
    let mut notional_sum = Decimal::ZERO;
    let mut previous_time: Option<i64> = None;
    for candle in candles {
        sim.now_ms = candle.time_ms;
        if let Some(previous) = previous_time {
            let periods = candle
                .time_ms
                .div_euclid(FUNDING_PERIOD_MS)
                .saturating_sub(previous.div_euclid(FUNDING_PERIOD_MS));
            let delta = (periods > 0)
                .then(|| {
                    sim.size
                        .checked_mul(candle.open)
                        .and_then(|notional| notional.checked_mul(params.funding_rate_8h))
                        .and_then(|fee| fee.checked_mul(Decimal::from(periods)))
                })
                .flatten();
            if let Some(delta) = delta {
                sim.funding = sim.funding.checked_add(delta).unwrap_or(Decimal::MAX);
            }
        }
        previous_time = Some(candle.time_ms);
        for price in candle.path() {
            sim.move_to(price);
        }
        sim.record();
        if let Some(notional) = sim.size.abs().checked_mul(sim.market) {
            notional_sum = notional_sum.checked_add(notional).unwrap_or(Decimal::MAX);
        }
    }

    let net_realized = sim
        .realized
        .checked_sub(sim.fees)
        .and_then(|v| v.checked_sub(sim.funding))
        .unwrap_or(Decimal::ZERO);
    let unrealized = sim.unrealized();
    let net_total = net_realized
        .checked_add(unrealized)
        .unwrap_or(Decimal::ZERO);
    let hundred = Decimal::ONE_HUNDRED;
    let return_pct = net_total
        .checked_div(params.capital)
        .and_then(|v| v.checked_mul(hundred))
        .unwrap_or(Decimal::ZERO)
        .round_dp(2);
    let max_drawdown_pct = sim
        .max_drawdown
        .checked_div(params.capital)
        .and_then(|v| v.checked_mul(hundred))
        .unwrap_or(Decimal::MAX)
        .round_dp(2);
    let avg_position_notional = notional_sum
        .checked_div(Decimal::from(candles.len()))
        .unwrap_or(Decimal::MAX)
        .round_dp(2);
    Ok(BacktestReport {
        candles: candles.len(),
        fills: sim.fills,
        buys: sim.buys,
        sells: sim.sells,
        realized_pnl: sim.realized.round_dp(4),
        fees: sim.fees.round_dp(4),
        funding: sim.funding.round_dp(4),
        net_realized: net_realized.round_dp(4),
        unrealized_pnl: unrealized.round_dp(4),
        net_total: net_total.round_dp(4),
        return_pct,
        max_drawdown: sim.max_drawdown.round_dp(4),
        max_drawdown_pct,
        peak_position_notional: sim.peak_notional.round_dp(2),
        avg_position_notional,
        final_position: sim.size,
        final_entry_price: sim.entry.round_dp(4),
        final_price: sim.market,
        paused_at_ms: sim.paused.as_ref().map(|(time, _)| *time),
        pause_reason: sim.paused.map(|(_, reason)| reason),
    })
}

/// Upper bound on sweep size, so a typo cannot start thousands of replays.
pub(crate) const MAX_SWEEP_COMBINATIONS: usize = 200;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SweepRow {
    pub(crate) grid_interval: Decimal,
    pub(crate) order_amount_usdc: Decimal,
    pub(crate) buy_window: usize,
    pub(crate) sell_window: usize,
    pub(crate) report: BacktestReport,
}

fn checked_sweep_combinations(
    intervals: usize,
    amounts: usize,
    buy_windows: usize,
    sell_windows: usize,
) -> Result<usize> {
    ensure!(
        intervals > 0 && amounts > 0 && buy_windows > 0 && sell_windows > 0,
        "至少需要一组参数"
    );
    let total = intervals
        .checked_mul(amounts)
        .and_then(|n| n.checked_mul(buy_windows))
        .and_then(|n| n.checked_mul(sell_windows))
        .ok_or_else(|| anyhow::anyhow!("参数组合数量溢出，超过上限 {MAX_SWEEP_COMBINATIONS}"))?;
    ensure!(
        total <= MAX_SWEEP_COMBINATIONS,
        "参数组合共 {total} 组，超过上限 {MAX_SWEEP_COMBINATIONS}"
    );
    Ok(total)
}

/// Replay every combination over the same candles, best net profit first. Any invalid
/// combination fails the whole sweep rather than being silently skipped.
pub(crate) fn sweep(
    base: &BacktestParams,
    grid_intervals: &[Decimal],
    amounts: &[Decimal],
    buy_windows: &[usize],
    sell_windows: &[usize],
    candles: &[Candle],
) -> Result<Vec<SweepRow>> {
    let total = checked_sweep_combinations(
        grid_intervals.len(),
        amounts.len(),
        buy_windows.len(),
        sell_windows.len(),
    )?;
    let mut rows = Vec::with_capacity(total);
    for &grid_interval in grid_intervals {
        for &order_amount_usdc in amounts {
            for &buy_window in buy_windows {
                for &sell_window in sell_windows {
                    let mut params = base.clone();
                    params.grid.grid_interval = grid_interval;
                    params.grid.order_amount_usdc = order_amount_usdc;
                    params.grid.buy_window = buy_window;
                    params.grid.sell_window = sell_window;
                    let report = run(&params, candles).with_context(|| {
                        format!(
                            "间距 {grid_interval} / 每格 {order_amount_usdc} / 窗口 {buy_window}-{sell_window} 回放失败"
                        )
                    })?;
                    rows.push(SweepRow {
                        grid_interval,
                        order_amount_usdc,
                        buy_window,
                        sell_window,
                        report,
                    });
                }
            }
        }
    }
    rows.sort_by_key(|row| std::cmp::Reverse(row.report.net_total));
    Ok(rows)
}
