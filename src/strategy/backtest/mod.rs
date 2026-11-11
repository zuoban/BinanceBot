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
#[cfg(test)]
use crate::types::{GridDirection, OrderSide};
use anyhow::{bail, ensure, Context, Result};
use rust_decimal::Decimal;
use serde::Serialize;

mod cli;
mod data;
mod sim;
mod sim_window;
#[cfg(test)]
mod tests;

pub use cli::{run_cli, BacktestArgs, DirectionArg};

#[cfg(test)]
use cli::{apply_rule_overrides, format_funding_impact, load_rules};
#[cfg(test)]
use data::{
    fetch_klines, fetch_klines_from_url, next_kline_page_start, parse_csv, MAX_BACKTEST_DAYS,
};
#[cfg(test)]
use sim::BtOrder;
use sim::Sim;
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
            if periods > 0 && !sim.size.is_zero() && !params.funding_rate_8h.is_zero() {
                let delta = sim
                    .size
                    .saturating_mul(candle.open)
                    .saturating_mul(params.funding_rate_8h)
                    .saturating_mul(Decimal::from(periods));
                sim.funding = sim.funding.saturating_add(delta);
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

    Ok(build_backtest_report(
        params,
        candles.len(),
        notional_sum,
        sim,
    ))
}

fn build_backtest_report(
    params: &BacktestParams,
    candles_len: usize,
    notional_sum: Decimal,
    sim: Sim<'_>,
) -> BacktestReport {
    let net_realized = sim
        .realized
        .saturating_sub(sim.fees)
        .saturating_sub(sim.funding);
    let unrealized = sim.unrealized();
    let net_total = net_realized.saturating_add(unrealized);
    let hundred = Decimal::ONE_HUNDRED;
    let return_pct = net_total
        .checked_div(params.capital)
        .map(|v| v.saturating_mul(hundred))
        .unwrap_or(if net_total.is_sign_negative() {
            Decimal::MIN
        } else {
            Decimal::MAX
        })
        .round_dp(2);
    let max_drawdown_pct = sim
        .max_drawdown
        .checked_div(params.capital)
        .and_then(|v| v.checked_mul(hundred))
        .unwrap_or(Decimal::MAX)
        .round_dp(2);
    let avg_position_notional = notional_sum
        .checked_div(Decimal::from(candles_len))
        .unwrap_or(Decimal::MAX)
        .round_dp(2);
    BacktestReport {
        candles: candles_len,
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
    }
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
