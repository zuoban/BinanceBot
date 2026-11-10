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

use crate::config::{AppConfig, GridConfig};
use crate::exchange::client::BinanceFuturesClient;
use crate::strategy::precision::SymbolRules;
use crate::types::{GridDirection, OrderSide};
use anyhow::{bail, ensure, Context, Result};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Serialize;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

const FUNDING_PERIOD_MS: i64 = 8 * 60 * 60 * 1000;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Candle {
    pub time_ms: i64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
}

impl Candle {
    pub fn tick(time_ms: i64, price: Decimal) -> Self {
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
pub struct BacktestParams {
    pub grid: GridConfig,
    pub rules: SymbolRules,
    /// Fraction of notional charged per fill, e.g. 0.0002.
    pub maker_fee_rate: Decimal,
    /// Fraction of position notional paid by longs (received by shorts) every 8 hours.
    pub funding_rate_8h: Decimal,
    /// Reference capital for percentage figures only; it does not limit trading.
    pub capital: Decimal,
    /// Experimental: only place exits at least this many grid spacings in profit versus the
    /// average entry (long: above, short: below). Replay-only, the live engine does not do this.
    pub exit_floor_spacings: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BacktestReport {
    pub candles: usize,
    pub fills: u64,
    pub buys: u64,
    pub sells: u64,
    pub realized_pnl: Decimal,
    pub fees: Decimal,
    pub funding: Decimal,
    /// realized - fees - funding
    pub net_realized: Decimal,
    pub unrealized_pnl: Decimal,
    pub net_total: Decimal,
    pub return_pct: Decimal,
    pub max_drawdown: Decimal,
    pub max_drawdown_pct: Decimal,
    pub peak_position_notional: Decimal,
    pub avg_position_notional: Decimal,
    pub final_position: Decimal,
    pub final_entry_price: Decimal,
    pub final_price: Decimal,
    pub paused_at_ms: Option<i64>,
    pub pause_reason: Option<String>,
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
            (self.market - self.entry) * self.size
        }
    }

    fn equity(&self) -> Decimal {
        self.realized - self.fees - self.funding + self.unrealized()
    }

    fn record(&mut self) {
        let equity = self.equity();
        self.peak_equity = self.peak_equity.max(equity);
        self.max_drawdown = self.max_drawdown.max(self.peak_equity - equity);
        self.peak_notional = self.peak_notional.max(self.size.abs() * self.market);
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
            && self
                .limit()
                .is_some_and(|limit| self.size.abs() * self.market >= limit)
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
                    self.market = target;
                    self.record();
                    self.maintain();
                    self.record();
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
        let signed = if side == OrderSide::Buy { qty } else { -qty };
        let (new_size, new_entry, realized_delta) =
            crate::types::apply_fill_to_position(self.size, self.entry, side, price, qty);
        self.size = new_size;
        self.entry = new_entry;
        self.realized += realized_delta;
        self.cash_flow -= signed * price;
        self.fees += price * qty * self.p.maker_fee_rate;
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
        let (opening_prices, closing_prices) = if opening == OrderSide::Buy {
            (buys.clone(), sells.clone())
        } else {
            (sells.clone(), buys.clone())
        };
        self.prune_and_trim_orders(&buys, &sells);
        if !self.replenish_opening_orders(&opening_prices) {
            return;
        }
        self.replenish_closing_and_remainder_orders(&closing_prices);
    }

    fn compute_window_prices(&self) -> (Vec<Decimal>, Vec<Decimal>) {
        let grid = &self.p.grid;
        let direction = grid.direction;
        let opening = direction.opening_side();
        let market = self.market;
        let position_value = self.size.abs() * market;
        let interval = grid.grid_interval;
        let last_fill = self.last_fill;
        let (mut buys, mut sells) = self.p.rules.grid_window_prices(
            market,
            interval,
            (grid.buy_window, grid.sell_window),
            (grid.min_price, grid.max_price),
            |_, price| last_fill.is_none_or(|fill| (price - fill).abs() >= interval),
        );
        if let Some(spacings) = self.p.exit_floor_spacings {
            if !self.size.is_zero() {
                let offset = spacings * interval;
                if direction == GridDirection::Long {
                    let floor = self.entry + offset;
                    sells.retain(|price| *price >= floor);
                } else {
                    let ceiling = self.entry - offset;
                    buys.retain(|price| *price <= ceiling);
                }
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
        self.orders.retain(|order| {
            if order.side == OrderSide::Buy {
                buys.contains(&order.price)
            } else {
                sells.contains(&order.price)
            }
        });
        // Keep closing orders within the position, nearest first.
        let mut position_qty = direction.position_quantity(self.size);
        let mut closing_orders: Vec<usize> = (0..self.orders.len())
            .filter(|&i| self.orders[i].side == closing)
            .collect();
        closing_orders.sort_by(|&a, &b| {
            direction.closing_price_cmp(self.orders[a].price, self.orders[b].price)
        });
        let mut drop = Vec::new();
        for index in closing_orders {
            if self.orders[index].qty <= position_qty {
                position_qty -= self.orders[index].qty;
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
        let position_value = self.size.abs() * market;
        for &price in opening_prices {
            if self
                .orders
                .iter()
                .any(|o| o.side == opening && o.price == price)
            {
                continue;
            }
            let maker_ok = if opening == OrderSide::Buy {
                price < market
            } else {
                price > market
            };
            let Some(qty) = self.qty_at(price).filter(|_| maker_ok) else {
                continue;
            };
            if let Some(limit) = limit {
                if self.p.grid.pause_on_max_position {
                    let projected = (self.size + direction.sign() * qty).abs() * market;
                    if projected > limit && projected > position_value {
                        self.pause("下一笔开仓单成交后预计持仓名义价值超过上限");
                        return false;
                    }
                }
                let valuation = market.max(price);
                let exposure = direction.position_quantity(self.size) * valuation
                    + self
                        .orders
                        .iter()
                        .filter(|o| o.side == opening)
                        .map(|o| o.price * o.qty)
                        .sum::<Decimal>()
                    + price * qty;
                if exposure > limit {
                    continue;
                }
            }
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
        let reserved = |orders: &[BtOrder]| -> Decimal {
            orders
                .iter()
                .filter(|o| o.side == closing)
                .map(|o| o.qty)
                .sum()
        };
        for &price in closing_prices {
            if self
                .orders
                .iter()
                .any(|o| o.side == closing && o.price == price)
            {
                continue;
            }
            let maker_ok = if closing == OrderSide::Sell {
                price > market
            } else {
                price < market
            };
            let Some(qty) = self.qty_at(price).filter(|_| maker_ok) else {
                continue;
            };
            let available = (direction.position_quantity(self.size) - reserved(&self.orders))
                .max(Decimal::ZERO);
            if available >= qty {
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
            .map(|o| o.qty)
            .sum();
        let available = (direction.position_quantity(self.size) - reserved(&self.orders)
            + remainder_qty)
            .max(Decimal::ZERO);
        if let Some(&price) = closing_prices.iter().find(|&&price| {
            !self
                .orders
                .iter()
                .any(|o| o.side == closing && o.price == price && !o.remainder)
                && if closing == OrderSide::Sell {
                    price > market
                } else {
                    price < market
                }
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
    if params.maker_fee_rate < Decimal::ZERO {
        bail!("maker_fee_rate 不能为负数");
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

pub fn run(params: &BacktestParams, candles: &[Candle]) -> Result<BacktestReport> {
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
            let periods = candle.time_ms.div_euclid(FUNDING_PERIOD_MS)
                - previous.div_euclid(FUNDING_PERIOD_MS);
            if periods > 0 {
                sim.funding +=
                    sim.size * candle.open * params.funding_rate_8h * Decimal::from(periods);
            }
        }
        previous_time = Some(candle.time_ms);
        for price in candle.path() {
            sim.move_to(price);
        }
        sim.record();
        notional_sum += sim.size.abs() * sim.market;
    }

    let net_realized = sim.realized - sim.fees - sim.funding;
    let unrealized = sim.unrealized();
    let net_total = net_realized + unrealized;
    let hundred = Decimal::ONE_HUNDRED;
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
        return_pct: (net_total / params.capital * hundred).round_dp(2),
        max_drawdown: sim.max_drawdown.round_dp(4),
        max_drawdown_pct: (sim.max_drawdown / params.capital * hundred).round_dp(2),
        peak_position_notional: sim.peak_notional.round_dp(2),
        avg_position_notional: (notional_sum / Decimal::from(candles.len())).round_dp(2),
        final_position: sim.size,
        final_entry_price: sim.entry.round_dp(4),
        final_price: sim.market,
        paused_at_ms: sim.paused.as_ref().map(|(time, _)| *time),
        pause_reason: sim.paused.map(|(_, reason)| reason),
    })
}

/// `time_ms,price` rows are ticks; `time_ms,open,high,low,close[,...]` rows are candles
/// (Binance kline CSV exports work as-is). A leading non-numeric row is treated as a header.
pub fn parse_csv(text: &str) -> Result<Vec<Candle>> {
    let mut candles = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split(',').map(str::trim).collect();
        if candles.is_empty() && fields[0].parse::<i64>().is_err() {
            continue;
        }
        let time_ms: i64 = fields[0]
            .parse()
            .with_context(|| format!("第 {} 行：时间必须是毫秒时间戳", index + 1))?;
        let number = |position: usize| -> Result<Decimal> {
            Decimal::from_str(fields[position]).with_context(|| {
                format!("第 {} 行：无法解析价格 {:?}", index + 1, fields[position])
            })
        };
        match fields.len() {
            2 => candles.push(Candle::tick(time_ms, number(1)?)),
            n if n >= 5 => candles.push(Candle {
                time_ms,
                open: number(1)?,
                high: number(2)?,
                low: number(3)?,
                close: number(4)?,
            }),
            n => bail!(
                "第 {} 行：需要 2 列（时间,价格）或至少 5 列（时间,开,高,低,收），实际 {} 列",
                index + 1,
                n
            ),
        }
    }
    Ok(candles)
}

pub async fn fetch_klines(
    symbol: &str,
    interval: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<Vec<Candle>> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;
    let mut start = start_ms;
    let mut candles: Vec<Candle> = Vec::new();
    loop {
        let url = format!(
            "https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&startTime={start}&endTime={end_ms}&limit=1500"
        );
        let rows: Vec<Vec<serde_json::Value>> = client
            .get(&url)
            .send()
            .await?
            .error_for_status()
            .context("K 线请求失败，请检查交易对和周期")?
            .json()
            .await?;
        let received = rows.len();
        for row in rows {
            let price = |position: usize| -> Result<Decimal> {
                Decimal::from_str(
                    row.get(position)
                        .and_then(|v| v.as_str())
                        .unwrap_or_default(),
                )
                .context("无法解析 K 线价格")
            };
            candles.push(Candle {
                time_ms: row
                    .first()
                    .and_then(|v| v.as_i64())
                    .context("无法解析 K 线时间")?,
                open: price(1)?,
                high: price(2)?,
                low: price(3)?,
                close: price(4)?,
            });
        }
        match candles.last() {
            Some(last) if received >= 1500 => start = last.time_ms + 1,
            _ => break,
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    Ok(candles)
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum DirectionArg {
    Long,
    Short,
}

#[derive(Debug, Clone, clap::Args)]
#[command(
    about = "Replay a grid configuration over historical prices",
    long_about = "Replay a grid configuration over historical candles. Provide --csv (time_ms,price or time_ms,open,high,low,close) or --symbol to download Binance USDⓈ-M klines for the last --days. With --symbol the price tick, quantity step and minimum notional come from exchangeInfo; the --tick-size/--step-size/--min-qty/--min-notional flags override them. Results are optimistic: orders fill when touched, with no queue position and no partial fills."
)]
pub struct BacktestArgs {
    /// Trading pair, e.g. BTCUSDC. Downloads klines unless --csv is given; also loads exchange rules.
    #[arg(long)]
    pub symbol: Option<String>,
    /// CSV file: `time_ms,price` ticks or `time_ms,open,high,low,close[,...]` candles.
    #[arg(long)]
    pub csv: Option<PathBuf>,
    /// Kline interval when downloading (1m, 5m, 1h ...).
    #[arg(long, default_value = "1m")]
    pub interval: String,
    /// How many days back to download.
    #[arg(long, default_value_t = 7)]
    pub days: u32,
    #[arg(long, value_enum, default_value_t = DirectionArg::Long)]
    pub direction: DirectionArg,
    /// Grid spacing. Comma-separated values run a sweep, e.g. `0.5,1,2`.
    #[arg(long, value_delimiter = ',', required = true)]
    pub grid_interval: Vec<Decimal>,
    /// Order amount per grid; comma-separated values run a sweep.
    #[arg(long, value_delimiter = ',', default_value = "100")]
    pub amount: Vec<Decimal>,
    /// Buy window depth; comma-separated values run a sweep.
    #[arg(long, value_delimiter = ',', default_value = "5")]
    pub buy_window: Vec<usize>,
    /// Sell window depth; comma-separated values run a sweep.
    #[arg(long, value_delimiter = ',', default_value = "5")]
    pub sell_window: Vec<usize>,
    #[arg(long)]
    pub min_price: Option<Decimal>,
    #[arg(long)]
    pub max_price: Option<Decimal>,
    #[arg(long)]
    pub max_position: Option<Decimal>,
    /// Pause and cancel everything at the position cap.
    #[arg(long)]
    pub pause_on_max_position: bool,
    /// At the cap, cancel only opening orders and keep exits.
    #[arg(long)]
    pub cancel_opening_on_max_position: bool,
    /// Maker fee as a fraction of notional.
    #[arg(long, default_value = "0.0002")]
    pub maker_fee: Decimal,
    /// Funding rate per 8 hours as a fraction of notional (positive: longs pay).
    #[arg(long, default_value = "0")]
    pub funding_rate: Decimal,
    /// Reference capital for percentage figures.
    #[arg(long, default_value = "1000")]
    pub capital: Decimal,
    #[arg(long)]
    pub tick_size: Option<Decimal>,
    #[arg(long)]
    pub step_size: Option<Decimal>,
    #[arg(long)]
    pub min_qty: Option<Decimal>,
    #[arg(long)]
    pub min_notional: Option<Decimal>,
    /// Experimental: only exit at least this many spacings in profit versus the average entry.
    #[arg(long)]
    pub exit_floor: Option<Decimal>,
    /// Print the report as JSON.
    #[arg(long)]
    pub json: bool,
}

async fn load_rules(args: &BacktestArgs) -> Result<SymbolRules> {
    ensure!(args.maker_fee >= Decimal::ZERO, "--maker-fee 不能为负数");
    let mut rules = match &args.symbol {
        Some(symbol) => {
            let client = BinanceFuturesClient::new(&AppConfig::default().exchange);
            let info = client
                .get_exchange_info(Some(symbol))
                .await
                .context("无法获取交易所规则")?;
            SymbolRules::from_exchange_info(&info, symbol)
                .with_context(|| format!("交易所没有可用的 {symbol} 规则"))?
        }
        // Offline defaults: the built-in price cap is meant for a single small-cap symbol and
        // would silently block every level on high-priced pairs, so use the exchange-info
        // fallback cap instead.
        None => SymbolRules {
            max_price: dec!(1000000),
            ..SymbolRules::default()
        },
    };
    if let Some(tick) = args.tick_size {
        ensure!(tick > Decimal::ZERO, "--tick-size 必须大于 0");
        rules.set_tick_size(tick);
    }
    if let Some(step) = args.step_size {
        ensure!(step > Decimal::ZERO, "--step-size 必须大于 0");
        rules.set_step_size(step);
    }
    if let Some(min_qty) = args.min_qty {
        ensure!(min_qty > Decimal::ZERO, "--min-qty 必须大于 0");
        rules.min_qty = min_qty;
    }
    if let Some(min_notional) = args.min_notional {
        ensure!(min_notional > Decimal::ZERO, "--min-notional 必须大于 0");
        rules.min_notional = min_notional;
    }
    Ok(rules)
}

/// Upper bound on sweep size, so a typo cannot start thousands of replays.
pub const MAX_SWEEP_COMBINATIONS: usize = 200;

#[derive(Debug, Clone, Serialize)]
pub struct SweepRow {
    pub grid_interval: Decimal,
    pub order_amount_usdc: Decimal,
    pub buy_window: usize,
    pub sell_window: usize,
    pub report: BacktestReport,
}

/// Replay every combination over the same candles, best net profit first. Any invalid
/// combination fails the whole sweep rather than being silently skipped.
pub fn sweep(
    base: &BacktestParams,
    grid_intervals: &[Decimal],
    amounts: &[Decimal],
    buy_windows: &[usize],
    sell_windows: &[usize],
    candles: &[Candle],
) -> Result<Vec<SweepRow>> {
    let total = grid_intervals.len() * amounts.len() * buy_windows.len() * sell_windows.len();
    ensure!(total > 0, "至少需要一组参数");
    ensure!(
        total <= MAX_SWEEP_COMBINATIONS,
        "参数组合共 {total} 组，超过上限 {MAX_SWEEP_COMBINATIONS}"
    );
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

fn when(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|time| time.to_rfc3339())
        .unwrap_or_else(|| ms.to_string())
}

const OPTIMISM_NOTE: &str =
    "注意：挂单一经触价即成交，不计排队与部分成交，结果偏乐观；未模拟杠杆、保证金与强平。";

fn print_report(report: &BacktestReport, candles: &[Candle], capital: Decimal) {
    println!(
        "回放 {} 根行情，{} 至 {}",
        report.candles,
        when(candles[0].time_ms),
        when(candles[candles.len() - 1].time_ms)
    );
    println!(
        "成交 {} 笔（买 {} / 卖 {}）",
        report.fills, report.buys, report.sells
    );
    println!("已实现盈亏      {}", report.realized_pnl);
    println!("手续费          -{}", report.fees);
    println!("资金费          -{}", report.funding);
    println!("净已实现        {}", report.net_realized);
    println!(
        "未实现盈亏      {}（期末价 {}，持仓 {}，均价 {}）",
        report.unrealized_pnl, report.final_price, report.final_position, report.final_entry_price
    );
    println!(
        "净收益合计      {}（{}% / 参考资金 {}）",
        report.net_total, report.return_pct, capital
    );
    println!(
        "最大回撤        {}（{}%）",
        report.max_drawdown, report.max_drawdown_pct
    );
    println!(
        "持仓名义价值    峰值 {} / 平均 {}",
        report.peak_position_notional, report.avg_position_notional
    );
    if let (Some(at), Some(reason)) = (report.paused_at_ms, &report.pause_reason) {
        println!("策略在 {} 暂停：{}", when(at), reason);
    }
    println!("{OPTIMISM_NOTE}");
}

fn print_sweep(rows: &[SweepRow], candles: &[Candle], capital: Decimal) {
    println!(
        "回放 {} 根行情，{} 至 {}；{} 组参数，按净收益从高到低排列（参考资金 {}）",
        candles.len(),
        when(candles[0].time_ms),
        when(candles[candles.len() - 1].time_ms),
        rows.len(),
        capital
    );
    println!(
        "{:>10} {:>9} {:>7} {:>7} {:>11} {:>8} {:>9} {:>11}  paused",
        "interval", "amount", "buy/sell", "fills", "net", "ret%", "maxdd%", "peak_pos"
    );
    for row in rows {
        let report = &row.report;
        println!(
            "{:>10} {:>9} {:>7} {:>7} {:>11} {:>8} {:>9} {:>11}  {}",
            row.grid_interval,
            row.order_amount_usdc,
            format!("{}/{}", row.buy_window, row.sell_window),
            report.fills,
            report.net_total,
            report.return_pct,
            report.max_drawdown_pct,
            report.peak_position_notional,
            if report.paused_at_ms.is_some() {
                "yes"
            } else {
                "-"
            }
        );
    }
    println!(
        "排序只按净收益，不含风险调整；请同时查看 maxdd% 与 peak_pos，并留意样本区间是否足够长。"
    );
    println!("{OPTIMISM_NOTE}");
}

pub async fn run_cli(args: &BacktestArgs) -> Result<()> {
    let rules = load_rules(args).await?;
    let candles = match (&args.csv, &args.symbol) {
        (Some(path), _) => parse_csv(
            &std::fs::read_to_string(path).with_context(|| format!("无法读取 {path:?}"))?,
        )?,
        (None, Some(symbol)) => {
            ensure!(args.days > 0, "--days 必须大于 0");
            let end = chrono::Utc::now().timestamp_millis();
            let start = end - i64::from(args.days) * 24 * 60 * 60 * 1000;
            fetch_klines(symbol, &args.interval, start, end).await?
        }
        (None, None) => bail!("请提供 --csv 或 --symbol"),
    };
    let base = BacktestParams {
        grid: GridConfig {
            direction: match args.direction {
                DirectionArg::Long => GridDirection::Long,
                DirectionArg::Short => GridDirection::Short,
            },
            grid_interval: args.grid_interval[0],
            order_amount_usdc: args.amount[0],
            buy_window: args.buy_window[0],
            sell_window: args.sell_window[0],
            post_only: true,
            min_price: args.min_price,
            max_price: args.max_price,
            max_position_usdc: args.max_position,
            pause_on_max_position: args.pause_on_max_position,
            cancel_opening_on_max_position: args.cancel_opening_on_max_position,
            min_liquidation_distance_pct: None,
        },
        rules,
        maker_fee_rate: args.maker_fee,
        funding_rate_8h: args.funding_rate,
        capital: args.capital,
        exit_floor_spacings: args.exit_floor,
    };
    let rows = sweep(
        &base,
        &args.grid_interval,
        &args.amount,
        &args.buy_window,
        &args.sell_window,
        &candles,
    )?;
    let idle = rows.iter().filter(|row| row.report.fills == 0).count();
    if idle > 0 {
        eprintln!(
            "警告：{idle}/{} 组参数没有任何成交。可能原因：每格数量按数量步长取整后低于最小数量或最小名义金额（可用 --step-size/--min-qty/--min-notional 覆盖），或 --min-price/--max-price 与网格间距把挂单挡在了行情之外，或行情区间内价格从未触及任何档位。",
            rows.len()
        );
    }
    match (rows.as_slice(), args.json) {
        ([row], true) => println!("{}", serde_json::to_string_pretty(&row.report)?),
        (_, true) => println!("{}", serde_json::to_string_pretty(&rows)?),
        ([row], false) => print_report(&row.report, &candles, args.capital),
        (_, false) => print_sweep(&rows, &candles, args.capital),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(grid: impl FnOnce(&mut GridConfig)) -> BacktestParams {
        let mut config = GridConfig {
            grid_interval: dec!(1),
            order_amount_usdc: dec!(100),
            buy_window: 3,
            sell_window: 3,
            max_position_usdc: None,
            ..GridConfig::default()
        };
        grid(&mut config);
        BacktestParams {
            grid: config,
            rules: SymbolRules::default(),
            maker_fee_rate: dec!(0.0002),
            funding_rate_8h: Decimal::ZERO,
            capital: dec!(1000),
            exit_floor_spacings: None,
        }
    }

    fn ticks(prices: &[Decimal]) -> Vec<Candle> {
        prices
            .iter()
            .enumerate()
            .map(|(i, price)| Candle::tick(i as i64 * 60_000, *price))
            .collect()
    }

    #[test]
    fn reaching_the_cap_pauses_before_a_closing_fill() {
        let mut p = params(|g| {
            g.buy_window = 1;
            g.sell_window = 1;
            g.min_price = Some(dec!(99));
            g.max_position_usdc = Some(dec!(100));
            g.pause_on_max_position = true;
        });
        p.rules.step_size = Decimal::ONE;
        p.rules.min_qty = Decimal::ONE;
        let report = run(&p, &ticks(&[dec!(99.5), dec!(99), dec!(100)])).unwrap();

        assert_eq!((report.buys, report.sells), (1, 0));
        assert_eq!(report.final_position, Decimal::ONE);
        assert_eq!(report.realized_pnl, Decimal::ZERO);
        assert_eq!(report.paused_at_ms, Some(120_000));
        assert_eq!(report.peak_position_notional, dec!(100));
    }

    #[test]
    fn regular_closing_allocation_preserves_the_remainder_queue() {
        let p = params(|g| g.grid_interval = dec!(10));
        let mut sim = Sim::new(&p, dec!(20), 0);
        sim.maintain();
        sim.move_to(dec!(10));
        assert!(sim.orders.iter().any(|o| {
            o.side == OrderSide::Sell && o.price == dec!(40) && o.qty == dec!(1.67) && o.remainder
        }));

        sim.move_to(dec!(30));
        for _ in 0..3 {
            sim.maintain();
            let exits: Vec<_> = sim
                .orders
                .iter()
                .filter(|o| o.side == OrderSide::Sell)
                .collect();
            assert_eq!(exits.len(), 1);
            assert_eq!((exits[0].price, exits[0].qty), (dec!(40), dec!(1.67)));
            assert!(exits[0].remainder);
        }

        let report = run(&p, &ticks(&[dec!(20), dec!(10), dec!(30), dec!(40)])).unwrap();
        assert_eq!((report.buys, report.sells), (1, 3));
        assert_eq!(report.final_position, Decimal::ZERO);
        assert_eq!(report.realized_pnl, dec!(166.7));
    }

    #[test]
    fn remainder_requires_a_valid_ordinary_quantity_at_its_price() {
        let mut p = params(|g| {
            g.buy_window = 1;
            g.sell_window = 1;
        });
        p.rules.step_size = Decimal::ONE;
        p.rules.min_qty = Decimal::ONE;
        let report = run(&p, &ticks(&[dec!(101), dec!(100), dec!(101)])).unwrap();

        assert_eq!((report.buys, report.sells), (1, 0));
        assert_eq!(report.final_position, Decimal::ONE);
        assert_eq!(report.realized_pnl, Decimal::ZERO);
    }

    #[test]
    fn remainder_top_up_is_capped_and_promoted_to_an_ordinary_order() {
        let p = params(|g| {
            g.grid_interval = dec!(10);
            g.buy_window = 1;
            g.sell_window = 1;
        });
        let mut sim = Sim::new(&p, dec!(30), 0);
        sim.size = dec!(3);
        sim.entry = dec!(10);
        sim.orders.push(BtOrder {
            side: OrderSide::Sell,
            price: dec!(40),
            qty: dec!(1.67),
            remainder: true,
        });
        sim.maintain();

        let exits: Vec<_> = sim
            .orders
            .iter()
            .filter(|o| o.side == OrderSide::Sell)
            .collect();
        assert_eq!(exits.len(), 1);
        assert_eq!((exits[0].price, exits[0].qty), (dec!(40), dec!(2.5)));
        assert!(!exits[0].remainder);
        sim.move_to(dec!(40));
        assert_eq!(sim.size, dec!(0.5));
        assert_eq!(sim.sells, 1);
    }

    #[test]
    fn replay_rejects_invalid_caps_and_price_bounds_in_restrict_only_mode() {
        let candles = ticks(&[dec!(100)]);
        for cap in [Decimal::ZERO, -Decimal::ONE] {
            let p = params(|g| g.max_position_usdc = Some(cap));
            assert!(run(&p, &candles).is_err());
        }
        for (minimum, maximum) in [
            (Some(dec!(110)), Some(dec!(90))),
            (Some(Decimal::ZERO), None),
            (Some(-Decimal::ONE), None),
            (None, Some(Decimal::ZERO)),
            (None, Some(-Decimal::ONE)),
        ] {
            let p = params(|g| {
                g.min_price = minimum;
                g.max_price = maximum;
            });
            assert!(run(&p, &candles).is_err());
        }
    }

    #[test]
    fn permissive_custom_filters_cannot_create_zero_quantity_fills() {
        let mut p = params(|g| {
            g.buy_window = 1;
            g.sell_window = 1;
        });
        p.rules.step_size = Decimal::ONE;
        p.rules.min_qty = Decimal::ZERO;
        p.rules.min_notional = Decimal::ZERO;
        let report = run(&p, &ticks(&[dec!(102), dec!(101), dec!(102)])).unwrap();

        assert_eq!(report.fills, 0);
        assert_eq!(report.final_position, Decimal::ZERO);
        assert_eq!(report.fees, Decimal::ZERO);
    }

    #[test]
    fn closing_fills_preserve_pre_fill_peak_notional_and_equity() {
        let mut p = params(|g| {
            g.buy_window = 1;
            g.sell_window = 1;
        });
        p.rules.step_size = Decimal::ONE;
        p.rules.min_qty = Decimal::ONE;
        p.maker_fee_rate = Decimal::ZERO;
        let candles = ticks(&[dec!(100), dec!(99), dec!(100)]);
        let report = run(&p, &candles).unwrap();

        assert_eq!(report.final_position, Decimal::ZERO);
        assert_eq!(report.peak_position_notional, dec!(100));
        assert_eq!(report.net_total, Decimal::ONE);

        // Before paying the closing fee, marked equity peaks at 0.01. The next
        // fee lowers it to -0.99, a drawdown of 1 rather than 0.99.
        p.maker_fee_rate = dec!(0.01);
        let with_fees = run(&p, &candles).unwrap();
        assert_eq!(with_fees.max_drawdown, Decimal::ONE);
        assert_eq!(with_fees.net_total, dec!(-0.99));

        p.maker_fee_rate = dec!(-0.0002);
        assert!(run(&p, &candles).is_err());
    }

    #[test]
    fn one_round_trip_earns_one_spacing_minus_fees() {
        let report = run(
            &params(|_| {}),
            &ticks(&[dec!(100), dec!(98.5), dec!(100.5)]),
        )
        .unwrap();
        // Buy 1.01 @ 99, sell 1.00 @ 100 (quantity rounds per price).
        assert_eq!((report.buys, report.sells), (1, 1));
        assert_eq!(report.realized_pnl, dec!(1));
        // 1.01 * 99 + 1.00 * 100 = 199.99 notional at 0.02%.
        assert_eq!(report.fees, dec!(0.0400));
        assert_eq!(report.final_position, dec!(0.01));
    }

    #[test]
    fn a_fast_move_keeps_filling_through_new_levels() {
        // Window is 3 levels deep, yet a drop of 6 levels must fill more than 3 buys because
        // the window is re-maintained after every fill.
        let report = run(&params(|_| {}), &ticks(&[dec!(100), dec!(93.5)])).unwrap();
        assert_eq!(report.buys, 6);
        assert_eq!(report.sells, 0);
    }

    #[test]
    fn pause_at_the_cap_cancels_everything() {
        let report = run(
            &params(|g| {
                g.max_position_usdc = Some(dec!(150));
                g.pause_on_max_position = true;
            }),
            &ticks(&[dec!(100), dec!(95)]),
        )
        .unwrap();
        assert_eq!(report.buys, 1);
        assert!(report.paused_at_ms.is_some());
    }

    #[test]
    fn cancel_opening_at_the_cap_keeps_exits_and_resumes() {
        let p = params(|g| {
            g.max_position_usdc = Some(dec!(150));
            g.cancel_opening_on_max_position = true;
        });
        let mut sim = Sim::new(&p, dec!(100), 0);
        sim.maintain();
        assert!(sim.orders.iter().any(|o| o.side == OrderSide::Buy));

        // Position value 200 is above the 150 cap: openings are cancelled, exits stay.
        sim.size = dec!(2);
        sim.entry = dec!(100);
        sim.maintain();
        assert!(sim.paused.is_none());
        assert!(sim.orders.iter().all(|o| o.side == OrderSide::Sell));
        assert!(!sim.orders.is_empty());

        // Back under the cap with room for another order: openings return.
        sim.size = dec!(0.5);
        sim.maintain();
        assert!(sim.orders.iter().any(|o| o.side == OrderSide::Buy));
    }

    #[test]
    fn short_grid_profits_when_price_falls_back() {
        let report = run(
            &params(|g| g.direction = GridDirection::Short),
            &ticks(&[dec!(100), dec!(101.5), dec!(99.5)]),
        )
        .unwrap();
        assert_eq!((report.sells, report.buys), (1, 1));
        assert!(report.realized_pnl > Decimal::ZERO);
    }

    #[test]
    fn funding_is_charged_to_longs_each_eight_hours() {
        let mut p = params(|_| {});
        p.funding_rate_8h = dec!(0.001);
        let eight_hours = FUNDING_PERIOD_MS;
        let candles = vec![
            Candle::tick(eight_hours - 60_000, dec!(100)),
            Candle::tick(eight_hours - 30_000, dec!(98.5)),
            Candle::tick(eight_hours + 60_000, dec!(98.5)),
        ];
        let report = run(&p, &candles).unwrap();
        // Long 1.01 @ 99 held across one boundary: 1.01 * 98.5 * 0.001.
        assert_eq!(report.funding, dec!(0.0995));
    }

    #[test]
    fn csv_accepts_ticks_candles_and_a_header() {
        let ticks = parse_csv("time,price\n1000,100.5\n2000,101\n").unwrap();
        assert_eq!(
            ticks,
            vec![
                Candle::tick(1000, dec!(100.5)),
                Candle::tick(2000, dec!(101))
            ]
        );
        let candles = parse_csv("1000,100,102,99,101,12.5,x\n").unwrap();
        assert_eq!(
            candles,
            vec![Candle {
                time_ms: 1000,
                open: dec!(100),
                high: dec!(102),
                low: dec!(99),
                close: dec!(101)
            }]
        );
        assert!(parse_csv("1000,100,101\n").is_err());
    }

    #[test]
    fn average_cost_accounting_matches_cash_flow_on_a_noisy_path() {
        for direction in [GridDirection::Long, GridDirection::Short] {
            let p = params(|g| {
                g.direction = direction;
                g.grid_interval = dec!(0.5);
            });
            let mut sim = Sim::new(&p, dec!(100), 0);
            sim.maintain();
            let mut seed: u64 = 12345;
            let mut price = dec!(100);
            for _ in 0..3000 {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let step = Decimal::from((seed >> 33) % 21) / dec!(10) - dec!(1);
                price = (price + step).max(dec!(60));
                sim.move_to(price);
            }
            assert!(sim.fills > 100);
            let gross = sim.realized + sim.unrealized();
            // The weighted-average entry divides, so allow Decimal rounding dust.
            let expected = sim.cash_flow + sim.size * sim.market;
            assert!(
                (gross - expected).abs() < dec!(0.000000001),
                "{gross} vs {expected}"
            );
        }
    }

    #[test]
    fn sweep_runs_every_combination_over_the_same_candles_best_first() {
        let candles = ticks(&[dec!(100), dec!(97), dec!(103), dec!(97), dec!(103)]);
        let base = params(|_| {});
        let intervals = [dec!(1), dec!(2), dec!(0.5)];
        let rows = sweep(
            &base,
            &intervals,
            &[dec!(100), dec!(200)],
            &[3],
            &[3, 5],
            &candles,
        )
        .unwrap();
        assert_eq!(rows.len(), 3 * 2 * 2);
        assert!(rows
            .windows(2)
            .all(|w| w[0].report.net_total >= w[1].report.net_total));
        // Each row matches a standalone replay of the same parameters.
        for row in &rows {
            let mut single = base.clone();
            single.grid.grid_interval = row.grid_interval;
            single.grid.order_amount_usdc = row.order_amount_usdc;
            single.grid.buy_window = row.buy_window;
            single.grid.sell_window = row.sell_window;
            let alone = run(&single, &candles).unwrap();
            assert_eq!(alone.net_total, row.report.net_total);
            assert_eq!(alone.fills, row.report.fills);
        }
        // Spacing changes the result, so the sweep is not just repeating one run.
        assert!(rows.iter().any(|r| r.report.fills != rows[0].report.fills));
    }

    #[test]
    fn sweep_fails_on_any_invalid_combination_and_caps_its_size() {
        let candles = ticks(&[dec!(100), dec!(98)]);
        let base = params(|_| {});
        // 0.005 is below the 0.01 tick: the whole sweep must fail, not skip it.
        assert!(sweep(
            &base,
            &[dec!(1), dec!(0.005)],
            &[dec!(100)],
            &[3],
            &[3],
            &candles
        )
        .is_err());
        assert!(sweep(&base, &[], &[dec!(100)], &[3], &[3], &candles).is_err());
        let many: Vec<usize> = (1..=21).collect();
        assert!(sweep(&base, &[dec!(1)], &[dec!(100)], &many, &many, &candles).is_err());
    }

    #[test]
    fn exit_floor_blocks_exits_below_average_entry_plus_the_margin() {
        let sells = |floor: Option<Decimal>, direction: GridDirection| {
            let mut p = params(|g| g.direction = direction);
            p.exit_floor_spacings = floor;
            let mut sim = Sim::new(&p, dec!(100), 0);
            // Long 2 at 103 or short 2 at 97, market back at 100.
            sim.size = if direction == GridDirection::Long {
                dec!(2)
            } else {
                dec!(-2)
            };
            sim.entry = if direction == GridDirection::Long {
                dec!(103)
            } else {
                dec!(97)
            };
            sim.maintain();
            let side = direction.closing_side();
            let mut exits: Vec<Decimal> = sim
                .orders
                .iter()
                .filter(|o| o.side == side)
                .map(|o| o.price)
                .collect();
            exits.sort();
            exits
        };
        // Without a floor the long exits sit at 101..103 although entry was 103.
        assert!(sells(None, GridDirection::Long)
            .iter()
            .any(|p| *p < dec!(103)));
        // Floor of 1 spacing: nothing below 104 survives, and the market is below it.
        assert!(sells(Some(dec!(1)), GridDirection::Long)
            .iter()
            .all(|p| *p >= dec!(104)));
        assert!(sells(Some(dec!(0)), GridDirection::Long)
            .iter()
            .all(|p| *p >= dec!(103)));
        // Short mirror: entry 97, floor 1 spacing means exits at or below 96.
        assert!(sells(None, GridDirection::Short)
            .iter()
            .any(|p| *p > dec!(97)));
        assert!(sells(Some(dec!(1)), GridDirection::Short)
            .iter()
            .all(|p| *p <= dec!(96)));
    }

    #[test]
    fn exit_floor_never_realizes_a_loss_on_a_whipsaw() {
        // Long-only grid over a down-then-up path: with a zero floor every exit is at or above
        // the average entry, so realized PnL cannot be negative.
        let mut path = Vec::new();
        for i in 0..40 {
            path.push(dec!(100) - Decimal::from(i) / dec!(4));
        }
        for i in 0..40 {
            path.push(dec!(90) + Decimal::from(i) / dec!(8));
        }
        let mut p = params(|g| g.max_position_usdc = Some(dec!(800)));
        p.maker_fee_rate = Decimal::ZERO;
        let plain = run(&p, &ticks(&path)).unwrap();
        p.exit_floor_spacings = Some(Decimal::ZERO);
        let floored = run(&p, &ticks(&path)).unwrap();
        assert!(
            plain.realized_pnl < Decimal::ZERO,
            "setup should lose without a floor"
        );
        assert!(floored.realized_pnl >= Decimal::ZERO);
    }

    #[test]
    fn prices_outside_the_rules_price_range_fail_loudly_instead_of_trading_nothing() {
        let mut p = params(|_| {});
        p.rules.max_price = dec!(100000);
        let error = run(&p, &ticks(&[dec!(118000), dec!(118100)])).unwrap_err();
        assert!(error.to_string().contains("价格范围"));
        p.rules.max_price = dec!(1000000);
        assert!(run(&p, &ticks(&[dec!(118000), dec!(118100)])).is_ok());
    }

    #[test]
    fn invalid_input_is_rejected() {
        let good = ticks(&[dec!(100)]);
        assert!(run(&params(|_| {}), &[]).is_err());
        assert!(run(&params(|g| g.grid_interval = dec!(0.005)), &good).is_err());
        assert!(run(&params(|g| g.pause_on_max_position = true), &good).is_err());
        let mut negative_fee = params(|_| {});
        negative_fee.maker_fee_rate = dec!(-0.0001);
        assert!(run(&negative_fee, &good).is_err());
        let backwards = vec![Candle::tick(2, dec!(100)), Candle::tick(1, dec!(100))];
        assert!(run(&params(|_| {}), &backwards).is_err());

        // When a fill in the opposite direction crosses zero, entry resets to fill price.
        let p = params(|g| g.direction = GridDirection::Short);
        let mut sim = Sim::new(&p, dec!(100), 0);
        sim.size = dec!(1);
        sim.entry = dec!(95);
        sim.orders.push(BtOrder {
            side: OrderSide::Sell,
            price: dec!(105),
            qty: dec!(2),
            remainder: false,
        });
        sim.fill_at(OrderSide::Sell, dec!(105));
        assert_eq!(sim.size, dec!(-1));
        assert_eq!(sim.entry, dec!(105));
    }

    #[tokio::test]
    async fn cli_rules_reject_invalid_fee_and_filter_overrides() {
        let base = BacktestArgs {
            symbol: None,
            csv: None,
            interval: "1m".into(),
            days: 7,
            direction: DirectionArg::Long,
            grid_interval: vec![dec!(1)],
            amount: vec![dec!(100)],
            buy_window: vec![3],
            sell_window: vec![3],
            min_price: None,
            max_price: None,
            max_position: None,
            pause_on_max_position: false,
            cancel_opening_on_max_position: false,
            maker_fee: dec!(0.0002),
            funding_rate: Decimal::ZERO,
            capital: dec!(1000),
            tick_size: None,
            step_size: None,
            min_qty: None,
            min_notional: None,
            exit_floor: None,
            json: false,
        };
        assert!(load_rules(&BacktestArgs {
            maker_fee: dec!(-0.0001),
            ..base.clone()
        })
        .await
        .is_err());
        assert!(load_rules(&BacktestArgs {
            min_qty: Some(Decimal::ZERO),
            ..base.clone()
        })
        .await
        .is_err());
        assert!(load_rules(&BacktestArgs {
            min_notional: Some(Decimal::ZERO),
            ..base
        })
        .await
        .is_err());
    }
}
