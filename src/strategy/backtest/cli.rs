use super::data::{fetch_klines, parse_csv, validate_kline_interval, MAX_BACKTEST_DAYS};
use super::{sweep, BacktestParams, BacktestReport, Candle, SweepRow};
use crate::config::{AppConfig, GridConfig};
use crate::exchange::client::BinanceFuturesClient;
use crate::strategy::precision::SymbolRules;
use crate::types::GridDirection;
use anyhow::{bail, ensure, Context, Result};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::path::PathBuf;

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

pub(super) async fn load_rules(args: &BacktestArgs) -> Result<SymbolRules> {
    ensure!(args.maker_fee >= Decimal::ZERO, "--maker-fee 不能为负数");
    let mut rules = match &args.symbol {
        Some(symbol) => {
            crate::config::validate_symbol(symbol)?;
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
    if args.csv.is_none() && args.symbol.is_some() {
        validate_kline_interval(&args.interval)?;
        ensure!(
            (1..=MAX_BACKTEST_DAYS).contains(&args.days),
            "--days 必须在 1..={MAX_BACKTEST_DAYS} 之间"
        );
    }
    let rules = load_rules(args).await?;
    let candles = match (&args.csv, &args.symbol) {
        (Some(path), _) => parse_csv(
            &std::fs::read_to_string(path).with_context(|| format!("无法读取 {path:?}"))?,
        )?,
        (None, Some(symbol)) => {
            let end = chrono::Utc::now().timestamp_millis();
            let start = end.saturating_sub(i64::from(args.days) * 24 * 60 * 60 * 1000);
            fetch_klines(symbol, &args.interval, start, end).await?
        }
        (None, None) => bail!("请提供 --csv 或 --symbol"),
    };
    ensure!(
        !args.grid_interval.is_empty()
            && !args.amount.is_empty()
            && !args.buy_window.is_empty()
            && !args.sell_window.is_empty(),
        "至少需要一组参数"
    );
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
