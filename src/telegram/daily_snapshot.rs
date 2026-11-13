use super::{
    escape_html, format_available_ratio, format_beijing_time, format_optional_money, mode_label,
};
use crate::types::format_decimal_dp;
use rust_decimal::Decimal;
use std::borrow::Cow;

type TextEscaper = fn(&str) -> Cow<'_, str>;

fn plain_text(value: &str) -> Cow<'_, str> {
    Cow::Borrowed(value)
}

fn html_text(value: &str) -> Cow<'_, str> {
    Cow::Owned(escape_html(value))
}

fn format_daily_account_lines(
    accounts: &[crate::snapshots::DailyAccountGroup],
    escape: TextEscaper,
) -> String {
    if accounts.is_empty() {
        return "暂无账户数据".to_string();
    }
    accounts
        .iter()
        .map(|acc| {
            let asset = escape(&acc.asset);
            let ratio = format_available_ratio(acc.available, acc.wallet);
            format!(
                "· {mode} {asset}：权益 {equity}｜可用 {available}（{ratio}）｜浮盈 {unrealized}",
                mode = mode_label(acc.mode),
                equity = format_optional_money(acc.equity, asset.as_ref(), false),
                available = format_optional_money(acc.available, asset.as_ref(), false),
                unrealized = format_optional_money(acc.unrealized, asset.as_ref(), true),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_daily_pnl_lines(
    trades: &[crate::snapshots::DailyTradeGroup],
    escape: TextEscaper,
) -> String {
    let mut pnl_groups = std::collections::BTreeMap::new();
    for trade in trades {
        let asset = crate::types::quote_asset(&trade.symbol).unwrap_or(&trade.symbol);
        let entry = pnl_groups.entry((trade.mode.as_str(), asset)).or_insert((
            trade.mode,
            Decimal::ZERO,
            0u64,
        ));
        entry.1 = entry.1.saturating_add(trade.realized_pnl);
        entry.2 = entry.2.saturating_add(trade.pending_pnl_count);
    }
    if pnl_groups.is_empty() {
        return "· 0.00".to_string();
    }
    pnl_groups
        .into_iter()
        .map(|((_, asset), (mode, pnl, pending))| {
            let asset = escape(asset);
            let formatted_pnl = format_decimal_dp(pnl, 4, true);
            let mode = mode_label(mode);
            if pending > 0 {
                format!("· {mode} {asset}：{formatted_pnl} {asset}（{pending} 笔待同步）")
            } else {
                format!("· {mode} {asset}：{formatted_pnl} {asset}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_daily_position_lines(
    positions: &[crate::snapshots::DailyPositionGroup],
    pair_positions: &[crate::snapshots::DailyPositionItem],
    escape: TextEscaper,
) -> String {
    if positions.is_empty() {
        return "暂无持仓数据".to_string();
    }
    let mut lines: Vec<String> = positions
        .iter()
        .map(|pos| {
            let asset = escape(&pos.asset);
            format!(
                "· {mode} {asset}：总持仓 {total}（多 {long} / 空 {short}）",
                mode = mode_label(pos.mode),
                total = format_optional_money(pos.total, asset.as_ref(), false),
                long = format_optional_money(pos.long, asset.as_ref(), false),
                short = format_optional_money(pos.short, asset.as_ref(), false),
            )
        })
        .collect();
    for item in pair_positions.iter().filter(|p| !p.size.is_zero()) {
        let asset = escape(crate::types::quote_asset(&item.symbol).unwrap_or(&item.symbol));
        let side = if item.size > Decimal::ZERO {
            "多"
        } else {
            "空"
        };
        lines.push(format!(
            "  - {symbol}（{side} {qty}）：{amount}｜浮盈 {unrealized}",
            symbol = escape(&item.symbol),
            qty = item.size.abs().normalize(),
            amount = format_optional_money(item.position_amount, asset.as_ref(), false),
            unrealized = format_optional_money(item.unrealized_pnl, asset.as_ref(), true),
        ));
    }
    lines.join("\n")
}

fn daily_trade_totals(trades: &[crate::snapshots::DailyTradeGroup]) -> (u64, u64, u64) {
    let buys = trades
        .iter()
        .fold(0u64, |acc, t| acc.saturating_add(t.buy_count));
    let sells = trades
        .iter()
        .fold(0u64, |acc, t| acc.saturating_add(t.sell_count));
    (buys.saturating_add(sells), buys, sells)
}

pub(crate) fn format_daily_snapshot_message(snapshot: &crate::snapshots::DailySnapshot) -> String {
    let account_lines = format_daily_account_lines(&snapshot.accounts, html_text);
    let (total_trades, total_buys, total_sells) = daily_trade_totals(&snapshot.trades);
    let pnl_lines = format_daily_pnl_lines(&snapshot.trades, html_text);
    let position_lines =
        format_daily_position_lines(&snapshot.positions, &snapshot.pair_positions, html_text);
    let time = format_beijing_time(snapshot.generated_at);
    format!(
        "<b>📊 每日运行数据快照（{date}）</b>\n\n<b>【账户概览】</b>\n{account_lines}\n\n<b>【昨日成交与盈亏】</b>\n· 成交笔数：{total_trades} 笔（买 {total_buys} / 卖 {total_sells}）\n· 已实现盈亏：\n{pnl_lines}\n\n<b>【持仓概览】</b>\n{position_lines}\n\n生成时间：{time}（北京时间）",
        date = escape_html(&snapshot.date),
    )
}

pub(crate) fn format_daily_snapshot_plain_body(
    snapshot: &crate::snapshots::DailySnapshot,
) -> String {
    let account_lines = format_daily_account_lines(&snapshot.accounts, plain_text);
    let (total_trades, total_buys, total_sells) = daily_trade_totals(&snapshot.trades);
    let pnl_lines = format_daily_pnl_lines(&snapshot.trades, plain_text);
    let position_lines =
        format_daily_position_lines(&snapshot.positions, &snapshot.pair_positions, plain_text);
    let time = format_beijing_time(snapshot.generated_at);
    format!(
        "【账户概览】\n{account_lines}\n\n【昨日成交与盈亏】\n· 成交笔数：{total_trades} 笔（买 {total_buys} / 卖 {total_sells}）\n· 已实现盈亏：\n{pnl_lines}\n\n【持仓概览】\n{position_lines}\n\n生成时间：{time}（北京时间）"
    )
}
