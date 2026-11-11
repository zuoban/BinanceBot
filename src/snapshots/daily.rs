use super::{
    aggregate_snapshot_bucket, select_valued_bucket_account, snapshot_max_age_secs,
    within_capture_window, SnapshotBucketKey,
};
use crate::runtime::RuntimeHandle;
use crate::types::{is_timestamp_fresh, resolve_quote_asset, BotSnapshot, TradingMode};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DailyAccountGroup {
    pub mode: TradingMode,
    pub asset: String,
    #[serde(default)]
    pub account_id: Option<String>,
    pub count: usize,
    pub equity: Option<Decimal>,
    pub available: Option<Decimal>,
    pub wallet: Option<Decimal>,
    pub unrealized: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DailyPositionGroup {
    pub mode: TradingMode,
    pub asset: String,
    pub count: usize,
    pub long: Option<Decimal>,
    pub short: Option<Decimal>,
    pub total: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DailyPositionItem {
    pub symbol: String,
    pub mode: TradingMode,
    pub direction: crate::types::GridDirection,
    pub status: crate::types::BotStatus,
    pub size: Decimal,
    pub entry_price: Decimal,
    pub mark_price: Option<Decimal>,
    pub position_amount: Option<Decimal>,
    pub unrealized_pnl: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DailyTradeGroup {
    pub symbol: String,
    pub mode: TradingMode,
    pub buy_count: u64,
    pub sell_count: u64,
    pub realized_pnl: Decimal,
    pub pending_pnl_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DailySnapshot {
    pub date: String,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub generated_at: DateTime<Utc>,
    #[serde(default)]
    pub accounts: Vec<DailyAccountGroup>,
    #[serde(default)]
    pub positions: Vec<DailyPositionGroup>,
    #[serde(default)]
    pub pair_positions: Vec<DailyPositionItem>,
    #[serde(default)]
    pub trades: Vec<DailyTradeGroup>,
}

fn build_daily_position_item(
    snapshot: &BotSnapshot,
    sync_interval: u64,
    now: DateTime<Utc>,
) -> DailyPositionItem {
    let mode = snapshot.mode;
    let position_fresh = mode == TradingMode::Paper
        || snapshot
            .position
            .update_time
            .is_some_and(|at| is_timestamp_fresh(at, now, snapshot_max_age_secs(sync_interval)));
    let mark_fresh = snapshot.ticker.mark_price_is_fresh(now);
    let mark_price = mark_fresh.then_some(snapshot.ticker.mark_price);
    let size = snapshot.position.size;
    let (position_amount, unrealized_pnl) = if !position_fresh {
        (None, None)
    } else if size.is_zero() {
        (Some(Decimal::ZERO), Some(Decimal::ZERO))
    } else if let Some(mark) = mark_price {
        (
            size.abs().checked_mul(mark),
            Some(snapshot.position.unrealized_pnl),
        )
    } else {
        (None, None)
    };
    DailyPositionItem {
        symbol: snapshot.symbol.clone(),
        mode,
        direction: snapshot.grid_config.direction,
        status: snapshot.status,
        size,
        entry_price: snapshot.position.entry_price,
        mark_price,
        position_amount,
        unrealized_pnl,
    }
}

fn build_daily_account_group(
    mode: TradingMode,
    asset: String,
    account_id: Option<String>,
    items: &[(&BotSnapshot, u64)],
    now: DateTime<Utc>,
) -> DailyAccountGroup {
    let (latest, valued) = select_valued_bucket_account(mode, &asset, items, now);
    let wallet = if mode == TradingMode::Paper {
        items
            .iter()
            .all(|(s, _)| s.account.asset == asset)
            .then(|| latest.map(|acc| acc.total_wallet_balance))
            .flatten()
    } else {
        valued.map(|acc| acc.total_wallet_balance)
    };
    DailyAccountGroup {
        mode,
        asset,
        account_id,
        count: items.len(),
        equity: valued.map(|acc| acc.margin_balance),
        available: valued.map(|acc| acc.available_balance),
        wallet,
        unrealized: valued.map(|acc| acc.unrealized_profit),
    }
}

fn build_daily_position_group(
    mode: TradingMode,
    asset: String,
    items: Vec<(&BotSnapshot, u64)>,
    now: DateTime<Utc>,
) -> DailyPositionGroup {
    let count = items.len();
    let group = aggregate_snapshot_bucket(mode, asset.clone(), None, items, now);
    let total = group
        .long
        .zip(group.short)
        .and_then(|(long, short)| long.checked_add(short));
    DailyPositionGroup {
        mode,
        asset,
        count,
        long: group.long,
        short: group.short,
        total,
    }
}

pub(crate) fn build_daily_snapshot(
    date: String,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    now: DateTime<Utc>,
    snapshots: &[(BotSnapshot, u64, Option<String>)],
    trades: Vec<DailyTradeGroup>,
) -> DailySnapshot {
    let mut account_buckets: BTreeMap<SnapshotBucketKey, Vec<(&BotSnapshot, u64)>> =
        BTreeMap::new();
    let mut position_buckets: BTreeMap<(String, String), Vec<(&BotSnapshot, u64)>> =
        BTreeMap::new();
    let mut pair_positions = Vec::with_capacity(snapshots.len());

    for (snapshot, sync_interval, account_id) in snapshots {
        let mode_str = snapshot.mode.as_str();
        let asset = resolve_quote_asset(&snapshot.symbol, &snapshot.account.asset);
        account_buckets
            .entry((mode_str.to_owned(), asset.to_owned(), account_id.clone()))
            .or_default()
            .push((snapshot, *sync_interval));
        position_buckets
            .entry((mode_str.to_owned(), asset.to_owned()))
            .or_default()
            .push((snapshot, *sync_interval));
        pair_positions.push(build_daily_position_item(snapshot, *sync_interval, now));
    }
    pair_positions.sort_by(|a, b| {
        a.mode
            .as_str()
            .cmp(b.mode.as_str())
            .then_with(|| a.symbol.cmp(&b.symbol))
    });
    let accounts = account_buckets
        .into_iter()
        .map(|((mode_str, asset, account_id), items)| {
            build_daily_account_group(
                TradingMode::from_db(&mode_str),
                asset,
                account_id,
                &items,
                now,
            )
        })
        .collect();
    let positions = position_buckets
        .into_iter()
        .map(|((mode_str, asset), items)| {
            build_daily_position_group(TradingMode::from_db(&mode_str), asset, items, now)
        })
        .collect();
    DailySnapshot {
        date,
        window_start,
        window_end,
        generated_at: now,
        accounts,
        positions,
        pair_positions,
        trades,
    }
}

pub(super) fn is_beijing_midnight_hour(hour: i64) -> bool {
    (hour + 8 * 3600).rem_euclid(24 * 3600) == 0
}

pub(crate) async fn capture_daily_snapshot_at_midnight(
    handle: &RuntimeHandle,
    midnight_hour: i64,
    now: DateTime<Utc>,
    snapshots: &[(BotSnapshot, u64, Option<String>)],
) -> anyhow::Result<Option<DailySnapshot>> {
    if !is_beijing_midnight_hour(midnight_hour) || !within_capture_window(midnight_hour, now) {
        return Ok(None);
    }
    let Some(window_end) = DateTime::from_timestamp(midnight_hour, 0) else {
        return Ok(None);
    };
    let window_start = window_end - chrono::Duration::days(1);
    let date = window_start
        .with_timezone(&crate::types::beijing_offset())
        .format("%Y-%m-%d")
        .to_string();
    let trades = handle
        .db
        .run_blocking(move |db| db.daily_trade_summaries(window_start, window_end))
        .await?;
    if !within_capture_window(midnight_hour, Utc::now()) {
        return Ok(None);
    }
    let snapshot = build_daily_snapshot(date, window_start, window_end, now, snapshots, trades);
    let saved = snapshot.clone();
    let inserted = handle
        .db
        .run_blocking(move |db| db.save_daily_snapshot(&saved))
        .await?;
    Ok(inserted.then_some(snapshot))
}

pub(super) async fn notify_daily_snapshot(handle: &RuntimeHandle, snapshot: &DailySnapshot) {
    let states = handle.active_states().await;
    let Some(first) = states.first().cloned() else {
        return;
    };
    let telegram = match handle.db.load_saved_config().await {
        Ok(saved) => saved.telegram,
        Err(_) => first.config.read().await.telegram.clone(),
    };
    if !telegram.enabled {
        return;
    }
    let notifier = first.telegram_notifier().clone();
    let snapshot = snapshot.clone();
    tokio::spawn(async move {
        if let Err(error) = notifier
            .send_daily_snapshot_notification(&telegram, &snapshot)
            .await
        {
            tracing::warn!(%error, "Failed to send Telegram daily snapshot notification");
            first
                .add_log("WARN", format!("Telegram 每日快照通知发送失败: {}", error))
                .await;
        }
    });
}
