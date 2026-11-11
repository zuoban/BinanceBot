use crate::runtime::RuntimeHandle;
use crate::types::{is_timestamp_fresh, quote_asset, BotSnapshot, TradingMode};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

// Legacy PAPER history summed independent symbol balances; keep that series separate.
const PAPER_SHARED_ACCOUNT_ID: &str = "paper-shared-v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotGroup {
    pub mode: TradingMode,
    pub asset: String,
    #[serde(default)]
    pub account_id: Option<String>,
    pub equity: Option<Decimal>,
    pub long: Option<Decimal>,
    pub short: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HourlySnapshot {
    pub hour: i64,
    pub groups: Vec<SnapshotGroup>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct HistoryAccount {
    pub mode: TradingMode,
    pub asset: String,
    pub account_id: Option<String>,
}

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

#[derive(Debug, Serialize)]
pub(crate) struct RawHourlySnapshot {
    pub hour: i64,
    pub groups: Box<serde_json::value::RawValue>,
}

#[derive(Debug, Serialize)]
pub(crate) struct RawOverviewHistory {
    pub snapshots: Vec<RawHourlySnapshot>,
    pub current_accounts: Vec<HistoryAccount>,
    pub daily_snapshots: Vec<Box<serde_json::value::RawValue>>,
}

fn snapshot_max_age_secs(interval: u64) -> i64 {
    interval.saturating_add(15).max(30).min(i64::MAX as u64) as i64
}

type SnapshotBucketKey = (String, String, Option<String>);

fn aggregate(
    snapshots: &[(BotSnapshot, u64, Option<String>)],
    now: DateTime<Utc>,
) -> Vec<SnapshotGroup> {
    let mut groups: BTreeMap<SnapshotBucketKey, Vec<(&BotSnapshot, u64)>> = BTreeMap::new();
    for (snapshot, sync_interval, account_id) in snapshots {
        let mode = snapshot.mode;
        let asset = resolve_snapshot_asset(&snapshot.symbol, &snapshot.account.asset);
        groups
            .entry((
                mode.as_str().to_owned(),
                asset.to_owned(),
                account_id.clone(),
            ))
            .or_default()
            .push((snapshot, *sync_interval));
    }
    groups
        .into_iter()
        .map(|((mode, asset, account_id), snapshots)| {
            aggregate_snapshot_bucket(
                TradingMode::from_db(&mode),
                asset,
                account_id,
                snapshots,
                now,
            )
        })
        .collect()
}

fn aggregate_snapshot_bucket(
    mode: TradingMode,
    asset: String,
    account_id: Option<String>,
    snapshots: Vec<(&BotSnapshot, u64)>,
    now: DateTime<Utc>,
) -> SnapshotGroup {
    let latest = snapshots
        .iter()
        .filter(|(s, interval)| {
            s.account.asset == asset
                && (mode == TradingMode::Paper
                    || is_timestamp_fresh(
                        s.account.update_time,
                        now,
                        snapshot_max_age_secs(*interval),
                    ))
        })
        .max_by_key(|(s, _)| s.account.update_time)
        .copied();
    let equity = if mode == TradingMode::Paper {
        if snapshots.iter().all(|(s, interval)| {
            s.account.asset == asset
                && (s.position.size.is_zero()
                    || (is_timestamp_fresh(
                        s.account.update_time,
                        now,
                        snapshot_max_age_secs(*interval),
                    ) && s.ticker.mark_price_is_fresh(now)))
        }) {
            latest.map(|(s, _)| s.account.margin_balance)
        } else {
            None
        }
    } else {
        latest.map(|(s, _)| s.account.margin_balance)
    };
    let mut long = Decimal::ZERO;
    let mut short = Decimal::ZERO;
    let mut valid = true;
    for (snapshot, interval) in snapshots {
        if mode != TradingMode::Paper
            && !snapshot
                .position
                .update_time
                .is_some_and(|at| is_timestamp_fresh(at, now, snapshot_max_age_secs(interval)))
        {
            valid = false;
            continue;
        }
        let size = snapshot.position.size;
        if size.is_zero() {
            continue;
        }
        if !snapshot.ticker.mark_price_is_fresh(now) {
            valid = false;
            continue;
        }
        let Some(amount) = size.abs().checked_mul(snapshot.ticker.mark_price) else {
            valid = false;
            continue;
        };
        let acc = if size > Decimal::ZERO {
            &mut long
        } else {
            &mut short
        };
        match acc.checked_add(amount) {
            Some(sum) => *acc = sum,
            None => valid = false,
        }
    }
    SnapshotGroup {
        mode,
        asset,
        account_id,
        equity,
        long: valid.then_some(long),
        short: valid.then_some(short),
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
        let mode = snapshot.mode;
        let asset = resolve_snapshot_asset(&snapshot.symbol, &snapshot.account.asset);
        account_buckets
            .entry((
                mode.as_str().to_owned(),
                asset.to_owned(),
                account_id.clone(),
            ))
            .or_default()
            .push((snapshot, *sync_interval));
        position_buckets
            .entry((mode.as_str().to_owned(), asset.to_owned()))
            .or_default()
            .push((snapshot, *sync_interval));

        let position_fresh = mode == TradingMode::Paper
            || snapshot.position.update_time.is_some_and(|at| {
                is_timestamp_fresh(at, now, snapshot_max_age_secs(*sync_interval))
            });
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
        pair_positions.push(DailyPositionItem {
            symbol: snapshot.symbol.clone(),
            mode,
            direction: snapshot.grid_config.direction,
            status: snapshot.status,
            size,
            entry_price: snapshot.position.entry_price,
            mark_price,
            position_amount,
            unrealized_pnl,
        });
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
            let mode = TradingMode::from_db(&mode_str);
            let count = items.len();
            let latest = items
                .iter()
                .filter(|(s, interval)| {
                    s.account.asset == asset
                        && (mode == TradingMode::Paper
                            || is_timestamp_fresh(
                                s.account.update_time,
                                now,
                                snapshot_max_age_secs(*interval),
                            ))
                })
                .max_by_key(|(s, _)| s.account.update_time)
                .copied();
            let paper_valued = mode != TradingMode::Paper
                || items.iter().all(|(s, interval)| {
                    s.account.asset == asset
                        && (s.position.size.is_zero()
                            || (is_timestamp_fresh(
                                s.account.update_time,
                                now,
                                snapshot_max_age_secs(*interval),
                            ) && s.ticker.mark_price_is_fresh(now)))
                });
            let valued = latest.filter(|_| paper_valued).map(|(s, _)| &s.account);
            let wallet = if mode == TradingMode::Paper {
                items
                    .iter()
                    .all(|(s, _)| s.account.asset == asset)
                    .then(|| latest.map(|(s, _)| s.account.total_wallet_balance))
                    .flatten()
            } else {
                valued.map(|acc| acc.total_wallet_balance)
            };
            DailyAccountGroup {
                mode,
                asset,
                account_id,
                count,
                equity: valued.map(|acc| acc.margin_balance),
                available: valued.map(|acc| acc.available_balance),
                wallet,
                unrealized: valued.map(|acc| acc.unrealized_profit),
            }
        })
        .collect();

    let positions = position_buckets
        .into_iter()
        .map(|((mode_str, asset), items)| {
            let mode = TradingMode::from_db(&mode_str);
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

fn is_beijing_midnight_hour(hour: i64) -> bool {
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

async fn notify_daily_snapshot(handle: &RuntimeHandle, snapshot: &DailySnapshot) {
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

fn resolve_snapshot_asset<'a>(symbol: &'a str, account_asset: &'a str) -> &'a str {
    quote_asset(symbol).unwrap_or(if account_asset.is_empty() {
        symbol
    } else {
        account_asset
    })
}

fn live_account_identity(
    mode: TradingMode,
    exchange: &crate::config::ExchangeConfig,
) -> Option<[u8; 32]> {
    (mode == TradingMode::Live).then(|| {
        crate::exchange::client::BinanceFuturesClient::configured_account_identity(exchange)
    })
}

async fn resolve_account_id(
    handle: &RuntimeHandle,
    cache: &mut HashMap<[u8; 32], String>,
    mode: TradingMode,
    identity: Option<[u8; 32]>,
) -> anyhow::Result<Option<String>> {
    match identity {
        Some(identity) => {
            if let Some(id) = cache.get(&identity) {
                return Ok(Some(id.clone()));
            }
            if let Some(id) = handle.db.cached_history_account_id(&identity) {
                cache.insert(identity, id.clone());
                return Ok(Some(id));
            }
            let id = handle
                .db
                .run_blocking(move |db| db.history_account_id(identity))
                .await?;
            cache.insert(identity, id.clone());
            Ok(Some(id))
        }
        None if mode == TradingMode::Paper => Ok(Some(PAPER_SHARED_ACCOUNT_ID.to_owned())),
        None => Ok(None),
    }
}

pub(crate) async fn current_accounts(
    handle: &RuntimeHandle,
) -> anyhow::Result<Vec<HistoryAccount>> {
    // Global changes hold this lock until persistence or rollback is complete.
    let (states, committed) = {
        let _membership = handle.membership_lock.lock().await;
        let states = handle.active_states().await;
        let config = handle.db.load_saved_config().await?;
        (states, config.strategy_configs()?)
    };
    let mut accounts = BTreeMap::new();
    let mut identities = HashMap::new();
    for state in states {
        let config = match state.pair_scope() {
            Some(symbol) => committed
                .iter()
                .find(|config| config.exchange.symbol == symbol),
            None => committed.first(),
        };
        let Some(config) = config else { continue };
        let symbol = config.exchange.symbol.as_str();
        let mode = config.exchange.trading_mode();
        let account_id = resolve_account_id(
            handle,
            &mut identities,
            mode,
            live_account_identity(mode, &config.exchange),
        )
        .await?;
        let asset = match quote_asset(symbol) {
            Some(asset) => asset.to_owned(),
            None => resolve_snapshot_asset(symbol, &state.account.read().await.asset).to_owned(),
        };
        accounts
            .entry((mode.as_str().to_owned(), asset.clone(), account_id.clone()))
            .or_insert(HistoryAccount {
                mode,
                asset,
                account_id,
            });
    }
    Ok(accounts.into_values().collect())
}

pub(crate) async fn collect_hourly(
    handle: &RuntimeHandle,
) -> anyhow::Result<Option<Vec<(BotSnapshot, u64, Option<String>)>>> {
    // Never queue behind an in-progress configuration or membership change.
    let Ok(_membership) = handle.membership_lock.try_lock() else {
        return Ok(None);
    };
    let states = handle.active_states().await;
    let mut updates = Vec::with_capacity(states.len());
    for state in &states {
        let Ok(update) = state.config_update_lock.try_lock() else {
            return Ok(None);
        };
        updates.push(update);
    }
    let observations = futures_util::future::join_all(states.iter().map(|state| async {
        let snapshot = state.snapshot_for_hourly().await;
        let config = state.config.read().await;
        let identity = live_account_identity(snapshot.mode, &config.exchange);
        (snapshot, config.exchange.sync_interval_secs, identity)
    }))
    .await;
    let mut identities = HashMap::new();
    let mut snapshots = Vec::with_capacity(observations.len());
    for (snapshot, interval, identity) in observations {
        let account_id =
            resolve_account_id(handle, &mut identities, snapshot.mode, identity).await?;
        snapshots.push((snapshot, interval, account_id));
    }
    Ok(Some(snapshots))
}

fn within_capture_window(hour: i64, now: DateTime<Utc>) -> bool {
    now.timestamp() >= hour && now.timestamp() - hour < 60
}

pub async fn run_hourly_snapshots(handle: RuntimeHandle) {
    if let Err(error) = handle
        .db
        .run_blocking(|db| {
            let now = Utc::now();
            db.cleanup_hourly_account_snapshots(now.timestamp())?;
            db.cleanup_daily_snapshots(now)
        })
        .await
    {
        tracing::warn!(%error, "Could not clean up account snapshots");
    }
    loop {
        let now = Utc::now().timestamp();
        let next = (now.div_euclid(3600) + 1) * 3600;
        let delay = u64::try_from(next - now).unwrap_or(1).max(1);
        tokio::time::sleep(Duration::from_secs(delay)).await;
        let now = Utc::now();
        // Never attach observations collected outside the first minute to this hour.
        if !within_capture_window(next, now) {
            continue;
        }
        let Some(deadline) = DateTime::from_timestamp(next + 60, 0) else {
            continue;
        };
        let remaining = (deadline - now).to_std().unwrap_or_default();
        let snapshots = match tokio::time::timeout(remaining, collect_hourly(&handle)).await {
            Ok(Ok(Some(snapshots))) => snapshots,
            Ok(Ok(None)) | Err(_) => continue,
            Ok(Err(error)) => {
                tracing::warn!(%error, "Could not identify account snapshots");
                continue;
            }
        };
        let now = Utc::now();
        if !within_capture_window(next, now) {
            continue;
        }
        let groups = aggregate(&snapshots, now);
        if !within_capture_window(next, Utc::now()) {
            continue;
        }
        if let Err(error) = handle
            .db
            .run_blocking(move |db| db.save_hourly_account_snapshot(next, &groups))
            .await
        {
            tracing::warn!(%error, "Could not save account snapshot");
        }
        if is_beijing_midnight_hour(next) {
            match capture_daily_snapshot_at_midnight(&handle, next, now, &snapshots).await {
                Ok(Some(daily)) => notify_daily_snapshot(&handle, &daily).await,
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(%error, "Could not save daily snapshot");
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "snapshots_tests.rs"]
mod tests;
