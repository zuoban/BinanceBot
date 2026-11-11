use crate::runtime::RuntimeHandle;
use crate::types::{
    is_timestamp_fresh, quote_asset, resolve_quote_asset, BotSnapshot, TradingMode,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

mod daily;

#[cfg(test)]
use daily::build_daily_snapshot;
pub(crate) use daily::{
    capture_daily_snapshot_at_midnight, DailyAccountGroup, DailyPositionGroup, DailyPositionItem,
    DailySnapshot, DailyTradeGroup,
};
use daily::{is_beijing_midnight_hour, notify_daily_snapshot};
// Legacy PAPER history summed independent symbol balances; keep that series separate.
const PAPER_SHARED_ACCOUNT_ID: &str = "paper-shared-v1";

mod types {
    use crate::types::TradingMode;
    use rust_decimal::Decimal;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SnapshotGroup {
        pub(crate) mode: TradingMode,
        pub(crate) asset: String,
        #[serde(default)]
        pub(crate) account_id: Option<String>,
        pub(crate) equity: Option<Decimal>,
        pub(crate) long: Option<Decimal>,
        pub(crate) short: Option<Decimal>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct HourlySnapshot {
        pub(crate) hour: i64,
        pub(crate) groups: Vec<SnapshotGroup>,
    }
}

pub(crate) use types::{HourlySnapshot, SnapshotGroup};
#[derive(Debug, Clone, Serialize)]
pub(crate) struct HistoryAccount {
    pub mode: TradingMode,
    pub asset: String,
    pub account_id: Option<String>,
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

pub(super) fn snapshot_max_age_secs(interval: u64) -> i64 {
    interval.saturating_add(15).max(30).min(i64::MAX as u64) as i64
}

pub(super) type SnapshotBucketKey = (String, String, Option<String>);
fn aggregate(
    snapshots: &[(BotSnapshot, u64, Option<String>)],
    now: DateTime<Utc>,
) -> Vec<SnapshotGroup> {
    let mut groups: BTreeMap<SnapshotBucketKey, Vec<(&BotSnapshot, u64)>> = BTreeMap::new();
    for (snapshot, sync_interval, account_id) in snapshots {
        let mode = snapshot.mode;
        let asset = resolve_quote_asset(&snapshot.symbol, &snapshot.account.asset);
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

pub(super) fn select_valued_bucket_account<'a>(
    mode: TradingMode,
    asset: &str,
    items: &[(&'a BotSnapshot, u64)],
    now: DateTime<Utc>,
) -> (
    Option<&'a crate::types::AccountInfo>,
    Option<&'a crate::types::AccountInfo>,
) {
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
        .map(|(s, _)| &s.account);
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
    (latest, latest.filter(|_| paper_valued))
}

pub(super) fn aggregate_snapshot_bucket(
    mode: TradingMode,
    asset: String,
    account_id: Option<String>,
    snapshots: Vec<(&BotSnapshot, u64)>,
    now: DateTime<Utc>,
) -> SnapshotGroup {
    let (_, valued) = select_valued_bucket_account(mode, &asset, &snapshots, now);
    let equity = valued.map(|acc| acc.margin_balance);
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

fn live_account_identity(
    mode: TradingMode,
    exchange: &crate::config::ExchangeConfig,
) -> Option<[u8; 32]> {
    (mode == TradingMode::Live)
        .then(|| crate::exchange::BinanceFuturesClient::configured_account_identity(exchange))
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
            None => resolve_quote_asset(symbol, &state.account.read().await.asset).to_owned(),
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
        let sync_interval = snapshot.sync_interval_secs;
        (snapshot, sync_interval, identity)
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

pub(super) fn within_capture_window(hour: i64, now: DateTime<Utc>) -> bool {
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
mod tests;
