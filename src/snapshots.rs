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
pub struct HistoryAccount {
    pub mode: TradingMode,
    pub asset: String,
    pub account_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct OverviewHistory {
    pub snapshots: Vec<HourlySnapshot>,
    pub current_accounts: Vec<HistoryAccount>,
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
            let mode = TradingMode::from_db(&mode);
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
                            ) && s.ticker.mark_price > Decimal::ZERO
                                && is_timestamp_fresh(s.ticker.mark_update_time, now, 15)))
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
                    && !snapshot.position.update_time.is_some_and(|at| {
                        is_timestamp_fresh(at, now, snapshot_max_age_secs(interval))
                    })
                {
                    valid = false;
                    continue;
                }
                let size = snapshot.position.size;
                if size.is_zero() {
                    continue;
                }
                let mark = snapshot.ticker.mark_price;
                if mark <= Decimal::ZERO
                    || !is_timestamp_fresh(snapshot.ticker.mark_update_time, now, 15)
                {
                    valid = false;
                    continue;
                }
                let amount = size.abs() * mark;
                if size > Decimal::ZERO {
                    long += amount;
                } else {
                    short += amount;
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
        })
        .collect()
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
        let states: Vec<_> = handle
            .states
            .read()
            .await
            .iter()
            .filter(|state| !state.is_retired())
            .cloned()
            .collect();
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
    let states: Vec<_> = handle
        .states
        .read()
        .await
        .iter()
        .filter(|state| !state.is_retired())
        .cloned()
        .collect();
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
        .run_blocking(|db| db.cleanup_hourly_account_snapshots(Utc::now().timestamp()))
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
        let deadline = DateTime::from_timestamp(next + 60, 0).unwrap();
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::AppConfig, db::Database, server::AppState};
    use rust_decimal_macros::dec;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    #[test]
    fn hourly_capture_rejects_observations_outside_first_minute() {
        let hour = 1_800_000_000;
        assert!(!within_capture_window(
            hour,
            DateTime::from_timestamp(hour - 1, 0).unwrap()
        ));
        assert!(within_capture_window(
            hour,
            DateTime::from_timestamp(hour, 0).unwrap()
        ));
        assert!(within_capture_window(
            hour,
            DateTime::from_timestamp(hour + 59, 999_999_999).unwrap()
        ));
        assert!(!within_capture_window(
            hour,
            DateTime::from_timestamp(hour + 60, 0).unwrap()
        ));
    }

    fn aggregate(snapshots: &[BotSnapshot], now: DateTime<Utc>) -> Vec<SnapshotGroup> {
        super::aggregate(
            &snapshots
                .iter()
                .cloned()
                .map(|snapshot| (snapshot, 3, None))
                .collect::<Vec<_>>(),
            now,
        )
    }

    async fn sample(symbol: &str, paper: bool) -> BotSnapshot {
        let mut config = AppConfig::default();
        config.exchange.symbol = symbol.into();
        if !paper {
            config.exchange.api_key = "live-key".into();
            config.exchange.api_secret = "live-secret".into();
        }
        let (tx, _rx) = mpsc::channel(1);
        let state = AppState::new(config, Arc::new(Database::open(":memory:").unwrap()), tx);
        state.ticker.write().await.mark_price = dec!(10);
        state.ticker.write().await.mark_update_time = Utc::now();
        if !paper {
            state.position.write().await.update_time = Some(Utc::now());
        }
        let mut snapshot = state.snapshot().await;
        snapshot.account.asset = quote_asset(symbol).unwrap_or(symbol).into();
        snapshot.account.total_wallet_balance = dec!(100);
        snapshot.account.margin_balance = dec!(110);
        snapshot.account.update_time = Utc::now();
        snapshot
    }

    #[tokio::test]
    async fn groups_separate_modes_and_deduplicate_shared_account_equity() {
        let mut paper1 = sample("BTCUSDC", true).await;
        paper1.position.size = dec!(2);
        paper1.position.unrealized_pnl = dec!(5);
        paper1.account.margin_balance = dec!(999);
        paper1.account.update_time -= chrono::Duration::seconds(1);
        let mut paper2 = sample("ETHUSDC", true).await;
        paper2.position.size = dec!(-3);
        let mut live1 = sample("BTCUSDC", false).await;
        let mut live2 = sample("ETHUSDC", false).await;
        live1.account.margin_balance = dec!(500);
        live2.account.margin_balance = dec!(600);
        live1.account.update_time -= chrono::Duration::seconds(1);
        let groups = aggregate(&[paper1, paper2, live1, live2], Utc::now());
        let paper = groups
            .iter()
            .find(|g| g.mode == TradingMode::Paper)
            .unwrap();
        assert_eq!(paper.equity, Some(dec!(110)));
        assert_eq!(paper.long, Some(dec!(20)));
        assert_eq!(paper.short, Some(dec!(30)));
        let live = groups.iter().find(|g| g.mode == TradingMode::Live).unwrap();
        assert_eq!(live.equity, Some(dec!(600)));
    }

    #[tokio::test]
    async fn adding_paper_symbols_preserves_capital_and_covers_all_positions() {
        let mut first = sample("BTCUSDC", true).await;
        first.account.total_wallet_balance = dec!(10020);
        first.account.margin_balance = dec!(10035);
        first.position.size = dec!(2);
        first.position.unrealized_pnl = dec!(10);
        let mut second = sample("ETHUSDC", true).await;
        second.account = first.account.clone();
        second.position.size = dec!(-3);
        second.position.unrealized_pnl = dec!(5);
        let now = Utc::now();
        let initial = aggregate(&[first.clone()], now).remove(0);
        let shared = aggregate(&[first, second], now).remove(0);
        assert_eq!(initial.equity, Some(dec!(10035)));
        assert_eq!(shared.equity, initial.equity);
        assert_eq!(
            (shared.long, shared.short),
            (Some(dec!(20)), Some(dec!(30)))
        );
    }

    #[tokio::test]
    async fn flat_paper_ledgers_use_the_latest_shared_balance_without_expiry() {
        let mut old = sample("BTCUSDC", true).await;
        let mut latest = sample("ETHUSDC", true).await;
        let now = Utc::now();
        old.account.update_time = now - chrono::Duration::minutes(2);
        latest.account.update_time = now - chrono::Duration::minutes(1);
        latest.account.margin_balance = dec!(10020);
        assert_eq!(aggregate(&[old, latest], now)[0].equity, Some(dec!(10020)));
    }

    #[tokio::test]
    async fn fresh_paper_symbols_do_not_mask_stale_open_valuations_or_marks() {
        let mut stale = sample("BTCUSDC", true).await;
        let mut current = sample("ETHUSDC", true).await;
        let now = Utc::now();
        stale.position.size = dec!(2);
        stale.account.update_time = now - chrono::Duration::seconds(31);
        current.account.update_time = now;
        let group = aggregate(&[stale.clone(), current.clone()], now).remove(0);
        assert_eq!(group.equity, None);
        assert_eq!((group.long, group.short), (Some(dec!(20)), Some(dec!(0))));
        stale.account.update_time = now;
        stale.ticker.mark_update_time = now - chrono::Duration::seconds(15);
        let group = aggregate(&[stale.clone(), current.clone()], now).remove(0);
        assert_eq!((group.equity, group.long, group.short), (None, None, None));
        stale.ticker.mark_update_time = now;
        assert_eq!(aggregate(&[stale, current], now)[0].equity, Some(dec!(110)));
    }

    #[tokio::test]
    async fn missing_paper_account_does_not_display_a_partial_shared_balance() {
        let mut missing = sample("BTCUSDC", true).await;
        let current = sample("ETHUSDC", true).await;
        missing.account = Default::default();
        assert_eq!(aggregate(&[missing, current], Utc::now())[0].equity, None);
    }

    #[tokio::test]
    async fn distinct_live_identities_never_share_equity_or_positions() {
        let mut first = sample("BTCUSDC", false).await;
        let mut second = sample("ETHUSDC", false).await;
        first.account.margin_balance = dec!(100);
        second.account.margin_balance = dec!(200);
        first.position.size = dec!(1);
        second.position.size = dec!(-2);
        let groups = super::aggregate(
            &[
                (first, 3, Some("first".into())),
                (second, 3, Some("second".into())),
            ],
            Utc::now(),
        );
        assert_eq!(groups.len(), 2);
        let first = groups
            .iter()
            .find(|group| group.account_id.as_deref() == Some("first"))
            .unwrap();
        let second = groups
            .iter()
            .find(|group| group.account_id.as_deref() == Some("second"))
            .unwrap();
        assert_eq!(
            (first.equity, first.long, first.short),
            (Some(dec!(100)), Some(dec!(10)), Some(dec!(0)))
        );
        assert_eq!(
            (second.equity, second.long, second.short),
            (Some(dec!(200)), Some(dec!(0)), Some(dec!(20)))
        );
    }

    #[test]
    fn legacy_live_groups_have_no_identity() {
        let group: SnapshotGroup = serde_json::from_str(
            r#"{"mode":"LIVE","asset":"USDC","equity":"100","long":null,"short":null}"#,
        )
        .unwrap();
        assert_eq!(group.mode, TradingMode::Live);
        assert_eq!(group.account_id, None);
    }

    #[tokio::test]
    async fn missing_prices_and_stale_live_accounts_remain_unknown() {
        let mut live = sample("BTCUSDC", false).await;
        live.account.update_time = Utc::now() - chrono::Duration::minutes(1);
        live.position.size = dec!(1);
        live.position.mark_price = Decimal::ZERO;
        live.ticker.mark_price = Decimal::ZERO;
        let group = aggregate(&[live], Utc::now()).remove(0);
        assert_eq!((group.equity, group.long, group.short), (None, None, None));
    }

    #[tokio::test]
    async fn live_account_uses_recognized_non_builtin_asset() {
        let mut live = sample("BTCXYZ", false).await;
        live.account.asset = "XYZ".into();
        let group = aggregate(&[live], Utc::now()).remove(0);
        assert_eq!(group.asset, "XYZ");
        assert_eq!(group.equity, Some(dec!(110)));
    }

    #[tokio::test]
    async fn live_freshness_accounts_for_sync_interval() {
        let now = Utc::now();
        let mut live = sample("BTCUSDC", false).await;
        live.account.update_time = now - chrono::Duration::seconds(55);
        live.position.update_time = Some(now - chrono::Duration::seconds(55));
        let group = super::aggregate(&[(live.clone(), 60, None)], now).remove(0);
        assert_eq!(
            (group.equity, group.long, group.short),
            (Some(dec!(110)), Some(dec!(0)), Some(dec!(0)))
        );
        let group = super::aggregate(&[(live, 3, None)], now).remove(0);
        assert_eq!((group.equity, group.long, group.short), (None, None, None));
    }

    #[tokio::test]
    async fn unconfirmed_and_stale_live_positions_do_not_appear_flat() {
        let mut live = sample("BTCUSDC", false).await;
        for update in [None, Some(Utc::now() - chrono::Duration::seconds(31))] {
            live.position.update_time = update;
            let group = aggregate(&[live.clone()], Utc::now()).remove(0);
            assert_eq!((group.long, group.short), (None, None));
        }
        live.position.update_time = Some(Utc::now());
        let group = aggregate(&[live], Utc::now()).remove(0);
        assert_eq!((group.long, group.short), (Some(dec!(0)), Some(dec!(0))));
    }

    #[tokio::test]
    async fn stale_mark_prices_do_not_produce_paper_equity_or_position_values() {
        let mut paper = sample("BTCUSDC", true).await;
        paper.position.size = dec!(2);
        paper.position.mark_price = dec!(10);
        paper.ticker.mark_update_time = Utc::now() - chrono::Duration::seconds(16);
        let group = aggregate(&[paper], Utc::now()).remove(0);
        assert_eq!((group.equity, group.long, group.short), (None, None, None));
    }

    #[tokio::test]
    async fn paper_positions_and_equity_are_separated_by_quote_asset() {
        let mut usdc = sample("BTCUSDC", true).await;
        let mut fdusd = sample("ETHFDUSD", true).await;
        usdc.position.size = dec!(2);
        fdusd.position.size = dec!(-3);
        let groups = aggregate(&[usdc, fdusd], Utc::now());
        let usdc = groups.iter().find(|g| g.asset == "USDC").unwrap();
        let fdusd = groups.iter().find(|g| g.asset == "FDUSD").unwrap();
        assert_eq!(
            (usdc.equity, usdc.long, usdc.short),
            (Some(dec!(110)), Some(dec!(20)), Some(dec!(0)))
        );
        assert_eq!(
            (fdusd.equity, fdusd.long, fdusd.short),
            (Some(dec!(110)), Some(dec!(0)), Some(dec!(30)))
        );
    }

    #[tokio::test]
    async fn paper_account_uses_symbol_quote_asset_at_startup() {
        let mut config = AppConfig::default();
        config.exchange.symbol = "ETHFDUSD".into();
        let (tx, _rx) = mpsc::channel(1);
        let state = AppState::new(config, Arc::new(Database::open(":memory:").unwrap()), tx);
        assert_eq!(state.account.read().await.asset, "FDUSD");
    }

    #[tokio::test]
    async fn mismatched_paper_account_asset_is_not_mislabeled_as_equity() {
        let mut fdusd = sample("ETHFDUSD", true).await;
        fdusd.account.asset = "USDC".into();
        let group = aggregate(&[fdusd], Utc::now()).remove(0);
        assert_eq!(group.asset, "FDUSD");
        assert_eq!(group.equity, None);
    }

    #[test]
    fn paper_history_retains_legacy_values_under_a_separate_identity() {
        let db = Database::open(":memory:").unwrap();
        let legacy: SnapshotGroup = serde_json::from_str(
            r#"{"mode":"PAPER","asset":"USDC","equity":"20000","long":"20","short":"30"}"#,
        )
        .unwrap();
        assert_eq!(legacy.account_id, None);
        let mut shared = legacy.clone();
        shared.account_id = Some(PAPER_SHARED_ACCOUNT_ID.to_owned());
        shared.equity = Some(dec!(10000));
        db.save_hourly_account_snapshot(3600, &[legacy]).unwrap();
        db.save_hourly_account_snapshot(7200, &[shared]).unwrap();
        let saved = db.hourly_account_snapshots(0).unwrap();
        assert_eq!(saved.len(), 2);
        assert_eq!(saved[0].groups[0].account_id, None);
        assert_eq!(saved[0].groups[0].equity, Some(dec!(20000)));
        assert_eq!(
            saved[1].groups[0].account_id.as_deref(),
            Some(PAPER_SHARED_ACCOUNT_ID)
        );
        assert_eq!(saved[1].groups[0].equity, Some(dec!(10000)));
    }

    #[test]
    fn snapshot_storage_is_idempotent_and_retains_only_30_days() {
        let db = Database::open(":memory:").unwrap();
        let now = 100 * 24 * 3600;
        let group = SnapshotGroup {
            mode: TradingMode::Paper,
            asset: "USDC".into(),
            account_id: None,
            equity: Some(dec!(100)),
            long: Some(dec!(0)),
            short: Some(dec!(0)),
        };
        db.save_hourly_account_snapshot(now - 31 * 24 * 3600, std::slice::from_ref(&group))
            .unwrap();
        db.save_hourly_account_snapshot(now, std::slice::from_ref(&group))
            .unwrap();
        let mut changed = group.clone();
        changed.equity = Some(dec!(200));
        db.save_hourly_account_snapshot(now, &[changed]).unwrap();
        let saved = db.hourly_account_snapshots(0).unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].groups[0].equity, Some(dec!(100)));
        db.cleanup_hourly_account_snapshots(now + 31 * 24 * 3600)
            .unwrap();
        assert!(db.hourly_account_snapshots(0).unwrap().is_empty());
    }
}
