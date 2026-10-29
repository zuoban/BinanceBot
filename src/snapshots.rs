use crate::runtime::RuntimeHandle;
use crate::types::{quote_asset, BotSnapshot, TradingMode};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotGroup {
    pub mode: TradingMode,
    pub asset: String,
    pub equity: Option<Decimal>,
    pub long: Option<Decimal>,
    pub short: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HourlySnapshot {
    pub hour: i64,
    pub groups: Vec<SnapshotGroup>,
}

fn fresh(now: DateTime<Utc>, updated: DateTime<Utc>, seconds: u64) -> bool {
    now.signed_duration_since(updated)
        .to_std()
        .is_ok_and(|age| age < Duration::from_secs(seconds))
}

fn aggregate(snapshots: &[(BotSnapshot, u64)], now: DateTime<Utc>) -> Vec<SnapshotGroup> {
    let mut groups: BTreeMap<(String, String), Vec<(&BotSnapshot, u64)>> = BTreeMap::new();
    for (snapshot, sync_interval) in snapshots {
        let mode = TradingMode::from_exchange(snapshot.dry_run, snapshot.grid_config.is_testnet);
        let asset = quote_asset(&snapshot.symbol).unwrap_or_else(|| {
            if snapshot.account.asset.is_empty() {
                &snapshot.symbol
            } else {
                &snapshot.account.asset
            }
        });
        groups
            .entry((mode.as_str().to_owned(), asset.to_owned()))
            .or_default()
            .push((snapshot, *sync_interval));
    }
    groups
        .into_iter()
        .map(|((mode, asset), snapshots)| {
            let mode = TradingMode::from_db(&mode);
            let latest = snapshots
                .iter()
                .filter(|(s, interval)| {
                    s.account.asset == asset
                        && fresh(
                            now,
                            s.account.update_time,
                            interval.saturating_add(15).max(30),
                        )
                })
                .max_by_key(|(s, _)| s.account.update_time)
                .copied();
            let equity = if mode == TradingMode::Paper {
                if snapshots.iter().all(|(s, _)| {
                    s.account.asset == asset
                        && (s.position.size.is_zero()
                            || (s.ticker.mark_price > Decimal::ZERO
                                && fresh(now, s.ticker.mark_update_time, 15)))
                }) {
                    Some(
                        snapshots
                            .iter()
                            .map(|(s, _)| {
                                s.account.total_wallet_balance + s.position.unrealized_pnl
                            })
                            .sum(),
                    )
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
                        .is_some_and(|at| fresh(now, at, interval.saturating_add(15).max(30)))
                {
                    valid = false;
                    continue;
                }
                let size = snapshot.position.size;
                if size.is_zero() {
                    continue;
                }
                let mark = snapshot.ticker.mark_price;
                if mark <= Decimal::ZERO || !fresh(now, snapshot.ticker.mark_update_time, 15) {
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
                equity,
                long: valid.then_some(long),
                short: valid.then_some(short),
            }
        })
        .collect()
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
        tokio::time::sleep(Duration::from_secs((next - now) as u64)).await;
        let now = Utc::now();
        // Do not assign current values to an earlier hour after a long suspension.
        if now.timestamp() - next >= 60 {
            continue;
        }
        let states = handle.states.read().await.clone();
        let snapshots = futures_util::future::join_all(states.iter().map(|state| async {
            let snapshot = state.snapshot().await;
            let interval = state.config.read().await.exchange.sync_interval_secs;
            (snapshot, interval)
        }))
        .await;
        let groups = aggregate(&snapshots, Utc::now());
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

    fn aggregate(snapshots: &[BotSnapshot], now: DateTime<Utc>) -> Vec<SnapshotGroup> {
        super::aggregate(
            &snapshots
                .iter()
                .cloned()
                .map(|snapshot| (snapshot, 3))
                .collect::<Vec<_>>(),
            now,
        )
    }

    async fn sample(symbol: &str, paper: bool) -> BotSnapshot {
        let mut config = AppConfig::default();
        config.exchange.symbol = symbol.into();
        config.exchange.dry_run = paper;
        let (tx, _rx) = mpsc::channel(1);
        let state = AppState::new(config, Arc::new(Database::open(":memory:").unwrap()), tx);
        state.account.write().await.asset = quote_asset(symbol).unwrap_or(symbol).into();
        state.account.write().await.total_wallet_balance = dec!(100);
        state.account.write().await.margin_balance = dec!(110);
        state.account.write().await.update_time = Utc::now();
        state.ticker.write().await.mark_price = dec!(10);
        state.ticker.write().await.mark_update_time = Utc::now();
        if !paper {
            state.position.write().await.update_time = Some(Utc::now());
        }
        state.snapshot().await
    }

    #[tokio::test]
    async fn groups_separate_modes_sum_paper_and_deduplicate_live_equity() {
        let mut paper1 = sample("BTCUSDC", true).await;
        paper1.position.size = dec!(2);
        paper1.position.unrealized_pnl = dec!(5);
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
        assert_eq!(paper.equity, Some(dec!(205)));
        assert_eq!(paper.long, Some(dec!(20)));
        assert_eq!(paper.short, Some(dec!(30)));
        let live = groups.iter().find(|g| g.mode == TradingMode::Live).unwrap();
        assert_eq!(live.equity, Some(dec!(600)));
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
        let group = super::aggregate(&[(live.clone(), 60)], now).remove(0);
        assert_eq!(
            (group.equity, group.long, group.short),
            (Some(dec!(110)), Some(dec!(0)), Some(dec!(0)))
        );
        let group = super::aggregate(&[(live, 3)], now).remove(0);
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
            (Some(dec!(100)), Some(dec!(20)), Some(dec!(0)))
        );
        assert_eq!(
            (fdusd.equity, fdusd.long, fdusd.short),
            (Some(dec!(100)), Some(dec!(0)), Some(dec!(30)))
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
    fn snapshot_storage_is_idempotent_and_retains_only_30_days() {
        let db = Database::open(":memory:").unwrap();
        let now = 100 * 24 * 3600;
        let group = SnapshotGroup {
            mode: TradingMode::Paper,
            asset: "USDC".into(),
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
