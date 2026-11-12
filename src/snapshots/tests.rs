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
    assert!(!within_capture_window(i64::MIN, Utc::now()));
    assert!(!within_capture_window(i64::MAX, Utc::now()));
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
    snapshot.generated_at = Utc::now();
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
async fn extreme_position_notional_overflow_marks_group_invalid_without_panicking() {
    let now = Utc::now();
    let mut first = sample("BTCUSDC", true).await;
    first.account.update_time = now;
    first.ticker.mark_price = Decimal::MAX;
    first.ticker.mark_update_time = now;
    first.position.size = dec!(2);
    let group = aggregate(&[first], now).remove(0);
    assert_eq!((group.long, group.short), (None, None));

    let mut a = sample("BTCUSDC", true).await;
    let mut b = sample("ETHUSDC", true).await;
    for s in [&mut a, &mut b] {
        s.account.update_time = now;
        s.ticker.mark_price = Decimal::MAX;
        s.ticker.mark_update_time = now;
        s.position.size = Decimal::ONE;
    }
    let sum_overflow = aggregate(&[a, b], now).remove(0);
    assert_eq!((sum_overflow.long, sum_overflow.short), (None, None));
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

#[test]
fn detects_beijing_midnight_hour_boundaries() {
    let midnight_utc = DateTime::parse_from_rfc3339("2026-11-09T16:00:00Z")
        .unwrap()
        .timestamp();
    assert!(is_beijing_midnight_hour(midnight_utc));
    assert!(!is_beijing_midnight_hour(midnight_utc + 3600));
    assert!(!is_beijing_midnight_hour(midnight_utc - 3600));
    assert!(is_beijing_midnight_hour(midnight_utc + 24 * 3600));
    let _ = is_beijing_midnight_hour(i64::MIN);
    let _ = is_beijing_midnight_hour(i64::MAX);
}

#[tokio::test]
async fn builds_and_persists_daily_snapshot_with_previous_day_trades_and_positions() {
    let db = Database::open(":memory:").unwrap();
    let window_end = DateTime::parse_from_rfc3339("2026-11-09T16:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let window_start = window_end - chrono::Duration::days(1);
    let now = window_end + chrono::Duration::seconds(5);

    let make_trade = |id: &str,
                      symbol: &str,
                      side: crate::types::OrderSide,
                      pnl: Decimal,
                      verified: bool,
                      at: DateTime<Utc>| {
        crate::types::TradeRecord {
            trade_id: id.into(),
            client_order_id: id.into(),
            symbol: symbol.into(),
            mode: TradingMode::Paper,
            side,
            price: dec!(100),
            quantity: dec!(1),
            amount_usdc: dec!(100),
            realized_pnl: pnl,
            commission: dec!(0.02),
            commission_asset: None,
            pnl_verified: verified,
            is_maker: true,
            timestamp: at,
            note: String::new(),
        }
    };

    // Before previous day window: excluded
    db.insert_trade(&make_trade(
        "before",
        "SOLUSDC",
        crate::types::OrderSide::Buy,
        Decimal::ZERO,
        true,
        window_start - chrono::Duration::seconds(1),
    ))
    .unwrap();
    // Inside previous day window: included
    db.insert_trade(&make_trade(
        "buy-1",
        "SOLUSDC",
        crate::types::OrderSide::Buy,
        Decimal::ZERO,
        true,
        window_start + chrono::Duration::hours(2),
    ))
    .unwrap();
    db.insert_trade(&make_trade(
        "sell-1",
        "SOLUSDC",
        crate::types::OrderSide::Sell,
        dec!(12.3456),
        true,
        window_start + chrono::Duration::hours(5),
    ))
    .unwrap();
    db.insert_trade(&make_trade(
        "sell-pending",
        "SOLUSDC",
        crate::types::OrderSide::Sell,
        Decimal::ZERO,
        false,
        window_end - chrono::Duration::seconds(10),
    ))
    .unwrap();
    // At or after midnight: excluded from previous day
    db.insert_trade(&make_trade(
        "after",
        "SOLUSDC",
        crate::types::OrderSide::Buy,
        Decimal::ZERO,
        true,
        window_end,
    ))
    .unwrap();

    let trades = db.daily_trade_summaries(window_start, window_end).unwrap();
    assert_eq!(trades.len(), 1);
    assert_eq!(trades[0].symbol, "SOLUSDC");
    assert_eq!(trades[0].buy_count, 1);
    assert_eq!(trades[0].sell_count, 2);
    assert_eq!(trades[0].realized_pnl, dec!(12.3456));
    assert_eq!(trades[0].pending_pnl_count, 1);

    let mut sol = sample("SOLUSDC", true).await;
    sol.account.update_time = now;
    sol.account.total_wallet_balance = dec!(10012.34);
    sol.account.margin_balance = dec!(10032.34);
    sol.account.available_balance = dec!(9500.00);
    sol.account.unrealized_profit = dec!(20.00);
    sol.ticker.mark_price = dec!(150);
    sol.ticker.mark_update_time = now;
    sol.position.size = dec!(2);
    sol.position.entry_price = dec!(140);
    sol.position.unrealized_pnl = dec!(20);

    let snapshot = build_daily_snapshot(
        "2026-11-09".into(),
        window_start,
        window_end,
        now,
        &[(sol, 3, Some(PAPER_SHARED_ACCOUNT_ID.into()))],
        trades,
    );
    assert_eq!(snapshot.date, "2026-11-09");
    assert_eq!(snapshot.accounts.len(), 1);
    assert_eq!(snapshot.accounts[0].equity, Some(dec!(10032.34)));
    assert_eq!(snapshot.accounts[0].available, Some(dec!(9500.00)));
    assert_eq!(snapshot.positions.len(), 1);
    assert_eq!(snapshot.positions[0].total, Some(dec!(300)));
    assert_eq!(snapshot.positions[0].long, Some(dec!(300)));
    assert_eq!(snapshot.positions[0].short, Some(dec!(0)));
    assert_eq!(snapshot.pair_positions.len(), 1);
    assert_eq!(snapshot.pair_positions[0].position_amount, Some(dec!(300)));

    assert!(db.save_daily_snapshot(&snapshot).unwrap());
    assert!(!db.save_daily_snapshot(&snapshot).unwrap());
    let loaded = db.daily_snapshots(10).unwrap();
    assert_eq!(loaded, vec![snapshot]);
}

#[tokio::test]
async fn try_capture_hour_retries_after_transient_config_update_lock_contention() {
    let db = Arc::new(Database::open(":memory:").unwrap());
    let mut config = AppConfig::default();
    config.pairs = vec![crate::config::TradingPairConfig::new("SOLUSDC".into())];
    db.save_config(&config).unwrap();
    db.save_pair_status(
        "SOLUSDC",
        TradingMode::Paper,
        crate::types::BotStatus::Paused,
    )
    .unwrap();
    let mut runtime = crate::runtime::TradingRuntime::start_with_test_exchange(
        &config,
        db.clone(),
        "http://127.0.0.1:1".into(),
    )
    .await
    .unwrap();
    let handle = runtime.handle();
    let state = handle.states.read().await[0].clone();
    let now_ts = Utc::now().timestamp();
    let hour = if is_beijing_midnight_hour(now_ts) {
        now_ts - 1
    } else {
        now_ts
    };

    let lock_guard = state.config_update_lock.lock().await;
    assert!(!try_capture_hour(&handle, hour).await);
    assert!(db.hourly_account_snapshots(0).unwrap().is_empty());
    drop(lock_guard);

    assert!(try_capture_hour(&handle, hour).await);
    let saved = db.hourly_account_snapshots(0).unwrap();
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].hour, hour);

    let _ = runtime.shutdown(Duration::from_secs(2)).await;
}
