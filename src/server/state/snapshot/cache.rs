use crate::config::AppConfig;
use crate::db::Database;
use crate::types::{AccountInfo, BotStatus, HourlyTradeStats, TradeRecord, TradingMode};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::warn;

pub(super) const SNAPSHOT_INTERVAL: Duration = Duration::from_millis(800);
pub(crate) const SNAPSHOT_IDLE_HEARTBEAT: Duration = Duration::from_secs(3);
const RESYNC_FRAME_TTL: Duration = Duration::from_millis(250);
const TRADE_CACHE_TTL: Duration = Duration::from_secs(5);
const QUERY_RETRY_DELAY: Duration = Duration::from_secs(5);

#[derive(PartialEq, Serialize)]
pub(crate) struct AccountSnapshot {
    pub(super) mode: TradingMode,
    pub(super) snapshot: AccountSnapshotData,
}

#[derive(PartialEq, Serialize)]
pub(super) struct AccountSnapshotData {
    pub(super) symbol: String,
    pub(super) status: BotStatus,
    pub(super) account_scope: String,
    pub(super) sync_interval_secs: u64,
    pub(super) ticker: AccountTicker,
    pub(super) position: AccountPosition,
    pub(super) account: AccountInfo,
    pub(super) account_cny_rate: Option<crate::fx::CnyRate>,
}

#[derive(PartialEq, Eq, Serialize)]
pub(super) struct AccountTicker {
    pub(super) mark_price: Decimal,
    pub(super) mark_update_time: DateTime<Utc>,
}

#[derive(PartialEq, Eq, Serialize)]
pub(super) struct AccountPosition {
    pub(super) size: Decimal,
    pub(super) unrealized_pnl: Decimal,
}

#[derive(Serialize)]
pub(crate) struct AccountFrame {
    pub generated_at: DateTime<Utc>,
    pub pairs: Vec<AccountSnapshot>,
}

#[derive(Serialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
enum AccountMessageRef<'a> {
    Accounts(&'a AccountFrame),
}

pub(crate) fn encode_accounts_frame(frame: &AccountFrame) -> serde_json::Result<String> {
    serde_json::to_string(&AccountMessageRef::Accounts(frame))
}

#[derive(Clone, PartialEq, Eq)]
pub(super) struct SnapshotScope {
    pub(super) symbol: String,
    pub(super) mode: TradingMode,
}

impl SnapshotScope {
    pub(super) fn from_config(config: &AppConfig) -> Self {
        Self {
            symbol: config.exchange.symbol.clone(),
            mode: config.exchange.trading_mode(),
        }
    }
}

#[derive(PartialEq, Eq)]
struct TradeKey {
    symbol: String,
    mode: TradingMode,
    hour: i64,
    revision: u64,
}

struct CachedQuery<K, V> {
    key: K,
    value: Option<V>,
    queried_at: DateTime<Utc>,
    loaded_at: Instant,
}

impl<K: PartialEq, V> CachedQuery<K, V> {
    fn reusable(&self, key: &K, now: DateTime<Utc>, ttl: Duration) -> bool {
        let ttl = if self.value.is_some() {
            ttl
        } else {
            QUERY_RETRY_DELAY
        };
        self.key == *key
            && self.loaded_at.elapsed() < ttl
            && now
                .signed_duration_since(self.queried_at)
                .to_std()
                .is_ok_and(|age| age < ttl)
    }

    fn new(key: K, value: Option<V>, queried_at: DateTime<Utc>) -> Self {
        Self {
            key,
            value,
            queried_at,
            loaded_at: Instant::now(),
        }
    }
}

#[derive(Default)]
pub(in crate::server::state) struct SnapshotCache {
    trades: Mutex<Option<CachedQuery<TradeKey, HourlyTradeStats>>>,
}

pub(in crate::server::state) struct ResyncFrame {
    generated: Instant,
    frame: Arc<str>,
}

impl ResyncFrame {
    pub(super) fn new(frame: Arc<str>) -> Self {
        Self {
            generated: Instant::now(),
            frame,
        }
    }

    pub(super) fn fresh(&self) -> Option<Arc<str>> {
        (self.generated.elapsed() < RESYNC_FRAME_TTL).then(|| Arc::clone(&self.frame))
    }
}

pub(in crate::server::state) struct LastSnapshotBroadcast {
    pub(super) sent_at: Instant,
    pub(super) receivers: usize,
    pub(super) snapshot: crate::types::BotSnapshot,
}

pub(super) fn bot_snapshot_content_unchanged(
    previous: &crate::types::BotSnapshot,
    current: &crate::types::BotSnapshot,
) -> bool {
    previous.status == current.status
        && previous.mode == current.mode
        && previous.account_scope == current.account_scope
        && previous.symbol == current.symbol
        && previous.sync_interval_secs == current.sync_interval_secs
        && previous.ticker == current.ticker
        && previous.position == current.position
        && previous.account == current.account
        && previous.account_cny_rate == current.account_cny_rate
        && previous.grid_config == current.grid_config
        && previous.active_orders == current.active_orders
        && previous.recent_trades == current.recent_trades
        && previous.hourly_trade_stats == current.hourly_trade_stats
        && previous.recent_logs == current.recent_logs
        && previous.stats.total_trades == current.stats.total_trades
        && previous.stats.completed_cycles == current.stats.completed_cycles
        && previous.stats.total_realized_profit == current.stats.total_realized_profit
        && previous.stats.total_realized_pnl == current.stats.total_realized_pnl
        && previous.stats.total_commission == current.stats.total_commission
        && previous.stats.non_quote_commission_trades == current.stats.non_quote_commission_trades
        && previous.stats.pending_pnl_trades == current.stats.pending_pnl_trades
        && previous.stats.total_volume_usdc == current.stats.total_volume_usdc
        && previous.stats.start_time == current.stats.start_time
        && previous.stats.active_buy_orders == current.stats.active_buy_orders
        && previous.stats.active_sell_orders == current.stats.active_sell_orders
}

#[cfg(test)]
fn split_volatile_field<'a>(s: &'a str, needle: &str) -> Option<(&'a str, &'a str)> {
    let pos = s.find(needle)?;
    let before = &s[..pos];
    let rest = &s[pos + needle.len()..];
    if let Some(inner) = rest.strip_prefix('"') {
        let end = inner.find('"')?;
        Some((before, &inner[end + 1..]))
    } else {
        let digits = rest.bytes().take_while(|b| b.is_ascii_digit()).count();
        (digits > 0).then_some((before, &rest[digits..]))
    }
}

#[cfg(test)]
pub(crate) fn snapshot_frame_unchanged(previous: &str, current: &str) -> bool {
    fn strip_volatile(frame: &str) -> (&str, &str, &str) {
        let (head, after_gen) =
            split_volatile_field(frame, "\"generated_at\":").unwrap_or(("", frame));
        let (mid, tail) =
            split_volatile_field(after_gen, "\"uptime_secs\":").unwrap_or((after_gen, ""));
        (head, mid, tail)
    }
    strip_volatile(previous) == strip_volatile(current)
}

pub(crate) fn sort_and_truncate_recent_trades(trades: &mut Vec<TradeRecord>, limit: usize) {
    trades.sort_unstable_by(|a, b| {
        b.timestamp
            .cmp(&a.timestamp)
            .then_with(|| b.trade_id.cmp(&a.trade_id))
    });
    trades.truncate(limit);
}

pub(super) struct SnapshotCharts {
    scope: SnapshotScope,
    revision: u64,
    stable: bool,
    pub(super) trades: Option<HourlyTradeStats>,
}

impl SnapshotCharts {
    pub(super) fn is_current(&self, config: &AppConfig, db: &Database) -> bool {
        self.stable
            && self.scope == SnapshotScope::from_config(config)
            && self.revision == db.symbol_trade_revision(&self.scope.symbol)
    }
}

impl SnapshotCache {
    pub(super) async fn get(
        &self,
        db: &Arc<Database>,
        scope: SnapshotScope,
        now: DateTime<Utc>,
    ) -> SnapshotCharts {
        // Serialize only dashboard readers; trading never takes this mutex.
        let mut cache = self.trades.lock().await;
        let revision = db.symbol_trade_revision(&scope.symbol);
        let key = TradeKey {
            symbol: scope.symbol.clone(),
            mode: scope.mode,
            hour: now.timestamp().div_euclid(3600),
            revision,
        };
        if let Some(entry) = cache
            .as_ref()
            .filter(|entry| entry.reusable(&key, now, TRADE_CACHE_TTL))
        {
            return SnapshotCharts {
                scope,
                revision,
                stable: true,
                trades: entry.value.clone(),
            };
        }
        let commit_token = db.trade_commit_stable_token();
        let query_scope = scope.clone();
        let value = db
            .run_blocking(move |db| {
                db.get_hourly_trade_stats(&query_scope.symbol, query_scope.mode, now)
            })
            .await
            .map_err(|error| warn!("Failed to load hourly trade counts: {}", error))
            .ok();
        let stable = commit_token.is_some()
            && db.trade_commit_stable_token() == commit_token
            && db.symbol_trade_revision(&scope.symbol) == revision;
        if stable {
            *cache = Some(CachedQuery::new(key, value.clone(), now));
        }
        SnapshotCharts {
            scope,
            revision,
            stable,
            trades: value,
        }
    }
}
