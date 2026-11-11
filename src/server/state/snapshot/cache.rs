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
pub(super) const SNAPSHOT_IDLE_HEARTBEAT: Duration = Duration::from_secs(3);
const TRADE_CACHE_TTL: Duration = Duration::from_secs(5);
const QUERY_RETRY_DELAY: Duration = Duration::from_secs(5);

#[derive(Serialize)]
pub(crate) struct AccountSnapshot {
    pub(super) mode: TradingMode,
    pub(super) snapshot: AccountSnapshotData,
}

#[derive(Serialize)]
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

#[derive(Serialize)]
pub(super) struct AccountTicker {
    pub(super) mark_price: Decimal,
    pub(super) mark_update_time: DateTime<Utc>,
}

#[derive(Serialize)]
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
pub(crate) enum AccountMessage {
    Accounts(AccountFrame),
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

pub(in crate::server::state) struct LastSnapshotBroadcast {
    pub(super) sent_at: Instant,
    pub(super) receivers: usize,
    pub(super) frame: Arc<str>,
}

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

pub(super) fn snapshot_frame_unchanged(previous: &str, current: &str) -> bool {
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
    pub(super) trades: Option<HourlyTradeStats>,
}

impl SnapshotCharts {
    pub(super) fn is_current(&self, config: &AppConfig, db: &Database) -> bool {
        self.scope == SnapshotScope::from_config(config)
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
        if cache
            .as_ref()
            .is_none_or(|entry| !entry.reusable(&key, now, TRADE_CACHE_TTL))
        {
            let query_scope = scope.clone();
            let value = db
                .run_blocking(move |db| {
                    db.get_hourly_trade_stats(&query_scope.symbol, query_scope.mode, now)
                })
                .await
                .map_err(|error| warn!("Failed to load hourly trade counts: {}", error))
                .ok();
            *cache = Some(CachedQuery::new(key, value, now));
        }
        SnapshotCharts {
            scope,
            revision,
            trades: cache.as_ref().and_then(|entry| entry.value.clone()),
        }
    }
}
