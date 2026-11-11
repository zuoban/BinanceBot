import { formatDecimal, parseSignedDecimal, rescaleDecimal, type Fixed } from './decimal';
import { compactNumber, numeric, positivePrice } from './format';
import type { AccountObservation, Mode, Overview, PairSummary, Snapshot, Trade } from './types';

export const SNAPSHOT_STALE_MS = 10_000;
export const PAIRS_STALE_MS = 25_000;
export const quoteAsset = (symbol: string) => ['USDC', 'USDT', 'FDUSD', 'BUSD'].find(a => symbol.endsWith(a)) ?? '';

const modeAsset = (mode: Mode, symbol: string) => {
  const asset = quoteAsset(symbol) || symbol;
  return { asset, key: `${mode}:${asset}` };
};

function groupByKey<T, G>(items: Iterable<T>, getKey: (item: T) => string, init: (item: T, key: string) => G, accumulate: (group: G, item: T) => void): Map<string, G> {
  const groups = new Map<string, G>();
  for (const item of items) {
    const key = getKey(item);
    let group = groups.get(key);
    if (!group) {
      group = init(item, key);
      groups.set(key, group);
    }
    accumulate(group, item);
  }
  return groups;
}

type ValuedPosition = { symbol: string; position: Pick<Snapshot['position'], 'size' | 'mark_price'>; ticker: Pick<Snapshot['ticker'], 'mark_price'> };

export function positionAmount(s: ValuedPosition): number | null {
  const size = numeric(s.position.size), tickerMark = numeric(s.ticker.mark_price);
  const mark = tickerMark !== null && tickerMark > 0 ? tickerMark : numeric(s.position.mark_price);
  if (size === null || mark === null || mark <= 0) return null;
  const amount = Math.abs(size * mark);
  return Number.isFinite(amount) ? amount : null;
}

export function comparePositionAmount(a: ValuedPosition, b: ValuedPosition) {
  const left = positionAmount(a), right = positionAmount(b);
  return (right ?? -1) - (left ?? -1) || a.symbol.localeCompare(b.symbol);
}

export interface PositionGroup {
  key: string;
  mode: Mode;
  asset: string;
  count: number;
  long: number | null;
  short: number | null;
  total: number | null;
}

export function positionGroups<T extends { mode: Mode; snapshot: ValuedPosition }>(entries: T[]): PositionGroup[] {
  const groups = groupByKey(
    entries,
    ({ mode, snapshot }) => modeAsset(mode, snapshot.symbol).key,
    ({ mode, snapshot }) => ({ mode, asset: modeAsset(mode, snapshot.symbol).asset, snapshots: [] as ValuedPosition[] }),
    (group, { snapshot }) => { group.snapshots.push(snapshot); },
  );
  return [...groups.entries()].map(([key, group]) => {
    let long = 0, short = 0, complete = true;
    for (const s of group.snapshots) {
      const size = numeric(s.position.size);
      const amount = size === 0 ? 0 : positionAmount(s);
      if (size === null || amount === null) { complete = false; continue; }
      if (size > 0) long += amount;
      if (size < 0) short += amount;
    }
    return { key, mode: group.mode, asset: group.asset, count: group.snapshots.length, long: complete ? long : null, short: complete ? short : null, total: complete ? long + short : null };
  });
}

export function entryPriceText(position: { size: string; entry_price: string }, compact = false) {
  const size = numeric(position.size), entry = numeric(position.entry_price);
  return size !== null && size !== 0 && entry !== null && entry > 0 ? positivePrice(entry, compact) : '--';
}

export function entryChange(position: { size: string; entry_price: string }, markPrice: unknown): { text: string; className?: 'positive' | 'negative' } {
  const size = numeric(position.size), entry = numeric(position.entry_price), mark = numeric(markPrice);
  const raw = size !== null && size !== 0 && entry !== null && entry > 0 && mark !== null && mark > 0 ? (mark / entry - 1) * 100 : null;
  const formatted = raw !== null && Number.isFinite(raw) ? compactNumber(raw, 2) : null;
  const rounded = formatted === null ? null : Number(formatted.replaceAll(',', ''));
  if (rounded === null) return { text: '--' };
  if (rounded === 0) return { text: '0%' };
  return { text: `${rounded > 0 ? '+' : ''}${formatted}%`, className: rounded > 0 ? 'positive' : 'negative' };
}

const isSameBeijingDay = (windowEnd: string | undefined, now: number) => {
  if (!windowEnd) return false;
  const endTs = Date.parse(windowEnd);
  return Number.isFinite(endTs) && Math.floor((endTs + 8 * 3600_000) / 86_400_000) === Math.floor((now + 8 * 3600_000) / 86_400_000);
};

export function todayTradeCounts(s: Snapshot | PairSummary, now = Date.now()) {
  if ('today_trade_counts' in s) {
    const counts = s.today_trade_counts;
    return counts && isSameBeijingDay(counts.window_end, now) ? { buy: counts.buy, sell: counts.sell } : null;
  }
  const stats = s.hourly_trade_stats;
  if (!stats || !isSameBeijingDay(stats.window_end, now)) return null;
  return stats.buckets.reduce((counts, b) => ({ buy: counts.buy + b.buy_count, sell: counts.sell + b.sell_count }), { buy: 0, sell: 0 });
}

export function todayCount(s: Snapshot | PairSummary, now = Date.now()) {
  const counts = todayTradeCounts(s, now);
  return counts ? counts.buy + counts.sell : null;
}

export interface ModeAssetPnlGroup {
  key: string;
  mode: Mode;
  asset: string;
  sum: number;
  pending: number;
  available: boolean;
}

export function groupModeAssetPnl<T extends { mode: Mode; symbol: string }>(
  items: Iterable<T>,
  extract: (item: T) => { pnl: unknown; pending?: number; valid?: boolean },
): Map<string, ModeAssetPnlGroup> {
  const raw = groupByKey(
    items,
    item => modeAsset(item.mode, item.symbol).key,
    (item, key) => ({ key, mode: item.mode, asset: modeAsset(item.mode, item.symbol).asset, values: [] as Fixed[], pending: 0, available: true }),
    (group, item) => {
      const { pnl, pending = 0, valid = true } = extract(item);
      const parsed = parseSignedDecimal(pnl);
      if (!valid || !parsed || !Number.isInteger(pending) || pending < 0) {
        group.available = false;
        if (Number.isInteger(pending) && pending > 0) group.pending += pending;
        if (parsed) group.values.push(parsed);
      } else {
        group.values.push(parsed);
        group.pending += pending;
      }
    },
  );
  const result = new Map<string, ModeAssetPnlGroup>();
  for (const [key, group] of raw) {
    const scale = group.values.reduce((max, item) => Math.max(max, item.s), 0);
    const totalScaled = group.values.reduce((acc, item) => acc + rescaleDecimal(item, scale), 0n);
    const sum = totalScaled === 0n ? 0 : Number(formatDecimal(totalScaled, scale));
    result.set(key, { key, mode: group.mode, asset: group.asset, sum, pending: group.pending, available: group.available && Number.isFinite(sum) });
  }
  return result;
}

export function todayPnlGroups(stats: Overview['today_pnl'], now = Date.now()) {
  if (!stats || !isSameBeijingDay(stats.window_end, now)) return null;
  return [...groupModeAssetPnl(stats.groups, row => ({ pnl: row.realized_pnl, pending: row.pending_pnl_count })).values()]
    .map(group => ({ ...group, total: group.available && !group.pending ? group.sum : null }));
}

export function groupTradePnl(trades: Trade[]) {
  const groups = groupModeAssetPnl(trades, trade => ({ pnl: trade.realized_pnl, valid: trade.pnl_verified }));
  return new Map([...groups.entries()].map(([key, group]) => [key, { mode: group.mode, asset: group.asset, total: group.sum, available: group.available }]));
}

export function availableRatio(available: number | null, wallet: number | null): number | null {
  return available !== null && wallet !== null && wallet > 0 ? available / wallet * 100 : null;
}

export interface AccountGroup {
  key: string;
  mode: Mode;
  asset: string;
  count: number;
  equity: number | null;
  available: number | null;
  wallet: number | null;
  unrealized: number | null;
  ratio: number | null;
  cny: number | null;
}

const entryRefNow = (entry: Pick<AccountObservation, 'generated_at_ms' | 'received_at_ms'>, now: number) =>
  entry.generated_at_ms !== undefined && entry.received_at_ms !== undefined
    ? entry.generated_at_ms + Math.max(0, now - entry.received_at_ms)
    : now;

const liveAccountMaxAge = (syncIntervalSecs: number | undefined) =>
  Math.max(30_000, (typeof syncIntervalSecs === 'number' && Number.isFinite(syncIntervalSecs) ? syncIntervalSecs : 3) * 3_000);

const recentAt = (timestamp: string, maxAge: number, refNow: number) => {
  const age = refNow - Date.parse(timestamp);
  return age >= -5_000 && age < maxAge;
};

export function accountGroups<T extends AccountObservation>(entries: T[], now = Date.now()): AccountGroup[] {
  const groups = groupByKey(
    entries,
    ({ mode, snapshot: s }) => `${mode}:${s.account.asset || quoteAsset(s.symbol)}`,
    ({ mode, snapshot: s }) => ({ mode, asset: s.account.asset || quoteAsset(s.symbol), items: [] as T[], observed: [] as T[], observationStale: false }),
    (group, entry) => {
      group.items.push(entry);
      if (!entry.account_observation_stale) group.observed.push(entry);
      group.observationStale ||= !!entry.account_observation_stale || (entry.mode === 'PAPER' && !entry.snapshot.account.asset);
    },
  );
  return [...groups.values()].map(({ mode, asset, items, observed, observationStale }) => {
    let latest: T | undefined;
    let latestTs = -Infinity;
    for (const entry of observed) {
      const s = entry.snapshot;
      const ts = Date.parse(s.account.update_time);
      const maxAge = mode === 'PAPER' ? Infinity : liveAccountMaxAge(s.sync_interval_secs);
      const age = entryRefNow(entry, now) - ts;
      if (age >= -5_000 && age < maxAge && ts > latestTs) {
        latestTs = ts;
        latest = entry;
      }
    }
    const chosen = latest ?? items[0];
    const account = chosen.snapshot;
    const unavailable = !latest || (mode === 'PAPER' && observationStale);
    // A shared paper account can include positions outside this page's symbol.
    // Its valuation time follows the oldest open position's mark price.
    const paperValued = items.every(entry => {
      const s = entry.snapshot;
      const size = numeric(s.position.size);
      if (size === null) return false;
      if (size === 0) return true;
      const refNow = entryRefNow(entry, now);
      return recentAt(s.account.update_time, 30_000, refNow) && recentAt(s.ticker.mark_update_time, 15_000, refNow) && (numeric(s.ticker.mark_price) ?? 0) > 0;
    });
    const sharedPosition = numeric(account.account.margin_balance) !== numeric(account.account.available_balance)
      || (numeric(account.account.unrealized_profit) ?? 0) !== 0;
    const valued = !unavailable && (mode !== 'PAPER' || (paperValued && (!sharedPosition || recentAt(account.account.update_time, 15_000, entryRefNow(chosen, now)))));
    const equity = valued ? numeric(account.account.margin_balance) : null;
    const available = valued ? numeric(account.account.available_balance) : null;
    const wallet = unavailable ? null : numeric(account.account.total_wallet_balance);
    const unrealized = valued ? numeric(account.account.unrealized_profit) : null;
    const rate = account.account_cny_rate?.asset === asset ? numeric(account.account_cny_rate.cny_per_unit) : null;
    return { key: `${mode}:${asset}`, mode, asset, count: items.length, equity, available, wallet, unrealized, ratio: availableRatio(available, wallet), cny: equity !== null && rate !== null && rate > 0 && Number.isFinite(equity * rate) ? equity * rate : null };
  });
}

const PAPER_ACCOUNT_THRESHOLDS = [-5_000, 15_000, 30_000] as const;
const MARK_THRESHOLDS = [-5_000, 15_000] as const;

const thresholdCrossingWallMs = (ts: number, offset: number, entry: AccountObservation) => {
  if (entry.generated_at_ms !== undefined && entry.received_at_ms !== undefined) {
    if (ts + offset - entry.generated_at_ms <= 0) return -Infinity;
    return ts + offset + entry.received_at_ms - entry.generated_at_ms;
  }
  return ts + offset;
};

export function isEntryMarkFresh(entry: AccountObservation, now = Date.now()): boolean {
  return !entry.account_observation_stale && recentAt(entry.snapshot.ticker.mark_update_time, 15_000, entryRefNow(entry, now));
}

export function entryMarkExpiry(entry: AccountObservation, now = Date.now()): number {
  if (entry.account_observation_stale) return Infinity;
  const markTs = Date.parse(entry.snapshot.ticker.mark_update_time);
  if (!Number.isFinite(markTs)) return Infinity;
  let next = Infinity;
  for (const offset of MARK_THRESHOLDS) {
    const crossing = thresholdCrossingWallMs(markTs, offset, entry);
    if (crossing > now && crossing - 1 < next) next = crossing - 1;
  }
  return next;
}

export function nextAccountExpiry(entries: readonly AccountObservation[], now = Date.now()): number {
  let next = Infinity;
  for (const entry of entries) {
    const s = entry.snapshot;
    const accTs = Date.parse(s.account.update_time);
    if (Number.isFinite(accTs)) {
      const offsets = entry.mode === 'PAPER' ? PAPER_ACCOUNT_THRESHOLDS : [-5_000, liveAccountMaxAge(s.sync_interval_secs)];
      for (const offset of offsets) {
        const crossing = thresholdCrossingWallMs(accTs, offset, entry);
        if (crossing > now && crossing - 1 < next) next = crossing - 1;
      }
    }
    const markExpiry = entryMarkExpiry(entry, now);
    if (markExpiry < next) next = markExpiry;
  }
  return next;
}
