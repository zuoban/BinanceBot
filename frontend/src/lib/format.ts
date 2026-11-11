import type { AccountObservation, GridConfig, Mode, Overview, PairSummary, Snapshot, Status, Trade } from './types';

export const SNAPSHOT_STALE_MS = 10_000;
export const modeLabel: Record<Mode, string> = { PAPER: '模拟盘', LIVE: '实盘', UNKNOWN: '未知模式' };
export const statusLabel: Record<Status, string> = { RUNNING: '运行中', PAUSED: '已暂停', STOPPED: '已停止', ERROR: '异常' };
export const directionLabel = (direction?: GridConfig['direction'] | string) => direction === 'SHORT' ? '做空' : '做多';
export const maxPositionPolicyLabel = (config: { pause_on_max_position?: boolean; cancel_opening_on_max_position?: boolean }) =>
  config.pause_on_max_position ? '暂停策略' : config.cancel_opening_on_max_position ? '仅撤开仓单' : '限制开仓';
export const quoteAsset = (symbol: string) => ['USDC', 'USDT', 'FDUSD', 'BUSD'].find(a => symbol.endsWith(a)) ?? '';
export const numeric = (value: unknown): number | null => typeof value === 'number' ? (Number.isFinite(value) ? value : null) : typeof value === 'string' && value.trim() !== '' && Number.isFinite(Number(value)) ? Number(value) : null;
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
const numberFormatters = new Map<number, Intl.NumberFormat>();
export const sig3Format = new Intl.NumberFormat('en-US', { maximumSignificantDigits: 3 });
export const beijingClockFormat = new Intl.DateTimeFormat('zh-CN', { timeZone: 'Asia/Shanghai', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
export function number(value: unknown, digits = 2) {
  const n = numeric(value);
  if (n === null) return '--';
  const rounded = (Math.abs(n) < 0.5 * 10 ** -digits ? 0 : n) || 0;
  let formatter = numberFormatters.get(digits);
  if (!formatter) {
    formatter = new Intl.NumberFormat('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits });
    numberFormatters.set(digits, formatter);
  }
  return formatter.format(rounded);
}
export const price = (value: unknown) => number(value, Math.min(10, Math.max(4, 2 - Math.floor(Math.log10(Math.abs(Number(value)) || 1)))));
export const trimFractionZeros = (value: string) => value.replace(/(\.\d*?[1-9])0+$|\.0+$/, '$1');
export const compactNumber = (value: unknown, digits = 2) => trimFractionZeros(number(value, digits));
export const compactPrice = (value: unknown) => trimFractionZeros(price(value));
export function positivePrice(value: unknown, compact = false) {
  const n = numeric(value);
  return n !== null && n > 0 ? (compact ? compactPrice(n) : price(n)) : '--';
}
export function positiveDecimalText(value: unknown): string | null {
  const n = numeric(value);
  return n !== null && n > 0 ? trimFractionZeros(String(value).trim()) : null;
}
export function entryPriceText(position: { size: string; entry_price: string }, compact = false) {
  const size = numeric(position.size), entry = numeric(position.entry_price);
  return size !== null && size !== 0 && entry !== null && entry > 0 ? (compact ? compactPrice(entry) : price(entry)) : '--';
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
export function cnyText(value: number | null | undefined, compact = false) {
  return value != null && Number.isFinite(value) ? `≈ ¥${compact ? compactNumber(value) : number(value)}` : '人民币估值待更新';
}
export function ratioText(value: number | null | undefined, compact = false) {
  return value != null && Number.isFinite(value) ? `${compact ? compactNumber(value) : number(value)}%` : '--';
}
const beijingTimeFormat = new Intl.DateTimeFormat('zh-CN', {
  timeZone: 'Asia/Shanghai',
  year: 'numeric',
  month: 'numeric',
  day: 'numeric',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: false,
});
export const time = (value: string | number) => {
  const ts = typeof value === 'number' ? value : Date.parse(value);
  return Number.isFinite(ts) ? beijingTimeFormat.format(ts) : '--';
};
function formatSeconds(seconds: number) {
  const [amount, unit] = seconds < 60 ? [seconds, '秒'] : seconds < 3600 ? [Math.floor(seconds / 60), '分钟'] : seconds < 86400 ? [Math.floor(seconds / 3600), '小时'] : [Math.floor(seconds / 86400), '天'];
  return `${amount}${unit}`;
}
export function relativeTime(value: string | number, now = Date.now()) {
  const timestamp = typeof value === 'number' ? value : Date.parse(value);
  if (!Number.isFinite(timestamp)) return '--';
  const seconds = Math.floor(Math.abs(now - timestamp) / 1000);
  return `${formatSeconds(seconds)}${timestamp > now ? '后' : '前'}`;
}
/** How long ago a past moment was; a clock a hair ahead of `now` still reads as just now. */
export function ageText(updatedAt: number, now = Date.now()) {
  const seconds = Math.max(0, Math.floor((now - updatedAt) / 1000));
  if (!Number.isFinite(seconds)) return '--';
  if (seconds < 2) return '刚刚';
  return `${formatSeconds(seconds)}前`;
}
export const beijingDay = (value = Date.now()) => new Date(value + 8 * 3600_000).toISOString().slice(0, 10);
const isSameBeijingDay = (windowEnd: string | undefined, now: number) => {
  if (!windowEnd) return false;
  const endTs = Date.parse(windowEnd);
  return Number.isFinite(endTs) && Math.floor((endTs + 8 * 3600_000) / 86_400_000) === Math.floor((now + 8 * 3600_000) / 86_400_000);
};
const normalizeFloatTotal = (total: number) => Math.abs(total) < 1e-12 ? 0 : Number(total.toPrecision(12));
export function todayTradeCounts(s: Snapshot | PairSummary, now = Date.now()) {
  if ('today_trade_counts' in s) {
    const counts = s.today_trade_counts;
    return counts && isSameBeijingDay(counts.window_end, now) ? { buy: counts.buy, sell: counts.sell } : null;
  }
  const stats = s.hourly_trade_stats;
  if (!stats || !isSameBeijingDay(stats.window_end, now)) return null;
  return stats.buckets.reduce((counts, b) => ({ buy: counts.buy + b.buy_count, sell: counts.sell + b.sell_count }), { buy: 0, sell: 0 });
}
export function todayPnlGroups(stats: Overview['today_pnl'], now = Date.now()) {
  if (!stats || !isSameBeijingDay(stats.window_end, now)) return null;
  const groups = groupByKey(
    stats.groups,
    row => modeAsset(row.mode, row.symbol).key,
    row => ({ mode: row.mode, asset: modeAsset(row.mode, row.symbol).asset, total: 0, pending: 0, available: true }),
    (group, row) => {
      const pnl = numeric(row.realized_pnl);
      if (pnl === null || !Number.isInteger(row.pending_pnl_count) || row.pending_pnl_count < 0) group.available = false;
      else { group.total += pnl; group.pending += row.pending_pnl_count; }
    },
  );
  return [...groups.entries()].map(([key, group]) => ({ ...group, key, total: group.available && !group.pending && Number.isFinite(group.total) ? normalizeFloatTotal(group.total) : null }));
}
export function groupTradePnl(trades: Trade[]) {
  const groups = groupByKey(
    trades,
    trade => modeAsset(trade.mode, trade.symbol).key,
    trade => ({ mode: trade.mode, asset: modeAsset(trade.mode, trade.symbol).asset, total: 0, available: true }),
    (group, trade) => {
      const pnl = numeric(trade.realized_pnl);
      if (!trade.pnl_verified || pnl === null) group.available = false;
      else group.total += pnl;
    },
  );
  for (const group of groups.values()) group.total = normalizeFloatTotal(group.total);
  return groups;
}
export function todayCount(s: Snapshot | PairSummary, now = Date.now()) {
  const counts = todayTradeCounts(s, now);
  return counts ? counts.buy + counts.sell : null;
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
export function accountGroups<T extends AccountObservation>(entries: T[], now = Date.now()): AccountGroup[] {
  const groups = groupByKey(
    entries,
    ({ mode, snapshot: s }) => `${mode}:${s.account.asset || quoteAsset(s.symbol)}`,
    ({ mode, snapshot: s }) => ({ mode, asset: s.account.asset || quoteAsset(s.symbol), snapshots: [] as AccountObservation['snapshot'][], observed: [] as AccountObservation['snapshot'][], observationStale: false }),
    (group, { mode, snapshot: s, account_observation_stale }) => {
      group.snapshots.push(s);
      if (!account_observation_stale) group.observed.push(s);
      group.observationStale ||= !!account_observation_stale || (mode === 'PAPER' && !s.account.asset);
    },
  );
  return [...groups.values()].map(({ mode, asset, snapshots, observed, observationStale }) => {
    // Exchange timestamps can lead the browser clock slightly.
    const recent = (timestamp: string, maxAge: number) => {
      const age = now - Date.parse(timestamp);
      return age >= -5_000 && age < maxAge;
    };
    let latest: AccountObservation['snapshot'] | undefined;
    let latestTs = -Infinity;
    const maxAge = mode === 'PAPER' ? Infinity : 30_000;
    for (const s of observed) {
      const ts = Date.parse(s.account.update_time);
      const age = now - ts;
      if (age >= -5_000 && age < maxAge && ts > latestTs) {
        latestTs = ts;
        latest = s;
      }
    }
    const account = latest ?? snapshots[0];
    const unavailable = !latest || (mode === 'PAPER' && observationStale);
    // A shared paper account can include positions outside this page's symbol.
    // Its valuation time follows the oldest open position's mark price.
    const paperValued = snapshots.every(s => {
      const size = numeric(s.position.size);
      if (size === null) return false;
      if (size === 0) return true;
      return recent(s.account.update_time, 30_000) && recent(s.ticker.mark_update_time, 15_000) && (numeric(s.ticker.mark_price) ?? 0) > 0;
    });
    const sharedPosition = numeric(account.account.margin_balance) !== numeric(account.account.available_balance)
      || (numeric(account.account.unrealized_profit) ?? 0) !== 0;
    const valued = !unavailable && (mode !== 'PAPER' || (paperValued && (!sharedPosition || recent(account.account.update_time, 15_000))));
    const equity = valued ? numeric(account.account.margin_balance) : null;
    const available = valued ? numeric(account.account.available_balance) : null;
    const wallet = unavailable ? null : numeric(account.account.total_wallet_balance);
    const unrealized = valued ? numeric(account.account.unrealized_profit) : null;
    const rate = account.account_cny_rate?.asset === asset ? numeric(account.account_cny_rate.cny_per_unit) : null;
    return { key: `${mode}:${asset}`, mode, asset, count: snapshots.length, equity, available, wallet, unrealized, ratio: availableRatio(available, wallet), cny: equity !== null && rate !== null && rate > 0 && Number.isFinite(equity * rate) ? equity * rate : null };
  });
}

const ACCOUNT_THRESHOLDS = [-5_000, 15_000, 30_000] as const;
const MARK_THRESHOLDS = [-5_000, 15_000] as const;
export function nextAccountExpiry(entries: readonly AccountObservation[], now = Date.now()): number {
  let next = Infinity;
  for (const { snapshot: s } of entries) {
    const accTs = Date.parse(s.account.update_time);
    if (Number.isFinite(accTs)) {
      for (const offset of ACCOUNT_THRESHOLDS) {
        if (accTs + offset > now && accTs + offset - 1 < next) next = accTs + offset - 1;
      }
    }
    const markTs = Date.parse(s.ticker.mark_update_time);
    if (Number.isFinite(markTs)) {
      for (const offset of MARK_THRESHOLDS) {
        if (markTs + offset > now && markTs + offset - 1 < next) next = markTs + offset - 1;
      }
    }
  }
  return next;
}
