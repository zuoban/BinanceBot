import type { AccountObservation, GridConfig, Mode, Overview, PairSummary, Snapshot, Status, Trade } from './types';

export const modeLabel: Record<Mode, string> = { PAPER: '模拟盘', LIVE: '实盘', UNKNOWN: '未知模式' };
export const statusLabel: Record<Status, string> = { RUNNING: '运行中', PAUSED: '已暂停', STOPPED: '已停止', ERROR: '异常' };
export const directionLabel = (direction?: GridConfig['direction'] | string) => direction === 'SHORT' ? '做空' : '做多';
export const maxPositionPolicyLabel = (config: { pause_on_max_position?: boolean; cancel_opening_on_max_position?: boolean }) =>
  config.pause_on_max_position ? '暂停策略' : config.cancel_opening_on_max_position ? '仅撤开仓单' : '限制开仓';
export const quoteAsset = (symbol: string) => ['USDC', 'USDT', 'FDUSD', 'BUSD'].find(a => symbol.endsWith(a)) ?? '';
export const numeric = (value: unknown): number | null => value === null || value === undefined || value === '' || !Number.isFinite(Number(value)) ? null : Number(value);
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
export function positionGroups<T extends { mode: Mode; snapshot: ValuedPosition }>(entries: T[]) {
  const groups = new Map<string, { mode: Mode; asset: string; snapshots: ValuedPosition[] }>();
  for (const { mode, snapshot } of entries) {
    const asset = quoteAsset(snapshot.symbol) || snapshot.symbol;
    const key = `${mode}:${asset}`;
    const group = groups.get(key) ?? { mode, asset, snapshots: [] };
    group.snapshots.push(snapshot);
    groups.set(key, group);
  }
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
export function number(value: unknown, digits = 2) {
  const n = numeric(value);
  if (n === null) return '--';
  const rounded = Math.abs(n) < 0.5 * 10 ** -digits ? 0 : n;
  return rounded.toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits });
}
export const price = (value: unknown) => number(value, Math.min(10, Math.max(4, 2 - Math.floor(Math.log10(Math.abs(Number(value)) || 1)))));
export const trimFractionZeros = (value: string) => value.replace(/(\.\d*?[1-9])0+$|\.0+$/, '$1');
export const compactNumber = (value: unknown, digits = 2) => trimFractionZeros(number(value, digits));
export const compactPrice = (value: unknown) => trimFractionZeros(price(value));
export function positivePrice(value: unknown, compact = false) {
  const n = numeric(value);
  return n !== null && n > 0 ? (compact ? compactPrice(n) : price(n)) : '--';
}
export function entryPriceText(position: { size: string; entry_price: string }, compact = false) {
  const size = numeric(position.size), entry = numeric(position.entry_price);
  return size !== null && size !== 0 && entry !== null && entry > 0 ? (compact ? compactPrice(entry) : price(entry)) : '--';
}
export function cnyText(value: number | null | undefined, compact = false) {
  return value != null && Number.isFinite(value) ? `≈ ¥${compact ? compactNumber(value) : number(value)}` : '人民币估值待更新';
}
export function ratioText(value: number | null | undefined, compact = false) {
  return value != null && Number.isFinite(value) ? `${compact ? compactNumber(value) : number(value)}%` : '--';
}
export const time = (value: string) => new Date(value).toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai', hour12: false });
function formatSeconds(seconds: number) {
  const [amount, unit] = seconds < 60 ? [seconds, '秒'] : seconds < 3600 ? [Math.floor(seconds / 60), '分钟'] : seconds < 86400 ? [Math.floor(seconds / 3600), '小时'] : [Math.floor(seconds / 86400), '天'];
  return `${amount}${unit}`;
}
export function relativeTime(value: string, now = Date.now()) {
  const timestamp = Date.parse(value);
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
const normalizeFloatTotal = (total: number) => Math.abs(total) < 1e-12 ? 0 : Number(total.toPrecision(12));
export function todayTradeCounts(s: Snapshot | PairSummary, now = Date.now()) {
  if ('today_trade_counts' in s) {
    const counts = s.today_trade_counts;
    return counts && Number.isFinite(Date.parse(counts.window_end)) && beijingDay(Date.parse(counts.window_end)) === beijingDay(now) ? { buy: counts.buy, sell: counts.sell } : null;
  }
  const stats = s.hourly_trade_stats;
  if (!stats || !Number.isFinite(Date.parse(stats.window_end)) || beijingDay(Date.parse(stats.window_end)) !== beijingDay(now)) return null;
  return stats.buckets.reduce((counts, b) => ({ buy: counts.buy + b.buy_count, sell: counts.sell + b.sell_count }), { buy: 0, sell: 0 });
}
export function todayPnlGroups(stats: Overview['today_pnl'], now = Date.now()) {
  if (!stats || !Number.isFinite(Date.parse(stats.window_end)) || beijingDay(Date.parse(stats.window_end)) !== beijingDay(now)) return null;
  const groups = new Map<string, { mode: Mode; asset: string; total: number; pending: number; available: boolean }>();
  for (const row of stats.groups) {
    const asset = quoteAsset(row.symbol) || row.symbol, key = `${row.mode}:${asset}`;
    const group = groups.get(key) ?? { mode: row.mode, asset, total: 0, pending: 0, available: true };
    const pnl = numeric(row.realized_pnl);
    if (pnl === null || !Number.isInteger(row.pending_pnl_count) || row.pending_pnl_count < 0) group.available = false;
    else { group.total += pnl; group.pending += row.pending_pnl_count; }
    groups.set(key, group);
  }
  return [...groups.entries()].map(([key, group]) => ({ ...group, key, total: group.available && !group.pending && Number.isFinite(group.total) ? normalizeFloatTotal(group.total) : null }));
}
export function groupTradePnl(trades: Trade[]) {
  const groups = new Map<string, { mode: Trade['mode']; asset: string; total: number; available: boolean }>();
  for (const trade of trades) {
    const asset = quoteAsset(trade.symbol) || trade.symbol;
    const key = `${trade.mode}:${asset}`;
    const group = groups.get(key) ?? { mode: trade.mode, asset, total: 0, available: true };
    const pnl = numeric(trade.realized_pnl);
    if (!trade.pnl_verified || pnl === null) group.available = false;
    else group.total += pnl;
    groups.set(key, group);
  }
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

export function accountGroups<T extends AccountObservation>(entries: T[], now = Date.now()) {
  const groups = new Map<string, { mode: Mode; asset: string; snapshots: AccountObservation['snapshot'][]; observed: AccountObservation['snapshot'][]; observationStale: boolean }>();
  for (const { mode, snapshot: s, account_observation_stale } of entries) {
    const asset = s.account.asset || quoteAsset(s.symbol), key = `${mode}:${asset}`;
    const group = groups.get(key) ?? { mode, asset, snapshots: [], observed: [], observationStale: false };
    group.snapshots.push(s);
    if (!account_observation_stale) group.observed.push(s);
    group.observationStale ||= !!account_observation_stale || (mode === 'PAPER' && !s.account.asset);
    groups.set(key, group);
  }
  return [...groups.values()].map(({ mode, asset, snapshots, observed, observationStale }) => {
    // Exchange timestamps can lead the browser clock slightly.
    const recent = (timestamp: string, maxAge: number) => {
      const age = now - Date.parse(timestamp);
      return age >= -5_000 && age < maxAge;
    };
    const latest = [...observed].filter(s => recent(s.account.update_time, mode === 'PAPER' ? Infinity : 30_000))
      .sort((a, b) => Date.parse(b.account.update_time) - Date.parse(a.account.update_time))[0];
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
