import { formatDecimal, formatDecimalDisplay, parseSignedDecimal, parseUnsignedDecimal, rescaleDecimal, scaleDecimalPair, type Fixed } from './decimal';
import { compactNumber, isValidEpochMs, numeric, positivePrice } from './format';
import type { AccountObservation, Mode, Overview, PairSummary, Snapshot, Trade } from './types';

export const SNAPSHOT_STALE_MS = 10_000;
export const PAIRS_STALE_MS = 25_000;
const QUOTE_SUFFIXES = ['FDUSD', 'BFUSD', 'USDC', 'USDT', 'BUSD', 'TUSD', 'USDP', 'DAI', 'EUR', 'BTC', 'ETH', 'BNB'] as const;
export const quoteAsset = (symbol: string, fallbackAsset = '') => {
  const normalized = symbol.trim().toUpperCase();
  const matched = QUOTE_SUFFIXES.find(asset => normalized.length > asset.length && normalized.endsWith(asset));
  return matched ?? fallbackAsset.trim().toUpperCase();
};

const modeAsset = (mode: Mode, symbol: string, fallbackAsset = '') => {
  const asset = quoteAsset(symbol, fallbackAsset) || symbol;
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

function selectMarkRaw(s: ValuedPosition): unknown {
  const tickerFixed = parseSignedDecimal(s.ticker.mark_price);
  if (tickerFixed) return tickerFixed.n > 0n ? s.ticker.mark_price : s.position.mark_price;
  const tickerNum = numeric(s.ticker.mark_price);
  return tickerNum !== null && tickerNum > 0 ? s.ticker.mark_price : s.position.mark_price;
}

function positionNotionalFixed(s: ValuedPosition): { signedSize: Fixed; notional: Fixed } | null {
  const size = parseSignedDecimal(s.position.size);
  const mark = parseSignedDecimal(selectMarkRaw(s));
  if (!size || !mark || mark.n <= 0n) return null;
  const absSize = size.n < 0n ? -size.n : size.n;
  return { signedSize: size, notional: { n: absSize * mark.n, s: size.s + mark.s } };
}

export function positionAmountText(s: ValuedPosition): string | null {
  const exact = positionNotionalFixed(s);
  if (exact) return formatDecimal(exact.notional.n, exact.notional.s);
  const fallback = positionAmount(s);
  return fallback !== null ? String(fallback) : null;
}

export function positionAmount(s: ValuedPosition): number | null {
  const exact = positionNotionalFixed(s);
  if (exact) {
    if (exact.notional.n === 0n) return 0;
    const n = Number(formatDecimal(exact.notional.n, exact.notional.s));
    return Number.isFinite(n) ? n : null;
  }
  const size = numeric(s.position.size), mark = numeric(selectMarkRaw(s));
  if (size === null || mark === null || mark <= 0) return null;
  const amount = Math.abs(size * mark);
  return Number.isFinite(amount) ? amount : null;
}

export function comparePositionAmount(a: ValuedPosition, b: ValuedPosition) {
  const exactA = positionNotionalFixed(a), exactB = positionNotionalFixed(b);
  if (exactA && exactB) {
    const [la, lb] = scaleDecimalPair(exactA.notional, exactB.notional);
    return lb < la ? -1 : lb > la ? 1 : a.symbol.localeCompare(b.symbol);
  }
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
  longText: string | null;
  shortText: string | null;
  totalText: string | null;
}

export function positionGroups<T extends { mode: Mode; snapshot: ValuedPosition & { account?: { asset?: string } } }>(entries: T[]): PositionGroup[] {
  const groups = groupByKey(
    entries,
    ({ mode, snapshot }) => modeAsset(mode, snapshot.symbol, snapshot.account?.asset).key,
    ({ mode, snapshot }) => ({ mode, asset: modeAsset(mode, snapshot.symbol, snapshot.account?.asset).asset, snapshots: [] as ValuedPosition[] }),
    (group, { snapshot }) => { group.snapshots.push(snapshot); },
  );
  return [...groups.entries()].map(([key, group]) => {
    const longFixed: Fixed[] = [], shortFixed: Fixed[] = [];
    let longFloat = 0, shortFloat = 0, hasFloat = false, complete = true;
    for (const s of group.snapshots) {
      const parsedSize = parseSignedDecimal(s.position.size);
      if (parsedSize && parsedSize.n === 0n) continue;
      const exact = positionNotionalFixed(s);
      if (exact) {
        if (exact.signedSize.n > 0n) longFixed.push(exact.notional);
        else if (exact.signedSize.n < 0n) shortFixed.push(exact.notional);
        continue;
      }
      const size = numeric(s.position.size);
      if (size === 0) continue;
      const amount = positionAmount(s);
      if (size === null || amount === null) { complete = false; continue; }
      hasFloat = true;
      if (size > 0) longFloat += amount;
      if (size < 0) shortFloat += amount;
    }
    if (!complete) {
      return { key, mode: group.mode, asset: group.asset, count: group.snapshots.length, long: null, short: null, total: null, longText: null, shortText: null, totalText: null };
    }
    const scale = [...longFixed, ...shortFixed].reduce((max, item) => Math.max(max, item.s), 0);
    const longScaled = longFixed.reduce((acc, item) => acc + rescaleDecimal(item, scale), 0n);
    const shortScaled = shortFixed.reduce((acc, item) => acc + rescaleDecimal(item, scale), 0n);
    const totalScaled = longScaled + shortScaled;
    if (!hasFloat) {
      const longText = formatDecimal(longScaled, scale);
      const shortText = formatDecimal(shortScaled, scale);
      const totalText = formatDecimal(totalScaled, scale);
      const long = longScaled === 0n ? 0 : Number(longText);
      const short = shortScaled === 0n ? 0 : Number(shortText);
      const total = totalScaled === 0n ? 0 : Number(totalText);
      return {
        key, mode: group.mode, asset: group.asset, count: group.snapshots.length,
        long: Number.isFinite(long) ? long : null,
        short: Number.isFinite(short) ? short : null,
        total: Number.isFinite(total) ? total : null,
        longText, shortText, totalText,
      };
    }
    const long = (longScaled === 0n ? 0 : Number(formatDecimal(longScaled, scale))) + longFloat;
    const short = (shortScaled === 0n ? 0 : Number(formatDecimal(shortScaled, scale))) + shortFloat;
    const total = long + short;
    return {
      key, mode: group.mode, asset: group.asset, count: group.snapshots.length,
      long: Number.isFinite(long) ? long : null,
      short: Number.isFinite(short) ? short : null,
      total: Number.isFinite(total) ? total : null,
      longText: Number.isFinite(long) ? String(long) : null,
      shortText: Number.isFinite(short) ? String(short) : null,
      totalText: Number.isFinite(total) ? String(total) : null,
    };
  });
}

export function entryPriceText(position: { size: string; entry_price: string }, compact = false) {
  const parsedSize = parseSignedDecimal(position.size);
  const hasPosition = parsedSize ? parsedSize.n !== 0n : (numeric(position.size) ?? 0) !== 0;
  return hasPosition ? positivePrice(position.entry_price, compact) : '--';
}

export function entryChange(position: { size: string; entry_price: string }, markPrice: unknown): { text: string; className?: 'positive' | 'negative' } {
  const parsedSize = parseSignedDecimal(position.size);
  const parsedEntry = parseUnsignedDecimal(position.entry_price);
  const parsedMark = parseUnsignedDecimal(markPrice);
  if (parsedSize && parsedEntry && parsedMark) {
    if (parsedSize.n === 0n || parsedEntry.n <= 0n || parsedMark.n <= 0n) return { text: '--' };
    const [m, e] = scaleDecimalPair(parsedMark, parsedEntry);
    const diff = m - e;
    const absDiff = diff < 0n ? -diff : diff;
    const roundedBp = (absDiff * 10000n + e / 2n) / e;
    if (roundedBp === 0n) return { text: '0%' };
    const formatted = formatDecimalDisplay(formatDecimal(roundedBp, 2), { maxFractionDigits: 2, trimZeros: true });
    if (!formatted) return { text: '--' };
    return { text: `${diff > 0n ? '+' : '-'}${formatted}%`, className: diff > 0n ? 'positive' : 'negative' };
  }
  const size = numeric(position.size), entry = numeric(position.entry_price), mark = numeric(markPrice);
  const raw = size !== null && size !== 0 && entry !== null && entry > 0 && mark !== null && mark > 0 ? (mark / entry - 1) * 100 : null;
  const formatted = raw !== null && Number.isFinite(raw) ? compactNumber(raw, 2) : null;
  const rounded = formatted === null ? null : Number(formatted.replaceAll(',', ''));
  if (rounded === null) return { text: '--' };
  if (rounded === 0) return { text: '0%' };
  return { text: `${rounded > 0 ? '+' : ''}${formatted}%`, className: rounded > 0 ? 'positive' : 'negative' };
}

const isSameBeijingDay = (windowEnd: string | undefined, now: number) => {
  if (!windowEnd || !isValidEpochMs(now)) return false;
  const endTs = Date.parse(windowEnd);
  return isValidEpochMs(endTs) && Math.floor((endTs + 8 * 3600_000) / 86_400_000) === Math.floor((now + 8 * 3600_000) / 86_400_000);
};

export function todayTradeCounts(s: Snapshot | PairSummary, now = Date.now()) {
  if ('today_trade_counts' in s) {
    const counts = s.today_trade_counts;
    return counts && isSameBeijingDay(counts.window_end, now) ? { buy: counts.buy, sell: counts.sell } : null;
  }
  const stats = s.hourly_trade_stats;
  if (!stats || !isValidEpochMs(Date.parse(stats.window_start)) || !isSameBeijingDay(stats.window_end, now)) return null;
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
  sumText: string;
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
    const sumText = formatDecimal(totalScaled, scale);
    const sum = totalScaled === 0n ? 0 : Number(sumText);
    result.set(key, { key, mode: group.mode, asset: group.asset, sum, sumText, pending: group.pending, available: group.available && Number.isFinite(sum) });
  }
  return result;
}

export function todayPnlGroups(stats: Overview['today_pnl'], now = Date.now()) {
  if (!stats || !isSameBeijingDay(stats.window_end, now)) return null;
  return [...groupModeAssetPnl(stats.groups, row => ({ pnl: row.realized_pnl, pending: row.pending_pnl_count })).values()]
    .map(group => {
      const ready = group.available && !group.pending;
      return { ...group, total: ready ? group.sum : null, totalText: ready ? group.sumText : null };
    });
}

export function groupTradePnl(trades: Trade[]) {
  const groups = groupModeAssetPnl(trades, trade => ({ pnl: trade.realized_pnl, valid: trade.pnl_verified }));
  return new Map([...groups.entries()].map(([key, group]) => [
    key,
    { mode: group.mode, asset: group.asset, total: group.sum, totalText: group.available ? group.sumText : null, available: group.available },
  ]));
}

export function availableRatio(available: number | null, wallet: number | null): number | null {
  return available !== null && wallet !== null && wallet > 0 ? available / wallet * 100 : null;
}

export interface AccountGroup {
  key: string;
  scopeKey: string;
  mode: Mode;
  asset: string;
  account_scope?: string;
  count: number;
  equity: number | null;
  equityText: string | null;
  available: number | null;
  availableText: string | null;
  wallet: number | null;
  walletText: string | null;
  unrealized: number | null;
  unrealizedText: string | null;
  ratio: number | null;
  cny: number | null;
  cnyText: string | null;
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
    ({ mode, snapshot: s }) => `${mode}:${s.account.asset || quoteAsset(s.symbol)}:${s.account_scope ?? ''}`,
    ({ mode, snapshot: s }) => ({ mode, asset: s.account.asset || quoteAsset(s.symbol), account_scope: s.account_scope, items: [] as T[], observed: [] as T[], observationStale: false }),
    (group, entry) => {
      group.items.push(entry);
      if (!entry.account_observation_stale) group.observed.push(entry);
      group.observationStale ||= !!entry.account_observation_stale || (entry.mode === 'PAPER' && !entry.snapshot.account.asset);
    },
  );
  return [...groups.values()].map(({ mode, asset, account_scope, items, observed, observationStale }) => {
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
    const unavailable = !latest || observationStale;
    // A shared paper account can include positions outside this page's symbol.
    // Its valuation time follows the oldest open position's mark price.
    const paperValued = items.every(entry => {
      const s = entry.snapshot;
      const sizeDec = parseSignedDecimal(s.position.size);
      if (sizeDec) {
        if (sizeDec.n === 0n) return true;
      } else {
        const size = numeric(s.position.size);
        if (size === null) return false;
        if (size === 0) return true;
      }
      const refNow = entryRefNow(entry, now);
      const markDec = parseUnsignedDecimal(s.ticker.mark_price);
      const markPositive = markDec ? markDec.n > 0n : (numeric(s.ticker.mark_price) ?? 0) > 0;
      return recentAt(s.account.update_time, 30_000, refNow) && recentAt(s.ticker.mark_update_time, 15_000, refNow) && markPositive;
    });
    const marginDec = parseSignedDecimal(account.account.margin_balance), availDec = parseSignedDecimal(account.account.available_balance), unrealDec = parseSignedDecimal(account.account.unrealized_profit);
    const walletDec = parseSignedDecimal(account.account.total_wallet_balance);
    const balanceDiff = marginDec && availDec ? scaleDecimalPair(marginDec, availDec)[0] !== scaleDecimalPair(marginDec, availDec)[1] : numeric(account.account.margin_balance) !== numeric(account.account.available_balance);
    const sharedPosition = balanceDiff || (unrealDec ? unrealDec.n !== 0n : (numeric(account.account.unrealized_profit) ?? 0) !== 0);
    const valued = !unavailable && (mode !== 'PAPER' || (paperValued && (!sharedPosition || recentAt(account.account.update_time, 15_000, entryRefNow(chosen, now)))));
    const equity = valued ? numeric(account.account.margin_balance) : null;
    const available = valued ? numeric(account.account.available_balance) : null;
    const wallet = unavailable ? null : numeric(account.account.total_wallet_balance);
    const unrealized = valued ? numeric(account.account.unrealized_profit) : null;
    const equityText = valued ? (marginDec ? formatDecimal(marginDec.n, marginDec.s) : (equity !== null ? String(equity) : null)) : null;
    const availableText = valued ? (availDec ? formatDecimal(availDec.n, availDec.s) : (available !== null ? String(available) : null)) : null;
    const walletText = unavailable ? null : (walletDec ? formatDecimal(walletDec.n, walletDec.s) : (wallet !== null ? String(wallet) : null));
    const unrealizedText = valued ? (unrealDec ? formatDecimal(unrealDec.n, unrealDec.s) : (unrealized !== null ? String(unrealized) : null)) : null;
    let ratio = availableRatio(available, wallet);
    if (ratio === null && valued && !unavailable && availDec && walletDec && walletDec.n > 0n) {
      const [availScaled, walletScaled] = scaleDecimalPair(availDec, walletDec);
      const ratioParsed = Number(formatDecimal((availScaled * 1_000_000n) / walletScaled, 4));
      ratio = Number.isFinite(ratioParsed) ? ratioParsed : null;
    }
    const rateRaw = account.account_cny_rate?.asset === asset ? account.account_cny_rate.cny_per_unit : null;
    const rateDec = parseUnsignedDecimal(rateRaw);
    const rate = numeric(rateRaw);
    let cny: number | null = null;
    let cnyText: string | null = null;
    if (valued && marginDec && rateDec && rateDec.n > 0n) {
      cnyText = formatDecimal(marginDec.n * rateDec.n, marginDec.s + rateDec.s);
      const cnyParsed = Number(cnyText);
      cny = Number.isFinite(cnyParsed) ? cnyParsed : null;
    } else if (equity !== null && rate !== null && rate > 0 && Number.isFinite(equity * rate)) {
      cny = equity * rate;
      cnyText = String(cny);
    }
    return {
      key: `${mode}:${asset}`, scopeKey: `${mode}:${asset}:${account_scope ?? ''}`, mode, asset, account_scope, count: items.length,
      equity, equityText, available, availableText, wallet, walletText, unrealized, unrealizedText,
      ratio, cny, cnyText,
    };
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
