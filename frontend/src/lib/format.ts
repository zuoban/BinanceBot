import type { Mode, Overview, Snapshot, Status } from './types';

export const modeLabel: Record<Mode, string> = { PAPER: '模拟盘', TESTNET: '测试网', LIVE: '实盘', UNKNOWN: '未知模式' };
export const statusLabel: Record<Status, string> = { RUNNING: '运行中', PAUSED: '已暂停', STOPPED: '已停止', ERROR: '异常' };
export const modeOf = (s: Snapshot): Mode => s.dry_run ? 'PAPER' : s.grid_config.is_testnet ? 'TESTNET' : 'LIVE';
export const quoteAsset = (symbol: string) => ['USDC', 'USDT', 'FDUSD', 'BUSD'].find(a => symbol.endsWith(a)) ?? '';
export const numeric = (value: unknown): number | null => value === null || value === undefined || value === '' || !Number.isFinite(Number(value)) ? null : Number(value);
export function positionAmount(s: Snapshot): number | null {
  const size = numeric(s.position.size), mark = numeric(s.ticker.mark_price);
  if (size === null || mark === null || mark <= 0) return null;
  const amount = Math.abs(size * mark);
  return Number.isFinite(amount) ? amount : null;
}
export function comparePositionAmount(a: Snapshot, b: Snapshot) {
  const left = positionAmount(a), right = positionAmount(b);
  return (right ?? -1) - (left ?? -1) || a.symbol.localeCompare(b.symbol);
}
export function number(value: unknown, digits = 2) {
  const n = numeric(value);
  return n === null ? '--' : n.toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits });
}
export const price = (value: unknown) => number(value, Math.min(10, Math.max(4, 2 - Math.floor(Math.log10(Math.abs(Number(value)) || 1)))));
export const time = (value: string) => new Date(value).toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai', hour12: false });
export const beijingDay = (value: number) => new Date(value + 8 * 3600_000).toISOString().slice(0, 10);
export function todayCount(s: Snapshot, now = Date.now()) {
  const stats = s.hourly_trade_stats;
  if (!stats || !Number.isFinite(Date.parse(stats.window_end)) || beijingDay(Date.parse(stats.window_end)) !== beijingDay(now)) return null;
  return stats.buckets.reduce((sum, b) => sum + b.buy_count + b.sell_count, 0);
}
export function accountGroups(entries: Overview['pairs'], now = Date.now()) {
  const groups = new Map<string, { mode: Mode; asset: string; snapshots: Snapshot[] }>();
  for (const { snapshot: s } of entries) {
    const mode = modeOf(s), asset = s.account.asset || quoteAsset(s.symbol), key = `${mode}:${asset}`;
    const group = groups.get(key) ?? { mode, asset, snapshots: [] };
    group.snapshots.push(s); groups.set(key, group);
  }
  return [...groups.values()].map(({ mode, asset, snapshots }) => {
    const latest = [...snapshots].sort((a, b) => Date.parse(b.account.update_time) - Date.parse(a.account.update_time))[0];
    const fresh = mode === 'PAPER' || now - Date.parse(latest.account.update_time) < 30_000;
    const sum = (values: (number | null)[]) => values.every(v => v !== null) ? values.reduce<number>((a, b) => a + b!, 0) : null;
    const equity = mode === 'PAPER' ? sum(snapshots.map(s => {
      const wallet = numeric(s.account.total_wallet_balance), pnl = numeric(s.position.unrealized_pnl);
      return wallet === null || pnl === null ? null : wallet + pnl;
    })) : fresh ? numeric(latest.account.margin_balance) : null;
    const available = mode === 'PAPER' ? sum(snapshots.map(s => numeric(s.account.available_balance))) : fresh ? numeric(latest.account.available_balance) : null;
    const rate = latest.account_cny_rate?.asset === asset ? numeric(latest.account_cny_rate.cny_per_unit) : null;
    return { key: `${mode}:${asset}`, mode, asset, count: snapshots.length, equity, available, cny: equity !== null && rate !== null ? equity * rate : null };
  });
}
