import { formatDecimal, formatDecimalDisplay, parseSignedDecimal, parseUnsignedDecimal, type Fixed } from './decimal';
import type { GridConfig, Mode, Status } from './types';

export const modeLabel: Record<Mode, string> = { PAPER: '模拟盘', LIVE: '实盘', UNKNOWN: '未知模式' };
export const statusLabel: Record<Status, string> = { RUNNING: '运行中', PAUSED: '已暂停', STOPPED: '已停止', ERROR: '异常' };
export const directionLabel = (direction?: GridConfig['direction'] | string) => direction === 'SHORT' ? '做空' : '做多';
export const maxPositionPolicyLabel = (config: { pause_on_max_position?: boolean; cancel_opening_on_max_position?: boolean }) =>
  config.pause_on_max_position ? '暂停策略' : config.cancel_opening_on_max_position ? '仅撤开仓单' : '限制开仓';
export const numeric = (value: unknown): number | null => typeof value === 'number' ? (Number.isFinite(value) ? value : null) : typeof value === 'string' && value.trim() !== '' && Number.isFinite(Number(value)) ? Number(value) : null;
const numberFormatters = new Map<number, Intl.NumberFormat>();
export const sig3Format = new Intl.NumberFormat('en-US', { maximumSignificantDigits: 3 });
export const beijingClockFormat = new Intl.DateTimeFormat('zh-CN', { timeZone: 'Asia/Shanghai', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
export function number(value: unknown, digits = 2) {
  const exact = formatDecimalDisplay(value, { minFractionDigits: digits, maxFractionDigits: digits, trimZeros: false });
  if (exact !== null) return exact;
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
function priceDigitsFromFixed(parsed: Fixed): number {
  if (parsed.n === 0n) return 4;
  const absDigits = (parsed.n < 0n ? -parsed.n : parsed.n).toString();
  const magnitude = absDigits.length - 1 - parsed.s;
  return Math.min(10, Math.max(4, 2 - magnitude));
}
export function price(value: unknown) {
  if (typeof value === 'string') {
    const parsed = parseSignedDecimal(value);
    if (parsed) return number(value, priceDigitsFromFixed(parsed));
  }
  return number(value, Math.min(10, Math.max(4, 2 - Math.floor(Math.log10(Math.abs(Number(value)) || 1)))));
}
export const trimFractionZeros = (value: string) => value.replace(/(\.\d*?[1-9])0+$|\.0+$/, '$1');
export const compactNumber = (value: unknown, digits = 2) => trimFractionZeros(number(value, digits));
const compactPrice = (value: unknown) => trimFractionZeros(price(value));
export const quantity = (value: unknown) => formatDecimalDisplay(value, { trimZeros: true }) ?? '--';
export function positivePrice(value: unknown, compact = false) {
  if (typeof value === 'string') {
    const parsed = parseUnsignedDecimal(value);
    if (parsed) {
      if (parsed.n <= 0n) return '--';
      const formatted = number(value, priceDigitsFromFixed(parsed));
      return compact ? trimFractionZeros(formatted) : formatted;
    }
  }
  const n = numeric(value);
  return n !== null && n > 0 ? (compact ? compactPrice(n) : price(n)) : '--';
}
export function positiveDecimalText(value: unknown): string | null {
  const parsed = parseUnsignedDecimal(value);
  return parsed && parsed.n > 0n ? formatDecimal(parsed.n, parsed.s) : null;
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
export const isValidEpochMs = (ms: number) => Number.isFinite(ms) && Math.abs(ms) <= 8.64e15;
export const time = (value: string | number) => {
  const ts = typeof value === 'number' ? value : Date.parse(value);
  return isValidEpochMs(ts) ? beijingTimeFormat.format(ts) : '--';
};
function formatSeconds(seconds: number) {
  const [amount, unit] = seconds < 60 ? [seconds, '秒'] : seconds < 3600 ? [Math.floor(seconds / 60), '分钟'] : seconds < 86400 ? [Math.floor(seconds / 3600), '小时'] : [Math.floor(seconds / 86400), '天'];
  return `${amount}${unit}`;
}
/** How long ago a past moment was; a clock a hair ahead of `now` still reads as just now. */
export function ageText(updatedAt: number, now = Date.now()) {
  const seconds = Math.max(0, Math.floor((now - updatedAt) / 1000));
  if (!Number.isFinite(seconds)) return '--';
  if (seconds < 2) return '刚刚';
  return `${formatSeconds(seconds)}前`;
}
export function beijingDay(value = Date.now()) {
  const shifted = (isValidEpochMs(value) ? value : Date.now()) + 8 * 3600_000;
  return new Date(isValidEpochMs(shifted) ? shifted : Date.now() + 8 * 3600_000).toISOString().slice(0, 10);
}
