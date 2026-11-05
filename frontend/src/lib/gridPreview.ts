/** Mirrors the engine's zero-anchored lattice (strategy/precision.rs `grid_window_prices`) with exact decimal arithmetic. */
interface Fixed { n: bigint; s: number }
const pattern = /^\d+(\.\d+)?$/;

function parse(value: unknown): Fixed | null {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  if (!pattern.test(text)) return null;
  const [whole, fraction = ''] = text.split('.');
  return { n: BigInt(whole + fraction), s: fraction.length };
}
const rescale = (value: Fixed, scale: number) => value.n * 10n ** BigInt(scale - value.s);
function format(n: bigint, scale: number) {
  const digits = n.toString().padStart(scale + 1, '0');
  const whole = digits.slice(0, digits.length - scale), fraction = digits.slice(digits.length - scale);
  return scale === 0 ? whole : `${whole}.${fraction}`.replace(/\.?0+$/, '');
}
const floorDiv = (a: bigint, b: bigint) => a / b;
const ceilDiv = (a: bigint, b: bigint) => (a + b - 1n) / b;

export interface GridPreviewInput { reference: unknown; interval: unknown; buyWindow: unknown; sellWindow: unknown; min?: unknown; max?: unknown }
export interface GridPreview { buys: string[]; sells: string[]; buyClipped: boolean; sellClipped: boolean }

export function gridPreview(input: GridPreviewInput): GridPreview | null {
  const reference = parse(input.reference), interval = parse(input.interval);
  const buyWindow = Number(input.buyWindow), sellWindow = Number(input.sellWindow);
  if (!reference || !interval || reference.n === 0n || interval.n === 0n) return null;
  if (![buyWindow, sellWindow].every(count => Number.isInteger(count) && count >= 1 && count <= 50)) return null;
  const min = parse(input.min), max = parse(input.max);
  const scale = Math.max(reference.s, interval.s, min?.s ?? 0, max?.s ?? 0);
  const mark = rescale(reference, scale), step = rescale(interval, scale);
  const lower = min ? (rescale(min, scale) > step ? rescale(min, scale) : step) : step;
  const upper = max ? rescale(max, scale) : null;
  let below = (ceilDiv(mark, step) - 1n) * step;
  if (upper !== null) { const cap = floorDiv(upper, step) * step; if (cap < below) below = cap; }
  let above = (floorDiv(mark, step) + 1n) * step;
  if (min) { const floor = ceilDiv(rescale(min, scale), step) * step; if (floor > above) above = floor; }
  const buys: string[] = [], sells: string[] = [];
  for (let price = below; buys.length < buyWindow && price >= lower && price > 0n && (upper === null || price <= upper); price -= step) buys.push(format(price, scale));
  for (let price = above; sells.length < sellWindow && price >= lower && (upper === null || price <= upper); price += step) sells.push(format(price, scale));
  return { buys, sells, buyClipped: buys.length < buyWindow, sellClipped: sells.length < sellWindow };
}

/** count × amount, exact. */
export function exposure(amount: unknown, count: number): string | null {
  const value = parse(amount);
  return value ? format(value.n * BigInt(count), value.s) : null;
}
/** Compare two decimal strings; null when either is not a plain decimal. */
export function exceeds(value: string | null, limit: unknown): boolean {
  const a = parse(value), b = parse(limit);
  if (!a || !b) return false;
  const scale = Math.max(a.s, b.s);
  return rescale(a, scale) > rescale(b, scale);
}

/** Mirrors `GridConfig::validate` for the price range: both bounds positive, minimum below maximum. */
export function priceRangeError(field: 'min' | 'max', min: unknown, max: unknown): string | null {
  const own = parse(field === 'min' ? min : max);
  const raw = field === 'min' ? min : max;
  if (raw !== null && raw !== undefined && raw !== '' && (!own || own.n === 0n)) return field === 'min' ? '价格下限必须大于 0' : '价格上限必须大于 0';
  const lower = parse(min), upper = parse(max);
  if (lower && upper && lower.n !== 0n && upper.n !== 0n) {
    const scale = Math.max(lower.s, upper.s);
    if (rescale(lower, scale) >= rescale(upper, scale)) return '价格下限必须小于上限';
  }
  return null;
}

/** Mirrors `SymbolRules::validate_grid_interval`: the interval must be a positive integer multiple of `tick_size`. */
export function tickSizeError(interval: unknown, tickSize: unknown): string | null {
  const iv = parse(interval), tick = parse(tickSize);
  if (!iv || !tick || iv.n === 0n || tick.n === 0n) return null;
  const scale = Math.max(iv.s, tick.s);
  const ivScaled = rescale(iv, scale), tickScaled = rescale(tick, scale);
  if (ivScaled < tickScaled || ivScaled % tickScaled !== 0n) {
    return `网格间距 ${format(iv.n, iv.s)} 必须是价格步长 ${format(tick.n, tick.s)} 的正整数倍`;
  }
  return null;
}
