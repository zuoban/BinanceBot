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

/** Exact sum of plain decimal strings. */
export function sumDecimals(values: string[]): string | null {
  if (!values.length) return '0';
  const parsed = values.map(parse);
  if (parsed.some(item => !item)) return null;
  const list = parsed as Fixed[];
  const scale = Math.max(...list.map(item => item.s));
  const total = list.reduce((acc, item) => acc + rescale(item, scale), 0n);
  return format(total, scale);
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

export interface SymbolSizingRules { stepSize?: unknown; minQty?: unknown; minNotional?: unknown }
export interface LevelOrderSizing { quantity: string; notional: string; error: string | null }

/**
 * Mirrors `SymbolRules::calculate_quantity`: truncates `amount / price` down to a multiple of `stepSize`
 * and checks `minQty` and `minNotional` on the truncated order.
 */
export function orderSizing(price: unknown, amount: unknown, rules?: SymbolSizingRules): LevelOrderSizing | null {
  const p = parse(price), a = parse(amount), step = parse(rules?.stepSize);
  if (!p || !a || !step || p.n === 0n || a.n === 0n || step.n === 0n) return null;
  const denomScale = p.s + step.s;
  const commonScale = Math.max(a.s, denomScale);
  const numerator = a.n * 10n ** BigInt(commonScale - a.s);
  const denominator = p.n * step.n * 10n ** BigInt(commonScale - denomScale);
  const steps = numerator / denominator;
  const qtyN = steps * step.n, qtyScale = step.s;
  const notionalN = qtyN * p.n, notionalScale = step.s + p.s;
  const quantity = format(qtyN, qtyScale), notional = format(notionalN, notionalScale);
  const minQty = parse(rules?.minQty);
  if (steps === 0n || (minQty && minQty.n > 0n && rescale({ n: qtyN, s: qtyScale }, Math.max(qtyScale, minQty.s)) < rescale(minQty, Math.max(qtyScale, minQty.s)))) {
    const floorQty = minQty && minQty.n > 0n ? format(minQty.n, minQty.s) : format(step.n, step.s);
    return { quantity, notional, error: `按数量步长 ${format(step.n, step.s)} 截断后数量为 ${quantity}，低于最小下单量 ${floorQty}，该档将无法挂单` };
  }
  const minNotional = parse(rules?.minNotional);
  if (minNotional && minNotional.n > 0n && rescale({ n: notionalN, s: notionalScale }, Math.max(notionalScale, minNotional.s)) < rescale(minNotional, Math.max(notionalScale, minNotional.s))) {
    return { quantity, notional, error: `按数量步长截断后实际金额为 ${notional}，低于最小名义金额 ${format(minNotional.n, minNotional.s)}，该档将无法挂单` };
  }
  return { quantity, notional, error: null };
}

/** Validates `order_amount_usdc` against `minNotional` and (when `referencePrice` is known) `stepSize` / `minQty` truncation. */
export function orderAmountError(amount: unknown, referencePrice?: unknown, rules?: SymbolSizingRules): string | null {
  const a = parse(amount);
  if (!a || a.n === 0n) return null;
  const minNotional = parse(rules?.minNotional);
  if (minNotional && minNotional.n > 0n) {
    const scale = Math.max(a.s, minNotional.s);
    if (rescale(a, scale) < rescale(minNotional, scale)) {
      return `每格金额不能低于最小名义金额 ${format(minNotional.n, minNotional.s)}`;
    }
  }
  if (referencePrice !== null && referencePrice !== undefined && String(referencePrice).trim() !== '') {
    return orderSizing(referencePrice, amount, rules)?.error ?? null;
  }
  return null;
}
