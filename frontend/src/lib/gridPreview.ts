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
function scalePair(a: Fixed, b: Fixed): [bigint, bigint, number] {
  const scale = Math.max(a.s, b.s);
  return [rescale(a, scale), rescale(b, scale), scale];
}
const formatPercent4 = (num: bigint, den: bigint): string => format((num * 1000000n + den / 2n) / den, 4);
const floorDiv = (a: bigint, b: bigint) => a / b;
const ceilDiv = (a: bigint, b: bigint) => (a + b - 1n) / b;

export interface GridPreviewInput { reference: unknown; interval: unknown; buyWindow: unknown; sellWindow: unknown; min?: unknown; max?: unknown }
export interface GridPreview { buys: string[]; sells: string[]; buyClipped: boolean; sellClipped: boolean }

export function gridPreview(input: GridPreviewInput): GridPreview | null {
  const reference = parse(input.reference), interval = parse(input.interval);
  const buyWindow = Number(input.buyWindow), sellWindow = Number(input.sellWindow);
  if (!reference || !interval || reference.n === 0n || interval.n === 0n) return null;
  if (![buyWindow, sellWindow].every(count => Number.isInteger(count) && count >= 1 && count <= 50)) return null;
  if (priceRangeError('min', input.min, input.max) || priceRangeError('max', input.min, input.max)) return null;
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
  const [left, right] = scalePair(a, b);
  return left > right;
}

/** True when `value` is a positive plain decimal (`> 0`). */
export function isPositiveDecimal(value: unknown): boolean {
  const parsed = parse(value);
  return !!parsed && parsed.n > 0n;
}

/** True when `value` is a plain decimal strictly between `0` and `100`. */
export function isPercentDecimal(value: unknown): boolean {
  const parsed = parse(value);
  return !!parsed && parsed.n > 0n && parsed.n < 100n * 10n ** BigInt(parsed.s);
}

/** Exact numeric equality for plain decimals; returns false when either side is not a plain decimal. */
export function sameDecimal(a: unknown, b: unknown): boolean {
  const left = parse(a), right = parse(b);
  if (!left || !right) return false;
  const [leftScaled, rightScaled] = scalePair(left, right);
  return leftScaled === rightScaled;
}

/** Mirrors `GridConfig::validate` for the price range: both bounds positive, minimum below maximum. */
export function priceRangeError(field: 'min' | 'max', min: unknown, max: unknown): string | null {
  const raw = field === 'min' ? min : max;
  const own = parse(raw);
  if (raw !== null && raw !== undefined && raw !== '' && (!own || own.n === 0n)) return field === 'min' ? '价格下限必须大于 0' : '价格上限必须大于 0';
  const lower = parse(min), upper = parse(max);
  if (lower && upper && lower.n !== 0n && upper.n !== 0n) {
    const [lowerScaled, upperScaled] = scalePair(lower, upper);
    if (lowerScaled >= upperScaled) return '价格下限必须小于上限';
  }
  return null;
}

/** Mirrors `SymbolRules::validate_grid_interval`: the interval must be a positive integer multiple of `tick_size`. */
export function tickSizeError(interval: unknown, tickSize: unknown): string | null {
  const iv = parse(interval), tick = parse(tickSize);
  if (!iv || !tick || iv.n === 0n || tick.n === 0n) return null;
  const [ivScaled, tickScaled] = scalePair(iv, tick);
  if (ivScaled < tickScaled || ivScaled % tickScaled !== 0n) {
    return `网格间距 ${format(iv.n, iv.s)} 必须是价格步长 ${format(tick.n, tick.s)} 的正整数倍`;
  }
  return null;
}

/** Default `grid_interval` at 1% of `markPrice`, quantized to the nearest positive integer multiple of `tickSize` when provided. */
export function defaultGridInterval(markPrice: unknown, tickSize?: unknown): string | null {
  const mark = parse(markPrice);
  if (!mark || mark.n === 0n) return null;
  const target: Fixed = { n: mark.n, s: mark.s + 2 };
  const tick = parse(tickSize);
  if (!tick || tick.n === 0n) return format(target.n, target.s);
  const [targetScaled, tickScaled] = scalePair(target, tick);
  const steps = (targetScaled + tickScaled / 2n) / tickScaled;
  const clamped = steps > 0n ? steps : 1n;
  return format(clamped * tick.n, tick.s);
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
  const [qtyScaled, minQtyScaled] = minQty ? scalePair({ n: qtyN, s: qtyScale }, minQty) : [0n, 0n];
  if (steps === 0n || (minQty && minQty.n > 0n && qtyScaled < minQtyScaled)) {
    const floorQty = minQty && minQty.n > 0n ? format(minQty.n, minQty.s) : format(step.n, step.s);
    return { quantity, notional, error: `按数量步长 ${format(step.n, step.s)} 截断后数量为 ${quantity}，低于最小下单量 ${floorQty}，该档将无法挂单` };
  }
  const minNotional = parse(rules?.minNotional);
  const [notionalScaled, minNotionalScaled] = minNotional ? scalePair({ n: notionalN, s: notionalScale }, minNotional) : [0n, 0n];
  if (minNotional && minNotional.n > 0n && notionalScaled < minNotionalScaled) {
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
    const [amountScaled, minNotionalScaled] = scalePair(a, minNotional);
    if (amountScaled < minNotionalScaled) {
      return `每格金额不能低于最小名义金额 ${format(minNotional.n, minNotional.s)}`;
    }
  }
  if (referencePrice !== null && referencePrice !== undefined && String(referencePrice).trim() !== '') {
    return orderSizing(referencePrice, amount, rules)?.error ?? null;
  }
  return null;
}

/** Informational capacity threshold; it does not predict profitability or the next cap event. */
const CAP_DROP_WARN_PERCENT = 3;

/**
 * Flat-position capacity estimate from target notionals:
 * floor(cap / amount) levels x spacing / reference. This excludes existing positions,
 * pending orders and quantity rounding; `tight` only marks estimates below the reference threshold.
 * Null when an input is not a plain decimal or not even one order fits under the cap.
 */
export function capCoverage(cap: unknown, amount: unknown, interval: unknown, reference: unknown): { levels: number; percent: string; tight: boolean } | null {
  const c = parse(cap), a = parse(amount), i = parse(interval), r = parse(reference);
  if (!c || !a || !i || !r || a.n === 0n || i.n === 0n || r.n === 0n) return null;
  const [capScaled, amountScaled] = scalePair(c, a);
  const levels = capScaled / amountScaled;
  if (levels === 0n) return null;
  const [intervalScaled, ref] = scalePair(i, r);
  const move = levels * intervalScaled;
  return {
    levels: Number(levels),
    percent: formatPercent4(move, ref),
    tight: move * 100n < BigInt(CAP_DROP_WARN_PERCENT) * ref,
  };
}

/** Grid spacing as a percentage of the reference price (up to 4 decimals); null when either is not a plain decimal. */
export function spacingPercent(interval: unknown, reference: unknown): string | null {
  const i = parse(interval), r = parse(reference);
  if (!i || !r || i.n === 0n || r.n === 0n) return null;
  const [iScaled, rScaled] = scalePair(i, r);
  return formatPercent4(iScaled, rScaled);
}

/** True when spacing is below the Maker round-trip fee estimate of 0.04% (2 × 0.02%) of the reference price. */
export function spacingBelowRoundTripFee(interval: unknown, reference: unknown): boolean {
  const i = parse(interval), r = parse(reference);
  if (!i || !r || i.n === 0n || r.n === 0n) return false;
  const [iScaled, rScaled] = scalePair(i, r);
  return iScaled * 2500n < rScaled;
}

/** Signed percentage distance `(price - reference) / reference * 100`. */
export function priceDiffPercent(price: unknown, reference: unknown): string | null {
  const p = parse(price), r = parse(reference);
  if (!p || !r || p.n === 0n || r.n === 0n) return null;
  const [pScaled, rScaled] = scalePair(p, r);
  const diff = pScaled - rScaled;
  if (diff === 0n) return '0%';
  const sign = diff > 0n ? '+' : '-';
  const absDiff = diff > 0n ? diff : -diff;
  const rounded2 = (absDiff * 10000n + rScaled / 2n) / rScaled;
  if (rounded2 > 0n) return `${sign}${format(rounded2, 2)}%`;
  const percent4 = formatPercent4(absDiff, rScaled);
  return percent4 !== '0' ? `${sign}${percent4}%` : `${sign}<0.0001%`;
}
