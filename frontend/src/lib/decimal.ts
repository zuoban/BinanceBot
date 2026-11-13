export type Numeric = string | number;

export interface Fixed {
  n: bigint;
  s: number;
}

const UNSIGNED_DECIMAL_PATTERN = /^\d+(\.\d+)?$/;
const SIGNED_DECIMAL_PATTERN = /^([+-])?(\d+)(?:\.(\d+))?$/;

const MAX_DECIMAL_INPUT_LENGTH = 64;
const MAX_DECIMAL_PART_DIGITS = 30;
const DECIMAL_CACHE_LIMIT = 4096;
const unsignedDecimalCache = new Map<string, Fixed | null>();
const signedDecimalCache = new Map<string, Fixed | null>();

function setDecimalCache(cache: Map<string, Fixed | null>, key: string, value: Fixed | null) {
  if (cache.has(key)) {
    cache.delete(key);
  } else if (cache.size >= DECIMAL_CACHE_LIMIT) {
    const oldest = cache.keys().next().value;
    if (oldest !== undefined) cache.delete(oldest);
  }
  cache.set(key, value);
}

export function parseUnsignedDecimal(value: unknown): Fixed | null {
  if (value === null || value === undefined) return null;
  if (typeof value === 'string' && value.length <= MAX_DECIMAL_INPUT_LENGTH) {
    const cached = unsignedDecimalCache.get(value);
    if (cached !== undefined) return cached;
  }
  const text = String(value).trim();
  if (text.length > MAX_DECIMAL_INPUT_LENGTH || !UNSIGNED_DECIMAL_PATTERN.test(text)) {
    if (typeof value === 'string' && value.length <= MAX_DECIMAL_INPUT_LENGTH) {
      setDecimalCache(unsignedDecimalCache, value, null);
    }
    return null;
  }
  const [whole, fraction = ''] = text.split('.');
  if (whole.replace(/^0+/, '').length > MAX_DECIMAL_PART_DIGITS || fraction.length > MAX_DECIMAL_PART_DIGITS) {
    if (typeof value === 'string' && value.length <= MAX_DECIMAL_INPUT_LENGTH) {
      setDecimalCache(unsignedDecimalCache, value, null);
    }
    return null;
  }
  const parsed: Fixed = { n: BigInt(whole + fraction), s: fraction.length };
  if (typeof value === 'string' && value.length <= MAX_DECIMAL_INPUT_LENGTH) {
    setDecimalCache(unsignedDecimalCache, value, parsed);
  }
  return parsed;
}

export function parseSignedDecimal(value: unknown): Fixed | null {
  if (value === null || value === undefined) return null;
  let text: string;
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) return null;
    if (value === 0) return { n: 0n, s: 0 };
    const raw = String(value);
    text = /[eE]/.test(raw) && Math.abs(value) < 1 ? value.toFixed(20) : raw;
  } else if (typeof value === 'string') {
    if (value.length <= MAX_DECIMAL_INPUT_LENGTH) {
      const cached = signedDecimalCache.get(value);
      if (cached !== undefined) return cached;
    }
    text = value.trim();
  } else {
    return null;
  }
  if (text.length > MAX_DECIMAL_INPUT_LENGTH) {
    if (typeof value === 'string' && value.length <= MAX_DECIMAL_INPUT_LENGTH) {
      setDecimalCache(signedDecimalCache, value, null);
    }
    return null;
  }
  const match = SIGNED_DECIMAL_PATTERN.exec(text);
  if (!match) {
    if (typeof value === 'string' && value.length <= MAX_DECIMAL_INPUT_LENGTH) {
      setDecimalCache(signedDecimalCache, value, null);
    }
    return null;
  }
  const [, sign, whole, fraction = ''] = match;
  if (whole.replace(/^0+/, '').length > MAX_DECIMAL_PART_DIGITS || fraction.length > MAX_DECIMAL_PART_DIGITS) {
    if (typeof value === 'string' && value.length <= MAX_DECIMAL_INPUT_LENGTH) {
      setDecimalCache(signedDecimalCache, value, null);
    }
    return null;
  }
  const magnitude = BigInt(whole + fraction);
  const parsed: Fixed = { n: sign === '-' && magnitude !== 0n ? -magnitude : magnitude, s: fraction.length };
  if (typeof value === 'string' && value.length <= MAX_DECIMAL_INPUT_LENGTH) {
    setDecimalCache(signedDecimalCache, value, parsed);
  }
  return parsed;
}

export const rescaleDecimal = (value: Fixed, scale: number): bigint => value.n * 10n ** BigInt(scale - value.s);

export function scaleDecimalPair(a: Fixed, b: Fixed): [bigint, bigint, number] {
  const scale = Math.max(a.s, b.s);
  return [rescaleDecimal(a, scale), rescaleDecimal(b, scale), scale];
}

export function formatDecimal(n: bigint, scale: number): string {
  if (n === 0n) return '0';
  const negative = n < 0n;
  const abs = negative ? -n : n;
  if (scale === 0) return negative ? `-${abs.toString()}` : abs.toString();
  const digits = abs.toString().padStart(scale + 1, '0');
  const split = digits.length - scale;
  const whole = digits.slice(0, split);
  const fraction = digits.slice(split).replace(/0+$/, '');
  const unsigned = fraction ? `${whole}.${fraction}` : whole;
  return negative ? `-${unsigned}` : unsigned;
}
export function subSignedDecimals(a: unknown, b: unknown): string | null {
  const left = parseSignedDecimal(a), right = parseSignedDecimal(b);
  if (!left || !right) return null;
  const [l, r, scale] = scaleDecimalPair(left, right);
  return formatDecimal(l - r, scale);
}
export function compareSignedDecimals(a: Numeric, b: Numeric): number {
  const left = parseSignedDecimal(a), right = parseSignedDecimal(b);
  if (left && right) {
    const [l, r] = scaleDecimalPair(left, right);
    return l < r ? -1 : l > r ? 1 : 0;
  }
  const na = typeof a === 'string' && a.trim() === '' ? NaN : Number(a);
  const nb = typeof b === 'string' && b.trim() === '' ? NaN : Number(b);
  if (Number.isFinite(na) && Number.isFinite(nb)) return na - nb;
  return Number.isFinite(na) ? 1 : Number.isFinite(nb) ? -1 : 0;
}

interface DecimalDisplayOptions {
  minFractionDigits?: number;
  maxFractionDigits?: number;
  trimZeros?: boolean;
}

export function formatDecimalDisplay(value: unknown, options: DecimalDisplayOptions = {}): string | null {
  const parsed = parseSignedDecimal(value);
  if (!parsed) return null;
  const { minFractionDigits = 0, maxFractionDigits, trimZeros = true } = options;
  let { n, s } = parsed;
  if (maxFractionDigits !== undefined && s > maxFractionDigits) {
    const factor = 10n ** BigInt(s - maxFractionDigits);
    const half = factor / 2n;
    n = n >= 0n ? (n + half) / factor : -((-n + half) / factor);
    s = maxFractionDigits;
  }
  if (!trimZeros && s < minFractionDigits) {
    n *= 10n ** BigInt(minFractionDigits - s);
    s = minFractionDigits;
  }
  if (n === 0n) {
    return !trimZeros && minFractionDigits > 0 ? `0.${'0'.repeat(minFractionDigits)}` : '0';
  }
  const negative = n < 0n;
  const abs = negative ? -n : n;
  const digits = abs.toString().padStart(s + 1, '0');
  const split = digits.length - s;
  const whole = digits.slice(0, split).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
  let fraction = s > 0 ? digits.slice(split) : '';
  if (trimZeros) {
    fraction = fraction.replace(/0+$/, '');
    if (fraction.length < minFractionDigits) fraction = fraction.padEnd(minFractionDigits, '0');
  }
  const unsigned = fraction ? `${whole}.${fraction}` : whole;
  return negative ? `-${unsigned}` : unsigned;
}
