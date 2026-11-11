export interface Fixed {
  n: bigint;
  s: number;
}

const UNSIGNED_DECIMAL_PATTERN = /^\d+(\.\d+)?$/;
const SIGNED_DECIMAL_PATTERN = /^([+-])?(\d+)(?:\.(\d+))?$/;

export function parseUnsignedDecimal(value: unknown): Fixed | null {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  if (!UNSIGNED_DECIMAL_PATTERN.test(text)) return null;
  const [whole, fraction = ''] = text.split('.');
  return { n: BigInt(whole + fraction), s: fraction.length };
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
    text = value.trim();
  } else {
    return null;
  }
  const match = SIGNED_DECIMAL_PATTERN.exec(text);
  if (!match) return null;
  const [, sign, whole, fraction = ''] = match;
  const magnitude = BigInt(whole + fraction);
  return { n: sign === '-' && magnitude !== 0n ? -magnitude : magnitude, s: fraction.length };
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

export function addSignedDecimals(a: unknown, b: unknown): string | null {
  const left = parseSignedDecimal(a), right = parseSignedDecimal(b);
  if (!left || !right) return null;
  const [l, r, scale] = scaleDecimalPair(left, right);
  return formatDecimal(l + r, scale);
}

export function subSignedDecimals(a: unknown, b: unknown): string | null {
  const left = parseSignedDecimal(a), right = parseSignedDecimal(b);
  if (!left || !right) return null;
  const [l, r, scale] = scaleDecimalPair(left, right);
  return formatDecimal(l - r, scale);
}

export function sumSignedDecimals(values: Iterable<unknown>): string | null {
  const parsed: Fixed[] = [];
  let scale = 0;
  for (const value of values) {
    const item = parseSignedDecimal(value);
    if (!item) return null;
    if (item.s > scale) scale = item.s;
    parsed.push(item);
  }
  if (!parsed.length) return '0';
  const total = parsed.reduce((acc, item) => acc + rescaleDecimal(item, scale), 0n);
  return formatDecimal(total, scale);
}

export function signedDecimalToNumber(value: string | null): number | null {
  if (value === null) return null;
  if (value === '0') return 0;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

export interface DecimalDisplayOptions {
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
