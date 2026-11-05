import { expect, it } from 'vitest';
import { exceeds, exposure, gridPreview, priceRangeError, tickSizeError } from '../lib/gridPreview';

const base = { interval: '0.5', buyWindow: 3, sellWindow: 3 };

it('places levels on the zero-anchored lattice around the reference price', () => {
  expect(gridPreview({ ...base, reference: '100.2' })).toEqual({ buys: ['100', '99.5', '99'], sells: ['100.5', '101', '101.5'], buyClipped: false, sellClipped: false });
});

it('skips the reference price itself when it sits exactly on a level', () => {
  const preview = gridPreview({ ...base, reference: '100' })!;
  expect(preview.buys).toEqual(['99.5', '99', '98.5']);
  expect(preview.sells).toEqual(['100.5', '101', '101.5']);
});

it('clips each side to the price range and reports it', () => {
  const preview = gridPreview({ ...base, reference: '100.2', min: '99.2', max: '100.9' })!;
  expect(preview.buys).toEqual(['100', '99.5']);
  expect(preview.sells).toEqual(['100.5']);
  expect(preview).toMatchObject({ buyClipped: true, sellClipped: true });
});

it('keeps exact decimals for small intervals', () => {
  expect(gridPreview({ interval: '0.0001', reference: '0.12345', buyWindow: 2, sellWindow: 2 })).toMatchObject({ buys: ['0.1234', '0.1233'], sells: ['0.1235', '0.1236'] });
});

it('returns nothing for incomplete or invalid input', () => {
  expect(gridPreview({ ...base, reference: '' })).toBeNull();
  expect(gridPreview({ ...base, reference: '100', interval: '0' })).toBeNull();
  expect(gridPreview({ ...base, reference: '100', buyWindow: 0 })).toBeNull();
  expect(gridPreview({ ...base, reference: '1e3' })).toBeNull();
});

it('computes exposure exactly and compares against the limit', () => {
  expect(exposure('33.33', 3)).toBe('99.99');
  expect(exceeds('99.99', '100')).toBe(false);
  expect(exceeds('100.01', '100')).toBe(true);
  expect(exceeds('100', null)).toBe(false);
});

it('validates the price range like the backend does', () => {
  expect(priceRangeError('min', null, null)).toBeNull();
  expect(priceRangeError('min', '', '')).toBeNull();
  expect(priceRangeError('min', '0', null)).toBe('价格下限必须大于 0');
  expect(priceRangeError('max', null, '0')).toBe('价格上限必须大于 0');
  expect(priceRangeError('min', '100', '100')).toBe('价格下限必须小于上限');
  expect(priceRangeError('max', '100.5', '100.50')).toBe('价格下限必须小于上限');
  expect(priceRangeError('min', '99.99', '100')).toBeNull();
  expect(priceRangeError('min', '90', null)).toBeNull();
});

it('validates that the grid interval is a positive integer multiple of tick_size', () => {
  expect(tickSizeError('0.1', null)).toBeNull();
  expect(tickSizeError('0.1', '0.01')).toBeNull();
  expect(tickSizeError('0.30', '0.1')).toBeNull();
  expect(tickSizeError('0.15', '0.1')).toBe('网格间距 0.15 必须是价格步长 0.1 的正整数倍');
  expect(tickSizeError('0.005', '0.01')).toBe('网格间距 0.005 必须是价格步长 0.01 的正整数倍');
});
