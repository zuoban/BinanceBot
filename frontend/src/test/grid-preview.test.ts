import { expect, it } from 'vitest';
import { defaultGridInterval, exceeds, exposure, gridPreview, orderAmountError, orderSizing, priceDiffPercent, priceRangeError, capCoverage, spacingBelowRoundTripFee, spacingPercent, sumDecimals, tickSizeError } from '../lib/gridPreview';

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
  expect(defaultGridInterval('142.3500', '0.1')).toBe('1.4');
  expect(defaultGridInterval('142.3500', '0.01')).toBe('1.42');
  expect(defaultGridInterval('0.004', '0.001')).toBe('0.001');
  expect(defaultGridInterval('142.35')).toBe('1.4235');
  expect(defaultGridInterval('')).toBeNull();
});

it('truncates order quantity by stepSize and flags orders below minQty or minNotional', () => {
  expect(orderSizing('80000', '100', { stepSize: '0.001', minQty: '0.001', minNotional: '5' }))
    .toEqual({ quantity: '0.001', notional: '80', error: null });
  expect(orderSizing('80000', '50', { stepSize: '0.001', minQty: '0.001', minNotional: '5' }))
    .toEqual({ quantity: '0', notional: '0', error: '按数量步长 0.001 截断后数量为 0，低于最小下单量 0.001，该档将无法挂单' });
  expect(orderSizing('100', '4.5', { stepSize: '0.01', minQty: '0.01', minNotional: '5' }))
    .toEqual({ quantity: '0.04', notional: '4', error: '按数量步长截断后实际金额为 4，低于最小名义金额 5，该档将无法挂单' });
});

it('sums truncated notionals and validates orderAmountError against minNotional and stepSize', () => {
  expect(sumDecimals([])).toBe('0');
  expect(sumDecimals(['99.18', '99.29', '99.39'])).toBe('297.86');
  expect(orderAmountError('4', '142.35', { stepSize: '0.01', minQty: '0.01', minNotional: '5' }))
    .toBe('每格金额不能低于最小名义金额 5');
  expect(orderAmountError('60', '80000', { stepSize: '0.001', minQty: '0.001', minNotional: '50' }))
    .toBe('按数量步长 0.001 截断后数量为 0，低于最小下单量 0.001，该档将无法挂单');
  expect(orderAmountError('100', '80000', { stepSize: '0.001', minQty: '0.001', minNotional: '50' }))
    .toBeNull();
});

it('computes signed percentage distance from the reference price', () => {
  expect(priceDiffPercent('100.5', '100')).toBe('+0.5%');
  expect(priceDiffPercent('99', '100')).toBe('-1%');
  expect(priceDiffPercent('142.4', '142.35')).toBe('+0.04%');
  expect(priceDiffPercent('142.3', '142.35')).toBe('-0.04%');
  expect(priceDiffPercent('80001', '80000')).toBe('+0.0013%');
  expect(priceDiffPercent('100', '100')).toBe('0%');
  expect(priceDiffPercent('100', '')).toBeNull();
});

it('flags spacing below the 0.04% Maker round-trip fee estimate, exactly at the boundary included', () => {
  expect(spacingBelowRoundTripFee('0.03', '100')).toBe(true);
  expect(spacingBelowRoundTripFee('0.04', '100')).toBe(false);
  expect(spacingBelowRoundTripFee('1', '100')).toBe(false);
  expect(spacingBelowRoundTripFee('x', '100')).toBe(false);
  expect(spacingPercent('1', '100')).toBe('1');
  expect(spacingPercent('0.03', '100')).toBe('0.03');
});

it('measures how far price must move one way to fill the position cap', () => {
  // floor(1600 / 200) = 8 levels x 50 / 75000 = 0.5333%
  expect(capCoverage('1600', '200', '50', '75000')).toEqual({ levels: 8, percent: '0.5333', tight: true });
  // 64 levels x 50 / 75000 = 4.2667%
  expect(capCoverage('12800', '200', '50', '75000')).toEqual({ levels: 64, percent: '4.2667', tight: false });
  // Exactly 3% is not tight: 30 levels x 1 / 1000.
  expect(capCoverage('6000', '200', '1', '1000')).toMatchObject({ percent: '3', tight: false });
  expect(capCoverage('5800', '200', '1', '1000')).toMatchObject({ levels: 29, tight: true });
  // Not even one order fits, or inputs are unusable: no figure (the over-limit warning covers it).
  expect(capCoverage('100', '200', '50', '75000')).toBeNull();
  expect(capCoverage(null, '200', '50', '75000')).toBeNull();
  expect(capCoverage('1600', '0', '50', '75000')).toBeNull();
});
