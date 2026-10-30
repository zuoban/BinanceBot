import { describe, expect, it } from 'vitest';
import { accountGroups, comparePositionAmount, positionAmount, positionGroups, relativeTime, todayCount, trimFractionZeros } from '../lib/format';
import { snapshot } from './fixtures';

it('removes only insignificant fractional zeros', () => {
  expect(['10,000.00', '99.5400', '0.0000', '0.0010', '-1.20', '--'].map(trimFractionZeros))
    .toEqual(['10,000', '99.54', '0', '0.001', '-1.2', '--']);
});

it('formats relative trade times across units and handles invalid or future dates', () => {
  const now = Date.parse('2026-01-02T00:00:00Z');
  const at = (seconds: number) => new Date(now - seconds * 1000).toISOString();
  expect([1, 59, 60, 3600, 86400].map(seconds => relativeTime(at(seconds), now)))
    .toEqual(['1秒前', '59秒前', '1分钟前', '1小时前', '1天前']);
  expect(relativeTime(at(-2), now)).toBe('2秒后');
  expect(relativeTime('invalid', now)).toBe('--');
});

describe('account and trade summaries', () => {
  it('sums independent paper accounts but deduplicates shared live accounts', () => {
    const paperA = snapshot('SOLUSDC'), paperB = snapshot('BTCUSDC');
    const liveA = snapshot('ETHUSDC', 'LIVE'), liveB = snapshot('XRPUSDC', 'LIVE');
    liveB.account.margin_balance = '200';
    liveB.account.available_balance = '175';
    liveB.account.update_time = new Date(Date.now() + 1000).toISOString();
    const groups = accountGroups([
      { snapshot: paperA, mode: 'PAPER', scoped: true }, { snapshot: paperB, mode: 'PAPER', scoped: true },
      { snapshot: liveA, mode: 'LIVE', scoped: true }, { snapshot: liveB, mode: 'LIVE', scoped: true },
    ]);
    expect(groups.find(g => g.mode === 'PAPER')).toMatchObject({ equity: 210, available: 160 });
    expect(groups.find(g => g.mode === 'LIVE')).toMatchObject({ equity: 200, available: 175 });
  });
  it('does not display a stale live account or yesterday’s trade total', () => {
    const s = snapshot('SOLUSDC', 'LIVE');
    s.account.update_time = new Date(Date.now() - 60_000).toISOString();
    s.hourly_trade_stats!.window_end = new Date(Date.now() - 86_400_000).toISOString();
    expect(accountGroups([{ snapshot: s, mode: 'LIVE', scoped: true }])[0].equity).toBeNull();
    expect(todayCount(s)).toBeNull();
  });
});

describe('position summaries', () => {
  it('groups by mode and quote asset and uses actual position direction', () => {
    const long = snapshot('BTCUSDC'), short = snapshot('ETHUSDC');
    const live = snapshot('SOLUSDC', 'LIVE'), otherAsset = snapshot('BTCUSDT');
    long.position.size = '3';
    short.position.size = '-1';
    short.grid_config.direction = 'LONG';
    live.position.size = '-2';
    otherAsset.position.size = '1';
    const groups = positionGroups([long, short, live, otherAsset].map(s => ({ snapshot: s, mode: s.dry_run ? 'PAPER' : 'LIVE', scoped: true })));
    expect(groups.find(g => g.key === 'PAPER:USDC')).toMatchObject({ long: 303, short: 101, total: 404 });
    expect(groups.find(g => g.key === 'LIVE:USDC')).toMatchObject({ long: 0, short: 202, total: 202 });
    expect(groups.find(g => g.key === 'PAPER:USDT')).toMatchObject({ total: 101 });
  });

  it('does not show partial totals when an open position cannot be valued', () => {
    const flat = snapshot('BTCUSDC'), missing = snapshot('ETHUSDC');
    flat.ticker.mark_price = '0';
    missing.position.size = '1';
    missing.ticker.mark_price = '0';
    missing.position.mark_price = '0';
    const entry = (s: typeof flat) => ({ snapshot: s, mode: 'PAPER' as const, scoped: true });
    expect(positionGroups([entry(flat)])[0]).toMatchObject({ total: 0, long: 0, short: 0 });
    expect(positionGroups([entry(flat), entry(missing)])[0]).toMatchObject({ total: null, long: null, short: null });
  });
});

describe('position amount sorting', () => {
  it('sorts absolute position values from largest to smallest and puts unavailable values last', () => {
    const large = snapshot('LARGEUSDC'), short = snapshot('SHORTUSDC');
    const empty = snapshot('EMPTYUSDC'), unavailable = snapshot('UNKNOWNUSDC');
    large.position.size = '3';
    short.position.size = '-2';
    unavailable.ticker.mark_price = '0';
    unavailable.position.mark_price = '0';
    expect([unavailable, empty, short, large].sort(comparePositionAmount).map(s => s.symbol))
      .toEqual(['LARGEUSDC', 'SHORTUSDC', 'EMPTYUSDC', 'UNKNOWNUSDC']);
    expect(positionAmount(short)).toBe(202);
    expect(positionAmount(unavailable)).toBeNull();
  });

  it('treats invalid quantities and prices as unavailable', () => {
    const s = snapshot();
    s.position.size = '';
    expect(positionAmount(s)).toBeNull();
    s.position.size = '2';
    s.ticker.mark_price = 'not-a-price';
    s.position.mark_price = '0';
    expect(positionAmount(s)).toBeNull();
  });

  it('uses the position mark when the ticker mark is unavailable', () => {
    const s = snapshot('BTCUSDC', 'LIVE');
    s.position.size = '-2';
    s.ticker.mark_price = '0';
    s.position.mark_price = '100';
    expect(positionAmount(s)).toBe(200);
    expect(positionGroups([{ snapshot: s, mode: 'LIVE', scoped: true }])[0])
      .toMatchObject({ long: 0, short: 200, total: 200 });
    s.ticker.mark_price = '101';
    expect(positionAmount(s)).toBe(202);
  });
});
