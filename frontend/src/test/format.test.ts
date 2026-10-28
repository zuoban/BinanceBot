import { describe, expect, it } from 'vitest';
import { accountGroups, comparePositionAmount, positionAmount, todayCount } from '../lib/format';
import { snapshot } from './fixtures';

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

describe('position amount sorting', () => {
  it('sorts absolute position values from largest to smallest and puts unavailable values last', () => {
    const large = snapshot('LARGEUSDC'), short = snapshot('SHORTUSDC');
    const empty = snapshot('EMPTYUSDC'), unavailable = snapshot('UNKNOWNUSDC');
    large.position.size = '3';
    short.position.size = '-2';
    unavailable.ticker.mark_price = '0';
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
    expect(positionAmount(s)).toBeNull();
  });
});
