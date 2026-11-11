import { describe, expect, it } from 'vitest';
import { compareSignedDecimals, formatDecimalDisplay, parseSignedDecimal, parseUnsignedDecimal, subSignedDecimals } from '../lib/decimal';
import { ageText, beijingDay, isValidEpochMs, number, positiveDecimalText, positivePrice, price, quantity, time, trimFractionZeros } from '../lib/format';
import { accountGroups, availableRatio, comparePositionAmount, entryChange, entryMarkExpiry, groupTradePnl, isEntryMarkFresh, nextAccountExpiry, positionAmount, positionGroups, todayCount, todayPnlGroups } from '../lib/portfolio';
import { snapshot } from './fixtures';

it('removes only insignificant fractional zeros and normalizes negative zero after rounding', () => {
  expect(['10,000.00', '99.5400', '0.0000', '0.0010', '-1.20', '--'].map(trimFractionZeros))
    .toEqual(['10,000', '99.54', '0', '0.001', '-1.2', '--']);
  expect(number(-0.001, 2)).toBe('0.00');
  expect(trimFractionZeros(number(-0.001, 2))).toBe('0');
});

it('formats quantities and signed decimals exactly without IEEE-754 truncation or 4-decimal caps', () => {
  expect(quantity('1.00025')).toBe('1.00025');
  expect(quantity('0.002000')).toBe('0.002');
  expect(quantity('10000.50')).toBe('10,000.5');
  expect(quantity('-2')).toBe('-2');
  expect(quantity('-0.000')).toBe('0');
  expect(quantity('9007199254740993.123456')).toBe('9,007,199,254,740,993.123456');
  expect(quantity('invalid')).toBe('--');
  expect(quantity('')).toBe('--');
  expect(formatDecimalDisplay('1234.5', { minFractionDigits: 2, trimZeros: false })).toBe('1,234.50');
  expect(subSignedDecimals('0.3', '0.1')).toBe('0.2');
});

it('preserves midpoint rounding, >2^53 precision, magnitude-based price digits, and exact notional/basis-point math', () => {
  expect(number('1.005', 2)).toBe('1.01');
  expect(number('1.00005', 4)).toBe('1.0001');
  expect(positivePrice('100.00005', true)).toBe('100.0001');
  expect(positivePrice('0.00001235', true)).toBe('0.0000124');
  expect(number('9007199254740993.12', 2)).toBe('9,007,199,254,740,993.12');
  expect(price('123.456789')).toBe('123.4568');
  expect(price('67420.50000000')).toBe('67,420.5000');
  expect(positivePrice('67420.50000000', true)).toBe('67,420.5');
  expect(entryChange({ size: '1', entry_price: '100' }, '100.005')).toEqual({ text: '+0.01%', className: 'positive' });
  const pos = snapshot('SOLUSDC');
  pos.position.size = '0.7';
  pos.ticker.mark_price = '1.45';
  expect(number(positionAmount(pos), 2)).toBe('1.02');
  expect(positionGroups([{ snapshot: pos, mode: 'PAPER', scoped: true }])[0].totalText).toBe('1.015');
  expect(compareSignedDecimals('9007199254740993', '9007199254740992')).toBe(1);
  expect(isValidEpochMs(NaN)).toBe(false);
  expect(isValidEpochMs(9e15)).toBe(false);
  expect(time(9e15)).toBe('--');
  expect(beijingDay(NaN)).toMatch(/^\d{4}-\d{2}-\d{2}$/);
});

it('normalizes positiveDecimalText with exact decimal parsing without float rounding', () => {
  expect(positiveDecimalText('00100.5000')).toBe('100.5');
  expect(positiveDecimalText('9007199254740993.123400')).toBe('9007199254740993.1234');
  expect(positiveDecimalText('0.000')).toBeNull();
  expect(positiveDecimalText('-1')).toBeNull();
  expect(positiveDecimalText('1e-5')).toBeNull();
});

it('rejects decimal strings exceeding 30 whole/fractional digits or 64 characters without throwing', () => {
  const valid30 = `${'9'.repeat(30)}.${'9'.repeat(30)}`;
  expect(parseUnsignedDecimal(valid30)).not.toBeNull();
  expect(parseSignedDecimal(`-${valid30}`)).not.toBeNull();
  expect(parseUnsignedDecimal(`${'0'.repeat(35)}1`)).toEqual({ n: 1n, s: 0 });
  for (const oversized of [
    '1'.repeat(31),
    `0.${'1'.repeat(31)}`,
    '9'.repeat(65),
    `-${'1'.repeat(31)}`,
    `-0.${'1'.repeat(31)}`,
    '1'.repeat(100_000),
  ]) {
    expect(parseUnsignedDecimal(oversized)).toBeNull();
    expect(parseSignedDecimal(oversized)).toBeNull();
    expect(quantity(oversized)).toBe('--');
  }
});

it('formats ageText across units and handles invalid or future timestamps', () => {
  const now = Date.parse('2026-01-02T00:00:00Z');
  const at = (seconds: number) => now - seconds * 1000;
  expect([0, 1, 2, 59, 60, 3600, 86400].map(seconds => ageText(at(seconds), now)))
    .toEqual(['刚刚', '刚刚', '2秒前', '59秒前', '1分钟前', '1小时前', '1天前']);
  expect(ageText(now + 2_000, now)).toBe('刚刚');
  expect(ageText(Date.parse('invalid'), now)).toBe('--');
});

describe('account and trade summaries', () => {
  it('calculates available ratio from wallet balance', () => {
    expect(availableRatio(80, 100)).toBe(80);
    expect(availableRatio(80, 0)).toBeNull();
    expect(availableRatio(null, 100)).toBeNull();
  });
  it('deduplicates shared paper and live balances without mixing their modes', () => {
    const paperA = snapshot('SOLUSDC'), paperB = snapshot('BTCUSDC');
    const liveA = snapshot('ETHUSDC', 'LIVE'), liveB = snapshot('XRPUSDC', 'LIVE');
    liveB.account.margin_balance = '200';
    liveB.account.available_balance = '175';
    liveB.account.total_wallet_balance = '250';
    liveB.account.unrealized_profit = '-50';
    liveB.account.update_time = new Date(Date.now() + 1000).toISOString();
    const groups = accountGroups([
      { snapshot: paperA, mode: 'PAPER', scoped: true }, { snapshot: paperB, mode: 'PAPER', scoped: true },
      { snapshot: liveA, mode: 'LIVE', scoped: true }, { snapshot: liveB, mode: 'LIVE', scoped: true },
    ]);
    expect(groups.find(g => g.mode === 'PAPER')).toMatchObject({ equity: 105, available: 80, wallet: 100, unrealized: 5 });
    expect(groups.find(g => g.mode === 'LIVE')).toMatchObject({ equity: 200, available: 175, wallet: 250, unrealized: -50 });
  });
  it('does not add capital or repeat shared pnl when a paper symbol is added', () => {
    const first = snapshot('BTCUSDC'), second = snapshot('ETHUSDC'), otherAsset = snapshot('BTCUSDT');
    first.account = { ...first.account, total_wallet_balance: '10020', margin_balance: '10035', available_balance: '9800', unrealized_profit: '15' };
    second.account = { ...first.account };
    first.position.size = '1';
    first.position.unrealized_pnl = '10';
    second.position.size = '-1';
    second.position.unrealized_pnl = '5';
    otherAsset.account.asset = 'USDT';
    const entry = (s: typeof first) => ({ snapshot: s, mode: s.mode, scoped: true });
    const initial = accountGroups([entry(first)])[0];
    const groups = accountGroups([entry(first), entry(second), entry(otherAsset)]);
    expect(groups.find(g => g.key === 'PAPER:USDC')).toMatchObject({ equity: initial.equity, wallet: initial.wallet, available: initial.available, unrealized: initial.unrealized, count: 2 });
    expect(groups.find(g => g.key === 'PAPER:USDC')).toMatchObject({ equity: 10035, wallet: 10020, available: 9800, unrealized: 15 });
    expect(groups.find(g => g.key === 'PAPER:USDT')).toMatchObject({ equity: 105, wallet: 100 });
  });
  it('takes the newest paper ledger even when flat observations are old', () => {
    const now = Date.now(), old = snapshot('BTCUSDC'), current = snapshot('ETHUSDC');
    old.account.update_time = new Date(now - 120_000).toISOString();
    current.account.update_time = new Date(now - 60_000).toISOString();
    current.account = { ...current.account, total_wallet_balance: '10020', margin_balance: '10020', available_balance: '10020', unrealized_profit: '0' };
    const entries = [old, current].map(s => ({ snapshot: s, mode: s.mode, scoped: true }));
    expect(accountGroups(entries, now)[0]).toMatchObject({ equity: 10020, wallet: 10020, available: 10020, unrealized: 0 });
    expect(accountGroups([...entries].reverse(), now)[0].equity).toBe(10020);
  });
  it('does not let a fresh paper symbol hide another stale open valuation or failed observation', () => {
    const now = Date.now(), old = snapshot('BTCUSDC'), current = snapshot('ETHUSDC');
    old.position.size = '1';
    old.account.update_time = new Date(now - 31_000).toISOString();
    const entries = [old, current].map(s => ({ snapshot: s, mode: s.mode, scoped: true }));
    expect(accountGroups(entries, now)[0]).toMatchObject({ equity: null, available: null, unrealized: null, wallet: 100 });
    old.account.update_time = new Date(now).toISOString();
    old.ticker.mark_update_time = new Date(now - 15_000).toISOString();
    expect(accountGroups(entries, now)[0].equity).toBeNull();
    old.ticker.mark_update_time = new Date(now).toISOString();
    expect(accountGroups(entries, now)[0].equity).toBe(105);
    expect(accountGroups([{ ...entries[0], account_observation_stale: true }, entries[1]], now)[0]).toMatchObject({ equity: null, wallet: null });
    old.account.asset = '';
    old.account.total_wallet_balance = '0';
    expect(accountGroups(entries, now)[0]).toMatchObject({ equity: null, available: null, wallet: null, unrealized: null });
  });
  it('hides stale shared valuations when the current symbol is flat', () => {
    const now = Date.now(), s = snapshot('BTCUSDC');
    s.position.size = '0';
    s.position.unrealized_pnl = '0';
    s.account = { ...s.account, total_wallet_balance: '10000', margin_balance: '10020', available_balance: '9800', unrealized_profit: '20', update_time: new Date(now - 15_000).toISOString() };
    const entries = [{ snapshot: s, mode: s.mode, scoped: true }];
    expect(accountGroups(entries, now)[0]).toMatchObject({ wallet: 10000, equity: null, available: null, unrealized: null });
    s.account.update_time = new Date(now - 14_999).toISOString();
    expect(accountGroups(entries, now)[0]).toMatchObject({ equity: 10020, available: 9800, unrealized: 20 });
  });
  it('does not display a stale live account or yesterday’s trade total', () => {
    const s = snapshot('SOLUSDC', 'LIVE');
    s.account.update_time = new Date(Date.now() - 60_000).toISOString();
    s.hourly_trade_stats!.window_end = new Date(Date.now() - 86_400_000).toISOString();
    expect(accountGroups([{ snapshot: s, mode: 'LIVE', scoped: true }])[0]).toMatchObject({ equity: null, wallet: null, unrealized: null });
    expect(todayCount(s)).toBeNull();
  });
  it('uses the newest valid observation of a shared live account, blanks stale observations, and isolates account_scope', () => {
    const now = Date.now(), expired = snapshot('SOLUSDC', 'LIVE'), older = snapshot('ETHUSDC', 'LIVE'), current = snapshot('BTCUSDC', 'LIVE');
    expired.account.update_time = new Date(now - 31_000).toISOString();
    expired.account.margin_balance = '999';
    older.account.update_time = new Date(now - 5_000).toISOString();
    older.account.margin_balance = '150';
    current.account.update_time = new Date(now - 1_000).toISOString();
    current.account.margin_balance = '200';
    const entries = [
      { snapshot: expired, mode: 'LIVE' as const, scoped: true },
      { snapshot: older, mode: 'LIVE' as const, scoped: true },
      { snapshot: current, mode: 'LIVE' as const, scoped: true },
    ];
    expect(accountGroups(entries, now)[0].equity).toBe(200);
    expect(accountGroups([{ ...entries[1], account_observation_stale: true }, entries[2]], now)[0]).toMatchObject({ equity: null, wallet: null });
    current.account.update_time = new Date(now - 31_000).toISOString();
    older.account.update_time = new Date(now - 31_000).toISOString();
    expect(accountGroups(entries, now)[0].equity).toBeNull();

    const scopeA = snapshot('SOLUSDC', 'LIVE'), scopeB = snapshot('BTCUSDC', 'LIVE');
    scopeA.account_scope = 'scope-a';
    scopeA.account = { ...scopeA.account, margin_balance: '120', update_time: new Date(now).toISOString() };
    scopeB.account_scope = 'scope-b';
    scopeB.account = { ...scopeB.account, margin_balance: '450', update_time: new Date(now).toISOString() };
    const scopedGroups = accountGroups([{ snapshot: scopeA, mode: 'LIVE' }, { snapshot: scopeB, mode: 'LIVE' }], now);
    expect(scopedGroups).toHaveLength(2);
    expect(scopedGroups.find(g => g.account_scope === 'scope-a')).toMatchObject({ key: 'LIVE:USDC', equity: 120 });
    expect(scopedGroups.find(g => g.account_scope === 'scope-b')).toMatchObject({ key: 'LIVE:USDC', equity: 450 });
  });
  it('honors dynamic sync_interval_secs and tolerates client/server clock skew on live accounts', () => {
    const now = 1_700_000_000_000;
    const slowSync = snapshot('SOLUSDC', 'LIVE');
    slowSync.sync_interval_secs = 60;
    slowSync.account.margin_balance = '320';
    slowSync.account.update_time = new Date(now - 45_000).toISOString();
    expect(accountGroups([{ snapshot: slowSync, mode: 'LIVE' }], now)[0].equity).toBe(320);
    expect(nextAccountExpiry([{ snapshot: slowSync, mode: 'LIVE' }], now)).toBe(now - 45_000 + 180_000 - 1);
    slowSync.account.update_time = new Date(now - 180_000).toISOString();
    expect(accountGroups([{ snapshot: slowSync, mode: 'LIVE' }], now)[0].equity).toBeNull();

    const serverNow = now - 20_000;
    const skewed = snapshot('BTCUSDC', 'LIVE');
    skewed.account.margin_balance = '500';
    skewed.account.update_time = new Date(serverNow - 15_000).toISOString();
    const skewedEntries = [{ snapshot: skewed, mode: 'LIVE' as const, generated_at_ms: serverNow, received_at_ms: now }];
    expect(accountGroups(skewedEntries, now)[0].equity).toBe(500);
    expect(nextAccountExpiry(skewedEntries, now)).toBe(now + 15_000 - 1);
    expect(accountGroups(skewedEntries, now + 15_000)[0].equity).toBeNull();
  });
  it('keeps a flat paper ledger but rejects stale open-position valuations', () => {
    const now = Date.now(), s = snapshot('SOLUSDC');
    const entries = [{ snapshot: s, mode: 'PAPER' as const, scoped: true }];
    s.position.unrealized_pnl = '0';
    s.account = { ...s.account, margin_balance: '100', available_balance: '100', unrealized_profit: '0' };
    s.account.update_time = new Date(now - 60_000).toISOString();
    s.ticker.mark_update_time = new Date(now - 60_000).toISOString();
    expect(accountGroups(entries, now)[0]).toMatchObject({ equity: 100, available: 100, wallet: 100 });
    expect(accountGroups([{ ...entries[0], account_observation_stale: true }], now)[0]).toMatchObject({ equity: null, available: null, wallet: null });
    s.position.size = '1';
    s.position.unrealized_pnl = '5';
    s.account = { ...s.account, margin_balance: '105', available_balance: '4', unrealized_profit: '5' };
    expect(accountGroups(entries, now)[0]).toMatchObject({ equity: null, available: null, unrealized: null, wallet: 100 });
    s.account.update_time = new Date(now).toISOString();
    expect(accountGroups(entries, now)[0].equity).toBeNull();
    s.ticker.mark_update_time = new Date(now).toISOString();
    expect(accountGroups(entries, now)[0].equity).toBe(105);
    s.account.update_time = new Date(now - 30_000).toISOString();
    expect(accountGroups(entries, now)[0].equity).toBeNull();
    s.account.update_time = new Date(now + 3_000).toISOString();
    s.ticker.mark_update_time = new Date(now + 3_000).toISOString();
    expect(accountGroups(entries, now)[0].equity).toBe(105);
    s.ticker.mark_update_time = new Date(now + 5_001).toISOString();
    expect(accountGroups(entries, now)[0].equity).toBeNull();
  });
});

describe('today realized pnl', () => {
  it('groups historical trades and hides incomplete totals', () => {
    const now = new Date().toISOString();
    const groups = todayPnlGroups({ window_start: now, window_end: now, groups: [
      { symbol: 'BTCUSDC', mode: 'PAPER', realized_pnl: '10.25', pending_pnl_count: 0 },
      { symbol: 'ETHUSDC', mode: 'PAPER', realized_pnl: '-3.5', pending_pnl_count: 2 },
      { symbol: 'SOLUSDC', mode: 'LIVE', realized_pnl: '-1', pending_pnl_count: 0 },
      { symbol: 'BTCUSDT', mode: 'PAPER', realized_pnl: '0', pending_pnl_count: 0 },
    ] })!;
    expect(groups.find(g => g.key === 'PAPER:USDC')).toMatchObject({ total: null, pending: 2 });
    expect(groups.find(g => g.key === 'LIVE:USDC')).toMatchObject({ total: -1, pending: 0 });
    expect(groups.find(g => g.key === 'PAPER:USDT')).toMatchObject({ total: 0 });
  });
  it('does not display missing or previous-day data', () => {
    const now = Date.parse('2026-01-01T16:00:00Z');
    expect(todayPnlGroups({ window_start: '', window_end: new Date(now - 1).toISOString(), groups: [] }, now)).toBeNull();
    expect(todayPnlGroups(null, now)).toBeNull();
  });
  it('eliminates float residue when summing realized pnl across trades and today groups', () => {
    const now = new Date().toISOString();
    const groups = todayPnlGroups({ window_start: now, window_end: now, groups: [
      { symbol: 'BTCUSDC', mode: 'PAPER', realized_pnl: '0.1', pending_pnl_count: 0 },
      { symbol: 'ETHUSDC', mode: 'PAPER', realized_pnl: '0.2', pending_pnl_count: 0 },
      { symbol: 'SOLUSDC', mode: 'PAPER', realized_pnl: '-0.3', pending_pnl_count: 0 },
    ] })!;
    expect(groups[0].total).toBe(0);
    const tradeMap = groupTradePnl([
      { trade_id: '1', symbol: 'BTCUSDC', mode: 'PAPER', side: 'BUY', price: '100', quantity: '1', amount_usdc: '100', realized_pnl: '0.1', commission: '0', pnl_verified: true, is_maker: true, timestamp: now, note: '' },
      { trade_id: '2', symbol: 'ETHUSDC', mode: 'PAPER', side: 'SELL', price: '100', quantity: '1', amount_usdc: '100', realized_pnl: '0.2', commission: '0', pnl_verified: true, is_maker: true, timestamp: now, note: '' },
      { trade_id: '3', symbol: 'SOLUSDC', mode: 'PAPER', side: 'SELL', price: '100', quantity: '1', amount_usdc: '100', realized_pnl: '-0.3', commission: '0', pnl_verified: true, is_maker: true, timestamp: now, note: '' },
    ]);
    expect(tradeMap.get('PAPER:USDC')?.total).toBe(0);
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
    const groups = positionGroups([long, short, live, otherAsset].map(s => ({ snapshot: s, mode: s.mode, scoped: true })));
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

it('describes data age without ever reading as the future', () => {
  const now = 1_000_000_000_000;
  expect(ageText(now + 40, now)).toBe('刚刚');
  expect(ageText(now - 1500, now)).toBe('刚刚');
  expect(ageText(now - 42_000, now)).toBe('42秒前');
  expect(ageText(now - 125_000, now)).toBe('2分钟前');
});

it('evaluates mark freshness and exact expiry boundaries across clock skew', () => {
  const now = 1_700_000_000_000;
  const s = snapshot('SOLUSDC');
  s.ticker.mark_update_time = new Date(now - 14_999).toISOString();
  expect(isEntryMarkFresh({ snapshot: s, mode: 'PAPER' }, now)).toBe(true);
  expect(entryMarkExpiry({ snapshot: s, mode: 'PAPER' }, now)).toBe(now);
  expect(isEntryMarkFresh({ snapshot: s, mode: 'PAPER' }, now + 1)).toBe(false);
  expect(isEntryMarkFresh({ snapshot: s, mode: 'PAPER', account_observation_stale: true }, now)).toBe(false);

  s.ticker.mark_update_time = new Date(now + 6_000).toISOString();
  expect(isEntryMarkFresh({ snapshot: s, mode: 'PAPER' }, now)).toBe(false);
  expect(entryMarkExpiry({ snapshot: s, mode: 'PAPER' }, now)).toBe(now + 1_000 - 1);
  expect(isEntryMarkFresh({ snapshot: s, mode: 'PAPER' }, now + 1_000)).toBe(true);

  const serverNow = now - 20_000;
  s.ticker.mark_update_time = new Date(serverNow - 5_000).toISOString();
  const skewed = { snapshot: s, mode: 'LIVE' as const, generated_at_ms: serverNow, received_at_ms: now };
  expect(isEntryMarkFresh(skewed, now)).toBe(true);
  expect(entryMarkExpiry(skewed, now)).toBe(now + 10_000 - 1);
  expect(isEntryMarkFresh(skewed, now + 10_000)).toBe(false);
});
