import { createElement } from 'react';
import { render, screen } from '@testing-library/react';
import { expect, it, vi } from 'vitest';
import { DepthChart, HourlyChart, todayHourlyData } from '../components/Charts';
import { resolveTrades } from '../pages/PairDetail';
import { snapshot, trade } from './fixtures';

const chartRender = vi.hoisted(() => vi.fn());
vi.mock('recharts', async importOriginal => ({
  ...await importOriginal<typeof import('recharts')>(),
  ResponsiveContainer: ({ children }: { children: React.ReactNode }) => children,
  BarChart: ({ data }: { data: unknown }) => { chartRender(data); return null; },
}));

it('shows all Beijing hours and leaves future hours without counts', () => {
  const data = todayHourlyData({
    window_start: '2026-10-30T16:00:00Z',
    window_end: '2026-10-30T17:30:00Z',
    realized_pnl: '0', pending_pnl_count: 0,
    buckets: [
      { hour_start: '2026-10-30T16:00:00Z', buy_count: 0, sell_count: 0 },
      { hour_start: '2026-10-30T17:00:00Z', buy_count: 2, sell_count: 1 },
    ],
  }, Date.parse('2026-10-30T17:30:00Z'));
  expect(data).toHaveLength(24);
  expect(data[0]).toEqual({ hour: '01:00', 买入: 2, 卖出: 1 });
  expect(data[1]).toEqual({ hour: '00:00', 买入: 0, 卖出: 0 });
  expect(data[2]).toEqual({ hour: '23:00', 买入: null, 卖出: null });
  expect(data[23]).toEqual({ hour: '02:00', 买入: null, 卖出: null });
});

it('skips hourly chart work on clock and ticker changes but refreshes for a new hour, new counts and a new day', () => {
  const now = Date.parse('2026-11-05T00:59:59Z');
  const s = snapshot();
  s.hourly_trade_stats = {
    window_start: '2026-11-04T16:00:00Z', window_end: new Date(now).toISOString(), realized_pnl: '0', pending_pnl_count: 0,
    buckets: [{ hour_start: '2026-11-05T00:00:00Z', buy_count: 2, sell_count: 3 }],
  };
  chartRender.mockClear();
  const { rerender } = render(createElement(HourlyChart, { snapshot: s, now }));
  const initialRenders = chartRender.mock.calls.length;
  expect(initialRenders).toBeGreaterThan(0);
  expect(chartRender.mock.calls.at(-1)![0][0]).toEqual({ hour: '08:00', 买入: 2, 卖出: 3 });
  const priceChange = { ...s, ticker: { ...s.ticker, mark_price: '102' } };
  rerender(createElement(HourlyChart, { snapshot: priceChange, now: now + 500 }));
  expect(chartRender).toHaveBeenCalledTimes(initialRenders);
  rerender(createElement(HourlyChart, { snapshot: priceChange, now: now + 1000 }));
  expect(chartRender).toHaveBeenCalledTimes(initialRenders + 1);
  expect(chartRender.mock.calls.at(-1)![0][0]).toEqual({ hour: '09:00', 买入: null, 卖出: null });
  const countChange = { ...priceChange, hourly_trade_stats: { ...s.hourly_trade_stats, buckets: [{ hour_start: '2026-11-05T00:00:00Z', buy_count: 4, sell_count: 3 }] } };
  rerender(createElement(HourlyChart, { snapshot: countChange, now: now + 2000 }));
  expect(chartRender).toHaveBeenCalledTimes(initialRenders + 2);
  expect(chartRender.mock.calls.at(-1)![0][1]).toEqual({ hour: '08:00', 买入: 4, 卖出: 3 });
  rerender(createElement(HourlyChart, { snapshot: countChange, now: Date.parse('2026-11-05T16:00:00Z') }));
  expect(screen.queryByRole('region', { name: '北京时间今日每小时成交笔数' })).not.toBeInTheDocument();
});

it('renders live grid depth levels with percentage distance from mark price', () => {
  const s = snapshot('SOLUSDC');
  s.ticker.mark_price = '100';
  s.active_orders = [
    { client_order_id: 's1', order_id: 1, symbol: 'SOLUSDC', side: 'SELL', price: '101.5', quantity: '1', amount_usdc: '101.5', status: 'NEW', created_at: '2026-11-05T00:00:00Z', grid_level: 1, is_take_profit: false, purpose: 'GRID', merge_sources: [] },
    { client_order_id: 'b1', order_id: 2, symbol: 'SOLUSDC', side: 'BUY', price: '98.5', quantity: '1', amount_usdc: '98.5', status: 'NEW', created_at: '2026-11-05T00:00:00Z', grid_level: -1, is_take_profit: false, purpose: 'GRID', merge_sources: [] },
  ];
  const { rerender } = render(createElement(DepthChart, { snapshot: s }));
  expect(screen.getByText('买单 1 笔 · 卖单 1 笔')).toBeInTheDocument();
  expect(screen.getByText('+1.5%')).toBeInTheDocument();
  expect(screen.getByText('-1.5%')).toBeInTheDocument();
  const levels = () => screen.getByRole('region', { name: '按价格从高到低排列的实时网格挂单' }).children;
  expect(levels()[1]).toHaveClass('grid-level-mark');
  rerender(createElement(DepthChart, { snapshot: { ...s, ticker: { ...s.ticker, mark_price: '103' } } }));
  expect(levels()[0]).toHaveClass('grid-level-mark');
  expect(levels()[1]).toHaveClass('grid-level-sell');
  expect(levels()[2]).toHaveClass('grid-level-buy');
  rerender(createElement(DepthChart, { snapshot: { ...s, ticker: { ...s.ticker, mark_price: '0' } } }));
  expect(levels()).toHaveLength(2);
  expect(s.active_orders.map(order => order.client_order_id)).toEqual(['s1', 'b1']);
});

it('guards todayHourlyData against invalid timestamps and sorts resolveTrades transitively across NaN timestamps', () => {
  expect(todayHourlyData({ window_start: 'invalid', window_end: 'invalid', realized_pnl: '0', pending_pnl_count: 0, buckets: [] }, Date.now())).toEqual([]);
  expect(todayHourlyData({ window_start: '2026-10-30T16:00:00Z', window_end: '2026-10-30T17:00:00Z', realized_pnl: '0', pending_pnl_count: 0, buckets: [] }, NaN)).toEqual([]);
  const s = snapshot('SOLUSDC');
  s.stats.total_trades = 3;
  const fetched = [
    trade({ trade_id: 'b', symbol: 'SOLUSDC', timestamp: 'not-a-date' }),
    trade({ trade_id: 'a', symbol: 'SOLUSDC', timestamp: '2026-11-05T01:00:00Z' }),
    trade({ trade_id: 'c', symbol: 'SOLUSDC', timestamp: '2026-11-05T02:00:00Z' }),
  ];
  expect(resolveTrades(s, fetched, false, 200, 100).map(t => t.trade_id)).toEqual(['c', 'a', 'b']);
});
