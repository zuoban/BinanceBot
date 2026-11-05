import { createElement } from 'react';
import { render, screen } from '@testing-library/react';
import { expect, it } from 'vitest';
import { DepthChart, todayHourlyData } from '../components/Charts';
import { snapshot } from './fixtures';

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

it('renders live grid depth levels with percentage distance from mark price', () => {
  const s = snapshot('SOLUSDC');
  s.ticker.mark_price = '100';
  s.active_orders = [
    { client_order_id: 's1', order_id: 1, symbol: 'SOLUSDC', side: 'SELL', price: '101.5', quantity: '1', amount_usdc: '101.5', status: 'NEW', created_at: '2026-11-05T00:00:00Z', grid_level: 1, is_take_profit: false, purpose: 'GRID', merge_sources: [] },
    { client_order_id: 'b1', order_id: 2, symbol: 'SOLUSDC', side: 'BUY', price: '98.5', quantity: '1', amount_usdc: '98.5', status: 'NEW', created_at: '2026-11-05T00:00:00Z', grid_level: -1, is_take_profit: false, purpose: 'GRID', merge_sources: [] },
  ];
  render(createElement(DepthChart, { snapshot: s }));
  expect(screen.getByText('+1.5%')).toBeInTheDocument();
  expect(screen.getByText('-1.5%')).toBeInTheDocument();
});
