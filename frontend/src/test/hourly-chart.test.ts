import { expect, it } from 'vitest';
import { todayHourlyData } from '../components/Charts';

it('shows all Beijing hours and leaves future hours without counts', () => {
  const data = todayHourlyData({
    window_start: '2026-10-30T16:00:00Z',
    window_end: '2026-10-30T17:30:00Z',
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
