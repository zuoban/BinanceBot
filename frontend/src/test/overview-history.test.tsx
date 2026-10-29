import { render, screen } from '@testing-library/react';
import { beforeEach, expect, it, vi } from 'vitest';
import { OverviewHistory } from '../components/OverviewHistory';
import type { HourlySnapshot } from '../lib/types';

const history = vi.hoisted(() => ({ data: [] as HourlySnapshot[], isPending: false, error: null }));
const chart = vi.hoisted(() => ({ data: [] as { hour: number; equity: number | null; long: number | null; short: number | null }[] }));
vi.mock('../lib/queries', () => ({ useOverviewHistory: () => history }));
vi.mock('recharts', () => ({
  ResponsiveContainer: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
  LineChart: ({ data }: { data: typeof chart.data }) => { chart.data = data; return <div />; },
  CartesianGrid: () => null, Legend: () => null, Line: () => null, Tooltip: () => null, XAxis: () => null, YAxis: () => null,
}));
beforeEach(() => {
  vi.stubGlobal('matchMedia', vi.fn().mockImplementation(query => ({ matches: false, media: query, addListener: vi.fn(), removeListener: vi.fn(), addEventListener: vi.fn(), removeEventListener: vi.fn() })));
});

it('shows a meaningful empty state before the first snapshot', () => {
  history.data = [];
  render(<OverviewHistory />);
  expect(screen.getByText('暂无快照数据，整点后开始记录')).toBeInTheDocument();
});

it('keeps missing hours and unavailable metrics as gaps', () => {
  const hour = Math.floor(Date.now() / 3600_000) * 3600;
  history.data = [{ hour: hour - 3600, groups: [{ mode: 'PAPER', asset: 'USDC', equity: '100', long: null, short: '0' }] }];
  render(<OverviewHistory />);
  expect(chart.data).toHaveLength(24);
  expect(chart.data[22]).toMatchObject({ equity: 100, long: null, short: 0 });
  expect(chart.data[23]).toMatchObject({ equity: null, long: null, short: null });
});
