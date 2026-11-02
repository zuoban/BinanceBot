import { render, screen, waitFor } from '@testing-library/react';
import { beforeEach, expect, it, vi } from 'vitest';
import { OverviewHistory } from '../components/OverviewHistory';
import type { HourlySnapshot } from '../lib/types';
import '../components/OverviewHistoryChart';

const history = vi.hoisted(() => ({ data: [] as HourlySnapshot[], isPending: false, error: null }));
const chart = vi.hoisted(() => ({ data: [] as { hour: number; equity: number | null }[] }));
vi.mock('../lib/queries', () => ({ useOverviewHistory: () => history }));
vi.mock('recharts', () => ({
  ResponsiveContainer: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
  LineChart: ({ data }: { data: typeof chart.data }) => { chart.data = data; return <div />; },
  CartesianGrid: () => null, Legend: () => null, Line: () => null, Tooltip: () => null, XAxis: () => null, YAxis: () => null,
}));
beforeEach(() => {
  history.data = [];
  chart.data = [];
  vi.stubGlobal('matchMedia', vi.fn().mockImplementation(query => ({ matches: false, media: query, addListener: vi.fn(), removeListener: vi.fn(), addEventListener: vi.fn(), removeEventListener: vi.fn() })));
});

it('shows a meaningful empty state before the first snapshot', () => {
  history.data = [];
  render(<OverviewHistory />);
  expect(screen.getByText('暂无快照数据，整点后开始记录')).toBeInTheDocument();
});

it('keeps missing hours and unavailable metrics as gaps', async () => {
  const hour = 1_800_000_000 - 1_800_000_000 % 3600;
  vi.spyOn(Date, 'now').mockReturnValue(hour * 1000);
  history.data = [{ hour: hour - 3600, groups: [{ mode: 'PAPER', asset: 'USDC', equity: '100', long: null, short: '0' }] }];
  render(<OverviewHistory />);
  expect(screen.getByRole('heading', { name: '账户权益趋势' })).toBeInTheDocument();
  expect(screen.queryByText('做多持仓')).not.toBeInTheDocument();
  expect(screen.queryByText('做空持仓')).not.toBeInTheDocument();
  await waitFor(() => expect(chart.data).toHaveLength(24), { timeout: 10_000 });
  expect(chart.data[0]).toMatchObject({ hour: hour - 23 * 3600, equity: null });
  expect(chart.data[22]).toMatchObject({ hour: hour - 3600, equity: 100 });
  expect(chart.data[23]).toMatchObject({ hour, equity: null });
  expect(chart.data.filter(point => point.equity !== null)).toHaveLength(1);
});
