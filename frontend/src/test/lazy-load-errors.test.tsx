import { Suspense, lazy } from 'react';
import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { App } from 'antd';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, expect, it, vi } from 'vitest';
import type * as Api from '../lib/api';
import type * as Queries from '../lib/queries';
import { DepthChart, HourlyChart } from '../components/Charts';
import { ControlProvider } from '../components/Controls';
import { LoadErrorBoundary } from '../components/LoadErrorBoundary';
import { OverviewHistory } from '../components/OverviewHistory';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import PairDetail from '../pages/PairDetail';
import Settings from '../pages/Settings';
import { pair, snapshot } from './fixtures';

vi.mock('../components/OverviewHistoryChart', () => { throw new Error('chart module unavailable'); });
vi.mock('../components/HourlyChartView', () => { throw new Error('hourly chart module unavailable'); });
vi.mock('../components/DatabaseSettings', () => { throw new Error('database module unavailable'); });
vi.mock('../components/DataTables', () => { throw new Error('data tables module unavailable'); });
vi.mock('../lib/queries', async importOriginal => {
  const actual = await importOriginal<typeof Queries>();
  return {
    ...actual,
    useOverviewHistory: () => ({ data: { snapshots: [{ hour: Math.floor(Date.now() / 3600000) * 3600, groups: [{ mode: 'PAPER', asset: 'USDC', account_id: null, equity: '100' }] }], current_accounts: [{ mode: 'PAPER', asset: 'USDC', account_id: null }] }, isPending: false, error: null }),
    usePairs: () => ({ data: [pair('SOLUSDC')], error: null, isPending: false, isFetching: false, refetch: vi.fn() }),
    usePairSnapshot: () => ({ data: snapshot('SOLUSDC'), dataUpdatedAt: Date.now(), error: null, live: true, stale: false, isPending: false, refetch: vi.fn() }),
  };
});
vi.mock('../lib/api', async importOriginal => {
  const actual = await importOriginal<typeof Api>();
  return { ...actual, api: vi.fn().mockResolvedValue({}), post: vi.fn() };
});

beforeEach(() => {
  vi.spyOn(console, 'error').mockImplementation(() => {});
});

it('offers recovery for both history charts when the module fails at Beijing midnight', async () => {
  const realNow = Date.now;
  const offset = Date.UTC(2026, 0, 1, 16, 30) - realNow();
  vi.spyOn(Date, 'now').mockImplementation(() => realNow() + offset);
  render(<><button>其他功能</button><OverviewHistory /></>);
  await waitFor(() => expect(screen.getAllByRole('alert')).toHaveLength(2));
  expect(screen.getByRole('button', { name: '其他功能' })).toBeEnabled();
  for (const alert of screen.getAllByRole('alert')) expect(within(alert).getByRole('button', { name: '刷新页面' })).toBeEnabled();
});

it('keeps the settings tabs usable when the database module fails', async () => {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(<QueryClientProvider client={client}><MemoryRouter><App><Settings /></App></MemoryRouter></QueryClientProvider>);
  const user = userEvent.setup();
  await user.click(screen.getByRole('tab', { name: '数据库操作' }));
  expect(await screen.findByText('数据库操作加载失败，请刷新页面重试。')).toBeInTheDocument();
  await user.click(screen.getByRole('tab', { name: '账户安全' }));
  expect(await screen.findByText('管理员密码')).toBeInTheDocument();
});

it('offers the supplied recovery action after a lazy import fails', async () => {
  const reload = vi.fn();
  const Broken = lazy(() => Promise.reject(new Error('module unavailable')));
  render(<LoadErrorBoundary label="面板" onReload={reload}><Suspense fallback="加载中"><Broken /></Suspense></LoadErrorBoundary>);
  await userEvent.click(await screen.findByRole('button', { name: '刷新页面' }));
  expect(reload).toHaveBeenCalledOnce();
});

it('keeps depth levels interactive when the lazy hourly chart chunk fails to load', async () => {
  const now = Date.parse('2026-11-05T00:30:00Z');
  const s = snapshot('SOLUSDC');
  s.hourly_trade_stats = {
    window_start: '2026-11-04T16:00:00Z',
    window_end: new Date(now).toISOString(),
    realized_pnl: '0',
    pending_pnl_count: 0,
    buckets: [{ hour_start: '2026-11-05T00:00:00Z', buy_count: 1, sell_count: 2 }],
  };
  s.active_orders = [
    { client_order_id: 'b1', order_id: 1, symbol: 'SOLUSDC', side: 'BUY', price: '98.5', quantity: '1', amount_usdc: '98.5', status: 'NEW', created_at: '2026-11-05T00:00:00Z', grid_level: -1, is_take_profit: false, purpose: 'GRID', merge_sources: [] },
  ];
  render(<><HourlyChart snapshot={s} now={now} /><DepthChart snapshot={s} /></>);
  expect(await screen.findByText('今日成交图加载失败，请刷新页面重试。')).toBeInTheDocument();
  expect(screen.getByRole('button', { name: '刷新页面' })).toBeEnabled();
  expect(screen.getByRole('region', { name: '按价格从高到低排列的实时网格挂单' })).toBeInTheDocument();
});

it('isolates DataTables chunk failures on PairDetail tabs while keeping monitor and history actions usable', async () => {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const user = userEvent.setup();
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={['/pairs/SOLUSDC']}>
        <App>
          <ControlProvider>
            <Routes><Route path="/pairs/:symbol" element={<PairDetail />} /></Routes>
          </ControlProvider>
        </App>
      </MemoryRouter>
    </QueryClientProvider>,
  );
  expect(screen.getByText('仓位与策略明细')).toBeInTheDocument();

  await user.click(screen.getByRole('tab', { name: /挂单/ }));
  expect(await screen.findByText('挂单列表加载失败，请刷新页面重试。')).toBeInTheDocument();

  await user.click(screen.getByRole('tab', { name: '成交历史' }));
  expect(await screen.findByText('成交历史加载失败，请刷新页面重试。')).toBeInTheDocument();
  expect(screen.getByRole('button', { name: /导出当前币种分析快照/ })).toBeEnabled();

  await user.click(screen.getByRole('tab', { name: '实时监控' }));
  expect(screen.getByText('仓位与策略明细')).toBeInTheDocument();
});
