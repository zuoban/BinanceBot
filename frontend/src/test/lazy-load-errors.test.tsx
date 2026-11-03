import { Suspense, lazy } from 'react';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { App } from 'antd';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, expect, it, vi } from 'vitest';
import { LoadErrorBoundary } from '../components/LoadErrorBoundary';
import { OverviewHistory } from '../components/OverviewHistory';
import Settings from '../pages/Settings';

vi.mock('../components/OverviewHistoryChart', () => { throw new Error('chart module unavailable'); });
vi.mock('../components/DatabaseSettings', () => { throw new Error('database module unavailable'); });
vi.mock('../lib/queries', () => ({
  useOverviewHistory: () => ({ data: [{ hour: Math.floor(Date.now() / 3600000) * 3600, groups: [{ mode: 'PAPER', asset: 'USDC', equity: '100' }] }], isPending: false, error: null }),
}));
vi.mock('../lib/api', () => ({ api: vi.fn().mockResolvedValue({}), post: vi.fn() }));

beforeEach(() => {
  vi.stubGlobal('matchMedia', vi.fn().mockImplementation(query => ({ matches: false, media: query, addListener: vi.fn(), removeListener: vi.fn(), addEventListener: vi.fn(), removeEventListener: vi.fn() })));
  vi.stubGlobal('ResizeObserver', class { observe() {} unobserve() {} disconnect() {} });
  vi.spyOn(console, 'error').mockImplementation(() => {});
});

it('keeps the history card usable when the chart module fails', async () => {
  render(<><button>其他功能</button><OverviewHistory /></>);
  expect(await screen.findByText('权益图加载失败，请刷新页面重试。')).toBeInTheDocument();
  expect(screen.getByRole('button', { name: '其他功能' })).toBeEnabled();
  expect(screen.getByText(/单位 USDC/)).toBeInTheDocument();
  expect(screen.getByRole('button', { name: '刷新页面' })).toBeEnabled();
});

it('keeps the settings tabs usable when the database module fails', async () => {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(<QueryClientProvider client={client}><App><Settings /></App></QueryClientProvider>);
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
