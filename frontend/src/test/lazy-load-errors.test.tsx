import { Suspense, lazy } from 'react';
import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { App } from 'antd';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, expect, it, vi } from 'vitest';
import type * as Api from '../lib/api';
import type * as Queries from '../lib/queries';
import { LoadErrorBoundary } from '../components/LoadErrorBoundary';
import { OverviewHistory } from '../components/OverviewHistory';
import { MemoryRouter } from 'react-router-dom';
import Settings from '../pages/Settings';

vi.mock('../components/OverviewHistoryChart', () => { throw new Error('chart module unavailable'); });
vi.mock('../components/DatabaseSettings', () => { throw new Error('database module unavailable'); });
vi.mock('../lib/queries', async importOriginal => {
  const actual = await importOriginal<typeof Queries>();
  return { ...actual, useOverviewHistory: () => ({ data: { snapshots: [{ hour: Math.floor(Date.now() / 3600000) * 3600, groups: [{ mode: 'PAPER', asset: 'USDC', account_id: null, equity: '100' }] }], current_accounts: [{ mode: 'PAPER', asset: 'USDC', account_id: null }] }, isPending: false, error: null }) };
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
