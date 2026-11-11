import { useSyncExternalStore } from 'react';
import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { createMemoryRouter, MemoryRouter, RouterProvider } from 'react-router-dom';
import { beforeEach, expect, it, vi } from 'vitest';
import Shell from '../components/Shell';
import { StrategyToggle, strategyActionGuard } from '../components/StrategyMenu';
import PairConfig from '../pages/PairConfig';
import PairDetail from '../pages/PairDetail';
import type * as Antd from 'antd';
import type * as Api from '../lib/api';
import type * as Queries from '../lib/queries';
import { PAIRS_STALE_MS, SNAPSHOT_STALE_MS } from '../lib/portfolio';
import { pair } from './fixtures';

const mocks = vi.hoisted(() => {
  const confirm = vi.fn(), success = vi.fn(), error = vi.fn();
  return { confirm, success, error, run: vi.fn(), api: vi.fn(), post: vi.fn(), busy: new Set<string>(), app: { modal: { confirm }, message: { success, error } } };
});
vi.mock('antd', async importOriginal => {
  const antd = await importOriginal<typeof Antd>();
  return { ...antd, App: { useApp: () => mocks.app } };
});
vi.mock('../lib/api', async importOriginal => {
  const actual = await importOriginal<typeof Api>();
  return { ...actual, api: mocks.api, post: mocks.post };
});
const pairsState = vi.hoisted(() => {
  const listeners = new Set<() => void>();
  return {
    dataUpdatedAt: Date.now(),
    setUpdatedAt(next: number) { this.dataUpdatedAt = next; for (const listener of listeners) listener(); },
    subscribe: (listener: () => void) => { listeners.add(listener); return () => listeners.delete(listener); },
  };
});
vi.mock('../lib/queries', async importOriginal => {
  const actual = await importOriginal<typeof Queries>();
  return {
    ...actual,
    usePairs: () => ({ data: [pair('BTCUSDC'), pair('ETHUSDC')], isPending: false, isFetching: false, error: null, dataUpdatedAt: useSyncExternalStore(pairsState.subscribe, () => pairsState.dataUpdatedAt) }),
    useLiveAccounts: () => ({ accounts: [], stale: false, error: null }),
    usePairSnapshot: () => ({ data: undefined, isPending: false, refetch: vi.fn() }),
  };
});
vi.mock('../components/Controls', () => ({ ControlProvider: ({ children }: { children: React.ReactNode }) => children, PairControls: () => null, useControls: () => ({ busy: mocks.busy, pending: {}, results: {}, clearResult: vi.fn(), run: mocks.run }) }));
vi.mock('../components/ThemeSwitch', () => ({ ThemeSwitch: () => null, ThemeSegmented: () => null, themeLabels: { system: '跟随系统', light: '浅色', dark: '深色' } }));

beforeEach(() => {
  vi.clearAllMocks();
  mocks.busy = new Set<string>();
  pairsState.setUpdatedAt(Date.now());
});

function setup(initialPath = '/pairs/BTCUSDC/config', configError?: Error) {
  mocks.api.mockImplementation((path: string) => path.startsWith('/api/exchange/symbols?')
    ? Promise.resolve(['BTCUSDC', 'SOLUSDC'])
    : configError ? Promise.reject(configError) : Promise.resolve({ ...pair('BTCUSDC').grid_config, post_only: true, config_version: 'version-1' }));
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const router = createMemoryRouter([{ element: <Shell />, children: [
    { path: '/', element: <div>总览页面</div> },
    { path: '/settings', element: <div>设置页面</div> },
    { path: '/pairs/new', element: <PairConfig /> },
    { path: '/pairs/:symbol', element: <PairDetail /> },
    { path: '/pairs/:symbol/config', element: <PairConfig /> },
  ] }], { initialEntries: [initialPath] });
  render(<QueryClientProvider client={client}><RouterProvider router={router} /></QueryClientProvider>);
  return router;
}

function breadcrumb() {
  return within(screen.getByRole('navigation', { name: '面包屑' }));
}

function expectBreadcrumb(items: { label: string; href?: string }[]) {
  const nav = breadcrumb();
  const entries = nav.getAllByRole('listitem');
  expect(entries.map(entry => entry.textContent?.replace(/^\//, ''))).toEqual(items.map(item => item.label));
  items.forEach(({ label, href }, index) => {
    if (href) expect(within(entries[index]).getByRole('link', { name: label })).toHaveAttribute('href', href);
    else {
      expect(within(entries[index]).queryByRole('link')).not.toBeInTheDocument();
      expect(within(entries[index]).getByText(label)).toHaveAttribute('aria-current', 'page');
    }
  });
}

it.each([
  ['/pairs/new', [{ label: '策略总览', href: '/' }, { label: '新增交易对' }]],
  ['/pairs/BTCUSDC', [{ label: '策略总览', href: '/' }, { label: 'BTCUSDC' }]],
  ['/pairs/BTCUSDC/config', [{ label: '策略总览', href: '/' }, { label: 'BTCUSDC', href: '/pairs/BTCUSDC' }, { label: '策略参数' }]],
])('shows the breadcrumb hierarchy for %s', (path, items) => {
  setup(path);
  expectBreadcrumb(items);
});

it.each(['/pairs/UNKNOWN', '/pairs/UNKNOWN/config'])('keeps the breadcrumb available for a missing pair at %s', path => {
  setup(path);
  expect(screen.getByText('该交易对不存在或已移除')).toBeInTheDocument();
  expectBreadcrumb(path.endsWith('/config')
    ? [{ label: '策略总览', href: '/' }, { label: 'UNKNOWN', href: '/pairs/UNKNOWN' }, { label: '策略参数' }]
    : [{ label: '策略总览', href: '/' }, { label: 'UNKNOWN' }]);
});

it('keeps the breadcrumb available when configuration loading fails', async () => {
  setup('/pairs/BTCUSDC/config', new Error('配置加载失败'));
  await screen.findByText('配置加载失败');
  expectBreadcrumb([{ label: '策略总览', href: '/' }, { label: 'BTCUSDC', href: '/pairs/BTCUSDC' }, { label: '策略参数' }]);
});

it.each(['/pairs/BTCUSDC/config', '/pairs/new'])('confirms before leaving a dirty form for the overview at %s', async path => {
  mocks.confirm.mockResolvedValueOnce(false).mockResolvedValueOnce(true);
  const router = setup(path);
  const user = userEvent.setup();
  const input = await screen.findByLabelText('网格间距');
  await waitFor(() => expect(input).toBeEnabled());
  await user.clear(input);
  await user.type(input, '0.2');
  await user.click(breadcrumb().getByRole('link', { name: '策略总览' }));
  await waitFor(() => expect(mocks.confirm).toHaveBeenCalledTimes(1));
  expect(router.state.location.pathname).toBe(path);
  expect(input).toHaveValue('0.2');
  await user.click(breadcrumb().getByRole('link', { name: '策略总览' }));
  await screen.findByText('总览页面');
  expect(router.state.location.pathname).toBe('/');
  expect(mocks.confirm).toHaveBeenCalledTimes(2);
});

it('returns to the strategy detail page and confirms before returning with unsaved changes', async () => {
  const firstRouter = setup();
  const user = userEvent.setup();
  await user.click(breadcrumb().getByRole('link', { name: 'BTCUSDC' }));
  expect(await screen.findByRole('heading', { name: 'BTCUSDC' })).toBeInTheDocument();
  expect(firstRouter.state.location.pathname).toBe('/pairs/BTCUSDC');
  await act(async () => { await firstRouter.navigate('/pairs/BTCUSDC/config'); });

  mocks.confirm.mockResolvedValueOnce(false).mockResolvedValueOnce(true);
  const input = await screen.findByLabelText('网格间距');
  await waitFor(() => expect(input).toBeEnabled());
  await user.clear(input);
  await user.type(input, '0.2');
  await user.click(breadcrumb().getByRole('link', { name: 'BTCUSDC' }));
  await waitFor(() => expect(mocks.confirm).toHaveBeenCalledTimes(1));
  expect(firstRouter.state.location.pathname).toBe('/pairs/BTCUSDC/config');
  await user.click(breadcrumb().getByRole('link', { name: 'BTCUSDC' }));
  expect(await screen.findByRole('heading', { name: 'BTCUSDC' })).toBeInTheDocument();
  expect(firstRouter.state.location.pathname).toBe('/pairs/BTCUSDC');
});

it('lists the pairs in the sidebar and keeps the config page when switching pairs', () => {
  setup();
  const sidebar = within(screen.getByRole('complementary'));
  expect(sidebar.getByRole('link', { name: '策略总览' })).toHaveAttribute('href', '/');
  expect(sidebar.getByRole('link', { name: '全局设置' })).toHaveAttribute('href', '/settings');
  const pairs = sidebar.getByRole('navigation', { name: '交易对' });
  expect(within(pairs).getByRole('link', { name: /BTCUSDC/ })).toHaveAttribute('aria-current', 'page');
  expect(within(pairs).getByRole('link', { name: /ETHUSDC/ })).not.toHaveAttribute('aria-current');
  expect(within(pairs).getByRole('link', { name: /ETHUSDC/ })).toHaveAttribute('href', '/pairs/ETHUSDC/config');
});

it('runs a pair action from the palette through the guarded control flow', async () => {
  setup();
  const user = userEvent.setup();
  await user.keyboard('{Control>}k{/Control}');
  const search = await screen.findByRole('combobox', { name: '搜索命令' });
  await user.type(search, '恢复 ETHUSDC');
  await user.keyboard('{Enter}');
  expect(mocks.run).toHaveBeenCalledWith(expect.objectContaining({ symbol: 'ETHUSDC' }), 'resume');
});

it('keeps CommandPalette resume actions enabled across 10s poll intervals until PAIRS_STALE_MS elapses', () => {
  vi.useFakeTimers();
  try {
    pairsState.setUpdatedAt(Date.now());
    setup('/');
    act(() => { vi.advanceTimersByTime(SNAPSHOT_STALE_MS + 2_000); });
    fireEvent.keyDown(window, { key: 'k', ctrlKey: true });
    const search = screen.getByRole('combobox', { name: '搜索命令' });
    fireEvent.change(search, { target: { value: '恢复 ETHUSDC' } });
    expect(screen.getByRole('option', { name: /恢复 ETHUSDC/ })).toBeInTheDocument();

    act(() => { vi.advanceTimersByTime(PAIRS_STALE_MS); });
    expect(screen.queryByRole('option', { name: /恢复 ETHUSDC/ })).not.toBeInTheDocument();
  } finally {
    vi.useRealTimers();
  }
});

it('jumps between pages and pairs with keyboard shortcuts but not while typing', async () => {
  const router = setup();
  const user = userEvent.setup();
  await user.keyboard('gs');
  await waitFor(() => expect(router.state.location.pathname).toBe('/settings'));
  await act(async () => { await router.navigate('/pairs/BTCUSDC'); });
  await user.keyboard('{]}');
  await waitFor(() => expect(router.state.location.pathname).toBe('/pairs/ETHUSDC'));
  await user.keyboard('{[}');
  await waitFor(() => expect(router.state.location.pathname).toBe('/pairs/BTCUSDC'));
  await act(async () => { await router.navigate('/pairs/BTCUSDC/config'); });
  const input = await screen.findByLabelText('网格间距');
  await user.click(input);
  await user.keyboard('{]}');
  expect(router.state.location.pathname).toBe('/pairs/BTCUSDC/config');
});

it('does not run a palette command when Enter commits an IME candidate and switches pairs on Enter', async () => {
  const router = setup();
  const user = userEvent.setup();
  await user.keyboard('{Control>}k{/Control}');
  const search = await screen.findByRole('combobox', { name: '搜索命令' });
  await user.type(search, 'ETHUSDC');
  const options = screen.getAllByRole('option');
  expect(options[0]).toHaveTextContent('ETHUSDC');
  expect(options[0]).toHaveAttribute('aria-selected', 'true');
  fireEvent.keyDown(search, { key: 'Enter', keyCode: 229 });
  expect(router.state.location.pathname).toBe('/pairs/BTCUSDC/config');
  await user.keyboard('{Enter}');
  await waitFor(() => expect(router.state.location.pathname).toBe('/pairs/ETHUSDC'));
});

it('opens the mobile navigation drawer, navigates to settings, and opens shortcut help with ?', async () => {
  const router = setup();
  const user = userEvent.setup();
  await user.click(screen.getByRole('button', { name: '打开主导航' }));
  const drawer = await screen.findByRole('dialog');
  await user.click(within(drawer).getByRole('link', { name: '全局设置' }));
  expect(await screen.findByText('设置页面')).toBeInTheDocument();

  await act(async () => { await router.navigate('/pairs/BTCUSDC/config'); });
  await user.keyboard('?');
  const modal = await screen.findByRole('dialog', { name: '键盘快捷键' });
  expect(within(modal).getByText('打开命令面板')).toBeInTheDocument();
  await user.keyboard('{Escape}');
  const bottom = screen.getByRole('navigation', { name: '底部导航' });
  await user.click(within(bottom).getByRole('link', { name: '设置' }));
  expect(await screen.findByText('设置页面')).toBeInTheDocument();
  expect(router.state.location.pathname).toBe('/settings');
});

it('only flags staleResumeBlocked when a non-running strategy is stale and not busy', () => {
  vi.useFakeTimers();
  try {
    const running = { ...pair('SOLUSDC'), status: 'RUNNING' as const };
    const paused = { ...pair('SOLUSDC'), status: 'PAUSED' as const };
    expect(strategyActionGuard(running, false, true)).toMatchObject({ toggleDisabled: true, staleResumeBlocked: false });
    expect(strategyActionGuard(running, true, false)).toMatchObject({ toggleDisabled: false, staleResumeBlocked: false });
    expect(strategyActionGuard(paused, true, false)).toMatchObject({ toggleDisabled: true, staleResumeBlocked: true });
    expect(strategyActionGuard(paused, true, true)).toMatchObject({ toggleDisabled: true, staleResumeBlocked: false });

    mocks.busy = new Set(['SOLUSDC']);
    const { unmount } = render(<MemoryRouter><StrategyToggle pair={running} stale={false} /></MemoryRouter>);
    fireEvent.mouseEnter(screen.getByRole('button', { name: /暂停 SOLUSDC/ }));
    act(() => { vi.advanceTimersByTime(200); });
    expect(screen.queryByText('状态过期，恢复前请刷新')).not.toBeInTheDocument();
    unmount();

    mocks.busy = new Set();
    render(<MemoryRouter><StrategyToggle pair={paused} stale /></MemoryRouter>);
    const resumeBtn = screen.getByRole('button', { name: '恢复 SOLUSDC' });
    fireEvent.mouseEnter(resumeBtn.parentElement!);
    act(() => { vi.advanceTimersByTime(200); });
    expect(screen.getByText('状态过期，恢复前请刷新')).toBeInTheDocument();
  } finally {
    vi.useRealTimers();
  }
});
