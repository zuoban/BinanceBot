import { act, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { createMemoryRouter, RouterProvider } from 'react-router-dom';
import { beforeEach, expect, it, vi } from 'vitest';
import Shell from '../components/Shell';
import PairConfig from '../pages/PairConfig';
import PairDetail from '../pages/PairDetail';
import { ApiError } from '../lib/api';
import type { GridConfig } from '../lib/types';
import { pair } from './fixtures';

const mocks = vi.hoisted(() => {
  const confirm = vi.fn(), success = vi.fn(), error = vi.fn();
  return { confirm, success, error, api: vi.fn(), post: vi.fn(), app: { modal: { confirm }, message: { success, error } } };
});
vi.mock('antd', async importOriginal => {
  const antd = await importOriginal<typeof import('antd')>();
  return { ...antd, App: { useApp: () => mocks.app } };
});
vi.mock('../lib/api', async importOriginal => ({ ...await importOriginal<typeof import('../lib/api')>(), api: mocks.api, post: mocks.post }));
vi.mock('../lib/queries', async () => {
  const { pair } = await import('./fixtures');
  return { usePairs: () => ({ data: [pair('BTCUSDC'), pair('ETHUSDC')] }), useOverview: () => ({ data: undefined }), useLiveAccounts: () => [], usePairSnapshot: () => ({ data: undefined, isPending: false, refetch: vi.fn() }) };
});
vi.mock('../components/Controls', () => ({ ControlProvider: ({ children }: { children: React.ReactNode }) => children, PairControls: () => null, useControls: () => ({ results: {} }) }));
vi.mock('../components/ThemeSwitch', () => ({ ThemeSwitch: () => null, themeLabels: { system: '跟随系统', light: '浅色', dark: '深色' } }));

beforeEach(() => {
  vi.clearAllMocks();
  vi.stubGlobal('matchMedia', vi.fn().mockImplementation(query => ({ matches: false, media: query, addListener: vi.fn(), removeListener: vi.fn(), addEventListener: vi.fn(), removeEventListener: vi.fn() })));
  vi.stubGlobal('ResizeObserver', class { observe() {} unobserve() {} disconnect() {} });
});
function setup(request: Promise<unknown>, initialPath = '/pairs/BTCUSDC/config', configError?: Error) {
  mocks.api.mockImplementation((_: string, init?: RequestInit) => init?.method === 'DELETE' ? request : configError ? Promise.reject(configError) : Promise.resolve({ ...pair('BTCUSDC').grid_config, config_version: 'version-1' }));
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

function expectDisabledAncestors(labels: string[]) {
  const nav = breadcrumb();
  for (const label of labels) {
    const item = nav.getByText(label);
    expect(item).toHaveAttribute('aria-disabled', 'true');
    expect(item.tagName).not.toBe('A');
    expect(nav.queryByRole('link', { name: label })).not.toBeInTheDocument();
  }
}

it.each([
  ['/pairs/new', [{ label: '策略总览', href: '/' }, { label: '新增交易对' }]],
  ['/pairs/BTCUSDC', [{ label: '策略总览', href: '/' }, { label: 'BTCUSDC' }]],
  ['/pairs/BTCUSDC/config', [{ label: '策略总览', href: '/' }, { label: 'BTCUSDC', href: '/pairs/BTCUSDC' }, { label: '策略参数' }]],
])('shows the breadcrumb hierarchy for %s', (path, items) => {
  setup(Promise.resolve(), path);
  expectBreadcrumb(items);
});

it.each(['/pairs/UNKNOWN', '/pairs/UNKNOWN/config'])('keeps the breadcrumb available for a missing pair at %s', path => {
  setup(Promise.resolve(), path);
  expect(screen.getByText('该交易对不存在或已移除')).toBeInTheDocument();
  expectBreadcrumb(path.endsWith('/config')
    ? [{ label: '策略总览', href: '/' }, { label: 'UNKNOWN', href: '/pairs/UNKNOWN' }, { label: '策略参数' }]
    : [{ label: '策略总览', href: '/' }, { label: 'UNKNOWN' }]);
});

it('keeps the breadcrumb available when configuration loading fails', async () => {
  setup(Promise.resolve(), '/pairs/BTCUSDC/config', new Error('配置加载失败'));
  await screen.findByText('配置加载失败');
  expectBreadcrumb([{ label: '策略总览', href: '/' }, { label: 'BTCUSDC', href: '/pairs/BTCUSDC' }, { label: '策略参数' }]);
});

it.each(['/pairs/BTCUSDC/config', '/pairs/new'])('confirms before leaving a dirty form for the overview at %s', async path => {
  mocks.confirm.mockResolvedValueOnce(false).mockResolvedValueOnce(true);
  const router = setup(Promise.resolve(), path);
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

it.each(['/pairs/BTCUSDC/config', '/pairs/new'])('disables breadcrumb ancestors during save at %s', async path => {
  let finish!: (value: string | GridConfig) => void;
  mocks.post.mockReturnValue(new Promise<string | GridConfig>(resolve => { finish = resolve; }));
  const router = setup(Promise.resolve(), path);
  const user = userEvent.setup();
  const input = await screen.findByLabelText('网格间距');
  await waitFor(() => expect(input).toBeEnabled());
  if (path === '/pairs/new') await user.type(screen.getByLabelText('合约交易对'), 'BTCUSDC');
  await user.clear(input);
  await user.type(input, '0.2');
  await user.click(screen.getByRole('button', { name: path === '/pairs/new' ? '保存并添加' : '保存并立即生效' }));
  await waitFor(() => expect(mocks.post).toHaveBeenCalled());
  const labels = path === '/pairs/new' ? ['策略总览'] : ['策略总览', 'BTCUSDC'];
  expectDisabledAncestors(labels);
  for (const label of labels) await user.click(breadcrumb().getByText(label));
  expect(router.state.location.pathname).toBe(path);
  await act(async () => { finish(path === '/pairs/new' ? 'BTCUSDC' : pair('BTCUSDC').grid_config); });
  expect(await screen.findByRole('heading', { name: 'BTCUSDC' })).toBeInTheDocument();
  expect(router.state.location.pathname).toBe('/pairs/BTCUSDC');
}, 15000);

it('shows the navigation menu and pair selector on a strategy page', () => {
  setup(Promise.resolve());
  expect(screen.getByRole('button', { name: '打开主导航' })).toBeInTheDocument();
  expect(screen.getByLabelText('切换交易对')).toBeInTheDocument();
});

it('returns to the strategy detail page', async () => {
  const router = setup(Promise.resolve());
  await userEvent.setup().click(within(screen.getByRole('navigation', { name: '面包屑' })).getByRole('link', { name: 'BTCUSDC' }));
  expect(await screen.findByRole('heading', { name: 'BTCUSDC' })).toBeInTheDocument();
  expect(router.state.location.pathname).toBe('/pairs/BTCUSDC');
});

it('confirms before returning with unsaved changes', async () => {
  mocks.confirm.mockResolvedValueOnce(false).mockResolvedValueOnce(true);
  const router = setup(Promise.resolve());
  const user = userEvent.setup();
  const input = await screen.findByLabelText('网格间距');
  await waitFor(() => expect(input).toBeEnabled());
  await user.clear(input);
  await user.type(input, '0.2');
  await user.click(within(screen.getByRole('navigation', { name: '面包屑' })).getByRole('link', { name: 'BTCUSDC' }));
  await waitFor(() => expect(mocks.confirm).toHaveBeenCalledTimes(1));
  expect(router.state.location.pathname).toBe('/pairs/BTCUSDC/config');
  await user.click(within(screen.getByRole('navigation', { name: '面包屑' })).getByRole('link', { name: 'BTCUSDC' }));
  expect(await screen.findByRole('heading', { name: 'BTCUSDC' })).toBeInTheDocument();
  expect(router.state.location.pathname).toBe('/pairs/BTCUSDC');
});

it('returns to detail after saving changed parameters', async () => {
  const router = setup(Promise.resolve());
  mocks.post.mockResolvedValue(pair('BTCUSDC').grid_config);
  const user = userEvent.setup();
  const input = await screen.findByLabelText('网格间距');
  await waitFor(() => expect(input).toBeEnabled());
  await user.clear(input);
  await user.type(input, '0.2');
  await user.click(screen.getByRole('button', { name: '保存并立即生效' }));
  expect(await screen.findByRole('heading', { name: 'BTCUSDC' })).toBeInTheDocument();
  expect(router.state.location.pathname).toBe('/pairs/BTCUSDC');
  expect(mocks.post).toHaveBeenCalledWith(expect.any(String), expect.objectContaining({ baseline_version: 'version-1' }));
  expect(mocks.confirm).not.toHaveBeenCalled();
}, 15000);

it('保留冲突时的输入，并在确认后重新加载版本', async () => {
  setup(Promise.resolve());
  mocks.post.mockRejectedValueOnce(new ApiError('策略配置已被其他请求修改', 409));
  const user = userEvent.setup();
  const input = await screen.findByLabelText('网格间距');
  await waitFor(() => expect(input).toBeEnabled());
  await user.clear(input);
  await user.type(input, '0.2');
  await user.click(screen.getByRole('button', { name: '保存并立即生效' }));
  const reload = await screen.findByRole('button', { name: '重新加载策略配置' });
  expect(input).toHaveValue('0.2');
  expect(mocks.post).toHaveBeenCalledWith(expect.any(String), expect.objectContaining({ baseline_version: 'version-1' }));
  mocks.confirm.mockResolvedValueOnce(false).mockResolvedValueOnce(true);
  await user.click(reload);
  expect(input).toHaveValue('0.2');
  mocks.api.mockResolvedValue({ ...pair('BTCUSDC').grid_config, grid_interval: '0.3', config_version: 'version-2' });
  await user.click(reload);
  await waitFor(() => expect(input).toHaveValue('0.3'));
  mocks.post.mockResolvedValue(pair('BTCUSDC').grid_config);
  await user.click(screen.getByRole('button', { name: '保存并立即生效' }));
  await waitFor(() => expect(mocks.post).toHaveBeenLastCalledWith(expect.any(String), expect.objectContaining({ baseline_version: 'version-2' })));
}, 20000);

it('does not redirect again after the user navigates while queries refresh', async () => {
  let finish!: () => void;
  const refresh = new Promise<void>(resolve => { finish = resolve; });
  const invalidate = vi.spyOn(QueryClient.prototype, 'invalidateQueries').mockReturnValue(refresh);
  try {
    const router = setup(Promise.resolve());
    mocks.post.mockResolvedValue(pair('BTCUSDC').grid_config);
    const user = userEvent.setup();
    const input = await screen.findByLabelText('网格间距');
    await waitFor(() => expect(input).toBeEnabled());
    await user.clear(input);
    await user.type(input, '0.2');
    await user.click(screen.getByRole('button', { name: '保存并立即生效' }));
    expect(await screen.findByRole('heading', { name: 'BTCUSDC' })).toBeInTheDocument();
    await act(async () => { await router.navigate('/settings'); });
    expect(router.state.location.pathname).toBe('/settings');
    await act(async () => { finish(); await refresh; });
    expect(router.state.location.pathname).toBe('/settings');
  } finally {
    invalidate.mockRestore();
  }
}, 15000);

it('opens the mobile navigation menu and navigates to settings', async () => {
  setup(Promise.resolve());
  const user = userEvent.setup();
  await user.click(screen.getByRole('button', { name: '打开主导航' }));
  const menu = await screen.findByRole('menu');
  expect(within(menu).getAllByRole('menuitem').map(item => item.textContent)).toEqual(['策略总览', '全局设置', '主题：跟随系统', '退出']);
  await user.click(within(menu).getByRole('menuitem', { name: '全局设置' }));
  expect(await screen.findByText('设置页面')).toBeInTheDocument();
});

it('blocks navigation while deleting and redirects a dirty form without another confirmation', async () => {
  let finish!: () => void;
  const request = new Promise<void>(resolve => { finish = resolve; });
  mocks.confirm.mockResolvedValue(true);
  const router = setup(request);
  const user = userEvent.setup();
  const input = await screen.findByLabelText('网格间距');
  await waitFor(() => expect(input).toBeEnabled());
  await user.clear(input);
  await user.type(input, '0.2');
  await user.click(screen.getByRole('button', { name: '删除策略' }));
  await waitFor(() => expect(mocks.api).toHaveBeenCalledWith('/api/pairs/BTCUSDC', expect.objectContaining({ method: 'DELETE' })));
  expectDisabledAncestors(['策略总览', 'BTCUSDC']);
  await user.click(breadcrumb().getByText('策略总览'));
  await user.click(breadcrumb().getByText('BTCUSDC'));
  expect(router.state.location.pathname).toBe('/pairs/BTCUSDC/config');
  await act(async () => { await router.navigate('/settings'); });
  expect(router.state.location.pathname).toBe('/pairs/BTCUSDC/config');
  expect(mocks.confirm).toHaveBeenCalledTimes(2);
  await act(async () => { finish(); });
  await screen.findByText('总览页面');
  expect(router.state.location.pathname).toBe('/');
  expect(mocks.confirm).toHaveBeenCalledTimes(2);
}, 15000);

it('restores navigation protection for unsaved changes when deletion fails', async () => {
  mocks.confirm.mockResolvedValueOnce(true).mockResolvedValueOnce(true).mockResolvedValueOnce(false);
  let fail!: (error: Error) => void;
  const request = new Promise<void>((_, reject) => { fail = reject; });
  const router = setup(request);
  const user = userEvent.setup();
  const input = await screen.findByLabelText('网格间距');
  await waitFor(() => expect(input).toBeEnabled());
  await user.clear(input);
  await user.type(input, '0.2');
  await user.click(screen.getByRole('button', { name: '删除策略' }));
  await waitFor(() => expect(mocks.api).toHaveBeenCalledWith('/api/pairs/BTCUSDC', expect.objectContaining({ method: 'DELETE' })));
  await act(async () => { fail(new Error('请先暂停该策略')); });
  await waitFor(() => expect(mocks.error).toHaveBeenCalledWith('请先暂停该策略'));
  await act(async () => { await router.navigate('/settings'); });
  await waitFor(() => expect(mocks.confirm).toHaveBeenCalledTimes(3));
  expect(router.state.location.pathname).toBe('/pairs/BTCUSDC/config');
}, 15000);
