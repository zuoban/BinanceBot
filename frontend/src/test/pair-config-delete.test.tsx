import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { UserEvent } from '@testing-library/user-event';
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
  return { confirm, success, error, run: vi.fn(), api: vi.fn(), post: vi.fn(), app: { modal: { confirm }, message: { success, error } } };
});
vi.mock('antd', async importOriginal => {
  const antd = await importOriginal<typeof import('antd')>();
  return { ...antd, App: { useApp: () => mocks.app } };
});
vi.mock('../lib/api', async importOriginal => ({ ...await importOriginal<typeof import('../lib/api')>(), api: mocks.api, post: mocks.post }));
vi.mock('../lib/queries', async () => {
  const { pair } = await import('./fixtures');
  return { usePairs: () => ({ data: [pair('BTCUSDC'), pair('ETHUSDC')], dataUpdatedAt: Date.now() }), useLiveAccounts: () => ({ accounts: [], stale: false, error: null }), usePairSnapshot: () => ({ data: undefined, isPending: false, refetch: vi.fn() }) };
});
vi.mock('../components/Controls', () => ({ ControlProvider: ({ children }: { children: React.ReactNode }) => children, PairControls: () => null, useControls: () => ({ results: {}, run: mocks.run }) }));
vi.mock('../components/ThemeSwitch', () => ({ ThemeSwitch: () => null, ThemeSegmented: () => null, themeLabels: { system: '跟随系统', light: '浅色', dark: '深色' } }));

beforeEach(() => {
  vi.clearAllMocks();
});
function setup(request: Promise<unknown>, initialPath = '/pairs/BTCUSDC/config', configError?: Error, symbols = (_path: string): Promise<string[]> => Promise.resolve(['BTCUSDC', 'SOLUSDC']), postOnly = true) {
  mocks.api.mockImplementation((path: string, init?: RequestInit) => path.startsWith('/api/exchange/symbols?') ? symbols(path) : init?.method === 'DELETE' ? request : configError ? Promise.reject(configError) : Promise.resolve({ ...pair('BTCUSDC').grid_config, post_only: postOnly, config_version: 'version-1' }));
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

async function selectSymbol(user: UserEvent, symbol: string) {
  const input = screen.getByLabelText('合约交易对');
  await waitFor(() => expect(input).toBeEnabled());
  await user.click(input);
  await user.type(input, symbol);
  await user.click(await screen.findByTitle(symbol));
}


it('loads symbols from the exchange and allows selecting a symbol', async () => {
  setup(Promise.resolve(), '/pairs/new');
  const user = userEvent.setup();
  await waitFor(() => expect(mocks.api).toHaveBeenCalledWith('/api/exchange/symbols?environment=mainnet', expect.anything()));
  await selectSymbol(user, 'SOLUSDC');
  expect(screen.getByLabelText('合约交易对').closest('.ant-select-content')).toHaveAttribute('title', 'SOLUSDC');
  expect(within(screen.getByRole('complementary')).getByText('模拟盘')).toBeInTheDocument();
});

it('shows loading, retries a failed symbol request and searches the returned symbols', async () => {
  let reject!: (reason?: unknown) => void;
  const pending = new Promise<string[]>((_, rejectPromise) => { reject = rejectPromise; });
  const symbols = vi.fn().mockReturnValueOnce(pending).mockResolvedValue(['SOLUSDC']);
  setup(Promise.resolve(), '/pairs/new', undefined, symbols);
  const user = userEvent.setup();
  expect(screen.getByText('正在加载交易对...')).toBeInTheDocument();
  expect(screen.getByLabelText('合约交易对')).toBeDisabled();
  await act(async () => { reject(new Error('网络错误')); });
  expect(await screen.findByText('交易对加载失败')).toBeInTheDocument();
  await user.click(screen.getByRole('button', { name: '重试' }));
  await waitFor(() => expect(symbols).toHaveBeenCalledTimes(2));
  await waitFor(() => expect(screen.getByLabelText('合约交易对')).toBeEnabled());
  await user.click(screen.getByLabelText('合约交易对'));
  await user.type(screen.getByLabelText('合约交易对'), 'NO-SUCH-SYMBOL');
  expect(await screen.findByText('没有匹配的交易对')).toBeInTheDocument();
  await user.clear(screen.getByLabelText('合约交易对'));
  await user.type(screen.getByLabelText('合约交易对'), 'sol');
  expect(await screen.findByTitle('SOLUSDC')).toBeInTheDocument();
});

it('locks the symbol while adding a pair and unlocks it if saving fails', async () => {
  let fail!: (error: Error) => void;
  const request = new Promise<never>((_, reject) => { fail = reject; });
  mocks.post.mockReturnValueOnce(request);
  setup(Promise.resolve(), '/pairs/new');
  const user = userEvent.setup();
  await selectSymbol(user, 'SOLUSDC');
  const symbol = screen.getByLabelText('合约交易对');
  await user.click(screen.getByRole('button', { name: '保存并添加' }));
  await waitFor(() => expect(mocks.post).toHaveBeenCalledWith('/api/pairs', expect.objectContaining({ symbol: 'SOLUSDC' })));
  await waitFor(() => expect(symbol).toBeDisabled());
  await act(async () => { fail(new Error('保存失败')); });
  await waitFor(() => expect(mocks.error).toHaveBeenCalledWith('保存失败'));
  await waitFor(() => expect(symbol).toBeEnabled());
  await selectSymbol(user, 'BTCUSDC');
  expect(symbol.closest('.ant-select-content')).toHaveAttribute('title', 'BTCUSDC');
}, 40000);

it('keeps the symbol read-only on the edit form without requesting exchange symbols', async () => {
  setup(Promise.resolve());
  const symbol = await screen.findByLabelText('合约交易对');
  await waitFor(() => expect(symbol).toHaveValue('BTCUSDC'));
  expect(symbol).toBeDisabled();
  expect(mocks.api.mock.calls.some(([path]) => String(path).startsWith('/api/exchange/symbols?'))).toBe(false);
});

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

it('submits the new strategy defaults', async () => {
  mocks.post.mockResolvedValue('SOLUSDC');
  setup(Promise.resolve(), '/pairs/new');
  expect(screen.getByText('订单仅作为挂单成交（Post Only）；如果订单会立即成交，交易所会拒绝该订单。')).toBeInTheDocument();
  expect(screen.queryByRole('checkbox', { name: /Post Only/ })).not.toBeInTheDocument();
  const user = userEvent.setup();
  await selectSymbol(user, 'SOLUSDC');
  await user.click(screen.getByRole('button', { name: '保存并添加' }));
  await waitFor(() => expect(mocks.post).toHaveBeenCalledWith('/api/pairs', expect.objectContaining({
    symbol: 'SOLUSDC', buy_window: 3, sell_window: 3, max_position_usdc: '1000', pause_on_max_position: true, post_only: true,
  })));
});

it('requires a position limit when pause is enabled and allows clearing it when disabled', async () => {
  mocks.post.mockResolvedValue('SOLUSDC');
  setup(Promise.resolve(), '/pairs/new');
  const user = userEvent.setup();
  await selectSymbol(user, 'SOLUSDC');
  await user.clear(screen.getByLabelText('最大持仓名义价值'));
  await user.click(screen.getByRole('button', { name: '保存并添加' }));
  expect(await screen.findByText('暂停策略需要大于 0 的持仓上限')).toBeInTheDocument();
  expect(mocks.post).not.toHaveBeenCalled();
  await user.click(screen.getByRole('switch', { name: '达到上限时仅撤开仓单' }));
  expect(await screen.findByText('撤销开仓单需要大于 0 的持仓上限')).toBeInTheDocument();
  await user.click(screen.getByRole('switch', { name: '达到上限时仅撤开仓单' }));
  await user.click(screen.getByRole('button', { name: '保存并添加' }));
  await waitFor(() => expect(mocks.post).toHaveBeenCalledWith('/api/pairs', expect.objectContaining({
    max_position_usdc: null, pause_on_max_position: false,
  })));
});

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
  if (path === '/pairs/new') await selectSymbol(user, 'BTCUSDC');
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
}, 40000);

it('lists the pairs in the sidebar and marks the current strategy', () => {
  setup(Promise.resolve());
  const sidebar = within(screen.getByRole('complementary'));
  expect(sidebar.getByRole('link', { name: '策略总览' })).toHaveAttribute('href', '/');
  expect(sidebar.getByRole('link', { name: '全局设置' })).toHaveAttribute('href', '/settings');
  const pairs = sidebar.getByRole('navigation', { name: '交易对' });
  expect(within(pairs).getByRole('link', { name: /BTCUSDC/ })).toHaveAttribute('aria-current', 'page');
  expect(within(pairs).getByRole('link', { name: /ETHUSDC/ })).not.toHaveAttribute('aria-current');
});

it('keeps the config page when switching pairs from the sidebar', () => {
  setup(Promise.resolve());
  const pairs = within(screen.getByRole('complementary')).getByRole('navigation', { name: '交易对' });
  expect(within(pairs).getByRole('link', { name: /ETHUSDC/ })).toHaveAttribute('href', '/pairs/ETHUSDC/config');
});

it('counts the modified fields in the save bar and lists them on demand', async () => {
  setup(Promise.resolve());
  const user = userEvent.setup();
  const interval = await screen.findByLabelText('网格间距');
  await waitFor(() => expect(interval).toBeEnabled());
  expect(screen.getByText('所有修改已保存')).toBeInTheDocument();
  await user.clear(interval);
  await user.type(interval, '0.7');
  const buy = screen.getByLabelText('买单窗口');
  await user.clear(buy);
  await user.type(buy, '9');
  expect(await screen.findByText('已修改 2 项')).toBeInTheDocument();
  await user.click(screen.getByRole('button', { name: '查看' }));
  expect(screen.getByRole('button', { name: '收起' })).toHaveAttribute('aria-expanded', 'true');
  const items = within(document.getElementById('pending-changes')!).getAllByRole('listitem').map(item => item.textContent);
  expect(items[0]).toContain('网格间距');
  expect(items[0]).toContain('0.7');
  expect(items[1]).toContain('买单窗口');
  await user.clear(interval);
  await user.type(interval, '0.1');
  await user.clear(buy);
  await user.type(buy, '5');
  expect(await screen.findByText('有未保存的修改')).toBeInTheDocument();
}, 40000);

it('runs a pair action from the palette through the guarded control flow', async () => {
  setup(Promise.resolve());
  const user = userEvent.setup();
  await user.keyboard('{Control>}k{/Control}');
  const search = await screen.findByRole('combobox', { name: '搜索命令' });
  await user.type(search, '恢复 ETHUSDC');
  await user.keyboard('{Enter}');
  expect(mocks.run).toHaveBeenCalledWith(expect.objectContaining({ symbol: 'ETHUSDC' }), 'resume');
});

it('jumps between pages and pairs with keyboard shortcuts but not while typing', async () => {
  const router = setup(Promise.resolve());
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

it('does not run a palette command when Enter commits an IME candidate', async () => {
  const router = setup(Promise.resolve());
  const user = userEvent.setup();
  await user.keyboard('{Control>}k{/Control}');
  const search = await screen.findByRole('combobox', { name: '搜索命令' });
  await user.type(search, 'ETHUSDC');
  fireEvent.keyDown(search, { key: 'Enter', keyCode: 229 });
  expect(router.state.location.pathname).toBe('/pairs/BTCUSDC/config');
  await user.keyboard('{Enter}');
  await waitFor(() => expect(router.state.location.pathname).toBe('/pairs/ETHUSDC'));
});

it('switches pairs from the command palette with the keyboard', async () => {
  const router = setup(Promise.resolve());
  const user = userEvent.setup();
  await user.keyboard('{Control>}k{/Control}');
  const search = await screen.findByRole('combobox', { name: '搜索命令' });
  await user.type(search, 'ETHUSDC');
  const options = screen.getAllByRole('option');
  expect(options[0]).toHaveTextContent('ETHUSDC');
  expect(options[0]).toHaveAttribute('aria-selected', 'true');
  await user.keyboard('{Enter}');
  await waitFor(() => expect(router.state.location.pathname).toBe('/pairs/ETHUSDC'));
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
  expect(mocks.post).toHaveBeenCalledWith(expect.any(String), expect.objectContaining({ baseline_version: 'version-1', post_only: true }));
  expect(mocks.confirm).not.toHaveBeenCalled();
}, 40000);

it('enables post-only when saving a legacy configuration', async () => {
  setup(Promise.resolve(), '/pairs/BTCUSDC/config', undefined, undefined, false);
  mocks.post.mockResolvedValue(pair('BTCUSDC').grid_config);
  const user = userEvent.setup();
  const input = await screen.findByLabelText('网格间距');
  await waitFor(() => expect(input).toBeEnabled());
  await user.click(screen.getByRole('button', { name: '保存并立即生效' }));
  await waitFor(() => expect(mocks.post).toHaveBeenCalledWith(expect.any(String), expect.objectContaining({ baseline_version: 'version-1', post_only: true })));
});

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
  mocks.confirm.mockResolvedValueOnce(false).mockResolvedValueOnce(true).mockResolvedValueOnce(true);
  await user.click(reload);
  expect(input).toHaveValue('0.2');
  mocks.api.mockRejectedValueOnce(new Error('重新加载失败'));
  await user.click(reload);
  await waitFor(() => expect(mocks.error).toHaveBeenCalledWith('重新加载失败'));
  expect(input).toHaveValue('0.2');
  mocks.api.mockResolvedValue({ ...pair('BTCUSDC').grid_config, grid_interval: '0.3', config_version: 'version-2' });
  await user.click(reload);
  await waitFor(() => expect(input).toHaveValue('0.3'));
  mocks.post.mockResolvedValue(pair('BTCUSDC').grid_config);
  await user.click(screen.getByRole('button', { name: '保存并立即生效' }));
  await waitFor(() => expect(mocks.post).toHaveBeenLastCalledWith(expect.any(String), expect.objectContaining({ baseline_version: 'version-2' })));
}, 40000);

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
}, 40000);

it('opens the mobile navigation drawer and navigates to settings', async () => {
  setup(Promise.resolve());
  const user = userEvent.setup();
  await user.click(screen.getByRole('button', { name: '打开主导航' }));
  const drawer = await screen.findByRole('dialog');
  await user.click(within(drawer).getByRole('link', { name: '全局设置' }));
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
}, 40000);

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
}, 40000);

it('fetches symbol preview on /pairs/new, shows latest price, auto-fills mark price and validates tick_size multiples', async () => {
  setup(Promise.resolve(), '/pairs/new');
  mocks.api.mockImplementation((path: string) => {
    if (path.startsWith('/api/exchange/symbols?')) return Promise.resolve(['BTCUSDC', 'SOLUSDC']);
    if (path.startsWith('/api/exchange/symbol-preview?')) return Promise.resolve({ symbol: 'SOLUSDC', last_price: '142.4000', mark_price: '142.3500', tick_size: '0.1', step_size: '0.01', min_qty: '0.01', min_notional: '5' });
    return Promise.resolve({ version: 'v1', has_api_key: false, has_api_secret: false });
  });
  const user = userEvent.setup();
  await selectSymbol(user, 'SOLUSDC');
  expect(await screen.findByText('最新价格：142.4 USDC')).toBeInTheDocument();
  expect(await screen.findByText('以当前标记价 142.35 为参考')).toBeInTheDocument();
  expect(screen.getByLabelText('网格间距')).toHaveValue('1.4');
  expect(screen.getByText('+0.32%')).toBeInTheDocument();
  expect(screen.getByText('-0.67%')).toBeInTheDocument();
  expect(screen.getByText('价格步长 0.1')).toBeInTheDocument();
  expect(screen.getByText('数量步长 0.01')).toBeInTheDocument();
  expect(screen.getByText('最小名义金额 5 USDC')).toBeInTheDocument();
  const amount = screen.getByLabelText('每格金额（报价资产）');
  await user.clear(amount);
  await user.type(amount, '4');
  expect(await screen.findByText(/全部 6 档均无法挂单：按数量步长截断后实际金额为/)).toBeInTheDocument();
  const interval = screen.getByLabelText('网格间距');
  await user.clear(interval);
  await user.type(interval, '0.15');
  expect(await screen.findAllByText('网格间距 0.15 必须是价格步长 0.1 的正整数倍')).not.toHaveLength(0);
}, 40000);


it('opens the keyboard shortcut help with ? and navigates from the bottom bar', async () => {
  const router = setup(Promise.resolve());
  const user = userEvent.setup();
  await user.keyboard('?');
  const modal = await screen.findByRole('dialog', { name: '键盘快捷键' });
  expect(within(modal).getByText('打开命令面板')).toBeInTheDocument();
  await user.keyboard('{Escape}');
  const bottom = screen.getByRole('navigation', { name: '底部导航' });
  await user.click(within(bottom).getByRole('link', { name: '设置' }));
  expect(await screen.findByText('设置页面')).toBeInTheDocument();
  expect(router.state.location.pathname).toBe('/settings');
});
