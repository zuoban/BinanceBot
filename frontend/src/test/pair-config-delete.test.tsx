import { act, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { createMemoryRouter, RouterProvider } from 'react-router-dom';
import { beforeEach, expect, it, vi } from 'vitest';
import Shell from '../components/Shell';
import PairConfig from '../pages/PairConfig';
import { pair } from './fixtures';

const mocks = vi.hoisted(() => ({ confirm: vi.fn(), success: vi.fn(), error: vi.fn(), api: vi.fn(), post: vi.fn() }));
vi.mock('antd', async importOriginal => {
  const antd = await importOriginal<typeof import('antd')>();
  return { ...antd, App: { useApp: () => ({ modal: { confirm: mocks.confirm }, message: { success: mocks.success, error: mocks.error } }) } };
});
vi.mock('../lib/api', async importOriginal => ({ ...await importOriginal<typeof import('../lib/api')>(), api: mocks.api, post: mocks.post }));
vi.mock('../lib/queries', async () => {
  const { pair } = await import('./fixtures');
  return { usePairs: () => ({ data: [pair('BTCUSDC'), pair('ETHUSDC')] }), useOverview: () => ({ data: undefined }), useLiveAccounts: () => [] };
});
vi.mock('../components/Controls', () => ({ ControlProvider: ({ children }: { children: React.ReactNode }) => children }));
vi.mock('../components/ThemeSwitch', () => ({ ThemeSwitch: () => null, themeLabels: { system: '跟随系统', light: '浅色', dark: '深色' } }));

beforeEach(() => {
  vi.clearAllMocks();
  vi.stubGlobal('matchMedia', vi.fn().mockImplementation(query => ({ matches: false, media: query, addListener: vi.fn(), removeListener: vi.fn(), addEventListener: vi.fn(), removeEventListener: vi.fn() })));
  vi.stubGlobal('ResizeObserver', class { observe() {} unobserve() {} disconnect() {} });
});
function setup(request: Promise<unknown>) {
  mocks.api.mockImplementation((_: string, init?: RequestInit) => init?.method === 'DELETE' ? request : Promise.resolve(pair('BTCUSDC').grid_config));
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const router = createMemoryRouter([{ element: <Shell />, children: [
    { path: '/', element: <div>总览页面</div> },
    { path: '/settings', element: <div>设置页面</div> },
    { path: '/pairs/:symbol', element: <div>策略详情页面</div> },
    { path: '/pairs/:symbol/config', element: <PairConfig /> },
  ] }], { initialEntries: ['/pairs/BTCUSDC/config'] });
  render(<QueryClientProvider client={client}><RouterProvider router={router} /></QueryClientProvider>);
  return router;
}

it('shows the navigation menu and pair selector on a strategy page', () => {
  setup(Promise.resolve());
  expect(screen.getByRole('button', { name: '打开主导航' })).toBeInTheDocument();
  expect(screen.getByLabelText('切换交易对')).toBeInTheDocument();
});

it('returns to the strategy detail page', async () => {
  const router = setup(Promise.resolve());
  await userEvent.setup().click(screen.getByRole('button', { name: '返回详情' }));
  expect(await screen.findByText('策略详情页面')).toBeInTheDocument();
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
  await user.click(screen.getByRole('button', { name: '返回详情' }));
  await waitFor(() => expect(mocks.confirm).toHaveBeenCalledTimes(1));
  expect(router.state.location.pathname).toBe('/pairs/BTCUSDC/config');
  await user.click(screen.getByRole('button', { name: '返回详情' }));
  expect(await screen.findByText('策略详情页面')).toBeInTheDocument();
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
  expect(await screen.findByText('策略详情页面')).toBeInTheDocument();
  expect(router.state.location.pathname).toBe('/pairs/BTCUSDC');
  expect(mocks.confirm).not.toHaveBeenCalled();
}, 15000);

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
    expect(await screen.findByText('策略详情页面')).toBeInTheDocument();
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
