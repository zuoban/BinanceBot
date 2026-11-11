/// <reference lib="es2024.promise" />
import { useSyncExternalStore } from 'react';
import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type * as Antd from 'antd';
import type * as Api from '../lib/api';
import type * as Queries from '../lib/queries';
import type { Pair } from '../lib/types';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { createMemoryRouter, RouterProvider } from 'react-router-dom';
import { beforeEach, expect, it, vi } from 'vitest';
import Shell from '../components/Shell';
import PairConfig from '../pages/PairConfig';
import { pair } from './fixtures';

const mocks = vi.hoisted(() => {
  const confirm = vi.fn(), success = vi.fn(), error = vi.fn();
  return { confirm, success, error, api: vi.fn(), post: vi.fn(), app: { modal: { confirm }, message: { success, error } } };
});
vi.mock('antd', async importOriginal => {
  const antd = await importOriginal<typeof Antd>();
  return { ...antd, App: { useApp: () => mocks.app } };
});
vi.mock('../lib/api', async importOriginal => ({ ...await importOriginal<typeof Api>(), api: mocks.api, post: mocks.post }));
const pairsState = vi.hoisted(() => {
  const listeners = new Set<() => void>();
  return {
    list: [] as Pair[],
    set(next: Pair[]) { this.list = next; for (const listener of listeners) listener(); },
    subscribe: (listener: () => void) => { listeners.add(listener); return () => listeners.delete(listener); },
  };
});
vi.mock('../lib/queries', async importOriginal => {
  const actual = await importOriginal<typeof Queries>();
  return {
    ...actual,
    usePairs: () => ({ data: useSyncExternalStore(pairsState.subscribe, () => pairsState.list), isFetching: false, dataUpdatedAt: Date.now() }),
    useLiveAccounts: () => ({ accounts: [], stale: false, error: null }),
  };
});
vi.mock('../components/Controls', () => ({ ControlProvider: ({ children }: { children: React.ReactNode }) => children, useControls: () => ({ busy: new Set<string>(), pending: {}, results: {}, clearResult: vi.fn(), run: vi.fn() }) }));
vi.mock('../components/ThemeSwitch', () => ({ ThemeSwitch: () => null, ThemeSegmented: () => null, themeLabels: { system: '跟随系统', light: '浅色', dark: '深色' } }));

beforeEach(() => {
  vi.clearAllMocks();
  pairsState.set([pair('BTCUSDC'), pair('ETHUSDC')]);
  mocks.confirm.mockReset();
  mocks.post.mockReset();
  mocks.api.mockImplementation((path: string) => path === '/api/global/config'
    ? Promise.resolve({ version: 'global-version', has_api_key: true, has_api_secret: true })
    : Promise.resolve({ ...pair(path.includes('ETHUSDC') ? 'ETHUSDC' : 'BTCUSDC').grid_config, config_version: 'version-1' }));
});

function setup(initialPath = '/pairs/BTCUSDC/config') {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const router = createMemoryRouter([{ element: <Shell />, children: [
    { path: '/', element: <div>总览页面</div> },
    { path: '/settings', element: <div>设置页面</div> },
    { path: '/pairs/new', element: <PairConfig /> },
    { path: '/pairs/:symbol', element: <div>策略详情</div> },
    { path: '/pairs/:symbol/config', element: <PairConfig /> },
  ] }], { initialEntries: [initialPath] });
  render(<QueryClientProvider client={client}><RouterProvider router={router} /></QueryClientProvider>);
  return router;
}

async function prepareLive() {
  const user = userEvent.setup();
  await screen.findByText('实盘使用真实资金，请核对交易方向和风险限制。');
  return user;
}

it('does not save when a pending live confirmation resolves after leaving and discarding changes', async () => {
  let confirmSave!: (value: boolean) => void;
  mocks.confirm.mockReturnValueOnce(new Promise<boolean>(resolve => { confirmSave = resolve; })).mockResolvedValueOnce(true);
  setup();
  const user = await prepareLive();
  await user.click(screen.getByRole('button', { name: '保存并立即生效' }));
  await waitFor(() => expect(mocks.confirm).toHaveBeenCalledTimes(1));
  await user.click(within(screen.getByRole('navigation', { name: '面包屑' })).getByRole('link', { name: '策略总览' }));
  await screen.findByText('总览页面');
  await act(async () => { confirmSave(true); });
  expect(mocks.post).not.toHaveBeenCalled();
});

it('ignores repeated submissions while confirmation or saving is pending and allows retry after cancellation', async () => {
  let confirmSave!: (value: boolean) => void;
  let finishSave!: (value: unknown) => void;
  mocks.confirm.mockReturnValueOnce(new Promise<boolean>(resolve => { confirmSave = resolve; })).mockResolvedValueOnce(true);
  mocks.post.mockReturnValueOnce(new Promise(resolve => { finishSave = resolve; }));
  setup();
  const user = await prepareLive();
  const button = screen.getByRole('button', { name: '保存并立即生效' });
  const form = button.closest('form')!;
  await user.click(button);
  await waitFor(() => expect(mocks.confirm).toHaveBeenCalledTimes(1));
  await act(async () => { fireEvent.submit(form); });
  await act(async () => { confirmSave(false); });
  expect(mocks.confirm).toHaveBeenCalledTimes(1);
  expect(mocks.post).not.toHaveBeenCalled();
  await user.click(button);
  await waitFor(() => expect(mocks.post).toHaveBeenCalledTimes(1));
  await act(async () => { fireEvent.submit(form); });
  expect(mocks.post).toHaveBeenCalledTimes(1);
  await act(async () => { finishSave('BTCUSDC'); });
});

it('destroys unsettled confirm dialogs on route switch without deadlocking subsequent saves', async () => {
  const { promise: neverSettles } = Promise.withResolvers<boolean>();
  const destroy = vi.fn();
  mocks.confirm
    .mockReturnValueOnce(Object.assign(neverSettles, { destroy }))
    .mockResolvedValueOnce(true);
  mocks.post.mockResolvedValueOnce(undefined);
  const router = setup();
  const user = await prepareLive();
  await user.click(screen.getByRole('button', { name: '保存并立即生效' }));
  await waitFor(() => expect(mocks.confirm).toHaveBeenCalledTimes(1));
  act(() => { void router.navigate('/pairs/ETHUSDC/config'); });
  await waitFor(() => expect(router.state.location.pathname).toBe('/pairs/ETHUSDC/config'));
  expect(destroy).toHaveBeenCalledTimes(1);
  expect(mocks.post).not.toHaveBeenCalled();
  const nextInterval = await screen.findByLabelText('网格间距');
  await waitFor(() => expect(nextInterval).toBeEnabled());
  await user.click(screen.getByRole('button', { name: '保存并立即生效' }));
  await waitFor(() => expect(mocks.confirm).toHaveBeenCalledTimes(2));
  await waitFor(() => expect(mocks.post).toHaveBeenCalledWith('/api/pairs/ETHUSDC/config', expect.anything()));
});

it('disables the form and delete button cleanly without crashing when the pair disappears from pairs', async () => {
  setup();
  await prepareLive();
  const interval = await screen.findByLabelText('网格间距');
  const saveButton = screen.getByRole('button', { name: '保存并立即生效' });
  const deleteButton = screen.getByRole('button', { name: '删除策略' });
  await waitFor(() => expect(interval).toBeEnabled());
  expect(deleteButton).toBeEnabled();
  act(() => { pairsState.set([pair('ETHUSDC')]); });
  expect(await screen.findByText('该交易对不存在或已移除')).toBeInTheDocument();
  expect(interval).toBeDisabled();
  expect(saveButton).toBeDisabled();
  expect(deleteButton).toBeDisabled();
});

it('surfaces symbol-preview errors inline with a retry button on /pairs/new', async () => {
  let previewAttempts = 0;
  mocks.api.mockImplementation((path: string) => {
    if (path.startsWith('/api/exchange/symbols?')) return Promise.resolve(['SOLUSDC']);
    if (path.startsWith('/api/exchange/symbol-preview?')) {
      previewAttempts++;
      if (previewAttempts === 1) return Promise.reject(new Error('预览超时'));
      return Promise.resolve({ symbol: 'SOLUSDC', last_price: '145.2', mark_price: '145.1', tick_size: '0.1', step_size: '0.01', min_qty: '0.01', min_notional: '5' });
    }
    return Promise.resolve({ version: 'global-version', has_api_key: false, has_api_secret: false });
  });
  setup('/pairs/new');
  const user = userEvent.setup();
  const symbolInput = screen.getByLabelText('合约交易对');
  await waitFor(() => expect(symbolInput).toBeEnabled());
  await user.click(symbolInput);
  await user.type(symbolInput, 'SOLUSDC');
  await user.click(await screen.findByTitle('SOLUSDC'));
  expect(await screen.findByText(/行情与交易规则加载失败：预览超时/)).toBeInTheDocument();
  await user.click(screen.getByRole('button', { name: '重试' }));
  expect(await screen.findByText('最新价格：145.2 USDC')).toBeInTheDocument();
  expect(previewAttempts).toBe(2);
});

it('clears red validation errors when reverting dirty fields on PairConfig', async () => {
  mocks.confirm.mockResolvedValue(true);
  setup();
  const user = await prepareLive();
  const interval = await screen.findByLabelText('网格间距');
  await waitFor(() => expect(interval).toBeEnabled());
  await user.clear(interval);
  await user.type(interval, '0');
  await user.click(screen.getByRole('button', { name: '保存并立即生效' }));
  expect(await screen.findByText('网格间距必须大于 0')).toBeInTheDocument();
  await user.click(screen.getByRole('button', { name: /还\s*原/ }));
  await waitFor(() => expect(interval).toHaveValue('0.1'));
  await waitFor(() => expect(screen.queryByText('网格间距必须大于 0')).not.toBeInTheDocument());
});

it('ignores global shortcuts during IME composition and skips redundant same-route navigations', async () => {
  const router = setup('/settings');
  const navigateSpy = vi.spyOn(router, 'navigate');
  await screen.findByText('设置页面');
  fireEvent.keyDown(window, { key: '?', isComposing: true });
  fireEvent.keyDown(window, { key: '?', keyCode: 229 });
  expect(screen.queryByRole('dialog', { name: '键盘快捷键' })).not.toBeInTheDocument();
  fireEvent.keyDown(window, { key: 'g' });
  fireEvent.keyDown(window, { key: 's' });
  expect(navigateSpy).not.toHaveBeenCalled();
  fireEvent.keyDown(window, { key: 'g' });
  fireEvent.keyDown(window, { key: 'o' });
  await waitFor(() => expect(router.state.location.pathname).toBe('/'));
});

it('disables the form while preflight refetchGlobal is pending and aborts an open save submission on revert', async () => {
  const preflight = Promise.withResolvers<{ version: string; has_api_key: boolean; has_api_secret: boolean }>();
  let globalCalls = 0;
  mocks.api.mockImplementation((path: string) => {
    if (path === '/api/global/config') {
      globalCalls++;
      return globalCalls === 1
        ? Promise.resolve({ version: 'global-version', has_api_key: true, has_api_secret: true })
        : preflight.promise;
    }
    return Promise.resolve({ ...pair('BTCUSDC').grid_config, config_version: 'version-1' });
  });
  const saveConfirm = Promise.withResolvers<boolean>();
  mocks.confirm.mockReturnValueOnce(saveConfirm.promise).mockResolvedValueOnce(true);

  setup();
  const user = await prepareLive();
  const interval = await screen.findByLabelText('网格间距');
  await waitFor(() => expect(interval).toBeEnabled());
  await user.clear(interval);
  await user.type(interval, '0.2');

  const saveButton = screen.getByRole('button', { name: '保存并立即生效' });
  await user.click(saveButton);
  await waitFor(() => expect(globalCalls).toBe(2));
  expect(saveButton).toHaveClass('ant-btn-loading');
  expect(interval).toBeDisabled();
  expect(mocks.confirm).not.toHaveBeenCalled();

  await act(async () => { preflight.resolve({ version: 'global-version', has_api_key: true, has_api_secret: true }); });
  await waitFor(() => expect(mocks.confirm).toHaveBeenCalledTimes(1));
  expect(interval).toBeEnabled();

  await user.click(screen.getByRole('button', { name: /还\s*原/ }));
  await waitFor(() => expect(interval).toHaveValue('0.1'));
  await act(async () => { saveConfirm.resolve(true); });
  expect(mocks.post).not.toHaveBeenCalled();
});
