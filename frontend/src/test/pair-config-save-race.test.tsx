import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type * as Antd from 'antd';
import type * as Api from '../lib/api';
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
vi.mock('../lib/queries', () => {
  return { usePairs: () => ({ data: [pair('BTCUSDC'), pair('ETHUSDC')] }), useOverview: () => ({ data: undefined }), useLiveAccounts: () => [] };
});
vi.mock('../components/Controls', () => ({ ControlProvider: ({ children }: { children: React.ReactNode }) => children, useControls: () => ({ results: {} }) }));
vi.mock('../components/ThemeSwitch', () => ({ ThemeSwitch: () => null, ThemeSegmented: () => null, themeLabels: { system: '跟随系统', light: '浅色', dark: '深色' } }));

beforeEach(() => {
  vi.clearAllMocks();
  mocks.confirm.mockReset();
  mocks.post.mockReset();
  vi.stubGlobal('matchMedia', vi.fn().mockImplementation(query => ({ matches: false, media: query, addListener: vi.fn(), removeListener: vi.fn(), addEventListener: vi.fn(), removeEventListener: vi.fn() })));
  vi.stubGlobal('ResizeObserver', class { observe() {} unobserve() {} disconnect() {} });
  mocks.api.mockImplementation((path: string) => path === '/api/global/config'
    ? Promise.resolve({ version: 'global-version', has_api_key: true, has_api_secret: true })
    : Promise.resolve({ ...pair(path.includes('ETHUSDC') ? 'ETHUSDC' : 'BTCUSDC').grid_config, config_version: 'version-1' }));
});

function setup() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const router = createMemoryRouter([{ element: <Shell />, children: [
    { path: '/', element: <div>总览页面</div> },
    { path: '/pairs/:symbol', element: <div>策略详情</div> },
    { path: '/pairs/:symbol/config', element: <PairConfig /> },
  ] }], { initialEntries: ['/pairs/BTCUSDC/config'] });
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

it('does not save the previous pair after switching to another configuration', async () => {
  let confirmSave!: (value: boolean) => void;
  mocks.confirm.mockReturnValueOnce(new Promise<boolean>(resolve => { confirmSave = resolve; })).mockResolvedValueOnce(true);
  const router = setup();
  const user = await prepareLive();
  await user.click(screen.getByRole('button', { name: '保存并立即生效' }));
  await waitFor(() => expect(mocks.confirm).toHaveBeenCalledTimes(1));
  act(() => { void router.navigate('/pairs/ETHUSDC/config'); });
  await waitFor(() => expect(router.state.location.pathname).toBe('/pairs/ETHUSDC/config'));
  await act(async () => { confirmSave(true); });
  expect(mocks.post).not.toHaveBeenCalled();
});
