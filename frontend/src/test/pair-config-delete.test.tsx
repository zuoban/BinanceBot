import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { createMemoryRouter, RouterProvider } from 'react-router-dom';
import { beforeEach, expect, it, vi } from 'vitest';
import Shell from '../components/Shell';
import PairConfig from '../pages/PairConfig';
import { pair } from './fixtures';

const mocks = vi.hoisted(() => ({ confirm: vi.fn(), success: vi.fn(), error: vi.fn(), api: vi.fn() }));
vi.mock('antd', async importOriginal => {
  const antd = await importOriginal<typeof import('antd')>();
  return { ...antd, App: { useApp: () => ({ modal: { confirm: mocks.confirm }, message: { success: mocks.success, error: mocks.error } }) } };
});
vi.mock('../lib/api', async importOriginal => ({ ...await importOriginal<typeof import('../lib/api')>(), api: mocks.api }));
vi.mock('../lib/queries', async () => {
  const { pair } = await import('./fixtures');
  return { usePairs: () => ({ data: [pair('BTCUSDC'), pair('ETHUSDC')] }), useOverview: () => ({ data: undefined }), useLiveAccounts: () => [] };
});
vi.mock('../components/Controls', () => ({ ControlProvider: ({ children }: { children: React.ReactNode }) => children }));
vi.mock('../components/ThemeSwitch', () => ({ ThemeSwitch: () => null }));

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
    { path: '/pairs/:symbol/config', element: <PairConfig /> },
  ] }], { initialEntries: ['/pairs/BTCUSDC/config'] });
  render(<QueryClientProvider client={client}><RouterProvider router={router} /></QueryClientProvider>);
  return router;
}

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
