import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { StrategyMenu } from '../components/StrategyMenu';
import type * as Api from '../lib/api';
import { pair } from './fixtures';

const mocks = vi.hoisted(() => ({ confirm: vi.fn(), success: vi.fn(), error: vi.fn(), api: vi.fn() }));
vi.mock('antd', () => ({
  App: { useApp: () => ({ modal: { confirm: mocks.confirm }, message: { success: mocks.success, error: mocks.error } }) },
  Button: ({ children, 'aria-label': label }: React.PropsWithChildren<{ 'aria-label': string }>) => <button aria-label={label}>{children}</button>,
  Dropdown: ({ menu }: { menu: { items: { key?: string; label?: string }[]; onClick: (info: { key: string }) => void } }) => <div>{menu.items.filter(item => item.key).map(item => <button key={item.key} onClick={() => menu.onClick({ key: item.key! })}>{item.label}</button>)}</div>,
}));
vi.mock('../lib/api', async importOriginal => ({ ...await importOriginal<typeof Api>(), api: mocks.api }));
vi.mock('react-router-dom', () => ({ useNavigate: () => vi.fn() }));
vi.mock('../components/Controls', () => ({ useControls: () => ({ busy: new Set(), run: vi.fn() }) }));

beforeEach(() => vi.clearAllMocks());
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals(); });
function setup() {
  const client = new QueryClient();
  const invalidate = vi.spyOn(client, 'invalidateQueries');
  render(<QueryClientProvider client={client}><StrategyMenu pair={pair('BTCUSDC')} stale={false} /></QueryClientProvider>);
  return invalidate;
}

it('deletes only after confirmation and refreshes strategy data', async () => {
  mocks.confirm.mockResolvedValue(true);
  mocks.api.mockResolvedValue(null);
  const invalidate = setup();
  await userEvent.setup().click(screen.getByRole('button', { name: '删除策略' }));
  await waitFor(() => expect(mocks.api).toHaveBeenCalledWith('/api/pairs/BTCUSDC', { method: 'DELETE', body: JSON.stringify({ confirm: true }), timeoutMs: 90_000 }));
  expect(mocks.confirm).toHaveBeenCalledOnce();
  expect(mocks.confirm.mock.invocationCallOrder[0]).toBeLessThan(mocks.api.mock.invocationCallOrder[0]);
  expect(invalidate).toHaveBeenCalledWith({ queryKey: ['pairs'] });
  expect(invalidate).toHaveBeenCalledWith({ queryKey: ['overview'] });
});

it('does not delete when cancelled and displays server errors', async () => {
  mocks.confirm.mockResolvedValueOnce(false).mockResolvedValueOnce(true);
  mocks.api.mockRejectedValueOnce(new Error('请先暂停该策略'));
  const invalidate = setup();
  const user = userEvent.setup();
  const button = screen.getByRole('button', { name: '删除策略' });
  await user.click(button);
  expect(mocks.api).not.toHaveBeenCalled();
  await user.click(button);
  await waitFor(() => expect(mocks.error).toHaveBeenCalledWith('请先暂停该策略'));
  expect(invalidate).toHaveBeenCalledWith({ queryKey: ['pairs'] });
  expect(invalidate).toHaveBeenCalledWith({ queryKey: ['overview'] });
});

it.each([200, 409, 500])('shows an explicit rejection without an uncertain result for status %s', async status => {
  const { api } = await vi.importActual<typeof Api>('../lib/api');
  mocks.confirm.mockResolvedValue(true);
  mocks.api.mockImplementation(api);
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({ success: false, message: '请先暂停该策略' }), { status })));
  const invalidate = setup();
  await userEvent.setup().click(screen.getByRole('button', { name: '删除策略' }));
  await waitFor(() => expect(mocks.error).toHaveBeenCalledWith('请先暂停该策略'));
  expect(invalidate).toHaveBeenCalledWith({ queryKey: ['pairs'] });
  expect(invalidate).toHaveBeenCalledWith({ queryKey: ['overview'] });
});

it.each([[200, 'invalid JSON'], [502, 'invalid JSON'], [200, 'null']] as const)('explains an uncertain deletion for an unusable response (status %s)', async (status, body) => {
  const { api } = await vi.importActual<typeof Api>('../lib/api');
  mocks.confirm.mockResolvedValue(true);
  mocks.api.mockImplementation(api);
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(body, { status })));
  const invalidate = setup();
  await userEvent.setup().click(screen.getByRole('button', { name: '删除策略' }));
  await waitFor(() => expect(mocks.error).toHaveBeenCalledWith(`服务返回异常 (${status})，请检查 API 连接；删除结果可能已生效，请刷新状态或查看日志后再操作`));
  expect(invalidate).toHaveBeenCalledWith({ queryKey: ['pairs'] });
  expect(invalidate).toHaveBeenCalledWith({ queryKey: ['overview'] });
});

it('explains an uncertain deletion after a timeout', async () => {
  const { api } = await vi.importActual<typeof Api>('../lib/api');
  const timeout = new AbortController();
  vi.spyOn(AbortSignal, 'timeout').mockReturnValue(timeout.signal);
  vi.stubGlobal('fetch', vi.fn((_path: string, init: RequestInit) => new Promise<Response>((_resolve, reject) => {
    init.signal!.addEventListener('abort', () => reject(init.signal!.reason), { once: true });
  })));
  mocks.confirm.mockResolvedValue(true);
  mocks.api.mockImplementation(api);
  const invalidate = setup();
  await userEvent.setup().click(screen.getByRole('button', { name: '删除策略' }));
  await waitFor(() => expect(mocks.api).toHaveBeenCalled());
  timeout.abort(new DOMException('Timed out', 'TimeoutError'));
  await waitFor(() => expect(mocks.error).toHaveBeenCalledWith('请求未完成，操作可能已生效；请刷新状态或查看日志后再操作'));
  expect(invalidate).toHaveBeenCalledWith({ queryKey: ['pairs'] });
  expect(invalidate).toHaveBeenCalledWith({ queryKey: ['overview'] });
});
