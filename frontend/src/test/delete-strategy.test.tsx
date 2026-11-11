import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { useDeleteStrategy } from '../components/DeleteStrategy';
import { ControlProvider } from '../components/Controls';
import { ApiError } from '../lib/api';
import type * as Api from '../lib/api';
import { pair } from './fixtures';

const mocks = vi.hoisted(() => ({ confirm: vi.fn(), success: vi.fn(), error: vi.fn(), warning: vi.fn(), api: vi.fn() }));
vi.mock('antd', () => ({
  App: { useApp: () => ({ modal: { confirm: mocks.confirm }, message: { success: mocks.success, error: mocks.error, warning: mocks.warning } }) },
  Input: ({ onChange, 'aria-label': label }: { onChange: (event: { target: { value: string } }) => void; 'aria-label': string }) => <input aria-label={label} onChange={event => onChange({ target: { value: event.target.value } })} />,
}));
vi.mock('../lib/api', async importOriginal => ({ ...await importOriginal<typeof Api>(), api: mocks.api }));

beforeEach(() => vi.clearAllMocks());
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals(); });
function Harness({ mode = 'PAPER' }: { mode?: 'PAPER' | 'LIVE' }) {
  const deletion = useDeleteStrategy();
  return <button onClick={() => void deletion.remove({ ...pair('BTCUSDC'), mode })}>删除策略</button>;
}
function setup(mode: 'PAPER' | 'LIVE' = 'PAPER') {
  const client = new QueryClient();
  const invalidate = vi.spyOn(client, 'invalidateQueries');
  const reset = vi.spyOn(client, 'resetQueries');
  render(<QueryClientProvider client={client}><ControlProvider><Harness mode={mode} /></ControlProvider></QueryClientProvider>);
  return Object.assign(invalidate, { reset });
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
  expect(invalidate).toHaveBeenCalledWith({ queryKey: ['accounts'] });
  expect(invalidate.reset).toHaveBeenCalledWith({ queryKey: ['overview-history'] });
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

it('appends the uncertain-mutation warning for 502 and 504 JSON gateway errors', async () => {
  mocks.confirm.mockResolvedValue(true);
  mocks.api.mockRejectedValueOnce(new ApiError('网关等待后端响应超时', 504));
  const invalidate = setup();
  const user = userEvent.setup();
  await user.click(screen.getByRole('button', { name: '删除策略' }));
  await waitFor(() => expect(mocks.error).toHaveBeenCalledWith('网关等待后端响应超时；删除结果可能已生效，请刷新状态或查看日志后再操作'));
  expect(invalidate).toHaveBeenCalledWith({ queryKey: ['pairs'] });
  expect(invalidate).toHaveBeenCalledWith({ queryKey: ['overview'] });

  const { api } = await vi.importActual<typeof Api>('../lib/api');
  mocks.api.mockImplementation(api);
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({ success: false, message: '无法连接后端服务，请检查服务是否已启动' }), { status: 502 })));
  await user.click(screen.getByRole('button', { name: '删除策略' }));
  await waitFor(() => expect(mocks.error).toHaveBeenLastCalledWith('无法连接后端服务，请检查服务是否已启动；删除结果可能已生效，请刷新状态或查看日志后再操作'));
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

it('keeps the delete button disabled for a live strategy until the symbol is typed back', async () => {
  const update = vi.fn();
  let resolve!: (value: boolean) => void;
  mocks.confirm.mockReturnValue(Object.assign(new Promise<boolean>(r => { resolve = r; }), { update }));
  setup('LIVE');
  await userEvent.setup().click(screen.getByRole('button', { name: '删除策略' }));
  const options = mocks.confirm.mock.calls[0][0];
  expect(options.okButtonProps).toMatchObject({ danger: true, disabled: true });
  render(options.content);
  const input = screen.getByLabelText('输入交易对名称以确认删除');
  await userEvent.setup().type(input, 'btcusd');
  expect(update).toHaveBeenLastCalledWith({ okButtonProps: { danger: true, disabled: true } });
  await userEvent.setup().type(input, 'c');
  expect(update).toHaveBeenLastCalledWith({ okButtonProps: { danger: true, disabled: false } });
  resolve(false);
  await waitFor(() => expect(mocks.api).not.toHaveBeenCalled());
});

it('does not require typing for a paper strategy', async () => {
  mocks.confirm.mockResolvedValue(false);
  setup('PAPER');
  await userEvent.setup().click(screen.getByRole('button', { name: '删除策略' }));
  expect(mocks.confirm.mock.calls[0][0].okButtonProps).toMatchObject({ danger: true, disabled: false });
});
