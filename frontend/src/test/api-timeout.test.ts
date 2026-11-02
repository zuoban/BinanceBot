import { afterEach, expect, it, vi } from 'vitest';
import { api } from '../lib/api';

afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals(); });

function pendingFetch() {
  return vi.fn((_path: string, init: RequestInit) => new Promise<Response>((_resolve, reject) => {
    const signal = init.signal!;
    if (signal.aborted) reject(signal.reason);
    else signal.addEventListener('abort', () => reject(signal.reason), { once: true });
  }));
}

it('times out a stalled request even when the caller supplies a cancellation signal', async () => {
  const timeout = new AbortController();
  const caller = new AbortController();
  const timeoutFactory = vi.spyOn(AbortSignal, 'timeout').mockReturnValue(timeout.signal);
  vi.stubGlobal('fetch', pendingFetch());
  const request = api('/api/overview', { signal: caller.signal });
  const result = expect(request).rejects.toMatchObject({ message: '无法连接后端服务，请检查网络后重试', status: 0 });
  expect(timeoutFactory).toHaveBeenCalledWith(15_000);
  timeout.abort(new DOMException('Timed out', 'TimeoutError'));
  await result;
  expect(caller.signal.aborted).toBe(false);
});

it('uses the caller-selected timeout and reports uncertain results for mutations', async () => {
  const timeout = new AbortController();
  const timeoutFactory = vi.spyOn(AbortSignal, 'timeout').mockReturnValue(timeout.signal);
  const fetchMock = pendingFetch();
  vi.stubGlobal('fetch', fetchMock);
  const request = api('/api/pairs/BTCUSDC', { method: 'DELETE', timeoutMs: 90_000 });
  const result = expect(request).rejects.toMatchObject({
    message: '请求未完成，操作可能已生效；请刷新状态或查看日志后再操作', status: 0,
  });
  expect(timeoutFactory).toHaveBeenCalledOnce();
  expect(timeoutFactory).toHaveBeenCalledWith(90_000);
  expect(fetchMock.mock.calls[0][1]).not.toHaveProperty('timeoutMs');
  timeout.abort(new DOMException('Timed out', 'TimeoutError'));
  await result;
});

it('preserves caller cancellation without treating it as a connection failure', async () => {
  const timeout = new AbortController();
  const caller = new AbortController();
  vi.spyOn(AbortSignal, 'timeout').mockReturnValue(timeout.signal);
  vi.stubGlobal('fetch', pendingFetch());
  const request = api('/api/overview', { signal: caller.signal });
  const reason = new DOMException('Cancelled', 'AbortError');
  const result = expect(request).rejects.toBe(reason);
  caller.abort(reason);
  await result;
  expect(timeout.signal.aborted).toBe(false);
});
