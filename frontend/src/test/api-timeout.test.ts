import { afterEach, expect, it, vi } from 'vitest';
import { api, ApiError } from '../lib/api';
import { downloadDatabase } from '../lib/database';

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

it('uses the 120s timeout for database exports instead of the default 15s timeout', async () => {
  const timeout = new AbortController();
  const timeoutFactory = vi.spyOn(AbortSignal, 'timeout').mockReturnValue(timeout.signal);
  vi.stubGlobal('fetch', pendingFetch());
  const request = downloadDatabase('/api/database/export');
  const result = expect(request).rejects.toMatchObject({ status: 0 });
  expect(timeoutFactory).toHaveBeenCalledOnce();
  expect(timeoutFactory).toHaveBeenCalledWith(120_000);
  timeout.abort(new DOMException('Timed out', 'TimeoutError'));
  await result;
});

it('distinguishes export body stream read failure from connection failure with ApiError', async () => {
  const brokenResponse = {
    ok: true,
    status: 200,
    headers: new Headers({ 'Content-Disposition': 'attachment; filename="snap.db"' }),
    blob: () => Promise.reject(new TypeError('network stream interrupted')),
  } as unknown as Response;
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(brokenResponse));
  const failure = downloadDatabase('/api/database/export');
  await expect(failure).rejects.toBeInstanceOf(ApiError);
  await expect(failure).rejects.toMatchObject({ message: '读取导出文件失败，请重试', status: 200, kind: 'invalid-response' });
});

it('reports connection error instead of uncertain mutation for read-only POST requests and strips readOnly from fetch', async () => {
  const timeout = new AbortController();
  vi.spyOn(AbortSignal, 'timeout').mockReturnValue(timeout.signal);
  const fetchMock = pendingFetch();
  vi.stubGlobal('fetch', fetchMock);
  const request = downloadDatabase('/api/database/export', ['trade_history']);
  const result = expect(request).rejects.toMatchObject({ message: '无法连接后端服务，请检查网络后重试', status: 0 });
  expect(fetchMock.mock.calls[0][1]).not.toHaveProperty('readOnly');
  timeout.abort(new DOMException('Timed out', 'TimeoutError'));
  await result;
});

it('skips file download if signal is aborted while reading the export blob', async () => {
  const caller = new AbortController();
  const createObjectURL = vi.spyOn(URL, 'createObjectURL');
  const response = {
    ok: true,
    status: 200,
    headers: new Headers({ 'Content-Disposition': 'attachment; filename="snap.db"' }),
    blob: async () => {
      caller.abort(new DOMException('Unmounted', 'AbortError'));
      return new Blob(['sqlite']);
    },
  } as unknown as Response;
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response));
  await downloadDatabase('/api/database/export', undefined, caller.signal);
  expect(createObjectURL).not.toHaveBeenCalled();
});
