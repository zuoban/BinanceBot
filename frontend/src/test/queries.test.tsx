/// <reference lib="es2024.promise" />
import { useLayoutEffect } from 'react';
import { act, renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { afterEach, expect, it, vi } from 'vitest';
import { useLiveAccounts, usePairSnapshot, useSocket } from '../lib/queries';
import { tokenStore } from '../lib/api';
import { pair, snapshot } from './fixtures';
import { useExpired } from '../lib/useNow';

class FakeSocket {
  static instances: FakeSocket[] = [];
  onmessage?: (event: { data: string }) => void;
  onclose?: () => void;
  onerror?: () => void;
  constructor(public url: string, public protocols?: string | string[]) { FakeSocket.instances.push(this); }
  close() { this.onclose?.(); }
  sendSnapshot(symbol: string, price: string) {
    const data = snapshot(symbol); data.ticker.last_price = price;
    this.onmessage?.({ data: JSON.stringify({ type: 'snapshot', data }) });
  }
}
afterEach(() => { vi.unstubAllGlobals(); FakeSocket.instances = []; });
it('only delivers the message type expected by a socket', () => {
  tokenStore.set('test-token');
  vi.stubGlobal('WebSocket', FakeSocket);
  const onAccounts = vi.fn();
  const { result } = renderHook(() => useSocket('/api/accounts/ws', 'accounts', onAccounts));
  act(() => FakeSocket.instances[0].sendSnapshot('SOLUSDC', '123'));
  expect(onAccounts).not.toHaveBeenCalled();
  expect(result.current.connected).toBe(false);
  const data = { generated_at: new Date().toISOString(), pairs: [] };
  act(() => FakeSocket.instances[0].onmessage?.({ data: JSON.stringify({ type: 'accounts', data }) }));
  expect(onAccounts).toHaveBeenCalledWith(data);
  expect(result.current.connected).toBe(true);
});

it('expires data even when its deadline passes between rendering and effects, and resets on refresh', async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date('2025-01-01T00:00:00Z'));
  const initialDeadline = Date.now() + 1;
  let renders = 0;
  const { result, rerender, unmount } = renderHook(({ deadline }) => {
    renders++;
    const expired = useExpired(deadline);
    useLayoutEffect(() => { vi.setSystemTime(initialDeadline + 1); }, []);
    return expired;
  }, { initialProps: { deadline: initialDeadline } });
  try {
    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    expect(result.current).toBe(true);
    rerender({ deadline: Date.now() + 10_000 });
    expect(result.current).toBe(false);
    const freshRenders = renders;
    await act(async () => { await vi.advanceTimersByTimeAsync(10_000); });
    expect(result.current).toBe(false);
    expect(renders).toBe(freshRenders);
    await act(async () => { await vi.advanceTimersByTimeAsync(1); });
    expect(result.current).toBe(true);
  } finally {
    unmount();
    vi.useRealTimers();
  }
});

it('updates socket activity without rendering healthy frames and publishes disconnects and recovery', async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date('2025-01-01T00:00:00Z'));
  vi.stubGlobal('WebSocket', FakeSocket);
  const onData = vi.fn();
  let renders = 0;
  const { result, unmount } = renderHook(() => {
    renders++;
    return useSocket('/api/accounts/ws', 'accounts', onData);
  });
  const send = () => FakeSocket.instances.at(-1)!.onmessage?.({ data: JSON.stringify({ type: 'accounts', data: { generated_at: new Date().toISOString(), pairs: [] } }) });
  try {
    act(send);
    const live = result.current, healthyRenders = renders;
    for (let i = 0; i < 4; i++) {
      await act(async () => { await vi.advanceTimersByTimeAsync(1000); send(); });
      expect(result.current).toBe(live);
      expect(result.current.lastReceived).toBe(Date.now());
      expect(renders).toBe(healthyRenders);
    }
    onData.mockReturnValueOnce(false);
    await act(async () => { await vi.advanceTimersByTimeAsync(1000); send(); });
    expect(result.current.lastReceived).toBe(Date.now() - 1000);
    expect(renders).toBe(healthyRenders);
    await act(async () => { await vi.advanceTimersByTimeAsync(4000); send(); });
    expect(result.current.connected).toBe(true);
    expect(result.current).not.toBe(live);
    expect(result.current.lastReceived).toBe(Date.now());
    act(() => FakeSocket.instances.at(-1)!.close());
    expect(result.current.connected).toBe(false);
    await act(async () => { await vi.advanceTimersByTimeAsync(1000); send(); });
    expect(FakeSocket.instances).toHaveLength(2);
    expect(result.current.connected).toBe(true);
  } finally {
    unmount();
    vi.useRealTimers();
  }
});

it('polls lightweight accounts after silence or rejected older frames and marks old observations stale', async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date('2025-01-01T00:00:00Z'));
  tokenStore.set('test-token');
  vi.stubGlobal('WebSocket', FakeSocket);
  const http = { generated_at: new Date().toISOString(), pairs: [{ snapshot: snapshot(), mode: 'PAPER' }] };
  const fetchMock = vi.fn().mockResolvedValueOnce(new Response(JSON.stringify({ success: true, data: http }), { status: 200 }))
    .mockImplementation(() => Promise.withResolvers<Response>().promise);
  vi.stubGlobal('fetch', fetchMock);
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const wrapper = ({ children }: { children: React.ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
  let renders = 0;
  const { result, unmount } = renderHook(() => {
    renders++;
    return useLiveAccounts();
  }, { wrapper });
  try {
    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    expect(result.current.accounts[0].snapshot.position.size).toBe('0');
    const streamed = snapshot();
    streamed.position.size = '2';
    streamed.ticker.mark_price = '123';
    act(() => FakeSocket.instances[0].onmessage?.({ data: JSON.stringify({ type: 'accounts', data: { generated_at: new Date().toISOString(), pairs: [{ mode: 'PAPER', snapshot: streamed }] } }) }));
    expect(result.current.accounts[0].snapshot.position.size).toBe('2');
    expect(result.current.accounts[0].snapshot.ticker.mark_price).toBe('123');
    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    const requests = fetchMock.mock.calls.length, healthyRenders = renders;
    await act(async () => { await vi.advanceTimersByTimeAsync(4999); });
    expect(fetchMock).toHaveBeenCalledTimes(requests);
    expect(renders).toBe(healthyRenders);
    const older = snapshot();
    older.position.size = '99';
    act(() => FakeSocket.instances[0].onmessage?.({ data: JSON.stringify({ type: 'accounts', data: { generated_at: '2024-12-31T23:59:59Z', pairs: [{ mode: 'PAPER', snapshot: older }] } }) }));
    expect(result.current.accounts[0].snapshot.position.size).toBe('2');
    await act(async () => { await vi.advanceTimersByTimeAsync(1); });
    expect(fetchMock).toHaveBeenCalledTimes(requests + 1);
    expect(result.current.accounts[0].account_observation_stale).toBe(false);
    await act(async () => { await vi.advanceTimersByTimeAsync(6000); });
    expect(result.current.accounts[0].account_observation_stale).toBe(true);
    expect(fetchMock.mock.calls.every(([path]) => path === '/api/accounts')).toBe(true);
  } finally {
    unmount();
    client.clear();
    vi.useRealTimers();
  }
});

it('keeps current account membership and modes and rejects late HTTP or older socket frames', async () => {
  tokenStore.set('test-token');
  vi.stubGlobal('WebSocket', FakeSocket);
  const pending = Promise.withResolvers<Response>();
  const fetchMock = vi.fn().mockReturnValue(pending.promise);
  vi.stubGlobal('fetch', fetchMock);
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const wrapper = ({ children }: { children: React.ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
  const { result, unmount } = renderHook(() => useLiveAccounts(), { wrapper });
  const send = (data: unknown) => act(() => FakeSocket.instances[0].onmessage?.({ data: JSON.stringify({ type: 'accounts', data }) }));
  const old = { generated_at: '2026-01-01T00:00:00Z', pairs: [{ mode: 'PAPER', snapshot: snapshot() }] };
  const current = { generated_at: '2026-01-01T00:00:01Z', pairs: [{ mode: 'LIVE', snapshot: snapshot('SOLUSDC', 'LIVE') }, { mode: 'PAPER', snapshot: snapshot() }] };
  try {
    await waitFor(() => expect(fetchMock).toHaveBeenCalledOnce());
    send(current);
    expect(result.current.accounts.map(entry => entry.mode)).toEqual(['LIVE', 'PAPER']);
    await act(async () => { pending.resolve(new Response(JSON.stringify({ success: true, data: old }), { status: 200 })); });
    send(old);
    expect(result.current.accounts.map(entry => entry.mode)).toEqual(['LIVE', 'PAPER']);
    send({ generated_at: '2026-01-01T00:00:02Z', pairs: [] });
    await waitFor(() => expect(result.current.accounts).toEqual([]));
  } finally {
    unmount();
    client.clear();
  }
});

it('ignores late frames after switching pairs and retains the new pair snapshot', async () => {
  tokenStore.set('test-token');
  vi.stubGlobal('WebSocket', FakeSocket);
  vi.stubGlobal('fetch', vi.fn().mockImplementation((path: string) => {
    const symbol = path.includes('BTCUSDC') ? 'BTCUSDC' : 'SOLUSDC';
    return Promise.resolve(new Response(JSON.stringify({ success: true, data: snapshot(symbol) }), { status: 200 }));
  }));
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const wrapper = ({ children }: { children: React.ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
  const { result, rerender } = renderHook(({ selected }) => usePairSnapshot(selected), { wrapper, initialProps: { selected: pair('SOLUSDC') } });
  await waitFor(() => expect(result.current.data?.symbol).toBe('SOLUSDC'));
  const old = FakeSocket.instances[0];
  rerender({ selected: pair('BTCUSDC') });
  await waitFor(() => expect(result.current.data?.symbol).toBe('BTCUSDC'));
  act(() => { FakeSocket.instances.at(-1)!.sendSnapshot('SOLUSDC', '999'); });
  expect(result.current.live).toBe(false);
  act(() => { old.sendSnapshot('SOLUSDC', '999'); FakeSocket.instances.at(-1)!.sendSnapshot('BTCUSDC', '123'); });
  expect(result.current.live).toBe(true);
  expect(result.current.data?.symbol).toBe('BTCUSDC');
  expect(result.current.data?.ticker.last_price).toBe('123');
  client.clear();
});

it('does not let a slow HTTP response replace a newer socket snapshot', async () => {
  tokenStore.set('test-token');
  vi.stubGlobal('WebSocket', FakeSocket);
  let finish!: (response: Response) => void;
  vi.stubGlobal('fetch', vi.fn().mockImplementation(() => new Promise<Response>(resolve => { finish = resolve; })));
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const wrapper = ({ children }: { children: React.ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
  const { result } = renderHook(() => usePairSnapshot(pair()), { wrapper });
  await waitFor(() => expect(FakeSocket.instances).toHaveLength(1));
  act(() => FakeSocket.instances[0].sendSnapshot('SOLUSDC', '123'));
  await waitFor(() => expect(result.current.data?.ticker.last_price).toBe('123'));
  const old = snapshot(); old.ticker.last_price = '99';
  await act(async () => { finish(new Response(JSON.stringify({ success: true, data: old }), { status: 200 })); await Promise.resolve(); });
  expect(result.current.data?.ticker.last_price).toBe('123');
  client.clear();
});

it('falls back at five seconds, marks old data stale, and recovers on a new frame', async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date('2025-01-01T00:00:00Z'));
  tokenStore.set('test-token');
  vi.stubGlobal('WebSocket', FakeSocket);
  const fetchMock = vi.fn().mockImplementation(() => new Promise<Response>(() => {}));
  vi.stubGlobal('fetch', fetchMock);
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const wrapper = ({ children }: { children: React.ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
  const { result, unmount } = renderHook(() => usePairSnapshot(pair()), { wrapper });
  try {
    act(() => FakeSocket.instances[0].sendSnapshot('SOLUSDC', '123'));
    expect(result.current.live).toBe(true);
    expect(result.current.stale).toBe(false);
    expect(result.current.data?.ticker.last_price).toBe('123');
    const initialRequests = fetchMock.mock.calls.length;

    await act(async () => { await vi.advanceTimersByTimeAsync(4999); });
    expect(result.current.live).toBe(true);
    expect(fetchMock).toHaveBeenCalledTimes(initialRequests);
    await act(async () => { await vi.advanceTimersByTimeAsync(1); });
    expect(result.current.live).toBe(false);
    expect(fetchMock).toHaveBeenCalledTimes(initialRequests + 1);

    await act(async () => { await vi.advanceTimersByTimeAsync(5000); });
    expect(result.current.stale).toBe(false);
    await act(async () => { await vi.advanceTimersByTimeAsync(1000); });
    expect(result.current.stale).toBe(true);
    act(() => FakeSocket.instances[0].sendSnapshot('SOLUSDC', '456'));
    expect(result.current.live).toBe(true);
    expect(result.current.stale).toBe(false);
    expect(result.current.data?.ticker.last_price).toBe('456');

    unmount();
    const requestsAtUnmount = fetchMock.mock.calls.length;
    await act(async () => { await vi.advanceTimersByTimeAsync(20_000); });
    expect(fetchMock).toHaveBeenCalledTimes(requestsAtUnmount);
  } finally {
    unmount();
    client.clear();
    vi.useRealTimers();
  }
});

it('skips scheduling a timer when already expired during render and clamps multi-month deadlines', () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date('2025-01-01T00:00:00Z'));
  const timeoutSpy = vi.spyOn(window, 'setTimeout');
  try {
    const past = renderHook(() => useExpired(Date.now() - 1000));
    expect(past.result.current).toBe(true);
    expect(timeoutSpy).not.toHaveBeenCalled();
    past.unmount();

    const farFuture = Date.now() + 60 * 86_400_000;
    const future = renderHook(() => useExpired(farFuture));
    expect(future.result.current).toBe(false);
    expect(timeoutSpy).toHaveBeenCalledWith(expect.any(Function), 2_147_483_647);
    future.unmount();
  } finally {
    timeoutSpy.mockRestore();
    vi.useRealTimers();
  }
});

it('ignores non-object JSON frames and detaches socket handlers before closing on unmount', () => {
  vi.stubGlobal('WebSocket', FakeSocket);
  const onData = vi.fn();
  const { result, unmount } = renderHook(() => useSocket('/api/accounts/ws', 'accounts', onData));
  const sock = FakeSocket.instances[0];
  act(() => {
    sock.onmessage?.({ data: 'null' });
    sock.onmessage?.({ data: '42' });
    sock.onmessage?.({ data: '"plain"' });
  });
  expect(onData).not.toHaveBeenCalled();
  expect(result.current.connected).toBe(false);
  let handlersClearedBeforeClose = false;
  const origClose = sock.close.bind(sock);
  sock.close = () => {
    handlersClearedBeforeClose = sock.onmessage === null && sock.onclose === null && sock.onerror === null;
    origClose();
  };
  unmount();
  expect(handlersClearedBeforeClose).toBe(true);
});

it('does not mark fresh live accounts stale when browser clock is ahead of server generated_at', async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date('2025-01-01T00:01:00Z'));
  vi.stubGlobal('WebSocket', FakeSocket);
  const lagged = { generated_at: '2025-01-01T00:00:00Z', pairs: [{ snapshot: snapshot(), mode: 'PAPER' }] };
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({ success: true, data: lagged }), { status: 200 })));
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const wrapper = ({ children }: { children: React.ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
  const { result, unmount } = renderHook(() => useLiveAccounts(), { wrapper });
  try {
    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    expect(result.current.stale).toBe(false);
    expect(result.current.accounts[0].account_observation_stale).toBe(false);
  } finally {
    unmount();
    client.clear();
    vi.useRealTimers();
  }
});

it('omits malformed sessionStorage tokens from WebSocket subprotocols and survives synchronous WebSocket errors', async () => {
  vi.useFakeTimers();
  tokenStore.set('corrupted token with spaces');
  vi.stubGlobal('WebSocket', FakeSocket);
  const first = renderHook(() => useSocket('/api/accounts/ws', 'accounts', vi.fn()));
  expect(FakeSocket.instances[0].protocols).toEqual(['auth']);
  first.unmount();

  let calls = 0;
  vi.stubGlobal('WebSocket', class extends FakeSocket {
    constructor(url: string, protocols?: string | string[]) {
      if (calls++ === 0) throw new DOMException('Invalid subprotocol', 'SyntaxError');
      super(url, protocols);
    }
  });
  const second = renderHook(() => useSocket('/api/accounts/ws', 'accounts', vi.fn()));
  try {
    expect(FakeSocket.instances).toHaveLength(1);
    await act(async () => { await vi.advanceTimersByTimeAsync(1000); });
    expect(FakeSocket.instances).toHaveLength(2);
  } finally {
    second.unmount();
    tokenStore.clear();
    vi.useRealTimers();
  }
});
