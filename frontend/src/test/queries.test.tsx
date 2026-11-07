/// <reference lib="es2024.promise" />
import { act, renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { afterEach, expect, it, vi } from 'vitest';
import { useLiveAccounts, usePairSnapshot, useSocket } from '../lib/queries';
import { tokenStore } from '../lib/api';
import { pair, snapshot } from './fixtures';

class FakeSocket {
  static instances: FakeSocket[] = [];
  onmessage?: (event: { data: string }) => void;
  onclose?: () => void;
  onerror?: () => void;
  constructor(public url: string) { FakeSocket.instances.push(this); }
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
  const { result, unmount } = renderHook(() => useLiveAccounts(), { wrapper });
  try {
    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    expect(result.current.accounts[0].snapshot.position.size).toBe('0');
    const streamed = snapshot();
    streamed.position.size = '2';
    streamed.ticker.mark_price = '123';
    act(() => FakeSocket.instances[0].onmessage?.({ data: JSON.stringify({ type: 'accounts', data: { generated_at: new Date().toISOString(), pairs: [{ mode: 'PAPER', snapshot: streamed }] } }) }));
    expect(result.current.accounts[0].snapshot.position.size).toBe('2');
    expect(result.current.accounts[0].snapshot.ticker.mark_price).toBe('123');
    const requests = fetchMock.mock.calls.length;
    await act(async () => { await vi.advanceTimersByTimeAsync(4999); });
    expect(fetchMock).toHaveBeenCalledTimes(requests);
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
    expect(result.current.accounts).toEqual([]);
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
  act(() => { old.sendSnapshot('SOLUSDC', '999'); FakeSocket.instances.at(-1)!.sendSnapshot('BTCUSDC', '123'); });
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
