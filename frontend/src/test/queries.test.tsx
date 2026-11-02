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
  const data = { pairs: [] };
  act(() => FakeSocket.instances[0].onmessage?.({ data: JSON.stringify({ type: 'accounts', data }) }));
  expect(onAccounts).toHaveBeenCalledWith(data);
  expect(result.current.connected).toBe(true);
});

it('expires silent account frames, falls back to HTTP and marks an old HTTP observation stale', async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date('2025-01-01T00:00:00Z'));
  tokenStore.set('test-token');
  vi.stubGlobal('WebSocket', FakeSocket);
  const http = snapshot();
  const overview = { generated_at: '', pairs: [{ snapshot: http, mode: 'PAPER' as const, scoped: true }] };
  const updatedAt = Date.now();
  const { result, unmount } = renderHook(() => useLiveAccounts(overview, updatedAt));
  try {
    const streamed = snapshot();
    streamed.position.size = '2';
    streamed.ticker.mark_price = '123';
    act(() => FakeSocket.instances[0].onmessage?.({ data: JSON.stringify({ type: 'accounts', data: { pairs: [{ mode: 'PAPER', snapshot: streamed }] } }) }));
    expect(result.current[0].snapshot.position.size).toBe('2');
    expect(result.current[0].snapshot.ticker.mark_price).toBe('123');
    await act(async () => { await vi.advanceTimersByTimeAsync(5000); });
    expect(result.current[0].snapshot.position.size).toBe('0');
    expect(result.current[0].account_observation_stale).toBe(false);
    await act(async () => { await vi.advanceTimersByTimeAsync(6000); });
    expect(result.current[0].account_observation_stale).toBe(true);
  } finally {
    unmount();
    vi.useRealTimers();
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
