import { act, renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { afterEach, expect, it, vi } from 'vitest';
import { usePairSnapshot } from '../lib/queries';
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
