import { useEffect, useRef, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { api, pairPath, socketUrl, tokenStore } from './api';
import type { HourlySnapshot, Overview, Pair, Snapshot } from './types';

const SOCKET_LIVE_MS = 5000;
const HTTP_FALLBACK_MS = 3000;
const SNAPSHOT_STALE_MS = 10_000;

export function usePairs() {
  return useQuery({ queryKey: ['pairs'], queryFn: ({ signal }) => api<Pair[]>('/api/pairs', { signal }), refetchInterval: 10_000 });
}
export function useOverview() {
  return useQuery({ queryKey: ['overview'], queryFn: ({ signal }) => api<Overview>('/api/overview', { signal }), refetchInterval: 3000 });
}
export function useOverviewHistory() {
  return useQuery({ queryKey: ['overview-history'], queryFn: ({ signal }) => api<HourlySnapshot[]>('/api/overview/history', { signal }), refetchInterval: 60_000 });
}

export function useSocket<T>(path: string | undefined, onData: (data: T) => void) {
  const callback = useRef(onData);
  callback.current = onData;
  const [lastReceived, setLastReceived] = useState(0);
  const [connected, setConnected] = useState(false);
  useEffect(() => {
    setLastReceived(0); setConnected(false);
    if (!path) return;
    let stopped = false, socket: WebSocket | undefined, retry: ReturnType<typeof setTimeout>;
    let attempts = 0;
    const connect = () => {
      const token = tokenStore.get();
      if (stopped || !token) return;
      socket = new WebSocket(socketUrl(path), ['auth', `token.${token}`]);
      socket.onmessage = event => {
        if (stopped) return;
        try {
          const envelope = JSON.parse(event.data);
          if (!['snapshot', 'accounts'].includes(envelope.type)) return;
          callback.current(envelope.data);
          attempts = 0; setConnected(true); setLastReceived(Date.now());
        } catch { /* Ignore malformed frames; HTTP remains the fallback. */ }
      };
      socket.onclose = () => {
        if (stopped) return;
        setConnected(false);
        retry = setTimeout(connect, Math.min(1000 * 2 ** attempts++, 15_000));
      };
      socket.onerror = () => socket?.close();
    };
    connect();
    return () => { stopped = true; clearTimeout(retry); socket?.close(); };
  }, [path]);
  return { lastReceived, connected };
}

export function usePairSnapshot(pair: Pair | undefined) {
  const client = useQueryClient();
  const lastFallback = useRef(0);
  const key = ['snapshot', pair?.symbol, pair?.scoped];
  const acceptSnapshot = (data: Snapshot) => {
    if (data.symbol !== pair?.symbol) return;
    // Cancel a pre-frame HTTP read before publishing the newer socket observation.
    void client.cancelQueries({ queryKey: key });
    client.setQueryData(key, data);
  };
  const socket = useSocket<Snapshot>(pair && pairPath(pair, 'ws'), acceptSnapshot);
  const query = useQuery({
    queryKey: key,
    queryFn: ({ signal }) => api<Snapshot>(pairPath(pair!, 'status'), { signal }),
    enabled: !!pair,
    refetchInterval: () => socket.connected && Date.now() - socket.lastReceived < SOCKET_LIVE_MS ? false : HTTP_FALLBACK_MS,
  });
  const [now, setNow] = useState(Date.now());
  useEffect(() => { const timer = setInterval(() => setNow(Date.now()), 1000); return () => clearInterval(timer); }, []);
  // The clock also restarts polling when an open socket silently stops sending.
  useEffect(() => {
    if (pair && now - socket.lastReceived >= SOCKET_LIVE_MS && now - query.dataUpdatedAt >= HTTP_FALLBACK_MS && now - lastFallback.current >= HTTP_FALLBACK_MS && !query.isFetching) {
      lastFallback.current = now;
      void query.refetch();
    }
  }, [now, socket.lastReceived, pair, query]);
  return { ...query, live: socket.connected && now - socket.lastReceived < SOCKET_LIVE_MS, stale: now - query.dataUpdatedAt > SNAPSHOT_STALE_MS };
}

type AccountFrame = { pairs: { mode: Overview['pairs'][number]['mode']; snapshot: Pick<Snapshot, 'symbol' | 'dry_run' | 'account'> & { position: Pick<Snapshot['position'], 'unrealized_pnl'> } }[] };
export function useLiveAccounts(overview: Overview | undefined) {
  const [frame, setFrame] = useState<AccountFrame>();
  const stream = useSocket<AccountFrame>('/api/accounts/ws', setFrame);
  if (Date.now() - stream.lastReceived > SOCKET_LIVE_MS) return overview?.pairs ?? [];
  const accounts = new Map<AccountFrame['pairs'][number]['mode'], Map<string, AccountFrame['pairs'][number]>>();
  for (const account of frame?.pairs ?? []) {
    let symbols = accounts.get(account.mode);
    if (!symbols) {
      symbols = new Map();
      accounts.set(account.mode, symbols);
    }
    if (!symbols.has(account.snapshot.symbol)) symbols.set(account.snapshot.symbol, account);
  }
  return overview?.pairs.map(entry => {
    const account = accounts.get(entry.mode)?.get(entry.snapshot.symbol);
    if (!account) return entry;
    return { ...entry, snapshot: { ...entry.snapshot, account: account.snapshot.account, position: { ...entry.snapshot.position, ...account.snapshot.position } } };
  }) ?? [];
}
