import { useEffect, useRef, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { api, pairPath, socketUrl, tokenStore } from './api';
import type { AccountFrame, AccountObservation, OverviewHistoryData, Overview, Pair, Snapshot, SocketMessage } from './types';

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
  return useQuery({ queryKey: ['overview-history'], queryFn: ({ signal }) => api<OverviewHistoryData>('/api/overview/history', { signal }), refetchInterval: 60_000 });
}

type SocketDataByType = { [Message in SocketMessage as Message['type']]: Message['data'] };

export function useSocket<T extends keyof SocketDataByType>(path: string | undefined, type: T, onData: (data: SocketDataByType[T]) => void | boolean) {
  const callback = useRef<(data: SocketDataByType[T]) => void | boolean>(onData);
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
      if (stopped) return;
      socket = new WebSocket(socketUrl(path), token ? ['auth', `token.${token}`] : ['auth']);
      socket.onmessage = event => {
        if (stopped) return;
        try {
          const envelope = JSON.parse(event.data) as SocketMessage;
          if (envelope.type !== type) return;
          if (callback.current(envelope.data as SocketDataByType[T]) === false) return;
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
  }, [path, type]);
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
  const socket = useSocket(pair && pairPath(pair, 'ws'), 'snapshot', acceptSnapshot);
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

export function useLiveAccounts() {
  const client = useQueryClient();
  const lastFallback = useRef(0);
  const key = ['accounts'];
  const stream = useSocket('/api/accounts/ws', 'accounts', data => {
    const current = client.getQueryData<AccountFrame>(key);
    if (current && Date.parse(data.generated_at) < Date.parse(current.generated_at)) return false;
    void client.cancelQueries({ queryKey: key });
    client.setQueryData(key, data);
  });
  const query = useQuery({
    queryKey: key,
    queryFn: async ({ signal }) => {
      const data = await api<AccountFrame>('/api/accounts', { signal });
      const current = client.getQueryData<AccountFrame>(key);
      return current && Date.parse(data.generated_at) < Date.parse(current.generated_at) ? current : data;
    },
    refetchInterval: () => stream.connected && Date.now() - stream.lastReceived < SOCKET_LIVE_MS ? false : HTTP_FALLBACK_MS,
  });
  const [now, setNow] = useState(Date.now());
  useEffect(() => { const timer = setInterval(() => setNow(Date.now()), 1000); return () => clearInterval(timer); }, []);
  useEffect(() => {
    if (now - stream.lastReceived >= SOCKET_LIVE_MS && now - query.dataUpdatedAt >= HTTP_FALLBACK_MS && now - lastFallback.current >= HTTP_FALLBACK_MS && !query.isFetching) {
      lastFallback.current = now;
      void query.refetch();
    }
  }, [now, stream.lastReceived, query]);
  const stale = now - query.dataUpdatedAt > SNAPSHOT_STALE_MS || now - Date.parse(query.data?.generated_at ?? '') > SNAPSHOT_STALE_MS;
  const accounts: AccountObservation[] = (query.data?.pairs ?? []).map(entry => ({ ...entry, account_observation_stale: stale }));
  return { accounts, stale, error: query.error };
}
