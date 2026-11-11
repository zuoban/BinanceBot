import { useEffect, useMemo, useRef, useState } from 'react';
import { useQuery, useQueryClient, type QueryClient, type QueryKey, type UseQueryResult } from '@tanstack/react-query';
import { api, pairPath, socketUrl, tokenStore } from './api';
import { SNAPSHOT_STALE_MS } from './format';
import type { AccountFrame, AccountObservation, OverviewHistoryData, Overview, Pair, Snapshot, SocketMessage } from './types';
import { useExpired, useNow } from './useNow';
const SOCKET_LIVE_MS = 5000;
const HTTP_FALLBACK_MS = 3000;

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
  const lastReceivedRef = useRef<{ path: string | undefined; time: number }>({ path, time: 0 });
  if (lastReceivedRef.current.path !== path) {
    lastReceivedRef.current = { path, time: 0 };
  }
  const [state, setState] = useState({ path, connected: false, epoch: 0 });
  useEffect(() => {
    lastReceivedRef.current = { path, time: 0 };
    setState({ path, connected: false, epoch: 0 });
    if (!path) return;
    let stopped = false, socket: WebSocket | undefined, retry: ReturnType<typeof setTimeout>;
    let attempts = 0, receiving = false;
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
          attempts = 0;
          const receivedAt = Date.now();
          const prevTime = lastReceivedRef.current.path === path ? lastReceivedRef.current.time : 0;
          lastReceivedRef.current = { path, time: receivedAt };
          if (!receiving || prevTime === 0 || receivedAt - prevTime >= SOCKET_LIVE_MS) {
            receiving = true;
            setState(prev => ({ path, connected: true, epoch: prev.epoch + 1 }));
          }
        } catch { /* Ignore malformed frames; HTTP remains the fallback. */ }
      };
      socket.onclose = () => {
        if (stopped) return;
        if (receiving) {
          receiving = false;
          setState(prev => prev.path === path ? { ...prev, connected: false } : prev);
        }
        retry = setTimeout(connect, Math.min(1000 * 2 ** attempts++, 15_000));
      };
      socket.onerror = () => socket?.close();
    };
    connect();
    return () => { stopped = true; clearTimeout(retry); socket?.close(); };
  }, [path, type]);
  const connected = state.path === path ? state.connected : false;
  return useMemo(() => ({
    get lastReceived() {
      return lastReceivedRef.current.path === path ? lastReceivedRef.current.time : 0;
    },
    connected,
  }), [path, connected, state.epoch]);
}

function useSocketFallbackQuery<TData>(key: QueryKey, queryFn: (signal: AbortSignal) => Promise<TData>, socket: { connected: boolean; lastReceived: number }, enabled = true) {
  const lastFallback = useRef(0);
  const query: UseQueryResult<TData, Error> = useQuery({
    queryKey: key,
    queryFn: ({ signal }) => queryFn(signal),
    enabled,
    refetchInterval: () => socket.connected && Date.now() - socket.lastReceived < SOCKET_LIVE_MS ? false : HTTP_FALLBACK_MS,
  });
  // A timer restarts polling when an open socket silently stops sending, without rendering a clock tick.
  useEffect(() => {
    if (!enabled) return;
    const check = () => {
      const now = Date.now();
      if (now - socket.lastReceived >= SOCKET_LIVE_MS && now - query.dataUpdatedAt >= HTTP_FALLBACK_MS && now - lastFallback.current >= HTTP_FALLBACK_MS && !query.isFetching) {
        lastFallback.current = now;
        void query.refetch();
      }
    };
    check();
    const timer = window.setInterval(check, 1000);
    return () => window.clearInterval(timer);
  }, [enabled, socket, query.dataUpdatedAt, query.isFetching, query.refetch]);
  return query;
}

function publishSocketQueryData<TData>(client: QueryClient, key: QueryKey, data: TData) {
  void client.cancelQueries({ queryKey: key });
  client.setQueryData(key, data);
}

export function usePairSnapshot(pair: Pair | undefined) {
  const client = useQueryClient();
  const key = ['snapshot', pair?.symbol, pair?.scoped];
  const acceptSnapshot = (data: Snapshot) => {
    if (data.symbol !== pair?.symbol) return false;
    publishSocketQueryData(client, key, data);
  };
  const socket = useSocket(pair && pairPath(pair, 'ws'), 'snapshot', acceptSnapshot);
  const query = useSocketFallbackQuery(key, signal => api<Snapshot>(pairPath(pair!, 'status'), { signal }), socket, !!pair);
  const now = useNow(1000, !!pair);
  return { ...query, now, live: socket.connected && now - socket.lastReceived < SOCKET_LIVE_MS, stale: now - query.dataUpdatedAt > SNAPSHOT_STALE_MS };
}

export function useLiveAccounts() {
  const client = useQueryClient();
  const key = ['accounts'];
  const stream = useSocket('/api/accounts/ws', 'accounts', data => {
    const generatedAt = Date.parse(data.generated_at);
    if (!Number.isFinite(generatedAt)) return false;
    const current = client.getQueryData<AccountFrame>(key);
    if (current && generatedAt < Date.parse(current.generated_at)) return false;
    publishSocketQueryData(client, key, data);
  });
  const query = useSocketFallbackQuery(key, async signal => {
    const data = await api<AccountFrame>('/api/accounts', { signal });
    const current = client.getQueryData<AccountFrame>(key);
    return current && Date.parse(data.generated_at) < Date.parse(current.generated_at) ? current : data;
  }, stream);
  const generatedAt = Date.parse(query.data?.generated_at ?? '');
  const stale = useExpired(Math.min(query.dataUpdatedAt, generatedAt) + SNAPSHOT_STALE_MS);
  const accounts = useMemo<AccountObservation[]>(() => (query.data?.pairs ?? []).map(entry => ({ ...entry, account_observation_stale: stale })), [query.data?.pairs, stale]);
  return { accounts, stale, error: query.error };
}
