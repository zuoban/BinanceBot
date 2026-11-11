import { useEffect, useMemo, useRef, useState } from 'react';
import { useQuery, useQueryClient, type QueryClient, type QueryKey, type UseQueryResult } from '@tanstack/react-query';
import { api, pairPath, socketUrl, tokenStore } from './api';
import { SNAPSHOT_STALE_MS } from './portfolio';
import type { AccountFrame, AccountObservation, OverviewHistoryData, Overview, Pair, Snapshot, SocketMessage } from './types';
import { useExpired } from './useNow';
const SOCKET_LIVE_MS = 5000;
const HTTP_FALLBACK_MS = 3000;
const TOKEN_CHAR_RE = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;

export async function invalidateStrategyQueries(client: QueryClient, options: {
  pairs?: boolean;
  overview?: boolean;
  accounts?: boolean;
  snapshot?: boolean | string;
  clearTrades?: boolean | string;
  configSymbol?: string;
  history?: 'invalidate' | 'reset';
  cancelFirst?: boolean;
} = {}) {
  const { pairs = true, overview = true, accounts = false, snapshot = false, clearTrades = false, configSymbol, history, cancelFirst = false } = options;
  const keys: QueryKey[] = [];
  if (pairs) keys.push(['pairs']);
  if (overview) keys.push(['overview']);
  if (accounts) keys.push(['accounts']);
  if (snapshot === true) keys.push(['snapshot']);
  else if (typeof snapshot === 'string') keys.push(['snapshot', snapshot]);
  if (configSymbol) keys.push(['config', configSymbol]);
  if (history === 'invalidate') keys.push(['overview-history']);
  const tradesPredicate = clearTrades
    ? (q: { queryKey: QueryKey }) => q.queryKey[0] === 'snapshot' && (clearTrades === true || q.queryKey[1] === clearTrades) && q.queryKey[3] === 'trades'
    : undefined;
  if (cancelFirst || tradesPredicate) {
    await Promise.all([
      ...(cancelFirst ? keys.map(queryKey => client.cancelQueries({ queryKey })) : []),
      ...(cancelFirst && history === 'reset' ? [client.cancelQueries({ queryKey: ['overview-history'] })] : []),
      ...(tradesPredicate ? [client.cancelQueries({ predicate: tradesPredicate })] : []),
    ]);
  }
  await Promise.all([
    ...(tradesPredicate ? [client.resetQueries({ predicate: tradesPredicate })] : []),
    ...keys.map(queryKey => client.invalidateQueries({ queryKey })),
    ...(history === 'reset' ? [client.resetQueries({ queryKey: ['overview-history'] })] : []),
  ]);
}
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
    const scheduleRetry = () => {
      if (stopped) return;
      if (receiving) {
        receiving = false;
        setState(prev => prev.path === path ? { ...prev, connected: false } : prev);
      }
      retry = setTimeout(connect, Math.min(1000 * 2 ** attempts++, 15_000));
    };
    const connect = () => {
      const rawToken = tokenStore.get();
      const token = rawToken && TOKEN_CHAR_RE.test(rawToken) ? rawToken : null;
      if (stopped) return;
      try {
        socket = new WebSocket(socketUrl(path), token ? ['auth', `token.${token}`] : ['auth']);
      } catch {
        socket = undefined;
        scheduleRetry();
        return;
      }
      socket.onmessage = event => {
        if (stopped) return;
        try {
          const envelope = JSON.parse(event.data) as Partial<SocketMessage> | null;
          if (!envelope || typeof envelope !== 'object' || envelope.type !== type) return;
          const receivedAt = Date.now();
          const prev = lastReceivedRef.current;
          const prevTime = prev.path === path ? prev.time : 0;
          lastReceivedRef.current = { path, time: receivedAt };
          if (callback.current(envelope.data as SocketDataByType[T]) === false) {
            lastReceivedRef.current = prev;
            return;
          }
          attempts = 0;
          if (!receiving || prevTime === 0 || receivedAt - prevTime >= SOCKET_LIVE_MS) {
            receiving = true;
            setState(s => ({ path, connected: true, epoch: s.epoch + 1 }));
          }
        } catch { /* Ignore malformed frames; HTTP remains the fallback. */ }
      };
      socket.onclose = scheduleRetry;
      socket.onerror = () => socket?.close();
    };
    connect();
    return () => {
      stopped = true;
      clearTimeout(retry);
      if (socket) {
        socket.onmessage = socket.onclose = socket.onerror = null;
        socket.close();
      }
    };
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
  const serializedKey = JSON.stringify(key);
  const query: UseQueryResult<TData, Error> = useQuery({
    queryKey: key,
    queryFn: ({ signal }) => queryFn(signal),
    enabled,
    refetchInterval: () => socket.connected && Date.now() - socket.lastReceived < SOCKET_LIVE_MS ? false : HTTP_FALLBACK_MS,
  });
  const latestRef = useRef({ socket, dataUpdatedAt: query.dataUpdatedAt, isFetching: query.isFetching, refetch: query.refetch });
  latestRef.current = { socket, dataUpdatedAt: query.dataUpdatedAt, isFetching: query.isFetching, refetch: query.refetch };
  // A timer restarts polling when an open socket silently stops sending, without rendering a clock tick.
  useEffect(() => {
    lastFallback.current = 0;
    if (!enabled) return;
    let timer: number | undefined;
    const step = () => {
      window.clearTimeout(timer);
      const hidden = typeof document !== 'undefined' && document.visibilityState === 'hidden';
      const { socket: currentSocket, dataUpdatedAt, isFetching, refetch } = latestRef.current;
      const now = Date.now();
      const socketAge = now - currentSocket.lastReceived;
      let startedRefetch = false;
      if (!hidden && socketAge >= SOCKET_LIVE_MS && now - dataUpdatedAt >= HTTP_FALLBACK_MS && now - lastFallback.current >= HTTP_FALLBACK_MS && !isFetching) {
        lastFallback.current = now;
        startedRefetch = true;
        void refetch();
      }
      const delay = socketAge < SOCKET_LIVE_MS
        ? Math.min(SOCKET_LIVE_MS, Math.max(1, SOCKET_LIVE_MS - socketAge))
        : hidden || isFetching || startedRefetch
          ? HTTP_FALLBACK_MS
          : Math.min(HTTP_FALLBACK_MS, Math.max(250, HTTP_FALLBACK_MS - (now - dataUpdatedAt), HTTP_FALLBACK_MS - (now - lastFallback.current)));
      timer = window.setTimeout(step, delay);
    };
    step();
    const onVisible = () => { if (document.visibilityState === 'visible') step(); };
    if (typeof document !== 'undefined') document.addEventListener('visibilitychange', onVisible);
    return () => {
      window.clearTimeout(timer);
      if (typeof document !== 'undefined') document.removeEventListener('visibilitychange', onVisible);
    };
  }, [enabled, serializedKey]);
  return query;
}

function publishSocketQueryData<TData>(client: QueryClient, key: QueryKey, data: TData) {
  if (client.isFetching({ queryKey: key }) > 0) void client.cancelQueries({ queryKey: key });
  client.setQueryData(key, data);
}

function isOlderSnapshot(next: Snapshot, current: Snapshot | undefined): boolean {
  if (!next.generated_at || !current?.generated_at) return false;
  const nextAt = Date.parse(next.generated_at), currentAt = Date.parse(current.generated_at);
  return Number.isFinite(nextAt) && Number.isFinite(currentAt) && nextAt < currentAt;
}

export function usePairSnapshot(pair: Pair | undefined) {
  const client = useQueryClient();
  const key = ['snapshot', pair?.symbol, pair?.scoped];
  const acceptSnapshot = (data: Snapshot) => {
    if (data.symbol !== pair?.symbol || isOlderSnapshot(data, client.getQueryData<Snapshot>(key))) return false;
    publishSocketQueryData(client, key, data);
  };
  const socket = useSocket(pair && pairPath(pair, 'ws'), 'snapshot', acceptSnapshot);
  const query = useSocketFallbackQuery(key, async signal => {
    if (!pair) throw new Error('该交易对不存在或已移除');
    const data = await api<Snapshot>(pairPath(pair, 'status'), { signal });
    const current = client.getQueryData<Snapshot>(key);
    return isOlderSnapshot(data, current) ? current! : data;
  }, socket, !!pair);
  const socketSilent = useExpired(pair && socket.connected ? socket.lastReceived + SOCKET_LIVE_MS - 1 : -Infinity);
  const stale = useExpired(pair ? query.dataUpdatedAt + SNAPSHOT_STALE_MS : -Infinity);
  return { ...query, live: !!pair && socket.connected && !socketSilent, stale };
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
  const stale = useExpired(query.data ? query.dataUpdatedAt + SNAPSHOT_STALE_MS : -Infinity);
  const accounts = useMemo<AccountObservation[]>(() => {
    const generatedAtMs = query.data ? Date.parse(query.data.generated_at) : NaN;
    const serverTiming = Number.isFinite(generatedAtMs) && query.dataUpdatedAt > 0
      ? { generated_at_ms: generatedAtMs, received_at_ms: query.dataUpdatedAt }
      : undefined;
    return (query.data?.pairs ?? []).map(entry => ({ ...entry, account_observation_stale: stale, ...serverTiming }));
  }, [query.data, query.dataUpdatedAt, stale]);
  return { accounts, stale, error: query.error };
}
