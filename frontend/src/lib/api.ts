import type { ApiResponse, Pair } from './types';

const TOKEN_KEY = 'binance_bot_auth_token';
let memoryToken: string | null = null;
let useMemoryToken = false;
export const tokenStore = {
  get: (): string | null => {
    if (useMemoryToken) return memoryToken;
    try { memoryToken = sessionStorage.getItem(TOKEN_KEY); } catch { /* Retain the last known token. */ }
    return memoryToken;
  },
  set: (token: string) => {
    memoryToken = token;
    useMemoryToken = true;
    try {
      sessionStorage.setItem(TOKEN_KEY, token);
      useMemoryToken = false;
    } catch { /* A stale stored token must not override this login. */ }
  },
  clear: () => {
    memoryToken = null;
    useMemoryToken = true;
    try {
      sessionStorage.removeItem(TOKEN_KEY);
      useMemoryToken = false;
    } catch { /* Keep the signed-out state when Web Storage is unavailable. */ }
  },
};
export const errorMessage = (error: unknown): string => error instanceof Error || error instanceof DOMException ? error.message : String(error);
export class ApiError extends Error {
  constructor(message: string, public status: number, public kind: 'rejection' | 'invalid-response' = 'rejection') { super(message); }
}
type ApiRequestInit = RequestInit & { timeoutMs?: number; readOnly?: boolean };
export async function authenticatedFetch(path: string, init: ApiRequestInit = {}) {
  const token = tokenStore.get();
  const { timeoutMs = 15_000, readOnly = false, ...requestInit } = init;
  const method = (requestInit.method ?? 'GET').toUpperCase();
  const mutating = !readOnly && method !== 'GET' && method !== 'HEAD';
  const headers = new Headers(requestInit.headers);
  if (token) headers.set('Authorization', `Bearer ${token}`);
  if (requestInit.body) headers.set('Content-Type', 'application/json');
  const timeout = AbortSignal.timeout(timeoutMs);
  const signal = requestInit.signal ? AbortSignal.any([requestInit.signal, timeout]) : timeout;
  let response: Response;
  try {
    response = await fetch(path, { ...requestInit, headers, signal });
  } catch (error) {
    if (requestInit.signal?.aborted && signal.reason === requestInit.signal.reason && signal.reason?.name !== 'TimeoutError') throw error;
    throw new ApiError(mutating
      ? '请求未完成，操作可能已生效；请刷新状态或查看日志后再操作'
      : '无法连接后端服务，请检查网络后重试', 0);
  }
  if (response.status === 401 && token && !path.startsWith('/api/auth/login')) {
    // A late response from an old session must not sign out a newer login.
    if (token === tokenStore.get()) {
      tokenStore.clear();
      window.dispatchEvent(new Event('auth-expired'));
    }
    throw new ApiError('登录已过期，请重新登录', 401);
  }
  return response;
}
export async function api<T>(path: string, init?: ApiRequestInit): Promise<T> {
  const response = await authenticatedFetch(path, init);
  let result: ApiResponse<T>;
  try { result = await response.json(); }
  catch { throw new ApiError(`服务返回异常 (${response.status})，请检查 API 连接`, response.status, 'invalid-response'); }
  if (!result || typeof result !== 'object' || typeof result.success !== 'boolean') {
    throw new ApiError(`服务返回异常 (${response.status})，请检查 API 连接`, response.status, 'invalid-response');
  }
  if (!response.ok || !result.success) throw new ApiError(result.message || '请求失败，请重试', response.status);
  return result.data;
}
export const post = <T,>(path: string, data: unknown, init?: Omit<ApiRequestInit, 'method' | 'body'>) =>
  api<T>(path, { ...init, method: 'POST', body: JSON.stringify(data) });
export const pairPath = (pair: Pick<Pair, 'symbol' | 'scoped'>, resource: string) =>
  pair.scoped ? `/api/pairs/${encodeURIComponent(pair.symbol)}/${resource}` : resource === 'ws' ? '/ws' : `/api/${resource}`;
export const pairRoute = (symbol: string, config = false) => `/pairs/${encodeURIComponent(symbol)}${config ? '/config' : ''}`;
export function socketUrl(path: string) {
  const url = new URL(path, window.location.origin);
  url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
  return url.href;
}
