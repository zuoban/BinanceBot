import type { ApiResponse, Pair } from './types';

const TOKEN_KEY = 'binance_bot_auth_token';
export const tokenStore = {
  get: () => sessionStorage.getItem(TOKEN_KEY),
  set: (token: string) => sessionStorage.setItem(TOKEN_KEY, token),
  clear: () => sessionStorage.removeItem(TOKEN_KEY),
};
export class ApiError extends Error {
  constructor(message: string, public status: number) { super(message); }
}
export async function authenticatedFetch(path: string, init: RequestInit = {}) {
  const token = tokenStore.get();
  const headers = new Headers(init.headers);
  if (token) headers.set('Authorization', `Bearer ${token}`);
  if (init.body) headers.set('Content-Type', 'application/json');
  let response: Response;
  try {
    response = await fetch(path, { ...init, headers, signal: init.signal ?? AbortSignal.timeout(15_000) });
  } catch (error) {
    if (init.signal?.aborted) throw error;
    throw new ApiError(init.method && init.method !== 'GET'
      ? '请求未完成，操作可能已生效；请刷新状态或查看日志后再操作'
      : '无法连接后端服务，请检查网络后重试', 0);
  }
  if (response.status === 401 && token && !path.startsWith('/api/auth/login')) {
    // A late response from an old session must not sign out a newer login.
    if (token && token === tokenStore.get()) {
      tokenStore.clear();
      window.dispatchEvent(new Event('auth-expired'));
    }
    throw new ApiError('登录已过期，请重新登录', 401);
  }
  return response;
}
export async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await authenticatedFetch(path, init);
  let result: ApiResponse<T>;
  try { result = await response.json(); }
  catch { throw new ApiError(`服务返回异常 (${response.status})，请检查 API 连接`, response.status); }
  if (!response.ok || !result.success) throw new ApiError(result.message || '请求失败，请重试', response.status);
  return result.data;
}
export const post = <T,>(path: string, data: unknown) => api<T>(path, { method: 'POST', body: JSON.stringify(data) });
export const pairPath = (pair: Pick<Pair, 'symbol' | 'scoped'>, resource: string) =>
  pair.scoped ? `/api/pairs/${encodeURIComponent(pair.symbol)}/${resource}` : resource === 'ws' ? '/ws' : `/api/${resource}`;
export function socketUrl(path: string) {
  const url = new URL(path, window.location.origin);
  url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
  return url.href;
}
