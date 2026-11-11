import { afterEach, describe, expect, it, vi } from 'vitest';
import { api, errorMessage, pairPath, pairRoute, tokenStore } from '../lib/api';
import { routeSymbol } from '../components/CommandPalette';
import { pair } from './fixtures';

afterEach(() => vi.unstubAllGlobals());
describe('API client', () => {
  it('sends bearer tokens and uses scoped and legacy paths', async () => {
    tokenStore.set('test-token');
    const fetcher = vi.fn().mockResolvedValue(new Response(JSON.stringify({ success: true, data: 7 }), { status: 200 }));
    vi.stubGlobal('fetch', fetcher);
    expect(await api<number>('/api/status')).toBe(7);
    expect(new Headers(fetcher.mock.calls[0][1].headers).get('Authorization')).toBe('Bearer test-token');
    expect(pairPath(pair(), 'status')).toBe('/api/pairs/SOLUSDC/status');
    expect(pairPath({ ...pair(), scoped: false }, 'ws')).toBe('/ws');
  });
  it('does not clear a newer session when an old request returns 401', async () => {
    tokenStore.set('old-token');
    let complete!: (value: Response) => void;
    vi.stubGlobal('fetch', vi.fn().mockReturnValue(new Promise(resolve => { complete = resolve; })));
    const request = api('/api/status');
    tokenStore.set('new-token');
    complete(new Response('{}', { status: 401 }));
    await expect(request).rejects.toThrow();
    expect(tokenStore.get()).toBe('new-token');
  });
  it('keeps the session when the authentication service fails', async () => {
    tokenStore.set('test-token');
    const expired = vi.fn();
    window.addEventListener('auth-expired', expired);
    try {
      vi.stubGlobal('fetch', vi.fn().mockImplementation(async () => new Response(
        JSON.stringify({ success: false, message: '认证服务异常，请稍后重试' }), { status: 500 },
      )));
      for (const path of ['/api/auth/status', '/api/status']) {
        await expect(api(path)).rejects.toThrow('认证服务异常，请稍后重试');
        expect(tokenStore.get()).toBe('test-token');
      }
      expect(expired).not.toHaveBeenCalled();
    } finally {
      window.removeEventListener('auth-expired', expired);
    }
  });
  it('reports the server error for an incorrect password', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({ success: false, message: '密码错误' }), { status: 401 })));
    await expect(api('/api/auth/login', { method: 'POST', body: '{}' })).rejects.toThrow('密码错误');
  });
  it('falls back to in-memory token storage when sessionStorage throws on get or only on set', () => {
    const brokenStorage = {
      getItem: () => { throw new DOMException('Blocked', 'SecurityError'); },
      setItem: () => { throw new DOMException('Blocked', 'SecurityError'); },
      removeItem: () => { throw new DOMException('Blocked', 'SecurityError'); },
      clear: () => {},
    };
    vi.stubGlobal('sessionStorage', brokenStorage);
    tokenStore.clear();
    expect(tokenStore.get()).toBeNull();
    tokenStore.set('fallback-token');
    expect(tokenStore.get()).toBe('fallback-token');
    tokenStore.clear();
    expect(tokenStore.get()).toBeNull();

    const quotaStorage = {
      getItem: () => null,
      setItem: () => { throw new DOMException('Quota', 'QuotaExceededError'); },
      removeItem: () => {},
      clear: () => {},
    };
    vi.stubGlobal('sessionStorage', quotaStorage);
    tokenStore.set('quota-fallback-token');
    expect(tokenStore.get()).toBe('quota-fallback-token');
    tokenStore.clear();
    expect(tokenStore.get()).toBeNull();
  });
  it('formats Error instances and arbitrary thrown values via errorMessage', () => {
    expect(errorMessage(new Error('明确错误'))).toBe('明确错误');
    expect(errorMessage('字符串错误')).toBe('字符串错误');
    expect(errorMessage(404)).toBe('404');
  });
  it('encodes pair route symbols and safely decodes route pathnames', () => {
    expect(pairRoute('SOLUSDC')).toBe('/pairs/SOLUSDC');
    expect(pairRoute('SOLUSDC', true)).toBe('/pairs/SOLUSDC/config');
    expect(pairRoute('BTC/USDC', true)).toBe('/pairs/BTC%2FUSDC/config');
    expect(routeSymbol('/pairs/BTC%2FUSDC/config')).toBe('BTC/USDC');
    expect(routeSymbol('/pairs/new')).toBeUndefined();
    expect(routeSymbol('/pairs/%6Eew')).toBeUndefined();
    expect(routeSymbol('/pairs/%E0%A4%A')).toBeUndefined();
  });
});
