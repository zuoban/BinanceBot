import { afterEach, describe, expect, it, vi } from 'vitest';
import { api, pairPath, tokenStore } from '../lib/api';
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
  it('reports the server error for an incorrect password', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({ success: false, message: '密码错误' }), { status: 401 })));
    await expect(api('/api/auth/login', { method: 'POST', body: '{}' })).rejects.toThrow('密码错误');
  });
});
