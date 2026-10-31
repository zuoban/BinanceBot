import { act, fireEvent, screen, waitFor } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({ api: vi.fn(), post: vi.fn() }));
vi.mock('../lib/api', async importOriginal => ({ ...await importOriginal<typeof import('../lib/api')>(), api: mocks.api, post: mocks.post }));
vi.mock('../components/Shell', async () => {
  const React = await import('react');
  const { SessionContext } = await import('../lib/session');
  return { default: () => {
    const { logout } = React.useContext(SessionContext);
    return <button onClick={() => void logout()}>退出测试</button>;
  } };
});
vi.mock('../pages/Login', () => ({ default: ({ onLogin }: { onLogin: () => void }) => <button onClick={() => { sessionStorage.setItem('binance_bot_auth_token', 'new-token'); onLogin(); }}>重新登录</button> }));
vi.mock('../lib/theme', () => ({ ThemeProvider: ({ children }: { children: React.ReactNode }) => children }));

afterEach(() => { vi.resetModules(); sessionStorage.clear(); document.body.innerHTML = ''; });

it('旧退出请求完成后不会清除新会话', async () => {
  let finish!: () => void;
  mocks.api.mockResolvedValue({ initialized: true, authenticated: true });
  mocks.post.mockReturnValue(new Promise<void>(resolve => { finish = resolve; }));
  sessionStorage.setItem('binance_bot_auth_token', 'old-token');
  document.body.innerHTML = '<div id="root"></div>';
  await import('../main');
  fireEvent.click(await screen.findByRole('button', { name: '退出测试' }));
  expect(mocks.post).toHaveBeenCalledWith('/api/auth/logout', {});
  act(() => window.dispatchEvent(new Event('auth-expired')));
  fireEvent.click(await screen.findByRole('button', { name: '重新登录' }));
  await waitFor(() => expect(sessionStorage.getItem('binance_bot_auth_token')).toBe('new-token'));
  await act(async () => { finish(); });
  expect(sessionStorage.getItem('binance_bot_auth_token')).toBe('new-token');
  expect(await screen.findByRole('button', { name: '退出测试' })).toBeInTheDocument();
});
