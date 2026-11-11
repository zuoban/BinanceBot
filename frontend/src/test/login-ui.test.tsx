/// <reference lib="es2024.promise" />
import { act, fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, expect, it, vi } from 'vitest';
import type * as Api from '../lib/api';
import Login from '../pages/Login';

const mocks = vi.hoisted(() => ({ api: vi.fn(), post: vi.fn(), setToken: vi.fn() }));
vi.mock('../lib/api', async importOriginal => {
  const actual = await importOriginal<typeof Api>();
  return { ...actual, api: mocks.api, post: mocks.post, tokenStore: { get: () => null, set: mocks.setToken, clear: vi.fn() } };
});
vi.mock('../components/ThemeSwitch', () => ({ ThemeSwitch: () => null }));

beforeEach(() => {
  vi.clearAllMocks();
});
function setup(initialized: boolean) {
  mocks.api.mockResolvedValue({ initialized });
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const onLogin = vi.fn();
  render(<QueryClientProvider client={client}><Login onLogin={onLogin} /></QueryClientProvider>);
  return onLogin;
}

it('requires a password before login', async () => {
  setup(true);
  const user = userEvent.setup();
  await user.click(await screen.findByRole('button', { name: '进入控制台' }));
  expect(await screen.findByText('请输入密码')).toBeInTheDocument();
  expect(mocks.post).not.toHaveBeenCalled();
});

it('enters the console without a password when none is configured', async () => {
  const onLogin = setup(false);
  expect(await screen.findByText('无需登录')).toBeInTheDocument();
  expect(screen.getByText(/未启用管理员密码认证/)).toBeInTheDocument();
  await userEvent.setup().click(screen.getByRole('button', { name: '进入控制台' }));
  expect(onLogin).toHaveBeenCalledOnce();
  expect(mocks.post).not.toHaveBeenCalled();
});

it('prevents duplicate login submissions while a request is already in flight', async () => {
  const { promise, resolve } = Promise.withResolvers<{ token: string }>();
  mocks.post.mockReturnValue(promise);
  const onLogin = setup(true);
  const user = userEvent.setup();
  const input = await screen.findByLabelText('管理员密码');
  await user.type(input, 'secret-password');
  const form = input.closest('form')!;
  fireEvent.submit(form);
  fireEvent.submit(form);
  await screen.findByRole('button', { name: /进入控制台/ });
  await act(async () => { resolve({ token: 'new-token' }); });
  expect(mocks.post).toHaveBeenCalledTimes(1);
  expect(onLogin).toHaveBeenCalledTimes(1);
});
