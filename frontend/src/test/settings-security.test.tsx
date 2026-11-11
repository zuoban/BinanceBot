/// <reference lib="es2024.promise" />
import { useState } from 'react';
import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, expect, it, vi } from 'vitest';
import type * as Api from '../lib/api';
import { EditContext } from '../lib/session';
import { MemoryRouter } from 'react-router-dom';
import Settings from '../pages/Settings';

const mocks = vi.hoisted(() => ({ api: vi.fn(), post: vi.fn(), confirm: vi.fn(), success: vi.fn(), error: vi.fn() }));
vi.mock('antd', async importOriginal => {
  const antd = await importOriginal<typeof import('antd')>();
  return { ...antd, App: { useApp: () => ({ modal: { confirm: mocks.confirm }, message: { success: mocks.success, error: mocks.error } }) } };
});
vi.mock('../lib/api', async importOriginal => {
  const actual = await importOriginal<typeof Api>();
  return { ...actual, api: mocks.api, post: mocks.post };
});

beforeEach(() => {
  vi.clearAllMocks();
  mocks.api.mockResolvedValue({ version: 'v1', recv_window: 5000, sync_interval_secs: 30, global_max_position_notional: null, pause_on_zero_available_margin: false, min_available_margin_pct: null });
});

function setup() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  function Wrapper() {
    const [dirty, setDirty] = useState(false);
    const [saving, setSaving] = useState(false);
    return <QueryClientProvider client={client}><EditContext.Provider value={{ dirty, setDirty, saving, setSaving }}><MemoryRouter><Settings /></MemoryRouter></EditContext.Provider></QueryClientProvider>;
  }
  return render(<Wrapper />);
}

it('等待风控确认时不提前锁定表单，保留页签未保存拦截，并在卸载时销毁确认弹窗且不提交', async () => {
  const { promise, resolve: confirm } = Promise.withResolvers<boolean>();
  const destroy = vi.fn();
  mocks.confirm
    .mockReturnValueOnce(Object.assign(promise, { destroy }))
    .mockResolvedValueOnce(false);
  const view = setup();
  const user = userEvent.setup();
  const toggle = await screen.findByRole('switch', { name: '可用保证金不足时暂停' });
  await waitFor(() => expect(toggle).toBeEnabled());
  await user.click(toggle);
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  await waitFor(() => expect(mocks.confirm).toHaveBeenCalledTimes(1));
  expect(toggle).toBeEnabled();
  await user.click(screen.getByRole('tab', { name: '账户安全' }));
  await waitFor(() => expect(mocks.confirm).toHaveBeenCalledTimes(2));
  expect(screen.getByRole('tab', { name: '连接与通知' })).toHaveAttribute('aria-selected', 'true');
  view.unmount();
  expect(destroy).toHaveBeenCalledTimes(1);
  await act(async () => { confirm(true); });
  expect(mocks.post).not.toHaveBeenCalled();
}, 40000);

it('取消风控确认后保留输入且允许继续编辑', async () => {
  mocks.confirm.mockResolvedValueOnce(false);
  setup();
  const user = userEvent.setup();
  const toggle = await screen.findByRole('switch', { name: '可用保证金不足时暂停' });
  await waitFor(() => expect(toggle).toBeEnabled());
  await user.click(toggle);
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  await waitFor(() => expect(mocks.confirm).toHaveBeenCalledTimes(1));
  expect(toggle).toBeEnabled();
  expect(toggle).toBeChecked();
  expect(mocks.post).not.toHaveBeenCalled();
}, 40000);

it('shows that the admin password comes from startup configuration', async () => {
  setup();
  const user = userEvent.setup();
  await user.click(screen.getByRole('tab', { name: '账户安全' }));
  expect(await screen.findByText(/不保存在数据库中/)).toBeInTheDocument();
  expect(screen.getByText(/ADMIN_PASSWORD/)).toBeInTheDocument();
  expect(mocks.confirm).not.toHaveBeenCalled();
});
