import { useState } from 'react';
import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, expect, it, vi } from 'vitest';
import { EditContext } from '../lib/session';
import Settings from '../pages/Settings';

const mocks = vi.hoisted(() => ({ api: vi.fn(), post: vi.fn(), confirm: vi.fn(), success: vi.fn(), error: vi.fn() }));
vi.mock('antd', async importOriginal => {
  const antd = await importOriginal<typeof import('antd')>();
  return { ...antd, App: { useApp: () => ({ modal: { confirm: mocks.confirm }, message: { success: mocks.success, error: mocks.error } }) } };
});
vi.mock('../lib/api', () => ({ api: mocks.api, post: mocks.post }));

beforeEach(() => {
  vi.clearAllMocks();
  vi.stubGlobal('matchMedia', vi.fn().mockImplementation(query => ({ matches: false, media: query, addListener: vi.fn(), removeListener: vi.fn(), addEventListener: vi.fn(), removeEventListener: vi.fn() })));
  vi.stubGlobal('ResizeObserver', class { observe() {} unobserve() {} disconnect() {} });
  mocks.api.mockResolvedValue({ version: 'v1', recv_window: 5000, sync_interval_secs: 30, global_max_position_notional: null, pause_on_zero_available_margin: false });
});

function setup() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  function Wrapper() {
    const [dirty, setDirty] = useState(false);
    const [saving, setSaving] = useState(false);
    return <QueryClientProvider client={client}><EditContext.Provider value={{ dirty, setDirty, saving, setSaving }}><Settings /></EditContext.Provider></QueryClientProvider>;
  }
  return render(<Wrapper />);
}

it('等待风控确认时锁定表单和页签，卸载后不会提交', async () => {
  let confirm!: (value: boolean) => void;
  mocks.confirm.mockReturnValueOnce(new Promise<boolean>(resolve => { confirm = resolve; }));
  const view = setup();
  const user = userEvent.setup();
  const toggle = await screen.findByRole('switch', { name: '可用保证金不足时暂停' });
  await waitFor(() => expect(toggle).toBeEnabled());
  await user.click(toggle);
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  await waitFor(() => expect(mocks.confirm).toHaveBeenCalledTimes(1));
  expect(toggle).toBeDisabled();
  await user.click(screen.getByRole('tab', { name: '账户安全' }));
  expect(screen.getByRole('tab', { name: '连接与通知' })).toHaveAttribute('aria-selected', 'true');
  view.unmount();
  await act(async () => { confirm(true); });
  expect(mocks.post).not.toHaveBeenCalled();
}, 40000);

it('取消风控确认后恢复编辑', async () => {
  mocks.confirm.mockResolvedValueOnce(false);
  setup();
  const user = userEvent.setup();
  const toggle = await screen.findByRole('switch', { name: '可用保证金不足时暂停' });
  await waitFor(() => expect(toggle).toBeEnabled());
  await user.click(toggle);
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  await waitFor(() => expect(toggle).toBeEnabled());
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
