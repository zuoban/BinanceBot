import { useState } from 'react';
import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, expect, it, vi } from 'vitest';
import { EditContext, SessionContext } from '../lib/session';
import Settings from '../pages/Settings';

const mocks = vi.hoisted(() => ({ api: vi.fn(), post: vi.fn(), confirm: vi.fn(), success: vi.fn(), error: vi.fn(), logout: vi.fn() }));
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
  mocks.logout.mockResolvedValue(undefined);
});

function setup() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  function Wrapper() {
    const [dirty, setDirty] = useState(false);
    const [saving, setSaving] = useState(false);
    return <QueryClientProvider client={client}><SessionContext.Provider value={{ logout: mocks.logout }}><EditContext.Provider value={{ dirty, setDirty, saving, setSaving }}><Settings /></EditContext.Provider></SessionContext.Provider></QueryClientProvider>;
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
}, 15000);

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
}, 15000);

it('密码有内容时切换页签需确认，取消保留输入，确认放弃后清空', async () => {
  setup();
  const user = userEvent.setup();
  await user.click(screen.getByRole('tab', { name: '账户安全' }));
  const input = await screen.findByLabelText('当前密码');
  await user.type(input, 'secret');
  mocks.confirm.mockResolvedValueOnce(false).mockResolvedValueOnce(true);
  await user.click(screen.getByRole('tab', { name: '连接与通知' }));
  expect(mocks.confirm).toHaveBeenCalledTimes(1);
  expect(input).toHaveValue('secret');
  await user.click(screen.getByRole('tab', { name: '连接与通知' }));
  await waitFor(() => expect(screen.getByRole('tab', { name: '连接与通知' })).toHaveAttribute('aria-selected', 'true'));
  await user.click(screen.getByRole('tab', { name: '账户安全' }));
  expect(await screen.findByLabelText('当前密码')).toHaveValue('');
  await user.click(screen.getByRole('tab', { name: '连接与通知' }));
  expect(mocks.confirm).toHaveBeenCalledTimes(2);
}, 15000);

it('清空密码表单后切换无需确认', async () => {
  setup();
  const user = userEvent.setup();
  await user.click(screen.getByRole('tab', { name: '账户安全' }));
  const input = await screen.findByLabelText('新密码');
  await user.type(input, 'secret');
  await user.clear(input);
  await user.click(screen.getByRole('tab', { name: '连接与通知' }));
  expect(mocks.confirm).not.toHaveBeenCalled();
  expect(screen.getByRole('tab', { name: '连接与通知' })).toHaveAttribute('aria-selected', 'true');
}, 15000);

it('提交期间阻止切换，失败后保留修改并要求确认，成功后退出登录', async () => {
  setup();
  const user = userEvent.setup();
  await user.click(screen.getByRole('tab', { name: '账户安全' }));
  await user.type(await screen.findByLabelText('当前密码'), 'oldsecret');
  await user.type(screen.getByLabelText('新密码'), 'newsecret');
  await user.type(screen.getByLabelText('确认新密码'), 'newsecret');
  let rejectRequest!: (error: Error) => void;
  mocks.post.mockImplementationOnce(() => new Promise((_resolve, reject) => { rejectRequest = reject; }));
  await user.click(screen.getByRole('button', { name: '更新密码' }));
  await waitFor(() => expect(mocks.post).toHaveBeenCalled());
  await user.click(screen.getByRole('tab', { name: '连接与通知' }));
  expect(screen.getByRole('tab', { name: '账户安全' })).toHaveAttribute('aria-selected', 'true');
  expect(mocks.confirm).not.toHaveBeenCalled();
  rejectRequest(new Error('更新失败'));
  await waitFor(() => expect(mocks.error).toHaveBeenCalledWith('更新失败'));
  mocks.confirm.mockResolvedValueOnce(false);
  await user.click(screen.getByRole('tab', { name: '连接与通知' }));
  expect(mocks.confirm).toHaveBeenCalledTimes(1);
  expect(screen.getByLabelText('当前密码')).toHaveValue('oldsecret');
  mocks.post.mockResolvedValueOnce({});
  await user.click(screen.getByRole('button', { name: '更新密码' }));
  await waitFor(() => expect(mocks.logout).toHaveBeenCalledWith(true));
  expect(screen.getByLabelText('当前密码')).toHaveValue('');
  await user.click(screen.getByRole('tab', { name: '连接与通知' }));
  expect(mocks.confirm).toHaveBeenCalledTimes(1);
}, 15000);
