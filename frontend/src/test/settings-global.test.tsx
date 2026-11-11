import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, expect, it, vi } from 'vitest';
import { MemoryRouter } from 'react-router-dom';
import type * as Api from '../lib/api';
import Settings from '../pages/Settings';

const mocks = vi.hoisted(() => ({ api: vi.fn(), post: vi.fn(), success: vi.fn(), error: vi.fn(), confirm: vi.fn() }));
vi.mock('antd', async importOriginal => {
  const antd = await importOriginal<typeof import('antd')>();
  return { ...antd, App: { useApp: () => ({ modal: { confirm: mocks.confirm }, message: { success: mocks.success, error: mocks.error } }) } };
});
vi.mock('../lib/api', async importOriginal => {
  const actual = await importOriginal<typeof Api>();
  return { ...actual, api: mocks.api, post: mocks.post };
});

const config = {
  version: 'version-1', recv_window: 5000, sync_interval_secs: 30, global_max_position_notional: '123.45',
  has_api_key: false, has_api_secret: false, api_key_preview: '', telegram_enabled: false,
  has_telegram_bot_token: false, telegram_chat_id: '',
};

beforeEach(() => {
  vi.clearAllMocks();
  mocks.confirm.mockResolvedValue(true);
  mocks.api.mockResolvedValue(config);
  mocks.post.mockImplementation(async (_url, values) => ({ ...config, ...values }));
});

function setup() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(<QueryClientProvider client={client}><MemoryRouter><Settings /></MemoryRouter></QueryClientProvider>);
}

it('显示已保存的金额并以十进制字符串提交新金额，清空时提交 null', async () => {
  setup();
  const user = userEvent.setup();
  const input = await screen.findByLabelText('全局最大持仓金额');
  await waitFor(() => expect(input).toHaveValue('123.45'));
  await user.clear(input);
  await user.type(input, '999.12345678');
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  await waitFor(() => expect(mocks.post).toHaveBeenCalledWith('/api/global/config', expect.objectContaining({ global_max_position_notional: '999.12345678' })));
  await waitFor(() => expect(mocks.success).toHaveBeenCalled());
  await user.clear(input);
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  await waitFor(() => expect(mocks.post).toHaveBeenLastCalledWith('/api/global/config', expect.objectContaining({ global_max_position_notional: null })));
}, 40000);

it('缺省时关闭保证金保护，并可开启保存', async () => {
  setup();
  const user = userEvent.setup();
  const toggle = await screen.findByRole('switch', { name: '可用保证金不足时暂停' });
  await waitFor(() => expect(toggle).toBeEnabled());
  expect(toggle).not.toBeChecked();
  await user.click(toggle);
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  await waitFor(() => expect(mocks.post).toHaveBeenCalledWith('/api/global/config', expect.objectContaining({ pause_on_zero_available_margin: true })));
  await waitFor(() => expect(mocks.success).toHaveBeenCalled());
  expect(toggle).toBeChecked();
}, 40000);

it('显示已开启的保证金保护并可关闭保存', async () => {
  mocks.api.mockResolvedValue({ ...config, pause_on_zero_available_margin: true });
  setup();
  const user = userEvent.setup();
  const toggle = await screen.findByRole('switch', { name: '可用保证金不足时暂停' });
  await waitFor(() => expect(toggle).toBeChecked());
  await user.click(toggle);
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  await waitFor(() => expect(mocks.post).toHaveBeenCalledWith('/api/global/config', expect.objectContaining({ pause_on_zero_available_margin: false })));
}, 40000);

it('取消高影响变更确认后不提交，并可再次确认保存', async () => {
  mocks.confirm.mockResolvedValueOnce(false).mockResolvedValueOnce(true);
  setup();
  const user = userEvent.setup();
  const toggle = await screen.findByRole('switch', { name: '可用保证金不足时暂停' });
  await waitFor(() => expect(toggle).toBeEnabled());
  await user.click(toggle);
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  await waitFor(() => expect(mocks.confirm).toHaveBeenCalled());
  expect(mocks.post).not.toHaveBeenCalled();
  expect(toggle).toBeChecked();
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  await waitFor(() => expect(mocks.post).toHaveBeenCalledWith('/api/global/config', expect.objectContaining({ pause_on_zero_available_margin: true })));
}, 40000);

it('保存失败时保留输入且不显示误导性的查询重试按钮', async () => {
  mocks.post.mockRejectedValueOnce(new Error('保存失败'));
  setup();
  const user = userEvent.setup();
  const input = await screen.findByLabelText('同步间隔（秒）');
  await waitFor(() => expect(input).toBeEnabled());
  await user.clear(input);
  await user.type(input, '45');
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  await screen.findByText('保存失败');
  expect(input).toHaveValue('45');
  expect(screen.queryByRole('button', { name: /重试/ })).not.toBeInTheDocument();
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  await waitFor(() => expect(mocks.post).toHaveBeenCalledTimes(2));
}, 40000);

it('配置冲突时保留输入并允许确认后重新加载', async () => {
  mocks.post.mockRejectedValueOnce(new Error('全局设置已被其他请求修改，请刷新后重试'));
  setup();
  const user = userEvent.setup();
  const input = await screen.findByLabelText('同步间隔（秒）');
  await waitFor(() => expect(input).toBeEnabled());
  await user.clear(input);
  await user.type(input, '45');
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  await screen.findByText('全局设置已被其他请求修改，请刷新后重试');
  expect(mocks.post).toHaveBeenCalledWith('/api/global/config', expect.objectContaining({ baseline_version: 'version-1' }));
  expect(input).toHaveValue('45');
  mocks.api.mockResolvedValue({ ...config, version: 'version-2', sync_interval_secs: 60 });
  await user.click(screen.getByRole('button', { name: '重新加载全局设置' }));
  await waitFor(() => expect(input).toHaveValue('60'));
  await user.clear(input);
  await user.type(input, '70');
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  await waitFor(() => expect(mocks.post).toHaveBeenLastCalledWith('/api/global/config', expect.objectContaining({ baseline_version: 'version-2', sync_interval_secs: 70 })));
}, 40000);

it('拒绝零金额', async () => {
  setup();
  const user = userEvent.setup();
  const input = await screen.findByLabelText('全局最大持仓金额');
  await waitFor(() => expect(input).toHaveValue('123.45'));
  await user.clear(input);
  await user.type(input, '0');
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  expect(await screen.findByText('请输入大于 0 的金额')).toBeInTheDocument();
  expect(mocks.post).not.toHaveBeenCalled();
}, 40000);

it('测试连接使用表单中尚未保存的凭据，且不保存设置', async () => {
  setup();
  const user = userEvent.setup();
  const key = await screen.findByLabelText('API Key');
  await waitFor(() => expect(key).toBeEnabled());
  await user.type(key, 'new-key');
  await user.type(screen.getByLabelText('API Secret'), 'new-secret');
  mocks.post.mockResolvedValueOnce('连接成功：API Key 有效，可读取合约账户');
  await user.click(screen.getByRole('button', { name: '测试连接' }));
  await waitFor(() => expect(mocks.post).toHaveBeenCalledWith('/api/global/test-exchange', expect.objectContaining({ api_key: 'new-key', api_secret: 'new-secret', recv_window: 5000 }), { readOnly: true }));
  expect(await screen.findByText(/连接成功/)).toBeInTheDocument();
  expect(mocks.post).not.toHaveBeenCalledWith('/api/global/config', expect.anything());
}, 40000);

it('测试失败时显示后端给出的原因', async () => {
  setup();
  const user = userEvent.setup();
  const token = await screen.findByLabelText('Bot Token');
  await waitFor(() => expect(token).toBeEnabled());
  await user.type(token, '123:abc');
  mocks.post.mockRejectedValueOnce(new Error('请填写 Bot Token 和 Chat ID（或先保存后再测试）'));
  await user.click(screen.getByRole('button', { name: '发送测试消息' }));
  await waitFor(() => expect(mocks.post).toHaveBeenCalledWith('/api/global/test-telegram', expect.objectContaining({ bot_token: '123:abc' }), { readOnly: true }));
  expect(await screen.findByText(/请填写 Bot Token 和 Chat ID/)).toBeInTheDocument();
  expect(mocks.error).toHaveBeenCalled();
}, 40000);

it('还原修改时一并清除字段校验错误', async () => {
  setup();
  const user = userEvent.setup();
  const input = await screen.findByLabelText('全局最大持仓金额');
  await waitFor(() => expect(input).toHaveValue('123.45'));
  await user.clear(input);
  await user.type(input, '0');
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  expect(await screen.findByText('请输入大于 0 的金额')).toBeInTheDocument();
  await user.click(screen.getByRole('button', { name: /还\s*原/ }));
  await waitFor(() => expect(input).toHaveValue('123.45'));
  await waitFor(() => expect(screen.queryByText('请输入大于 0 的金额')).not.toBeInTheDocument());
});
