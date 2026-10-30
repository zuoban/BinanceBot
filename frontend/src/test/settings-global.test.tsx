import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, expect, it, vi } from 'vitest';
import Settings from '../pages/Settings';

const mocks = vi.hoisted(() => ({ api: vi.fn(), post: vi.fn(), success: vi.fn(), error: vi.fn(), confirm: vi.fn() }));
vi.mock('antd', async importOriginal => {
  const antd = await importOriginal<typeof import('antd')>();
  return { ...antd, App: { useApp: () => ({ modal: { confirm: mocks.confirm }, message: { success: mocks.success, error: mocks.error } }) } };
});
vi.mock('../lib/api', () => ({ api: mocks.api, post: mocks.post }));

const config = {
  version: 'version-1', recv_window: 5000, sync_interval_secs: 30, global_max_position_notional: '123.45',
  has_api_key: false, has_api_secret: false, api_key_preview: '', telegram_enabled: false,
  has_telegram_bot_token: false, telegram_chat_id: '',
};

beforeEach(() => {
  vi.clearAllMocks();
  mocks.confirm.mockResolvedValue(true);
  vi.stubGlobal('matchMedia', vi.fn().mockImplementation(query => ({ matches: false, media: query, addListener: vi.fn(), removeListener: vi.fn(), addEventListener: vi.fn(), removeEventListener: vi.fn() })));
  vi.stubGlobal('ResizeObserver', class { observe() {} unobserve() {} disconnect() {} });
  mocks.api.mockResolvedValue(config);
  mocks.post.mockImplementation(async (_url, values) => ({ ...config, ...values }));
});

function setup() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(<QueryClientProvider client={client}><Settings /></QueryClientProvider>);
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
}, 15000);

it('缺省时关闭保证金保护，并可开启保存', async () => {
  setup();
  const user = userEvent.setup();
  const toggle = await screen.findByRole('switch', { name: '可用保证金不足时暂停' });
  await waitFor(() => expect(toggle).toBeEnabled());
  expect(toggle).not.toBeChecked();
  expect(screen.getByText('开启后，任意已配置策略的可用保证金小于或等于零时，暂停所有策略、撤销机器人挂单并保留持仓。')).toBeInTheDocument();
  await user.click(toggle);
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  await waitFor(() => expect(mocks.post).toHaveBeenCalledWith('/api/global/config', expect.objectContaining({ pause_on_zero_available_margin: true })));
  await waitFor(() => expect(mocks.success).toHaveBeenCalled());
  expect(toggle).toBeChecked();
}, 15000);

it('显示已开启的保证金保护并可关闭保存', async () => {
  mocks.api.mockResolvedValue({ ...config, pause_on_zero_available_margin: true });
  setup();
  const user = userEvent.setup();
  const toggle = await screen.findByRole('switch', { name: '可用保证金不足时暂停' });
  await waitFor(() => expect(toggle).toBeChecked());
  await user.click(toggle);
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  await waitFor(() => expect(mocks.post).toHaveBeenCalledWith('/api/global/config', expect.objectContaining({ pause_on_zero_available_margin: false })));
}, 15000);

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
}, 15000);

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
}, 15000);

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
}, 15000);

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
}, 15000);
