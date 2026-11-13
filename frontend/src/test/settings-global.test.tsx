/// <reference lib="es2024.promise" />
import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, expect, it, vi } from 'vitest';
import { MemoryRouter } from 'react-router-dom';
import type * as Api from '../lib/api';
import Settings from '../pages/Settings';
import { globalConfig } from './fixtures';

const mocks = vi.hoisted(() => ({ api: vi.fn(), post: vi.fn(), success: vi.fn(), error: vi.fn(), confirm: vi.fn() }));
vi.mock('antd', async importOriginal => {
  const antd = await importOriginal<typeof import('antd')>();
  return { ...antd, App: { useApp: () => ({ modal: { confirm: mocks.confirm }, message: { success: mocks.success, error: mocks.error } }) } };
});
vi.mock('../lib/api', async importOriginal => {
  const actual = await importOriginal<typeof Api>();
  return { ...actual, api: mocks.api, post: mocks.post };
});

const config = globalConfig({ global_max_position_notional: '123.45' });

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

it('未保存密钥时仅填写单边 API Key 或 Secret 会在打开确认弹窗前阻止提交', async () => {
  setup();
  const user = userEvent.setup();
  const key = await screen.findByLabelText('API Key');
  await waitFor(() => expect(key).toBeEnabled());
  await user.type(key, 'only-key');
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  expect(await screen.findByText('请同时填写 API Key 和 API Secret')).toBeInTheDocument();
  expect(mocks.confirm).not.toHaveBeenCalled();
  expect(mocks.post).not.toHaveBeenCalled();
}, 40000);

it('启用 Telegram 通知时校验凭证完整性与清除冲突，并在超限金额时拦截提交', async () => {
  setup();
  const user = userEvent.setup();
  const notional = await screen.findByLabelText('全局最大持仓金额');
  await waitFor(() => expect(notional).toHaveValue('123.45'));
  await user.clear(notional);
  await user.type(notional, '1000000000001');
  await user.click(await screen.findByRole('switch', { name: '通知开关' }));
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  expect(await screen.findByText('全局最大持仓金额超出允许上限')).toBeInTheDocument();
  expect(await screen.findByText('请填写 Bot Token')).toBeInTheDocument();
  expect(await screen.findByText('请填写 Chat ID')).toBeInTheDocument();
  expect(mocks.confirm).not.toHaveBeenCalled();

  await user.click(screen.getByRole('checkbox', { name: /清除 Telegram 凭证/ }));
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  expect(await screen.findByText('请先关闭通知后再清除 Telegram 凭证')).toBeInTheDocument();
  expect(mocks.confirm).not.toHaveBeenCalled();
  expect(mocks.post).not.toHaveBeenCalled();
}, 40000);

it('输入 API Key 或开启通知开关时不提前标红未编辑字段，修正后立即消除错误', async () => {
  setup();
  const user = userEvent.setup();
  const key = await screen.findByLabelText('API Key');
  await waitFor(() => expect(key).toBeEnabled());
  await user.type(key, 'first-key');
  expect(screen.queryByText('请同时填写 API Key 和 API Secret')).not.toBeInTheDocument();

  await user.click(screen.getByRole('switch', { name: '通知开关' }));
  expect(screen.queryByText('请填写 Bot Token')).not.toBeInTheDocument();
  expect(screen.queryByText('请填写 Chat ID')).not.toBeInTheDocument();

  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  expect(await screen.findByText('请同时填写 API Key 和 API Secret')).toBeInTheDocument();
  expect(await screen.findByText('请填写 Bot Token')).toBeInTheDocument();
  expect(await screen.findByText('请填写 Chat ID')).toBeInTheDocument();

  await user.clear(key);
  await user.click(screen.getByRole('switch', { name: '通知开关' }));
  await waitFor(() => expect(screen.queryByText('请同时填写 API Key 和 API Secret')).not.toBeInTheDocument());
  await waitFor(() => expect(screen.queryByText('请填写 Bot Token')).not.toBeInTheDocument());
  await waitFor(() => expect(screen.queryByText('请填写 Chat ID')).not.toBeInTheDocument());
}, 40000);

it('已保存 API 凭证时阻止单边轮换，并拦截半残缺的已保存凭证状态', async () => {
  mocks.api.mockResolvedValueOnce(globalConfig({ has_api_key: true, has_api_secret: true, api_key_preview: 'abcd...wxyz' }));
  const user = userEvent.setup();
  const firstClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const { unmount } = render(<QueryClientProvider client={firstClient}><MemoryRouter><Settings /></MemoryRouter></QueryClientProvider>);
  const key = await screen.findByLabelText('API Key');
  await waitFor(() => expect(key).toBeEnabled());
  await user.type(key, 'rotated-only-key');
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  expect(await screen.findByText('更换或配置 API 凭证时请同时填写 API Key 和 API Secret')).toBeInTheDocument();
  expect(mocks.post).not.toHaveBeenCalled();
  unmount();

  mocks.api.mockResolvedValueOnce(globalConfig({ has_api_key: true, has_api_secret: false, api_key_preview: 'abcd...wxyz' }));
  const secondClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(<QueryClientProvider client={secondClient}><MemoryRouter><Settings /></MemoryRouter></QueryClientProvider>);
  const syncInput = await screen.findByLabelText('同步间隔（秒）');
  await waitFor(() => expect(syncInput).toBeEnabled());
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  expect(await screen.findByText('更换或配置 API 凭证时请同时填写 API Key 和 API Secret')).toBeInTheDocument();
  expect(mocks.post).not.toHaveBeenCalled();
}, 40000);

it('校验 API Key 与 Telegram Bot Token 格式，拒绝空白、控制字符与路径注入字符', async () => {
  setup();
  const user = userEvent.setup();
  const key = await screen.findByLabelText('API Key');
  await waitFor(() => expect(key).toBeEnabled());
  await user.type(key, 'bad key');
  expect(await screen.findByText('API Key 格式无效')).toBeInTheDocument();

  const token = screen.getByLabelText('Bot Token');
  await user.type(token, '123/evil?x=1');
  expect(await screen.findByText('Telegram Bot Token 格式无效')).toBeInTheDocument();
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  expect(mocks.post).not.toHaveBeenCalled();
}, 40000);

it('提交失败后在 API Secret 输入空白仍保留缺失提示，拒绝非法字符并在输入合法密钥后消除错误', async () => {
  setup();
  const user = userEvent.setup();
  const key = await screen.findByLabelText('API Key');
  await waitFor(() => expect(key).toBeEnabled());
  const secret = screen.getByLabelText('API Secret');
  const token = screen.getByLabelText('Bot Token');
  const chatId = screen.getByLabelText('Chat ID');

  await user.type(key, 'valid-key');
  const clearApiCheckbox = screen.getByRole('checkbox', { name: /清除已保存的 API Key 和 Secret/ });
  await user.click(clearApiCheckbox);
  await user.click(clearApiCheckbox);
  expect(screen.queryByText('请同时填写 API Key 和 API Secret')).not.toBeInTheDocument();

  await user.type(token, '123:abc');
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  expect(await screen.findByText('请同时填写 API Key 和 API Secret')).toBeInTheDocument();
  expect(await screen.findByText('请填写 Chat ID')).toBeInTheDocument();

  await user.type(secret, ' ');
  await waitFor(() => expect(secret).toHaveValue(' '));
  expect(screen.getByText('请同时填写 API Key 和 API Secret')).toBeInTheDocument();

  await user.clear(secret);
  await user.type(secret, 'bad secret');
  expect(await screen.findByText('API Secret 格式无效')).toBeInTheDocument();
  expect(screen.getByRole('button', { name: '测试连接' })).toBeDisabled();

  await user.clear(secret);
  await user.type(secret, 'valid-secret');
  await user.type(chatId, '10001');
  await waitFor(() => expect(screen.queryByText('API Secret 格式无效')).not.toBeInTheDocument());
  await waitFor(() => expect(screen.queryByText('请同时填写 API Key 和 API Secret')).not.toBeInTheDocument());
  await waitFor(() => expect(screen.queryByText('请填写 Chat ID')).not.toBeInTheDocument());
  expect(screen.getByRole('button', { name: '测试连接' })).toBeEnabled();
}, 40000);

it('拒绝含 %/@/非 ASCII 的 Bot Token、超长凭证与含控制字符的 Chat ID，并在编辑时废除旧保存确认', async () => {
  const { promise: confirmPromise, resolve: resolveConfirm } = Promise.withResolvers<boolean>();
  mocks.confirm.mockReturnValueOnce(confirmPromise);
  setup();
  const user = userEvent.setup();
  const token = await screen.findByLabelText('Bot Token');
  await waitFor(() => expect(token).toBeEnabled());
  await user.type(token, '123%abc');
  expect(await screen.findByText('Telegram Bot Token 格式无效')).toBeInTheDocument();
  await user.clear(token);
  await user.type(token, '123@abc');
  expect(await screen.findByText('Telegram Bot Token 格式无效')).toBeInTheDocument();
  await user.clear(token);

  const chatId = screen.getByLabelText('Chat ID');
  await user.type(chatId, 'x'.repeat(129));
  expect(await screen.findByText('Telegram Chat ID 格式无效')).toBeInTheDocument();
  await user.clear(chatId);

  const toggle = screen.getByRole('switch', { name: '可用保证金不足时暂停' });
  await user.click(toggle);
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  await waitFor(() => expect(mocks.confirm).toHaveBeenCalledTimes(1));

  const syncInput = screen.getByLabelText('同步间隔（秒）');
  await user.clear(syncInput);
  await user.type(syncInput, '45');
  await act(async () => { resolveConfirm(true); });
  expect(mocks.post).not.toHaveBeenCalled();
}, 40000);

it('支持配置 ntfy 通知、发送测试消息并在清除凭证时要求确认', async () => {
  setup();
  const user = userEvent.setup();
  const serverUrl = await screen.findByLabelText('服务地址');
  await waitFor(() => expect(serverUrl).toHaveValue('https://ntfy.sh'));
  const topic = screen.getByLabelText('Topic');
  const token = screen.getByLabelText('访问令牌（可选）');

  await user.clear(serverUrl);
  await user.type(serverUrl, 'https://ntfy.example.com');
  await user.type(topic, 'bot_alerts-01');
  await user.type(token, 'tk_secret123');

  mocks.post.mockResolvedValueOnce('测试消息已发送，请在 ntfy 中查看');
  await user.click(screen.getByRole('button', { name: '发送 ntfy 测试消息' }));
  await waitFor(() => expect(mocks.post).toHaveBeenCalledWith(
    '/api/global/test-ntfy',
    { server_url: 'https://ntfy.example.com', topic: 'bot_alerts-01', token: 'tk_secret123' },
    { readOnly: true },
  ));
  expect(await screen.findByText(/请在 ntfy 中查看/)).toBeInTheDocument();

  await user.click(screen.getByRole('switch', { name: 'ntfy 通知开关' }));
  mocks.post.mockImplementationOnce(async (_url, values) => ({
    ...config,
    ...values,
    ntfy_enabled: true,
    ntfy_server_url: 'https://ntfy.example.com',
    ntfy_topic: 'bot_alerts-01',
    has_ntfy_token: true,
  }));
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  await waitFor(() => expect(mocks.post).toHaveBeenLastCalledWith('/api/global/config', expect.objectContaining({
    ntfy_enabled: true,
    ntfy_server_url: 'https://ntfy.example.com',
    ntfy_topic: 'bot_alerts-01',
    ntfy_token: 'tk_secret123',
    clear_ntfy_credentials: false,
  })));
  expect(await screen.findByText('已保存访问令牌，留空保持不变')).toBeInTheDocument();

  await user.click(screen.getByRole('switch', { name: 'ntfy 通知开关' }));
  await user.click(screen.getByRole('checkbox', { name: /清除 ntfy 凭证/ }));
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  await waitFor(() => expect(mocks.confirm).toHaveBeenCalled());
  await waitFor(() => expect(mocks.post).toHaveBeenLastCalledWith('/api/global/config', expect.objectContaining({
    ntfy_enabled: false,
    ntfy_server_url: '',
    ntfy_topic: '',
    ntfy_token: '',
    clear_ntfy_credentials: true,
  })));
}, 40000);

it('校验 ntfy 服务地址、Topic、访问令牌与开启通知及清除冲突', async () => {
  setup();
  const user = userEvent.setup();
  const serverUrl = await screen.findByLabelText('服务地址');
  await waitFor(() => expect(serverUrl).toBeEnabled());
  const topic = screen.getByLabelText('Topic');
  const token = screen.getByLabelText('访问令牌（可选）');

  await user.click(screen.getByRole('switch', { name: 'ntfy 通知开关' }));
  expect(screen.queryByText('请填写 Topic')).not.toBeInTheDocument();
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  expect(await screen.findByText('请填写 Topic')).toBeInTheDocument();

  await user.click(screen.getByRole('checkbox', { name: /清除 ntfy 凭证/ }));
  await user.click(screen.getByRole('button', { name: '保存全局设置' }));
  expect(await screen.findByText('请先关闭通知后再清除 ntfy 凭证')).toBeInTheDocument();

  await user.click(screen.getByRole('checkbox', { name: /清除 ntfy 凭证/ }));
  await user.clear(serverUrl);
  await user.type(serverUrl, 'https://ntfy.sh/subpath');
  expect(await screen.findByText(/ntfy 服务地址格式无效/)).toBeInTheDocument();
  expect(screen.getByRole('button', { name: '发送 ntfy 测试消息' })).toBeDisabled();

  await user.clear(serverUrl);
  await user.type(serverUrl, 'https://ntfy.sh');
  await user.type(topic, 'bad/topic');
  expect(await screen.findByText(/ntfy Topic 格式无效/)).toBeInTheDocument();

  await user.clear(topic);
  await user.type(topic, 'valid_topic');
  await user.type(token, 'bad token');
  expect(await screen.findByText('ntfy 访问令牌格式无效')).toBeInTheDocument();
  expect(screen.getByRole('button', { name: '发送 ntfy 测试消息' })).toBeDisabled();

  await user.clear(token);
  await waitFor(() => expect(screen.queryByText('ntfy 访问令牌格式无效')).not.toBeInTheDocument());
  await waitFor(() => expect(screen.queryByText('请填写 Topic')).not.toBeInTheDocument());
  expect(screen.getByRole('button', { name: '发送 ntfy 测试消息' })).toBeEnabled();
  expect(mocks.post).not.toHaveBeenCalled();
}, 40000);
