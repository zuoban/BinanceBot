import { act, renderHook } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { App } from 'antd';
import { afterEach, expect, it, vi } from 'vitest';
import { ControlProvider, runBatchControls, useControls } from '../components/Controls';
import type { ControlResult } from '../components/Controls';
import { api, ApiError, post } from '../lib/api';
import { pair, snapshot } from './fixtures';

vi.mock('antd', () => ({
  App: Object.assign(({ children }: { children: React.ReactNode }) => children, {
    useApp: () => ({ modal: { confirm: vi.fn() }, message: { warning: vi.fn(), error: vi.fn() } }),
  }),
}));
vi.mock('@ant-design/icons', () => ({}));

vi.mock('../lib/api', () => ({
  ApiError: class extends Error {
    constructor(message: string, public status: number, public kind: 'rejection' | 'invalid-response' = 'rejection') { super(message); }
  },
  api: vi.fn(),
  post: vi.fn(),
  pairPath: (value: { symbol: string }, path: string) => `/api/pairs/${value.symbol}/${path}`,
}));

afterEach(() => { vi.useRealTimers(); vi.clearAllMocks(); });

it.each([
  ['pause', new ApiError('Failed to send action: channel closed', 200)],
  ['cancel_all', new ApiError('Failed to send action: channel closed', 200)],
  ['resume', new ApiError('网络错误', 0)],
  ['resume', new ApiError('策略未确认控制操作', 200)],
  ['rebalance', new ApiError('部分撤单后重建失败', 200)],
  ['rebalance', new ApiError('响应无效', 200, 'invalid-response')],
] as const)('%s reports an uncertain result and refreshes after a control error', async (action, error) => {
  vi.mocked(post).mockRejectedValue(error);
  const client = new QueryClient();
  const invalidate = vi.spyOn(client, 'invalidateQueries');
  const wrapper = ({ children }: { children: React.ReactNode }) => <QueryClientProvider client={client}><App><ControlProvider>{children}</ControlProvider></App></QueryClientProvider>;
  const { result, unmount } = renderHook(() => useControls(), { wrapper });
  try {
    let outcome!: ControlResult;
    await act(async () => { outcome = await result.current.run(pair(), action, true); });
    expect(outcome).toBe('request_unconfirmed');
    expect(result.current.results.SOLUSDC).toContain('操作可能已生效');
    expect(result.current.results.SOLUSDC).toContain('核对交易所挂单');
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ['pairs'] });
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ['overview'] });
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ['snapshot', 'SOLUSDC'] });
  } finally {
    unmount();
    client.clear();
  }
});

it.each(['pause', 'cancel_all'] as const)('%s reports cancellation as unverified even when the paused snapshot has no orders', async action => {
  vi.mocked(post).mockResolvedValue(undefined);
  vi.mocked(api).mockResolvedValue(snapshot());
  const client = new QueryClient();
  const wrapper = ({ children }: { children: React.ReactNode }) => <QueryClientProvider client={client}><App><ControlProvider>{children}</ControlProvider></App></QueryClientProvider>;
  const { result, unmount } = renderHook(() => useControls(), { wrapper });
  try {
    let pending!: Promise<ControlResult>;
    act(() => { pending = result.current.run(pair(), action, true); });
    await act(async () => { await pending; });
    expect(await pending).toBe('cancellation_unconfirmed');
    expect(post).toHaveBeenCalledWith('/api/pairs/SOLUSDC/control', { action });
    expect(result.current.results.SOLUSDC).toBe('已暂停，撤单结果尚未确认，请检查交易所挂单及日志');
    expect(result.current.busy.has('SOLUSDC')).toBe(false);
  } finally {
    unmount();
    client.clear();
  }
});

it('does not submit a second operation for a locked pair', async () => {
  vi.mocked(post).mockResolvedValue(undefined);
  vi.mocked(api).mockResolvedValue(snapshot());
  const client = new QueryClient();
  const wrapper = ({ children }: { children: React.ReactNode }) => <QueryClientProvider client={client}><App><ControlProvider>{children}</ControlProvider></App></QueryClientProvider>;
  const { result, unmount } = renderHook(() => useControls(), { wrapper });
  try {
    let first!: Promise<ControlResult>;
    let second!: Promise<ControlResult>;
    act(() => {
      first = result.current.run(pair(), 'pause');
      second = result.current.run(pair(), 'pause');
    });
    expect(await second).toBe('not_submitted');
    expect(post).toHaveBeenCalledTimes(1);
    await act(async () => { await first; });
    expect(await first).toBe('cancellation_unconfirmed');
  } finally {
    unmount();
    client.clear();
  }
});

it('reports both pause and cancellation as unverified when the paused state is not observed', async () => {
  vi.mocked(post).mockResolvedValue(undefined);
  vi.mocked(api).mockResolvedValue({ ...snapshot(), status: 'RUNNING' });
  const client = new QueryClient();
  const wrapper = ({ children }: { children: React.ReactNode }) => <QueryClientProvider client={client}><App><ControlProvider>{children}</ControlProvider></App></QueryClientProvider>;
  const { result, unmount } = renderHook(() => useControls(), { wrapper });
  try {
    let pending!: Promise<ControlResult>;
    act(() => { pending = result.current.run(pair(), 'pause'); });
    await act(async () => { await pending; });
    expect(await pending).toBe('state_unconfirmed');
    expect(api).toHaveBeenCalledTimes(5);
    expect(result.current.results.SOLUSDC).toBe('已提交，暂停状态及撤单结果尚未确认，请检查交易所挂单及日志');
  } finally {
    unmount();
    client.clear();
  }
});

it('keeps the submitted pause unconfirmed when the status request fails', async () => {
  vi.mocked(post).mockResolvedValue(undefined);
  vi.mocked(api).mockRejectedValue(new Error('网络错误'));
  const client = new QueryClient();
  const wrapper = ({ children }: { children: React.ReactNode }) => <QueryClientProvider client={client}><App><ControlProvider>{children}</ControlProvider></App></QueryClientProvider>;
  const { result, unmount } = renderHook(() => useControls(), { wrapper });
  try {
    let pending!: Promise<ControlResult>;
    act(() => { pending = result.current.run(pair(), 'pause'); });
    await act(async () => { await pending; });
    expect(await pending).toBe('state_unconfirmed');
    expect(result.current.results.SOLUSDC).toBe('已提交，暂停状态及撤单结果尚未确认，请检查交易所挂单及日志');
  } finally {
    unmount();
    client.clear();
  }
});

it('still confirms resume when the strategy is running', async () => {
  vi.mocked(post).mockResolvedValue(undefined);
  vi.mocked(api).mockResolvedValue({ ...snapshot(), status: 'RUNNING' });
  const client = new QueryClient();
  const wrapper = ({ children }: { children: React.ReactNode }) => <QueryClientProvider client={client}><App><ControlProvider>{children}</ControlProvider></App></QueryClientProvider>;
  const { result, unmount } = renderHook(() => useControls(), { wrapper });
  try {
    let pending!: Promise<ControlResult>;
    act(() => { pending = result.current.run(pair(), 'resume', true); });
    await act(async () => { await pending; });
    expect(await pending).toBe('confirmed');
    expect(result.current.results.SOLUSDC).toBe('已确认');
  } finally {
    unmount();
    client.clear();
  }
});

it('submits resumes serially even when the first is not confirmed', async () => {
  let finishFirst!: (value: 'state_unconfirmed') => void;
  const first = new Promise<'state_unconfirmed'>(resolve => { finishFirst = resolve; });
  const run = vi.fn().mockReturnValueOnce(first).mockResolvedValueOnce('confirmed');
  const pending = runBatchControls([pair('SOLUSDC'), pair('BTCUSDC')], 'resume', run);
  expect(run).toHaveBeenCalledTimes(1);
  finishFirst('state_unconfirmed');
  expect(await pending).toEqual([{ pair: pair('SOLUSDC'), result: 'state_unconfirmed' }]);
  expect(run).toHaveBeenCalledTimes(2);
  expect(run).toHaveBeenNthCalledWith(2, expect.objectContaining({ symbol: 'BTCUSDC' }), 'resume', true);
});

it.each(['PAPER', 'TESTNET', 'LIVE'] as const)('binds a confirmed resume to the displayed %s mode', async mode => {
  vi.mocked(post).mockRejectedValue(new ApiError('策略运行模式已变化', 200));
  const client = new QueryClient();
  const wrapper = ({ children }: { children: React.ReactNode }) => <QueryClientProvider client={client}><App><ControlProvider>{children}</ControlProvider></App></QueryClientProvider>;
  const { result, unmount } = renderHook(() => useControls(), { wrapper });
  try {
    let accepted: string = '';
    await act(async () => { accepted = await result.current.run({ ...pair(), mode }, 'resume', true); });
    expect(accepted).toBe('request_unconfirmed');
    expect(post).toHaveBeenCalledWith('/api/pairs/SOLUSDC/control', { action: 'resume', expected_mode: mode });
    expect(result.current.results.SOLUSDC).toContain('策略运行模式已变化');
  } finally {
    unmount();
    client.clear();
  }
});
