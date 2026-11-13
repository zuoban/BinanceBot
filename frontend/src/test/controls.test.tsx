import { act, render, renderHook } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { App } from 'antd';
import { afterEach, expect, it, vi } from 'vitest';
import { ControlProvider, runBatchControls, useControls } from '../components/Controls';
import type { ControlResult } from '../components/Controls';
import { api, ApiError, post } from '../lib/api';
import type * as Api from '../lib/api';
import { pair, snapshot } from './fixtures';

const modalConfirm = vi.hoisted(() => vi.fn());
const messages = vi.hoisted(() => ({ warning: vi.fn(), error: vi.fn() }));
vi.mock('antd', () => ({
  App: Object.assign(({ children }: { children: React.ReactNode }) => children, {
    useApp: () => ({ modal: { confirm: modalConfirm }, message: messages }),
  }),
}));
vi.mock('@ant-design/icons', () => ({}));

vi.mock('../lib/api', async importOriginal => ({
  ...await importOriginal<typeof Api>(),
  api: vi.fn(),
  post: vi.fn(),
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
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ['accounts'] });
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

it.each(['PAPER', 'LIVE'] as const)('binds a confirmed resume to the displayed %s mode', async mode => {
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

it('throws a clear error when useControls is called outside ControlProvider', () => {
  expect(() => renderHook(() => useControls())).toThrow('useControls must be used within ControlProvider');
});

it('aborts pending confirmation timers and skips state/message updates on unmount', async () => {
  vi.useFakeTimers();
  vi.mocked(post).mockResolvedValue(undefined);
  vi.mocked(api).mockResolvedValue(snapshot());
  const client = new QueryClient();
  const wrapper = ({ children }: { children: React.ReactNode }) => <QueryClientProvider client={client}><App><ControlProvider>{children}</ControlProvider></App></QueryClientProvider>;
  const { result, unmount } = renderHook(() => useControls(), { wrapper });
  let pending!: Promise<ControlResult>;
  act(() => { pending = result.current.run(pair(), 'pause', true); });
  await act(async () => { await vi.advanceTimersByTimeAsync(0); });
  expect(post).toHaveBeenCalledOnce();
  expect(api).not.toHaveBeenCalled();
  unmount();
  await act(async () => { await vi.advanceTimersByTimeAsync(5000); });
  expect(await pending).toBe('state_unconfirmed');
  expect(api).not.toHaveBeenCalled();
  expect(messages.warning).not.toHaveBeenCalled();
  client.clear();
});

it('isolates per-symbol useControls subscriptions from unrelated symbol transitions', async () => {
  let finishPost!: () => void;
  vi.mocked(post).mockReturnValue(new Promise<void>(resolve => { finishPost = resolve; }));
  vi.mocked(api).mockResolvedValue({ ...snapshot('SOLUSDC'), status: 'PAUSED' });
  const client = new QueryClient();
  let solRenders = 0;
  let btcRenders = 0;
  let staticRenders = 0;
  function Probe() {
    const sol = useControls('SOLUSDC');
    solRenders++;
    return <button type="button" onClick={() => void sol.run(pair('SOLUSDC'), 'pause', true)}>{sol.busy.has('SOLUSDC') ? 'busy' : 'idle'}</button>;
  }
  function UnrelatedProbe() {
    useControls('BTCUSDC');
    btcRenders++;
    return null;
  }
  function StaticProbe() {
    useControls('');
    staticRenders++;
    return null;
  }
  const { getByRole, unmount } = render(
    <QueryClientProvider client={client}>
      <App>
        <ControlProvider>
          <Probe />
          <UnrelatedProbe />
          <StaticProbe />
        </ControlProvider>
      </App>
    </QueryClientProvider>,
  );
  const initialBtc = btcRenders;
  const initialStatic = staticRenders;
  act(() => { getByRole('button').click(); });
  expect(solRenders).toBeGreaterThan(1);
  expect(btcRenders).toBe(initialBtc);
  expect(staticRenders).toBe(initialStatic);
  await act(async () => { finishPost(); });
  unmount();
  client.clear();
});

it('completes two-step reverse confirmation for an active position, submits mode and direction, and invalidates config', async () => {
  const preflight = {
    ...snapshot('SOLUSDC', 'LIVE'),
    status: 'RUNNING' as const,
    position: { ...snapshot('SOLUSDC', 'LIVE').position, size: '2.5' },
  };
  const postReverse = {
    ...preflight,
    grid_config: { ...preflight.grid_config, direction: 'SHORT' as const },
    position: { ...preflight.position, size: '-2.5' },
  };
  vi.mocked(api).mockResolvedValueOnce(preflight).mockResolvedValue(postReverse);
  vi.mocked(post).mockResolvedValue(undefined);
  modalConfirm.mockResolvedValueOnce(true).mockResolvedValueOnce(true);

  const client = new QueryClient();
  const invalidate = vi.spyOn(client, 'invalidateQueries');
  const wrapper = ({ children }: { children: React.ReactNode }) => <QueryClientProvider client={client}><App><ControlProvider>{children}</ControlProvider></App></QueryClientProvider>;
  const { result, unmount } = renderHook(() => useControls(), { wrapper });
  try {
    let outcome!: ControlResult;
    await act(async () => {
      outcome = await result.current.run({ ...pair('SOLUSDC'), mode: 'LIVE', status: 'RUNNING' }, 'reverse');
    });
    expect(outcome).toBe('confirmed');
    expect(result.current.results.SOLUSDC).toBe('反手交易已完成');
    expect(modalConfirm).toHaveBeenCalledTimes(2);
    expect(modalConfirm.mock.calls[0][0]).toMatchObject({
      title: 'SOLUSDC · 反手交易确认（1/2）',
      okText: '下一步',
      cancelText: '取消',
    });
    expect(modalConfirm.mock.calls[1][0]).toMatchObject({
      title: 'SOLUSDC · 最终执行确认（2/2）',
      okText: '确认反手',
      okButtonProps: { danger: true },
      cancelText: '取消',
    });
    const { unmount: unmountStepOne, container: stepOneContainer } = render(<>{modalConfirm.mock.calls[0][0].content}</>);
    expect(stepOneContainer.textContent).toContain('多头 2.5');
    expect(stepOneContainer.textContent).toContain('撤单 → 市价平多头 2.5 → 切换方向 → 市价开空 2.5');
    expect(stepOneContainer.textContent).toContain('保持运行并按新方向重建网格');
    unmountStepOne();

    const { unmount: unmountStepTwo, container: stepTwoContainer } = render(<>{modalConfirm.mock.calls[1][0].content}</>);
    expect(stepTwoContainer.textContent).toContain('滑点与 Taker 手续费');
    unmountStepTwo();

    expect(post).toHaveBeenCalledWith(
      '/api/pairs/SOLUSDC/control',
      { action: 'reverse', expected_mode: 'LIVE', expected_direction: 'LONG' },
      { timeoutMs: 60_000 },
    );
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ['config', 'SOLUSDC'] });
  } finally {
    unmount();
    client.clear();
  }
});

it('aborts reverse without submitting when canceled at step 1 or step 2', async () => {
  vi.mocked(api).mockResolvedValue(snapshot('SOLUSDC', 'PAPER'));
  const client = new QueryClient();
  const wrapper = ({ children }: { children: React.ReactNode }) => <QueryClientProvider client={client}><App><ControlProvider>{children}</ControlProvider></App></QueryClientProvider>;
  const { result, unmount } = renderHook(() => useControls(), { wrapper });
  try {
    modalConfirm.mockResolvedValueOnce(false);
    let firstOutcome!: ControlResult;
    await act(async () => { firstOutcome = await result.current.run(pair('SOLUSDC'), 'reverse'); });
    expect(firstOutcome).toBe('not_submitted');
    expect(modalConfirm).toHaveBeenCalledTimes(1);
    expect(post).not.toHaveBeenCalled();

    modalConfirm.mockClear();
    modalConfirm.mockResolvedValueOnce(true).mockResolvedValueOnce(false);
    let secondOutcome!: ControlResult;
    await act(async () => { secondOutcome = await result.current.run(pair('SOLUSDC'), 'reverse'); });
    expect(secondOutcome).toBe('not_submitted');
    expect(modalConfirm).toHaveBeenCalledTimes(2);
    expect(post).not.toHaveBeenCalled();
  } finally {
    unmount();
    client.clear();
  }
});

it('handles empty-position reverse confirmation and preserves PAUSED status expectation', async () => {
  const base = snapshot('SOLUSDC', 'PAPER');
  const preflight = {
    ...base,
    status: 'PAUSED' as const,
    grid_config: { ...base.grid_config, direction: 'SHORT' as const },
    position: { ...base.position, size: '0' },
  };
  const postReverse = {
    ...preflight,
    grid_config: { ...preflight.grid_config, direction: 'LONG' as const },
  };
  vi.mocked(api).mockResolvedValueOnce(preflight).mockResolvedValue(postReverse);
  vi.mocked(post).mockResolvedValue(undefined);
  modalConfirm.mockResolvedValueOnce(true).mockResolvedValueOnce(true);

  const client = new QueryClient();
  const wrapper = ({ children }: { children: React.ReactNode }) => <QueryClientProvider client={client}><App><ControlProvider>{children}</ControlProvider></App></QueryClientProvider>;
  const { result, unmount } = renderHook(() => useControls(), { wrapper });
  try {
    let outcome!: ControlResult;
    const pausedShortPair = { ...pair('SOLUSDC'), status: 'PAUSED' as const, grid_config: preflight.grid_config };
    await act(async () => { outcome = await result.current.run(pausedShortPair, 'reverse'); });
    expect(outcome).toBe('confirmed');
    expect(result.current.results.SOLUSDC).toBe('反手交易已完成');
    const { unmount: unmountStepOne, container } = render(<>{modalConfirm.mock.calls[0][0].content}</>);
    expect(container.textContent).toContain('空仓（0）');
    expect(container.textContent).toContain('撤销策略挂单 → 切换策略方向（不提交市价单）');
    expect(container.textContent).toContain('保持已暂停');
    unmountStepOne();
    expect(post).toHaveBeenCalledWith(
      '/api/pairs/SOLUSDC/control',
      { action: 'reverse', expected_mode: 'PAPER', expected_direction: 'SHORT' },
      { timeoutMs: 60_000 },
    );
  } finally {
    unmount();
    client.clear();
  }
});
