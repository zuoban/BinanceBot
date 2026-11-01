import { act, renderHook } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { App } from 'antd';
import { afterEach, expect, it, vi } from 'vitest';
import { ControlProvider, runBatchControls, useControls } from '../components/Controls';
import { api, post } from '../lib/api';
import { pair, snapshot } from './fixtures';

vi.mock('../lib/api', () => ({
  api: vi.fn(),
  post: vi.fn(),
  pairPath: (value: { symbol: string }, path: string) => `/api/pairs/${value.symbol}/${path}`,
}));

afterEach(() => { vi.useRealTimers(); vi.clearAllMocks(); });

it.each([true, false])('confirms pause only when orders clear on the fifth attempt: %s', async clears => {
  vi.useFakeTimers();
  vi.mocked(post).mockResolvedValue(undefined);
  const pendingOrders = { ...snapshot(), active_orders: [{}] };
  vi.mocked(api).mockResolvedValue(pendingOrders);
  if (clears) vi.mocked(api).mockResolvedValueOnce(pendingOrders).mockResolvedValueOnce(pendingOrders)
    .mockResolvedValueOnce(pendingOrders).mockResolvedValueOnce(pendingOrders).mockResolvedValueOnce(snapshot());
  const client = new QueryClient();
  const wrapper = ({ children }: { children: React.ReactNode }) => <QueryClientProvider client={client}><App><ControlProvider>{children}</ControlProvider></App></QueryClientProvider>;
  const { result, unmount } = renderHook(() => useControls(), { wrapper });
  try {
    let pending!: Promise<boolean>;
    act(() => { pending = result.current.run(pair(), 'pause'); });
    await act(async () => { await vi.advanceTimersByTimeAsync(3200); });
    expect(api).toHaveBeenCalledTimes(4);
    expect(result.current.busy.has('SOLUSDC')).toBe(true);
    await act(async () => { await vi.advanceTimersByTimeAsync(800); });
    expect(await pending).toBe(clears);
    expect(api).toHaveBeenCalledTimes(5);
    expect(result.current.results.SOLUSDC).toBe(clears ? '已确认' : '已提交，状态尚未确认，请查看日志');
    expect(result.current.busy.has('SOLUSDC')).toBe(false);
  } finally {
    unmount();
    client.clear();
  }
});

it('submits resumes serially even when the first is not confirmed', async () => {
  let finishFirst!: (value: boolean) => void;
  const first = new Promise<boolean>(resolve => { finishFirst = resolve; });
  const run = vi.fn().mockReturnValueOnce(first).mockResolvedValueOnce(true);
  const pending = runBatchControls([pair('SOLUSDC'), pair('BTCUSDC')], 'resume', run);
  expect(run).toHaveBeenCalledTimes(1);
  finishFirst(false);
  await pending;
  expect(run).toHaveBeenCalledTimes(2);
  expect(run).toHaveBeenNthCalledWith(2, expect.objectContaining({ symbol: 'BTCUSDC' }), 'resume', true);
});
