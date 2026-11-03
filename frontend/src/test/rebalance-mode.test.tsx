import { act, renderHook } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { App } from 'antd';
import { afterEach, expect, it, vi } from 'vitest';
import { ControlProvider, useControls } from '../components/Controls';
import { modeLabel } from '../lib/format';
import { post } from '../lib/api';
import { pair } from './fixtures';

const confirm = vi.hoisted(() => vi.fn());
vi.mock('antd', () => ({
  App: Object.assign(({ children }: { children: React.ReactNode }) => children, {
    useApp: () => ({ modal: { confirm }, message: { warning: vi.fn(), error: vi.fn() } }),
  }),
}));
vi.mock('@ant-design/icons', () => ({}));
vi.mock('../lib/api', () => ({
  api: vi.fn(), post: vi.fn(),
  pairPath: (value: { symbol: string }, path: string) => `/api/pairs/${value.symbol}/${path}`,
}));

afterEach(() => { vi.clearAllMocks(); });

it.each(['PAPER', 'LIVE'] as const)('binds the rebalance confirmation to displayed %s mode', async mode => {
  confirm.mockResolvedValue(true);
  vi.mocked(post).mockResolvedValue(undefined);
  const client = new QueryClient();
  const wrapper = ({ children }: { children: React.ReactNode }) => <QueryClientProvider client={client}><App><ControlProvider>{children}</ControlProvider></App></QueryClientProvider>;
  const { result, unmount } = renderHook(() => useControls(), { wrapper });
  try {
    await act(async () => { await result.current.run({ ...pair(), mode }, 'rebalance'); });
    expect(confirm).toHaveBeenCalledWith(expect.objectContaining({ content: expect.stringContaining(modeLabel[mode]) }));
    expect(post).toHaveBeenCalledWith('/api/pairs/SOLUSDC/control', { action: 'rebalance', expected_mode: mode });
  } finally {
    unmount();
    client.clear();
  }
});
