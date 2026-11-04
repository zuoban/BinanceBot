import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, expect, it, vi } from 'vitest';
import { OverviewHistory } from '../components/OverviewHistory';
import type { HistoryAccount, OverviewHistoryData } from '../lib/types';

const history = vi.hoisted(() => ({ data: { snapshots: [], current_accounts: [] } as OverviewHistoryData, isPending: false, error: null }));
vi.mock('../lib/queries', () => ({ useOverviewHistory: () => history }));
vi.mock('../components/OverviewHistoryChart', () => ({ default: () => <div>权益图</div> }));

const paper = { mode: 'PAPER' as const, asset: 'USDC', account_id: null };
const oldLive = { mode: 'LIVE' as const, asset: 'USDC', account_id: 'old-id' };
const newLive = { mode: 'LIVE' as const, asset: 'USDC', account_id: 'new-id' };
const unknownLive = { mode: 'LIVE' as const, asset: 'USDC', account_id: null };
const unknownMode = { mode: 'UNKNOWN' as const, asset: 'USDC', account_id: null };
const group = (account: HistoryAccount, equity: string) => ({ ...account, equity, long: null, short: null });

beforeEach(() => {
  history.data = { snapshots: [], current_accounts: [] };
  vi.stubGlobal('matchMedia', vi.fn().mockImplementation(query => ({ matches: false, media: query, addListener: vi.fn(), removeListener: vi.fn(), addEventListener: vi.fn(), removeEventListener: vi.fn() })));
  vi.stubGlobal('ResizeObserver', class { observe() {} unobserve() {} disconnect() {} });
});

it('shows a meaningful empty state before accounts or snapshots exist', () => {
  render(<OverviewHistory />);
  expect(screen.getByText('暂无快照数据，整点后开始记录')).toBeInTheDocument();
});

it('defaults to the current account before its first snapshot and allows choosing history', async () => {
  const hour = Math.floor(Date.now() / 3600000) * 3600;
  history.data = { snapshots: [{ hour, groups: [group(oldLive, '100')] }], current_accounts: [newLive] };
  const user = userEvent.setup();
  render(<OverviewHistory />);
  expect(screen.getByText('近 24 小时暂无可用的权益快照')).toBeInTheDocument();
  expect(screen.getByText('近 7 天暂无可用的零点权益快照')).toBeInTheDocument();
  await user.click(screen.getByRole('combobox', { name: '选择趋势账户' }));
  await user.click(screen.getByText('实盘 · USDC · 历史账户'));
  expect(within(screen.getByLabelText('最近一次快照')).getByText('100')).toBeInTheDocument();
  expect(screen.queryByText('new-id')).not.toBeInTheDocument();
  expect(screen.queryByText('old-id')).not.toBeInTheDocument();
});

it('follows account changes and keeps unidentified live history separate from paper history', async () => {
  const hour = Math.floor(Date.now() / 3600000) * 3600;
  history.data = { snapshots: [{ hour, groups: [group(paper, '10'), group(oldLive, '100'), group(unknownLive, '50'), group(unknownMode, '25')] }], current_accounts: [paper] };
  const user = userEvent.setup();
  const view = render(<OverviewHistory />);
  expect(within(screen.getByLabelText('最近一次快照')).getByText('10')).toBeInTheDocument();
  await user.click(screen.getByRole('combobox', { name: '选择趋势账户' }));
  await user.click(screen.getByText('实盘 · USDC · 身份未知'));
  expect(within(screen.getByLabelText('最近一次快照')).getByText('50')).toBeInTheDocument();
  await user.click(screen.getByRole('combobox', { name: '选择趋势账户' }));
  await user.click(screen.getByText('未知模式 · USDC · 身份未知'));
  expect(within(screen.getByLabelText('最近一次快照')).getByText('25')).toBeInTheDocument();
  history.data = { ...history.data, current_accounts: [oldLive] };
  view.rerender(<OverviewHistory />);
  expect(within(screen.getByLabelText('最近一次快照')).getByText('100')).toBeInTheDocument();
  history.data = { ...history.data, current_accounts: [newLive] };
  view.rerender(<OverviewHistory />);
  expect(screen.getByText('近 24 小时暂无可用的权益快照')).toBeInTheDocument();
});
