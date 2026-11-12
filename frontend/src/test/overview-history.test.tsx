import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, expect, it, vi } from 'vitest';
import { OverviewHistory } from '../components/OverviewHistory';
import type * as Queries from '../lib/queries';
import type { HistoryAccount, OverviewHistoryData } from '../lib/types';

const history = vi.hoisted(() => ({ data: { snapshots: [], current_accounts: [] } as OverviewHistoryData | undefined, isPending: false, error: null as Error | null, refetch: vi.fn() }));
vi.mock('../lib/queries', async importOriginal => {
  const actual = await importOriginal<typeof Queries>();
  return { ...actual, useOverviewHistory: () => history };
});
vi.mock('../components/OverviewHistoryChart', () => ({ default: ({ data }: { data: { equity: number | null }[] }) => <div data-testid="history-chart" data-equities={JSON.stringify(data.map(point => point.equity))}>权益图</div> }));

const paper = { mode: 'PAPER' as const, asset: 'USDC', account_id: null };
const oldLive = { mode: 'LIVE' as const, asset: 'USDC', account_id: 'old-id' };
const newLive = { mode: 'LIVE' as const, asset: 'USDC', account_id: 'new-id' };
const unknownLive = { mode: 'LIVE' as const, asset: 'USDC', account_id: null };
const unknownMode = { mode: 'UNKNOWN' as const, asset: 'USDC', account_id: null };
const group = (account: HistoryAccount, equity: string) => ({ ...account, equity });

beforeEach(() => {
  history.data = { snapshots: [], current_accounts: [] };
  history.isPending = false;
  history.error = null;
  history.refetch.mockReset();
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

it('keeps legacy paper capital separate from the shared paper account series', async () => {
  const hour = Math.floor(Date.now() / 3600000) * 3600;
  const shared = { ...paper, account_id: 'paper-shared-v1' };
  history.data = { snapshots: [
    { hour: hour - 3600, groups: [group(paper, '20000')] },
    { hour, groups: [group(shared, '10020')] },
  ], current_accounts: [shared] };
  const user = userEvent.setup();
  render(<OverviewHistory />);
  expect(within(screen.getByLabelText('最近一次快照')).getByText('10,020')).toBeInTheDocument();
  const currentCharts = await screen.findAllByTestId('history-chart');
  expect(currentCharts.some(chart => chart.getAttribute('data-equities')?.includes('10020'))).toBe(true);
  expect(currentCharts.every(chart => !chart.getAttribute('data-equities')?.includes('20000'))).toBe(true);
  await user.click(screen.getByRole('combobox', { name: '选择趋势账户' }));
  await user.click(screen.getByText('模拟盘 · USDC · 历史账户'));
  expect(within(screen.getByLabelText('最近一次快照')).getByText('20,000')).toBeInTheDocument();
  expect(screen.getAllByTestId('history-chart').every(chart => !chart.getAttribute('data-equities')?.includes('10020'))).toBe(true);
});

it('renders daily snapshots with account overview, trade counts, realized pnl, and position overview, and switches dates', async () => {
  const hour = Math.floor(Date.now() / 3600000) * 3600;
  history.data = {
    snapshots: [{ hour, groups: [group(paper, '10050')] }],
    current_accounts: [paper],
    daily_snapshots: [
      {
        date: '2026-11-09',
        window_start: '2026-11-08T16:00:00Z',
        window_end: '2026-11-09T16:00:00Z',
        generated_at: '2026-11-09T16:00:05Z',
        accounts: [{
          mode: 'PAPER',
          asset: 'USDC',
          account_id: 'paper-shared-v1',
          count: 2,
          equity: '10050.25',
          available: '9000.00',
          wallet: '10000.00',
          unrealized: '50.25',
        }],
        positions: [{
          mode: 'PAPER',
          asset: 'USDC',
          count: 2,
          long: '1500.50',
          short: '200.00',
          total: '1700.50',
        }],
        pair_positions: [{
          symbol: 'SOLUSDC',
          mode: 'PAPER',
          direction: 'LONG',
          status: 'RUNNING',
          size: '10',
          entry_price: '145',
          mark_price: '150.05',
          position_amount: '1500.50',
          unrealized_pnl: '50.50',
        }],
        trades: [{
          symbol: 'SOLUSDC',
          mode: 'PAPER',
          buy_count: 8,
          sell_count: 7,
          realized_pnl: '32.75',
          pending_pnl_count: 0,
        }],
      },
      {
        date: '2026-11-08',
        window_start: '2026-11-07T16:00:00Z',
        window_end: '2026-11-08T16:00:00Z',
        generated_at: '2026-11-08T16:00:03Z',
        accounts: [{
          mode: 'PAPER',
          asset: 'USDC',
          account_id: 'paper-shared-v1',
          count: 1,
          equity: '9980.00',
          available: '9980.00',
          wallet: '9980.00',
          unrealized: '0',
        }],
        positions: [{
          mode: 'PAPER',
          asset: 'USDC',
          count: 1,
          long: '0',
          short: '0',
          total: '0',
        }],
        pair_positions: [],
        trades: [{
          symbol: 'SOLUSDC',
          mode: 'PAPER',
          buy_count: 2,
          sell_count: 1,
          realized_pnl: '-20',
          pending_pnl_count: 1,
        }],
      },
    ],
  };
  const user = userEvent.setup();
  render(<OverviewHistory />);
  const section = screen.getByLabelText('每日运行快照');
  expect(within(section).getByText('每日运行快照')).toBeInTheDocument();
  expect(within(section).getByText('成交与实现盈亏（2026-11-09）')).toBeInTheDocument();
  expect(within(section).getByText(/10,050\.25/)).toBeInTheDocument();
  expect(within(section).getByText(/^15/)).toBeInTheDocument();
  expect(within(section).getAllByText('32.75').length).toBeGreaterThan(0);
  expect(within(section).getByText(/1,700\.5/)).toBeInTheDocument();
  expect(within(section).getByLabelText('持仓明细')).toHaveTextContent('SOLUSDC');

  await user.click(within(section).getByLabelText('选择每日快照日期'));
  expect(document.querySelector('td[title="2026-11-07"]')).toHaveClass('ant-picker-cell-disabled');
  const targetCell = document.querySelector<HTMLElement>('td[title="2026-11-08"]');
  expect(targetCell).not.toBeNull();
  expect(targetCell).not.toHaveClass('ant-picker-cell-disabled');
  await user.click(targetCell!);
  expect(within(section).getByText('成交与实现盈亏（2026-11-08）')).toBeInTheDocument();
  expect(within(section).getByText('（1 笔待同步）')).toBeInTheDocument();
  expect(within(section).queryByText('-20')).not.toBeInTheDocument();
  expect(within(section).getAllByText('--')).toHaveLength(2);
});

it('surfaces a background poll error and retry action without unmounting existing snapshot data', async () => {
  const hour = Math.floor(Date.now() / 3600000) * 3600;
  history.data = {
    snapshots: [{ hour, groups: [group(paper, '10050')] }],
    current_accounts: [paper],
    daily_snapshots: [{
      date: '2026-11-09',
      window_start: '2026-11-08T16:00:00Z',
      window_end: '2026-11-09T16:00:00Z',
      generated_at: '2026-11-09T16:00:05Z',
      accounts: [{
        mode: 'PAPER',
        asset: 'USDC',
        account_id: 'paper-shared-v1',
        count: 1,
        equity: '10050.00',
        available: '9000.00',
        wallet: '10000.00',
        unrealized: '50.00',
      }],
      positions: [],
      pair_positions: [],
      trades: [],
    }],
  };
  const user = userEvent.setup();
  const view = render(<OverviewHistory />);
  const initialCharts = await screen.findAllByTestId('history-chart');
  const initialDaily = screen.getByLabelText('每日运行快照');
  expect(within(screen.getByLabelText('最近一次快照')).getByText('10,050')).toBeInTheDocument();

  history.error = new Error('网络波动，快照刷新失败');
  view.rerender(<OverviewHistory />);

  const alert = screen.getByRole('alert');
  expect(alert).toHaveTextContent('网络波动，快照刷新失败');
  expect(within(screen.getByLabelText('最近一次快照')).getByText('10,050')).toBeInTheDocument();
  expect(screen.getAllByTestId('history-chart')[0]).toBe(initialCharts[0]);
  expect(screen.getByLabelText('每日运行快照')).toBe(initialDaily);

  await user.click(within(alert).getByRole('button', { name: /重\s*试/ }));
  expect(history.refetch).toHaveBeenCalledOnce();
});

it('skips empty hourly charts when only daily snapshots exist', () => {
  history.data = {
    snapshots: [],
    current_accounts: [],
    daily_snapshots: [{
      date: '2026-11-09',
      window_start: '2026-11-08T16:00:00Z',
      window_end: '2026-11-09T16:00:00Z',
      generated_at: '2026-11-09T16:00:05Z',
      accounts: [],
      positions: [],
      pair_positions: [],
      trades: [],
    }],
  };
  render(<OverviewHistory />);
  expect(screen.queryByText('近 24 小时')).not.toBeInTheDocument();
  expect(screen.getByLabelText('每日运行快照')).toBeInTheDocument();
});
