import { render, screen } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { beforeEach, expect, it, vi } from 'vitest';
import PairDetail from '../pages/PairDetail';
import { Pnl } from '../components/Common';
import { pair, snapshot } from './fixtures';
import type * as Queries from '../lib/queries';
import type { Pair, Snapshot } from '../lib/types';

const state = vi.hoisted(() => ({
  data: undefined as Snapshot | undefined,
  pairs: [] as Pair[] | undefined,
  pairsPending: false,
  pairsFetching: false,
}));
vi.mock('../lib/queries', async importOriginal => {
  const actual = await importOriginal<typeof Queries>();
  return {
    ...actual,
    usePairs: () => ({ data: state.pairs, isPending: state.pairsPending, isFetching: state.pairsFetching }),
    usePairSnapshot: () => ({ data: state.data, live: true, stale: false, isPending: false }),
  };
});
vi.mock('../components/Controls', () => ({ PairControls: () => null, useControls: () => ({ busy: new Set<string>(), pending: {}, results: {}, clearResult: vi.fn(), run: vi.fn() }) }));
vi.mock('../components/Charts', () => ({ DepthChart: () => null, HourlyChart: () => null }));
vi.mock('../components/DataTables', () => ({ LogsTable: () => null, OrdersTable: () => null, TradesTable: () => null }));
vi.mock('../components/HistoryActions', () => ({ HistoryActions: () => null }));

beforeEach(() => {
  state.data = snapshot();
  state.pairs = [pair()];
  state.pairsPending = false;
  state.pairsFetching = false;
});

function view() {
  return <MemoryRouter initialEntries={['/pairs/SOLUSDC']}><Routes><Route path="/pairs/:symbol" element={<PairDetail />} /></Routes></MemoryRouter>;
}

it('updates the net realized profit only after pending trades are verified', () => {
  state.data!.stats.total_realized_pnl = '12.5';
  state.data!.stats.total_commission = '2.25';
  state.data!.stats.pending_pnl_trades = 1;
  const { rerender } = render(view());
  expect(screen.getByText('扣费后 --')).toBeInTheDocument();
  state.data!.stats.pending_pnl_trades = 0;
  rerender(view());
  expect(screen.getByText('扣费后 10.25')).toBeInTheDocument();
});

it.each([
  ['missing realized profit', { total_realized_pnl: null }],
  ['missing commission', { total_commission: null }],
  ['empty commission', { total_commission: '' }],
  ['invalid realized profit', { total_realized_pnl: 'invalid' }],
  ['missing pending count', { pending_pnl_trades: undefined }],
  ['invalid pending count', { pending_pnl_trades: -1 }],
])('does not show a definite net profit with %s', (_label, fields) => {
  Object.assign(state.data!.stats, fields);
  render(view());
  expect(screen.getByText('扣费后 --')).toBeInTheDocument();
});

it('shows zero when the inputs are explicitly zero', () => {
  render(view());
  expect(screen.getByText('扣费后 0.00')).toBeInTheDocument();
});

it('uses wallet balance for the available ratio when equity includes unrealized profit', () => {
  render(view());
  expect(screen.getByText('可用比例（按钱包余额）').parentElement).toHaveTextContent('80.00%');
});

it('does not show an available ratio when wallet balance is zero', () => {
  state.data!.account.total_wallet_balance = '0';
  render(view());
  expect(screen.getByText('可用比例（按钱包余额）').parentElement).toHaveTextContent('--');
});

it('uses the snapshot mode for account freshness while pair metadata catches up', () => {
  const current = snapshot('SOLUSDC', 'LIVE');
  current.account = { ...current.account, margin_balance: '100', available_balance: '100', unrealized_profit: '0', update_time: new Date(Date.now() - 60_000).toISOString() };
  state.data = current;
  render(view());
  const card = screen.getByText('账户权益 / USDC').closest('.ant-card')!;
  expect(card.querySelector('.ant-statistic-content-value')).toHaveTextContent('--');
});

it('displays the strategy direction badge in the detail signal strip', () => {
  state.data!.grid_config.direction = 'SHORT';
  render(view());
  expect(screen.getByRole('img', { name: '策略方向：做空' })).toHaveClass('strategy-direction-short');
  expect(screen.getByText('方向').parentElement).toHaveTextContent('↓做空');
});

it('computes net pnl and position quantity exactly and clamps sub-1e-12 float residue in Pnl', () => {
  state.data!.stats.total_realized_pnl = '0.3';
  state.data!.stats.total_commission = '0.1';
  state.data!.position.size = '1.00025';
  render(<>{view()}<div data-testid="residue"><Pnl value={-2.78e-17} compact /></div></>);
  expect(screen.getByText('扣费后 0.20')).toBeInTheDocument();
  expect(screen.getByText('持仓数量 1.00025')).toBeInTheDocument();
  expect(screen.getByTestId('residue')).toHaveTextContent('0');
});

it('shows detail loading instead of a missing-pair warning while pairs are still refetching', () => {
  state.data = undefined;
  state.pairs = [pair('BTCUSDC')];
  state.pairsFetching = true;
  const { rerender } = render(view());
  expect(screen.queryByText('该交易对不存在或已移除')).not.toBeInTheDocument();
  expect(screen.getByRole('status', { name: '正在加载策略详情' })).toBeInTheDocument();
  state.pairsFetching = false;
  rerender(view());
  expect(screen.getByText('该交易对不存在或已移除')).toBeInTheDocument();
});
