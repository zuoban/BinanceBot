import { render, screen } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { beforeEach, expect, it, vi } from 'vitest';
import PairDetail from '../pages/PairDetail';
import { pair, snapshot } from './fixtures';
import type { Snapshot } from '../lib/types';

const state = vi.hoisted(() => ({ data: undefined as Snapshot | undefined }));
vi.mock('../lib/queries', () => ({
  usePairs: () => ({ data: [pair()], isPending: false }),
  usePairSnapshot: () => ({ data: state.data, live: true, stale: false, isPending: false }),
}));
vi.mock('../components/Controls', () => ({ PairControls: () => null, useControls: () => ({ results: {} }) }));
vi.mock('../components/Charts', () => ({ DepthChart: () => null, HourlyChart: () => null }));
vi.mock('../components/DataTables', () => ({ LogsTable: () => null, OrdersTable: () => null, TradesTable: () => null }));
vi.mock('../components/HistoryActions', () => ({ HistoryActions: () => null }));

beforeEach(() => {
  state.data = snapshot();
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
