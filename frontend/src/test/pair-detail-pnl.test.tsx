import { act, render, renderHook, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { beforeEach, expect, it, vi } from 'vitest';
import PairDetail, { resolveLogs, resolveTrades } from '../pages/PairDetail';
import { Pnl } from '../components/Common';
import { invalidateStrategyQueries } from '../lib/queries';
import { AccountsContext } from '../lib/session';
import { log, pair, snapshot, trade } from './fixtures';
import type * as Api from '../lib/api';
import type * as Queries from '../lib/queries';
import type { Log, Pair, Snapshot, Trade } from '../lib/types';

const state = vi.hoisted(() => ({
  data: undefined as Snapshot | undefined,
  dataUpdatedAt: 0,
  pairs: [] as Pair[] | undefined,
  pairsPending: false,
  pairsFetching: false,
  pairsError: null as Error | null,
  queryError: null as Error | null,
  stale: false,
  refetchPairs: vi.fn(),
  refetchQuery: vi.fn(),
  api: vi.fn(),
}));
vi.mock('../lib/api', async importOriginal => ({ ...await importOriginal<typeof Api>(), api: state.api }));
vi.mock('../lib/queries', async importOriginal => {
  const actual = await importOriginal<typeof Queries>();
  return {
    ...actual,
    usePairs: () => ({ data: state.pairs, error: state.pairsError, isPending: state.pairsPending, isFetching: state.pairsFetching, refetch: state.refetchPairs }),
    usePairSnapshot: () => ({ data: state.data, dataUpdatedAt: state.dataUpdatedAt, error: state.queryError, live: true, stale: state.stale, isPending: false, refetch: state.refetchQuery }),
  };
});
vi.mock('../components/Controls', () => ({ PairControls: () => null, useControls: () => ({ busy: new Set<string>(), pending: {}, results: {}, clearResult: vi.fn(), run: vi.fn() }) }));
vi.mock('../components/Charts', () => ({ DepthChart: () => null, HourlyChart: () => null }));
vi.mock('../components/DataTables', () => ({
  OrdersTable: () => null,
  TradesTable: ({ trades }: { trades: Trade[] }) => <div data-testid="trades-ids">{trades.map(t => t.trade_id).join(',')}</div>,
  LogsTable: ({ logs }: { logs: Log[] }) => <div data-testid="logs-msgs">{logs.map(l => l.message).join(',')}</div>,
}));
vi.mock('../components/HistoryActions', () => ({ HistoryActions: () => null }));

let testClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });

beforeEach(() => {
  testClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  state.data = snapshot();
  state.dataUpdatedAt = 0;
  state.pairs = [pair()];
  state.pairsPending = false;
  state.pairsFetching = false;
  state.pairsError = null;
  state.queryError = null;
  state.stale = false;
  state.refetchPairs.mockReset();
  state.refetchQuery.mockReset();
  state.api.mockReset();
});

function view(initialEntry = '/pairs/SOLUSDC') {
  return (
    <QueryClientProvider client={testClient}>
      <MemoryRouter initialEntries={[initialEntry]}>
        <Routes><Route path="/pairs/:symbol" element={<PairDetail />} /></Routes>
      </MemoryRouter>
    </QueryClientProvider>
  );
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
  ['invalid non-quote count', { non_quote_commission_trades: -1 }],
])('does not show a definite net profit with %s', (_label, fields) => {
  Object.assign(state.data!.stats, fields);
  render(view());
  expect(screen.getByText('扣费后 --')).toBeInTheDocument();
});

it('withholds quote net PnL and annotates cumulative fees when non-quote commissions exist', () => {
  state.data!.stats.total_realized_pnl = '12.5';
  state.data!.stats.total_commission = '0.04';
  state.data!.stats.non_quote_commission_trades = 2;
  state.data!.stats.pending_pnl_trades = 1;
  const { rerender } = render(view());
  expect(screen.getByText('扣费后 --')).toBeInTheDocument();

  state.data!.stats.pending_pnl_trades = 0;
  rerender(view());
  expect(screen.getByText('含非计价币手续费')).toBeInTheDocument();
  expect(screen.queryByText(/扣费后/)).not.toBeInTheDocument();
  expect(screen.getByText('累计手续费').parentElement).toHaveTextContent('0.0400 USDC（另 2 笔其他币种）');
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

it('guards ErrorNotice retry so missing-pair errors do not force-refetch disabled snapshot query', async () => {
  state.data = undefined;
  state.pairs = undefined;
  state.pairsError = new Error('列表加载失败');
  const user = userEvent.setup();
  const { unmount } = render(view());
  await user.click(screen.getByRole('button', { name: /重\s*试/ }));
  expect(state.refetchPairs).toHaveBeenCalledOnce();
  expect(state.refetchQuery).not.toHaveBeenCalled();
  unmount();

  state.pairs = [pair()];
  state.pairsError = null;
  state.queryError = new Error('快照加载失败');
  render(view());
  await user.click(screen.getByRole('button', { name: /重\s*试/ }));
  expect(state.refetchPairs).toHaveBeenCalledOnce();
  expect(state.refetchQuery).toHaveBeenCalledOnce();
});

it('lazily fetches full buffered trades and logs on tab activation and falls back cleanly on failure', async () => {
  state.data!.recent_trades = [{ trade_id: 'snap-t1', symbol: 'SOLUSDC', mode: 'PAPER', side: 'BUY', price: '100', quantity: '1', amount_usdc: '100', realized_pnl: '0', commission: '0', pnl_verified: true, is_maker: true, timestamp: '2026-11-05T01:00:00Z', note: '' }];
  state.data!.recent_logs = [{ timestamp: '2026-11-05T01:01:00Z', level: 'INFO', message: 'snap-log' }];
  state.api.mockImplementation((path: string) => {
    if (path === '/api/pairs/SOLUSDC/trades') {
      return Promise.resolve([
        { trade_id: 'full-t2', symbol: 'SOLUSDC', mode: 'PAPER', side: 'SELL', price: '101', quantity: '1', amount_usdc: '101', realized_pnl: '1', commission: '0', pnl_verified: true, is_maker: true, timestamp: '2026-11-05T01:02:00Z', note: '' },
        { trade_id: 'snap-t1', symbol: 'SOLUSDC', mode: 'PAPER', side: 'BUY', price: '100', quantity: '1', amount_usdc: '100', realized_pnl: '0', commission: '0', pnl_verified: true, is_maker: true, timestamp: '2026-11-05T01:00:00Z', note: '' },
      ]);
    }
    if (path === '/api/pairs/SOLUSDC/logs') {
      return Promise.resolve([
        { timestamp: '2026-11-05T01:01:00Z', level: 'SUCCESS', message: 'newer-log' },
        { timestamp: '2026-11-05T01:00:00Z', level: 'INFO', message: 'older-log' },
      ]);
    }
    return Promise.reject(new Error('unexpected path'));
  });
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const user = userEvent.setup();
  const { unmount } = render(<QueryClientProvider client={client}>{view()}</QueryClientProvider>);
  expect(state.api).not.toHaveBeenCalled();

  await user.click(screen.getByRole('tab', { name: '成交历史' }));
  await waitFor(() => expect(screen.getByTestId('trades-ids')).toHaveTextContent('full-t2,snap-t1'));

  await user.click(screen.getByRole('tab', { name: '运行日志' }));
  await waitFor(() => expect(screen.getByTestId('logs-msgs')).toHaveTextContent('newer-log,older-log'));
  unmount();

  const failingClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  state.api.mockRejectedValue(new Error('接口不可用'));
  render(<QueryClientProvider client={failingClient}>{view('/pairs/SOLUSDC?tab=trades')}</QueryClientProvider>);
  await waitFor(() => expect(state.api).toHaveBeenCalledWith('/api/pairs/SOLUSDC/trades', expect.anything()));
  expect(await screen.findByTestId('trades-ids')).toHaveTextContent('snap-t1');
});

it('never resurrects pre-clear fetched trades after clearing trades and receiving a new fill', async () => {
  const preClearFetched: Trade[] = [
    { trade_id: 'old-2', symbol: 'SOLUSDC', mode: 'PAPER', side: 'SELL', price: '102', quantity: '1', amount_usdc: '102', realized_pnl: '2', commission: '0', pnl_verified: true, is_maker: true, timestamp: '2026-11-05T01:02:00Z', note: '' },
    { trade_id: 'old-1', symbol: 'SOLUSDC', mode: 'PAPER', side: 'BUY', price: '100', quantity: '1', amount_usdc: '100', realized_pnl: '0', commission: '0', pnl_verified: true, is_maker: true, timestamp: '2026-11-05T01:00:00Z', note: '' },
  ];
  state.api.mockResolvedValue(preClearFetched);
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const { rerender, unmount } = render(<QueryClientProvider client={client}>{view('/pairs/SOLUSDC?tab=trades')}</QueryClientProvider>);
  await waitFor(() => expect(screen.getByTestId('trades-ids')).toHaveTextContent('old-2,old-1'));

  const postClear = snapshot('SOLUSDC', 'PAPER');
  postClear.stats.total_trades = 1;
  postClear.recent_trades = [
    { trade_id: 'new-1', symbol: 'SOLUSDC', mode: 'PAPER', side: 'BUY', price: '99', quantity: '1', amount_usdc: '99', realized_pnl: '0', commission: '0', pnl_verified: true, is_maker: true, timestamp: '2026-11-05T01:05:00Z', note: '' },
    { trade_id: 'other-sym', symbol: 'BTCUSDC', mode: 'PAPER', side: 'BUY', price: '80000', quantity: '1', amount_usdc: '80000', realized_pnl: '0', commission: '0', pnl_verified: true, is_maker: true, timestamp: '2026-11-05T01:05:01Z', note: '' },
  ];
  state.data = postClear;
  state.dataUpdatedAt = Date.now() + 10_000;
  rerender(<QueryClientProvider client={client}>{view('/pairs/SOLUSDC?tab=trades')}</QueryClientProvider>);
  expect(screen.getByTestId('trades-ids')).toHaveTextContent(/^new-1$/);
  expect(resolveTrades(postClear, preClearFetched, false, 1_000, 2_000).map(t => t.trade_id)).toEqual(['new-1']);
  unmount();

  const inactiveClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  inactiveClient.setQueryData(['snapshot', 'SOLUSDC', true, 'trades', 'PAPER'], preClearFetched);
  await invalidateStrategyQueries(inactiveClient, { pairs: false, overview: false, snapshot: 'SOLUSDC', clearTrades: 'SOLUSDC', cancelFirst: true });
  expect(inactiveClient.getQueryData(['snapshot', 'SOLUSDC', true, 'trades', 'PAPER'])).toBeUndefined();
});

it('preserves the newest 50 live logs during a >=50-log burst between /logs polls', async () => {
  const oldFetched: Log[] = Array.from({ length: 60 }, (_, idx) => ({
    timestamp: new Date(Date.parse('2026-11-05T01:00:59Z') - idx * 1000).toISOString(),
    level: 'INFO',
    message: `old-${59 - idx}`,
  }));
  state.api.mockResolvedValue(oldFetched);
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const { rerender, unmount } = render(<QueryClientProvider client={client}>{view('/pairs/SOLUSDC?tab=logs')}</QueryClientProvider>);
  await waitFor(() => expect(screen.getByTestId('logs-msgs')).toHaveTextContent(/^old-59,old-58/));

  const burstLogs: Log[] = Array.from({ length: 50 }, (_, idx) => ({
    timestamp: new Date(Date.parse('2026-11-05T01:02:50Z') - idx * 1000).toISOString(),
    level: 'INFO',
    message: `burst-${49 - idx}`,
  }));
  const updated = snapshot('SOLUSDC', 'PAPER');
  updated.recent_logs = burstLogs;
  state.data = updated;
  state.dataUpdatedAt = Date.now() + 10_000;
  rerender(<QueryClientProvider client={client}>{view('/pairs/SOLUSDC?tab=logs')}</QueryClientProvider>);
  const text = screen.getByTestId('logs-msgs').textContent ?? '';
  expect(text.startsWith('burst-49,burst-48,')).toBe(true);
  expect(text.includes(',burst-0,old-59,')).toBe(true);
  expect(resolveLogs(updated, oldFetched, false, 1_000, 2_000)).toHaveLength(110);
  unmount();
});

it('renders valid account equity when s.generated_at and dataUpdatedAt have a 20s browser clock skew or sibling offset', () => {
  const clientNow = Date.now();
  for (const serverNow of [clientNow - 20_000, clientNow + 20_000]) {
    const skewed = snapshot('SOLUSDC', 'PAPER');
    skewed.generated_at = new Date(serverNow).toISOString();
    skewed.position.size = '1';
    skewed.account = { ...skewed.account, margin_balance: '105', available_balance: '80', unrealized_profit: '5', update_time: new Date(serverNow).toISOString() };
    skewed.ticker.mark_update_time = new Date(serverNow).toISOString();
    state.data = skewed;
    state.dataUpdatedAt = clientNow;
    const { unmount } = render(view());
    const card = screen.getByText('账户权益 / USDC').closest('.ant-card')!;
    expect(card.querySelector('.ant-statistic-content-value')).toHaveTextContent('105.00');
    unmount();
  }

  // Fallback to sibling liveAccounts clock offset when s.generated_at is omitted in unit-test fixtures, while respecting sibling PAPER valuation.
  const serverNow = clientNow - 20_000;
  const withoutGen = snapshot('SOLUSDC', 'PAPER');
  withoutGen.position.size = '1';
  withoutGen.account = { ...withoutGen.account, margin_balance: '210', available_balance: '150', unrealized_profit: '10', update_time: new Date(serverNow).toISOString() };
  withoutGen.ticker.mark_update_time = new Date(serverNow).toISOString();
  const sibling = snapshot('BTCUSDC', 'PAPER');
  sibling.position.size = '1';
  sibling.account = { ...withoutGen.account };
  sibling.ticker.mark_update_time = new Date(serverNow).toISOString();
  const otherMode = snapshot('ETHUSDC', 'LIVE');
  otherMode.account = { ...otherMode.account, margin_balance: '999', update_time: new Date(serverNow).toISOString() };
  state.data = withoutGen;
  state.dataUpdatedAt = clientNow;
  render(<AccountsContext.Provider value={[
    { mode: 'LIVE', snapshot: otherMode, generated_at_ms: serverNow, received_at_ms: clientNow },
    { mode: 'PAPER', snapshot: sibling, generated_at_ms: serverNow, received_at_ms: clientNow },
  ]}>{view()}</AccountsContext.Provider>);
  const card = screen.getByText('账户权益 / USDC').closest('.ant-card')!;
  expect(card.querySelector('.ant-statistic-content-value')).toHaveTextContent('210.00');
});

it('merges newer WS trades, pnl_verified upgrades, and WS logs when an overlapping HTTP poll resolves late', () => {
  const wsSnap = snapshot('SOLUSDC', 'PAPER');
  wsSnap.stats.total_trades = 3;
  wsSnap.recent_trades = [
    trade({ trade_id: 't3', timestamp: '2026-11-05T01:03:00Z', pnl_verified: false }),
    trade({ trade_id: 't2', timestamp: '2026-11-05T01:02:00Z', pnl_verified: true, realized_pnl: '1.5' }),
  ];
  const slowHttpTrades: Trade[] = [
    trade({ trade_id: 't2', timestamp: '2026-11-05T01:02:00Z', pnl_verified: false, realized_pnl: '0' }),
    trade({ trade_id: 't1', timestamp: '2026-11-05T01:01:00Z', pnl_verified: true, realized_pnl: '0.5' }),
  ];
  const merged = resolveTrades(wsSnap, slowHttpTrades, false, 2_020, 2_000);
  expect(merged.map(t => [t.trade_id, t.pnl_verified])).toEqual([['t3', false], ['t2', true], ['t1', true]]);
  expect(resolveTrades(wsSnap, [], false, 2_050, 2_000)).toEqual([]);
  const clearedSnap = snapshot('SOLUSDC', 'PAPER');
  clearedSnap.stats.total_trades = 0;
  expect(resolveTrades(clearedSnap, slowHttpTrades, false, 2_000, 2_050)).toEqual([]);

  wsSnap.recent_logs = [
    log({ timestamp: '2026-11-05T01:03:00Z', message: 'ws-newest' }),
    log({ timestamp: '2026-11-05T01:02:00Z', message: 'shared-head' }),
  ];
  const slowHttpLogs: Log[] = [
    log({ timestamp: '2026-11-05T01:02:00Z', message: 'shared-head' }),
    log({ timestamp: '2026-11-05T01:01:00Z', message: 'older-tail' }),
  ];
  expect(resolveLogs(wsSnap, slowHttpLogs, false, 2_020, 2_000).map(l => l.message)).toEqual(['ws-newest', 'shared-head', 'older-tail']);
  const emptyRecent = snapshot('SOLUSDC', 'PAPER');
  emptyRecent.recent_logs = [];
  const sameTsLogs = [log({ timestamp: '2026-11-05T01:00:00Z', message: 'newest-same-ts' }), log({ timestamp: '2026-11-05T01:00:00Z', message: 'older-same-ts' })];
  expect(resolveLogs(emptyRecent, sameTsLogs, false, 2_000, 1_000).map(l => l.message)).toEqual(['newest-same-ts', 'older-same-ts']);
});

it('isolates LIVE account equity across different account_scope tags and credential epochs', () => {
  const now = Date.now();
  const current = snapshot('SOLUSDC', 'LIVE');
  current.account_scope = 'scope-a';
  current.account = { ...current.account, margin_balance: '120', available_balance: '90', total_wallet_balance: '120', unrealized_profit: '0', update_time: new Date(now - 2_000).toISOString() };
  const otherAccountSibling = snapshot('BTCUSDC', 'LIVE');
  otherAccountSibling.account_scope = 'scope-b';
  otherAccountSibling.account = { ...otherAccountSibling.account, margin_balance: '999', available_balance: '999', total_wallet_balance: '999', unrealized_profit: '0', update_time: new Date(now).toISOString() };
  const staleEpochSelf = snapshot('SOLUSDC', 'LIVE');
  staleEpochSelf.account_scope = 'scope-b';
  staleEpochSelf.account = { ...otherAccountSibling.account, margin_balance: '888' };
  state.pairs = [{ ...pair('SOLUSDC'), mode: 'LIVE' }, { ...pair('BTCUSDC'), mode: 'LIVE' }];
  state.data = current;
  state.dataUpdatedAt = now;
  render(<AccountsContext.Provider value={[
    { mode: 'LIVE', snapshot: staleEpochSelf, generated_at_ms: now, received_at_ms: now },
    { mode: 'LIVE', snapshot: otherAccountSibling, generated_at_ms: now, received_at_ms: now },
  ]}>{view()}</AccountsContext.Provider>);
  const card = screen.getByText('账户权益 / USDC').closest('.ant-card')!;
  expect(card.querySelector('.ant-statistic-content-value')).toHaveTextContent('120.00');
});

it('withholds shared PAPER equity as -- while a same-asset PAPER sibling in pairs is unobserved', () => {
  const now = Date.now();
  const current = snapshot('SOLUSDC', 'PAPER');
  current.account_scope = 'paper-scope';
  current.account = { ...current.account, margin_balance: '105', available_balance: '80', total_wallet_balance: '100', unrealized_profit: '5', update_time: new Date(now).toISOString() };
  const sibling = snapshot('BTCUSDC', 'PAPER');
  sibling.account_scope = 'paper-scope';
  sibling.account = { ...current.account };
  sibling.ticker.mark_update_time = new Date(now).toISOString();
  state.pairs = [pair('SOLUSDC'), pair('BTCUSDC')];
  state.data = current;
  state.dataUpdatedAt = now;
  const { rerender } = render(<AccountsContext.Provider value={[]}>{view()}</AccountsContext.Provider>);
  const card = screen.getByText('账户权益 / USDC').closest('.ant-card')!;
  expect(card.querySelector('.ant-statistic-content-value')).toHaveTextContent('--');

  rerender(<AccountsContext.Provider value={[{ mode: 'PAPER', snapshot: sibling, generated_at_ms: now, received_at_ms: now }]}>{view()}</AccountsContext.Provider>);
  expect(card.querySelector('.ant-statistic-content-value')).toHaveTextContent('105.00');
});

it('blanks LIVE equity to -- when PairDetail snapshot is stale even if a fresh LIVE sibling exists', () => {
  const now = Date.now();
  const staleCurrent = snapshot('SOLUSDC', 'LIVE');
  staleCurrent.account_scope = 'scope-a';
  staleCurrent.account = { ...staleCurrent.account, margin_balance: '120', available_balance: '100', total_wallet_balance: '120', unrealized_profit: '0', update_time: new Date(now).toISOString() };
  const freshSibling = snapshot('BTCUSDC', 'LIVE');
  freshSibling.account_scope = 'scope-a';
  freshSibling.account = { ...staleCurrent.account, margin_balance: '500', total_wallet_balance: '500', available_balance: '500' };
  state.pairs = [{ ...pair('SOLUSDC'), mode: 'LIVE' }, { ...pair('BTCUSDC'), mode: 'LIVE' }];
  state.data = staleCurrent;
  state.dataUpdatedAt = now - 20_000;
  state.stale = true;
  render(<AccountsContext.Provider value={[{ mode: 'LIVE', snapshot: freshSibling, account_observation_stale: false, generated_at_ms: now, received_at_ms: now }]}>{view()}</AccountsContext.Provider>);
  const card = screen.getByText('账户权益 / USDC').closest('.ant-card')!;
  expect(card.querySelector('.ant-statistic-content-value')).toHaveTextContent('--');
});

it('withholds shared PAPER equity during LIVE -> PAPER transition while /api/pairs still holds stale LIVE metadata', () => {
  const now = Date.now();
  const paperSelf = snapshot('SOLUSDC', 'PAPER');
  paperSelf.account_scope = 'paper-shared-v1';
  paperSelf.account = { ...paperSelf.account, margin_balance: '105', available_balance: '80', total_wallet_balance: '100', unrealized_profit: '5', update_time: new Date(now).toISOString() };
  state.pairs = [{ ...pair('SOLUSDC'), mode: 'LIVE', account_scope: 'live-scope' }, { ...pair('BTCUSDC'), mode: 'LIVE', account_scope: 'live-scope' }];
  state.data = paperSelf;
  state.dataUpdatedAt = now;
  const { rerender } = render(<AccountsContext.Provider value={[]}>{view()}</AccountsContext.Provider>);
  const card = screen.getByText('账户权益 / USDC').closest('.ant-card')!;
  expect(card.querySelector('.ant-statistic-content-value')).toHaveTextContent('--');

  const paperSibling = snapshot('BTCUSDC', 'PAPER');
  paperSibling.account_scope = 'paper-shared-v1';
  paperSibling.account = { ...paperSelf.account };
  paperSibling.ticker.mark_update_time = new Date(now).toISOString();
  state.pairs = [{ ...pair('SOLUSDC'), mode: 'PAPER', account_scope: 'paper-shared-v1' }, { ...pair('BTCUSDC'), mode: 'PAPER', account_scope: 'paper-shared-v1' }];
  rerender(<AccountsContext.Provider value={[{ mode: 'PAPER', snapshot: paperSibling, generated_at_ms: now, received_at_ms: now }]}>{view()}</AccountsContext.Provider>);
  expect(card.querySelector('.ant-statistic-content-value')).toHaveTextContent('105.00');
});

it('rejects older WS snapshots and preserves newer cached snapshots when a stale HTTP poll resolves', async () => {
  const { usePairSnapshot: realUsePairSnapshot } = await vi.importActual<typeof Queries>('../lib/queries');
  let onMessage: ((event: { data: string }) => void) | undefined;
  vi.stubGlobal('WebSocket', class {
    set onmessage(fn: ((event: { data: string }) => void) | undefined) { onMessage = fn; }
    get onmessage() { return onMessage; }
    onclose?: () => void;
    onerror?: () => void;
    close() {}
  });
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  try {
    const newer = snapshot('SOLUSDC', 'PAPER'); newer.generated_at = '2026-11-05T01:00:05Z'; newer.ticker.last_price = '105';
    const olderWs = snapshot('SOLUSDC', 'PAPER'); olderWs.generated_at = '2026-11-05T01:00:02Z'; olderWs.ticker.last_price = '102';
    const olderHttp = snapshot('SOLUSDC', 'PAPER'); olderHttp.generated_at = '2026-11-05T01:00:03Z'; olderHttp.ticker.last_price = '103';
    const freshestWs = snapshot('SOLUSDC', 'PAPER'); freshestWs.generated_at = '2026-11-05T01:00:09Z'; freshestWs.ticker.last_price = '109';
    state.api.mockResolvedValueOnce(newer).mockResolvedValueOnce(olderHttp);
    const wrapper = ({ children }: { children: React.ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
    const { result, unmount } = renderHook(() => realUsePairSnapshot(pair('SOLUSDC')), { wrapper });
    await waitFor(() => expect(result.current.data?.ticker.last_price).toBe('105'));
    act(() => onMessage?.({ data: JSON.stringify({ type: 'snapshot', data: olderWs }) }));
    expect(result.current.live).toBe(false);
    expect(result.current.data?.ticker.last_price).toBe('105');
    await act(async () => { await result.current.refetch(); });
    expect(result.current.data?.ticker.last_price).toBe('105');
    act(() => onMessage?.({ data: JSON.stringify({ type: 'snapshot', data: freshestWs }) }));
    expect(result.current.live).toBe(true);
    expect(result.current.data?.ticker.last_price).toBe('109');
    unmount();
  } finally {
    client.clear();
    vi.unstubAllGlobals();
  }
});
