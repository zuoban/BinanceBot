import { act, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { App } from 'antd';
import { MemoryRouter, useLocation } from 'react-router-dom';
import { beforeEach, expect, it, vi } from 'vitest';
import OverviewPage from '../pages/Overview';
import { runBatchControls } from '../components/Controls';
import { summarySnapshot } from './fixtures';
import type { Overview } from '../lib/types';
import { AccountsContext } from '../lib/session';
import type * as Queries from '../lib/queries';

const overview = vi.hoisted(() => ({ data: undefined as Overview | undefined, error: null as Error | null, dataUpdatedAt: Date.now(), isPending: false, refetch: vi.fn() }));
const actions = vi.hoisted(() => ({ run: vi.fn(), clearResult: vi.fn(), results: {} as Record<string, string>, remove: vi.fn(), busy: new Set<string>(), pending: {} as Record<string, string>, deleting: false }));

beforeEach(() => {
  overview.data = undefined;
  overview.isPending = false;
  overview.error = null;
  overview.dataUpdatedAt = Date.now();
  overview.refetch.mockReset();
  actions.results = {};
  actions.clearResult.mockReset();
  vi.mocked(runBatchControls).mockReset().mockResolvedValue([]);
});

vi.mock('../lib/queries', async importOriginal => {
  const actual = await importOriginal<typeof Queries>();
  return { ...actual, useOverview: () => overview, useOverviewHistory: () => ({ data: { snapshots: [], current_accounts: [] }, isPending: false, error: null }) };
});
vi.mock('../components/DeleteStrategy', () => ({ useDeleteStrategy: () => ({ remove: actions.remove, busy: actions.deleting }) }));
vi.mock('../components/Controls', () => ({
  useControls: () => ({ busy: actions.busy, pending: actions.pending, results: actions.results, clearResult: actions.clearResult, run: actions.run }),
  runBatchControls: vi.fn(),
  pendingLabel: { pause: '暂停中…', resume: '恢复中…', cancel_all: '撤单中…', rebalance: '重建中…' },
}));

it('confirms and resumes the freshly refreshed targets including newly paused live strategies', async () => {
  const old = summarySnapshot('BTCUSDC'); old.status = 'PAUSED';
  const running = summarySnapshot('ETHUSDC'); running.status = 'RUNNING';
  const newlyPaused = summarySnapshot('SOLUSDC'); newlyPaused.status = 'PAUSED';
  const anotherPaused = summarySnapshot('XRPUSDC'); anotherPaused.status = 'PAUSED';
  overview.data = { generated_at: '', pairs: [{ snapshot: old, mode: 'PAPER', scoped: true }], recent_trades: [] };
  overview.refetch.mockResolvedValue({ data: { generated_at: '', pairs: [
    { snapshot: running, mode: 'PAPER', scoped: true },
    { snapshot: newlyPaused, mode: 'LIVE', scoped: true },
    { snapshot: anotherPaused, mode: 'PAPER', scoped: true },
  ], recent_trades: [] }, error: null });
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  const user = userEvent.setup();
  await user.type(screen.getByLabelText('搜索交易对'), 'BTC');
  await user.click(screen.getByRole('button', { name: /全部策略操作/ }));
  await user.click(screen.getByText('全部恢复'));
  expect(await screen.findByText('将操作 2 个交易对，不受当前筛选影响。其中包含实盘策略，恢复后会使用真实资金下单。')).toBeInTheDocument();
  expect(runBatchControls).not.toHaveBeenCalled();
  await user.click(screen.getByRole('button', { name: '确认执行' }));
  await waitFor(() => expect(runBatchControls).toHaveBeenCalledWith([
    expect.objectContaining({ symbol: 'SOLUSDC', status: 'PAUSED', mode: 'LIVE', scoped: true }),
    expect.objectContaining({ symbol: 'XRPUSDC', status: 'PAUSED', mode: 'PAPER', scoped: true }),
  ], 'resume', actions.run));
});

it('does not resume when the refreshed confirmation is cancelled', async () => {
  const paused = summarySnapshot('SOLUSDC'); paused.status = 'PAUSED';
  overview.data = { generated_at: '', pairs: [], recent_trades: [] };
  overview.refetch.mockResolvedValue({ data: { generated_at: '', pairs: [{ snapshot: paused, mode: 'PAPER', scoped: true }], recent_trades: [] }, error: null });
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  const user = userEvent.setup();
  await user.click(screen.getByRole('button', { name: /全部策略操作/ }));
  await user.click(screen.getByText('全部恢复'));
  expect(await screen.findByText('将操作 1 个交易对，不受当前筛选影响。')).toBeInTheDocument();
  await user.click(screen.getByRole('button', { name: /取\s*消/ }));
  expect(runBatchControls).not.toHaveBeenCalled();
});

it('reports an empty refreshed resume list without showing a confirmation', async () => {
  const paused = summarySnapshot('SOLUSDC'); paused.status = 'PAUSED';
  const running = summarySnapshot('BTCUSDC'); running.status = 'RUNNING';
  overview.data = { generated_at: '', pairs: [{ snapshot: paused, mode: 'LIVE', scoped: true }], recent_trades: [] };
  overview.refetch.mockResolvedValue({ data: { generated_at: '', pairs: [{ snapshot: running, mode: 'PAPER', scoped: true }], recent_trades: [] }, error: null });
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  const user = userEvent.setup();
  await user.click(screen.getByRole('button', { name: /全部策略操作/ }));
  await user.click(screen.getByText('全部恢复'));
  expect(await screen.findByText('当前没有可恢复的策略')).toBeInTheDocument();
  expect(screen.queryByRole('button', { name: '确认执行' })).not.toBeInTheDocument();
  expect(runBatchControls).not.toHaveBeenCalled();
});

it('does not offer stale resume targets when refresh fails', async () => {
  const paused = summarySnapshot('SOLUSDC'); paused.status = 'PAUSED';
  overview.data = { generated_at: '', pairs: [{ snapshot: paused, mode: 'PAPER', scoped: true }], recent_trades: [] };
  overview.refetch.mockResolvedValue({ data: overview.data, error: new Error('连接失败') });
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  const user = userEvent.setup();
  await user.click(screen.getByRole('button', { name: /全部策略操作/ }));
  await user.click(screen.getByText('全部恢复'));
  expect(await screen.findByText('批量操作失败：连接失败')).toBeInTheDocument();
  expect(screen.queryByRole('button', { name: '确认执行' })).not.toBeInTheDocument();
  expect(runBatchControls).not.toHaveBeenCalled();
});

it('refreshes an empty stale snapshot and pauses every returned strategy', async () => {
  overview.data = { generated_at: '', pairs: [], recent_trades: [] };
  overview.dataUpdatedAt = Date.now() - 20_000;
  const paused = summarySnapshot('SOLUSDC'); paused.status = 'PAUSED';
  const running = summarySnapshot('BTCUSDC'); running.status = 'RUNNING';
  overview.refetch.mockResolvedValue({ data: { generated_at: '', pairs: [paused, running].map(s => ({ snapshot: s, mode: 'PAPER', scoped: true })), recent_trades: [] }, error: null });
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  const user = userEvent.setup();
  await user.click(screen.getByRole('button', { name: /全部策略操作/ }));
  await user.click(screen.getByText('全部暂停'));
  await user.click(await screen.findByRole('button', { name: '确认执行' }));
  await waitFor(() => expect(runBatchControls).toHaveBeenCalledWith([
    expect.objectContaining({ symbol: 'SOLUSDC' }), expect.objectContaining({ symbol: 'BTCUSDC' }),
  ], 'pause', actions.run));
});

it('reports a refresh failure without submitting stale pause targets', async () => {
  overview.data = { generated_at: '', pairs: [{ snapshot: summarySnapshot(), mode: 'PAPER', scoped: true }], recent_trades: [] };
  overview.refetch.mockResolvedValue({ data: overview.data, error: new Error('连接失败') });
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  const user = userEvent.setup();
  await user.click(screen.getByRole('button', { name: /全部策略操作/ }));
  await user.click(screen.getByText('全部暂停'));
  await user.click(await screen.findByRole('button', { name: '确认执行' }));
  expect(await screen.findByText('批量操作失败：连接失败')).toBeInTheDocument();
  expect(runBatchControls).not.toHaveBeenCalled();
});

it('lists each batch outcome per strategy and only offers a retry when nothing was submitted', async () => {
  const all = ['SOLUSDC', 'BNBUSDC', 'BTCUSDC', 'ETHUSDC', 'XRPUSDC', 'ADAUSDC'].map(symbol => { const item = summarySnapshot(symbol); item.status = 'RUNNING'; return item; });
  const s = all[0];
  overview.data = { generated_at: '', pairs: all.map(item => ({ snapshot: item, mode: 'PAPER' as const, scoped: true })), recent_trades: [] };
  overview.refetch.mockResolvedValue({ data: overview.data, error: null });
  const target = { symbol: s.symbol, status: s.status, mode: 'PAPER' as const, grid_config: s.grid_config, scoped: true };
  vi.mocked(runBatchControls).mockResolvedValue([
    { pair: target, result: 'failed' },
    { pair: { ...target, symbol: 'BNBUSDC' }, result: 'request_unconfirmed' },
    { pair: { ...target, symbol: 'BTCUSDC' }, result: 'state_unconfirmed' },
    { pair: { ...target, symbol: 'ETHUSDC' }, result: 'cancellation_unconfirmed' },
    { pair: { ...target, symbol: 'XRPUSDC' }, result: 'not_submitted' },
  ]);
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  const user = userEvent.setup();
  await user.click(screen.getByRole('button', { name: /全部策略操作/ }));
  await user.click(screen.getByText('全部暂停'));
  await user.click(await screen.findByRole('button', { name: '确认执行' }));
  const drawer = await screen.findByLabelText('批量操作结果', { selector: '[role="dialog"]' });
  expect(within(drawer).getByText('全部暂停：2/6 已暂停')).toBeInTheDocument();
  const rows = within(drawer).getAllByRole('listitem');
  expect(rows.map(row => within(row).getByRole('link').textContent)).toEqual(['SOLUSDC', 'BNBUSDC', 'XRPUSDC', 'BTCUSDC', 'ETHUSDC']);
  expect(within(rows[0]).getByText('提交前出错，未提交')).toBeInTheDocument();
  expect(within(rows[1]).getByText('请求结果不明，操作可能已生效')).toBeInTheDocument();
  expect(within(rows[3]).getByText('暂停状态及撤单结果未确认')).toBeInTheDocument();
  expect(within(rows[4]).getByText('已暂停，撤单结果未确认')).toBeInTheDocument();
  expect(within(rows[0]).getByRole('button', { name: '重试暂停' })).toBeInTheDocument();
  expect(within(rows[2]).getByRole('button', { name: '重试暂停' })).toBeInTheDocument();
  expect(within(rows[1]).queryByRole('button')).not.toBeInTheDocument();
  expect(within(rows[3]).queryByRole('button')).not.toBeInTheDocument();
  await user.click(within(rows[0]).getByRole('button', { name: '重试暂停' }));
  expect(actions.run).toHaveBeenCalledWith(expect.objectContaining({ symbol: 'SOLUSDC' }), 'pause');
});

it('sorts lightweight strategy summaries by aggregated active orders', () => {
  const first = summarySnapshot('BTCUSDC'), second = summarySnapshot('ETHUSDC');
  first.stats.active_buy_orders = 2;
  first.stats.active_sell_orders = 4;
  second.stats.active_buy_orders = 4;
  second.stats.active_sell_orders = 1;
  overview.data = { generated_at: new Date().toISOString(), pairs: [second, first].map(s => ({ snapshot: s, mode: s.mode, scoped: true })), recent_trades: [] };
  render(<MemoryRouter initialEntries={['/?sort=orders']}><App><OverviewPage /></App></MemoryRouter>);
  const symbols = [...screen.getByRole('region', { name: '交易策略' }).querySelectorAll('.pair-name')].map(link => link.textContent);
  expect(symbols).toEqual(['BTCUSDC', 'ETHUSDC']);
});

it('shows today’s executed buy and sell counts instead of active orders on the strategy card', () => {
  const s = summarySnapshot('BTCUSDC');
  s.stats.active_buy_orders = 7;
  s.stats.active_sell_orders = 9;
  overview.data = { generated_at: new Date().toISOString(), pairs: [{ snapshot: s, mode: 'PAPER', scoped: true }], recent_trades: [] };
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  const card = screen.getByRole('link', { name: 'BTCUSDC' }).closest('.strategy-card') as HTMLElement;
  expect(within(card).getByText('今日成交').parentElement).toHaveTextContent('5 笔');
  expect(within(card).getByText('买单 / 卖单').parentElement).toHaveTextContent('2 / 3');
});

it.each(['PAPER', 'LIVE'] as const)('shows shared %s asset balances once when a symbol is added', mode => {
  const s = summarySnapshot('BTCUSDC', mode);
  s.account.total_wallet_balance = '1289.95';
  s.account.unrealized_profit = '-525.02';
  s.account.margin_balance = '764.93';
  s.account.available_balance = '300';
  const entries = [{ snapshot: s, mode, scoped: true }];
  overview.data = { generated_at: new Date().toISOString(), pairs: entries, recent_trades: [] };
  const page = () => <MemoryRouter><AccountsContext.Provider value={entries}><App><OverviewPage /></App></AccountsContext.Provider></MemoryRouter>;
  const { rerender } = render(page());
  const assets = screen.getByLabelText('USDC账户资产');
  expect(within(assets).getByText('钱包余额').parentElement).toHaveTextContent('1,289.95');
  expect(within(assets).getByText('未实现盈亏').parentElement).toHaveTextContent('-525.02');
  expect(screen.getByText('保证金余额').closest('.account-card')).toHaveTextContent('764.93');
  expect(screen.getByText('可用余额').parentElement).toHaveTextContent('300 USDC');
  expect(within(assets).queryByText('可转出')).not.toBeInTheDocument();
  const second = summarySnapshot('ETHUSDC', mode);
  second.account = { ...s.account };
  s.position.size = '3';
  second.position.size = '-1';
  entries.push({ snapshot: second, mode, scoped: true });
  overview.data = { ...overview.data, pairs: [...entries] };
  rerender(page());
  expect(screen.getAllByText('保证金余额')).toHaveLength(1);
  const shared = screen.getByText('保证金余额').closest('.account-card') as HTMLElement;
  expect(shared.querySelector('.ant-statistic-content-value')).toHaveTextContent('764.93');
  expect(within(shared).getByText('钱包余额').parentElement).toHaveTextContent('1,289.95');
  expect(within(shared).getByText('未实现盈亏').parentElement).toHaveTextContent('-525.02');
  expect(within(shared).getByText('可用余额').parentElement).toHaveTextContent('300 USDC');
  expect(screen.getByText('总持仓金额 / USDC').closest('.position-summary')).toHaveTextContent('404');
});

it('shows grouped today pnl and pending trades across all strategies', () => {
  const a = summarySnapshot('BTCUSDC');
  const now = new Date().toISOString();
  overview.data = { generated_at: now, pairs: [{ snapshot: a, mode: 'PAPER', scoped: true }], recent_trades: [], today_pnl: { window_start: now, window_end: now, groups: [
    { symbol: 'BTCUSDC', mode: 'PAPER', realized_pnl: '8', pending_pnl_count: 1 },
    { symbol: 'DELETEDUSDT', mode: 'LIVE', realized_pnl: '-3', pending_pnl_count: 0 },
  ] } };
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  const summary = screen.getByLabelText('策略运行概览');
  const section = within(summary).getByLabelText('今日已实现盈亏');
  expect(within(section).getByText('--')).toBeInTheDocument();
  expect(within(section).getByText('盈亏待同步')).toBeInTheDocument();
  expect(within(section).getByText('-3')).toBeInTheDocument();
  expect(within(section).getByText('USDT')).toBeInTheDocument();
});

it('shows unavailable trade counts when a strategy has no current-day statistics', () => {
  const s = summarySnapshot('BTCUSDC');
  s.today_trade_counts = null;
  overview.data = { generated_at: new Date().toISOString(), pairs: [{ snapshot: s, mode: 'PAPER', scoped: true }], recent_trades: [] };
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  const card = screen.getByRole('link', { name: 'BTCUSDC' }).closest('.strategy-card') as HTMLElement;
  expect(within(card).getByText('今日成交').parentElement).toHaveTextContent('-- 笔');
  expect(within(card).getByText('买单 / 卖单').parentElement).toHaveTextContent('-- / --');
  expect(within(screen.getByLabelText('策略运行概览')).getByText('买单 -- 笔 / 卖单 -- 笔')).toBeInTheDocument();
});

it('shows five newest trades and expands to twenty across strategies', async () => {
  const btc = summarySnapshot('BTCUSDC'), eth = summarySnapshot('ETHUSDC');
  const trade = (symbol: string, index: number) => ({
    trade_id: `${symbol}-${index}`, symbol, mode: symbol === 'BTCUSDC' ? 'PAPER' as const : 'LIVE' as const,
    side: 'BUY' as const, price: '123.456789', quantity: '1', amount_usdc: '100', realized_pnl: index === 20 ? '-0.004' : '0', commission: '0',
    pnl_verified: true, is_maker: true, timestamp: new Date(Date.UTC(2026, 0, 1, 0, index)).toISOString(), note: `成交 ${index}`,
  });
  const recentTrades = Array.from({ length: 22 }, (_, i) => i % 2 === 0 ? trade('BTCUSDC', i) : { ...trade('ETHUSDC', i), side: 'SELL' as const, pnl_verified: i !== 21 }).reverse().slice(0, 20);
  overview.data = { generated_at: new Date().toISOString(), pairs: [{ snapshot: btc, mode: 'PAPER', scoped: true }, { snapshot: eth, mode: 'LIVE', scoped: true }], recent_trades: recentTrades };
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  const table = screen.getByText('最近成交').closest('.ant-card')!;
  const card = within(table as HTMLElement);
  expect(card.getAllByRole('row').slice(1)).toHaveLength(5);
  const summary = within(card.getByLabelText('当前显示成交汇总'));
  expect(summary.getByText(/当前显示/)).toHaveTextContent('当前显示 5 笔');
  expect(summary.getByText(/买入/)).toHaveTextContent('买入 2 笔');
  expect(summary.getByText(/卖出/)).toHaveTextContent('卖出 3 笔');
  expect(summary.getByText(/合计盈亏/)).toHaveTextContent('模拟盘 -0.004 USDC');
  expect(summary.getByText(/合计盈亏/)).toHaveTextContent('实盘 待核实 USDC');

  expect(card.getByRole('columnheader', { name: '方向' })).not.toHaveClass('ant-table-cell-fix-left');
  expect(card.getAllByRole('row')[1]).toHaveTextContent('ETHUSDC');
  expect(card.getAllByRole('row')[1]).toHaveTextContent('成交 21');
  const mobileTrades = table.querySelector('.overview-trades-mobile') as HTMLElement;
  const firstTrade = within(mobileTrades).getAllByRole('listitem')[0];
  expect(within(firstTrade).getByText('成交均价').nextElementSibling).toHaveTextContent('123.4568');
  expect(within(firstTrade).queryByText('模式')).not.toBeInTheDocument();
  expect(within(firstTrade).queryByText('数量')).not.toBeInTheDocument();
  expect(within(firstTrade).queryByText('手续费')).not.toBeInTheDocument();
  expect(within(firstTrade).queryByText('成交方式')).not.toBeInTheDocument();
  expect(within(firstTrade).queryByText('成交时间（北京）')).not.toBeInTheDocument();
  expect(within(firstTrade).queryByText('备注')).not.toBeInTheDocument();
  expect(firstTrade.querySelector('.trade-list-time')).toHaveTextContent(/前|后/);
  expect(card.getAllByRole('row')[1]).not.toHaveTextContent('Maker');
  const user = userEvent.setup();
  await user.click(card.getByRole('button', { name: '展开最近 20 条' }));
  expect(card.getAllByRole('row').slice(1)).toHaveLength(20);
  expect(summary.getByText(/当前显示/)).toHaveTextContent('当前显示 20 笔');
  expect(summary.getByText(/买入/)).toHaveTextContent('买入 10 笔');
  expect(summary.getByText(/卖出/)).toHaveTextContent('卖出 10 笔');
  expect(card.getAllByRole('row')[20]).toHaveTextContent('成交 2');
  expect(card.queryByText('成交 1')).not.toBeInTheDocument();
  await user.click(card.getByRole('button', { name: '收起' }));
  expect(card.getAllByRole('row').slice(1)).toHaveLength(5);
});

it('offers creation when there are no strategies', () => {
  overview.data = { generated_at: new Date().toISOString(), pairs: [], recent_trades: [] };
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  expect(screen.getByText('暂无策略数据')).toBeInTheDocument();
  expect(screen.getAllByRole('link', { name: /新增交易对/ })).toHaveLength(2);
});

it('shows position amounts and their long/short shares for all strategies', async () => {
  const long = summarySnapshot('BTCUSDC'), short = summarySnapshot('ETHUSDC');
  long.position.size = '3';
  short.position.size = '-1';
  overview.data = { generated_at: new Date().toISOString(), pairs: [long, short].map(s => ({ snapshot: s, mode: 'PAPER' as const, scoped: true })), recent_trades: [] };
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  expect(screen.getByText('总持仓金额 / USDC').closest('.position-summary')).toHaveTextContent('404');
  expect(screen.getByText('做多持仓金额').parentElement).toHaveTextContent('303');
  expect(screen.getByText('做空持仓金额').parentElement).toHaveTextContent('101');
  expect(screen.getByText('多空比（金额占比）').parentElement).toHaveTextContent('75% / 25%');
  expect(screen.getByText('覆盖策略').parentElement).toHaveTextContent('2 个');
  await userEvent.setup().type(screen.getByLabelText('搜索交易对'), 'BTC');
  expect(screen.getByText('总持仓金额 / USDC').closest('.position-summary')).toHaveTextContent('404');
});

it('shows available balance as a percentage of wallet balance', () => {
  const s = summarySnapshot('BTCUSDC');
  const pairs = [{ snapshot: s, mode: 'PAPER' as const, scoped: true }];
  overview.data = { generated_at: new Date().toISOString(), pairs, recent_trades: [] };
  render(<MemoryRouter><App><AccountsContext.Provider value={pairs}><OverviewPage /></AccountsContext.Provider></App></MemoryRouter>);
  expect(screen.getByText('可用比例（按钱包余额）').parentElement).toHaveTextContent('80%');
});

it('does not calculate an available ratio for zero wallet balance', () => {
  const s = summarySnapshot('BTCUSDC');
  s.account.total_wallet_balance = '0';
  s.position.unrealized_pnl = '0';
  const pairs = [{ snapshot: s, mode: 'PAPER' as const, scoped: true }];
  overview.data = { generated_at: new Date().toISOString(), pairs, recent_trades: [] };
  render(<MemoryRouter><App><AccountsContext.Provider value={pairs}><OverviewPage /></AccountsContext.Provider></App></MemoryRouter>);
  expect(screen.getByText('可用比例（按钱包余额）').parentElement).toHaveTextContent('--');
});

it('marks an unchanged overview stale as time passes and clears the warning on refresh', () => {
  vi.useFakeTimers();
  try {
    overview.dataUpdatedAt = Date.now();
    overview.data = { generated_at: '', pairs: [{ snapshot: summarySnapshot('BTCUSDC'), mode: 'PAPER', scoped: true }], recent_trades: [] };
    const page = <MemoryRouter><App><OverviewPage /></App></MemoryRouter>;
    const { rerender, unmount } = render(page);
    expect(screen.queryByText('数据暂未更新，请检查连接')).not.toBeInTheDocument();
    act(() => { vi.advanceTimersByTime(10_000); });
    expect(screen.queryByText('数据暂未更新，请检查连接')).not.toBeInTheDocument();
    act(() => { vi.advanceTimersByTime(1_000); });
    expect(screen.getByText('数据暂未更新，请检查连接')).toBeInTheDocument();
    overview.dataUpdatedAt = Date.now();
    rerender(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
    expect(screen.queryByText('数据暂未更新，请检查连接')).not.toBeInTheDocument();
    unmount();
  } finally {
    vi.useRealTimers();
  }
});

it('does not mark the overview banner stale while initial data is still loading', () => {
  overview.data = undefined;
  overview.isPending = true;
  overview.error = null;
  overview.dataUpdatedAt = 0;
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  expect(screen.getByLabelText('策略运行概览')).not.toHaveClass('is-stale');
  expect(screen.queryByText('数据暂未更新，请检查连接')).not.toBeInTheDocument();
});

it('disables rebuilding a paused strategy while keeping resume available', async () => {
  const paused = summarySnapshot('BTCUSDC'); paused.status = 'PAUSED';
  overview.data = { generated_at: '', pairs: [{ snapshot: paused, mode: 'PAPER', scoped: true }], recent_trades: [] };
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  await userEvent.setup().click(screen.getByRole('button', { name: 'BTCUSDC 更多操作' }));
  expect(screen.getByRole('menuitem', { name: /重建 BTCUSDC 网格/ })).toHaveAttribute('aria-disabled', 'true');
  expect(screen.getByRole('button', { name: '恢复 BTCUSDC' })).toBeEnabled();
});

it('renders strategy operation results with signal tier styling and allows dismissing settled results', async () => {
  const s = summarySnapshot('BTCUSDC');
  actions.results = { BTCUSDC: '已暂停，撤单结果尚未确认，请检查交易所挂单及日志' };
  overview.data = { generated_at: '', pairs: [{ snapshot: s, mode: 'PAPER', scoped: true }], recent_trades: [] };
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  const banner = screen.getByRole('status');
  expect(banner).toHaveClass('operation-result', 'signal-yellow');
  expect(banner).toHaveTextContent('已暂停，撤单结果尚未确认，请检查交易所挂单及日志');
  await userEvent.setup().click(screen.getByRole('button', { name: '关闭 BTCUSDC 操作提示' }));
  expect(actions.clearResult).toHaveBeenCalledWith('BTCUSDC');
});

it('links recent trade symbols to their strategy detail pages', async () => {
  const s = summarySnapshot('BTCUSDC');
  overview.data = {
    generated_at: '', pairs: [{ snapshot: s, mode: 'PAPER', scoped: true }],
    recent_trades: [{ trade_id: 't1', symbol: 'BTCUSDC', mode: 'PAPER', side: 'BUY', price: '100', quantity: '1', amount_usdc: '100', realized_pnl: '0', commission: '0', pnl_verified: true, is_maker: true, timestamp: new Date().toISOString(), note: '' }],
  };
  function Location() { return <output data-testid="location">{useLocation().pathname}</output>; }
  render(<MemoryRouter><App><OverviewPage /><Location /></App></MemoryRouter>);
  const table = screen.getByText('最近成交').closest('.ant-card') as HTMLElement;
  const links = within(table).getAllByRole('link', { name: 'BTCUSDC' });
  expect(links[0]).toHaveAttribute('href', '/pairs/BTCUSDC');
  await userEvent.setup().click(links[0]);
  expect(screen.getByTestId('location')).toHaveTextContent('/pairs/BTCUSDC');
});
