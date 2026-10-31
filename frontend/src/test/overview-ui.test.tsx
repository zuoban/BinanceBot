import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { App } from 'antd';
import { MemoryRouter, useLocation } from 'react-router-dom';
import { beforeEach, expect, it, vi } from 'vitest';
import OverviewPage from '../pages/Overview';
import { snapshot } from './fixtures';
import type { Overview } from '../lib/types';
import { AccountsContext } from '../lib/session';

const overview = vi.hoisted(() => ({ data: undefined as Overview | undefined, error: null, dataUpdatedAt: Date.now(), isPending: false, refetch: vi.fn() }));
beforeEach(() => {
  vi.stubGlobal('matchMedia', vi.fn().mockImplementation(query => ({ matches: false, media: query, addListener: vi.fn(), removeListener: vi.fn(), addEventListener: vi.fn(), removeEventListener: vi.fn() })));
  vi.stubGlobal('ResizeObserver', class { observe() {} unobserve() {} disconnect() {} });
});
vi.mock('../lib/queries', () => ({ useOverview: () => overview, useOverviewHistory: () => ({ data: [], isPending: false, error: null }) }));
const actions = vi.hoisted(() => ({ run: vi.fn(), remove: vi.fn(), busy: new Set<string>(), deleting: false }));
vi.mock('../components/DeleteStrategy', () => ({ useDeleteStrategy: () => ({ remove: actions.remove, busy: actions.deleting }) }));
vi.mock('../components/Controls', () => ({ useControls: () => ({ busy: actions.busy, results: {}, run: actions.run }), runBatchControls: vi.fn() }));

it('opens the selected strategy configuration from its card', async () => {
  const s = snapshot('BTCUSDC');
  s.status = 'RUNNING';
  overview.dataUpdatedAt = Date.now();
  s.grid_config.max_position_usdc = '2000';
  s.grid_config.pause_on_max_position = true;
  overview.data = { generated_at: new Date().toISOString(), pairs: [{ snapshot: s, mode: 'PAPER', scoped: true }] };
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  await userEvent.setup().click(screen.getByRole('button', { name: '查看 BTCUSDC 策略配置' }));
  expect(await screen.findByText('BTCUSDC 策略配置')).toBeInTheDocument();
  expect(screen.getByText('2000 USDC')).toBeInTheDocument();
  expect(screen.getByText('暂停策略')).toBeInTheDocument();
  expect(screen.getByRole('link', { name: /查看完整配置/ })).toHaveAttribute('href', '/pairs/BTCUSDC/config');
  await userEvent.setup().click(screen.getByRole('button', { name: 'BTCUSDC 更多操作' }));
  expect(screen.getByRole('menuitem', { name: '查看策略' })).toBeInTheDocument();
  await userEvent.setup().click(screen.getByRole('menuitem', { name: '暂停策略' }));
  expect(actions.run).toHaveBeenCalledWith(expect.objectContaining({ symbol: 'BTCUSDC' }), 'pause');
  await userEvent.setup().click(screen.getByRole('button', { name: 'BTCUSDC 更多操作' }));
  await userEvent.setup().click(screen.getByRole('menuitem', { name: '删除策略' }));
  expect(actions.remove).toHaveBeenCalledWith(expect.objectContaining({ symbol: 'BTCUSDC' }));
});

it('navigates when the strategy menu item is activated', async () => {
  const s = snapshot('BTCUSDC');
  overview.data = { generated_at: new Date().toISOString(), pairs: [{ snapshot: s, mode: 'PAPER', scoped: true }] };
  function Location() { return <output data-testid="location">{useLocation().pathname}</output>; }
  render(<MemoryRouter><App><OverviewPage /><Location /></App></MemoryRouter>);
  const user = userEvent.setup();
  await user.click(screen.getByRole('button', { name: 'BTCUSDC 更多操作' }));
  await user.click(screen.getByRole('menuitem', { name: '查看策略' }));
  expect(screen.getByTestId('location')).toHaveTextContent('/pairs/BTCUSDC');
  await user.click(screen.getByRole('button', { name: 'BTCUSDC 更多操作' }));
  await user.click(screen.getByRole('menuitem', { name: '编辑策略' }));
  expect(screen.getByTestId('location')).toHaveTextContent('/pairs/BTCUSDC/config');
});

it('identifies each strategy direction without opening its configuration', () => {
  const long = snapshot('BTCUSDC'), short = snapshot('ETHUSDC');
  long.grid_config.direction = 'LONG';
  short.grid_config.direction = 'SHORT';
  overview.data = { generated_at: new Date().toISOString(), pairs: [long, short].map(s => ({ snapshot: s, mode: 'PAPER' as const, scoped: true })) };
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  expect(screen.getByRole('img', { name: '策略方向：做多' }).querySelector('.anticon-arrow-up')).toBeInTheDocument();
  expect(screen.getByRole('img', { name: '策略方向：做空' }).querySelector('.anticon-arrow-down')).toBeInTheDocument();
  expect(screen.getByRole('img', { name: '策略方向：做多' })).toHaveTextContent('做多');
  expect(screen.getByRole('img', { name: '策略方向：做空' })).toHaveTextContent('做空');
});

it('summarizes statuses and lets the user clear a search with no results', async () => {
  const running = snapshot('BTCUSDC'); running.status = 'RUNNING';
  const error = snapshot('SOLUSDC'); error.status = 'ERROR';
  overview.data = { generated_at: new Date().toISOString(), pairs: [running, error].map(s => ({ snapshot: s, mode: 'PAPER' as const, scoped: true })) };
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  const summary = screen.getByLabelText('策略运行概览');
  expect(within(summary).getByText('运行中').parentElement).toHaveTextContent('1');
  expect(within(summary).getByText('策略总数').parentElement).toHaveTextContent('2');
  expect(within(summary).getByText('异常 1')).toBeInTheDocument();
  expect(within(summary).getByText('今日成交').parentElement).toHaveTextContent('10 笔');
  expect(within(summary).getByText('买单 4 笔 / 卖单 6 笔')).toBeInTheDocument();
  const user = userEvent.setup();
  await user.type(screen.getByLabelText('搜索交易对'), 'NOTFOUND');
  expect(screen.getByText('没有符合筛选条件的策略')).toBeInTheDocument();
  await user.click(screen.getAllByRole('button', { name: '清除筛选' })[0]);
  expect(screen.getByText('BTCUSDC')).toBeInTheDocument();
});

it('shows exchange asset balances and available balance', () => {
  const s = snapshot('BTCUSDC', 'LIVE');
  s.account.total_wallet_balance = '1289.95';
  s.account.unrealized_profit = '-525.02';
  s.account.margin_balance = '764.93';
  s.account.available_balance = '300';
  overview.data = { generated_at: new Date().toISOString(), pairs: [{ snapshot: s, mode: 'LIVE', scoped: true }] };
  render(<MemoryRouter><AccountsContext.Provider value={overview.data.pairs}><App><OverviewPage /></App></AccountsContext.Provider></MemoryRouter>);
  const assets = screen.getByLabelText('USDC账户资产');
  expect(within(assets).getByText('钱包余额').parentElement).toHaveTextContent('1,289.95');
  expect(within(assets).getByText('未实现盈亏').parentElement).toHaveTextContent('-525.02');
  expect(screen.getByText('保证金余额').closest('.account-card')).toHaveTextContent('764.93');
  expect(screen.getByText('可用余额').parentElement).toHaveTextContent('300 USDC');
  expect(within(assets).queryByText('可转出')).not.toBeInTheDocument();
});

it('shows grouped today pnl and pending trades across all strategies', () => {
  const a = snapshot('BTCUSDC');
  const now = new Date().toISOString();
  overview.data = { generated_at: now, pairs: [{ snapshot: a, mode: 'PAPER', scoped: true }], today_pnl: { window_start: now, window_end: now, groups: [
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
  const s = snapshot('BTCUSDC');
  s.hourly_trade_stats = null;
  overview.data = { generated_at: new Date().toISOString(), pairs: [{ snapshot: s, mode: 'PAPER', scoped: true }] };
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  expect(within(screen.getByLabelText('策略运行概览')).getByText('买单 -- 笔 / 卖单 -- 笔')).toBeInTheDocument();
});

it('shows five newest trades and expands to twenty across strategies', async () => {
  const btc = snapshot('BTCUSDC'), eth = snapshot('ETHUSDC');
  const trade = (symbol: string, index: number) => ({
    trade_id: `${symbol}-${index}`, symbol, mode: symbol === 'BTCUSDC' ? 'PAPER' as const : 'LIVE' as const,
    side: 'BUY' as const, price: '123.456789', quantity: '1', amount_usdc: '100', realized_pnl: '0', commission: '0',
    pnl_verified: true, is_maker: true, timestamp: new Date(Date.UTC(2026, 0, 1, 0, index)).toISOString(), note: `成交 ${index}`,
  });
  btc.recent_trades = Array.from({ length: 11 }, (_, i) => trade('BTCUSDC', i * 2));
  eth.recent_trades = Array.from({ length: 11 }, (_, i) => trade('ETHUSDC', i * 2 + 1));
  overview.data = { generated_at: new Date().toISOString(), pairs: [{ snapshot: btc, mode: 'PAPER', scoped: true }, { snapshot: eth, mode: 'LIVE', scoped: true }] };
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  const table = screen.getByText('最近成交').closest('.ant-card')!;
  const card = within(table as HTMLElement);
  expect(card.getAllByRole('row').slice(1)).toHaveLength(5);
  expect(card.getByRole('columnheader', { name: '方向' })).not.toHaveClass('ant-table-cell-fix-left');
  expect(card.getAllByRole('row')[1]).toHaveTextContent('ETHUSDC');
  expect(card.getAllByRole('row')[1]).toHaveTextContent('成交 21');
  const mobileTrades = table.querySelector('.overview-trades-mobile') as HTMLElement;
  const firstTrade = within(mobileTrades).getAllByRole('listitem')[0];
  expect(within(firstTrade).getByText('成交均价').nextElementSibling).toHaveTextContent('123.4568');
  const user = userEvent.setup();
  await user.click(card.getByRole('button', { name: '展开最近 20 条' }));
  expect(card.getAllByRole('row').slice(1)).toHaveLength(20);
  expect(card.getAllByRole('row')[20]).toHaveTextContent('成交 2');
  expect(card.queryByText('成交 1')).not.toBeInTheDocument();
  await user.click(card.getByRole('button', { name: '收起' }));
  expect(card.getAllByRole('row').slice(1)).toHaveLength(5);
});

it('offers creation when there are no strategies', () => {
  overview.data = { generated_at: new Date().toISOString(), pairs: [] };
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  expect(screen.getByText('暂无策略数据')).toBeInTheDocument();
  expect(screen.getAllByRole('link', { name: /新增交易对/ })).toHaveLength(2);
});

it('shows position amounts and their long/short shares for all strategies', async () => {
  const long = snapshot('BTCUSDC'), short = snapshot('ETHUSDC');
  long.position.size = '3';
  short.position.size = '-1';
  overview.data = { generated_at: new Date().toISOString(), pairs: [long, short].map(s => ({ snapshot: s, mode: 'PAPER' as const, scoped: true })) };
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  expect(screen.getByText('总持仓金额 / USDC').parentElement).toHaveTextContent('404');
  expect(screen.getByText('做多持仓金额').parentElement).toHaveTextContent('303');
  expect(screen.getByText('做空持仓金额').parentElement).toHaveTextContent('101');
  expect(screen.getByText('多空比（金额占比）').parentElement).toHaveTextContent('75% / 25%');
  await userEvent.setup().type(screen.getByLabelText('搜索交易对'), 'BTC');
  expect(screen.getByText('总持仓金额 / USDC').parentElement).toHaveTextContent('404');
});

it('shows available balance as a percentage of wallet balance', () => {
  const s = snapshot('BTCUSDC');
  const pairs = [{ snapshot: s, mode: 'PAPER' as const, scoped: true }];
  overview.data = { generated_at: new Date().toISOString(), pairs };
  render(<MemoryRouter><App><AccountsContext.Provider value={pairs}><OverviewPage /></AccountsContext.Provider></App></MemoryRouter>);
  expect(screen.getByText('可用比例').parentElement).toHaveTextContent('80%');
});

it('does not calculate an available ratio for zero wallet balance', () => {
  const s = snapshot('BTCUSDC');
  s.account.total_wallet_balance = '0';
  s.position.unrealized_pnl = '0';
  const pairs = [{ snapshot: s, mode: 'PAPER' as const, scoped: true }];
  overview.data = { generated_at: new Date().toISOString(), pairs };
  render(<MemoryRouter><App><AccountsContext.Provider value={pairs}><OverviewPage /></AccountsContext.Provider></App></MemoryRouter>);
  expect(screen.getByText('可用比例').parentElement).toHaveTextContent('--');
});
