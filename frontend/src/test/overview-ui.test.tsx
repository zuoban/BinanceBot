import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { App } from 'antd';
import { MemoryRouter, useLocation } from 'react-router-dom';
import { beforeEach, expect, it, vi } from 'vitest';
import OverviewPage from '../pages/Overview';
import { summarySnapshot } from './fixtures';
import type { Overview } from '../lib/types';
import { AccountsContext } from '../lib/session';
import { entryChange } from '../lib/portfolio';
import type * as Portfolio from '../lib/portfolio';
import type * as Queries from '../lib/queries';

vi.mock('../lib/portfolio', async importOriginal => {
  const actual = await importOriginal<typeof Portfolio>();
  return { ...actual, entryChange: vi.fn(actual.entryChange) };
});

const overview = vi.hoisted(() => ({ data: undefined as Overview | undefined, error: null as Error | null, dataUpdatedAt: Date.now(), isPending: false, refetch: vi.fn() }));
beforeEach(() => {
  overview.data = undefined;
  overview.isPending = false;
  overview.error = null;
  overview.dataUpdatedAt = Date.now();
  overview.refetch.mockReset();
  actions.results = {};
  actions.clearResult.mockReset();
});
vi.mock('../lib/queries', async importOriginal => { const actual = await importOriginal<typeof Queries>(); return { ...actual, useOverview: () => overview, useOverviewHistory: () => ({ data: { snapshots: [], current_accounts: [] }, isPending: false, error: null }) }; });
const actions = vi.hoisted(() => ({ run: vi.fn(), clearResult: vi.fn(), results: {} as Record<string, string>, remove: vi.fn(), busy: new Set<string>(), pending: {} as Record<string, string>, deleting: false }));
vi.mock('../components/DeleteStrategy', () => ({ useDeleteStrategy: () => ({ remove: actions.remove, busy: actions.deleting }) }));
vi.mock('../components/Controls', () => ({ useControls: () => ({ busy: actions.busy, pending: actions.pending, results: actions.results, clearResult: actions.clearResult, run: actions.run }), runBatchControls: vi.fn(), pendingLabel: { pause: '暂停中…', resume: '恢复中…', cancel_all: '撤单中…', rebalance: '重建中…' } }));
it('orders running strategies first, then by absolute position amount, and updates after a status change', () => {
  const strategies = [
    ['BTCUSDC', 'PAUSED', '100'],
    ['BNBUSDC', 'PAUSED', '1'],
    ['SOLUSDC', 'RUNNING', '2'],
    ['ZECUSDC', 'RUNNING', '-3'],
    ['ADAUSDC', 'RUNNING', '2'],
    ['ETHUSDC', 'RUNNING', '0'],
    ['XRPUSDC', 'RUNNING', 'invalid'],
  ] as const;
  const pairs = strategies.map(([symbol, status, size]) => {
    const s = summarySnapshot(symbol);
    s.status = status;
    s.position.size = size;
    return { snapshot: s, mode: s.mode, scoped: true };
  });
  overview.data = { generated_at: new Date().toISOString(), pairs, recent_trades: [] };
  const page = <MemoryRouter><App><OverviewPage /></App></MemoryRouter>;
  const { rerender } = render(page);
  const symbols = () => [...screen.getByRole('region', { name: '交易策略' }).querySelectorAll('.pair-name')].map(link => link.textContent);
  expect(symbols()).toEqual(['ZECUSDC', 'ADAUSDC', 'SOLUSDC', 'ETHUSDC', 'XRPUSDC', 'BTCUSDC', 'BNBUSDC']);
  pairs[0].snapshot.status = 'RUNNING';
  overview.data = { ...overview.data, pairs: [...pairs] };
  rerender(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  expect(symbols()).toEqual(['BTCUSDC', 'ZECUSDC', 'ADAUSDC', 'SOLUSDC', 'ETHUSDC', 'XRPUSDC', 'BNBUSDC']);
});

it('keeps unchanged strategy cards memoized when another snapshot or control result changes', () => {
  const first = { snapshot: summarySnapshot('BTCUSDC'), mode: 'PAPER' as const, scoped: true };
  const second = { snapshot: summarySnapshot('ETHUSDC'), mode: 'PAPER' as const, scoped: true };
  overview.data = { generated_at: new Date().toISOString(), pairs: [first, second], recent_trades: [] };
  const { rerender } = render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  vi.mocked(entryChange).mockClear();
  overview.data = { ...overview.data, pairs: [{ ...first, snapshot: { ...first.snapshot, ticker: { ...first.snapshot.ticker, mark_price: '102' } } }, second] };
  rerender(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  expect(entryChange).toHaveBeenCalledTimes(1);
  expect(entryChange).toHaveBeenCalledWith(first.snapshot.position, '102');
  vi.mocked(entryChange).mockClear();
  actions.results = { BTCUSDC: '已确认' };
  rerender(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  expect(entryChange).toHaveBeenCalledTimes(1);
  expect(screen.getByText('已确认')).toBeInTheDocument();
  expect(entryChange).not.toHaveBeenCalledWith(second.snapshot.position, second.snapshot.ticker.mark_price);
});

it('places the add link in the strategy section and navigates to it', async () => {
  overview.data = { generated_at: new Date().toISOString(), pairs: [{ snapshot: summarySnapshot('BTCUSDC'), mode: 'PAPER', scoped: true }], recent_trades: [] };
  overview.isPending = false;
  overview.error = null;
  function Location() { return <output data-testid="location">{useLocation().pathname}</output>; }
  render(<MemoryRouter><App><OverviewPage /><Location /></App></MemoryRouter>);
  const section = screen.getByRole('region', { name: '交易策略' });
  const link = within(section).getByRole('link', { name: /新增交易对/ });
  expect(link).toHaveAttribute('href', '/pairs/new');
  expect(screen.getAllByRole('link', { name: /新增交易对/ })).toHaveLength(1);
  await userEvent.setup().click(link);
  expect(screen.getByTestId('location')).toHaveTextContent('/pairs/new');
});
it('transfers focus to the strategy add link when loading completes', async () => {
  overview.data = undefined;
  overview.isPending = true;
  overview.error = null;
  const { rerender } = render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  screen.getByRole('link', { name: /新增交易对/ }).focus();
  overview.data = { generated_at: new Date().toISOString(), pairs: [{ snapshot: summarySnapshot('BTCUSDC'), mode: 'PAPER', scoped: true }], recent_trades: [] };
  overview.isPending = false;
  rerender(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  const section = screen.getByRole('region', { name: '交易策略' });
  expect(within(section).getByRole('link', { name: /新增交易对/ })).toHaveFocus();
  await userEvent.setup().tab();
  expect(screen.getByRole('textbox', { name: '搜索交易对' })).toHaveFocus();
});

it('does not steal focus when loading completes', () => {
  overview.data = undefined;
  overview.isPending = true;
  overview.error = null;
  const { rerender } = render(<MemoryRouter><App><OverviewPage /><button>其他操作</button></App></MemoryRouter>);
  const other = screen.getByRole('button', { name: '其他操作' });
  other.focus();
  overview.data = { generated_at: new Date().toISOString(), pairs: [{ snapshot: summarySnapshot('BTCUSDC'), mode: 'PAPER', scoped: true }], recent_trades: [] };
  overview.isPending = false;
  rerender(<MemoryRouter><App><OverviewPage /><button>其他操作</button></App></MemoryRouter>);
  expect(other).toHaveFocus();
});


it.each(['loading', 'error'])('keeps the add link available during initial %s', state => {
  overview.data = undefined;
  overview.isPending = state === 'loading';
  overview.error = state === 'error' ? new Error('请求失败') : null;
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  expect(screen.getByRole('link', { name: /新增交易对/ })).toHaveAttribute('href', '/pairs/new');
  expect(screen.getAllByRole('link', { name: /新增交易对/ })).toHaveLength(1);
});

it('places the add link in the strategy skeleton while loading', () => {
  overview.isPending = true;
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  const loading = screen.getByRole('status', { name: '正在同步策略数据' });
  expect(within(loading).getByRole('link', { name: /新增交易对/ })).toBeInTheDocument();
  expect(screen.getByRole('heading', { name: '策略运行概览' }).parentElement).not.toContainElement(screen.getByRole('link', { name: /新增交易对/ }));
});

it('keeps both add links available when there are no strategies', () => {
  overview.data = { generated_at: new Date().toISOString(), pairs: [], recent_trades: [] };
  overview.isPending = false;
  overview.error = null;
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  const section = screen.getByRole('region', { name: '交易策略' });
  expect(within(section).getAllByRole('link', { name: /新增交易对/ })).toHaveLength(2);
  expect(within(section).getByRole('button', { name: /全部策略操作/ })).toBeEnabled();
  for (const link of within(section).getAllByRole('link', { name: /新增交易对/ })) expect(link).toHaveAttribute('href', '/pairs/new');
});

it('keeps the strategy add link after a refresh error', () => {
  overview.data = { generated_at: new Date().toISOString(), pairs: [{ snapshot: summarySnapshot('BTCUSDC'), mode: 'PAPER', scoped: true }], recent_trades: [] };
  overview.isPending = false;
  overview.error = new Error('刷新失败');
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  expect(within(screen.getByRole('region', { name: '交易策略' })).getByRole('link', { name: /新增交易对/ })).toHaveAttribute('href', '/pairs/new');
  expect(screen.getAllByRole('link', { name: /新增交易对/ })).toHaveLength(1);
});

it('opens the selected strategy configuration from its card', async () => {
  const s = summarySnapshot('BTCUSDC');
  s.status = 'RUNNING';
  overview.dataUpdatedAt = Date.now();
  s.grid_config.max_position_usdc = '2000';
  s.grid_config.pause_on_max_position = true;
  overview.data = { generated_at: new Date().toISOString(), pairs: [{ snapshot: s, mode: 'PAPER', scoped: true }], recent_trades: [] };
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  await userEvent.setup().click(screen.getByRole('button', { name: '查看 BTCUSDC 策略配置' }));
  expect(await screen.findByText('BTCUSDC 策略配置')).toBeInTheDocument();
  expect(screen.getByText('2000 USDC')).toBeInTheDocument();
  expect(screen.getByText('暂停策略')).toBeInTheDocument();
  expect(screen.getByRole('link', { name: /查看完整配置/ })).toHaveAttribute('href', '/pairs/BTCUSDC/config');
  await userEvent.setup().click(screen.getByRole('button', { name: '暂停 BTCUSDC' }));
  expect(actions.run).toHaveBeenCalledWith(expect.objectContaining({ symbol: 'BTCUSDC' }), 'pause');
  await userEvent.setup().click(screen.getByRole('button', { name: 'BTCUSDC 更多操作' }));
  expect(screen.getByRole('menuitem', { name: '查看策略' })).toBeInTheDocument();
  expect(screen.queryByRole('menuitem', { name: '删除策略' })).not.toBeInTheDocument();
  expect(screen.queryByRole('menuitem', { name: /暂停策略|恢复策略/ })).not.toBeInTheDocument();
});

it('navigates when the strategy menu item is activated', async () => {
  const s = summarySnapshot('BTCUSDC');
  overview.data = { generated_at: new Date().toISOString(), pairs: [{ snapshot: s, mode: 'PAPER', scoped: true }], recent_trades: [] };
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
  const long = summarySnapshot('BTCUSDC'), short = summarySnapshot('ETHUSDC');
  long.grid_config.direction = 'LONG';
  short.grid_config.direction = 'SHORT';
  overview.data = { generated_at: new Date().toISOString(), pairs: [long, short].map(s => ({ snapshot: s, mode: 'PAPER' as const, scoped: true })), recent_trades: [] };
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  expect(screen.getByRole('img', { name: '策略方向：做多' }).querySelector('.anticon-arrow-up')).toBeInTheDocument();
  expect(screen.getByRole('img', { name: '策略方向：做空' }).querySelector('.anticon-arrow-down')).toBeInTheDocument();
  expect(screen.getByRole('img', { name: '策略方向：做多' })).toHaveTextContent('做多');
  expect(screen.getByRole('img', { name: '策略方向：做空' })).toHaveTextContent('做空');
});
it.each([
  ['long rising', '1', '100', '101', '100', '+1%', 'positive'],
  ['short falling', '-1', '100', '99', '100', '-1%', 'negative'],
  ['short rising', '-1', '100', '101', '100', '+1%', 'positive'],
  ['long falling', '1', '100', '99', '100', '-1%', 'negative'],
  ['unchanged', '1', '100', '100', '100', '0%', ''],
  ['rounded negative zero', '1', '100', '99.999', '100', '0%', ''],
  ['rounded positive zero', '1', '100', '100.001', '100', '0%', ''],
  ['zero position', '0', '100', '101', '--', '--', ''],
  ['invalid position', 'invalid', '100', '101', '--', '--', ''],
  ['infinite position', 'Infinity', '100', '101', '--', '--', ''],
  ['zero entry', '1', '0', '101', '--', '--', ''],
  ['negative entry', '1', '-1', '101', '--', '--', ''],
  ['invalid entry', '1', 'invalid', '101', '--', '--', ''],
  ['infinite entry', '1', 'Infinity', '101', '--', '--', ''],
  ['zero mark', '1', '100', '0', '100', '--', ''],
  ['negative mark', '1', '100', '-1', '100', '--', ''],
  ['invalid mark', '1', '100', 'invalid', '100', '--', ''],
  ['infinite mark', '1', '100', 'Infinity', '100', '--', ''],
  ['overflowing change', '1', '1e-308', '1e308', '0', '--', ''],
])('shows entry and mark change for %s', (_case, size, entry, mark, expectedEntry, expectedChange, color) => {
  const s = summarySnapshot('BTCUSDC');
  s.position.size = size;
  s.position.entry_price = entry;
  s.position.mark_price = '123';
  s.ticker.mark_price = mark;
  s.grid_config.direction = size === '-1' ? 'SHORT' : 'LONG';
  overview.data = { generated_at: new Date().toISOString(), pairs: [{ snapshot: s, mode: 'PAPER', scoped: true }], recent_trades: [] };
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  const card = screen.getByRole('link', { name: 'BTCUSDC' }).closest('.strategy-card') as HTMLElement;
  const entryMetric = within(card).getByText('开仓价格').nextElementSibling as HTMLElement;
  const changeMetric = within(card).getByText('涨跌幅').nextElementSibling as HTMLElement;
  expect(entryMetric.textContent).toBe(expectedEntry);
  expect(changeMetric.textContent).toBe(expectedChange);
  expect(changeMetric.className).toBe(color);
});


it('only reports normal operation when every strategy is running', () => {
  const running = summarySnapshot('BTCUSDC'); running.status = 'RUNNING';
  overview.data = { generated_at: new Date().toISOString(), pairs: [{ snapshot: running, mode: 'PAPER', scoped: true }], recent_trades: [] };
  const { unmount } = render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  expect(screen.getByText('全部策略运行正常')).toBeInTheDocument();
  unmount();
  overview.data = { generated_at: new Date().toISOString(), pairs: [], recent_trades: [] };
  const empty = render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  expect(screen.queryByText('全部策略运行正常')).not.toBeInTheDocument();
  empty.unmount();
  const stopped = summarySnapshot('BTCUSDC'); stopped.status = 'STOPPED';
  overview.data = { generated_at: new Date().toISOString(), pairs: [{ snapshot: stopped, mode: 'PAPER', scoped: true }], recent_trades: [] };
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  expect(screen.queryByText('全部策略运行正常')).not.toBeInTheDocument();
  expect(screen.getByText('已停止 1')).toBeInTheDocument();
});

it('filters the strategy list from the signal strip and keeps the filter in the URL', async () => {
  const running = summarySnapshot('BTCUSDC'); running.status = 'RUNNING';
  const error = summarySnapshot('SOLUSDC'); error.status = 'ERROR';
  overview.data = { generated_at: new Date().toISOString(), pairs: [running, error].map(s => ({ snapshot: s, mode: 'PAPER' as const, scoped: true })), recent_trades: [] };
  function Location() { const l = useLocation(); return <output data-testid="url">{l.pathname}{l.search}</output>; }
  render(<MemoryRouter><App><OverviewPage /><Location /></App></MemoryRouter>);
  const user = userEvent.setup();
  const chip = within(screen.getByLabelText('策略运行概览')).getByRole('button', { name: /异常 1/ });
  expect(chip).toHaveAttribute('aria-pressed', 'false');
  await user.click(chip);
  expect(chip).toHaveAttribute('aria-pressed', 'true');
  expect(screen.getByTestId('url')).toHaveTextContent('?status=ERROR');
  expect(screen.queryByRole('link', { name: 'BTCUSDC' })).not.toBeInTheDocument();
  expect(screen.getByRole('link', { name: 'SOLUSDC' })).toBeInTheDocument();
  await user.click(chip);
  expect(screen.getByTestId('url')).not.toHaveTextContent('status=');
  expect(screen.getByRole('link', { name: 'BTCUSDC' })).toBeInTheDocument();
});

it('clears search, sort and status together with one click', async () => {
  const running = summarySnapshot('BTCUSDC'); running.status = 'RUNNING';
  const error = summarySnapshot('SOLUSDC'); error.status = 'ERROR';
  overview.data = { generated_at: new Date().toISOString(), pairs: [running, error].map(s => ({ snapshot: s, mode: 'PAPER' as const, scoped: true })), recent_trades: [] };
  function Location() { const l = useLocation(); return <output data-testid="url">{l.pathname}{l.search}</output>; }
  render(<MemoryRouter initialEntries={['/?q=sol&sort=symbol&status=ERROR']}><App><OverviewPage /><Location /></App></MemoryRouter>);
  await userEvent.setup().click(screen.getAllByRole('button', { name: '清除筛选' })[0]);
  expect(screen.getByTestId('url')).toHaveTextContent(/^\/$/);
  expect(screen.getByRole('textbox', { name: '搜索交易对' })).toHaveValue('');
  expect(screen.getByRole('link', { name: 'BTCUSDC' })).toBeInTheDocument();
});

it('shows the validate-first guide only while every strategy is on paper and remembers dismissal', async () => {
  localStorage.removeItem('binance_bot_paper_guide_dismissed');
  const paper = summarySnapshot('BTCUSDC');
  overview.data = { generated_at: new Date().toISOString(), pairs: [{ snapshot: paper, mode: 'PAPER', scoped: true }], recent_trades: [] };
  const first = render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  expect(screen.getByRole('complementary', { name: '新手指引' })).toBeInTheDocument();
  await userEvent.setup().click(screen.getByRole('button', { name: '关闭新手指引' }));
  expect(screen.queryByRole('complementary', { name: '新手指引' })).not.toBeInTheDocument();
  first.unmount();
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  expect(screen.queryByRole('complementary', { name: '新手指引' })).not.toBeInTheDocument();
  localStorage.removeItem('binance_bot_paper_guide_dismissed');
});

it('hides the guide once any strategy is live', () => {
  localStorage.removeItem('binance_bot_paper_guide_dismissed');
  overview.data = { generated_at: new Date().toISOString(), pairs: [
    { snapshot: summarySnapshot('BTCUSDC'), mode: 'PAPER', scoped: true }, { snapshot: summarySnapshot('ETHUSDC', 'LIVE'), mode: 'LIVE', scoped: true },
  ], recent_trades: [] };
  render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
  expect(screen.queryByRole('complementary', { name: '新手指引' })).not.toBeInTheDocument();
});

it('marks the summary as stale and shows how old the data is', () => {
  vi.useFakeTimers();
  try {
    overview.dataUpdatedAt = Date.now() - 42_000;
    overview.data = { generated_at: '', pairs: [{ snapshot: summarySnapshot('BTCUSDC'), mode: 'PAPER', scoped: true }], recent_trades: [] };
    render(<MemoryRouter><App><OverviewPage /></App></MemoryRouter>);
    expect(screen.getByLabelText('策略运行概览')).toHaveClass('is-stale');
    expect(screen.getByText(/上次更新于 4\d秒前/)).toBeInTheDocument();
  } finally {
    vi.useRealTimers();
  }
});

it('restores search and sort from the URL', () => {
  overview.data = { generated_at: new Date().toISOString(), pairs: ['BTCUSDC', 'ETHUSDC'].map(symbol => ({ snapshot: summarySnapshot(symbol), mode: 'PAPER' as const, scoped: true })), recent_trades: [] };
  render(<MemoryRouter initialEntries={['/?q=eth&sort=symbol']}><App><OverviewPage /></App></MemoryRouter>);
  expect(screen.getByRole('textbox', { name: '搜索交易对' })).toHaveValue('eth');
  expect(screen.getByRole('link', { name: 'ETHUSDC' })).toBeInTheDocument();
  expect(screen.queryByRole('link', { name: 'BTCUSDC' })).not.toBeInTheDocument();
});

it('summarizes statuses and lets the user clear a search with no results', async () => {
  const running = summarySnapshot('BTCUSDC'); running.status = 'RUNNING';
  const error = summarySnapshot('SOLUSDC'); error.status = 'ERROR';
  overview.data = { generated_at: new Date().toISOString(), pairs: [running, error].map(s => ({ snapshot: s, mode: 'PAPER' as const, scoped: true })), recent_trades: [] };
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

it('updates finance disclosure from AccountsContext without re-rendering OverviewPage strategy cards', () => {
  const first = summarySnapshot('BTCUSDC');
  overview.data = { generated_at: new Date().toISOString(), pairs: [{ snapshot: first, mode: 'PAPER', scoped: true }], recent_trades: [] };
  const accountA = [{ snapshot: summarySnapshot('BTCUSDC'), mode: 'PAPER' as const, scoped: true }];
  const nextSnap = summarySnapshot('BTCUSDC');
  nextSnap.account = { ...nextSnap.account, margin_balance: '888.5', total_wallet_balance: '880', available_balance: '700' };
  const accountB = [{ snapshot: nextSnap, mode: 'PAPER' as const, scoped: true }];
  const { rerender } = render(<MemoryRouter><AccountsContext.Provider value={accountA}><App><OverviewPage /></App></AccountsContext.Provider></MemoryRouter>);
  expect(screen.getByText('保证金余额').closest('.account-card')).toHaveTextContent('105');
  vi.mocked(entryChange).mockClear();
  rerender(<MemoryRouter><AccountsContext.Provider value={accountB}><App><OverviewPage /></App></AccountsContext.Provider></MemoryRouter>);
  expect(screen.getByText('保证金余额').closest('.account-card')).toHaveTextContent('888.5');
  expect(entryChange).not.toHaveBeenCalled();
});
