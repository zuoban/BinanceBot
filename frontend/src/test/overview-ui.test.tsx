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
import { entryChange } from '../lib/format';

vi.mock('../lib/format', async importOriginal => {
  const actual = await importOriginal<typeof import('../lib/format')>();
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
  vi.mocked(runBatchControls).mockReset().mockResolvedValue([]);
});
vi.mock('../lib/queries', () => ({ useOverview: () => overview, useOverviewHistory: () => ({ data: { snapshots: [], current_accounts: [] }, isPending: false, error: null }) }));
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
  expect(within(drawer).getByText('全部暂停：1/6 已确认')).toBeInTheDocument();
  const rows = within(drawer).getAllByRole('listitem');
  expect(rows.map(row => within(row).getByRole('link').textContent)).toEqual(['SOLUSDC', 'BNBUSDC', 'XRPUSDC', 'BTCUSDC', 'ETHUSDC']);
  expect(within(rows[0]).getByText('提交前出错，未提交')).toBeInTheDocument();
  expect(within(rows[1]).getByText('请求结果不明，操作可能已生效')).toBeInTheDocument();
  expect(within(rows[3]).getByText('暂停状态及撤单结果未确认')).toBeInTheDocument();
  expect(within(rows[4]).getByText('已暂停，撤单结果未确认')).toBeInTheDocument();
  // Only strategies that were certainly not submitted may be retried; uncertain ones must be checked first.
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
