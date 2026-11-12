import { act, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { App } from 'antd';
import { beforeEach, expect, it, vi } from 'vitest';
import { LogsTable, OrdersTable, TradesTable } from '../components/DataTables';
import { log as makeLog, order as makeOrder, trade as makeTrade } from './fixtures';

const exported = vi.hoisted(() => ({ download: vi.fn(), copy: vi.fn() }));
vi.mock('../lib/exportCsv', async importOriginal => ({ ...await importOriginal<typeof import('../lib/exportCsv')>(), downloadCsv: exported.download, copyText: exported.copy }));

function stubNarrowViewport(initialNarrow = false) {
  let narrow = initialNarrow;
  const listeners = new Set<(event: MediaQueryListEvent) => void>();
  vi.stubGlobal('matchMedia', vi.fn().mockImplementation((query: string) => ({
    get matches() { return query === '(max-width: 600px)' ? narrow : false; },
    media: query,
    addListener: (cb: (event: MediaQueryListEvent) => void) => { if (query === '(max-width: 600px)') listeners.add(cb); },
    removeListener: (cb: (event: MediaQueryListEvent) => void) => { listeners.delete(cb); },
    addEventListener: (_: string, cb: (event: MediaQueryListEvent) => void) => { if (query === '(max-width: 600px)') listeners.add(cb); },
    removeEventListener: (_: string, cb: (event: MediaQueryListEvent) => void) => { listeners.delete(cb); },
  })));
  return (nextNarrow: boolean) => {
    narrow = nextNarrow;
    act(() => {
      for (const listener of listeners) listener({ matches: narrow, media: '(max-width: 600px)' } as MediaQueryListEvent);
    });
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  exported.copy.mockResolvedValue(true);
});

const trade = (id: string, side: 'BUY' | 'SELL', pnl: string) => makeTrade({ trade_id: id, symbol: 'BTCUSDC', side, price: '67420.5', quantity: '0.002', amount_usdc: '134.84', realized_pnl: pnl, commission: '0.02', note: '=cmd' });
const order = (id: string, side: 'BUY' | 'SELL', price: string) => makeOrder({ client_order_id: id, symbol: 'BTCUSDC', side, price, quantity: '0.002' });
const log = (level: string, message: string) => makeLog({ level, message });
const view = (node: React.ReactNode) => render(<App>{node}</App>);

it('filters trades by side and exports exactly the rows shown, with spreadsheet-safe cells', async () => {
  view(<TradesTable trades={[trade('1', 'BUY', '-0.11'), trade('2', 'SELL', '0.42')]} />);
  const user = userEvent.setup();
  expect(screen.getByRole('status')).toHaveTextContent('显示 2 / 2 条');
  await user.click(within(screen.getByRole('radiogroup', { name: '按方向筛选' })).getByText('卖出'));
  expect(screen.getByRole('status')).toHaveTextContent('显示 1 / 2 条');
  await user.click(screen.getByRole('button', { name: /导出 CSV/ }));
  const [name, csv] = exported.download.mock.calls[0];
  expect(name).toMatch(/^trades-\d{4}-\d{2}-\d{2}\.csv$/);
  const lines = String(csv).split('\r\n');
  expect(lines).toHaveLength(2);
  expect(lines[1]).toContain('卖出');
  expect(lines[1]).toContain(',0.42,');
  expect(lines[1]).toContain("'=cmd");
});

it('keeps negative PnL numeric in the export', async () => {
  view(<TradesTable trades={[trade('1', 'BUY', '-0.11')]} />);
  await userEvent.setup().click(screen.getByRole('button', { name: /导出 CSV/ }));
  expect(String(exported.download.mock.calls[0][1])).toContain(',-0.11,');
});

it('does not add filters or export to the overview trades card', () => {
  view(<TradesTable overview trades={[trade('1', 'BUY', '1')]} />);
  expect(screen.queryByRole('button', { name: /导出 CSV/ })).not.toBeInTheDocument();
  expect(screen.queryByRole('radiogroup', { name: '按方向筛选' })).not.toBeInTheDocument();
});

it('copies the raw price rather than the formatted one', async () => {
  const setNarrow = stubNarrowViewport(false);
  view(<OrdersTable orders={[order('o1', 'BUY', '67420.50000000')]} mark="67420" />);
  expect(screen.queryByRole('list', { name: '挂单列表' })).not.toBeInTheDocument();
  const desktopButton = screen.getByRole('button', { name: '复制 67420.50000000' });
  await userEvent.setup().click(desktopButton);
  expect(exported.copy).toHaveBeenCalledWith('67420.50000000');
  expect(await screen.findByText('已复制 67420.50000000')).toBeInTheDocument();
  expect(desktopButton).toHaveClass('copyable-copied');

  setNarrow(true);
  expect(screen.queryByRole('table')).not.toBeInTheDocument();
  const mobileButton = within(screen.getByRole('list', { name: '挂单列表' })).getByRole('button', { name: '复制 67420.50000000' });
  await userEvent.setup().click(mobileButton);
  expect(exported.copy).toHaveBeenCalledTimes(2);
});

it('filters orders by side and disables export when nothing matches', async () => {
  view(<OrdersTable orders={[order('o1', 'BUY', '99')]} mark="100" />);
  const user = userEvent.setup();
  await user.click(within(screen.getByRole('radiogroup', { name: '按方向筛选' })).getByText('卖出'));
  expect(screen.getByRole('status')).toHaveTextContent('显示 0 / 1 条');
  expect(screen.getByRole('button', { name: /导出 CSV/ })).toBeDisabled();
});

it('renders UNVERIFIED order purpose as 待核实', () => {
  view(<OrdersTable orders={[{ ...order('o-unverified', 'BUY', '99'), purpose: 'UNVERIFIED' }]} mark="100" />);
  expect(screen.getAllByText('待核实').length).toBeGreaterThan(0);
});

it('renders and exports non-quote and mixed commission assets without rounding small BNB fees to zero', async () => {
  const rows = [
    makeTrade({ trade_id: 't-bnb', symbol: 'SOLUSDC', side: 'BUY', commission: '0.00004500', commission_asset: 'BNB' }),
    makeTrade({ trade_id: 't-mixed-quote', symbol: 'SOLUSDC', side: 'SELL', commission: '0.04', commission_asset: 'MIXED' }),
    makeTrade({ trade_id: 't-mixed-zero', symbol: 'SOLUSDC', side: 'SELL', commission: '0', commission_asset: 'MIXED' }),
  ];
  view(<TradesTable trades={rows} />);
  expect(screen.getByText('0.000045 BNB')).toBeInTheDocument();
  expect(screen.getByText('0.0400 + 其他币种')).toBeInTheDocument();
  expect(screen.getByText('多币种')).toBeInTheDocument();

  await userEvent.setup().click(screen.getByRole('button', { name: /导出 CSV/ }));
  const csv = String(exported.download.mock.calls[0][1]);
  expect(csv).toContain(',0.000045 BNB,');
  expect(csv).toContain(',0.04 + 其他币种,');
  expect(csv).toContain(',多币种,');
});

it('filters logs by level and exports the filtered rows', async () => {
  view(<LogsTable logs={[log('INFO', '网格已对账'), log('ERROR', '撤单失败'), log('WARN', '接近上限')]} />);
  const user = userEvent.setup();
  await user.click(within(screen.getByRole('radiogroup', { name: '按级别筛选' })).getByText('ERROR'));
  expect(screen.getByRole('status')).toHaveTextContent('显示 1 / 3 条');
  expect(screen.getAllByText('撤单失败').length).toBeGreaterThan(0);
  expect(screen.queryAllByText('网格已对账')).toHaveLength(0);
  await user.click(screen.getByRole('button', { name: /导出 CSV/ }));
  await waitFor(() => expect(exported.download).toHaveBeenCalled());
  expect(String(exported.download.mock.calls[0][1]).split('\r\n')).toHaveLength(2);
});

it('renders the same filtered rows in the table and in the narrow-screen card list', async () => {
  const setNarrow = stubNarrowViewport(false);
  view(<LogsTable logs={[log('INFO', '网格已对账'), log('ERROR', '撤单失败')]} />);
  expect(screen.getByRole('table').querySelectorAll('tbody tr.ant-table-row')).toHaveLength(2);
  expect(screen.queryByRole('list', { name: '日志列表' })).not.toBeInTheDocument();
  await userEvent.setup().click(within(screen.getByRole('radiogroup', { name: '按级别筛选' })).getByText('ERROR'));
  expect(screen.getByRole('table').querySelectorAll('tbody tr.ant-table-row')).toHaveLength(1);
  setNarrow(true);
  expect(screen.queryByRole('table')).not.toBeInTheDocument();
  expect(screen.getByRole('list', { name: '日志列表' }).querySelectorAll('li')).toHaveLength(1);
});

it('keeps duplicate log keys unique across pages and stable when new logs arrive', async () => {
  const setNarrow = stubNarrowViewport(false);
  const logs = Array.from({ length: 61 }, () => log('INFO', '网格已对账'));
  const { rerender } = view(<LogsTable logs={logs} />);
  const pageKeys = () => [...screen.getByRole('table').querySelectorAll('tbody tr.ant-table-row')].map(row => row.getAttribute('data-row-key'));
  const keys = pageKeys();
  const user = userEvent.setup();
  await user.click(screen.getByTitle('2').querySelector('a')!);
  keys.push(...pageKeys());
  await user.click(screen.getByTitle('3').querySelector('a')!);
  const oldestKey = pageKeys()[0];
  keys.push(...pageKeys());
  expect(keys).toHaveLength(61);
  expect(new Set(keys).size).toBe(61);
  rerender(<App><LogsTable logs={[log('INFO', '新日志'), log('INFO', '网格已对账'), ...logs]} /></App>);
  expect(pageKeys()).toContain(oldestKey);

  setNarrow(true);
  const oldestMobile = screen.getByRole('list', { name: '日志列表' }).lastElementChild;
  rerender(<App><LogsTable logs={[log('INFO', '最新日志'), log('INFO', '新日志'), log('INFO', '网格已对账'), ...logs]} /></App>);
  expect(screen.getByRole('list', { name: '日志列表' }).lastElementChild).toBe(oldestMobile);
});

it('caps the card list but keeps the export complete', async () => {
  stubNarrowViewport(true);
  const many = Array.from({ length: 120 }, (_, i) => log('INFO', `事件 ${i}`));
  view(<LogsTable logs={many} />);
  expect(screen.queryByRole('table')).not.toBeInTheDocument();
  expect(screen.getByRole('list', { name: '日志列表' }).querySelectorAll('li')).toHaveLength(100);
  expect(screen.getByText('仅显示最近 100 条，完整数据请导出 CSV。')).toBeInTheDocument();
  await userEvent.setup().click(screen.getByRole('button', { name: /导出 CSV/ }));
  expect(String(exported.download.mock.calls[0][1]).split('\r\n')).toHaveLength(121);
});

it('exposes SUCCESS instead of DEBUG in the log level filter and styles success badges', async () => {
  view(<LogsTable logs={[log('INFO', '常规同步'), log('SUCCESS', '止盈成交并补挂网格')]} />);
  const group = screen.getByRole('radiogroup', { name: '按级别筛选' });
  expect(within(group).queryByText('DEBUG')).not.toBeInTheDocument();
  expect(within(group).getByText('SUCCESS')).toBeInTheDocument();
  await userEvent.setup().click(within(group).getByText('SUCCESS'));
  expect(screen.getByRole('status')).toHaveTextContent('显示 1 / 2 条');
  const badges = screen.getAllByText('SUCCESS').filter(el => el.classList.contains('log-level'));
  expect(badges.length).toBeGreaterThan(0);
  for (const badge of badges) expect(badge).toHaveClass('log-level-success');
});

it('renders recent trade times with ageText so slight browser clock lag never reads as 0秒后 or 3秒后', () => {
  const fillTime = Date.parse('2026-11-05T01:00:03Z');
  view(<TradesTable overview now={fillTime - 3_000} trades={[makeTrade({ trade_id: 'lag-1', timestamp: new Date(fillTime).toISOString() })]} />);
  expect(screen.queryByText(/后/)).not.toBeInTheDocument();
  expect(screen.getAllByText('刚刚').length).toBeGreaterThan(0);
});
