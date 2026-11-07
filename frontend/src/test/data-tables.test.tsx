import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { App } from 'antd';
import { beforeEach, expect, it, vi } from 'vitest';
import { LogsTable, OrdersTable, TradesTable } from '../components/DataTables';
import type { Log, Order, Trade } from '../lib/types';

const exported = vi.hoisted(() => ({ download: vi.fn(), copy: vi.fn() }));
vi.mock('../lib/exportCsv', async importOriginal => ({ ...await importOriginal<typeof import('../lib/exportCsv')>(), downloadCsv: exported.download, copyText: exported.copy }));

beforeEach(() => {
  vi.clearAllMocks();
  exported.copy.mockResolvedValue(true);
});

const trade = (id: string, side: 'BUY' | 'SELL', pnl: string): Trade => ({ trade_id: id, symbol: 'BTCUSDC', mode: 'PAPER', side, price: '67420.5', quantity: '0.002', amount_usdc: '134.84', realized_pnl: pnl, commission: '0.02', pnl_verified: true, is_maker: true, timestamp: '2026-11-05T01:00:00Z', note: '=cmd' });
const order = (id: string, side: 'BUY' | 'SELL', price: string): Order => ({ client_order_id: id, order_id: 1, symbol: 'BTCUSDC', side, price, quantity: '0.002', amount_usdc: '100', status: 'NEW', created_at: '2026-11-05T01:00:00Z', grid_level: 1, is_take_profit: false, purpose: 'GRID', merge_sources: [] });
const log = (level: string, message: string): Log => ({ timestamp: '2026-11-05T01:00:00Z', level, message });
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
  view(<OrdersTable orders={[order('o1', 'BUY', '67420.50000000')]} mark="67420" />);
  // The table and the narrow-screen card list both render the price; either one copies the raw value.
  const buttons = screen.getAllByRole('button', { name: '复制 67420.50000000' });
  expect(buttons).toHaveLength(2);
  await userEvent.setup().click(buttons[0]);
  expect(exported.copy).toHaveBeenCalledWith('67420.50000000');
  expect(await screen.findByText('已复制 67420.50000000')).toBeInTheDocument();
});

it('filters orders by side and disables export when nothing matches', async () => {
  view(<OrdersTable orders={[order('o1', 'BUY', '99')]} mark="100" />);
  const user = userEvent.setup();
  await user.click(within(screen.getByRole('radiogroup', { name: '按方向筛选' })).getByText('卖出'));
  expect(screen.getByRole('status')).toHaveTextContent('显示 0 / 1 条');
  expect(screen.getByRole('button', { name: /导出 CSV/ })).toBeDisabled();
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
  view(<LogsTable logs={[log('INFO', '网格已对账'), log('ERROR', '撤单失败')]} />);
  expect(screen.getByRole('list', { name: '日志列表' }).querySelectorAll('li')).toHaveLength(2);
  await userEvent.setup().click(within(screen.getByRole('radiogroup', { name: '按级别筛选' })).getByText('ERROR'));
  expect(screen.getByRole('list', { name: '日志列表' }).querySelectorAll('li')).toHaveLength(1);
  expect(screen.getByRole('table').querySelectorAll('tbody tr.ant-table-row')).toHaveLength(1);
});

it('caps the card list but keeps the export complete', async () => {
  const many = Array.from({ length: 120 }, (_, i) => log('INFO', `事件 ${i}`));
  view(<LogsTable logs={many} />);
  expect(screen.getByRole('list', { name: '日志列表' }).querySelectorAll('li')).toHaveLength(100);
  expect(screen.getByText('仅显示最近 100 条，完整数据请导出 CSV。')).toBeInTheDocument();
  await userEvent.setup().click(screen.getByRole('button', { name: /导出 CSV/ }));
  expect(String(exported.download.mock.calls[0][1]).split('\r\n')).toHaveLength(121);
});
