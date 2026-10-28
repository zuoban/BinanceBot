import { Table, Tag } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { number, price, time } from '../lib/format';
import type { Log, Order, Trade } from '../lib/types';
import { Pnl } from './Common';

const side = (value: string) => <Tag color={value === 'BUY' ? 'green' : 'red'}>{value === 'BUY' ? '买入' : '卖出'}</Tag>;
const purposeLabel: Record<string, string> = { GRID: '网格', TAKE_PROFIT: '止盈', REMAINDER: '余量合并', LEGACY: '历史订单' };
export function OrdersTable({ orders, mark }: { orders: Order[]; mark?: string }) {
  const columns: ColumnsType<Order> = [
    { title: '方向', dataIndex: 'side', render: side, width: 90, filters: [{ text: '买入', value: 'BUY' }, { text: '卖出', value: 'SELL' }], onFilter: (v, o) => o.side === v },
    { title: '价格', dataIndex: 'price', render: price, sorter: (a, b) => Number(a.price) - Number(b.price) },
    { title: '距标记价格', render: (_, o) => Number(mark) > 0 ? `${number((Number(o.price) / Number(mark) - 1) * 100)}%` : '--' },
    { title: '数量', dataIndex: 'quantity', render: price },
    { title: '金额', dataIndex: 'amount_usdc', render: (v: string) => number(v) },
    { title: '用途', render: (_, o) => purposeLabel[o.purpose] ?? (o.is_take_profit ? '止盈' : '历史订单') },
    { title: '状态', dataIndex: 'status' },
    { title: '时间（北京）', dataIndex: 'created_at', render: time, width: 185 },
  ];
  return <Table<Order> rowKey="client_order_id" columns={columns} dataSource={orders} size="small" scroll={{ x: 900 }}
    pagination={{ pageSize: 15, showSizeChanger: false, hideOnSinglePage: true }}
    expandable={{ rowExpandable: o => !!o.merge_sources?.length, expandedRowRender: o => <ul>{o.merge_sources.map(source => <li key={source.client_order_id}>{source.client_order_id} · {price(source.price)} × {price(source.quantity)}</li>)}</ul> }} />;
}
export function TradesTable({ trades }: { trades: Trade[] }) {
  const columns: ColumnsType<Trade> = [
    { title: '时间（北京）', dataIndex: 'timestamp', render: time, width: 185 },
    { title: '方向', dataIndex: 'side', render: side, width: 90 },
    { title: '成交价格', dataIndex: 'price', render: price },
    { title: '数量', dataIndex: 'quantity', render: price },
    { title: '金额', dataIndex: 'amount_usdc', render: (v: string) => number(v) },
    { title: '已实现盈亏', render: (_, t) => t.pnl_verified ? <Pnl value={t.realized_pnl} /> : <Tag>待同步</Tag> },
    { title: '手续费', dataIndex: 'commission', render: (v: string) => number(v, 4) },
    { title: '成交方式', render: (_, t) => t.is_maker ? 'Maker' : 'Taker' },
    { title: '备注', dataIndex: 'note', ellipsis: true },
  ];
  return <Table<Trade> rowKey="trade_id" columns={columns} dataSource={trades} size="small" scroll={{ x: 1000 }} pagination={{ pageSize: 15, showSizeChanger: false, hideOnSinglePage: true }} />;
}
export function LogsTable({ logs }: { logs: Log[] }) {
  return <Table<Log> rowKey={l => `${l.timestamp}-${l.level}-${l.message}`} dataSource={logs} size="small" scroll={{ x: 600 }} pagination={{ pageSize: 30, showSizeChanger: false }} columns={[
    { title: '时间（北京）', dataIndex: 'timestamp', render: time, width: 185 },
    { title: '级别', dataIndex: 'level', width: 90, filters: ['INFO', 'WARN', 'ERROR', 'DEBUG'].map(value => ({ text: value, value })), onFilter: (v, l) => l.level === v, render: v => <Tag color={v === 'ERROR' ? 'error' : v === 'WARN' ? 'warning' : 'default'}>{v}</Tag> },
    { title: '内容', dataIndex: 'message', className: 'log-message' },
  ]} />;
}
