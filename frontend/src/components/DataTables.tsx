import { useEffect, useMemo, useState } from 'react';
import { App, Button, Segmented, Table, Tag } from 'antd';
import { CopyOutlined, DownloadOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import { number, price, relativeTime, time } from '../lib/format';
import type { Log, Order, Trade } from '../lib/types';
import { Pnl } from './Common';
import { copyText, downloadCsv, toCsv } from '../lib/exportCsv';

const side = (value: string) => <span className={`side-tag side-${value === 'BUY' ? 'buy' : 'sell'}`}><span aria-hidden="true">{value === 'BUY' ? '↑' : '↓'}</span>{value === 'BUY' ? '买入' : '卖出'}</span>;
const purposeLabel: Record<string, string> = { GRID: '网格', TAKE_PROFIT: '止盈', REMAINDER: '余量合并', LEGACY: '历史订单' };
/** Click-to-copy for a price or quantity. The visible text stays the formatted value; the raw value is what gets copied. */
function Copyable({ value, children }: { value: string; children: React.ReactNode }) {
  const { message } = App.useApp();
  return <button type="button" className="copyable" title="点击复制" aria-label={`复制 ${value}`} onClick={() => void copyText(value).then(ok => ok ? message.success(`已复制 ${value}`) : message.error('复制失败，请手动选择文本'))}>
    {children}<CopyOutlined aria-hidden="true" />
  </button>;
}

type SideFilter = 'ALL' | 'BUY' | 'SELL';
const sideOptions = [{ value: 'ALL', label: '全部' }, { value: 'BUY', label: '买入' }, { value: 'SELL', label: '卖出' }];
function SideFilterBar({ value, onChange, shown, total, onExport, exportLabel }: { value: SideFilter; onChange: (value: SideFilter) => void; shown: number; total: number; onExport: () => void; exportLabel: string }) {
  return <div className="table-toolbar">
    <Segmented<SideFilter> size="small" aria-label="按方向筛选" value={value} onChange={onChange} options={sideOptions as { value: SideFilter; label: string }[]} />
    <span className="muted" role="status">显示 {shown} / {total} 条</span>
    <Button size="small" icon={<DownloadOutlined />} disabled={!shown} onClick={onExport}>{exportLabel}</Button>
  </div>;
}

export function OrdersTable({ orders, mark }: { orders: Order[]; mark?: string }) {
  const [filter, setFilter] = useState<SideFilter>('ALL');
  const rows = useMemo(() => filter === 'ALL' ? orders : orders.filter(order => order.side === filter), [orders, filter]);
  const exportRows = () => downloadCsv(`orders-${new Date().toISOString().slice(0, 10)}.csv`, toCsv(
    ['方向', '价格', '数量', '金额', '用途', '状态', '时间（北京）'],
    rows.map(o => [o.side === 'BUY' ? '买入' : '卖出', o.price, o.quantity, o.amount_usdc, purposeLabel[o.purpose] ?? (o.is_take_profit ? '止盈' : '历史订单'), o.status, time(o.created_at)]),
  ));
  const columns: ColumnsType<Order> = [
    { title: '方向', dataIndex: 'side', render: side, width: 90, fixed: 'left' },
    { title: '价格', align: 'right', className: 'table-numeric', dataIndex: 'price', render: (v: string) => <Copyable value={v}>{price(v)}</Copyable>, sorter: (a, b) => Number(a.price) - Number(b.price) },
    { title: '距标记价格', align: 'right', className: 'table-numeric', render: (_, o) => Number(mark) > 0 ? `${number((Number(o.price) / Number(mark) - 1) * 100)}%` : '--' },
    { title: '数量', align: 'right', className: 'table-numeric', dataIndex: 'quantity', render: price },
    { title: '金额', align: 'right', className: 'table-numeric', dataIndex: 'amount_usdc', render: (v: string) => number(v) },
    { title: '用途', render: (_, o) => purposeLabel[o.purpose] ?? (o.is_take_profit ? '止盈' : '历史订单') },
    { title: '状态', dataIndex: 'status' },
    { title: '时间（北京）', dataIndex: 'created_at', render: time, width: 185 },
  ];
  return <><SideFilterBar value={filter} onChange={setFilter} shown={rows.length} total={orders.length} onExport={exportRows} exportLabel="导出 CSV" /><div className="table-desktop"><p className="table-scroll-hint">左右滑动查看完整挂单信息</p><Table<Order> rowKey="client_order_id" columns={columns} dataSource={rows} size="small" scroll={{ x: 900 }}
    pagination={{ pageSize: 15, showSizeChanger: false, hideOnSinglePage: true }}
    expandable={{ rowExpandable: o => !!o.merge_sources?.length, expandedRowRender: o => <ul>{o.merge_sources.map(source => <li key={source.client_order_id}>{source.client_order_id} · {price(source.price)} × {price(source.quantity)}</li>)}</ul> }} /></div>
    <div className="table-mobile">{rows.length ? <ul className="trade-list" aria-label="挂单列表">{rows.map(o => <li className="trade-list-item" key={o.client_order_id}>
      <div className="trade-list-heading">{side(o.side)}<strong><Copyable value={o.price}>{price(o.price)}</Copyable></strong><span className="trade-list-time">{purposeLabel[o.purpose] ?? (o.is_take_profit ? '止盈' : '历史订单')}</span></div>
      <dl>
        <div><dt>数量</dt><dd>{price(o.quantity)}</dd></div>
        <div><dt>金额</dt><dd>{number(o.amount_usdc)}</dd></div>
        <div><dt>距标记价</dt><dd>{Number(mark) > 0 ? `${number((Number(o.price) / Number(mark) - 1) * 100)}%` : '--'}</dd></div>
      </dl>
    </li>)}</ul> : <div className="trade-list-empty">暂无挂单</div>}</div></>;
}
export function TradesTable({ trades: allTrades, overview = false }: { trades: Trade[]; overview?: boolean }) {
  const [now, setNow] = useState(Date.now);
  const [filter, setFilter] = useState<SideFilter>('ALL');
  const trades = useMemo(() => overview || filter === 'ALL' ? allTrades : allTrades.filter(trade => trade.side === filter), [allTrades, filter, overview]);
  const exportRows = () => downloadCsv(`trades-${new Date().toISOString().slice(0, 10)}.csv`, toCsv(
    ['时间（北京）', '方向', '成交价格', '数量', '金额', '已实现盈亏', '手续费', '成交方式', '备注'],
    trades.map(t => [time(t.timestamp), t.side === 'BUY' ? '买入' : '卖出', t.price, t.quantity, t.amount_usdc, t.pnl_verified ? t.realized_pnl : '待同步', t.pnl_verified ? t.commission : '待同步', t.pnl_verified ? (t.is_maker ? 'Maker' : 'Taker') : '待同步', t.note]),
  ));
  useEffect(() => {
    if (!overview || !trades.length) return;
    setNow(Date.now());
    const timer = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, [overview, trades.length]);
  const columns: ColumnsType<Trade> = [
    ...(overview ? [{ title: '交易对', dataIndex: 'symbol', className: 'trade-symbol', width: 130 }] : []),
    { title: '方向', dataIndex: 'side', render: side, width: 90, fixed: overview ? undefined : 'left' },
    { title: overview ? '成交时间' : '时间（北京）', dataIndex: 'timestamp', className: overview ? 'trade-time' : undefined, render: overview ? (value: string) => <span title={Number.isFinite(Date.parse(value)) ? `${time(value)}（北京时间）` : undefined}>{relativeTime(value, now)}</span> : time, width: overview ? 90 : 185 },
    { title: '成交价格', align: 'right', className: 'table-numeric', dataIndex: 'price', render: (v: string) => overview ? price(v) : <Copyable value={v}>{price(v)}</Copyable> },
    { title: '数量', align: 'right', className: 'table-numeric', dataIndex: 'quantity', render: price },
    { title: '金额', align: 'right', className: 'table-numeric', dataIndex: 'amount_usdc', render: (v: string) => number(v) },
    { title: overview ? '盈亏' : '已实现盈亏', align: 'right', className: 'table-numeric', render: (_, t) => t.pnl_verified ? <Pnl value={t.realized_pnl} /> : <Tag>待同步</Tag> },
    { title: '手续费', align: 'right', className: 'table-numeric', render: (_, t) => t.pnl_verified ? number(t.commission, 4) : <Tag>待同步</Tag> },
    { title: '成交方式', render: (_, t) => t.pnl_verified ? (t.is_maker ? 'Maker' : 'Taker') : <Tag>待同步</Tag> },
    { title: '备注', dataIndex: 'note', ellipsis: true },
  ];
  return <div>
    {!overview && <SideFilterBar value={filter} onChange={setFilter} shown={trades.length} total={allTrades.length} onExport={exportRows} exportLabel="导出 CSV" />}
    {!overview && <p className="table-scroll-hint">左右滑动查看完整成交信息</p>}
    {overview && <p className="overview-trades-scroll-hint">左右滑动查看完整成交信息</p>}
    <div className={overview ? 'overview-trades-desktop' : 'table-desktop'}><Table<Trade> rowKey="trade_id" columns={columns} dataSource={trades} size="small" scroll={{ x: 1000 }} pagination={overview ? false : { pageSize: 15, showSizeChanger: false, hideOnSinglePage: true }} /></div>
    {!overview && <div className="table-mobile">{trades.length ? <ul className="trade-list" aria-label="成交列表">{trades.slice(0, 50).map(trade => <li className="trade-list-item" key={trade.trade_id}>
      <div className="trade-list-heading">{side(trade.side)}<strong><Copyable value={trade.price}>{price(trade.price)}</Copyable></strong><span className="trade-list-time" title={`${time(trade.timestamp)}（北京时间）`}>{relativeTime(trade.timestamp, now)}</span></div>
      <dl>
        <div><dt>数量</dt><dd>{price(trade.quantity)}</dd></div>
        <div><dt>金额</dt><dd>{number(trade.amount_usdc)}</dd></div>
        <div><dt>盈亏</dt><dd>{trade.pnl_verified ? <Pnl value={trade.realized_pnl} /> : <Tag>待同步</Tag>}</dd></div>
      </dl>
    </li>)}</ul> : <div className="trade-list-empty">暂无成交记录</div>}{trades.length > 50 && <p className="muted">仅显示最近 50 条，完整数据请导出 CSV。</p>}</div>}
    {overview && <div className="overview-trades-mobile">{trades.length ? <ul className="trade-list">{trades.map(trade => <li className="trade-list-item" key={trade.trade_id}>
      <div className="trade-list-heading"><strong>{trade.symbol}</strong>{side(trade.side)}<span className="trade-list-time" title={Number.isFinite(Date.parse(trade.timestamp)) ? `${time(trade.timestamp)}（北京时间）` : undefined}>{relativeTime(trade.timestamp, now)}</span></div>
      <dl>
        <div><dt>成交均价</dt><dd>{price(trade.price)}</dd></div>
        <div><dt>金额</dt><dd>{number(trade.amount_usdc)}</dd></div>
        <div><dt>盈亏</dt><dd>{trade.pnl_verified ? <Pnl value={trade.realized_pnl} /> : <Tag>待同步</Tag>}</dd></div>
      </dl>
    </li>)}</ul> : <div className="trade-list-empty">暂无成交记录</div>}</div>}
  </div>;
}
const levels = ['ALL', 'ERROR', 'WARN', 'INFO', 'DEBUG'] as const;
type LevelFilter = typeof levels[number];
export function LogsTable({ logs }: { logs: Log[] }) {
  const [level, setLevel] = useState<LevelFilter>('ALL');
  const rows = useMemo(() => level === 'ALL' ? logs : logs.filter(log => log.level === level), [logs, level]);
  const exportRows = () => downloadCsv(`logs-${new Date().toISOString().slice(0, 10)}.csv`, toCsv(['时间（北京）', '级别', '内容'], rows.map(l => [time(l.timestamp), l.level, l.message])));
  return <>
    <div className="table-toolbar">
      <Segmented<LevelFilter> size="small" aria-label="按级别筛选" value={level} onChange={setLevel} options={levels.map(value => ({ value, label: value === 'ALL' ? '全部' : value }))} />
      <span className="muted" role="status">显示 {rows.length} / {logs.length} 条</span>
      <Button size="small" icon={<DownloadOutlined />} disabled={!rows.length} onClick={exportRows}>导出 CSV</Button>
    </div>
    <div className="table-desktop"><p className="table-scroll-hint">左右滑动查看完整日志</p>
    <Table<Log> rowKey={l => `${l.timestamp}-${l.level}-${l.message}`} dataSource={rows} size="small" scroll={{ x: 600 }} pagination={{ pageSize: 30, showSizeChanger: false }} columns={[
      { title: '时间（北京）', dataIndex: 'timestamp', render: time, width: 185 },
      { title: '级别', dataIndex: 'level', width: 90, render: v => <span className={`log-level log-level-${String(v).toLowerCase()}`}>{v}</span> },
      { title: '内容', dataIndex: 'message', className: 'log-message' },
    ]} /></div>
    <div className="table-mobile">{rows.length ? <ul className="trade-list" aria-label="日志列表">{rows.slice(0, 100).map(l => <li className="trade-list-item" key={`${l.timestamp}-${l.level}-${l.message}`}>
      <div className="trade-list-heading"><span className={`log-level log-level-${l.level.toLowerCase()}`}>{l.level}</span><span className="trade-list-time">{time(l.timestamp)}</span></div>
      <p className="log-message">{l.message}</p>
    </li>)}</ul> : <div className="trade-list-empty">暂无日志</div>}{rows.length > 100 && <p className="muted">仅显示最近 100 条，完整数据请导出 CSV。</p>}</div>
  </>;
}
