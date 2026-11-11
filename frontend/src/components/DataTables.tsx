import { createContext, memo, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import { App, Button, Segmented, Table, Tag } from 'antd';
import { CheckOutlined, CopyOutlined, DownloadOutlined } from '@ant-design/icons';
import { Link, useInRouterContext } from 'react-router-dom';
import type { ColumnsType } from 'antd/es/table';
import { beijingDay, number, price, quantity, relativeTime, time } from '../lib/format';
import { priceDiffPercent } from '../lib/gridPreview';
import { useNow } from '../lib/useNow';
import type { Log, Order, Trade } from '../lib/types';
import { Pnl, SideTag } from './Common';
import { copyText, downloadCsv, toCsv } from '../lib/exportCsv';

const side = (value: string) => <SideTag side={value === 'BUY' ? 'BUY' : 'SELL'} />;
const purposeLabel: Record<string, string> = { GRID: '网格', TAKE_PROFIT: '止盈', REMAINDER: '余量合并', LEGACY: '历史订单' };
function orderPurpose(o: Order) {
  return purposeLabel[o.purpose] ?? (o.is_take_profit ? '止盈' : '历史订单');
}
const NowContext = createContext(0);
function NowProvider({ currentNow, enabled, children }: { currentNow?: number; enabled: boolean; children: React.ReactNode }) {
  const fallbackNow = useNow(1000, currentNow === undefined && enabled);
  return <NowContext.Provider value={currentNow ?? fallbackNow}>{children}</NowContext.Provider>;
}
const NARROW_MEDIA_QUERY = '(max-width: 600px)';
function useNarrowViewport(skip = false) {
  const [narrow, setNarrow] = useState(() => !skip && typeof window !== 'undefined' && Boolean(window.matchMedia?.(NARROW_MEDIA_QUERY)?.matches));
  useEffect(() => {
    if (skip || typeof window === 'undefined' || typeof window.matchMedia !== 'function') return;
    const media = window.matchMedia(NARROW_MEDIA_QUERY);
    setNarrow(media.matches);
    const update = () => setNarrow(media.matches);
    media.addEventListener('change', update);
    return () => media.removeEventListener('change', update);
  }, [skip]);
  return narrow;
}
function RelativeTime({ value, className }: { value: string; className?: string }) {
  const now = useContext(NowContext);
  const timestamp = Date.parse(value);
  return <span className={className} title={Number.isFinite(timestamp) ? `${time(timestamp)}（北京时间）` : undefined}>{relativeTime(timestamp, now)}</span>;
}
const MarkPriceContext = createContext<string | undefined>(undefined);
function OrderMarkDiff({ orderPrice }: { orderPrice: string }) {
  const mark = useContext(MarkPriceContext);
  return <>{priceDiffPercent(orderPrice, mark) ?? '--'}</>;
}
/** Click-to-copy for a price or quantity. The visible text stays the formatted value; the raw value is what gets copied. */
function Copyable({ value, children }: { value: string; children: React.ReactNode }) {
  const { message } = App.useApp();
  const [copied, setCopied] = useState(false);
  const timer = useRef<number | null>(null);
  useEffect(() => () => { if (timer.current !== null) window.clearTimeout(timer.current); }, []);
  return <button type="button" className={`copyable${copied ? ' copyable-copied' : ''}`} title={copied ? '已复制' : '点击复制'} aria-label={`复制 ${value}`} onClick={() => void copyText(value).then(ok => {
    if (ok) {
      setCopied(true);
      if (timer.current !== null) window.clearTimeout(timer.current);
      timer.current = window.setTimeout(() => setCopied(false), 1500);
      void message.success(`已复制 ${value}`);
    } else {
      void message.error('复制失败，请手动选择文本');
    }
  })}>
    {children}{copied ? <CheckOutlined aria-hidden="true" /> : <CopyOutlined aria-hidden="true" />}
  </button>;
}
function SymbolCell({ symbol }: { symbol: string }) {
  const inRouter = useInRouterContext();
  return inRouter ? <Link className="trade-symbol-link" to={`/pairs/${symbol}`}>{symbol}</Link> : <span>{symbol}</span>;
}
type SideFilter = 'ALL' | 'BUY' | 'SELL';
const sideOptions: { value: SideFilter; label: string }[] = [{ value: 'ALL', label: '全部' }, { value: 'BUY', label: '买入' }, { value: 'SELL', label: '卖出' }];
function TableFilterBar<T extends string>({ label, value, onChange, options, shown, total, onExport }: { label: string; value: T; onChange: (value: T) => void; options: { value: T; label: string }[]; shown: number; total: number; onExport: () => void }) {
  return <div className="table-toolbar">
    <Segmented<T> size="small" aria-label={label} value={value} onChange={onChange} options={options} />
    <span className="muted" role="status">显示 {shown} / {total} 条</span>
    <Button size="small" icon={<DownloadOutlined />} disabled={!shown} onClick={onExport}>导出 CSV</Button>
  </div>;
}
function ResponsiveTableSection({ hint, desktop, mobileClassName = 'table-mobile', hasRows, totalCount, emptyFilteredText, emptyAllText, listLabel, mobileRows, truncatedLimit, totalFilteredCount }: {
  hint: string;
  desktop: React.ReactNode;
  mobileClassName?: string;
  hasRows: boolean;
  totalCount: number;
  emptyFilteredText: string;
  emptyAllText: string;
  listLabel: string;
  mobileRows: React.ReactNode;
  truncatedLimit?: number;
  totalFilteredCount?: number;
}) {
  return <>
    <div className="table-desktop"><p className="table-scroll-hint">{hint}</p>{desktop}</div>
    <div className={mobileClassName}>
      {hasRows ? <ul className="trade-list" aria-label={listLabel}>{mobileRows}</ul> : <div className="trade-list-empty">{totalCount ? emptyFilteredText : emptyAllText}</div>}
      {truncatedLimit !== undefined && (totalFilteredCount ?? 0) > truncatedLimit && <p className="muted">仅显示最近 {truncatedLimit} 条，完整数据请导出 CSV。</p>}
    </div>
  </>;
}
const orderColumns: ColumnsType<Order> = [
  { title: '方向', dataIndex: 'side', render: side, width: 90, fixed: 'left' },
  { title: '价格', align: 'right', className: 'table-numeric', dataIndex: 'price', render: (v: string) => <Copyable value={v}>{price(v)}</Copyable>, sorter: (a, b) => Number(a.price) - Number(b.price) },
  { title: '距标记价格', align: 'right', className: 'table-numeric', render: (_, o) => <OrderMarkDiff orderPrice={o.price} /> },
  { title: '数量', align: 'right', className: 'table-numeric', dataIndex: 'quantity', render: quantity },
  { title: '金额', align: 'right', className: 'table-numeric', dataIndex: 'amount_usdc', render: (v: string) => number(v) },
  { title: '用途', render: (_, o) => orderPurpose(o) },
  { title: '状态', dataIndex: 'status' },
  { title: '时间（北京）', className: 'table-numeric', dataIndex: 'created_at', render: time, width: 185 },
];
const orderPagination = { pageSize: 15, showSizeChanger: false, hideOnSinglePage: true } as const;
const orderScroll = { x: 900 } as const;
const orderExpandable = {
  rowExpandable: (o: Order) => !!o.merge_sources?.length,
  expandedRowRender: (o: Order) => <ul className="order-merge-sources">{o.merge_sources.map(source => <li key={source.client_order_id}>{source.client_order_id} · {price(source.price)} × {quantity(source.quantity)}</li>)}</ul>,
};
const OrderMobileItem = memo(function OrderMobileItem({ order: o }: { order: Order }) {
  return <li className="trade-list-item">
    <div className="trade-list-heading">{side(o.side)}<strong><Copyable value={o.price}>{price(o.price)}</Copyable></strong><span className="trade-list-time">{orderPurpose(o)}</span></div>
    <dl>
      <div><dt>数量</dt><dd>{quantity(o.quantity)}</dd></div>
      <div><dt>金额</dt><dd>{number(o.amount_usdc)}</dd></div>
      <div><dt>距标记价</dt><dd><OrderMarkDiff orderPrice={o.price} /></dd></div>
    </dl>
    {o.merge_sources?.length > 0 && <ul className="order-merge-sources order-merge-sources-mobile">{o.merge_sources.map(source => <li key={source.client_order_id}>{source.client_order_id} · {price(source.price)} × {quantity(source.quantity)}</li>)}</ul>}
  </li>;
});
export const OrdersTable = memo(function OrdersTable({ orders, mark }: { orders: Order[]; mark?: string }) {
  const [filter, setFilter] = useState<SideFilter>('ALL');
  const rows = useMemo(() => filter === 'ALL' ? orders : orders.filter(order => order.side === filter), [orders, filter]);
  const exportRows = useCallback(() => downloadCsv(`orders-${beijingDay()}.csv`, toCsv(
    ['方向', '价格', '数量', '金额', '用途', '状态', '时间（北京）'],
    rows.map(o => [o.side === 'BUY' ? '买入' : '卖出', o.price, o.quantity, o.amount_usdc, orderPurpose(o), o.status, time(o.created_at)]),
  )), [rows]);
  const body = useMemo(() => <>
    <TableFilterBar label="按方向筛选" value={filter} onChange={setFilter} options={sideOptions} shown={rows.length} total={orders.length} onExport={exportRows} />
    <ResponsiveTableSection
      hint="左右滑动查看完整挂单信息"
      desktop={<Table<Order> rowKey="client_order_id" columns={orderColumns} dataSource={rows} size="small" scroll={orderScroll}
        pagination={orderPagination}
        expandable={orderExpandable} />}
      hasRows={rows.length > 0}
      totalCount={orders.length}
      emptyFilteredText="没有符合筛选条件的挂单"
      emptyAllText="暂无挂单"
      listLabel="挂单列表"
      mobileRows={rows.map(o => <OrderMobileItem key={o.client_order_id} order={o} />)}
    />
  </>, [filter, rows, orders.length, exportRows]);
  return <MarkPriceContext.Provider value={mark}>{body}</MarkPriceContext.Provider>;
});
const tradePagination = { pageSize: 15, showSizeChanger: false, hideOnSinglePage: true } as const;
const tradeScroll = { x: 1000 } as const;
export const TradesTable = memo(function TradesTable({ trades: allTrades, overview = false, now: currentNow }: { trades: Trade[]; overview?: boolean; now?: number }) {
  const [filter, setFilter] = useState<SideFilter>('ALL');
  const isNarrow = useNarrowViewport(overview);
  const trades = useMemo(() => overview || filter === 'ALL' ? allTrades : allTrades.filter(trade => trade.side === filter), [allTrades, filter, overview]);
  const exportRows = useCallback(() => downloadCsv(`trades-${beijingDay()}.csv`, toCsv(
    ['时间（北京）', '方向', '成交价格', '数量', '金额', '已实现盈亏', '手续费', '成交方式', '备注'],
    trades.map(t => [time(t.timestamp), t.side === 'BUY' ? '买入' : '卖出', t.price, t.quantity, t.amount_usdc, t.pnl_verified ? t.realized_pnl : '待同步', t.pnl_verified ? t.commission : '待同步', t.pnl_verified ? (t.is_maker ? 'Maker' : 'Taker') : '待同步', t.note]),
  )), [trades]);
  const columns = useMemo<ColumnsType<Trade>>(() => [
    ...(overview ? [{ title: '交易对', dataIndex: 'symbol', className: 'trade-symbol', width: 130, render: (symbol: string) => <SymbolCell symbol={symbol} /> }] : []),
    { title: '方向', dataIndex: 'side', render: side, width: 90, fixed: overview ? undefined : 'left' },
    { title: overview ? '成交时间' : '时间（北京）', className: 'table-numeric', dataIndex: 'timestamp', render: overview ? (value: string) => <RelativeTime value={value} /> : time, width: overview ? 90 : 185 },
    { title: '成交价格', align: 'right', className: 'table-numeric', dataIndex: 'price', render: (v: string) => overview ? price(v) : <Copyable value={v}>{price(v)}</Copyable> },
    { title: '数量', align: 'right', className: 'table-numeric', dataIndex: 'quantity', render: quantity },
    { title: '金额', align: 'right', className: 'table-numeric', dataIndex: 'amount_usdc', render: (v: string) => number(v) },
    { title: overview ? '盈亏' : '已实现盈亏', align: 'right', className: 'table-numeric', render: (_, t) => t.pnl_verified ? <Pnl value={t.realized_pnl} /> : <Tag>待同步</Tag> },
    { title: '手续费', align: 'right', className: 'table-numeric', render: (_, t) => t.pnl_verified ? number(t.commission, 4) : <Tag>待同步</Tag> },
    { title: '成交方式', render: (_, t) => t.pnl_verified ? (t.is_maker ? 'Maker' : 'Taker') : <Tag>待同步</Tag> },
    { title: '备注', dataIndex: 'note', ellipsis: true },
  ], [overview]);
  const body = useMemo(() => <div>
    {!overview && <TableFilterBar label="按方向筛选" value={filter} onChange={setFilter} options={sideOptions} shown={trades.length} total={allTrades.length} onExport={exportRows} />}
    <ResponsiveTableSection
      hint="左右滑动查看完整成交信息"
      desktop={<Table<Trade> rowKey="trade_id" columns={columns} dataSource={trades} size="small" scroll={tradeScroll} pagination={overview ? false : tradePagination} />}
      mobileClassName={overview ? 'overview-trades-mobile' : 'table-mobile'}
      hasRows={trades.length > 0}
      totalCount={overview ? 0 : allTrades.length}
      emptyFilteredText="没有符合筛选条件的成交记录"
      emptyAllText="暂无成交记录"
      listLabel="成交列表"
      truncatedLimit={overview ? undefined : 50}
      totalFilteredCount={trades.length}
      mobileRows={(overview ? trades : trades.slice(0, 50)).map(trade => <li className="trade-list-item" key={trade.trade_id}>
        <div className="trade-list-heading">{overview ? <><strong><SymbolCell symbol={trade.symbol} /></strong>{side(trade.side)}</> : <>{side(trade.side)}<strong><Copyable value={trade.price}>{price(trade.price)}</Copyable></strong></>}<RelativeTime value={trade.timestamp} className="trade-list-time" /></div>
        <dl>
          <div><dt>{overview ? '成交均价' : '数量'}</dt><dd>{overview ? price(trade.price) : quantity(trade.quantity)}</dd></div>
          <div><dt>金额</dt><dd>{number(trade.amount_usdc)}</dd></div>
          <div><dt>盈亏</dt><dd>{trade.pnl_verified ? <Pnl value={trade.realized_pnl} /> : <Tag>待同步</Tag>}</dd></div>
        </dl>
      </li>)}
    />
  </div>, [overview, filter, trades, allTrades.length, exportRows, columns]);
  return <NowProvider currentNow={currentNow} enabled={trades.length > 0 && (overview || isNarrow)}>{body}</NowProvider>;
});
const levels = ['ALL', 'ERROR', 'WARN', 'INFO', 'DEBUG'] as const;
type LevelFilter = typeof levels[number];
const levelOptions: { value: LevelFilter; label: string }[] = levels.map(value => ({ value, label: value === 'ALL' ? '全部' : value }));
const logColumns: ColumnsType<Log> = [
  { title: '时间（北京）', className: 'table-numeric', dataIndex: 'timestamp', render: time, width: 185 },
  { title: '级别', dataIndex: 'level', width: 90, render: v => <span className={`log-level log-level-${String(v).toLowerCase()}`}>{v}</span> },
  { title: '内容', dataIndex: 'message', className: 'log-message' },
];
const logPagination = { pageSize: 30, showSizeChanger: false, hideOnSinglePage: true } as const;
const logScroll = { x: 600 } as const;
const logRowClassName = (l: Log) => l.level === 'ERROR' ? 'log-row-error' : l.level === 'WARN' ? 'log-row-warn' : '';
export const LogsTable = memo(function LogsTable({ logs }: { logs: Log[] }) {
  const [level, setLevel] = useState<LevelFilter>('ALL');
  const rows = useMemo(() => {
    const filtered = level === 'ALL' ? logs : logs.filter(log => log.level === level);
    const counts = new Map<string, number>();
    const keyed = new Array<Log & { key: string }>(filtered.length);
    for (let i = filtered.length - 1; i >= 0; i--) {
      const log = filtered[i];
      const base = `${log.timestamp}\0${log.level}\0${log.message}`;
      const sequence = counts.get(base) ?? 0;
      counts.set(base, sequence + 1);
      keyed[i] = { ...log, key: `${base}-${sequence}` };
    }
    return keyed;
  }, [logs, level]);
  const exportRows = () => downloadCsv(`logs-${beijingDay()}.csv`, toCsv(['时间（北京）', '级别', '内容'], rows.map(l => [time(l.timestamp), l.level, l.message])));
  return <>
    <TableFilterBar label="按级别筛选" value={level} onChange={setLevel} options={levelOptions} shown={rows.length} total={logs.length} onExport={exportRows} />
    <ResponsiveTableSection
      hint="左右滑动查看完整日志"
      desktop={<Table<Log> rowKey="key" rowClassName={logRowClassName} dataSource={rows} size="small" scroll={logScroll} pagination={logPagination} columns={logColumns} />}
      hasRows={rows.length > 0}
      totalCount={logs.length}
      emptyFilteredText="没有符合筛选条件的日志"
      emptyAllText="暂无日志"
      listLabel="日志列表"
      truncatedLimit={100}
      totalFilteredCount={rows.length}
      mobileRows={rows.slice(0, 100).map(l => <li className={`trade-list-item log-item-${l.level.toLowerCase()}`} key={l.key}>
        <div className="trade-list-heading"><span className={`log-level log-level-${l.level.toLowerCase()}`}>{l.level}</span><span className="trade-list-time">{time(l.timestamp)}</span></div>
        <p className="log-message">{l.message}</p>
      </li>)}
    />
  </>;
});
