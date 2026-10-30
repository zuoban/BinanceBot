import { Bar, BarChart, CartesianGrid, Legend, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import type { Snapshot } from '../lib/types';
import { price, todayCount } from '../lib/format';
import { NoData } from './Common';

export function todayHourlyData(stats: NonNullable<Snapshot['hourly_trade_stats']>, now = Date.now()) {
  const start = Date.parse(stats.window_start);
  const currentHour = Math.floor((now - start) / 3600_000);
  const buckets = new Map(stats.buckets.map(bucket => [Date.parse(bucket.hour_start), bucket]));
  return Array.from({ length: 24 }, (_, index) => {
    const hour = (currentHour - index + 24) % 24;
    const bucket = buckets.get(start + hour * 3600_000);
    return { hour: `${String(hour).padStart(2, '0')}:00`, 买入: bucket?.buy_count ?? null, 卖出: bucket?.sell_count ?? null };
  });
}

export function HourlyChart({ snapshot }: { snapshot: Snapshot }) {
  if (todayCount(snapshot) === null) return <NoData />;
  const data = todayHourlyData(snapshot.hourly_trade_stats!);
  return <div className="hourly-chart-scroll" role="region" tabIndex={0} aria-label="北京时间今日每小时成交笔数，可横向滚动"><div className="chart hourly-chart"><ResponsiveContainer width="100%" height={270}>
    <BarChart data={data} barGap={3} margin={{ top: 15, right: 5, bottom: 5, left: -20 }}>
      <CartesianGrid strokeDasharray="3 5" vertical={false} stroke="#e7ece9" /><XAxis dataKey="hour" interval={0} tickLine={false} axisLine={false} tick={{ fontSize: 11 }} /><YAxis allowDecimals={false} axisLine={false} tickLine={false} />
      <Legend /><Tooltip cursor={{ fill: '#f3f6f4' }} /><Bar dataKey="买入" fill="#17745e" radius={[3, 3, 0, 0]} /><Bar dataKey="卖出" fill="#d2825c" radius={[3, 3, 0, 0]} />
    </BarChart>
  </ResponsiveContainer></div></div>;
}
export function DepthChart({ snapshot }: { snapshot: Snapshot }) {
  const orders = [...snapshot.active_orders].sort((a, b) => Number(b.price) - Number(a.price));
  if (!orders.length) return <NoData />;
  const mark = Number(snapshot.ticker.mark_price);
  const entries = orders.map(order => ({ kind: 'order' as const, value: Number(order.price), order }));
  const rows: ({ kind: 'order'; value: number; order: typeof orders[number] } | { kind: 'mark'; value: number })[] = entries;
  if (Number.isFinite(mark) && mark > 0) rows.push({ kind: 'mark', value: mark });
  rows.sort((a, b) => b.value - a.value);
  return <div className="grid-levels" role="region" aria-label="按价格从高到低排列的实时网格挂单" tabIndex={0}>
    {rows.map(row => row.kind === 'mark'
      ? <div className="grid-level grid-level-mark" key="mark"><span>标记价</span><span className="grid-level-line" /><strong>{price(row.value)}</strong></div>
      : <div className={`grid-level grid-level-${row.order.side.toLowerCase()}`} key={row.order.client_order_id}>
        <span>{row.order.side === 'BUY' ? '买单' : '卖单'} <span className="grid-level-quantity">{price(row.order.quantity)}</span></span><span className="grid-level-line" /><strong>{price(row.order.price)}</strong>
      </div>)}
  </div>;
}
