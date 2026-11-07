import { theme } from 'antd';
import { Bar, BarChart, CartesianGrid, Legend, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import type { Snapshot } from '../lib/types';
import { price, todayCount } from '../lib/format';
import { priceDiffPercent } from '../lib/gridPreview';
import { useNow } from '../lib/useNow';
import { NoData } from './Common';

export function todayHourlyData(stats: NonNullable<Snapshot['hourly_trade_stats']>, now = Date.now()) {
  const start = Date.parse(stats.window_start);
  const currentHour = Math.min(23, Math.max(0, Math.floor((now - start) / 3600_000)));
  const buckets = new Map(stats.buckets.map(bucket => [Date.parse(bucket.hour_start), bucket]));
  return Array.from({ length: 24 }, (_, index) => {
    const hour = (currentHour - index + 24) % 24;
    const bucket = buckets.get(start + hour * 3600_000);
    return { hour: `${String(hour).padStart(2, '0')}:00`, 买入: bucket?.buy_count ?? null, 卖出: bucket?.sell_count ?? null };
  });
}

export function HourlyChart({ snapshot }: { snapshot: Snapshot }) {
  const { token } = theme.useToken();
  const now = useNow();
  if (todayCount(snapshot, now) === null) return <NoData />;
  const data = todayHourlyData(snapshot.hourly_trade_stats!, now);
  return <>
    <div className="muted chart-caption">北京时间 · 最近时段在左侧 · 可横向滚动查看</div>
    <div className="hourly-chart-scroll" role="region" tabIndex={0} aria-label="北京时间今日每小时成交笔数，可横向滚动"><div className="chart hourly-chart"><ResponsiveContainer width="100%" height={270}>
      <BarChart data={data} barGap={4} margin={{ top: 16, right: 12, bottom: 4, left: -12 }}>
        <CartesianGrid strokeDasharray="3 5" vertical={false} stroke={token.colorBorderSecondary} />
        <XAxis dataKey="hour" interval={0} tickLine={false} axisLine={false} tick={{ fontSize: 11, fontFamily: 'ui-monospace, "SF Mono", Menlo, Consolas, monospace', fill: token.colorTextTertiary }} />
        <YAxis allowDecimals={false} axisLine={false} tickLine={false} tick={{ fontSize: 11, fontFamily: 'ui-monospace, "SF Mono", Menlo, Consolas, monospace', fill: token.colorTextTertiary }} />
        <Legend /><Tooltip cursor={{ fill: token.colorFillSecondary }} contentStyle={{ background: token.colorBgElevated, borderColor: token.colorBorder, borderRadius: 8, color: token.colorText }} labelStyle={{ color: token.colorText }} />
        <Bar dataKey="买入" fill={token.colorSuccess} radius={[3, 3, 0, 0]} /><Bar dataKey="卖出" fill={token.colorError} radius={[3, 3, 0, 0]} />
      </BarChart>
    </ResponsiveContainer></div></div>
  </>;
}
export function DepthChart({ snapshot }: { snapshot: Snapshot }) {
  const orders = snapshot.active_orders;
  if (!orders.length) return <NoData />;
  const mark = Number(snapshot.ticker.mark_price);
  const hasMark = Number.isFinite(mark) && mark > 0;
  const entries = orders.map(order => ({ kind: 'order' as const, value: Number(order.price), order }));
  const rows: ({ kind: 'order'; value: number; order: typeof orders[number] } | { kind: 'mark'; value: number })[] = entries;
  if (hasMark) rows.push({ kind: 'mark', value: mark });
  rows.sort((a, b) => b.value - a.value);
  return <div className="grid-levels" role="region" aria-label="按价格从高到低排列的实时网格挂单" tabIndex={0}>
    {rows.map(row => {
      if (row.kind === 'mark') return <div className="grid-level grid-level-mark" key="mark"><span>标记价</span><span className="grid-level-line" /><strong>{price(row.value)}</strong></div>;
      const diff = hasMark ? priceDiffPercent(row.order.price, snapshot.ticker.mark_price) : null;
      return <div className={`grid-level grid-level-${row.order.side.toLowerCase()}`} key={row.order.client_order_id}>
        <span>{row.order.side === 'BUY' ? '买单' : '卖单'} <span className="grid-level-quantity">{price(row.order.quantity)}</span></span><span className="grid-level-line" />{diff && <span className="grid-level-diff">{diff}</span>}<strong>{price(row.order.price)}</strong>
      </div>;
    })}
  </div>;
}
