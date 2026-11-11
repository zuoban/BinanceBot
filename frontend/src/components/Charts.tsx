import { memo, useMemo, useRef } from 'react';
import { theme } from 'antd';
import { Bar, BarChart, CartesianGrid, Legend, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import type { Snapshot } from '../lib/types';
import { numeric, price, todayCount } from '../lib/format';
import { priceDiffPercent } from '../lib/gridPreview';
import { useNow } from '../lib/useNow';
import { NoData, useCenterElementScroll } from './Common';
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

export const HourlyChart = memo(function HourlyChart({ snapshot, now: currentNow }: { snapshot: Snapshot; now?: number }) {
  const fallbackNow = useNow(1000, currentNow === undefined);
  const now = currentNow ?? fallbackNow;
  return <HourlyChartContent stats={snapshot.hourly_trade_stats} hourBucket={Math.floor(now / 3600_000)} available={todayCount(snapshot, now) !== null} />;
});

const HourlyChartContent = memo(function HourlyChartContent({ stats, hourBucket, available }: { stats: Snapshot['hourly_trade_stats']; hourBucket: number; available: boolean }) {
  const { token } = theme.useToken();
  const data = useMemo(() => stats ? todayHourlyData(stats, hourBucket * 3600_000) : [], [stats, hourBucket]);
  const axisTick = useMemo(() => ({ fontSize: 12, fontFamily: 'var(--font-mono)', fill: token.colorTextTertiary }), [token.colorTextTertiary]);
  if (!available) return <NoData />;
  return <>
    <div className="muted chart-caption">北京时间 · 最近时段在左侧 · 选择柱形查看每小时明细</div>
    <div className="chart hourly-chart" role="region" aria-label="北京时间今日每小时成交笔数"><ResponsiveContainer width="100%" height={270}>
      <BarChart accessibilityLayer data={data} barGap={2} margin={{ top: 16, right: 12, bottom: 4, left: -12 }}>
        <CartesianGrid strokeDasharray="3 5" vertical={false} stroke={token.colorBorderSecondary} />
        <XAxis dataKey="hour" interval="preserveStartEnd" minTickGap={24} tickLine={false} axisLine={false} tick={axisTick} />
        <YAxis allowDecimals={false} axisLine={false} tickLine={false} tick={axisTick} />
        <Legend /><Tooltip cursor={{ fill: token.colorFillSecondary }} contentStyle={{ background: token.colorBgElevated, borderColor: token.colorBorder, borderRadius: 8, boxShadow: 'var(--overlay-shadow)', color: token.colorText, fontSize: 12 }} labelStyle={{ color: token.colorText, fontFamily: 'var(--font-mono)', fontWeight: 600, marginBottom: 4 }} itemStyle={{ fontFamily: 'var(--font-mono)' }} />
        <Bar dataKey="买入" fill={token.colorSuccess} radius={[3, 3, 0, 0]} /><Bar dataKey="卖出" fill={token.colorError} radius={[3, 3, 0, 0]} />
      </BarChart>
    </ResponsiveContainer></div>
  </>;
});
export const DepthChart = memo(function DepthChart({ snapshot }: { snapshot: Snapshot }) {
  const orders = snapshot.active_orders;
  const containerRef = useRef<HTMLDivElement>(null);
  const mark = numeric(snapshot.ticker.mark_price);
  const hasMark = mark !== null && mark > 0;
  useCenterElementScroll(containerRef, '.grid-level-mark', [snapshot.symbol, orders.length, hasMark]);
  const { buyCount, sellCount, orderRows } = useMemo(() => {
    let buy = 0, sell = 0;
    for (const order of orders) {
      if (order.side === 'BUY') buy++;
      else sell++;
    }
    const sorted = orders
      .map(order => ({ kind: 'order' as const, value: numeric(order.price) ?? -Infinity, order }))
      .sort((a, b) => b.value - a.value);
    return { buyCount: buy, sellCount: sell, orderRows: sorted };
  }, [orders]);
  const rows = useMemo(() => {
    if (!hasMark) return orderRows;
    const result: ({ kind: 'order'; value: number; order: typeof orders[number] } | { kind: 'mark'; value: number })[] = [...orderRows];
    const insertIdx = result.findIndex(r => r.value < mark);
    if (insertIdx === -1) result.push({ kind: 'mark', value: mark });
    else result.splice(insertIdx, 0, { kind: 'mark', value: mark });
    return result;
  }, [orderRows, hasMark, mark]);
  if (!orders.length) return <NoData />;
  return <>
    <div className="muted chart-caption">买单 {buyCount} 笔 · 卖单 {sellCount} 笔</div>
    <div ref={containerRef} className="grid-levels" role="region" aria-label="按价格从高到低排列的实时网格挂单" tabIndex={0}>
      {rows.map(row => {
        if (row.kind === 'mark') return <div className="grid-level grid-level-mark" key="mark"><span>标记价</span><span className="grid-level-line" /><strong>{price(row.value)}</strong></div>;
        const diff = hasMark ? priceDiffPercent(row.order.price, snapshot.ticker.mark_price) : null;
        return <div className={`grid-level grid-level-${row.order.side.toLowerCase()}`} key={row.order.client_order_id}>
          <span>{row.order.side === 'BUY' ? '买单' : '卖单'} <span className="grid-level-quantity">{price(row.order.quantity)}</span></span><span className="grid-level-line" />{diff && <span className="grid-level-diff">{diff}</span>}<strong>{price(row.order.price)}</strong>
        </div>;
      })}
    </div>
  </>;
});
