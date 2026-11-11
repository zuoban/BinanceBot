import { memo, useMemo, useRef } from 'react';
import { theme } from 'antd';
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import type { Snapshot } from '../lib/types';
import { compareSignedDecimals, parseSignedDecimal } from '../lib/decimal';
import { isValidEpochMs, numeric, price, quantity } from '../lib/format';
import { todayCount } from '../lib/portfolio';
import { priceDiffPercent } from '../lib/gridPreview';
import { serverAlignedNow, useUtcHour } from '../lib/useNow';
import { NoData, useCenterElementScroll } from './Common';
export function todayHourlyData(stats: NonNullable<Snapshot['hourly_trade_stats']>, now = Date.now()) {
  const start = Date.parse(stats.window_start);
  if (!isValidEpochMs(start) || !isValidEpochMs(now)) return [];
  const currentHour = Math.min(23, Math.max(0, Math.floor((now - start) / 3600_000)));
  const buckets = new Map<number, (typeof stats.buckets)[number]>();
  for (const bucket of stats.buckets) {
    const ts = Date.parse(bucket.hour_start);
    if (isValidEpochMs(ts)) buckets.set(ts, bucket);
  }
  return Array.from({ length: 24 }, (_, index) => {
    const hour = (currentHour - index + 24) % 24;
    const bucket = buckets.get(start + hour * 3600_000);
    return { hour: `${String(hour).padStart(2, '0')}:00`, 买入: bucket?.buy_count ?? null, 卖出: bucket?.sell_count ?? null };
  });
}

const barChartMargin = { top: 12, right: 12, bottom: 4, left: -12 };
const barRadius: [number, number, number, number] = [2, 2, 0, 0];

function sameHourlyStats(a: Snapshot['hourly_trade_stats'], b: Snapshot['hourly_trade_stats']) {
  if (a === b) return true;
  if (!a || !b || a.window_start !== b.window_start || a.buckets.length !== b.buckets.length) return false;
  for (let i = 0; i < a.buckets.length; i++) {
    const left = a.buckets[i], right = b.buckets[i];
    if (left.hour_start !== right.hour_start || left.buy_count !== right.buy_count || left.sell_count !== right.sell_count) return false;
  }
  return true;
}

export const HourlyChart = memo(function HourlyChart({ snapshot, receivedAt, now: currentNow }: { snapshot: Snapshot; receivedAt?: number; now?: number }) {
  const liveHour = useUtcHour(currentNow === undefined, snapshot.generated_at, receivedAt);
  const now = currentNow ?? serverAlignedNow(snapshot.generated_at, receivedAt) ?? Date.now();
  const hourBucket = currentNow === undefined ? liveHour : Math.floor(currentNow / 3_600_000);
  return <HourlyChartContent stats={snapshot.hourly_trade_stats} hourBucket={hourBucket} available={todayCount(snapshot, now) !== null} />;
});

const HourlyChartContent = memo(function HourlyChartContent({ stats, hourBucket, available }: { stats: Snapshot['hourly_trade_stats']; hourBucket: number; available: boolean }) {
  const { token } = theme.useToken();
  const data = useMemo(() => stats ? todayHourlyData(stats, hourBucket * 3600_000) : [], [stats, hourBucket]);
  const axisTick = useMemo(() => ({ fontSize: 12, fontFamily: 'var(--font-mono)', fill: token.colorTextTertiary }), [token.colorTextTertiary]);
  const tooltipCursor = useMemo(() => ({ fill: token.colorFillSecondary }), [token.colorFillSecondary]);
  const tooltipContentStyle = useMemo(() => ({ background: token.colorBgElevated, borderColor: token.colorBorder, borderRadius: 8, boxShadow: 'var(--overlay-shadow)', color: token.colorText, fontSize: 12 }), [token.colorBgElevated, token.colorBorder, token.colorText]);
  const tooltipLabelStyle = useMemo(() => ({ color: token.colorText, fontFamily: 'var(--font-mono)', fontWeight: 500, marginBottom: 4 }), [token.colorText]);
  const tooltipItemStyle = useMemo(() => ({ fontFamily: 'var(--font-mono)' }), []);
  if (!available) return <NoData />;
  return <>
    <div className="chart-caption-row">
      <span className="muted">北京时间 · 最近时段在左侧</span>
      <span className="chart-legend" aria-hidden="true"><span className="chart-legend-item"><i className="chart-dot chart-dot-buy" />买入</span><span className="chart-legend-item"><i className="chart-dot chart-dot-sell" />卖出</span></span>
    </div>
    <div className="chart" role="region" aria-label="北京时间今日每小时成交笔数"><ResponsiveContainer width="100%" height={264}>
      <BarChart accessibilityLayer data={data} barGap={2} maxBarSize={12} margin={barChartMargin}>
        <CartesianGrid vertical={false} stroke={token.colorBorderSecondary} />
        <XAxis dataKey="hour" interval="preserveStartEnd" minTickGap={24} tickLine={false} axisLine={false} tick={axisTick} />
        <YAxis allowDecimals={false} axisLine={false} tickLine={false} tick={axisTick} />
        <Tooltip cursor={tooltipCursor} contentStyle={tooltipContentStyle} labelStyle={tooltipLabelStyle} itemStyle={tooltipItemStyle} />
        <Bar dataKey="买入" fill={token.colorSuccess} radius={barRadius} isAnimationActive={false} /><Bar dataKey="卖出" fill={token.colorError} radius={barRadius} isAnimationActive={false} />
      </BarChart>
    </ResponsiveContainer></div>
  </>;
}, (prev, next) => prev.available === next.available && prev.hourBucket === next.hourBucket && sameHourlyStats(prev.stats, next.stats));
export const DepthChart = memo(function DepthChart({ snapshot }: { snapshot: Snapshot }) {
  const orders = snapshot.active_orders;
  const containerRef = useRef<HTMLDivElement>(null);
  const markPrice = snapshot.ticker.mark_price;
  const parsedMark = parseSignedDecimal(markPrice);
  const mark = numeric(markPrice);
  const hasMark = parsedMark ? parsedMark.n > 0n : mark !== null && mark > 0;
  useCenterElementScroll(containerRef, '.grid-level-mark', [snapshot.symbol, orders.length, hasMark]);
  const { buyCount, sellCount, orderRows } = useMemo(() => {
    let buy = 0, sell = 0;
    for (const order of orders) {
      if (order.side === 'BUY') buy++;
      else sell++;
    }
    const sorted = orders
      .map(order => ({ kind: 'order' as const, order, priceText: price(order.price), qtyText: quantity(order.quantity) }))
      .sort((a, b) => compareSignedDecimals(b.order.price, a.order.price));
    return { buyCount: buy, sellCount: sell, orderRows: sorted };
  }, [orders]);
  const rows = useMemo(() => {
    if (!hasMark) return orderRows;
    const result: (typeof orderRows[number] | { kind: 'mark'; priceText: string })[] = [...orderRows];
    const insertIdx = orderRows.findIndex(r => compareSignedDecimals(r.order.price, markPrice) < 0);
    const markRow = { kind: 'mark' as const, priceText: price(markPrice) };
    if (insertIdx === -1) result.push(markRow);
    else result.splice(insertIdx, 0, markRow);
    return result;
  }, [orderRows, hasMark, markPrice]);
  if (!orders.length) return <NoData />;
  return <>
    <div className="muted chart-caption">买单 {buyCount} 笔 · 卖单 {sellCount} 笔</div>
    <div ref={containerRef} className="grid-levels" role="region" aria-label="按价格从高到低排列的实时网格挂单" tabIndex={0}>
      {rows.map(row => {
        if (row.kind === 'mark') return <div className="grid-level grid-level-mark" key="mark"><span>标记价</span><span className="grid-level-line" /><strong>{row.priceText}</strong></div>;
        const diff = hasMark ? priceDiffPercent(row.order.price, markPrice) : null;
        return <div className={`grid-level grid-level-${row.order.side.toLowerCase()}`} key={row.order.client_order_id}>
          <span>{row.order.side === 'BUY' ? '买单' : '卖单'} <span className="grid-level-quantity">{row.qtyText}</span></span><span className="grid-level-line" />{diff && <span className="grid-level-diff">{diff}</span>}<strong>{row.priceText}</strong>
        </div>;
      })}
    </div>
  </>;
});
