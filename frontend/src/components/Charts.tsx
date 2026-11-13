import { createContext, Suspense, lazy, memo, useContext, useMemo, useRef } from 'react';
import type { Snapshot } from '../lib/types';
import { compareSignedDecimals, parseSignedDecimal } from '../lib/decimal';
import { isValidEpochMs, numeric, price, quantity } from '../lib/format';
import { todayCount } from '../lib/portfolio';
import { priceDiffPercent } from '../lib/gridPreview';
import { serverAlignedNow, useUtcHour } from '../lib/useNow';
import { NoData, useCenterElementScroll } from './Common';
import type { HourlyChartPoint } from './HourlyChartView';
import { LoadErrorBoundary } from './LoadErrorBoundary';

const HourlyChartView = lazy(() => import('./HourlyChartView'));

export function todayHourlyData(stats: NonNullable<Snapshot['hourly_trade_stats']>, now = Date.now()): HourlyChartPoint[] {
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
  const data = useMemo(() => stats ? todayHourlyData(stats, hourBucket * 3600_000) : [], [stats, hourBucket]);
  if (!available) return <NoData />;
  return <>
    <div className="chart-caption-row">
      <span className="muted">北京时间 · 最近时段在左侧</span>
      <span className="chart-legend" aria-hidden="true"><span className="chart-legend-item"><i className="chart-dot chart-dot-buy" />买入</span><span className="chart-legend-item"><i className="chart-dot chart-dot-sell" />卖出</span></span>
    </div>
    <div className="chart" role="region" aria-label="北京时间今日每小时成交笔数">
      <LoadErrorBoundary label="今日成交图">
        <Suspense fallback={<div className="history-empty history-plot-loading" role="status">正在加载图表…</div>}>
          <HourlyChartView data={data} />
        </Suspense>
      </LoadErrorBoundary>
    </div>
  </>;
}, (prev, next) => prev.available === next.available && prev.hourBucket === next.hourBucket && sameHourlyStats(prev.stats, next.stats));
function sameDepthOrders(a: Snapshot['active_orders'], b: Snapshot['active_orders']) {
  if (a === b) return true;
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    const left = a[i], right = b[i];
    if (
      left.client_order_id !== right.client_order_id ||
      left.side !== right.side ||
      left.price !== right.price ||
      left.quantity !== right.quantity
    ) return false;
  }
  return true;
}

const DepthMarkPriceContext = createContext<string | null>(null);

const DepthOrderDiff = memo(function DepthOrderDiff({ orderPrice }: { orderPrice: string }) {
  const markPrice = useContext(DepthMarkPriceContext);
  const diff = markPrice ? priceDiffPercent(orderPrice, markPrice) : null;
  return diff ? <span className="grid-level-diff">{diff}</span> : null;
});

const DepthOrderRow = memo(function DepthOrderRow({
  side,
  orderPrice,
  priceText,
  qtyText,
}: {
  side: 'BUY' | 'SELL';
  orderPrice: string;
  priceText: string;
  qtyText: string;
}) {
  return (
    <div className={`grid-level grid-level-${side.toLowerCase()}`}>
      <span>{side === 'BUY' ? '买单' : '卖单'} <span className="grid-level-quantity">{qtyText}</span></span>
      <span className="grid-level-line" />
      <DepthOrderDiff orderPrice={orderPrice} />
      <strong>{priceText}</strong>
    </div>
  );
});

export const DepthChart = memo(function DepthChart({ snapshot }: { snapshot: Snapshot }) {
  const stableOrdersRef = useRef(snapshot.active_orders);
  if (!sameDepthOrders(stableOrdersRef.current, snapshot.active_orders)) {
    stableOrdersRef.current = snapshot.active_orders;
  }
  const orders = stableOrdersRef.current;
  const containerRef = useRef<HTMLDivElement>(null);
  const markPrice = snapshot.ticker.mark_price;
  const parsedMark = parseSignedDecimal(markPrice);
  const mark = numeric(markPrice);
  const hasMark = parsedMark ? parsedMark.n > 0n : mark !== null && mark > 0;
  useCenterElementScroll(containerRef, '.grid-level-mark', `${snapshot.symbol}:${orders.length}:${hasMark ? 1 : 0}`);
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
  const insertIdx = hasMark ? orderRows.findIndex(r => compareSignedDecimals(r.order.price, markPrice) < 0) : -2;
  const rows = useMemo(() => {
    if (insertIdx === -2) return orderRows;
    const result: (typeof orderRows[number] | { kind: 'mark' })[] = [...orderRows];
    if (insertIdx === -1) result.push({ kind: 'mark' });
    else result.splice(insertIdx, 0, { kind: 'mark' });
    return result;
  }, [orderRows, insertIdx]);
  const markPriceText = hasMark ? price(markPrice) : null;
  if (!orders.length) return <NoData />;
  return <>
    <div className="muted chart-caption">买单 {buyCount} 笔 · 卖单 {sellCount} 笔</div>
    <DepthMarkPriceContext.Provider value={hasMark ? markPrice : null}>
      <div ref={containerRef} className="grid-levels" role="region" aria-label="按价格从高到低排列的实时网格挂单" tabIndex={0}>
        {rows.map(row => {
          if (row.kind === 'mark') {
            return <div className="grid-level grid-level-mark" key="mark"><span>标记价</span><span className="grid-level-line" /><strong>{markPriceText}</strong></div>;
          }
          return (
            <DepthOrderRow
              key={row.order.client_order_id}
              side={row.order.side}
              orderPrice={row.order.price}
              priceText={row.priceText}
              qtyText={row.qtyText}
            />
          );
        })}
      </div>
    </DepthMarkPriceContext.Provider>
  </>;
}, (prev, next) =>
  prev.snapshot.symbol === next.snapshot.symbol &&
  prev.snapshot.ticker.mark_price === next.snapshot.ticker.mark_price &&
  sameDepthOrders(prev.snapshot.active_orders, next.snapshot.active_orders),
);
