import { memo, useId } from 'react';
import { Area, ComposedChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';

type HistoryPoint = { hour: number; equity: number | null };
const hourFormat = new Intl.DateTimeFormat('zh-CN', { timeZone: 'Asia/Shanghai', hour: '2-digit', minute: '2-digit', hour12: false });
const dayFormat = new Intl.DateTimeFormat('zh-CN', { timeZone: 'Asia/Shanghai', month: '2-digit', day: '2-digit' });
const wanFormat = new Intl.NumberFormat('en-US', { maximumFractionDigits: 6 });
const formatHour = (hour: number) => hourFormat.format(hour * 1000);
const formatDay = (hour: number) => dayFormat.format(hour * 1000);
const axisTick = { fontSize: 11, fontFamily: 'var(--font-mono)', fill: 'var(--text-muted)' };
const tooltipCursor = { stroke: 'var(--border-strong)', strokeDasharray: '3 4' };
const tooltipContentStyle = { background: 'var(--bg-raised)', borderColor: 'var(--border-strong)', borderRadius: 8, boxShadow: 'var(--overlay-shadow)', color: 'var(--text-primary)', fontSize: 12 };
const tooltipLabelStyle = { color: 'var(--text-primary)', fontFamily: 'var(--font-mono)', fontWeight: 600, marginBottom: 4 };
const tooltipItemStyle = { color: 'var(--text-primary)', fontFamily: 'var(--font-mono)' };
const dailyMargin = { top: 12, right: 16, bottom: 0, left: 4 };
const hourlyMargin = { top: 12, right: 12, bottom: 0, left: 4 };
const dailyActiveDot = { r: 4, fill: 'var(--text-secondary)' };
const hourlyActiveDot = { r: 4, fill: 'var(--history-equity)' };
function HistoryDot({ cx, cy, payload, stroke, index, data, discrete }: { cx?: number; cy?: number; payload?: { hour: number }; stroke?: string; index?: number; data: HistoryPoint[]; discrete: boolean }) {
  if (cx == null || cy == null || !payload || (index != null && data[index]?.equity == null)) return null;
  const isolated = discrete || (index != null && data[index - 1]?.equity == null && data[index + 1]?.equity == null);
  return <circle cx={cx} cy={cy} r={2} fill={discrete ? (stroke ?? 'var(--history-equity)') : stroke} className={payload.hour % (4 * 3600) === 0 || isolated ? undefined : 'history-point-hourly'} />;
}

export default memo(function OverviewHistoryChart({ data, ticks, asset, formatValue, formatDate, daily = false, discrete = false }: {
  data: HistoryPoint[];
  ticks: number[];
  asset: string | undefined;
  formatValue: (value: unknown) => string;
  formatDate: (hour: number) => string;
  daily?: boolean;
  discrete?: boolean;
}) {
  const rawId = useId().replace(/[^a-zA-Z0-9_-]/g, '');
  const fillId = `equityAreaFill-${rawId}`;
  const useWan = data.some(point => point.equity !== null && Math.abs(point.equity) >= 100_000);
  const axisValue = (value: number) => useWan ? `${wanFormat.format(value / 10000)}万` : formatValue(value);
  const lineColor = daily ? 'var(--text-secondary)' : 'var(--history-equity)';
  const chartHeight = daily ? 196 : 236;
  return <ResponsiveContainer width="100%" height={chartHeight}>
    <ComposedChart accessibilityLayer data={data} margin={daily ? dailyMargin : hourlyMargin}>
      {!daily && !discrete && <defs>
        <linearGradient id={fillId} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="var(--history-equity)" stopOpacity={0.18} />
          <stop offset="100%" stopColor="var(--history-equity)" stopOpacity={0} />
        </linearGradient>
      </defs>}
      <CartesianGrid vertical={false} stroke="var(--border-hairline)" />
      <XAxis dataKey="hour" ticks={ticks} tickFormatter={daily ? formatDay : formatHour} tickLine={false} axisLine={false} interval={daily ? 'equidistantPreserveEnd' : 'preserveEnd'} minTickGap={daily ? 24 : 12} tick={axisTick} />
      <YAxis width="auto" domain={['auto', 'auto']} tickFormatter={axisValue} tickLine={false} axisLine={false} tick={axisTick} tickCount={daily ? 3 : 4} />
      <Tooltip labelFormatter={(value: unknown) => `${formatDate(Number(value))} · 北京时间`} formatter={(value: unknown) => `${formatValue(value)} ${asset ?? ''}`} cursor={tooltipCursor} contentStyle={tooltipContentStyle} labelStyle={tooltipLabelStyle} itemStyle={tooltipItemStyle} />
      <Area type="monotone" name="账户权益" dataKey="equity" stroke={discrete ? 'none' : lineColor} strokeWidth={daily ? 1.5 : 2} fill={!daily && !discrete ? `url(#${fillId})` : 'none'} dot={<HistoryDot data={data} discrete={discrete} stroke={lineColor} />} activeDot={daily ? dailyActiveDot : hourlyActiveDot} connectNulls={false} isAnimationActive={false} />
    </ComposedChart>
  </ResponsiveContainer>;
});
