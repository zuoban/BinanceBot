import { memo } from 'react';
import { Area, ComposedChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';

type HistoryPoint = { hour: number; equity: number | null };
const hourFormat = new Intl.DateTimeFormat('zh-CN', { timeZone: 'Asia/Shanghai', hour: '2-digit', minute: '2-digit', hour12: false });
const dayFormat = new Intl.DateTimeFormat('zh-CN', { timeZone: 'Asia/Shanghai', month: '2-digit', day: '2-digit' });
const formatHour = (hour: number) => hourFormat.format(hour * 1000);
const formatDay = (hour: number) => dayFormat.format(hour * 1000);
const axisTick = { fontSize: 11, fontFamily: 'var(--font-mono)', fill: 'var(--text-muted)' };
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
  const useWan = data.some(point => point.equity !== null && Math.abs(point.equity) >= 100_000);
  const axisValue = (value: number) => useWan ? `${(value / 10000).toLocaleString('en-US', { maximumFractionDigits: 6 })}万` : formatValue(value);
  const tooltip = {
    labelFormatter: (value: unknown) => `${formatDate(Number(value))} · 北京时间`,
    formatter: (value: unknown) => `${formatValue(value)} ${asset ?? ''}`,
    cursor: { stroke: 'var(--border-strong)', strokeDasharray: '3 4' },
    contentStyle: { background: 'var(--bg-raised)', borderColor: 'var(--border-strong)', borderRadius: 8, boxShadow: 'var(--overlay-shadow)', color: 'var(--text-primary)', fontSize: 12 },
    labelStyle: { color: 'var(--text-primary)', fontFamily: 'var(--font-mono)', fontWeight: 600, marginBottom: 4 },
    itemStyle: { color: 'var(--text-primary)', fontFamily: 'var(--font-mono)' },
  };
  const lineColor = daily ? 'var(--text-secondary)' : 'var(--history-equity)';
  const chartHeight = daily ? 196 : 236;
  return <ResponsiveContainer width="100%" height={chartHeight}>
    <ComposedChart data={data} margin={{ top: 12, right: daily ? 16 : 12, bottom: 0, left: 4 }}>
      {!daily && !discrete && <defs>
        <linearGradient id="equityAreaFill" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="var(--history-equity)" stopOpacity={0.18} />
          <stop offset="100%" stopColor="var(--history-equity)" stopOpacity={0} />
        </linearGradient>
      </defs>}
      <CartesianGrid vertical={false} stroke="var(--border-hairline)" />
      <XAxis dataKey="hour" ticks={ticks} tickFormatter={daily ? formatDay : formatHour} tickLine={false} axisLine={false} interval={daily ? 'equidistantPreserveEnd' : 'preserveEnd'} minTickGap={daily ? 24 : 12} tick={axisTick} />
      <YAxis width="auto" domain={['auto', 'auto']} tickFormatter={axisValue} tickLine={false} axisLine={false} tick={axisTick} tickCount={daily ? 3 : 4} />
      <Tooltip {...tooltip} />
      <Area type="monotone" name="账户权益" dataKey="equity" stroke={discrete ? 'none' : lineColor} strokeWidth={daily ? 1.5 : 2} fill={!daily && !discrete ? 'url(#equityAreaFill)' : 'none'} dot={<HistoryDot data={data} discrete={discrete} stroke={lineColor} />} activeDot={{ r: 4, fill: lineColor }} connectNulls={false} isAnimationActive={false} />
    </ComposedChart>
  </ResponsiveContainer>;
});
