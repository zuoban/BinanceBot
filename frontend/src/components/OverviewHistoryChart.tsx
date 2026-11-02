import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';

type HistoryPoint = { hour: number; equity: number | null };

function HistoryDot({ cx, cy, payload, stroke, index, data }: { cx?: number; cy?: number; payload?: { hour: number }; stroke?: string; index?: number; data: HistoryPoint[] }) {
  if (cx == null || cy == null || !payload || (index != null && data[index]?.equity == null)) return null;
  const isolated = index != null && data[index - 1]?.equity == null && data[index + 1]?.equity == null;
  return <circle cx={cx} cy={cy} r={2} fill={stroke} className={payload.hour % (4 * 3600) === 0 || isolated ? 'history-point' : 'history-point history-point-hourly'} />;
}

export default function OverviewHistoryChart({ data, ticks, asset, formatValue, formatDate }: {
  data: HistoryPoint[];
  ticks: number[];
  asset: string | undefined;
  formatValue: (value: unknown) => string;
  formatDate: (hour: number) => string;
}) {
  const formatHour = (hour: number) => new Date(hour * 1000).toLocaleTimeString('zh-CN', { timeZone: 'Asia/Shanghai', hour: '2-digit', minute: '2-digit', hour12: false });
  const axisValue = (value: number) => Math.abs(value) >= 10000 ? `${(value / 10000).toLocaleString('en-US', { maximumFractionDigits: 4 })}万` : formatValue(value);
  const tooltip = { labelFormatter: (value: unknown) => `${formatDate(Number(value))} · 北京时间`, formatter: (value: unknown) => `${formatValue(value)} ${asset ?? ''}`, cursor: { stroke: '#8aa99b', strokeDasharray: '3 4' } };
  return <ResponsiveContainer width="100%" height={180}>
    <LineChart data={data} margin={{ top: 12, right: 8, bottom: 0, left: 0 }}>
      <CartesianGrid vertical={false} stroke="#e7ece9" />
      <XAxis dataKey="hour" ticks={ticks} tickFormatter={formatHour} tickLine={false} axisLine={false} minTickGap={10} tick={{ fontSize: 11 }} />
      <YAxis width={52} domain={['auto', 'auto']} tickFormatter={axisValue} tickLine={false} axisLine={false} tick={{ fontSize: 11 }} tickCount={3} />
      <Tooltip {...tooltip} />
      <Line name="账户权益" dataKey="equity" stroke="var(--history-equity)" strokeWidth={2} dot={<HistoryDot data={data} />} activeDot={{ r: 4 }} connectNulls={false} isAnimationActive={false} />
    </LineChart>
  </ResponsiveContainer>;
}
