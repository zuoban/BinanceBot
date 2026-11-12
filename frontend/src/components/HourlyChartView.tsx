import { memo, useMemo } from 'react';
import { theme } from 'antd';
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';

export type HourlyChartPoint = { hour: string; 买入: number | null; 卖出: number | null };

const barChartMargin = { top: 12, right: 12, bottom: 4, left: 4 };
const barRadius: [number, number, number, number] = [2, 2, 0, 0];
const axisTick = { fontSize: 12, fontFamily: 'var(--font-mono)', fill: 'var(--text-muted)' };
const tooltipContentStyle = { background: 'var(--bg-elevated)', borderColor: 'var(--border-strong)', borderRadius: 8, boxShadow: 'var(--overlay-shadow)', color: 'var(--text-primary)', fontSize: 12 };
const tooltipLabelStyle = { color: 'var(--text-primary)', fontFamily: 'var(--font-mono)', fontWeight: 500, marginBottom: 4 };
const tooltipItemStyle = { fontFamily: 'var(--font-mono)' };

export default memo(function HourlyChartView({ data }: { data: HourlyChartPoint[] }) {
  const { token } = theme.useToken();
  const tooltipCursor = useMemo(() => ({ fill: token.colorFillSecondary }), [token.colorFillSecondary]);
  return <ResponsiveContainer width="100%" height={264}>
    <BarChart accessibilityLayer data={data} barGap={2} maxBarSize={12} margin={barChartMargin}>
      <CartesianGrid vertical={false} stroke="var(--border-hairline)" />
      <XAxis dataKey="hour" interval="preserveStartEnd" minTickGap={24} tickLine={false} axisLine={false} tick={axisTick} />
      <YAxis width="auto" allowDecimals={false} axisLine={false} tickLine={false} tick={axisTick} />
      <Tooltip cursor={tooltipCursor} contentStyle={tooltipContentStyle} labelStyle={tooltipLabelStyle} itemStyle={tooltipItemStyle} />
      <Bar dataKey="买入" fill="var(--positive)" radius={barRadius} isAnimationActive={false} /><Bar dataKey="卖出" fill="var(--negative)" radius={barRadius} isAnimationActive={false} />
    </BarChart>
  </ResponsiveContainer>;
});
