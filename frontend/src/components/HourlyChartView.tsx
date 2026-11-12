import { memo, useMemo } from 'react';
import { theme } from 'antd';
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';

export type HourlyChartPoint = { hour: string; 买入: number | null; 卖出: number | null };

const barChartMargin = { top: 12, right: 12, bottom: 4, left: -12 };
const barRadius: [number, number, number, number] = [2, 2, 0, 0];

export default memo(function HourlyChartView({ data }: { data: HourlyChartPoint[] }) {
  const { token } = theme.useToken();
  const axisTick = useMemo(() => ({ fontSize: 12, fontFamily: 'var(--font-mono)', fill: token.colorTextTertiary }), [token.colorTextTertiary]);
  const tooltipCursor = useMemo(() => ({ fill: token.colorFillSecondary }), [token.colorFillSecondary]);
  const tooltipContentStyle = useMemo(() => ({ background: token.colorBgElevated, borderColor: token.colorBorder, borderRadius: 8, boxShadow: 'var(--overlay-shadow)', color: token.colorText, fontSize: 12 }), [token.colorBgElevated, token.colorBorder, token.colorText]);
  const tooltipLabelStyle = useMemo(() => ({ color: token.colorText, fontFamily: 'var(--font-mono)', fontWeight: 500, marginBottom: 4 }), [token.colorText]);
  const tooltipItemStyle = useMemo(() => ({ fontFamily: 'var(--font-mono)' }), []);
  return <ResponsiveContainer width="100%" height={264}>
    <BarChart accessibilityLayer data={data} barGap={2} maxBarSize={12} margin={barChartMargin}>
      <CartesianGrid vertical={false} stroke={token.colorBorderSecondary} />
      <XAxis dataKey="hour" interval="preserveStartEnd" minTickGap={24} tickLine={false} axisLine={false} tick={axisTick} />
      <YAxis allowDecimals={false} axisLine={false} tickLine={false} tick={axisTick} />
      <Tooltip cursor={tooltipCursor} contentStyle={tooltipContentStyle} labelStyle={tooltipLabelStyle} itemStyle={tooltipItemStyle} />
      <Bar dataKey="买入" fill={token.colorSuccess} radius={barRadius} isAnimationActive={false} /><Bar dataKey="卖出" fill={token.colorError} radius={barRadius} isAnimationActive={false} />
    </BarChart>
  </ResponsiveContainer>;
});
