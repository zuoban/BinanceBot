import { useState } from 'react';
import { Card, Select } from 'antd';
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { useOverviewHistory } from '../lib/queries';
import { modeLabel, number, trimFractionZeros } from '../lib/format';
import type { Mode } from '../lib/types';

const formatValue = (value: unknown) => trimFractionZeros(number(value));
const formatHour = (hour: number) => new Date(hour * 1000).toLocaleTimeString('zh-CN', { timeZone: 'Asia/Shanghai', hour: '2-digit', minute: '2-digit', hour12: false });
const formatDate = (hour: number) => new Date(hour * 1000).toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false });
const axisValue = (value: number) => Math.abs(value) >= 10000 ? `${(value / 10000).toLocaleString('en-US', { maximumFractionDigits: 4 })}万` : formatValue(value);

type HistoryPoint = { hour: number; equity: number | null };
function HistoryDot({ cx, cy, payload, stroke, index, data }: { cx?: number; cy?: number; payload?: { hour: number }; stroke?: string; index?: number; data: HistoryPoint[] }) {
  if (cx == null || cy == null || !payload || (index != null && data[index]?.equity == null)) return null;
  const isolated = index != null && data[index - 1]?.equity == null && data[index + 1]?.equity == null;
  return <circle cx={cx} cy={cy} r={2} fill={stroke} className={payload.hour % (4 * 3600) === 0 || isolated ? 'history-point' : 'history-point history-point-hourly'} />;
}

export function OverviewHistory() {
  const history = useOverviewHistory();
  const [selected, setSelected] = useState<string>();
  const groups = new Map<string, { mode: Mode; asset: string }>();
  for (const hour of history.data ?? []) for (const group of hour.groups) groups.set(`${group.mode}:${group.asset}`, group);
  const options = [...groups.entries()].map(([value, group]) => ({ value, label: `${modeLabel[group.mode]} · ${group.asset}` }));
  const key = selected && groups.has(selected) ? selected : options[0]?.value;
  const asset = key ? groups.get(key)?.asset : undefined;
  const lastHour = Math.floor(Date.now() / 3600_000) * 3600;
  const byHour = new Map((history.data ?? []).map(point => [point.hour, point]));
  const data = Array.from({ length: 24 }, (_, i) => {
    const hour = lastHour - (23 - i) * 3600;
    const group = byHour.get(hour)?.groups.find(g => `${g.mode}:${g.asset}` === key);
    return { hour, equity: group?.equity == null ? null : Number(group.equity), recorded: !!group };
  });
  const latest = [...data].reverse().find(point => point.recorded);
  const ticks = data.filter(point => point.hour % (4 * 3600) === 0).map(point => point.hour);
  const tooltip = { labelFormatter: (value: unknown) => `${formatDate(Number(value))} · 北京时间`, formatter: (value: unknown) => `${formatValue(value)} ${asset ?? ''}`, cursor: { stroke: '#8aa99b', strokeDasharray: '3 4' } };
  return <Card className="overview-history" size="small">
    <div className="history-heading"><div><h2>账户权益趋势</h2><span>近 24 小时 · 每小时记录 · 北京时间</span></div>{options.length > 0 && <Select aria-label="选择趋势账户" value={key} options={options} onChange={setSelected} />}</div>
    {history.isPending ? <div className="history-empty">正在加载快照…</div> : history.error ? <div className="history-empty" role="alert">快照加载失败，请稍后重试</div> : !options.length ? <div className="history-empty">暂无快照数据，整点后开始记录</div> : <>
      <div className="history-metrics" aria-label="最近一次快照">
        <div className="history-metric history-equity"><span>账户权益</span><strong>{formatValue(latest?.equity)}</strong></div>
      </div>
      <div className="history-caption"><span>单位 {asset}</span><span>最近记录 {latest ? formatDate(latest.hour) : '--'}</span></div>
      <div className="history-plot chart" aria-label="近24小时账户权益折线图"><div className="history-plot-label">账户权益</div><ResponsiveContainer width="100%" height={135}>
        <LineChart data={data} margin={{ top: 9, right: 8, bottom: 0, left: 0 }}>
          <CartesianGrid vertical={false} stroke="#e7ece9" />
          <XAxis dataKey="hour" ticks={ticks} tickFormatter={formatHour} tickLine={false} axisLine={false} minTickGap={10} tick={{ fontSize: 10 }} />
          <YAxis width={52} domain={['auto', 'auto']} tickFormatter={axisValue} tickLine={false} axisLine={false} tick={{ fontSize: 10 }} tickCount={3} />
          <Tooltip {...tooltip} />
          <Line name="账户权益" dataKey="equity" stroke="var(--history-equity)" strokeWidth={2} dot={<HistoryDot data={data} />} activeDot={{ r: 4 }} connectNulls={false} isAnimationActive={false} />
        </LineChart>
      </ResponsiveContainer></div>

    </>}
  </Card>;
}
