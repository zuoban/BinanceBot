import { Suspense, lazy, useState } from 'react';
import { Card, Select } from 'antd';
import { useOverviewHistory } from '../lib/queries';
import { modeLabel, number, trimFractionZeros } from '../lib/format';
import type { Mode } from '../lib/types';
import { LoadErrorBoundary } from './LoadErrorBoundary';

const OverviewHistoryChart = lazy(() => import('./OverviewHistoryChart'));
const formatValue = (value: unknown) => trimFractionZeros(number(value));
const formatDate = (hour: number) => new Date(hour * 1000).toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false });

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
  return <Card className="overview-history" size="small">
    <div className="history-heading"><div><h2>账户权益趋势</h2></div>{options.length > 0 && !history.isPending && !history.error && <div className="history-metric history-equity" aria-label="最近一次快照"><span>最近账户权益 · {asset}</span><strong>{formatValue(latest?.equity)}</strong></div>}</div>
    {options.length > 0 && <div className="history-account-row"><Select aria-label="选择趋势账户" value={key} options={options} onChange={setSelected} /></div>}
    {history.isPending ? <div className="history-empty">正在加载快照…</div> : history.error ? <div className="history-empty" role="alert">快照加载失败，请稍后重试</div> : !options.length ? <div className="history-empty">暂无快照数据，整点后开始记录</div> : <>
      <div className="history-caption"><span>单位 {asset} · 近 24 小时 · 北京时间</span><span>最近记录 {latest ? formatDate(latest.hour) : '--'}</span></div>
      <div className="history-plot chart" aria-label={`近24小时${asset ?? ''}账户权益折线图，最近记录 ${latest ? `${formatDate(latest.hour)}，权益 ${formatValue(latest.equity)}` : '暂无'}`}>
        <LoadErrorBoundary label="权益图" onReload={() => window.location.reload()}>
          <Suspense fallback={<div className="history-empty" style={{ minHeight: 180 }} role="status">正在加载图表…</div>}>
            <OverviewHistoryChart data={data} ticks={ticks} asset={asset} formatValue={formatValue} formatDate={formatDate} />
          </Suspense>
        </LoadErrorBoundary>
      </div>

    </>}
  </Card>;
}
