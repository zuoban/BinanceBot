import { Suspense, lazy, useState } from 'react';
import { Card, Select } from 'antd';
import { useOverviewHistory } from '../lib/queries';
import { modeLabel, number, trimFractionZeros } from '../lib/format';
import type { HistoryAccount } from '../lib/types';
import { LoadErrorBoundary } from './LoadErrorBoundary';

const OverviewHistoryChart = lazy(() => import('./OverviewHistoryChart'));
const formatValue = (value: unknown) => trimFractionZeros(number(value));
const formatDate = (hour: number) => new Date(hour * 1000).toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false });
const accountKey = (account: HistoryAccount) => JSON.stringify([account.mode, account.asset, account.account_id]);
const isUnidentified = (account: HistoryAccount) => account.mode !== 'PAPER' && account.account_id === null;

export function OverviewHistory() {
  const history = useOverviewHistory();
  const [selected, setSelected] = useState<{ current: string; key: string }>();
  const snapshots = history.data?.snapshots ?? [];
  const currentAccounts = history.data?.current_accounts ?? [];
  const currentKeys = new Set(currentAccounts.map(accountKey));
  const current = JSON.stringify([...currentKeys].sort());
  const groups = new Map<string, HistoryAccount>();
  for (const account of currentAccounts) groups.set(accountKey(account), account);
  for (const hour of snapshots) for (const group of hour.groups) groups.set(accountKey(group), group);
  const categoryCounts = new Map<string, number>();
  const entries = [...groups.entries()].map(([value, group]) => {
    const category = isUnidentified(group) ? '身份未知' : currentKeys.has(value) ? '当前账户' : '历史账户';
    const numberKey = `${modeLabel[group.mode]} · ${group.asset} · ${category}`;
    categoryCounts.set(numberKey, (categoryCounts.get(numberKey) ?? 0) + 1);
    return { value, numberKey };
  });
  const historicalNumbers = new Map<string, number>();
  const options = entries.map(({ value, numberKey }) => {
    const count = (historicalNumbers.get(numberKey) ?? 0) + 1;
    historicalNumbers.set(numberKey, count);
    const peers = categoryCounts.get(numberKey) ?? 1;
    return { value, label: `${numberKey}${peers > 1 ? ` ${count}` : ''}` };
  });
  const key = selected?.current === current && groups.has(selected.key) ? selected.key : currentAccounts[0] ? accountKey(currentAccounts[0]) : options[0]?.value;
  const account = key ? groups.get(key) : undefined;
  const asset = account?.asset;
  const discrete = account ? isUnidentified(account) : false;
  const lastHour = Math.floor(Date.now() / 3600_000) * 3600;
  const byHour = new Map(snapshots.map(point => [point.hour, point]));
  const pointAt = (hour: number) => {
    const group = byHour.get(hour)?.groups.find(g => accountKey(g) === key);
    return { hour, equity: group?.equity == null ? null : Number(group.equity), recorded: !!group };
  };
  const data = Array.from({ length: 24 }, (_, i) => pointAt(lastHour - (23 - i) * 3600));
  const midnight = Math.floor((lastHour + 8 * 3600) / (24 * 3600)) * 24 * 3600 - 8 * 3600;
  const dailyData = Array.from({ length: 7 }, (_, i) => pointAt(midnight - (6 - i) * 24 * 3600));
  const latestDay = [...dailyData].reverse().find(point => point.recorded);
  const hasDailyEquity = dailyData.some(point => point.equity !== null);
  const latest = [...data].reverse().find(point => point.recorded);
  const ticks = data.filter(point => point.hour % (4 * 3600) === 0).map(point => point.hour);
  return <Card className="overview-history" size="small">
    <div className="history-heading"><div><h2>账户权益趋势</h2></div>{options.length > 0 && !history.isPending && !history.error && <div className="history-metric history-equity" aria-label="最近一次快照"><span>最近账户权益 · {asset}</span><strong>{formatValue(latest?.equity)}</strong></div>}</div>
    {options.length > 1 && <div className="history-account-row"><Select aria-label="选择趋势账户" value={key} options={options} onChange={next => setSelected({ current, key: next })} /></div>}
    {history.isPending ? <div className="history-empty">正在加载快照…</div> : history.error ? <div className="history-empty" role="alert">快照加载失败，请稍后重试</div> : !options.length ? <div className="history-empty">暂无快照数据，整点后开始记录</div> : <>
      <div className="history-caption"><div className="history-caption-main"><h3>近 24 小时</h3><span>单位 {asset} · 北京时间</span></div><span>最近记录 {latest ? formatDate(latest.hour) : '--'}</span></div>
      {data.some(point => point.equity !== null) ? <div className="history-plot chart" role="group" aria-label={`近24小时${asset ?? ''}账户权益折线图，最近记录 ${latest ? `${formatDate(latest.hour)}，权益 ${formatValue(latest.equity)}` : '暂无'}`}>
        <LoadErrorBoundary label="24 小时权益图" onReload={() => window.location.reload()}>
          <Suspense fallback={<div className="history-empty history-plot-loading" role="status">正在加载图表…</div>}>
            <OverviewHistoryChart data={data} ticks={ticks} asset={asset} formatValue={formatValue} formatDate={formatDate} discrete={discrete} />
          </Suspense>
        </LoadErrorBoundary>
      </div> : <div className="history-empty">近 24 小时暂无可用的权益快照</div>}
      <div className="history-daily">
        <div className="history-caption"><div className="history-caption-main"><h3>近 7 天</h3><span>单位 {asset} · 每日 00:00 · 北京时间</span></div><span>最近零点记录 {latestDay ? formatDate(latestDay.hour) : '--'}</span></div>
        {hasDailyEquity ? <div className="history-plot chart" role="group" aria-label={`近7天${asset ?? ''}账户权益折线图，每日北京时间零点，最近记录 ${latestDay ? `${formatDate(latestDay.hour)}，权益 ${formatValue(latestDay.equity)}` : '暂无'}`}>
          <LoadErrorBoundary label="7 天权益图" onReload={() => window.location.reload()}>
            <Suspense fallback={<div className="history-empty history-plot-loading" role="status">正在加载图表…</div>}>
              <OverviewHistoryChart data={dailyData} ticks={dailyData.map(point => point.hour)} asset={asset} formatValue={formatValue} formatDate={formatDate} daily discrete={discrete} />
            </Suspense>
          </LoadErrorBoundary>
        </div> : <div className="history-empty">近 7 天暂无可用的零点权益快照</div>}
      </div>

    </>}
  </Card>;
}
