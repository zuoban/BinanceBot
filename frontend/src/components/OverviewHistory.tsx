import { Suspense, lazy, useEffect, useMemo, useRef, useState } from 'react';
import { Card, Select } from 'antd';
import { useOverviewHistory } from '../lib/queries';
import { compactNumber, isValidEpochMs, modeLabel, numeric } from '../lib/format';
import { useUtcHour } from '../lib/useNow';
import type { HistoryAccount, OverviewHistoryData } from '../lib/types';
import { ErrorNotice } from './Common';
import { LoadErrorBoundary } from './LoadErrorBoundary';

const OverviewHistoryChart = lazy(() => import('./OverviewHistoryChart'));
const LazyDailySnapshotsSection = lazy(() => import('./DailySnapshotsSection').then(mod => ({ default: mod.DailySnapshotsSection })));
const historyDateFormat = new Intl.DateTimeFormat('zh-CN', { timeZone: 'Asia/Shanghai', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false });
const formatDate = (hour: number) => isValidEpochMs(hour * 1000) ? historyDateFormat.format(hour * 1000) : '--';
const accountKey = (account: HistoryAccount) => account.account_id === null ? `${account.mode}\0${account.asset}` : `${account.mode}\0${account.asset}\0${account.account_id}`;
const isUnidentified = (account: HistoryAccount) => account.mode !== 'PAPER' && account.account_id === null;

function buildHistoryAccountOptions(
  snapshots: OverviewHistoryData['snapshots'] | undefined,
  currentAccounts: HistoryAccount[] | undefined,
) {
  const snapshotList = snapshots ?? [];
  const accountList = currentAccounts ?? [];
  const currentKeys = new Set(accountList.map(accountKey));
  const current = [...currentKeys].sort().join('\x01');
  const groups = new Map<string, HistoryAccount>();
  for (const account of accountList) groups.set(accountKey(account), account);
  for (const hour of snapshotList) for (const group of hour.groups) groups.set(accountKey(group), group);
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
  return { current, groups, options };
}

function buildHistorySeries(
  snapshots: OverviewHistoryData['snapshots'] | undefined,
  account: HistoryAccount | undefined,
  lastHour: number,
) {
  const byHour = new Map<number, OverviewHistoryData['snapshots'][number]['groups']>();
  for (const point of snapshots ?? []) byHour.set(point.hour, point.groups);
  const pointAt = (hour: number) => {
    if (!account) return { hour, equity: null, equityText: null as string | null };
    const list = byHour.get(hour);
    if (list) {
      for (let i = 0; i < list.length; i++) {
        const g = list[i];
        if (g.mode === account.mode && g.asset === account.asset && g.account_id === account.account_id) {
          const equity = numeric(g.equity);
          return { hour, equity, equityText: equity !== null ? g.equity : null };
        }
      }
    }
    return { hour, equity: null, equityText: null as string | null };
  };
  const data = Array.from({ length: 24 }, (_, i) => pointAt(lastHour - (23 - i) * 3600));
  const midnight = Math.floor((lastHour + 8 * 3600) / (24 * 3600)) * 24 * 3600 - 8 * 3600;
  const dailyData = Array.from({ length: 7 }, (_, i) => pointAt(midnight - (6 - i) * 24 * 3600));
  let latestDay: (typeof dailyData)[number] | undefined;
  for (let i = dailyData.length - 1; i >= 0; i--) { if (dailyData[i].equity !== null) { latestDay = dailyData[i]; break; } }
  let latest: (typeof data)[number] | undefined;
  for (let i = data.length - 1; i >= 0; i--) { if (data[i].equity !== null) { latest = data[i]; break; } }
  const ticks = data.filter(point => point.hour % (4 * 3600) === 0).map(point => point.hour);
  const dailyTicks = dailyData.map(point => point.hour);
  return { data, dailyData, latestDay, latest, ticks, dailyTicks };
}

export function OverviewHistory() {
  const history = useOverviewHistory();
  const utcHour = useUtcHour();
  const lastHour = utcHour * 3600;
  const initialHourRef = useRef(utcHour);
  const refetchHistory = 'refetch' in history && typeof history.refetch === 'function' ? history.refetch : undefined;
  const refetchHistoryRef = useRef(refetchHistory);
  refetchHistoryRef.current = refetchHistory;
  useEffect(() => {
    if (initialHourRef.current === utcHour) return;
    initialHourRef.current = utcHour;
    void refetchHistoryRef.current?.();
  }, [utcHour]);
  const [selected, setSelected] = useState<{ current: string; key: string }>();
  const snapshots = history.data?.snapshots;
  const currentAccounts = history.data?.current_accounts;
  const dailySnapshots = history.data?.daily_snapshots ?? [];
  const { current, groups, options } = useMemo(
    () => buildHistoryAccountOptions(snapshots, currentAccounts),
    [snapshots, currentAccounts],
  );
  const key = selected?.current === current && groups.has(selected.key) ? selected.key : currentAccounts?.[0] ? accountKey(currentAccounts[0]) : options[0]?.value;
  const account = key ? groups.get(key) : undefined;
  const asset = account?.asset;
  const discrete = account ? isUnidentified(account) : false;
  const { data, dailyData, latestDay, latest, ticks, dailyTicks } = useMemo(
    () => buildHistorySeries(snapshots, account, lastHour),
    [snapshots, account, lastHour],
  );
  const renderPlot = (points: typeof data, plotTicks: number[], label: string, ariaLabel: string, emptyText: string, daily = false) =>
    points.some(point => point.equity !== null) ? <div className="history-plot chart" role="group" aria-label={ariaLabel}>
      <LoadErrorBoundary label={label}>
        <Suspense fallback={<div className="history-empty history-plot-loading" role="status">正在加载图表…</div>}>
          <OverviewHistoryChart data={points} ticks={plotTicks} asset={asset} formatValue={compactNumber} formatDate={formatDate} daily={daily} discrete={discrete} />
        </Suspense>
      </LoadErrorBoundary>
    </div> : <div className="history-empty">{emptyText}</div>;
  const retryHistory = 'refetch' in history && typeof history.refetch === 'function' ? () => void history.refetch() : undefined;
  return <Card className="overview-history" size="small">
    <div className="history-heading">
      <div><h2>账户权益趋势</h2></div>
      {options.length > 0 && !history.isPending && !!history.data && (
        <div className="history-metric" aria-label="最近一次快照">
          <span>最近账户权益 · {asset}</span>
          <strong>{compactNumber((latest ?? latestDay)?.equityText)}</strong>
        </div>
      )}
    </div>
    {history.error && !!history.data && <ErrorNotice error={history.error} retry={retryHistory} />}
    {options.length > 1 && !history.isPending && !!history.data && (
      <div className="history-account-row">
        <Select aria-label="选择趋势账户" value={key} options={options} onChange={next => setSelected({ current, key: next })} />
      </div>
    )}
    {history.isPending && !history.data ? (
      <div className="history-empty">正在加载快照…</div>
    ) : history.error && !history.data ? (
      <ErrorNotice error={history.error} retry={retryHistory} />
    ) : !options.length && !dailySnapshots.length ? (
      <div className="history-empty">暂无快照数据，整点后开始记录</div>
    ) : (
      <>
        {options.length > 0 && (
          <div className="history-charts-grid">
            <div className="history-chart-primary">
              <div className="history-caption">
                <div className="history-caption-main"><h3>近 24 小时</h3><span>单位 {asset} · 北京时间</span></div>
                <span className="numeric">最近记录 {latest ? formatDate(latest.hour) : '--'}</span>
              </div>
              {renderPlot(data, ticks, '24 小时权益图', `近24小时${asset ?? ''}账户权益折线图，最近记录 ${latest ? `${formatDate(latest.hour)}，权益 ${compactNumber(latest.equityText)}` : '暂无'}`, '近 24 小时暂无可用的权益快照')}
            </div>
            <div className="history-chart-secondary">
              <div className="history-caption">
                <div className="history-caption-main"><h3>近 7 天</h3><span>每日 00:00 · 北京时间</span></div>
                <span className="numeric">最近零点记录 {latestDay ? formatDate(latestDay.hour) : '--'}</span>
              </div>
              {renderPlot(dailyData, dailyTicks, '7 天权益图', `近7天${asset ?? ''}账户权益折线图，每日北京时间零点，最近记录 ${latestDay ? `${formatDate(latestDay.hour)}，权益 ${compactNumber(latestDay.equityText)}` : '暂无'}`, '近 7 天暂无可用的零点权益快照', true)}
            </div>
          </div>
        )}
        {dailySnapshots.length > 0 && <LoadErrorBoundary label="每日运行快照"><Suspense fallback={<div className="history-empty history-plot-loading" role="status">正在加载每日运行快照…</div>}><LazyDailySnapshotsSection snapshots={dailySnapshots} /></Suspense></LoadErrorBoundary>}
      </>
    )}
  </Card>;
}
