import { Suspense, lazy, memo, useMemo, useState } from 'react';
import { Button, Card, Select } from 'antd';
import { useOverviewHistory } from '../lib/queries';
import { availableRatio, compactNumber, compactPrice, modeLabel, numeric, quoteAsset, ratioText, time } from '../lib/format';
import { useExpired } from '../lib/useNow';
import type { DailySnapshot, HistoryAccount, Mode, OverviewHistoryData } from '../lib/types';
import { Pnl } from './Common';
import { LoadErrorBoundary } from './LoadErrorBoundary';

const OverviewHistoryChart = lazy(() => import('./OverviewHistoryChart'));
const historyDateFormat = new Intl.DateTimeFormat('zh-CN', { timeZone: 'Asia/Shanghai', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false });
const formatDate = (hour: number) => historyDateFormat.format(hour * 1000);
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
    if (!account) return { hour, equity: null };
    const list = byHour.get(hour);
    if (list) {
      for (let i = 0; i < list.length; i++) {
        const g = list[i];
        if (g.mode === account.mode && g.asset === account.asset && g.account_id === account.account_id) {
          return { hour, equity: numeric(g.equity) };
        }
      }
    }
    return { hour, equity: null };
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

const DailySnapshotsSection = memo(function DailySnapshotsSection({ snapshots }: { snapshots: DailySnapshot[] }) {
  const [selectedDate, setSelectedDate] = useState<string>();
  const active = useMemo(
    () => snapshots.find(s => s.date === selectedDate) ?? snapshots[0],
    [snapshots, selectedDate],
  );
  const options = useMemo(
    () => snapshots.map(s => ({ value: s.date, label: `${s.date}（北京时间）` })),
    [snapshots],
  );
  const tradeSummary = useMemo(() => {
    if (!active) return { buy: 0, sell: 0, total: 0, pnlGroups: [] as { key: string; mode: Mode; asset: string; pnl: number; pending: number }[] };
    let buy = 0, sell = 0;
    const groups = new Map<string, { key: string; mode: Mode; asset: string; pnl: number; pending: number }>();
    for (const item of active.trades) {
      buy += item.buy_count;
      sell += item.sell_count;
      const asset = quoteAsset(item.symbol) || item.symbol;
      const key = `${item.mode}:${asset}`;
      let group = groups.get(key);
      if (!group) {
        group = { key, mode: item.mode, asset, pnl: 0, pending: 0 };
        groups.set(key, group);
      }
      group.pnl += numeric(item.realized_pnl) ?? 0;
      group.pending += item.pending_pnl_count;
    }
    return { buy, sell, total: buy + sell, pnlGroups: [...groups.values()] };
  }, [active]);
  const openPositions = useMemo(
    () => (active?.pair_positions ?? []).filter(item => (numeric(item.size) ?? 0) !== 0),
    [active],
  );
  if (!active) return null;
  return <div className="history-daily daily-snapshot-section" aria-label="每日运行快照">
    <div className="history-caption">
      <div className="history-caption-main"><h3>每日运行快照</h3><span>每日 00:00 自动归档前一日数据 · 北京时间</span></div>
      <span className="numeric">生成于 {time(active.generated_at)}</span>
    </div>
    {options.length > 1 && (
      <div className="history-account-row">
        <Select aria-label="选择每日快照日期" value={active.date} options={options} onChange={setSelectedDate} />
      </div>
    )}
    <div className="daily-snapshot-grid">
      <div className="daily-snapshot-block">
        <h4>账户概览</h4>
        {!active.accounts.length ? <div className="muted">暂无账户记录</div> : active.accounts.map((acc, idx) => {
          const equity = numeric(acc.equity), available = numeric(acc.available), wallet = numeric(acc.wallet), unrealized = numeric(acc.unrealized);
          const ratio = availableRatio(available, wallet);
          return <div className="daily-snapshot-item" key={`${acc.mode}-${acc.asset}-${acc.account_id ?? idx}`}>
            <div className="daily-snapshot-item-head">
              <strong>{modeLabel[acc.mode]} · {acc.asset}</strong>
              <span className="muted">{acc.count} 个策略</span>
            </div>
            <div className="daily-snapshot-metrics">
              <div><span>账户权益</span><strong>{compactNumber(equity)} <small>{acc.asset}</small></strong></div>
              <div><span>可用余额</span><strong>{compactNumber(available)} <small>({ratioText(ratio, true)})</small></strong></div>
              <div><span>钱包余额</span><strong>{compactNumber(wallet)} <small>{acc.asset}</small></strong></div>
              <div><span>未实现盈亏</span><strong><Pnl value={unrealized} compact /> <small>{acc.asset}</small></strong></div>
            </div>
          </div>;
        })}
      </div>
      <div className="daily-snapshot-block">
        <h4>成交与实现盈亏（{active.date}）</h4>
        <div className="daily-snapshot-item">
          <div className="daily-snapshot-metrics">
            <div><span>成交笔数</span><strong>{tradeSummary.total} <small>笔</small></strong></div>
            <div><span>买单 / 卖单</span><strong>{tradeSummary.buy} <small>/</small> {tradeSummary.sell}</strong></div>
            <div className="daily-snapshot-span">
              <span>实现盈亏</span>
              {!tradeSummary.pnlGroups.length ? <strong>0</strong> : tradeSummary.pnlGroups.map(group => (
                <div className="daily-snapshot-pnl-row" key={group.key}>
                  <span>{modeLabel[group.mode]} {group.asset}</span>
                  <strong><Pnl value={group.pnl} compact /> <small>{group.asset}</small>{group.pending > 0 && <em className="pending-pnl-warn">（{group.pending} 笔待同步）</em>}</strong>
                </div>
              ))}
            </div>
          </div>
          {active.trades.length > 0 && (
            <div className="daily-snapshot-sublist">
              {active.trades.map(t => {
                const quote = quoteAsset(t.symbol) || t.symbol;
                return <div className="daily-snapshot-subrow" key={`${t.mode}:${t.symbol}`}>
                  <span>{t.symbol} <small className="muted">{modeLabel[t.mode]} · 买 {t.buy_count} / 卖 {t.sell_count}</small></span>
                  <strong><Pnl value={t.realized_pnl} compact /> <small>{quote}</small>{t.pending_pnl_count > 0 && <em className="pending-pnl-warn"> 待同步</em>}</strong>
                </div>;
              })}
            </div>
          )}
        </div>
      </div>
      <div className="daily-snapshot-block">
        <h4>持仓概览</h4>
        {!active.positions.length ? <div className="muted">暂无持仓记录</div> : active.positions.map(pos => {
          const total = numeric(pos.total), long = numeric(pos.long), short = numeric(pos.short);
          return <div className="daily-snapshot-item" key={`${pos.mode}:${pos.asset}`}>
            <div className="daily-snapshot-item-head">
              <strong>{modeLabel[pos.mode]} · {pos.asset}</strong>
              <span className="muted">覆盖 {pos.count} 个策略</span>
            </div>
            <div className="daily-snapshot-metrics">
              <div><span>总持仓金额</span><strong>{compactNumber(total)} <small>{pos.asset}</small></strong></div>
              <div><span>做多 / 做空</span><strong><span className="position-long">{compactNumber(long)}</span> <small>/</small> <span className="position-short">{compactNumber(short)}</span></strong></div>
            </div>
          </div>;
        })}
        {openPositions.length > 0 && (
          <div className="daily-snapshot-sublist" aria-label="持仓明细">
            {openPositions.map(item => {
              const size = numeric(item.size) ?? 0;
              const quote = quoteAsset(item.symbol) || item.symbol;
              return <div className="daily-snapshot-subrow" key={`${item.mode}:${item.symbol}`}>
                <span>{item.symbol} <small className={size > 0 ? 'position-long' : 'position-short'}>{size > 0 ? '做多' : '做空'} {compactPrice(Math.abs(size))}</small></span>
                <strong>{compactNumber(item.position_amount)} <small>{quote}</small> · 浮盈 <Pnl value={item.unrealized_pnl} compact /></strong>
              </div>;
            })}
          </div>
        )}
      </div>
    </div>
  </div>;
});

export function OverviewHistory() {
  const history = useOverviewHistory();
  const liveHour = Math.floor(Date.now() / 3600_000);
  useExpired((liveHour + 1) * 3600_000 - 1);
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
  const lastHour = Math.floor(Date.now() / 3600_000) * 3600;
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
  return <Card className="overview-history" size="small">
    <div className="history-heading">
      <div><h2>账户权益趋势</h2></div>
      {options.length > 0 && !history.isPending && !history.error && (
        <div className="history-metric" aria-label="最近一次快照">
          <span>最近账户权益 · {asset}</span>
          <strong>{compactNumber((latest ?? latestDay)?.equity)}</strong>
        </div>
      )}
    </div>
    {options.length > 1 && !history.isPending && !history.error && (
      <div className="history-account-row">
        <Select aria-label="选择趋势账户" value={key} options={options} onChange={next => setSelected({ current, key: next })} />
      </div>
    )}
    {history.isPending ? (
      <div className="history-empty">正在加载快照…</div>
    ) : history.error ? (
      <div className="history-empty" role="alert">
        快照加载失败，请稍后重试
        {'refetch' in history && typeof history.refetch === 'function' && (
          <> <Button type="link" size="small" onClick={() => void history.refetch()}>重试</Button></>
        )}
      </div>
    ) : !options.length && !dailySnapshots.length ? (
      <div className="history-empty">暂无快照数据，整点后开始记录</div>
    ) : (
      <>
        <div className="history-charts-grid">
          <div className="history-chart-primary">
            <div className="history-caption">
              <div className="history-caption-main"><h3>近 24 小时</h3><span>单位 {asset} · 北京时间</span></div>
              <span className="numeric">最近记录 {latest ? formatDate(latest.hour) : '--'}</span>
            </div>
            {renderPlot(data, ticks, '24 小时权益图', `近24小时${asset ?? ''}账户权益折线图，最近记录 ${latest ? `${formatDate(latest.hour)}，权益 ${compactNumber(latest.equity)}` : '暂无'}`, '近 24 小时暂无可用的权益快照')}
          </div>
          <div className="history-chart-secondary">
            <div className="history-caption">
              <div className="history-caption-main"><h3>近 7 天</h3><span>每日 00:00 · 北京时间</span></div>
              <span className="numeric">最近零点记录 {latestDay ? formatDate(latestDay.hour) : '--'}</span>
            </div>
            {renderPlot(dailyData, dailyTicks, '7 天权益图', `近7天${asset ?? ''}账户权益折线图，每日北京时间零点，最近记录 ${latestDay ? `${formatDate(latestDay.hour)}，权益 ${compactNumber(latestDay.equity)}` : '暂无'}`, '近 7 天暂无可用的零点权益快照', true)}
          </div>
        </div>
        {dailySnapshots.length > 0 && <DailySnapshotsSection snapshots={dailySnapshots} />}
      </>
    )}
  </Card>;
}
