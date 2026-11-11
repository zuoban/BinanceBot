import { memo, useMemo, useState } from 'react';
import { Button, Card } from 'antd';
import { modeLabel } from '../lib/format';
import { comparePositionAmount, groupTradePnl } from '../lib/portfolio';
import type { Overview, Status, Trade } from '../lib/types';
import { FreshnessText, Pnl, SignalButton, SignalChip, type Signal } from './Common';
import { TradesTable } from './DataTables';

type StatusFilter = 'all' | Status | 'LIVE';
type OverviewSort = 'position' | 'symbol' | 'today' | 'orders';

export function filterAndSortOverviewEntries(
  entries: Overview['pairs'],
  search: string,
  statusFilter: StatusFilter,
  sort: OverviewSort,
  tradeCountsBySymbol: Map<string, { buy: number; sell: number } | null>,
) {
  const query = search.toUpperCase().trim();
  const matchesStatus = (e: Overview['pairs'][number]) => statusFilter === 'all' || (statusFilter === 'LIVE' ? e.mode === 'LIVE' : e.snapshot.status === statusFilter);
  const filtered = entries.filter(e => e.snapshot.symbol.includes(query) && matchesStatus(e));
  if (sort === 'position') {
    return filtered.sort((a, b) => (Number(b.snapshot.status === 'RUNNING') - Number(a.snapshot.status === 'RUNNING')) || comparePositionAmount(a.snapshot, b.snapshot));
  }
  if (sort === 'today') {
    const totalToday = (symbol: string) => { const c = tradeCountsBySymbol.get(symbol); return c ? c.buy + c.sell : -1; };
    return filtered
      .map(entry => ({ entry, count: totalToday(entry.snapshot.symbol) }))
      .sort((a, b) => (b.count - a.count) || a.entry.snapshot.symbol.localeCompare(b.entry.snapshot.symbol))
      .map(item => item.entry);
  }
  if (sort === 'orders') {
    return filtered.sort((a, b) => (b.snapshot.stats.active_buy_orders + b.snapshot.stats.active_sell_orders) - (a.snapshot.stats.active_buy_orders + a.snapshot.stats.active_sell_orders) || a.snapshot.symbol.localeCompare(b.snapshot.symbol));
  }
  return filtered.sort((a, b) => a.snapshot.symbol.localeCompare(b.snapshot.symbol));
}


export function OverviewBanner({
  hasData,
  stale,
  dataUpdatedAt,
  statusFilter,
  onToggleStatus,
  counts,
  today,
  pnlGroups,
}: {
  hasData: boolean;
  stale: boolean;
  dataUpdatedAt: number;
  statusFilter: StatusFilter;
  onToggleStatus: (value: Exclude<StatusFilter, 'all'>) => void;
  counts: { total: number; running: number; paused: number; stopped: number; error: number; live: number };
  today: { buy: number; sell: number } | null;
  pnlGroups: { key: string; asset: string; pending: number; total: number | null }[] | null;
}) {
  const renderStatusButton = (value: Exclude<StatusFilter, 'all'>, tier: Signal, label: string, count: number) =>
    hasData && count > 0 ? <SignalButton tier={tier} active={statusFilter === value} onClick={() => onToggleStatus(value)}>{label} {count}</SignalButton> : null;
  return <section className={`overview-banner${stale ? ' is-stale' : ''}`} aria-label="策略运行概览">
    <div className="signal-strip">
      {stale && <SignalChip tier="yellow">数据暂未更新，请检查连接</SignalChip>}
      {stale && dataUpdatedAt > 0 && <span className="muted freshness"><FreshnessText updatedAt={dataUpdatedAt} prefix="上次更新于 " /></span>}
      {renderStatusButton('LIVE', 'orange', '实盘策略', counts.live)}
      {renderStatusButton('ERROR', 'red', '异常', counts.error)}
      {renderStatusButton('PAUSED', 'blue', '已暂停', counts.paused)}
      {hasData && !stale && counts.total > 0 && counts.running === counts.total && <SignalChip tier="calm">全部策略运行正常</SignalChip>}
      {renderStatusButton('STOPPED', 'blue', '已停止', counts.stopped)}
    </div>
    <div className="overview-stats">
      <div className="overview-stat"><span>运行中</span><strong>{hasData ? counts.running : '--'}</strong></div>
      <div className="overview-stat"><span>策略总数</span><strong>{hasData ? counts.total : '--'}</strong></div>
      <div className="overview-stat"><span>今日成交</span><strong>{hasData && today ? today.buy + today.sell : '--'}<small> 笔</small></strong><em className="overview-trade-breakdown">买单 {hasData && today ? today.buy : '--'} 笔 / 卖单 {hasData && today ? today.sell : '--'} 笔</em></div>
      <div className="overview-stat" aria-label="今日已实现盈亏">
        <span>今日已实现盈亏</span>
        {!pnlGroups ? <strong>--</strong> : !pnlGroups.length ? <strong>0</strong> : pnlGroups.map(group => <div className="overview-pnl-group" key={group.key}>
          <strong><Pnl value={group.total} compact /><small>{group.asset}</small></strong>
          {group.pending > 0 && <span>盈亏待同步</span>}
        </div>)}
      </div>
    </div>
  </section>;
}

export const OverviewTradesCard = memo(function OverviewTradesCard({ recentTrades }: { recentTrades: Trade[] }) {
  const [tradesExpanded, setTradesExpanded] = useState(false);
  const displayedTrades = useMemo(() => recentTrades.slice(0, tradesExpanded ? 20 : 5), [recentTrades, tradesExpanded]);
  const { buyTrades, sellTrades } = useMemo(() => {
    let buy = 0, sell = 0;
    for (const trade of displayedTrades) {
      if (trade.side === 'BUY') buy++;
      else if (trade.side === 'SELL') sell++;
    }
    return { buyTrades: buy, sellTrades: sell };
  }, [displayedTrades]);
  const tradePnlGroups = useMemo(() => groupTradePnl(displayedTrades), [displayedTrades]);
  return <Card className="overview-trades" title="最近成交" extra={<span className="muted">全部策略当前模式 · 最近 20 条</span>}>
    <div className="recent-trades-summary" aria-label="当前显示成交汇总">
      <span>当前显示 <strong>{displayedTrades.length}</strong> 笔</span>
      <span>买入 <strong>{buyTrades}</strong> 笔</span>
      <span>卖出 <strong>{sellTrades}</strong> 笔</span>
      <span>合计盈亏 {tradePnlGroups.size === 0 ? <strong>--</strong> : [...tradePnlGroups.entries()].map(([key, group]) => <span key={key}> {modeLabel[group.mode]} <strong>{group.available && Number.isFinite(group.total) ? <Pnl value={group.total} compact /> : <span className="pending-pnl-warn">待核实</span>}</strong> {group.asset}</span>)}</span>
    </div>
    <div id="recent-trades-list"><TradesTable overview trades={displayedTrades} /></div>
    {recentTrades.length > 5 && <div className="overview-trades-toggle"><Button type="text" aria-expanded={tradesExpanded} aria-controls="recent-trades-list" onClick={() => setTradesExpanded(value => !value)}>{tradesExpanded ? '收起' : '展开最近 20 条'}</Button></div>}
  </Card>;
});
