import { useCallback, useContext, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { App, Button, Card, Dropdown, Empty, Grid, Input, Select } from 'antd';
import { PlusOutlined, SearchOutlined, ThunderboltOutlined } from '@ant-design/icons';
import { useOverview } from '../lib/queries';
import { useUrlParams } from '../lib/urlState';
import { AccountsContext } from '../lib/session';
import { errorMessage } from '../lib/api';
import { useAsyncLock, useMountedRef } from '../lib/asyncGuard';
import { beijingDay, comparePositionAmount, groupTradePnl, modeLabel, positionGroups, SNAPSHOT_STALE_MS, todayPnlGroups, todayTradeCounts } from '../lib/format';
import { ErrorNotice, FreshnessText, PageTitle, Pnl, SignalButton, SignalChip, type Signal } from '../components/Common';
import { runBatchControls, useControls } from '../components/Controls';
import type { Overview, Pair, Trade } from '../lib/types';
import { OverviewLoading } from '../components/Loading';
import { OverviewFinanceDisclosure } from '../components/OverviewFinanceDisclosure';
import { OverviewHistory } from '../components/OverviewHistory';
import { StrategyCard, toPair } from '../components/StrategyCard';
import { BatchResultDrawer, type BatchReport } from '../components/BatchResultDrawer';
import { PaperGuide } from '../components/PaperGuide';
import { TradesTable } from '../components/DataTables';
import { useAccountGroups, useBeijingDay, useExpired } from '../lib/useNow';
const emptyPairs: Overview['pairs'] = [];
const emptyTrades: Trade[] = [];
export default function OverviewPage() {
  const overview = useOverview();
  const navigate = useNavigate();
  const addLink = useRef<HTMLAnchorElement>(null);
  const restoreAddFocus = useRef(false);
  const setAddLink = useCallback((node: HTMLAnchorElement | null) => {
    if (!node && addLink.current === document.activeElement) restoreAddFocus.current = true;
    addLink.current = node;
  }, []);
  const hasOverview = !!overview.data;
  useLayoutEffect(() => {
    if (restoreAddFocus.current && document.activeElement === document.body) addLink.current?.focus();
    restoreAddFocus.current = false;
  }, [hasOverview]);
  const addPairButton = (props?: { ref?: React.Ref<HTMLAnchorElement>; icon?: React.ReactNode; type?: 'primary' }) => <Button {...props} href="/pairs/new" onClick={event => {
    if (event.button === 0 && !event.metaKey && !event.ctrlKey && !event.shiftKey && !event.altKey) {
      event.preventDefault();
      navigate('/pairs/new');
    }
  }}>新增交易对</Button>;
  const addPairLink = addPairButton({ ref: setAddLink, icon: <PlusOutlined /> });
  const accounts = useContext(AccountsContext);
  const { modal, message } = App.useApp();
  const controls = useControls();
  const [{ q: search, sort, status: statusFilter }, updateFilters] = useUrlParams({
    q: { fallback: '' },
    sort: { fallback: 'position', allowed: ['position', 'symbol', 'today', 'orders'] },
    status: { fallback: 'all', allowed: ['all', 'RUNNING', 'PAUSED', 'ERROR', 'STOPPED', 'LIVE'] },
  });
  const setSearch = (value: string) => updateFilters({ q: value });
  const setSort = (value: typeof sort) => updateFilters({ sort: value });
  const setStatusFilter = (value: typeof statusFilter) => updateFilters({ status: value });
  const [batchBusy, setBatchBusy] = useState(false);
  const [report, setReport] = useState<BatchReport | null>(null);
  const [tradesExpanded, setTradesExpanded] = useState(false);
  const compactFinance = Grid.useBreakpoint().md === false;
  const [financesExpanded, setFinancesExpanded] = useState(false);
  const dayNow = useBeijingDay();
  const dataExpired = useExpired(overview.data ? overview.dataUpdatedAt + SNAPSHOT_STALE_MS : Infinity);
  const runBatch = useAsyncLock();
  const mounted = useMountedRef();
  const entries = overview.data?.pairs ?? emptyPairs;
  const recentTrades = overview.data?.recent_trades ?? emptyTrades;
  const displayedTrades = useMemo(() => recentTrades.slice(0, tradesExpanded ? 20 : 5), [recentTrades, tradesExpanded]);
  const buyTrades = displayedTrades.filter(trade => trade.side === 'BUY').length;
  const sellTrades = displayedTrades.filter(trade => trade.side === 'SELL').length;
  const tradePnlGroups = useMemo(() => groupTradePnl(displayedTrades), [displayedTrades]);
  const stale = !!overview.error || (!!overview.data && dataExpired);
  const pairs = useMemo(() => entries.map(toPair), [entries]);
  let runningCount = 0, pausedCount = 0, stoppedCount = 0, errorCount = 0, liveCount = 0, allPaper = pairs.length > 0;
  for (const p of pairs) {
    if (p.status === 'RUNNING') runningCount++;
    else if (p.status === 'PAUSED') pausedCount++;
    else if (p.status === 'STOPPED') stoppedCount++;
    else if (p.status === 'ERROR') errorCount++;
    if (p.mode === 'LIVE') liveCount++;
    if (p.mode !== 'PAPER') allPaper = false;
  }
  const currentDay = beijingDay(dayNow);
  const tradeCountsBySymbol = useMemo(() => new Map(entries.map(e => [e.snapshot.symbol, todayTradeCounts(e.snapshot, dayNow)])), [entries, currentDay]);
  const batch = (action: 'pause' | 'resume') => runBatch(async () => {
    setBatchBusy(true);
    const refreshPairs = async () => {
      const refreshed = await overview.refetch();
      if (!mounted.current) return null;
      if (refreshed.error || !refreshed.data) throw refreshed.error ?? new Error('无法刷新策略列表');
      return refreshed.data.pairs;
    };
    try {
      let targets: Pair[] = [];
      if (action === 'resume') {
        const latest = await refreshPairs();
        if (!latest) return;
        targets = latest.filter(e => e.snapshot.status !== 'RUNNING').map(toPair);
        if (!targets.length) { message.info('当前没有可恢复的策略'); return; }
      }
      const accepted = await modal.confirm({
        title: `${action === 'resume' ? '恢复' : '暂停'}全部策略`,
        content: action === 'pause' ? '将刷新策略列表并暂停全部交易对，不受当前筛选影响。' : `将操作 ${targets.length} 个交易对，不受当前筛选影响。${targets.some(p => p.mode === 'LIVE') ? '其中包含实盘策略，恢复后会使用真实资金下单。' : ''}`,
        okText: '确认执行', cancelText: '取消',
      });
      if (!accepted || !mounted.current) return;
      let selected = targets;
      if (action === 'pause') {
        const latest = await refreshPairs();
        if (!latest) return;
        selected = latest.map(toPair);
        if (!selected.length) { message.info('当前没有可暂停的策略'); return; }
      }
      const outcomes = await runBatchControls(selected, action, controls.run);
      if (!mounted.current) return;
      if (outcomes.length) setReport({ action, total: selected.length, outcomes });
      else { setReport(null); message.success(`已${action === 'pause' ? '暂停' : '恢复'} ${selected.length} 个策略`); }
    } catch (error) {
      if (mounted.current) message.error(`批量操作失败：${errorMessage(error)}`);
    } finally {
      if (mounted.current) setBatchBusy(false);
    }
  });
  const visible = useMemo(() => {
    const query = search.toUpperCase().trim();
    const matchesStatus = (e: typeof entries[number]) => statusFilter === 'all' || (statusFilter === 'LIVE' ? e.mode === 'LIVE' : e.snapshot.status === statusFilter);
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
  }, [entries, search, statusFilter, sort, tradeCountsBySymbol]);
  const clearFilters = () => updateFilters({ q: '', sort: 'position', status: 'all' });
  const counts = [...tradeCountsBySymbol.values()];
  const today = counts.every(c => c !== null) ? counts.reduce<{ buy: number; sell: number }>((sum, c) => ({ buy: sum.buy + c.buy, sell: sum.sell + c.sell }), { buy: 0, sell: 0 }) : null;
  const pnlGroups = useMemo(() => todayPnlGroups(overview.data?.today_pnl, dayNow), [overview.data?.today_pnl, currentDay]);
  const accountList = useAccountGroups(accounts);
  const positionList = useMemo(() => positionGroups(entries), [entries]);
  const renderStatusButton = (value: Exclude<typeof statusFilter, 'all'>, tier: Signal, label: string, count: number) =>
    overview.data && count > 0 ? <SignalButton tier={tier} active={statusFilter === value} onClick={() => setStatusFilter(statusFilter === value ? 'all' : value)}>{label} {count}</SignalButton> : null;
  return <>
    <PageTitle title="策略运行概览" description="账户、持仓与全部策略的实时状态" />
    <ErrorNotice error={overview.error} retry={() => void overview.refetch()} />
    {overview.data && allPaper && <PaperGuide firstSymbol={pairs[0].symbol} />}
    <section className={`overview-banner${stale ? ' is-stale' : ''}`} aria-label="策略运行概览">
      <div className="signal-strip">
        {stale && <SignalChip tier="yellow">数据暂未更新，请检查连接</SignalChip>}
        {stale && overview.dataUpdatedAt > 0 && <span className="muted freshness"><FreshnessText updatedAt={overview.dataUpdatedAt} prefix="上次更新于 " /></span>}
        {renderStatusButton('LIVE', 'orange', '实盘策略', liveCount)}
        {renderStatusButton('ERROR', 'red', '异常', errorCount)}
        {renderStatusButton('PAUSED', 'blue', '已暂停', pausedCount)}
        {overview.data && !stale && pairs.length > 0 && runningCount === pairs.length && <SignalChip tier="calm">全部策略运行正常</SignalChip>}
        {renderStatusButton('STOPPED', 'blue', '已停止', stoppedCount)}
      </div>
      <div className="overview-stats">
        <div className="overview-stat"><span>运行中</span><strong>{overview.data ? runningCount : '--'}</strong></div>
        <div className="overview-stat"><span>策略总数</span><strong>{overview.data ? pairs.length : '--'}</strong></div>
        <div className="overview-stat"><span>今日成交</span><strong>{overview.data && today ? today.buy + today.sell : '--'}<small> 笔</small></strong><em className="overview-trade-breakdown">买单 {overview.data && today ? today.buy : '--'} 笔 / 卖单 {overview.data && today ? today.sell : '--'} 笔</em></div>
        <div className="overview-stat" aria-label="今日已实现盈亏">
          <span>今日已实现盈亏</span>
          {!pnlGroups ? <strong>--</strong> : !pnlGroups.length ? <strong>0</strong> : pnlGroups.map(group => <div className="overview-pnl-group" key={group.key}>
            <strong><Pnl value={group.total} compact /><small>{group.asset}</small></strong>
            {group.pending > 0 && <span>盈亏待同步</span>}
          </div>)}
        </div>
      </div>
    </section>
    {overview.isPending && !overview.data ? <OverviewLoading addPairLink={addPairLink} /> : !overview.data ? <section className="strategies-section" aria-labelledby="strategies-heading"><div className="section-heading"><h2 id="strategies-heading">交易策略</h2>{addPairLink}</div></section> : <>
    <OverviewFinanceDisclosure accountList={accountList} positionList={positionList} stale={stale} compactFinance={compactFinance} financesExpanded={financesExpanded} setFinancesExpanded={setFinancesExpanded} />
    <section className={`strategies-section${stale ? ' is-stale' : ''}`} aria-labelledby="strategies-heading">
    <div className="section-heading"><h2 id="strategies-heading">交易策略</h2>
      <div className="strategy-heading-actions">
        <Dropdown trigger={['click']} disabled={batchBusy || controls.busy.size > 0} menu={{ items: [{ key: 'resume', label: '全部恢复' }, { key: 'pause', label: '全部暂停' }], onClick: ({ key }) => void batch(key as 'pause' | 'resume') }}>
          <Button loading={batchBusy} icon={<ThunderboltOutlined />}>全部策略操作</Button>
        </Dropdown>
        {addPairLink}
      </div>
    </div>
    <div className="filter-bar">
      <Input prefix={<SearchOutlined />} placeholder="搜索交易对" aria-label="搜索交易对" allowClear value={search} onChange={e => setSearch(e.target.value)} />
      <Select aria-label="策略排序" value={sort} onChange={setSort} options={[{ value: 'position', label: '按状态与持仓排序' }, { value: 'symbol', label: '按交易对排序' }, { value: 'today', label: '按今日成交排序' }, { value: 'orders', label: '按挂单数排序' }]} />
      {(search || statusFilter !== 'all') && <span className="muted filter-count">显示 {visible.length} / {entries.length} 个</span>}
      {(search || sort !== 'position' || statusFilter !== 'all') && <Button type="text" onClick={clearFilters}>清除筛选</Button>}
    </div>
    <div className="strategy-grid">
      {visible.map(entry => {
        const symbol = entry.snapshot.symbol, counts = tradeCountsBySymbol.get(symbol);
        return <StrategyCard key={symbol} entry={entry} stale={stale} buyCount={counts?.buy ?? null} sellCount={counts?.sell ?? null} controlResult={controls.results[symbol]} clearResult={controls.clearResult} />;
      })}
      {!visible.length && <Card className="strategy-empty"><Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description={pairs.length ? '没有符合筛选条件的策略' : '暂无策略数据'}>{pairs.length ? <Button onClick={clearFilters}>清除筛选</Button> : addPairButton({ type: 'primary' })}</Empty></Card>}
    </div>
    </section>
    <Card className="overview-trades" title="最近成交" extra={<span className="muted">全部策略当前模式 · 最近 20 条</span>}>
      <div className="recent-trades-summary" aria-label="当前显示成交汇总">
        <span>当前显示 <strong>{displayedTrades.length}</strong> 笔</span>
        <span>买入 <strong>{buyTrades}</strong> 笔</span>
        <span>卖出 <strong>{sellTrades}</strong> 笔</span>
        <span>合计盈亏 {tradePnlGroups.size === 0 ? <strong>--</strong> : [...tradePnlGroups.entries()].map(([key, group]) => <span key={key}> {modeLabel[group.mode]} <strong>{group.available && Number.isFinite(group.total) ? <Pnl value={group.total} compact /> : <span className="pending-pnl-warn">待核实</span>}</strong> {group.asset}</span>)}</span>
      </div>
      <div id="recent-trades-list"><TradesTable overview trades={displayedTrades} /></div>
      {recentTrades.length > 5 && <div className="overview-trades-toggle"><Button type="text" aria-expanded={tradesExpanded} aria-controls="recent-trades-list" onClick={() => setTradesExpanded(value => !value)}>{tradesExpanded ? '收起' : '展开最近 20 条'}</Button></div>}
    </Card>
    <OverviewHistory />
    <BatchResultDrawer report={report} busy={controls.busy} onClose={() => setReport(null)} onRetry={(pair, action) => void (async () => {
      const result = await controls.run(pair, action);
      if (!mounted.current || !result || result === 'not_submitted') return;
      setReport(current => {
        if (!current) return null;
        const outcomes = result === 'confirmed'
          ? current.outcomes.filter(item => item.pair.symbol !== pair.symbol)
          : current.outcomes.map(item => item.pair.symbol === pair.symbol ? { ...item, result } : item);
        return outcomes.length ? { ...current, outcomes } : null;
      });
    })()} />
    </>}
  </>;
}
