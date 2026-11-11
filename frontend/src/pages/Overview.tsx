import { useCallback, useContext, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { App, Button, Card, Dropdown, Empty, Grid, Input, Select } from 'antd';
import { PlusOutlined, SearchOutlined, ThunderboltOutlined } from '@ant-design/icons';
import { useOverview } from '../lib/queries';
import { useUrlParams } from '../lib/urlState';
import { AccountsContext } from '../lib/session';
import { errorMessage } from '../lib/api';
import { useAsyncLock, useMountedRef } from '../lib/asyncGuard';
import { useConfirmDialog } from '../lib/confirmDialog';
import { beijingDay } from '../lib/format';
import { positionGroups, SNAPSHOT_STALE_MS, todayPnlGroups, todayTradeCounts } from '../lib/portfolio';
import { ErrorNotice, PageTitle } from '../components/Common';
import { runBatchControls, useControls } from '../components/Controls';
import type { Overview, Pair, Trade } from '../lib/types';
import { OverviewLoading } from '../components/Loading';
import { OverviewFinanceDisclosure } from '../components/OverviewFinanceDisclosure';
import { OverviewHistory } from '../components/OverviewHistory';
import { filterAndSortOverviewEntries, OverviewBanner, OverviewTradesCard } from '../components/OverviewSections';
import { StrategyCard, toPair } from '../components/StrategyCard';
import { BatchResultDrawer, type BatchReport } from '../components/BatchResultDrawer';
import { PaperGuide } from '../components/PaperGuide';
import { useAccountGroups, useBeijingDay, useExpired } from '../lib/useNow';
const emptyPairs: Overview['pairs'] = [], emptyTrades: Trade[] = [];
export default function OverviewPage() {
  const overview = useOverview(), navigate = useNavigate();
  const addLink = useRef<HTMLAnchorElement>(null), restoreAddFocus = useRef(false);
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
    if (event.button === 0 && !event.metaKey && !event.ctrlKey && !event.shiftKey && !event.altKey) { event.preventDefault(); navigate('/pairs/new'); }
  }}>新增交易对</Button>;
  const addPairLink = addPairButton({ ref: setAddLink, icon: <PlusOutlined /> });
  const accounts = useContext(AccountsContext), { message } = App.useApp(), { confirm } = useConfirmDialog(), controls = useControls();
  const [{ q: search, sort, status: statusFilter }, updateFilters] = useUrlParams({
    q: { fallback: '' },
    sort: { fallback: 'position', allowed: ['position', 'symbol', 'today', 'orders'] },
    status: { fallback: 'all', allowed: ['all', 'RUNNING', 'PAUSED', 'ERROR', 'STOPPED', 'LIVE'] },
  });
  const [batchBusy, setBatchBusy] = useState(false), [report, setReport] = useState<BatchReport | null>(null), batchId = useRef(0);
  const compactFinance = Grid.useBreakpoint().md === false, [financesExpanded, setFinancesExpanded] = useState(false);
  const dayNow = useBeijingDay(), dataExpired = useExpired(overview.data ? overview.dataUpdatedAt + SNAPSHOT_STALE_MS : Infinity);
  const runBatch = useAsyncLock(), mounted = useMountedRef();
  const entries = overview.data?.pairs ?? emptyPairs, recentTrades = overview.data?.recent_trades ?? emptyTrades;
  const stale = !!overview.error || (!!overview.data && dataExpired);
  const pairs = useMemo(() => entries.map(toPair), [entries]), pairsRef = useRef(pairs);
  pairsRef.current = pairs;
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
      const accepted = await confirm({
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
      const id = ++batchId.current;
      if (outcomes.length) setReport({ id, action, total: selected.length, outcomes });
      else { setReport(null); message.success(`已${action === 'pause' ? '暂停' : '恢复'} ${selected.length} 个策略`); }
    } catch (error) {
      if (mounted.current) message.error(`批量操作失败：${errorMessage(error)}`);
    } finally {
      if (mounted.current) setBatchBusy(false);
    }
  });
  const visible = useMemo(() => filterAndSortOverviewEntries(entries, search, statusFilter, sort, tradeCountsBySymbol), [entries, search, statusFilter, sort, tradeCountsBySymbol]);
  const clearFilters = () => updateFilters({ q: '', sort: 'position', status: 'all' });
  const counts = [...tradeCountsBySymbol.values()];
  const today = counts.every(c => c !== null) ? counts.reduce<{ buy: number; sell: number }>((sum, c) => ({ buy: sum.buy + c.buy, sell: sum.sell + c.sell }), { buy: 0, sell: 0 }) : null;
  const pnlGroups = useMemo(() => todayPnlGroups(overview.data?.today_pnl, dayNow), [overview.data?.today_pnl, currentDay]);
  const accountList = useAccountGroups(accounts), positionList = useMemo(() => positionGroups(entries), [entries]);
  return <>
    <PageTitle title="策略运行概览" description="账户、持仓与全部策略的实时状态" />
    <ErrorNotice error={overview.error} retry={() => void overview.refetch()} />
    {overview.data && allPaper && <PaperGuide firstSymbol={pairs[0].symbol} />}
    <OverviewBanner hasData={!!overview.data} stale={stale} dataUpdatedAt={overview.dataUpdatedAt} statusFilter={statusFilter}
      onToggleStatus={value => updateFilters({ status: statusFilter === value ? 'all' : value })}
      counts={{ total: pairs.length, running: runningCount, paused: pausedCount, stopped: stoppedCount, error: errorCount, live: liveCount }} today={today} pnlGroups={pnlGroups} />
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
      <Input prefix={<SearchOutlined />} placeholder="搜索交易对" aria-label="搜索交易对" allowClear value={search} onChange={e => updateFilters({ q: e.target.value })} />
      <Select aria-label="策略排序" value={sort} onChange={value => updateFilters({ sort: value })} options={[{ value: 'position', label: '按状态与持仓排序' }, { value: 'symbol', label: '按交易对排序' }, { value: 'today', label: '按今日成交排序' }, { value: 'orders', label: '按挂单数排序' }]} />
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
    <OverviewTradesCard recentTrades={recentTrades} />
    <OverviewHistory />
    <BatchResultDrawer report={report} busy={controls.busy} onClose={() => setReport(null)} onRetry={(pair, action) => {
      if (!report) return;
      const activeReportId = report.id, currentPair = pairs.find(p => p.symbol === pair.symbol);
      const applyReport = (update: (outcomes: BatchReport['outcomes'], liveSymbols: Set<string>) => BatchReport['outcomes']) => setReport(current => {
        if (!current || current.id !== activeReportId) return current;
        const outcomes = update(current.outcomes, new Set(pairsRef.current.map(p => p.symbol)));
        return outcomes.length ? { ...current, outcomes } : null;
      });
      if (!currentPair) { applyReport((outcomes, liveSymbols) => outcomes.filter(item => liveSymbols.has(item.pair.symbol) && item.pair.symbol !== pair.symbol)); return; }
      void (async () => {
        const result = await controls.run(currentPair, action);
        if (!mounted.current || !result || result === 'not_submitted') return;
        applyReport((outcomes, liveSymbols) => (result === 'confirmed'
          ? outcomes.filter(item => item.pair.symbol !== currentPair.symbol)
          : outcomes.map(item => item.pair.symbol === currentPair.symbol ? { ...item, pair: currentPair, result } : item))
          .filter(item => liveSymbols.has(item.pair.symbol)));
      })();
    }} />
    </>}
  </>;
}
