import { memo, useCallback, useContext, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { App, Button, Card, Col, Dropdown, Empty, Grid, Input, Popover, Row, Select, Space, Statistic } from 'antd';
import { ArrowRightOutlined, EyeOutlined, PlusOutlined, SearchOutlined, ThunderboltOutlined } from '@ant-design/icons';
import { useOverview } from '../lib/queries';
import { useUrlParams } from '../lib/urlState';
import { AccountsContext } from '../lib/session';
import { beijingDay, cnyText, compactNumber as number, compactPrice as price, entryChange, entryPriceText, groupTradePnl, maxPositionPolicyLabel, modeLabel, positivePrice, positionAmount, positionGroups, quoteAsset, ratioText, SNAPSHOT_STALE_MS, todayPnlGroups, todayTradeCounts, trimFractionZeros, type AccountGroup, type PositionGroup } from '../lib/format';
import { ControlResultBanner, DirectionTag, DirectionText, ErrorNotice, FreshnessText, PageTitle, Pnl, SignalButton, SignalChip, StatusTag, type Signal } from '../components/Common';
import { runBatchControls, useControls } from '../components/Controls';
import type { GridConfig, Overview, Pair, Trade } from '../lib/types';
import { OverviewLoading } from '../components/Loading';
import { OverviewHistory } from '../components/OverviewHistory';
import { StrategyMenu, StrategyToggle } from '../components/StrategyMenu';
import { BatchResultDrawer, type BatchReport } from '../components/BatchResultDrawer';
import { PaperGuide } from '../components/PaperGuide';
import { TradesTable } from '../components/DataTables';

import { useAccountGroups, useBeijingDay, useExpired } from '../lib/useNow';
const emptyPairs: Overview['pairs'] = [];
const emptyTrades: Trade[] = [];
const toPair = (e: Overview['pairs'][number]): Pair => ({ symbol: e.snapshot.symbol, status: e.snapshot.status, mode: e.mode, grid_config: e.snapshot.grid_config, scoped: e.scoped });
const StrategyConfigPopover = memo(function StrategyConfigPopover({ symbol, config, quoteUnit }: { symbol: string; config: GridConfig; quoteUnit: string }) {
  return <div className="strategy-config-popover">
    <dl>
      <div><dt>交易方向</dt><dd><DirectionText direction={config.direction} /></dd></div>
      <div><dt>网格间距</dt><dd>{trimFractionZeros(config.grid_interval)}{quoteUnit}</dd></div>
      <div><dt>每格金额</dt><dd>{trimFractionZeros(config.order_amount_usdc)}{quoteUnit}</dd></div>
      <div><dt>买单 / 卖单窗口</dt><dd>{config.buy_window} / {config.sell_window}</dd></div>
      <div><dt>最低价格</dt><dd>{config.min_price == null ? '未设置' : trimFractionZeros(config.min_price)}</dd></div>
      <div><dt>最高价格</dt><dd>{config.max_price == null ? '未设置' : trimFractionZeros(config.max_price)}</dd></div>
      <div><dt>最大持仓名义价值</dt><dd>{config.max_position_usdc == null ? '未设置' : `${trimFractionZeros(config.max_position_usdc)}${quoteUnit}`}</dd></div>
      <div><dt>持仓上限处理方式</dt><dd>{maxPositionPolicyLabel(config)}</dd></div>
      <div><dt>强平距离阈值</dt><dd>{config.min_liquidation_distance_pct == null ? '未设置' : `${trimFractionZeros(config.min_liquidation_distance_pct)}%`}</dd></div>
      <div><dt>仅挂单</dt><dd>{config.post_only ? '开启' : '关闭'}</dd></div>
    </dl>
    <Link to={`/pairs/${symbol}/config`}>查看完整配置 <ArrowRightOutlined /></Link>
  </div>;
});
function OverviewFinanceDisclosure({
  accountList,
  positionList,
  stale,
  compactFinance,
  financesExpanded,
  setFinancesExpanded,
}: {
  accountList: AccountGroup[];
  positionList: PositionGroup[];
  stale: boolean;
  compactFinance: boolean;
  financesExpanded: boolean;
  setFinancesExpanded: (open: boolean) => void;
}) {
  if (!accountList.length && !positionList.length) return null;
  return <details className="finance-disclosure" open={!compactFinance || financesExpanded} onToggle={event => { if (compactFinance) setFinancesExpanded(event.currentTarget.open); }}>
    <summary className="finance-disclosure-summary">
      <span className="finance-disclosure-label">账户与持仓明细<ArrowRightOutlined aria-hidden="true" /></span>
      <span className={`finance-summary-values${stale ? ' is-stale' : ''}`}>
        {accountList.map(group => <span key={`account-${group.key}`}><small>账户权益 / {group.asset}</small><strong>{number(group.equity)}</strong></span>)}
        {positionList.map(group => <span key={`position-${group.key}`}><small>总持仓 / {group.asset}</small><strong>{number(group.total)}</strong></span>)}
      </span>
    </summary>
    <div className={`overview-finances${stale ? ' is-stale' : ''}`}>
      {accountList.length > 0 && <section className="overview-finance-group" aria-labelledby="accounts-heading">
        <div className="section-heading"><h2 id="accounts-heading">账户概览</h2></div>
        <Row gutter={[16, 16]}>
          {accountList.map(group => <Col span={24} key={group.key}><Card className="account-card finance-card" size="small">
            <div className="section-line finance-card-heading"><span>保证金余额</span></div>
            <div className="finance-card-primary"><Statistic value={group.equity ?? '--'} formatter={() => number(group.equity)} suffix={group.asset} />
              <span className="finance-card-note muted">{cnyText(group.cny, true)}</span></div>
            <div className="finance-metrics">
              <div><span>可用余额</span><strong>{number(group.available)} <small>{group.asset}</small></strong></div>
              <div><span>可用比例（按钱包余额）</span><strong>{ratioText(group.ratio, true)}</strong></div>
            </div>
            <div className="finance-metrics" aria-label={`${group.asset}账户资产`}>
              <div><span>钱包余额</span><strong>{number(group.wallet)}</strong></div>
              <div><span>未实现盈亏</span><strong><Pnl value={group.unrealized} /></strong></div>
            </div>
            <div className="muted">{group.count} 个策略共用账户 · 去重展示</div>
          </Card></Col>)}
        </Row>
      </section>}
      {positionList.length > 0 && <section className="overview-finance-group" aria-labelledby="positions-heading">
        <div className="section-heading"><h2 id="positions-heading">持仓概览</h2><span className="muted">按标记价格计算 · 全部策略</span></div>
        <Row gutter={[16, 16]}>
          {positionList.map(group => <Col span={24} key={group.key}>
            <Card className="position-summary finance-card" size="small">
              <div className="section-line finance-card-heading"><span>总持仓金额 / {group.asset}</span></div>
              <div className="finance-card-primary">
                <Statistic value={group.total ?? '--'} formatter={() => number(group.total)} suffix={group.asset} />
                <span className="finance-card-note muted">按标记价格估值</span>
              </div>
              <div className="finance-metrics">
                <div><span>做多持仓金额</span><strong className="position-long">{number(group.long)}</strong></div>
                <div><span>做空持仓金额</span><strong className="position-short">{number(group.short)}</strong></div>
              </div>
              <div className="finance-metrics">
                <div><span>多空比（金额占比）</span><strong>{group.total !== null && group.total > 0 && group.long !== null && group.short !== null ? `${number(group.long / group.total * 100, 1)}% / ${number(group.short / group.total * 100, 1)}%` : '--'}</strong></div>
                <div><span>覆盖策略</span><strong>{group.count} <small>个</small></strong></div>
              </div>
              <div className="position-summary-footer muted">按实际持仓方向统计{group.total === null ? ' · 部分持仓数据不可用' : ''}</div>
            </Card>
          </Col>)}
        </Row>
      </section>}
    </div>
  </details>;
}
function sameGridConfig(a: GridConfig, b: GridConfig) {
  return a === b || (
    a.direction === b.direction &&
    a.grid_interval === b.grid_interval &&
    a.order_amount_usdc === b.order_amount_usdc &&
    a.buy_window === b.buy_window &&
    a.sell_window === b.sell_window &&
    a.min_price === b.min_price &&
    a.max_price === b.max_price &&
    a.max_position_usdc === b.max_position_usdc &&
    a.pause_on_max_position === b.pause_on_max_position &&
    a.cancel_opening_on_max_position === b.cancel_opening_on_max_position &&
    a.min_liquidation_distance_pct === b.min_liquidation_distance_pct &&
    a.post_only === b.post_only
  );
}
const StrategyCard = memo(function StrategyCard({
  entry,
  stale,
  buyCount,
  sellCount,
  controlResult,
  clearResult,
}: {
  entry: Overview['pairs'][number];
  stale: boolean;
  buyCount: number | null;
  sellCount: number | null;
  controlResult?: string;
  clearResult?: (symbol: string) => void;
}) {
  const { snapshot: s } = entry;
  const pair = useMemo(() => toPair(entry), [entry]);
  const quote = quoteAsset(s.symbol), quoteSuffix = quote ? ` / ${quote}` : '', quoteUnit = quote ? ` ${quote}` : '';
  const change = entryChange(s.position, s.ticker.mark_price);
  return <Card className={`strategy-card ${s.grid_config.direction === 'SHORT' ? 'strategy-short' : 'strategy-long'}`}>
    <div className="section-line strategy-card-heading">
      <div className="strategy-identity"><Link className="pair-name" to={`/pairs/${s.symbol}`}>{s.symbol}</Link></div>
      <Space className="strategy-card-actions" size={6}><Popover trigger="click" placement="bottom" title={`${s.symbol} 策略配置`} content={<StrategyConfigPopover symbol={s.symbol} config={s.grid_config} quoteUnit={quoteUnit} />}><Button size="small" icon={<EyeOutlined />} aria-label={`查看 ${s.symbol} 策略配置`} /></Popover><StrategyToggle pair={pair} stale={stale} /><StrategyMenu pair={pair} stale={stale} /></Space>
    </div>
    <div className="strategy-main">
      <div className="strategy-price"><span className="muted">标记价格{quoteSuffix}</span><strong>{positivePrice(s.ticker.mark_price, true)}</strong></div>
      <div className="strategy-tags"><DirectionTag direction={s.grid_config.direction} /><StatusTag status={s.status} /></div>
    </div>
    <div className="strategy-metrics" aria-label={`${s.symbol} 策略指标`}>
      <div><span>持仓金额{quoteSuffix}</span><strong>{number(positionAmount(s))}</strong></div>
      <div><span>持仓数量</span><strong>{price(s.position.size)}</strong></div>
      <div><span>开仓价格</span><strong>{entryPriceText(s.position, true)}</strong></div>
      <div><span>涨跌幅</span><strong className={change.className}>{change.text}</strong></div>
      <div><span>已实现盈亏{quoteSuffix}</span><strong><Pnl value={s.stats.total_realized_pnl} compact /></strong></div>
      <div><span>未实现盈亏{quoteSuffix}</span><strong><Pnl value={s.position.unrealized_pnl} compact /></strong></div>
      <div><span>今日成交</span><strong>{buyCount !== null && sellCount !== null ? buyCount + sellCount : '--'} <small>笔</small></strong></div>
      <div><span>买单 / 卖单</span><strong>{buyCount ?? '--'} <small>/</small> {sellCount ?? '--'}</strong></div>
    </div>
    <ControlResultBanner symbol={s.symbol} result={controlResult} clearResult={clearResult} variant="inline" />
  </Card>;
}, (prev, next) => {
  if (prev.stale !== next.stale || prev.buyCount !== next.buyCount || prev.sellCount !== next.sellCount || prev.controlResult !== next.controlResult || prev.clearResult !== next.clearResult) return false;
  const a = prev.entry, b = next.entry;
  if (a === b) return true;
  const sa = a.snapshot, sb = b.snapshot;
  return a.mode === b.mode && a.scoped === b.scoped && sa.symbol === sb.symbol && sa.status === sb.status &&
    sa.ticker.mark_price === sb.ticker.mark_price &&
    sa.position.size === sb.position.size && sa.position.entry_price === sb.position.entry_price &&
    sa.position.mark_price === sb.position.mark_price && sa.position.unrealized_pnl === sb.position.unrealized_pnl &&
    sa.stats.total_realized_pnl === sb.stats.total_realized_pnl &&
    sameGridConfig(sa.grid_config, sb.grid_config);
});
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
  const setSort = (value: string) => updateFilters({ sort: value });
  const setStatusFilter = (value: string) => updateFilters({ status: value });
  const [batchBusy, setBatchBusy] = useState(false);
  const [report, setReport] = useState<BatchReport | null>(null);
  const [tradesExpanded, setTradesExpanded] = useState(false);
  const compactFinance = Grid.useBreakpoint().md === false;
  const [financesExpanded, setFinancesExpanded] = useState(false);
  const dayNow = useBeijingDay();
  const dataExpired = useExpired(overview.data ? overview.dataUpdatedAt + SNAPSHOT_STALE_MS : Infinity);
  const batchLock = useRef(false);
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => { mounted.current = false; };
  }, []);
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
  const batch = async (action: 'pause' | 'resume') => {
    if (batchLock.current) return;
    batchLock.current = true; setBatchBusy(true);
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
      if (mounted.current) message.error(`批量操作失败：${error instanceof Error ? error.message : String(error)}`);
    } finally { batchLock.current = false; if (mounted.current) setBatchBusy(false); }
  };
  const visible = useMemo(() => {
    const query = search.toUpperCase().trim();
    const matchesStatus = (e: typeof entries[number]) => statusFilter === 'all' || (statusFilter === 'LIVE' ? e.mode === 'LIVE' : e.snapshot.status === statusFilter);
    const filtered = entries.filter(e => e.snapshot.symbol.includes(query) && matchesStatus(e));
    if (sort === 'position') {
      return filtered
        .map(entry => ({ entry, running: entry.snapshot.status === 'RUNNING' ? 1 : 0, amount: positionAmount(entry.snapshot) ?? -1 }))
        .sort((a, b) => (b.running - a.running) || (b.amount - a.amount) || a.entry.snapshot.symbol.localeCompare(b.entry.snapshot.symbol))
        .map(item => item.entry);
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
  const renderStatusButton = (value: string, tier: Signal, label: string, count: number) =>
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
