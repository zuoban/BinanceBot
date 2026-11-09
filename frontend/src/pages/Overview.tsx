import { useCallback, useContext, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { App, Button, Card, Col, Dropdown, Empty, Grid, Input, Popover, Row, Select, Space, Statistic } from 'antd';
import { ArrowRightOutlined, CloseOutlined, EyeOutlined, PlusOutlined, SearchOutlined, ThunderboltOutlined } from '@ant-design/icons';
import { useOverview } from '../lib/queries';
import { useUrlParams } from '../lib/urlState';
import { AccountsContext } from '../lib/session';
import { ageText, accountGroups, beijingDay, cnyText, compactNumber as number, compactPrice as price, comparePositionAmount, entryChange, entryPriceText, groupTradePnl, maxPositionPolicyLabel, modeLabel, positivePrice, positionAmount, positionGroups, quoteAsset, ratioText, SNAPSHOT_STALE_MS, todayPnlGroups, todayTradeCounts, trimFractionZeros } from '../lib/format';
import { controlResultMeta, DirectionTag, DirectionText, ErrorNotice, PageTitle, Pnl, SignalButton, SignalChip, signalIcon, StatusTag, type Signal } from '../components/Common';
import { runBatchControls, useControls } from '../components/Controls';
import type { Overview, Pair, Trade } from '../lib/types';
import { OverviewLoading } from '../components/Loading';
import { OverviewHistory } from '../components/OverviewHistory';
import { StrategyMenu, StrategyToggle } from '../components/StrategyMenu';
import { BatchResultDrawer, type BatchReport } from '../components/BatchResultDrawer';
import { PaperGuide } from '../components/PaperGuide';
import { TradesTable } from '../components/DataTables';

import { useNow } from '../lib/useNow';
const emptyPairs: Overview['pairs'] = [];
const emptyTrades: Trade[] = [];
const toPair = (e: { snapshot: { symbol: string; status: Pair['status']; grid_config: Pair['grid_config'] }; mode: Pair['mode']; scoped: boolean }): Pair => ({ symbol: e.snapshot.symbol, status: e.snapshot.status, mode: e.mode, grid_config: e.snapshot.grid_config, scoped: e.scoped });
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
  const now = useNow();
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
  const stale = !!overview.error || (!!overview.data && now - overview.dataUpdatedAt > SNAPSHOT_STALE_MS);
  const pairs = entries.map(toPair);
  let runningCount = 0, pausedCount = 0, stoppedCount = 0, errorCount = 0, liveCount = 0, allPaper = pairs.length > 0;
  for (const p of pairs) {
    if (p.status === 'RUNNING') runningCount++;
    else if (p.status === 'PAUSED') pausedCount++;
    else if (p.status === 'STOPPED') stoppedCount++;
    else if (p.status === 'ERROR') errorCount++;
    if (p.mode === 'LIVE') liveCount++;
    if (p.mode !== 'PAPER') allPaper = false;
  }
  const currentDay = beijingDay(now);
  const tradeCountsBySymbol = useMemo(() => new Map(entries.map(e => [e.snapshot.symbol, todayTradeCounts(e.snapshot, now)])), [entries, currentDay]);
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
    const totalToday = (symbol: string) => { const c = tradeCountsBySymbol.get(symbol); return c ? c.buy + c.sell : -1; };
    const matchesStatus = (e: typeof entries[number]) => statusFilter === 'all' || (statusFilter === 'LIVE' ? e.mode === 'LIVE' : e.snapshot.status === statusFilter);
    return entries.filter(e => e.snapshot.symbol.includes(query) && matchesStatus(e))
      .sort((a, b) => sort === 'position' ? Number(b.snapshot.status === 'RUNNING') - Number(a.snapshot.status === 'RUNNING') || comparePositionAmount(a.snapshot, b.snapshot) : sort === 'today' ? totalToday(b.snapshot.symbol) - totalToday(a.snapshot.symbol) || a.snapshot.symbol.localeCompare(b.snapshot.symbol) : sort === 'orders' ? (b.snapshot.stats.active_buy_orders + b.snapshot.stats.active_sell_orders) - (a.snapshot.stats.active_buy_orders + a.snapshot.stats.active_sell_orders) || a.snapshot.symbol.localeCompare(b.snapshot.symbol) : a.snapshot.symbol.localeCompare(b.snapshot.symbol));
  }, [entries, search, statusFilter, sort, tradeCountsBySymbol]);
  const clearFilters = () => updateFilters({ q: '', sort: 'position', status: 'all' });
  const counts = [...tradeCountsBySymbol.values()];
  const today = counts.every(c => c !== null) ? counts.reduce<{ buy: number; sell: number }>((sum, c) => ({ buy: sum.buy + c.buy, sell: sum.sell + c.sell }), { buy: 0, sell: 0 }) : null;
  const pnlGroups = todayPnlGroups(overview.data?.today_pnl, now);
  const accountList = accountGroups(accounts, now);
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
        {stale && overview.dataUpdatedAt > 0 && <span className="muted freshness">上次更新于 {ageText(overview.dataUpdatedAt, now)}</span>}
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
    {(accountList.length > 0 || positionList.length > 0) && <details className="finance-disclosure" open={!compactFinance || financesExpanded} onToggle={event => { if (compactFinance) setFinancesExpanded(event.currentTarget.open); }}>
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
    </div></details>}
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
      {visible.map(e => {
        const { snapshot: s } = e;
        const pair = toPair(e);
        const quote = quoteAsset(s.symbol), result = controls.results[s.symbol], resultMeta = result ? controlResultMeta(result) : null;
        const tradeCounts = tradeCountsBySymbol.get(s.symbol) ?? null;
        const change = entryChange(s.position, s.ticker.mark_price);
        return <Card key={s.symbol} className={`strategy-card ${s.grid_config.direction === 'SHORT' ? 'strategy-short' : 'strategy-long'}`}>
          <div className="section-line strategy-card-heading"><div className="strategy-identity"><Link className="pair-name" to={`/pairs/${s.symbol}`}>{s.symbol}</Link><DirectionTag direction={s.grid_config.direction} /><StatusTag status={s.status} /></div></div>
          <div className="strategy-main">
            <div className="strategy-price"><span className="muted">标记价格 / {quote}</span><strong>{positivePrice(s.ticker.mark_price, true)}</strong></div>
            <Space className="strategy-card-actions" size={6}><Popover trigger="click" placement="bottomRight" title={`${s.symbol} 策略配置`} content={
              <div className="strategy-config-popover">
                <dl>
                  <div><dt>交易方向</dt><dd><DirectionText direction={s.grid_config.direction} /></dd></div>
                  <div><dt>网格间距</dt><dd>{trimFractionZeros(s.grid_config.grid_interval)} {quote}</dd></div>
                  <div><dt>每格金额</dt><dd>{trimFractionZeros(s.grid_config.order_amount_usdc)} {quote}</dd></div>
                  <div><dt>买单 / 卖单窗口</dt><dd>{s.grid_config.buy_window} / {s.grid_config.sell_window}</dd></div>
                  <div><dt>最低价格</dt><dd>{s.grid_config.min_price == null ? '未设置' : trimFractionZeros(s.grid_config.min_price)}</dd></div>
                  <div><dt>最高价格</dt><dd>{s.grid_config.max_price == null ? '未设置' : trimFractionZeros(s.grid_config.max_price)}</dd></div>
                  <div><dt>最大持仓名义价值</dt><dd>{s.grid_config.max_position_usdc == null ? '未设置' : `${trimFractionZeros(s.grid_config.max_position_usdc)} ${quote}`}</dd></div>
                  <div><dt>持仓上限处理方式</dt><dd>{maxPositionPolicyLabel(s.grid_config)}</dd></div>
                  <div><dt>强平距离阈值</dt><dd>{s.grid_config.min_liquidation_distance_pct == null ? '未设置' : `${trimFractionZeros(s.grid_config.min_liquidation_distance_pct)}%`}</dd></div>
                  <div><dt>仅挂单</dt><dd>{s.grid_config.post_only ? '开启' : '关闭'}</dd></div>
                </dl>
                <Link to={`/pairs/${s.symbol}/config`}>查看完整配置 <ArrowRightOutlined /></Link>
              </div>
            }><Button size="small" icon={<EyeOutlined />} aria-label={`查看 ${s.symbol} 策略配置`} /></Popover><StrategyToggle pair={pair} stale={stale} /><StrategyMenu pair={pair} stale={stale} /></Space>
          </div>
          <div className="strategy-metrics" aria-label={`${s.symbol} 策略指标`}>
            <div><span>持仓金额 / {quote}</span><strong>{number(positionAmount(s))}</strong></div>
            <div><span>持仓数量</span><strong>{price(s.position.size)}</strong></div>
            <div><span>开仓价格</span><strong>{entryPriceText(s.position, true)}</strong></div>
            <div><span>涨跌幅</span><strong className={change.className}>{change.text}</strong></div>
            <div><span>已实现盈亏 / {quote}</span><strong><Pnl value={s.stats.total_realized_pnl} compact /></strong></div>
            <div><span>未实现盈亏 / {quote}</span><strong><Pnl value={s.position.unrealized_pnl} compact /></strong></div>
            <div><span>今日成交</span><strong>{tradeCounts ? tradeCounts.buy + tradeCounts.sell : '--'} <small>笔</small></strong></div>
            <div><span>买单 / 卖单</span><strong>{tradeCounts?.buy ?? '--'} <small>/</small> {tradeCounts?.sell ?? '--'}</strong></div>
          </div>
          {result && resultMeta && <div className={`operation-result signal-${resultMeta.tier}`} role="status"><span className="signal-icon" aria-hidden="true">{signalIcon[resultMeta.tier]}</span><span className="operation-result-text">{result}</span>{!resultMeta.pending && controls.clearResult && <button type="button" className="operation-result-close" aria-label={`关闭 ${s.symbol} 操作提示`} onClick={() => controls.clearResult?.(s.symbol)}><CloseOutlined aria-hidden="true" /></button>}</div>}
        </Card>;
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
      <div id="recent-trades-list"><TradesTable overview trades={displayedTrades} now={now} /></div>
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
