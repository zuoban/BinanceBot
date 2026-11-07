import { useCallback, useContext, useEffect, useLayoutEffect, useRef, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { App, Button, Card, Col, Dropdown, Empty, Input, Popover, Row, Select, Space, Statistic } from 'antd';
import { ArrowDownOutlined, ArrowRightOutlined, ArrowUpOutlined, EyeOutlined, PlusOutlined, SearchOutlined, ThunderboltOutlined } from '@ant-design/icons';
import { useOverview } from '../lib/queries';
import { useUrlParams } from '../lib/urlState';
import { AccountsContext } from '../lib/session';
import { ageText, accountGroups, comparePositionAmount, modeLabel, numeric, number as fixedNumber, positionAmount, positionGroups, price as fixedPrice, quoteAsset, todayCount, todayPnlGroups, todayTradeCounts, trimFractionZeros } from '../lib/format';
import { ErrorNotice, PageTitle, Pnl, SignalButton, SignalChip, StatusTag } from '../components/Common';
import { runBatchControls, useControls } from '../components/Controls';
import type { Pair, Trade } from '../lib/types';
import { OverviewLoading } from '../components/Loading';
import { OverviewHistory } from '../components/OverviewHistory';
import { StrategyMenu, StrategyToggle } from '../components/StrategyMenu';
import { BatchResultDrawer, type BatchReport } from '../components/BatchResultDrawer';
import { PaperGuide } from '../components/PaperGuide';
import { TradesTable } from '../components/DataTables';

const number = (value: unknown, digits?: number) => trimFractionZeros(fixedNumber(value, digits));
const price = (value: unknown) => trimFractionZeros(fixedPrice(value));

function groupTradePnl(trades: Trade[]) {
  const groups = new Map<string, { mode: Trade['mode']; asset: string; total: number; available: boolean }>();
  for (const trade of trades) {
    const asset = quoteAsset(trade.symbol) || trade.symbol;
    const key = `${trade.mode}:${asset}`;
    const group = groups.get(key) ?? { mode: trade.mode, asset, total: 0, available: true };
    const pnl = numeric(trade.realized_pnl);
    if (!trade.pnl_verified || pnl === null) group.available = false;
    else group.total += pnl;
    groups.set(key, group);
  }
  return groups;
}

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
  const addPairLink = <Button href="/pairs/new" ref={setAddLink} icon={<PlusOutlined />} onClick={event => {
    if (event.button === 0 && !event.metaKey && !event.ctrlKey && !event.shiftKey && !event.altKey) {
      event.preventDefault();
      navigate('/pairs/new');
    }
  }}>新增交易对</Button>;
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
  const [now, setNow] = useState(Date.now());
  useEffect(() => { const timer = setInterval(() => setNow(Date.now()), 1000); return () => clearInterval(timer); }, []);
  const batchLock = useRef(false);
  const entries = overview.data?.pairs ?? [];
  const recentTrades = overview.data?.recent_trades ?? [];
  const displayedTrades = recentTrades.slice(0, tradesExpanded ? 20 : 5);
  const buyTrades = displayedTrades.filter(trade => trade.side === 'BUY').length;
  const sellTrades = displayedTrades.filter(trade => trade.side === 'SELL').length;
  const tradePnlGroups = groupTradePnl(displayedTrades);
  const stale = !!overview.error || now - overview.dataUpdatedAt > 10_000;
  const pairs = entries.map(e => ({ symbol: e.snapshot.symbol, status: e.snapshot.status, mode: e.mode, grid_config: e.snapshot.grid_config, scoped: e.scoped } satisfies Pair));
  const batch = async (action: 'pause' | 'resume') => {
    if (batchLock.current) return;
    batchLock.current = true; setBatchBusy(true);
    try {
      let targets: Pair[] = [];
      if (action === 'resume') {
        const refreshed = await overview.refetch();
        if (refreshed.error || !refreshed.data) throw refreshed.error ?? new Error('无法刷新策略列表');
        targets = refreshed.data.pairs.filter(e => e.snapshot.status !== 'RUNNING').map(e => ({ symbol: e.snapshot.symbol, status: e.snapshot.status, mode: e.mode, grid_config: e.snapshot.grid_config, scoped: e.scoped } satisfies Pair));
        if (!targets.length) { message.info('当前没有可恢复的策略'); return; }
      }
      const accepted = await modal.confirm({
        title: `${action === 'resume' ? '恢复' : '暂停'}全部策略`,
        content: action === 'pause' ? '将刷新策略列表并暂停全部交易对，不受当前筛选影响。' : `将操作 ${targets.length} 个交易对，不受当前筛选影响。${targets.some(p => p.mode === 'LIVE') ? '其中包含实盘策略，恢复后会使用真实资金下单。' : ''}`,
        okText: '确认执行', cancelText: '取消',
      });
      if (!accepted) return;
      let selected = targets;
      if (action === 'pause') {
        const refreshed = await overview.refetch();
        if (refreshed.error || !refreshed.data) throw refreshed.error ?? new Error('无法刷新策略列表');
        selected = refreshed.data.pairs.map(e => ({ symbol: e.snapshot.symbol, status: e.snapshot.status, mode: e.mode, grid_config: e.snapshot.grid_config, scoped: e.scoped } satisfies Pair));
        if (!selected.length) { message.info('当前没有可暂停的策略'); return; }
      }
      const outcomes = await runBatchControls(selected, action, controls.run);
      if (outcomes.length) setReport({ action, total: selected.length, outcomes });
      else message.success(`已${action === 'pause' ? '暂停' : '恢复'} ${selected.length} 个策略`);
    } catch (error) {
      message.error(`批量操作失败：${error instanceof Error ? error.message : String(error)}`);
    } finally { batchLock.current = false; setBatchBusy(false); }
  };
  const matchesStatus = (e: typeof entries[number]) => statusFilter === 'all' || (statusFilter === 'LIVE' ? e.mode === 'LIVE' : e.snapshot.status === statusFilter);
  const visible = entries.filter(e => e.snapshot.symbol.includes(search.toUpperCase().trim()) && matchesStatus(e))
    .sort((a, b) => sort === 'position' ? Number(b.snapshot.status === 'RUNNING') - Number(a.snapshot.status === 'RUNNING') || comparePositionAmount(a.snapshot, b.snapshot) : sort === 'today' ? (todayCount(b.snapshot) ?? -1) - (todayCount(a.snapshot) ?? -1) : sort === 'orders' ? (b.snapshot.stats.active_buy_orders + b.snapshot.stats.active_sell_orders) - (a.snapshot.stats.active_buy_orders + a.snapshot.stats.active_sell_orders) : a.snapshot.symbol.localeCompare(b.snapshot.symbol));
  const clearFilters = () => updateFilters({ q: '', sort: 'position', status: 'all' });
  const counts = entries.map(e => todayTradeCounts(e.snapshot));
  const today = counts.every(c => c !== null) ? counts.reduce<{ buy: number; sell: number }>((sum, c) => ({ buy: sum.buy + c!.buy, sell: sum.sell + c!.sell }), { buy: 0, sell: 0 }) : null;
  const pnlGroups = todayPnlGroups(overview.data?.today_pnl);
  return <>
    <PageTitle title="策略运行概览" description="账户、持仓与全部策略的实时状态" />
    <ErrorNotice error={overview.error} retry={() => void overview.refetch()} />
    {overview.data && pairs.length > 0 && pairs.every(p => p.mode === 'PAPER') && <PaperGuide firstSymbol={pairs[0].symbol} />}
    <section className={`overview-banner${stale ? ' is-stale' : ''}`} aria-label="策略运行概览">
      <div className="signal-strip">
        {stale && <SignalChip tier="yellow">数据暂未更新，请检查连接</SignalChip>}
        {stale && overview.dataUpdatedAt > 0 && <span className="muted freshness">上次更新于 {ageText(overview.dataUpdatedAt, now)}</span>}
        {overview.data && pairs.some(p => p.mode === 'LIVE') && <SignalButton tier="orange" active={statusFilter === 'LIVE'} onClick={() => setStatusFilter(statusFilter === 'LIVE' ? 'all' : 'LIVE')}>实盘策略 {pairs.filter(p => p.mode === 'LIVE').length}</SignalButton>}
        {overview.data && pairs.some(p => p.status === 'ERROR') && <SignalButton tier="red" active={statusFilter === 'ERROR'} onClick={() => setStatusFilter(statusFilter === 'ERROR' ? 'all' : 'ERROR')}>异常 {pairs.filter(p => p.status === 'ERROR').length}</SignalButton>}
        {overview.data && pairs.some(p => p.status === 'PAUSED') && <SignalButton tier="blue" active={statusFilter === 'PAUSED'} onClick={() => setStatusFilter(statusFilter === 'PAUSED' ? 'all' : 'PAUSED')}>已暂停 {pairs.filter(p => p.status === 'PAUSED').length}</SignalButton>}
        {overview.data && !stale && pairs.length > 0 && pairs.every(p => p.status === 'RUNNING') && <SignalChip tier="calm">全部策略运行正常</SignalChip>}
        {overview.data && pairs.some(p => p.status === 'STOPPED') && <SignalButton tier="blue" active={statusFilter === 'STOPPED'} onClick={() => setStatusFilter(statusFilter === 'STOPPED' ? 'all' : 'STOPPED')}>已停止 {pairs.filter(p => p.status === 'STOPPED').length}</SignalButton>}
      </div>
      <div className="overview-stats">
        <div className="overview-stat"><span>运行中</span><strong>{overview.data ? pairs.filter(p => p.status === 'RUNNING').length : '--'}</strong></div>
        <div className="overview-stat"><span>策略总数</span><strong>{overview.data ? pairs.length : '--'}</strong></div>
        <div className="overview-stat"><span>今日成交</span><strong>{overview.data && today ? today.buy + today.sell : '--'}<small> 笔</small></strong><em className="overview-trade-breakdown">买单 {overview.data && today ? today.buy : '--'} 笔 / 卖单 {overview.data && today ? today.sell : '--'} 笔</em></div>
        <div className="overview-stat overview-pnl" aria-label="今日已实现盈亏">
          <span>今日已实现盈亏</span>
          {!pnlGroups ? <strong>--</strong> : !pnlGroups.length ? <strong>0</strong> : pnlGroups.map(group => <div className="overview-pnl-group" key={group.key}>
            <strong>{group.total === null ? '--' : <Pnl value={group.total} compact />} <small>{group.asset}</small></strong>
            {group.pending > 0 && <span>盈亏待同步</span>}
          </div>)}
        </div>
      </div>
    </section>
    {overview.isPending && !overview.data ? <OverviewLoading addPairLink={addPairLink} /> : !overview.data ? <section className="strategies-section" aria-labelledby="strategies-heading"><div className="section-heading"><h2 id="strategies-heading">交易策略</h2>{addPairLink}</div></section> : <>
    <div className={`overview-finances${stale ? ' is-stale' : ''}`}>
    <section className="overview-finance-group" aria-labelledby="accounts-heading">
    <div className="section-heading"><h2 id="accounts-heading">账户概览</h2></div>
    <Row gutter={[16, 16]}>
      {accountGroups(accounts).map(group => <Col xs={24} xl={12} key={group.key}><Card className="account-card finance-card" size="small">
        <div className="section-line finance-card-heading"><span>保证金余额</span></div>
        <div className="finance-card-primary"><Statistic value={group.equity ?? '--'} formatter={() => number(group.equity)} suffix={group.asset} />
          <span className="finance-card-note muted">{group.cny !== null ? `≈ ¥${number(group.cny)}` : '人民币估值待更新'}</span></div>
        <div className="finance-metrics">
          <div><span>可用余额</span><strong>{number(group.available)} <small>{group.asset}</small></strong></div>
          <div><span>可用比例（按钱包余额）</span><strong>{group.ratio !== null ? `${number(group.ratio)}%` : '--'}</strong></div>
        </div>
        <div className="finance-metrics" aria-label={`${group.asset}账户资产`}>
          <div><span>钱包余额</span><strong>{number(group.wallet)}</strong></div>
          <div><span>未实现盈亏</span><strong>{group.unrealized === null ? '--' : <Pnl value={group.unrealized} />}</strong></div>
        </div>
        <div className="muted">{group.count} 个策略共用账户 · 去重展示</div>
      </Card></Col>)}
    </Row>
    </section>
    {entries.length > 0 && <section className="overview-finance-group" aria-labelledby="positions-heading">
      <div className="section-heading"><h2 id="positions-heading">持仓概览</h2><span className="muted">按标记价格计算 · 全部策略</span></div>
      <Row gutter={[16, 16]}>
        {positionGroups(entries).map(group => <Col xs={24} xl={12} key={group.key}>
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
      {(search || sort !== 'position' || statusFilter !== 'all') && <Button type="text" onClick={clearFilters}>清除筛选</Button>}
    </div>
    <Row gutter={[20, 20]}>
      {visible.map(({ snapshot: s, mode: m, scoped }) => {
        const pair: Pair = { symbol: s.symbol, status: s.status, mode: m, grid_config: s.grid_config, scoped };
        const quote = quoteAsset(s.symbol), result = controls.results[s.symbol];
        const tradeCounts = todayTradeCounts(s);
        const size = numeric(s.position.size), entry = numeric(s.position.entry_price), mark = numeric(s.ticker.mark_price);
        const validEntry = size !== null && size !== 0 && entry !== null && entry > 0;
        const change = validEntry && mark !== null && mark > 0 ? (mark / entry - 1) * 100 : null;
        const formattedChange = change !== null && Number.isFinite(change) ? number(change, 2) : null;
        const roundedChange = formattedChange === null ? null : Number(formattedChange.replaceAll(',', ''));
        return <Col xs={24} lg={12} xxl={8} key={s.symbol}><Card className={`strategy-card ${s.grid_config.direction === 'SHORT' ? 'strategy-short' : 'strategy-long'}`}>
          <div className="section-line strategy-card-heading"><div className="strategy-identity"><Link className="pair-name" to={`/pairs/${s.symbol}`}>{s.symbol}</Link><span className="strategy-direction" role="img" aria-label={s.grid_config.direction === 'SHORT' ? '策略方向：做空' : '策略方向：做多'}>{s.grid_config.direction === 'SHORT' ? <><ArrowDownOutlined />做空</> : <><ArrowUpOutlined />做多</>}</span></div><Space className="strategy-card-actions" size={2}><StatusTag status={s.status} /><Popover trigger="click" placement="bottomRight" title={`${s.symbol} 策略配置`} content={
            <div className="strategy-config-popover">
              <dl>
                <div><dt>交易方向</dt><dd>{s.grid_config.direction === 'SHORT' ? '做空' : '做多'}</dd></div>
                <div><dt>网格间距</dt><dd>{trimFractionZeros(s.grid_config.grid_interval)} {quote}</dd></div>
                <div><dt>每格金额</dt><dd>{trimFractionZeros(s.grid_config.order_amount_usdc)} {quote}</dd></div>
                <div><dt>买单 / 卖单窗口</dt><dd>{s.grid_config.buy_window} / {s.grid_config.sell_window}</dd></div>
                <div><dt>最低价格</dt><dd>{s.grid_config.min_price == null ? '未设置' : trimFractionZeros(s.grid_config.min_price)}</dd></div>
                <div><dt>最高价格</dt><dd>{s.grid_config.max_price == null ? '未设置' : trimFractionZeros(s.grid_config.max_price)}</dd></div>
                <div><dt>最大持仓名义价值</dt><dd>{s.grid_config.max_position_usdc == null ? '未设置' : `${trimFractionZeros(s.grid_config.max_position_usdc)} ${quote}`}</dd></div>
                <div><dt>持仓上限处理方式</dt><dd>{s.grid_config.pause_on_max_position ? '暂停策略' : s.grid_config.cancel_opening_on_max_position ? '仅撤开仓单' : '限制开仓'}</dd></div>
                <div><dt>仅挂单</dt><dd>{s.grid_config.post_only ? '开启' : '关闭'}</dd></div>
              </dl>
              <Link to={`/pairs/${s.symbol}/config`}>查看完整配置 <ArrowRightOutlined /></Link>
            </div>
          }><Button type="text" size="small" icon={<EyeOutlined />} aria-label={`查看 ${s.symbol} 策略配置`} /></Popover><StrategyToggle pair={pair} stale={stale} /><StrategyMenu pair={pair} stale={stale} /></Space></div>
          <div className="strategy-price"><span className="muted">标记价格 / {quote}</span><strong>{Number(s.ticker.mark_price) > 0 ? price(s.ticker.mark_price) : '--'}</strong></div>
          <div className="strategy-metrics" aria-label={`${s.symbol} 策略指标`}>
            <div><span>持仓金额 / {quote}</span><strong>{number(positionAmount(s))}</strong></div>
            <div><span>持仓数量</span><strong>{price(s.position.size)}</strong></div>
            <div><span>开仓价格</span><strong>{validEntry ? price(entry) : '--'}</strong></div>
            <div><span>涨跌幅</span><strong className={roundedChange === null || roundedChange === 0 ? undefined : roundedChange > 0 ? 'positive' : 'negative'}>{roundedChange === null ? '--' : roundedChange === 0 ? '0%' : `${roundedChange > 0 ? '+' : ''}${formattedChange}%`}</strong></div>
            <div><span>已实现盈亏 / {quote}</span><strong><Pnl value={s.stats.total_realized_pnl} compact /></strong></div>
            <div><span>未实现盈亏 / {quote}</span><strong><Pnl value={s.position.unrealized_pnl} compact /></strong></div>
            <div><span>今日成交</span><strong>{tradeCounts ? tradeCounts.buy + tradeCounts.sell : '--'} <small>笔</small></strong></div>
            <div><span>买单 / 卖单</span><strong>{tradeCounts?.buy ?? '--'} <small>/</small> {tradeCounts?.sell ?? '--'}</strong></div>
          </div>
          {result && <div className={result.startsWith('失败') ? 'negative operation-result' : 'muted operation-result'} role="status">{result}</div>}
        </Card></Col>;
      })}
      {!visible.length && <Col span={24}><Card><Empty description={pairs.length ? '没有符合筛选条件的策略' : '暂无策略数据'}>{pairs.length ? <Button onClick={clearFilters}>清除筛选</Button> : <Link to="/pairs/new"><Button type="primary">新增交易对</Button></Link>}</Empty></Card></Col>}
    </Row>
    </section>
    <Card className="overview-trades" title="最近成交" extra={<span className="muted">全部策略当前模式 · 最近 20 条</span>}>
      <div className="recent-trades-summary" aria-label="当前显示成交汇总">
        <span>当前显示 <strong>{displayedTrades.length}</strong> 笔</span>
        <span>买入 <strong>{buyTrades}</strong> 笔</span>
        <span>卖出 <strong>{sellTrades}</strong> 笔</span>
        <span className="recent-trades-pnl">合计盈亏 {tradePnlGroups.size === 0 ? <strong>--</strong> : [...tradePnlGroups.entries()].map(([key, group]) => <span key={key}> {modeLabel[group.mode]} <strong>{group.available && Number.isFinite(group.total) ? Math.abs(group.total) > 0 && Math.abs(group.total) < 0.01 ? <span className={group.total < 0 ? 'negative numeric' : 'positive numeric'}>{group.total.toLocaleString('en-US', { maximumSignificantDigits: 3 })}</span> : <Pnl value={group.total} compact /> : '待核实'}</strong> {group.asset}</span>)}</span>
      </div>
      <div id="recent-trades-list"><TradesTable overview trades={displayedTrades} /></div>
      {recentTrades.length > 5 && <div className="overview-trades-toggle"><Button type="text" aria-expanded={tradesExpanded} aria-controls="recent-trades-list" onClick={() => setTradesExpanded(value => !value)}>{tradesExpanded ? '收起' : '展开最近 20 条'}</Button></div>}
    </Card>
    <OverviewHistory />
    <BatchResultDrawer report={report} onClose={() => setReport(null)} onRetry={(pair, action) => void controls.run(pair, action)} />
    </>}
  </>;
}
