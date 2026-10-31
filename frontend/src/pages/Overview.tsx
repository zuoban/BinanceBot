import { useContext, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { App, Button, Card, Col, Dropdown, Empty, Input, Popover, Row, Select, Space, Statistic } from 'antd';
import { ArrowDownOutlined, ArrowRightOutlined, ArrowUpOutlined, EyeOutlined, PlusOutlined, SearchOutlined, ThunderboltOutlined } from '@ant-design/icons';
import { useOverview } from '../lib/queries';
import { AccountsContext } from '../lib/session';
import { accountGroups, comparePositionAmount, modeLabel, number as fixedNumber, positionAmount, positionGroups, price as fixedPrice, quoteAsset, todayCount, todayPnlGroups, todayTradeCounts, trimFractionZeros } from '../lib/format';
import { ErrorNotice, ModeTag, PageTitle, Pnl, StatusTag } from '../components/Common';
import { runBatchControls, useControls } from '../components/Controls';
import type { Pair } from '../lib/types';
import { OverviewLoading } from '../components/Loading';
import { OverviewHistory } from '../components/OverviewHistory';
import { StrategyMenu } from '../components/StrategyMenu';
import { TradesTable } from '../components/DataTables';

const number = (value: unknown, digits?: number) => trimFractionZeros(fixedNumber(value, digits));
const price = (value: unknown) => trimFractionZeros(fixedPrice(value));

export default function OverviewPage() {
  const overview = useOverview();
  const accounts = useContext(AccountsContext);
  const { modal, message } = App.useApp();
  const controls = useControls();
  const [search, setSearch] = useState('');
  const [sort, setSort] = useState('position');
  const [batchBusy, setBatchBusy] = useState(false);
  const [tradesExpanded, setTradesExpanded] = useState(false);
  const batchLock = useRef(false);
  const entries = overview.data?.pairs ?? [];
  const recentTrades = entries.flatMap(entry => entry.snapshot.recent_trades).sort((a, b) => Date.parse(b.timestamp) - Date.parse(a.timestamp)).slice(0, 20);
  const stale = !!overview.error || Date.now() - overview.dataUpdatedAt > 10_000;
  const pairs = entries.map(e => ({ symbol: e.snapshot.symbol, status: e.snapshot.status, mode: e.mode, grid_config: e.snapshot.grid_config, scoped: e.scoped } satisfies Pair));
  const batch = async (action: 'pause' | 'resume') => {
    if (batchLock.current) return;
    batchLock.current = true; setBatchBusy(true);
    try {
      const targets = pairs.filter(p => action === 'resume' ? p.status !== 'RUNNING' : p.status === 'RUNNING');
      if (!targets.length) return;
      const accepted = await modal.confirm({
        title: `${action === 'resume' ? '恢复' : '暂停'}全部策略`,
        content: `将操作 ${targets.length} 个交易对，不受当前筛选影响。${action === 'resume' && targets.some(p => p.mode === 'LIVE') ? '其中包含实盘策略，恢复后会使用真实资金下单。' : ''}`,
        okText: '确认执行', cancelText: '取消',
      });
      if (!accepted) return;
      await runBatchControls(targets, action, controls.run);
    } catch (error) {
      message.error(`批量操作失败：${error instanceof Error ? error.message : String(error)}`);
    } finally { batchLock.current = false; setBatchBusy(false); }
  };
  const visible = entries.filter(e => e.snapshot.symbol.includes(search.toUpperCase().trim()))
    .sort((a, b) => sort === 'position' ? comparePositionAmount(a.snapshot, b.snapshot) : sort === 'today' ? (todayCount(b.snapshot) ?? -1) - (todayCount(a.snapshot) ?? -1) : sort === 'orders' ? b.snapshot.active_orders.length - a.snapshot.active_orders.length : a.snapshot.symbol.localeCompare(b.snapshot.symbol));
  const clearFilters = () => { setSearch(''); setSort('position'); };
  const counts = entries.map(e => todayTradeCounts(e.snapshot));
  const today = counts.every(c => c !== null) ? counts.reduce<{ buy: number; sell: number }>((sum, c) => ({ buy: sum.buy + c!.buy, sell: sum.sell + c!.sell }), { buy: 0, sell: 0 }) : null;
  const pnlGroups = todayPnlGroups(overview.data?.today_pnl);
  return <>
    <PageTitle title="策略总览"
      extra={<Link to="/pairs/new"><Button icon={<PlusOutlined />}>新增交易对</Button></Link>} />
    <ErrorNotice error={overview.error} retry={() => void overview.refetch()} />
    <div className="overview-banner" aria-label="策略运行概览">
      <div className="overview-summary"><h2>策略运行概览</h2><p>{overview.dataUpdatedAt ? `最近同步 ${new Date(overview.dataUpdatedAt).toLocaleTimeString('zh-CN', { timeZone: 'Asia/Shanghai', hour12: false })} · 北京时间` : '等待首次同步'}</p><span className={stale ? 'sync-warning' : 'sync-ok'}>{stale ? '数据暂未更新，请检查连接' : '自动刷新 / 3 秒'}</span></div>
      <div className="overview-stats">
        <div className="overview-stat"><span>运行中</span><strong>{overview.data ? pairs.filter(p => p.status === 'RUNNING').length : '--'}</strong></div>
        <div className="overview-stat"><span>策略总数</span><strong>{overview.data ? pairs.length : '--'}</strong></div>
        <div className="overview-stat"><span>今日成交</span><strong>{overview.data && today ? today.buy + today.sell : '--'}<small> 笔</small></strong></div>
        <div className="overview-stat overview-pnl" aria-label="今日已实现盈亏">
          <span>今日已实现盈亏</span>
          {!pnlGroups ? <strong>--</strong> : !pnlGroups.length ? <strong>0</strong> : pnlGroups.map(group => <div className="overview-pnl-group" key={group.key}>
            <strong>{group.total === null ? '--' : <Pnl value={group.total} compact />} <small>{group.asset}</small></strong>
            {group.pending > 0 && <span>盈亏待同步</span>}
          </div>)}
        </div>
        <div className="overview-trade-breakdown"><span>买单 {overview.data && today ? today.buy : '--'} 笔 / 卖单 {overview.data && today ? today.sell : '--'} 笔</span>{overview.data && <span className="overview-status-links"><span>已暂停 {pairs.filter(p => p.status === 'PAUSED').length}</span><span>异常 {pairs.filter(p => p.status === 'ERROR').length}</span></span>}</div>
      </div>
    </div>
    {overview.isPending && !overview.data ? <OverviewLoading /> : overview.data && <>
    <div className="overview-finances">
    <section className="overview-finance-group" aria-labelledby="accounts-heading">
    <div className="section-heading"><h2 id="accounts-heading">账户概览</h2></div>
    <Row gutter={[16, 16]} className="account-row">
      {accountGroups(accounts).map(group => <Col xs={24} xl={12} key={group.key}><Card className="account-card" size="small">
        <div className="section-line account-card-heading"><span>保证金余额</span><ModeTag mode={group.mode} /></div>
        <div className="account-equity-line"><Statistic value={group.equity ?? '--'} formatter={() => number(group.equity)} suffix={group.asset} />
          <span className="account-cny muted">{group.cny !== null ? `≈ ¥${number(group.cny)}` : '人民币估值待更新'}</span></div>
        <div className="account-availability">
          <div><span>可用余额</span><strong>{number(group.available)} <small>{group.asset}</small></strong></div>
          <div><span>可用比例</span><strong>{group.available !== null && group.wallet !== null && group.wallet > 0 ? `${number(group.available / group.wallet * 100)}%` : '--'}</strong></div>
        </div>
        <div className="account-assets" aria-label={`${group.asset}账户资产`}>
          <div><span>钱包余额</span><strong>{number(group.wallet)}</strong></div>
          <div><span>未实现盈亏</span><strong>{group.unrealized === null ? '--' : <Pnl value={group.unrealized} />}</strong></div>
        </div>
        {group.mode !== 'PAPER' && <div className="muted">{group.count} 个策略共用账户 · 去重展示</div>}
      </Card></Col>)}
    </Row>
    </section>
    {entries.length > 0 && <section className="overview-finance-group" aria-labelledby="positions-heading">
      <div className="section-heading"><h2 id="positions-heading">持仓概览</h2><span className="muted">按标记价格计算 · 全部策略</span></div>
      <Row gutter={[16, 16]} className="position-row">
        {positionGroups(entries).map(group => <Col xs={24} xl={12} key={group.key}>
          <Card className="position-summary" size="small">
            <div className="position-summary-metrics">
              <div className="position-total"><span>总持仓金额 / {group.asset}</span><strong>{number(group.total)}</strong></div>
              <div><span>做多持仓金额</span><strong className="position-long">{number(group.long)}</strong></div>
              <div><span>做空持仓金额</span><strong className="position-short">{number(group.short)}</strong></div>
              <div><span>多空比（金额占比）</span><strong>{group.total !== null && group.total > 0 && group.long !== null && group.short !== null ? `${number(group.long / group.total * 100, 1)}% / ${number(group.short / group.total * 100, 1)}%` : '--'}</strong></div>
            </div>
            <div className="section-line position-summary-footer"><span className="muted">按实际持仓方向统计{group.total === null ? ' · 部分持仓数据不可用' : ''}</span><ModeTag mode={group.mode} /></div>
          </Card>
        </Col>)}
      </Row>
    </section>}
    </div>
    <section className="strategies-section" aria-labelledby="strategies-heading">
    <div className="section-heading"><h2 id="strategies-heading">交易策略</h2>
      <Dropdown trigger={['click']} disabled={batchBusy || controls.busy.size > 0 || !pairs.length} menu={{ items: [{ key: 'resume', label: '全部恢复', disabled: stale }, { key: 'pause', label: '全部暂停' }], onClick: ({ key }) => void batch(key as 'pause' | 'resume') }}>
        <Button loading={batchBusy} icon={<ThunderboltOutlined />}>全部策略操作</Button>
      </Dropdown>
    </div>
    <div className="filter-bar">
      <Input prefix={<SearchOutlined />} placeholder="搜索交易对" aria-label="搜索交易对" allowClear value={search} onChange={e => setSearch(e.target.value)} />
      <Select aria-label="策略排序" value={sort} onChange={setSort} options={[{ value: 'position', label: '按持仓金额排序' }, { value: 'symbol', label: '按交易对排序' }, { value: 'today', label: '按今日成交排序' }, { value: 'orders', label: '按挂单数排序' }]} />
      {(search || sort !== 'position') && <Button type="text" onClick={clearFilters}>清除筛选</Button>}
    </div>
    <Row gutter={[20, 20]}>
      {visible.map(({ snapshot: s, mode: m, scoped }) => {
        const pair: Pair = { symbol: s.symbol, status: s.status, mode: m, grid_config: s.grid_config, scoped };
        const quote = quoteAsset(s.symbol), result = controls.results[s.symbol];
        return <Col xs={24} lg={12} xxl={8} key={s.symbol}><Card className={`strategy-card ${s.grid_config.direction === 'SHORT' ? 'strategy-short' : 'strategy-long'}`}>
          <div className="section-line"><div className="strategy-identity"><Link className="pair-name" to={`/pairs/${s.symbol}`}>{s.symbol}</Link><span className="strategy-direction" role="img" aria-label={s.grid_config.direction === 'SHORT' ? '策略方向：做空' : '策略方向：做多'}>{s.grid_config.direction === 'SHORT' ? <><ArrowDownOutlined />做空</> : <><ArrowUpOutlined />做多</>}</span><ModeTag mode={m} /></div><Space size={2}><StatusTag status={s.status} /><Popover trigger="click" placement="bottomRight" title={`${s.symbol} 策略配置`} content={
            <div className="strategy-config-popover">
              <dl>
                <div><dt>交易方向</dt><dd>{s.grid_config.direction === 'SHORT' ? '做空' : '做多'}</dd></div>
                <div><dt>运行模式</dt><dd>{modeLabel[m]}</dd></div>
                <div><dt>网格间距</dt><dd>{trimFractionZeros(s.grid_config.grid_interval)} {quote}</dd></div>
                <div><dt>每格金额</dt><dd>{trimFractionZeros(s.grid_config.order_amount_usdc)} {quote}</dd></div>
                <div><dt>买单 / 卖单窗口</dt><dd>{s.grid_config.buy_window} / {s.grid_config.sell_window}</dd></div>
                <div><dt>最低价格</dt><dd>{s.grid_config.min_price == null ? '未设置' : trimFractionZeros(s.grid_config.min_price)}</dd></div>
                <div><dt>最高价格</dt><dd>{s.grid_config.max_price == null ? '未设置' : trimFractionZeros(s.grid_config.max_price)}</dd></div>
                <div><dt>最大持仓名义价值</dt><dd>{s.grid_config.max_position_usdc == null ? '未设置' : `${trimFractionZeros(s.grid_config.max_position_usdc)} ${quote}`}</dd></div>
                <div><dt>达到持仓上限时</dt><dd>{s.grid_config.pause_on_max_position ? '暂停策略' : '限制开仓'}</dd></div>
                <div><dt>仅挂单</dt><dd>{s.grid_config.post_only ? '开启' : '关闭'}</dd></div>
              </dl>
              <Link to={`/pairs/${s.symbol}/config`}>查看完整配置 <ArrowRightOutlined /></Link>
            </div>
          }><Button type="text" size="small" icon={<EyeOutlined />} aria-label={`查看 ${s.symbol} 策略配置`} /></Popover><StrategyMenu pair={pair} stale={stale} /></Space></div>
          <div className="strategy-price"><span className="muted">标记价格</span><strong>{Number(s.ticker.mark_price) > 0 ? price(s.ticker.mark_price) : '--'}</strong></div>
          <div className="strategy-metrics"><div><span>已实现盈亏 / {quote}</span><strong><Pnl value={s.stats.total_realized_pnl} compact /></strong></div><div><span>未实现盈亏 / {quote}</span><strong><Pnl value={s.position.unrealized_pnl} compact /></strong></div><div><span>持仓金额 / {quote}</span><strong>{number(positionAmount(s))}</strong></div><div><span>持仓数量</span><strong>{price(s.position.size)}</strong></div><div><span>今日成交</span><strong>{todayCount(s) ?? '--'} <small>笔</small></strong></div><div><span>买单 / 卖单</span><strong>{s.stats.active_buy_orders} <small>/</small> {s.stats.active_sell_orders}</strong></div></div>
          {result && <div className={result.startsWith('失败') ? 'negative operation-result' : 'muted operation-result'} role="status">{result}</div>}
        </Card></Col>;
      })}
      {!visible.length && <Col span={24}><Card><Empty description={pairs.length ? '没有符合筛选条件的策略' : '暂无策略数据'}>{pairs.length ? <Button onClick={clearFilters}>清除筛选</Button> : <Link to="/pairs/new"><Button type="primary">新增交易对</Button></Link>}</Empty></Card></Col>}
    </Row>
    </section>
    <Card className="overview-trades" title="最近成交" extra={<span className="muted">全部策略当前模式 · 最近 20 条</span>}>
      <div id="recent-trades-list"><TradesTable overview trades={recentTrades.slice(0, tradesExpanded ? 20 : 5)} /></div>
      {recentTrades.length > 5 && <div className="overview-trades-toggle"><Button type="text" aria-expanded={tradesExpanded} aria-controls="recent-trades-list" onClick={() => setTradesExpanded(value => !value)}>{tradesExpanded ? '收起' : '展开最近 20 条'}</Button></div>}
    </Card>
    <OverviewHistory />
    </>}
  </>;
}
