import { useContext, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { App, Button, Card, Col, Dropdown, Empty, Input, Row, Select, Space, Statistic, Tag } from 'antd';
import { ArrowRightOutlined, PlusOutlined, ReloadOutlined, SearchOutlined, ThunderboltOutlined } from '@ant-design/icons';
import { useOverview } from '../lib/queries';
import { AccountsContext } from '../lib/session';
import { accountGroups, comparePositionAmount, modeLabel, number, positionAmount, price, quoteAsset, todayCount } from '../lib/format';
import { ErrorNotice, ModeTag, PageTitle, Pnl, StatusTag } from '../components/Common';
import { PairControls, runBatchControls, useControls } from '../components/Controls';
import type { Pair } from '../lib/types';

export default function OverviewPage() {
  const overview = useOverview();
  const accounts = useContext(AccountsContext);
  const { modal, message } = App.useApp();
  const controls = useControls();
  const [search, setSearch] = useState('');
  const [status, setStatus] = useState('ALL');
  const [mode, setMode] = useState('ALL');
  const [sort, setSort] = useState('position');
  const [batchBusy, setBatchBusy] = useState(false);
  const batchLock = useRef(false);
  const entries = overview.data?.pairs ?? [];
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
  const visible = entries.filter(e => e.snapshot.symbol.includes(search.toUpperCase().trim()) && (status === 'ALL' || e.snapshot.status === status) && (mode === 'ALL' || e.mode === mode))
    .sort((a, b) => sort === 'position' ? comparePositionAmount(a.snapshot, b.snapshot) : sort === 'today' ? (todayCount(b.snapshot) ?? -1) - (todayCount(a.snapshot) ?? -1) : sort === 'orders' ? b.snapshot.active_orders.length - a.snapshot.active_orders.length : a.snapshot.symbol.localeCompare(b.snapshot.symbol));
  const today = entries.every(e => todayCount(e.snapshot) !== null) ? entries.reduce((n, e) => n + todayCount(e.snapshot)!, 0) : null;
  return <>
    <PageTitle eyebrow="WORKSPACE / OVERVIEW" title="策略总览" description="账户全貌与每一条运行中的策略。"
      extra={<><Button icon={<ReloadOutlined />} loading={overview.isFetching} onClick={() => void overview.refetch()} aria-label="刷新看板" /><Link to="/pairs/new"><Button type="primary" icon={<PlusOutlined />}>新增交易对</Button></Link></>} />
    <ErrorNotice error={overview.error} retry={() => void overview.refetch()} />
    <div className="overview-banner"><div><span className="eyebrow">YOUR STRATEGIES, AT A GLANCE</span><h2>保持全局视野。</h2><p>{overview.data ? `${pairs.length} 个交易对 · ${pairs.filter(p => p.status === 'RUNNING').length} 个正在运行` : '正在同步策略数据…'}</p></div>
      <div className="banner-stat"><span>今日成交 · 北京时间</span><strong>{overview.data ? today ?? '--' : '--'}<small>笔</small></strong><span>{stale ? '数据暂未更新' : '自动刷新 / 3 秒'}</span></div>
    </div>
    <Row gutter={[16, 16]} className="account-row">
      {accountGroups(accounts).map(group => <Col xs={24} md={12} xl={8} key={group.key}><Card className="account-card" size="small">
        <div className="section-line"><span className="eyebrow">{group.asset} ACCOUNT</span><Tag>{modeLabel[group.mode]}</Tag></div>
        <Statistic title="账户权益" value={group.equity ?? '--'} precision={2} suffix={group.asset} />
        <div className="section-line account-bottom"><span>可用 {number(group.available)}</span><span>{group.cny !== null ? `≈ ¥${number(group.cny)}` : '人民币估值待更新'}</span></div>
        <div className="muted">{group.mode === 'PAPER' ? `${group.count} 个独立模拟账户合计` : `${group.count} 个策略共用账户 · 去重展示`}</div>
      </Card></Col>)}
    </Row>
    <div className="section-heading"><h2>交易策略 <span>{pairs.length.toString().padStart(2, '0')}</span></h2>
      <Dropdown trigger={['click']} disabled={batchBusy || controls.busy.size > 0 || !pairs.length} menu={{ items: [{ key: 'resume', label: '全部恢复', disabled: stale }, { key: 'pause', label: '全部暂停' }], onClick: ({ key }) => void batch(key as 'pause' | 'resume') }}>
        <Button loading={batchBusy} icon={<ThunderboltOutlined />}>批量操作</Button>
      </Dropdown>
    </div>
    <Space wrap className="filter-bar">
      <Input prefix={<SearchOutlined />} placeholder="搜索交易对" aria-label="搜索交易对" allowClear value={search} onChange={e => setSearch(e.target.value)} />
      <Select aria-label="运行状态筛选" value={status} onChange={setStatus} options={[{ value: 'ALL', label: '全部状态' }, { value: 'RUNNING', label: '运行中' }, { value: 'PAUSED', label: '已暂停' }, { value: 'ERROR', label: '异常' }, { value: 'STOPPED', label: '已停止' }]} />
      <Select aria-label="交易模式筛选" value={mode} onChange={setMode} options={[{ value: 'ALL', label: '全部模式' }, ...Object.entries(modeLabel).map(([value, label]) => ({ value, label }))]} />
      <Select aria-label="策略排序" value={sort} onChange={setSort} options={[{ value: 'position', label: '按持仓金额排序' }, { value: 'symbol', label: '按交易对排序' }, { value: 'today', label: '按今日成交排序' }, { value: 'orders', label: '按挂单数排序' }]} />
      {(search || status !== 'ALL' || mode !== 'ALL' || sort !== 'position') && <Button type="text" onClick={() => { setSearch(''); setStatus('ALL'); setMode('ALL'); setSort('position'); }}>清除筛选</Button>}
    </Space>
    <Row gutter={[20, 20]}>
      {visible.map(({ snapshot: s, mode: m, scoped }) => {
        const pair: Pair = { symbol: s.symbol, status: s.status, mode: m, grid_config: s.grid_config, scoped };
        const quote = quoteAsset(s.symbol), result = controls.results[s.symbol];
        return <Col xs={24} lg={12} xxl={8} key={s.symbol}><Card className="strategy-card">
          <div className="section-line"><Space><div className="coin-monogram">{s.symbol.slice(0, 1)}</div><div><Link className="pair-name" to={`/pairs/${s.symbol}`}>{s.symbol}</Link><div className="muted">{s.grid_config.direction === 'SHORT' ? '做空' : '做多'}网格 · {quote}</div></div></Space><StatusTag status={s.status} /></div>
          <div className="strategy-price"><span className="muted">标记价格</span><strong>{Number(s.ticker.mark_price) > 0 ? price(s.ticker.mark_price) : '--'}</strong><ModeTag mode={m} /></div>
          <div className="strategy-metrics"><div><span>已实现盈亏 / {quote}</span><strong><Pnl value={s.stats.total_realized_pnl} /></strong></div><div><span>未实现盈亏 / {quote}</span><strong><Pnl value={s.position.unrealized_pnl} /></strong></div><div><span>今日成交</span><strong>{todayCount(s) ?? '--'} <small>笔</small></strong></div><div><span>买单 / 卖单</span><strong>{s.stats.active_buy_orders} <small>/</small> {s.stats.active_sell_orders}</strong></div><div><span>持仓数量</span><strong>{price(s.position.size)}</strong></div><div><span>持仓金额 / {quote}</span><strong>{number(positionAmount(s))}</strong></div></div>
          <div className="strategy-footer"><PairControls pair={pair} compact stale={stale} /><Link to={`/pairs/${s.symbol}`}><Button type="text" size="small">查看策略 <ArrowRightOutlined /></Button></Link></div>
          {result && <div className={result.startsWith('失败') ? 'negative operation-result' : 'muted operation-result'} role="status">{result}</div>}
        </Card></Col>;
      })}
      {!visible.length && <Col span={24}><Card loading={overview.isPending}><Empty description={pairs.length ? '没有符合筛选条件的策略' : '暂无策略数据'} /></Card></Col>}
    </Row>
  </>;
}
