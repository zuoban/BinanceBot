import { Link, useParams } from 'react-router-dom';
import { Alert, Card, Col, Descriptions, Row, Skeleton, Space, Statistic, Tabs, Tag } from 'antd';
import { usePairSnapshot, usePairs } from '../lib/queries';
import { accountGroups, number, price, quoteAsset } from '../lib/format';
import { ErrorNotice, ModeTag, PageTitle, Pnl, StatusTag } from '../components/Common';
import { PairControls, useControls } from '../components/Controls';
import { DepthChart, HourlyChart } from '../components/Charts';
import { LogsTable, OrdersTable, TradesTable } from '../components/DataTables';
import { HistoryActions } from '../components/HistoryActions';

export default function PairDetail() {
  const { symbol } = useParams();
  const pairs = usePairs();
  const pair = pairs.data?.find(p => p.symbol === symbol);
  const query = usePairSnapshot(pair);
  const controls = useControls();
  const s = query.data, asset = quoteAsset(symbol ?? '');
  const account = s && accountGroups([{ snapshot: s, mode: pair!.mode, scoped: pair!.scoped }])[0];
  const currentPair = pair && s ? { ...pair, status: s.status, grid_config: s.grid_config, mode: s.dry_run ? 'PAPER' as const : s.grid_config.is_testnet ? 'TESTNET' as const : 'LIVE' as const } : pair;
  return <>
    <PageTitle eyebrow="WORKSPACE / STRATEGY" title={symbol ?? '策略详情'} description="行情、网格与成交，随策略实时更新。"
      extra={<>{currentPair && <PairControls pair={currentPair} stale={query.stale || !!query.error} />}<Link className="ant-btn" to={`/pairs/${symbol}/config`}>策略参数</Link></>} />
    <ErrorNotice error={pairs.error || query.error} retry={() => { void pairs.refetch(); void query.refetch(); }} />
    {pairs.data && !pair && <Alert type="warning" title="该交易对不存在或已移除" />}
    {controls.results[symbol ?? ''] && <Alert type="info" title={controls.results[symbol ?? '']} />}
    {pair && <div className="detail-status"><Space><ModeTag mode={currentPair!.mode} /><StatusTag status={currentPair!.status} /><Tag color={query.live ? 'success' : 'warning'}>{query.live ? '实时连接' : 'HTTP 自动刷新'}</Tag></Space><span className="muted">{query.dataUpdatedAt ? `最近同步 ${new Date(query.dataUpdatedAt).toLocaleTimeString('zh-CN')}` : '等待首次快照'}</span></div>}
    {query.stale && s && <Alert type="warning" showIcon title="快照暂未更新，数值可能已过期；仍可撤单并暂停。" />}
    {!s && pair && <Card><Skeleton active /></Card>}
    {s && <>
      <Row gutter={[16, 16]} className="metric-row">
        <Col xs={12} xl={6}><Card><Statistic title={`标记价格 / ${asset}`} value={Number(s.ticker.mark_price) > 0 ? price(s.ticker.mark_price) : '--'} /><div className="muted">最新成交 {price(s.ticker.last_price)}</div></Card></Col>
        <Col xs={12} xl={6}><Card><Statistic title={`已实现盈亏 / ${asset}`} value={s.stats.total_realized_pnl} formatter={() => <Pnl value={s.stats.total_realized_pnl} />} /><div className="muted">扣费后 {number(Number(s.stats.total_realized_pnl) - Number(s.stats.total_commission))}</div></Card></Col>
        <Col xs={12} xl={6}><Card><Statistic title={`持仓未实现 / ${asset}`} value={s.position.unrealized_pnl} formatter={() => <Pnl value={s.position.unrealized_pnl} />} /><div className="muted">持仓数量 {price(s.position.size)}</div></Card></Col>
        <Col xs={12} xl={6}><Card><Statistic title={`账户权益 / ${account?.asset || asset}`} value={account?.equity ?? '--'} precision={2} /><div className="muted">≈ ¥{number(account?.cny)} · 可用 {number(account?.available)}</div></Card></Col>
      </Row>
      <Tabs defaultActiveKey="monitor" items={[
        { key: 'monitor', label: '实时监控', children: <>
          <Row gutter={[20, 20]}><Col xs={24} xl={14}><Card title="今日成交" extra={<span className="muted">北京时间 · 最新小时在左</span>}><HourlyChart snapshot={s} /></Card></Col>
            <Col xs={24} xl={10}><Card title="实时网格深度"><DepthChart snapshot={s} /></Card></Col></Row>
          <Card title="仓位与策略明细" className="detail-card"><Descriptions column={{ xs: 1, sm: 2, lg: 3 }} items={[
            { key: 'direction', label: '方向', children: s.grid_config.direction === 'SHORT' ? '做空' : '做多' },
            { key: 'entry', label: '持仓均价', children: price(s.position.entry_price) },
            { key: 'liquidation', label: '预估强平价', children: Number(s.position.liquidation_price) > 0 ? price(s.position.liquidation_price) : '--' },
            { key: 'leverage', label: '杠杆', children: `${s.position.leverage}×` },
            { key: 'interval', label: '网格间距', children: s.grid_config.grid_interval },
            { key: 'amount', label: '每格金额', children: `${s.grid_config.order_amount_usdc} ${asset}` },
            { key: 'commission', label: '累计手续费', children: number(s.stats.total_commission, 4) },
            { key: 'cycles', label: '历史配对循环', children: s.stats.completed_cycles },
            { key: 'trades', label: '累计成交', children: s.stats.total_trades },
            { key: 'pending', label: '待核实盈亏', children: `${s.stats.pending_pnl_trades} 笔` },
            { key: 'volume', label: '累计成交额', children: `${number(s.stats.total_volume_usdc)} ${asset}` },
            { key: 'available', label: '可用保证金比例', children: account?.equity && account.available !== null ? `${number(account.available / account.equity * 100)}%` : '--' },
          ]} /></Card>
        </> },
        { key: 'orders', label: `挂单 (${s.active_orders.length})`, children: <Card><OrdersTable orders={s.active_orders} mark={s.ticker.mark_price} /></Card> },
        { key: 'trades', label: '成交历史', children: <Card title="最近成交" extra={<HistoryActions />}><Alert type="info" title="此处展示最近成交；导出数据库包含全部交易对、全部模式的完整记录。" /><TradesTable trades={s.recent_trades} /></Card> },
        { key: 'logs', label: '运行日志', children: <Card><LogsTable logs={s.recent_logs} /></Card> },
      ]} />
    </>}
  </>;
}
