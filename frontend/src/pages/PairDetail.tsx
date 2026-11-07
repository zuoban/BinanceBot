import { useNavigate, useParams } from 'react-router-dom';
import { Alert, Button, Card, Col, Descriptions, Row, Statistic, Tabs } from 'antd';
import { SettingOutlined } from '@ant-design/icons';
import { usePairSnapshot, usePairs } from '../lib/queries';
import { accountGroups, cnyText, directionLabel, entryPriceText, number, numeric, positivePrice, price, quoteAsset, ageText, ratioText, todayTradeCounts, trimFractionZeros } from '../lib/format';
import { ErrorNotice, ModeTag, PageBreadcrumb, PageTitle, Pnl, SignalChip, StatusTag } from '../components/Common';
import { PairControls, useControls } from '../components/Controls';
import { DepthChart, HourlyChart } from '../components/Charts';
import { LogsTable, OrdersTable, TradesTable } from '../components/DataTables';
import { HistoryActions } from '../components/HistoryActions';
import { DetailLoading } from '../components/Loading';
import { useDocumentTitle } from '../lib/documentTitle';
import { useUrlParam } from '../lib/urlState';

export default function PairDetail() {
  const { symbol } = useParams();
  const navigate = useNavigate();
  const pairs = usePairs();
  const pair = pairs.data?.find(p => p.symbol === symbol);
  const query = usePairSnapshot(pair);
  const controls = useControls();
  const now = query.now;
  const [tab, setTab] = useUrlParam<string>('tab', 'monitor', ['monitor', 'orders', 'trades', 'logs']);
  const s = query.data, asset = quoteAsset(symbol ?? '');
  const account = s && pair ? accountGroups([{ snapshot: s, mode: s.mode, account_observation_stale: query.stale }], now)[0] : undefined;
  const today = s ? todayTradeCounts(s, now) : null;
  useDocumentTitle(`${positivePrice(s?.ticker.mark_price)} | ${today?.buy ?? '--'}B | ${today?.sell ?? '--'}S`);
  const todayPnl = today && s?.hourly_trade_stats && s.hourly_trade_stats.pending_pnl_count === 0 ? s.hourly_trade_stats.realized_pnl : null;
  const realizedPnl = numeric(s?.stats.total_realized_pnl), commission = numeric(s?.stats.total_commission);
  const netPnl = s?.stats.pending_pnl_trades === 0 && realizedPnl !== null && commission !== null ? realizedPnl - commission : null;
  const currentPair = pair && s ? { ...pair, status: s.status, grid_config: s.grid_config, mode: s.mode } : pair;
  return <div>
    <PageBreadcrumb items={[{ label: '策略总览', to: '/' }, { label: symbol ?? '策略详情' }]} />
    <PageTitle title={symbol ?? '策略详情'} description="行情、网格与成交，随策略实时更新"
      extra={<div className="detail-actions">{currentPair && <PairControls pair={currentPair} stale={query.stale || !!query.error} />}<Button icon={<SettingOutlined />} onClick={() => navigate(`/pairs/${symbol}/config`)}>策略参数</Button></div>} />
    <ErrorNotice error={pairs.error || query.error} retry={() => { void pairs.refetch(); void query.refetch(); }} />
    {pairs.data && !pair && <Alert type="warning" title="该交易对不存在或已移除" />}
    {controls.results[symbol ?? ''] && <Alert type={controls.results[symbol ?? ''].startsWith('未提交') ? 'error' : 'info'} title={controls.results[symbol ?? '']} />}
    {pair && <div className="detail-status signal-strip"><StatusTag status={currentPair!.status} /><ModeTag mode={currentPair!.mode} /><SignalChip tier={query.live ? 'calm' : 'yellow'}>{query.live ? '实时连接' : 'HTTP 自动刷新'}</SignalChip><span className="muted freshness" title={query.dataUpdatedAt ? `最近同步 ${new Date(query.dataUpdatedAt).toLocaleTimeString('zh-CN')}` : undefined}>{query.dataUpdatedAt ? `更新于 ${ageText(query.dataUpdatedAt, now)}` : '等待首次快照'}</span></div>}
    {query.stale && s && <Alert type="warning" showIcon title="快照暂未更新，数值可能已过期；仍可撤单并暂停。" />}
    {!s && (pairs.isPending || (pair && query.isPending)) && <DetailLoading />}
    {s && <>
      <Row gutter={[16, 16]} className={`metric-row${query.stale ? ' is-stale' : ''}`}>
        <Col xs={24} sm={12} xl={6}><Card><Statistic title={`标记价格 / ${asset}`} value={positivePrice(s.ticker.mark_price)} /><div className="muted">最新成交 {price(s.ticker.last_price)}</div></Card></Col>
        <Col xs={24} sm={12} xl={6}><Card><Statistic title={`已实现盈亏 / ${asset}`} value={s.stats.total_realized_pnl} formatter={() => <Pnl value={s.stats.total_realized_pnl} />} /><div className="muted">扣费后 {number(netPnl)}</div></Card></Col>
        <Col xs={24} sm={12} xl={6}><Card><Statistic title={`持仓未实现 / ${asset}`} value={s.position.unrealized_pnl} formatter={() => <Pnl value={s.position.unrealized_pnl} />} /><div className="muted">持仓数量 {price(s.position.size)}</div></Card></Col>
        <Col xs={24} sm={12} xl={6}><Card><Statistic title={`账户权益 / ${account?.asset || asset}`} value={account?.equity ?? '--'} formatter={() => number(account?.equity)} /><div className="muted">{cnyText(account?.cny)} · 可用 {number(account?.available)}</div></Card></Col>
      </Row>
      <Tabs className="detail-tabs" activeKey={tab} onChange={setTab} items={[
        { key: 'monitor', label: '实时监控', children: <>
          <Row gutter={[20, 20]}><Col xs={24} xl={14}><Card title="今日成交"><div className="today-trade-summary" aria-label="今日成交汇总">
            <div><span>成交总数</span><strong>{today ? today.buy + today.sell : '--'} <small>笔</small></strong></div>
            <div><span>买入</span><strong>{today?.buy ?? '--'} <small>笔</small></strong></div>
            <div><span>卖出</span><strong>{today?.sell ?? '--'} <small>笔</small></strong></div>
            <div><span>已实现盈亏 / {asset}</span><strong>{todayPnl === null ? '--' : <Pnl value={todayPnl} compact />}</strong>{today && s.hourly_trade_stats!.pending_pnl_count > 0 && <small>盈亏待同步</small>}</div>
          </div><HourlyChart snapshot={s} /></Card></Col>
            <Col xs={24} xl={10}><Card title="实时网格深度" extra={<span className="muted">价格 / {asset} · 数量</span>}><DepthChart snapshot={s} /></Card></Col></Row>
          <Card title="仓位与策略明细" className="detail-card"><Descriptions column={{ xs: 1, sm: 2, lg: 3 }} items={[
            { key: 'direction', label: '方向', children: directionLabel(s.grid_config.direction) },
            { key: 'entry', label: '持仓均价', children: entryPriceText(s.position) },
            { key: 'liquidation', label: '预估强平价', children: positivePrice(s.position.liquidation_price) },
            { key: 'leverage', label: '杠杆', children: `${s.position.leverage}×` },
            { key: 'interval', label: '网格间距', children: `${trimFractionZeros(s.grid_config.grid_interval)} ${asset}` },
            { key: 'amount', label: '每格金额', children: `${trimFractionZeros(s.grid_config.order_amount_usdc)} ${asset}` },
            { key: 'commission', label: '累计手续费', children: number(s.stats.total_commission, 4) },
            { key: 'cycles', label: '历史配对循环', children: s.stats.completed_cycles },
            { key: 'trades', label: '累计成交', children: s.stats.total_trades },
            { key: 'pending', label: '待核实盈亏', children: `${s.stats.pending_pnl_trades} 笔` },
            { key: 'volume', label: '累计成交额', children: `${number(s.stats.total_volume_usdc)} ${asset}` },
            { key: 'available', label: '可用比例（按钱包余额）', children: ratioText(account?.ratio) },
          ]} /></Card>
        </> },
        { key: 'orders', label: `挂单 (${s.active_orders.length})`, children: <Card><OrdersTable key={symbol} orders={s.active_orders} mark={s.ticker.mark_price} /></Card> },
        { key: 'trades', label: '成交历史', children: <Card title="最近成交" extra={pair ? <HistoryActions key={pair.symbol} symbol={pair.symbol} /> : null}><Alert type="info" title={`此处展示当前模式的最近成交；导出和清理作用于 ${symbol ?? ''} 的全部模式。`} /><TradesTable key={symbol} trades={s.recent_trades} /></Card> },
        { key: 'logs', label: '运行日志', children: <Card><LogsTable key={symbol} logs={s.recent_logs} /></Card> },
      ]} />
    </>}
  </div>;
}
