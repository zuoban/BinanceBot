import { useContext, useMemo } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { Alert, Button, Card, Col, Descriptions, Row, Statistic, Tabs } from 'antd';
import { SettingOutlined } from '@ant-design/icons';
import { QueryClient, QueryClientContext, useQuery } from '@tanstack/react-query';
import { api, pairPath, pairRoute } from '../lib/api';
import { usePairSnapshot, usePairs } from '../lib/queries';
import { signedDecimalToNumber, subSignedDecimals } from '../lib/decimal';
import { beijingClockFormat, cnyText, number, positivePrice, quantity, ratioText, trimFractionZeros } from '../lib/format';
import { entryPriceText, quoteAsset, todayTradeCounts } from '../lib/portfolio';
import type { LogItem, Pair, Snapshot, Trade } from '../lib/types';
import { ControlResultBanner, DirectionTag, DirectionText, ErrorNotice, FreshnessText, ModeTag, PageBreadcrumb, PageTitle, Pnl, SignalChip, StatusTag } from '../components/Common';
import { PairControls, useControls } from '../components/Controls';
import { DepthChart, HourlyChart } from '../components/Charts';
import { LogsTable, OrdersTable, TradesTable } from '../components/DataTables';
import { HistoryActions } from '../components/HistoryActions';
import { DetailLoading } from '../components/Loading';
import { useDocumentTitle } from '../lib/documentTitle';
import { useUrlParam } from '../lib/urlState';
import { useAccountGroups, useBeijingDay } from '../lib/useNow';

const detailTabs = ['monitor', 'orders', 'trades', 'logs'] as const;
const fallbackClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });

function resolveTrades(snapshot: Snapshot | undefined, fetched: Trade[] | undefined, isError: boolean, fetchedAt: number, snapshotAt: number): Trade[] {
  const recent = snapshot?.recent_trades ?? [];
  if (!snapshot || isError || !Array.isArray(fetched) || !fetchedAt) return recent;
  if (snapshot.stats.total_trades === 0 && !recent.length && snapshotAt > fetchedAt) return recent;
  const scoped = fetched.filter(trade => trade.mode === snapshot.mode);
  if (fetchedAt >= snapshotAt) return scoped;
  const byId = new Map(recent.map(trade => [trade.trade_id, trade]));
  const seen = new Set(scoped.map(trade => trade.trade_id));
  return [...recent.filter(trade => !seen.has(trade.trade_id)), ...scoped.map(trade => byId.get(trade.trade_id) ?? trade)];
}

function resolveLogs(snapshot: Snapshot | undefined, fetched: LogItem[] | undefined, isError: boolean, fetchedAt: number, snapshotAt: number): LogItem[] {
  const recent = snapshot?.recent_logs ?? [];
  if (!snapshot || isError || !Array.isArray(fetched) || !fetchedAt) return recent;
  if (!fetched.length) return snapshotAt > fetchedAt ? recent : fetched;
  const firstTs = Date.parse(fetched[0].timestamp), lastTs = Date.parse(fetched[fetched.length - 1].timestamp);
  const chronological = fetched.length > 1 && !(firstTs > lastTs);
  const ordered = chronological ? [...fetched].reverse() : fetched;
  if (fetchedAt >= snapshotAt || !recent.length) return ordered;
  const head = ordered[0];
  const overlap = recent.findIndex(item => item.timestamp === head.timestamp && item.level === head.level && item.message === head.message);
  return overlap > 0 ? [...recent.slice(0, overlap), ...ordered] : overlap === 0 ? ordered : ordered.length >= recent.length ? ordered : recent;
}

function usePairTabHistory(pair: Pair | undefined, tab: (typeof detailTabs)[number], snapshot: Snapshot | undefined, snapshotUpdatedAt = 0) {
  const client = useContext(QueryClientContext) ?? fallbackClient;
  const activeMode = snapshot?.mode ?? pair?.mode;
  const tradesQuery = useQuery({
    queryKey: ['snapshot', pair?.symbol, pair?.scoped, 'trades', activeMode],
    queryFn: ({ signal }) => { if (!pair) throw new Error('该交易对不存在或已移除'); return api<Trade[]>(pairPath(pair, 'trades'), { signal }); },
    enabled: !!pair && tab === 'trades',
    refetchInterval: 5_000,
  }, client);
  const logsQuery = useQuery({
    queryKey: ['snapshot', pair?.symbol, pair?.scoped, 'logs', activeMode],
    queryFn: ({ signal }) => { if (!pair) throw new Error('该交易对不存在或已移除'); return api<LogItem[]>(pairPath(pair, 'logs'), { signal }); },
    enabled: !!pair && tab === 'logs',
    refetchInterval: 5_000,
  }, client);
  const trades = useMemo(() => resolveTrades(snapshot, tradesQuery.data, tradesQuery.isError, tradesQuery.dataUpdatedAt, snapshotUpdatedAt), [snapshot, tradesQuery.data, tradesQuery.isError, tradesQuery.dataUpdatedAt, snapshotUpdatedAt]);
  const logs = useMemo(() => resolveLogs(snapshot, logsQuery.data, logsQuery.isError, logsQuery.dataUpdatedAt, snapshotUpdatedAt), [snapshot, logsQuery.data, logsQuery.isError, logsQuery.dataUpdatedAt, snapshotUpdatedAt]);
  return { trades, logs };
}

export default function PairDetail() {
  const { symbol } = useParams(), navigate = useNavigate();
  const pairs = usePairs(), pair = pairs.data?.find(p => p.symbol === symbol);
  const query = usePairSnapshot(pair), controls = useControls(), dayNow = useBeijingDay();
  const [tab, setTab] = useUrlParam('tab', 'monitor', detailTabs);
  const s = query.data, asset = quoteAsset(symbol ?? '');
  const { trades, logs } = usePairTabHistory(pair, tab, s, query.dataUpdatedAt);
  const accountEntries = useMemo(() => s && pair ? [{ snapshot: s, mode: s.mode, account_observation_stale: query.stale }] : [], [s, pair, query.stale]);
  const account = useAccountGroups(accountEntries, !!s && !!pair)[0];
  const today = useMemo(() => s ? todayTradeCounts(s, dayNow) : null, [s, dayNow]);
  useDocumentTitle(`${positivePrice(s?.ticker.mark_price)} | ${today?.buy ?? '--'}B | ${today?.sell ?? '--'}S`);
  const todayPnl = today && s?.hourly_trade_stats && s.hourly_trade_stats.pending_pnl_count === 0 ? s.hourly_trade_stats.realized_pnl : null;
  const netPnl = s?.stats.pending_pnl_trades === 0 ? signedDecimalToNumber(subSignedDecimals(s.stats.total_realized_pnl, s.stats.total_commission)) : null;
  const currentPair = pair && s ? { ...pair, status: s.status, grid_config: s.grid_config, mode: s.mode } : pair;
  return <div>
    <PageBreadcrumb items={[{ label: '策略总览', to: '/' }, { label: symbol ?? '策略详情' }]} />
    <PageTitle title={symbol ?? '策略详情'} description="行情、网格与成交，随策略实时更新"
      extra={<div className="detail-actions">{currentPair && <PairControls pair={currentPair} stale={query.stale || !!query.error} />}<Button icon={<SettingOutlined />} disabled={!symbol} onClick={() => { if (symbol) navigate(pairRoute(symbol, true)); }}>策略参数</Button></div>} />
    <ErrorNotice error={pairs.error || query.error} retry={() => { if (pairs.error) void pairs.refetch(); if (pair) void query.refetch(); }} />
    {pairs.data && !pair && !pairs.isFetching && <Alert type="warning" title="该交易对不存在或已移除" />}
    {symbol && <ControlResultBanner symbol={symbol} results={controls.results} clearResult={controls.clearResult} variant="alert" />}
    {pair && <div className="detail-status signal-strip"><StatusTag status={currentPair!.status} /><DirectionTag direction={currentPair!.grid_config.direction} /><ModeTag mode={currentPair!.mode} /><SignalChip tier={query.live ? 'calm' : 'yellow'}>{query.live ? '实时连接' : 'HTTP 自动刷新'}</SignalChip><span className="muted freshness" title={query.dataUpdatedAt ? `最近同步 ${beijingClockFormat.format(query.dataUpdatedAt)}` : undefined}>{query.dataUpdatedAt ? <FreshnessText updatedAt={query.dataUpdatedAt} prefix="更新于 " /> : '等待首次快照'}</span></div>}
    {query.stale && s && <Alert type="warning" showIcon title="快照暂未更新，数值可能已过期；仍可撤单并暂停。" />}
    {!s && (pairs.isPending || (!pair && pairs.isFetching) || (pair && query.isPending)) && <DetailLoading />}
    {s && <>
      <Row gutter={[16, 16]} className={`metric-row${query.stale ? ' is-stale' : ''}`}>
        <Col xs={12} xl={6} className="metric-col"><Card><Statistic title={`标记价格 / ${asset}`} value={positivePrice(s.ticker.mark_price)} /><div className="muted">最新成交 {positivePrice(s.ticker.last_price)}</div></Card></Col>
        <Col xs={12} xl={6} className="metric-col"><Card><Statistic title={`已实现盈亏 / ${asset}`} value={s.stats.total_realized_pnl} formatter={() => <Pnl value={s.stats.total_realized_pnl} />} /><div className="muted">扣费后 {number(netPnl)}</div></Card></Col>
        <Col xs={12} xl={6} className="metric-col"><Card><Statistic title={`持仓未实现 / ${asset}`} value={s.position.unrealized_pnl} formatter={() => <Pnl value={s.position.unrealized_pnl} />} /><div className="muted">持仓数量 {quantity(s.position.size)}</div></Card></Col>
        <Col xs={12} xl={6} className="metric-col"><Card><Statistic title={`账户权益 / ${account?.asset || asset}`} value={account?.equity ?? '--'} formatter={() => number(account?.equity)} /><div className="muted">{cnyText(account?.cny)} · 可用 {number(account?.available)}</div></Card></Col>
      </Row>
      <Tabs className="detail-tabs" activeKey={tab} onChange={key => { const next = detailTabs.find(t => t === key); if (next) setTab(next); }} items={[
        { key: 'monitor', label: '实时监控', children: <>
          <Row gutter={[20, 20]}><Col xs={24} xl={14}><Card title="今日成交"><div className="today-trade-summary" aria-label="今日成交汇总">
            <div><span>成交总数</span><strong>{today ? today.buy + today.sell : '--'} <small>笔</small></strong></div>
            <div><span>买入</span><strong>{today?.buy ?? '--'} <small>笔</small></strong></div>
            <div><span>卖出</span><strong>{today?.sell ?? '--'} <small>笔</small></strong></div>
            <div><span>已实现盈亏 / {asset}</span><strong><Pnl value={todayPnl} compact /></strong>{today && s.hourly_trade_stats!.pending_pnl_count > 0 && <small className="pending-pnl-warn">盈亏待同步</small>}</div>
          </div><HourlyChart snapshot={s} /></Card></Col>
            <Col xs={24} xl={10}><Card title="实时网格深度" extra={<span className="muted">价格 / {asset} · 数量</span>}><DepthChart snapshot={s} /></Card></Col></Row>
          <Card title="仓位与策略明细" className="detail-card"><Descriptions column={{ xs: 1, sm: 2, lg: 3 }} items={[
            { key: 'direction', label: '方向', children: <DirectionText direction={s.grid_config.direction} /> },
            { key: 'entry', label: '持仓均价', children: entryPriceText(s.position) },
            { key: 'liquidation', label: '预估强平价', children: positivePrice(s.position.liquidation_price) },
            { key: 'leverage', label: '杠杆', children: `${s.position.leverage}×` },
            { key: 'interval', label: '网格间距', children: `${trimFractionZeros(s.grid_config.grid_interval)} ${asset}` },
            { key: 'amount', label: '每格金额', children: `${trimFractionZeros(s.grid_config.order_amount_usdc)} ${asset}` },
            { key: 'commission', label: '累计手续费', children: number(s.stats.total_commission, 4) },
            { key: 'cycles', label: '历史配对循环', children: s.stats.completed_cycles },
            { key: 'trades', label: '累计成交', children: s.stats.total_trades },
            { key: 'pending', label: '待核实盈亏', children: s.stats.pending_pnl_trades > 0 ? <span className="pending-pnl-warn">{s.stats.pending_pnl_trades} 笔</span> : `${s.stats.pending_pnl_trades} 笔` },
            { key: 'volume', label: '累计成交额', children: `${number(s.stats.total_volume_usdc)} ${asset}` },
            { key: 'available', label: '可用比例（按钱包余额）', children: ratioText(account?.ratio) },
          ]} /></Card>
        </> },
        { key: 'orders', label: `挂单 (${s.active_orders.length})`, children: <Card><OrdersTable key={symbol} orders={s.active_orders} mark={s.ticker.mark_price} /></Card> },
        { key: 'trades', label: '成交历史', children: <Card title="最近成交" extra={pair ? <HistoryActions key={pair.symbol} symbol={pair.symbol} /> : null}><Alert type="info" title={`此处展示当前模式的最近成交；导出和清理作用于 ${symbol ?? ''} 的全部模式。`} /><TradesTable key={symbol} trades={trades} /></Card> },
        { key: 'logs', label: '运行日志', children: <Card><LogsTable key={symbol} logs={logs} /></Card> },
      ]} />
    </>}
  </div>;
}
