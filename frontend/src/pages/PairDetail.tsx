import { useContext, useMemo } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { Alert, Button, Card, Col, Descriptions, Row, Statistic, Tabs } from 'antd';
import { SettingOutlined } from '@ant-design/icons';
import { QueryClient, QueryClientContext, useQuery } from '@tanstack/react-query';
import { api, pairPath, pairRoute } from '../lib/api';
import { usePairSnapshot, usePairs } from '../lib/queries';
import { subSignedDecimals } from '../lib/decimal';
import { beijingClockFormat, cnyText, isValidEpochMs, number, positivePrice, quantity, ratioText, trimFractionZeros } from '../lib/format';
import { entryPriceText, quoteAsset, todayTradeCounts } from '../lib/portfolio';
import { AccountsContext } from '../lib/session';
import type { AccountObservation, Log, Pair, Snapshot, Trade } from '../lib/types';
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

export function resolveTrades(snapshot: Snapshot | undefined, fetched: Trade[] | undefined, isError: boolean, fetchedAt: number, snapshotAt: number): Trade[] {
  const recent = snapshot?.recent_trades ?? [];
  if (!snapshot) return recent;
  const scopedRecent = recent.filter(trade => trade.symbol === snapshot.symbol && trade.mode === snapshot.mode);
  if (isError || !Array.isArray(fetched) || !fetchedAt) return scopedRecent;
  const scoped = fetched.filter(trade => trade.symbol === snapshot.symbol && trade.mode === snapshot.mode);
  if ((fetchedAt > snapshotAt && scoped.length === 0) || (snapshotAt >= fetchedAt && snapshot.stats.total_trades === 0)) return [];
  if (snapshotAt > fetchedAt && snapshot.stats.total_trades <= scopedRecent.length) {
    const fetchedById = new Map(scoped.map(trade => [trade.trade_id, trade]));
    return scopedRecent.slice(0, snapshot.stats.total_trades).map(trade => {
      const other = fetchedById.get(trade.trade_id);
      return other && trade.pnl_verified !== other.pnl_verified ? (trade.pnl_verified ? trade : other) : trade;
    });
  }
  const merged = new Map<string, Trade>();
  const [primary, secondary] = fetchedAt >= snapshotAt ? [scoped, scopedRecent] : [scopedRecent, scoped];
  for (const trade of primary) merged.set(trade.trade_id, trade);
  for (const trade of secondary) {
    const existing = merged.get(trade.trade_id);
    merged.set(trade.trade_id, existing ? (existing.pnl_verified !== trade.pnl_verified ? (existing.pnl_verified ? existing : trade) : existing) : trade);
  }
  const entries = [...merged.values()].map(trade => {
    const ts = Date.parse(trade.timestamp);
    return { trade, ts, valid: Number.isFinite(ts) };
  });
  entries.sort((a, b) => (a.valid !== b.valid ? (b.valid ? 1 : -1) : a.valid ? b.ts - a.ts : 0) || b.trade.trade_id.localeCompare(a.trade.trade_id));
  const sorted = entries.map(entry => entry.trade);
  return fetchedAt >= snapshotAt ? sorted : sorted.slice(0, Math.max(snapshot.stats.total_trades, scopedRecent.length));
}

export function resolveLogs(snapshot: Snapshot | undefined, fetched: Log[] | undefined, isError: boolean, fetchedAt: number, snapshotAt: number): Log[] {
  const recent = snapshot?.recent_logs ?? [];
  if (!snapshot || isError || !Array.isArray(fetched) || !fetchedAt) return recent;
  if (!fetched.length) return snapshotAt > fetchedAt ? recent : fetched;
  if (!recent.length) return fetched;
  const head = fetched[0];
  const overlap = recent.findIndex(item => item.timestamp === head.timestamp && item.level === head.level && item.message === head.message);
  if (overlap > 0) return [...recent.slice(0, overlap), ...fetched].slice(0, 250);
  if (overlap === 0) return fetched;
  const tailTs = Date.parse(recent[recent.length - 1].timestamp), headTs = Date.parse(head.timestamp);
  if (fetchedAt >= snapshotAt ? tailTs > headTs : tailTs >= headTs) return [...recent, ...fetched].slice(0, 250);
  return fetchedAt >= snapshotAt || fetched.length >= recent.length ? fetched : recent;
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
    queryFn: ({ signal }) => { if (!pair) throw new Error('该交易对不存在或已移除'); return api<Log[]>(pairPath(pair, 'logs'), { signal }); },
    enabled: !!pair && tab === 'logs',
    refetchInterval: 5_000,
  }, client);
  const trades = useMemo(
    () => (tab === 'trades' ? resolveTrades(snapshot, tradesQuery.data, tradesQuery.isError, tradesQuery.dataUpdatedAt, snapshotUpdatedAt) : []),
    [tab, snapshot, tradesQuery.data, tradesQuery.isError, tradesQuery.dataUpdatedAt, snapshotUpdatedAt],
  );
  const logs = useMemo(
    () => (tab === 'logs' ? resolveLogs(snapshot, logsQuery.data, logsQuery.isError, logsQuery.dataUpdatedAt, snapshotUpdatedAt) : []),
    [tab, snapshot, logsQuery.data, logsQuery.isError, logsQuery.dataUpdatedAt, snapshotUpdatedAt],
  );
  return { trades, logs };
}

const sameScope = (a?: string, b?: string) => (a ?? '') === (b ?? '');

function buildDetailAccountEntries(s: Snapshot | undefined, pair: Pair | undefined, liveAccounts: AccountObservation[], pairs: Pair[] | undefined, dataUpdatedAt: number, snapshotStale: boolean): AccountObservation[] {
  if (!s || !pair) return [];
  const receivedAtMs = dataUpdatedAt > 0 ? dataUpdatedAt : undefined;
  const parsedGeneratedAt = s.generated_at ? Date.parse(s.generated_at) : NaN;
  const skewRef = liveAccounts.find(entry => entry.generated_at_ms !== undefined && entry.received_at_ms !== undefined);
  const generatedAtMs = Number.isFinite(parsedGeneratedAt)
    ? parsedGeneratedAt
    : receivedAtMs !== undefined && skewRef
      ? receivedAtMs + (skewRef.generated_at_ms! - skewRef.received_at_ms!)
      : undefined;
  if (snapshotStale) {
    return [{ mode: s.mode, snapshot: s, account_observation_stale: true, generated_at_ms: generatedAtMs, received_at_ms: receivedAtMs }];
  }
  const ownLive = liveAccounts.find(e => e.snapshot.symbol === pair.symbol);
  const epochAligned = !ownLive || (ownLive.mode === s.mode && sameScope(ownLive.snapshot.account_scope, s.account_scope));
  const siblings = epochAligned
    ? liveAccounts.filter(entry => entry.snapshot.symbol !== pair.symbol && entry.mode === s.mode && sameScope(entry.snapshot.account_scope, s.account_scope))
    : [];
  const targetAsset = s.account.asset || quoteAsset(pair.symbol);
  const siblingSymbols = new Set(siblings.map(entry => entry.snapshot.symbol));
  const pairsEpochStale = pair.mode !== s.mode || (pair.account_scope !== undefined && !sameScope(pair.account_scope, s.account_scope));
  const missingPaperSibling = s.mode === 'PAPER' && (pairsEpochStale || Boolean(pairs?.some(p =>
    p.symbol !== pair.symbol
    && p.mode === 'PAPER'
    && quoteAsset(p.symbol) === targetAsset
    && (p.account_scope === undefined || sameScope(p.account_scope, s.account_scope))
    && !siblingSymbols.has(p.symbol),
  )));
  return [...siblings, { snapshot: s, mode: s.mode, account_observation_stale: missingPaperSibling, generated_at_ms: generatedAtMs, received_at_ms: receivedAtMs }];
}

export default function PairDetail() {
  const { symbol } = useParams(), navigate = useNavigate(), liveAccounts = useContext(AccountsContext);
  const pairs = usePairs(), pair = pairs.data?.find(p => p.symbol === symbol);
  const query = usePairSnapshot(pair), controls = useControls();
  const s = query.data, asset = quoteAsset(symbol ?? ''), stale = query.stale || !!query.error;
  const dayNow = useBeijingDay(s?.generated_at, query.dataUpdatedAt);
  const [tab, setTab] = useUrlParam('tab', 'monitor', detailTabs);
  const { trades, logs } = usePairTabHistory(pair, tab, s, query.dataUpdatedAt);
  const accountEntries = useMemo(() => buildDetailAccountEntries(s, pair, liveAccounts, pairs.data, query.dataUpdatedAt, stale), [s, pair, liveAccounts, pairs.data, query.dataUpdatedAt, stale]);
  const groups = useAccountGroups(accountEntries, !!s && !!pair);
  const account = s ? groups.find(g => g.mode === s.mode && g.asset === (s.account.asset || asset) && sameScope(g.account_scope, s.account_scope)) : undefined;
  const today = useMemo(() => s ? todayTradeCounts(s, dayNow) : null, [s, dayNow]);
  useDocumentTitle(`${positivePrice(s?.ticker.mark_price)} | ${today?.buy ?? '--'}B | ${today?.sell ?? '--'}S`);
  const todayPnl = today && s?.hourly_trade_stats && s.hourly_trade_stats.pending_pnl_count === 0 ? s.hourly_trade_stats.realized_pnl : null;
  const netPnl = s?.stats.pending_pnl_trades === 0 ? subSignedDecimals(s.stats.total_realized_pnl, s.stats.total_commission) : null;
  const currentPair = pair && s ? { ...pair, status: s.status, grid_config: s.grid_config, mode: s.mode, ...(s.account_scope !== undefined && { account_scope: s.account_scope }) } : pair;
  return <div>
    <PageBreadcrumb items={[{ label: '策略总览', to: '/' }, { label: symbol ?? '策略详情' }]} />
    <PageTitle title={symbol ?? '策略详情'} description="行情、网格与成交，随策略实时更新"
      extra={<div className="detail-actions">{currentPair && <PairControls pair={currentPair} stale={stale} />}<Button icon={<SettingOutlined />} disabled={!symbol} onClick={() => { if (symbol) navigate(pairRoute(symbol, true)); }}>策略参数</Button></div>} />
    <ErrorNotice error={pairs.error || query.error} retry={() => { if (pairs.error) void pairs.refetch(); if (pair) void query.refetch(); }} />
    {pairs.data && !pair && !pairs.isFetching && <Alert type="warning" title="该交易对不存在或已移除" />}
    {symbol && <ControlResultBanner symbol={symbol} results={controls.results} clearResult={controls.clearResult} variant="alert" />}
    {pair && <div className="detail-status signal-strip"><StatusTag status={currentPair!.status} /><DirectionTag direction={currentPair!.grid_config.direction} /><ModeTag mode={currentPair!.mode} /><SignalChip tier={query.live ? 'calm' : 'yellow'}>{query.live ? '实时连接' : 'HTTP 自动刷新'}</SignalChip><span className="muted freshness" title={query.dataUpdatedAt > 0 && isValidEpochMs(query.dataUpdatedAt) ? `最近同步 ${beijingClockFormat.format(query.dataUpdatedAt)}` : undefined}>{query.dataUpdatedAt ? <FreshnessText updatedAt={query.dataUpdatedAt} prefix="更新于 " /> : '等待首次快照'}</span></div>}
    {stale && !query.error && s && <Alert type="warning" showIcon title="快照暂未更新，数值可能已过期；仍可撤单并暂停。" />}
    {!s && (pairs.isPending || (!pair && pairs.isFetching) || (pair && query.isPending)) && <DetailLoading />}
    {s && <>
      <Row gutter={[16, 16]} className={`metric-row${stale ? ' is-stale' : ''}`}>
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
          </div><HourlyChart snapshot={s} receivedAt={query.dataUpdatedAt} /></Card></Col>
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
