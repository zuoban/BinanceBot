import { memo, useMemo } from 'react';
import { Link } from 'react-router-dom';
import { Button, Card, Popover, Space } from 'antd';
import { ArrowRightOutlined, EyeOutlined } from '@ant-design/icons';
import { pairRoute } from '../lib/api';
import { sameGridConfig } from '../lib/configDiff';
import { compactNumber as number, maxPositionPolicyLabel, positivePrice, quantity, trimFractionZeros } from '../lib/format';
import { entryChange, entryPriceText, positionAmountText, quoteAsset } from '../lib/portfolio';
import type { GridConfig, Overview, Pair } from '../lib/types';
import { ControlResultBanner, DirectionTag, DirectionText, Pnl, StatusTag } from './Common';
import { StrategyMenu, StrategyToggle } from './StrategyMenu';

export const toPair = (e: Overview['pairs'][number]): Pair => ({
  symbol: e.snapshot.symbol,
  status: e.snapshot.status,
  mode: e.mode,
  grid_config: e.snapshot.grid_config,
  scoped: e.scoped,
  ...(e.snapshot.account_scope !== undefined && { account_scope: e.snapshot.account_scope }),
});

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
    <Link to={pairRoute(symbol, true)}>查看完整配置 <ArrowRightOutlined /></Link>
  </div>;
});

export const StrategyCard = memo(function StrategyCard({
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
  const quote = quoteAsset(s.symbol, s.account.asset), quoteSuffix = quote ? ` / ${quote}` : '', quoteUnit = quote ? ` ${quote}` : '';
  const change = entryChange(s.position, s.ticker.mark_price);
  return <Card className="strategy-card">
    <div className="section-line strategy-card-heading">
      <div className="strategy-identity"><Link className="pair-name" to={pairRoute(s.symbol)}>{s.symbol}</Link></div>
      <Space className="strategy-card-actions"><Popover trigger="click" placement="bottom" title={`${s.symbol} 策略配置`} content={<StrategyConfigPopover symbol={s.symbol} config={s.grid_config} quoteUnit={quoteUnit} />}><Button size="small" icon={<EyeOutlined />} aria-label={`查看 ${s.symbol} 策略配置`} /></Popover><StrategyToggle pair={pair} stale={stale} /><StrategyMenu pair={pair} stale={stale} /></Space>
    </div>
    <div className="strategy-main">
      <div className="strategy-price"><span className="muted">标记价格{quoteSuffix}</span><strong>{positivePrice(s.ticker.mark_price, true)}</strong></div>
      <div className="strategy-tags"><DirectionTag direction={s.grid_config.direction} /><StatusTag status={s.status} /></div>
    </div>
    <div className="strategy-metrics" aria-label={`${s.symbol} 策略指标`}>
      <div><span>持仓金额{quoteSuffix}</span><strong>{number(positionAmountText(s))}</strong></div>
      <div><span>持仓数量</span><strong>{quantity(s.position.size)}</strong></div>
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
  return a.mode === b.mode && a.scoped === b.scoped && sa.account_scope === sb.account_scope && sa.symbol === sb.symbol && sa.status === sb.status &&
    sa.account.asset === sb.account.asset &&
    sa.ticker.mark_price === sb.ticker.mark_price &&
    sa.position.size === sb.position.size && sa.position.entry_price === sb.position.entry_price &&
    sa.position.mark_price === sb.position.mark_price && sa.position.unrealized_pnl === sb.position.unrealized_pnl &&
    sa.stats.total_realized_pnl === sb.stats.total_realized_pnl &&
    sameGridConfig(sa.grid_config, sb.grid_config);
});
