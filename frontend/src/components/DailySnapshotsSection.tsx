import { memo, useMemo, useState } from 'react';
import { DatePicker } from 'antd';
import dayjs from 'dayjs';
import 'dayjs/locale/zh-cn';
import { Link, useInRouterContext } from 'react-router-dom';
import { pairRoute } from '../lib/api';
import { parseSignedDecimal } from '../lib/decimal';
import { modeLabel, number, numeric, quantity, ratioText, time } from '../lib/format';
import { availableRatio, groupModeAssetPnl, quoteAsset, type ModeAssetPnlGroup } from '../lib/portfolio';
import type { DailySnapshot, Mode } from '../lib/types';
import { ModeTag, Pnl, SideTag } from './Common';
function isNonZero(value: string | number | null | undefined) {
  if (typeof value === 'string') {
    const parsed = parseSignedDecimal(value);
    if (parsed) return parsed.n !== 0n;
  }
  return (numeric(value) ?? 0) !== 0;
}

function SnapshotSymbol({ symbol }: { symbol: string }) {
  const inRouter = useInRouterContext();
  return inRouter
    ? <Link className="daily-symbol daily-symbol-link" to={pairRoute(symbol)}>{symbol}</Link>
    : <span className="daily-symbol">{symbol}</span>;
}

function ScopeLabel({ mode, asset, badge }: { mode: Mode; asset: string; badge: boolean }) {
  if (badge || mode === 'LIVE' || mode === 'UNKNOWN') {
    return <span className="daily-snapshot-scope"><ModeTag mode={mode} /><strong>{asset}</strong></span>;
  }
  return <span className="daily-snapshot-scope-quiet">{modeLabel[mode]} · <strong>{asset}</strong></span>;
}

function DailyAccountsBlock({ accounts }: { accounts: DailySnapshot['accounts'] }) {
  return <section className="daily-snapshot-block" aria-labelledby="daily-accounts-heading">
    <h4 id="daily-accounts-heading">账户概览</h4>
    {!accounts.length ? <div className="daily-snapshot-empty muted">暂无账户记录</div> : accounts.map((acc, idx) => {
      const ratio = availableRatio(numeric(acc.available), numeric(acc.wallet));
      return <div className="daily-snapshot-item" key={`${acc.mode}-${acc.asset}-${acc.account_id ?? idx}`}>
        <div className="daily-snapshot-item-head">
          <ScopeLabel mode={acc.mode} asset={acc.asset} badge />
          <span className="daily-snapshot-meta">{acc.count} 个策略</span>
        </div>
        <div className="daily-snapshot-metrics daily-snapshot-metrics-hero">
          <div className="daily-snapshot-metric">
            <span className="daily-metric-label">账户权益</span>
            <strong>{number(acc.equity)} <small>{acc.asset}</small></strong>
          </div>
          <div className="daily-snapshot-metric">
            <span className="daily-metric-label">未实现盈亏</span>
            <strong><Pnl value={acc.unrealized} /> <small>{acc.asset}</small></strong>
          </div>
        </div>
        <div className="daily-snapshot-metrics daily-snapshot-metrics-ruled">
          <div className="daily-snapshot-metric">
            <span className="daily-metric-label">钱包余额</span>
            <strong>{number(acc.wallet)} <small>{acc.asset}</small></strong>
          </div>
          <div className="daily-snapshot-metric">
            <span className="daily-metric-label">可用余额</span>
            <strong>{number(acc.available)} <small>({ratioText(ratio)})</small></strong>
          </div>
        </div>
      </div>;
    })}
  </section>;
}

function DailyTradesBlock({ date, trades, sharedScope }: { date: string; trades: DailySnapshot['trades']; sharedScope: string | null }) {
  const tradeSummary = useMemo(() => {
    let buy = 0, sell = 0;
    const modes = new Set<DailySnapshot['trades'][number]['mode']>();
    for (const item of trades) {
      buy += item.buy_count;
      sell += item.sell_count;
      modes.add(item.mode);
    }
    const pnlGroups: ModeAssetPnlGroup[] = [...groupModeAssetPnl(trades, item => ({ pnl: item.realized_pnl, pending: item.pending_pnl_count })).values()];
    return { buy, sell, total: buy + sell, pnlGroups, multiMode: modes.size > 1 };
  }, [trades]);
  const singleGroup = tradeSummary.pnlGroups.length === 1 ? tradeSummary.pnlGroups[0] : null;
  const quietSingleScope = singleGroup ? `${singleGroup.mode}:${singleGroup.asset}` === sharedScope : false;
  return <section className="daily-snapshot-block" aria-labelledby="daily-trades-heading">
    <h4 id="daily-trades-heading">成交与实现盈亏（{date}）</h4>
    <div className="daily-snapshot-item">
      <div className="daily-snapshot-item-head">
        {singleGroup
          ? <ScopeLabel mode={singleGroup.mode} asset={singleGroup.asset} badge={!quietSingleScope} />
          : <span className="daily-snapshot-scope-label">{tradeSummary.pnlGroups.length > 1 ? '多账户汇总' : '当日成交汇总'}</span>}
        <span className="daily-snapshot-meta">{trades.length > 0 ? `${trades.length} 个成交合约` : '暂无成交'}</span>
      </div>
      <div className="daily-snapshot-metrics daily-snapshot-metrics-hero">
        <div className="daily-snapshot-metric">
          <span className="daily-metric-label">成交笔数</span>
          <strong>{tradeSummary.total} <small>笔</small></strong>
        </div>
        <div className="daily-snapshot-metric">
          <span className="daily-metric-label">买单 / 卖单</span>
          <strong className="daily-dual-figure">
            <span>{tradeSummary.buy}</span>
            <small>/</small>
            <span>{tradeSummary.sell}</span>
          </strong>
        </div>
      </div>
      <div className="daily-snapshot-pnl-band">
        {!tradeSummary.pnlGroups.length ? (
          <div className="daily-snapshot-pnl-row">
            <span className="daily-metric-label">实现盈亏</span>
            <strong>0.00</strong>
          </div>
        ) : singleGroup ? (
          <div className="daily-snapshot-pnl-row">
            <span className="daily-metric-label">实现盈亏</span>
            <strong><Pnl value={singleGroup.available && singleGroup.pending === 0 ? singleGroup.sumText : null} /> <small>{singleGroup.asset}</small>{singleGroup.pending > 0 && <em className="pending-pnl-warn">（{singleGroup.pending} 笔待同步）</em>}</strong>
          </div>
        ) : (
          <div className="daily-snapshot-pnl-stack">
            <span className="daily-metric-label">实现盈亏</span>
            {tradeSummary.pnlGroups.map(group => (
              <div className="daily-snapshot-pnl-row" key={group.key}>
                <span className="daily-pnl-account">{modeLabel[group.mode]} · {group.asset}</span>
                <strong><Pnl value={group.available && group.pending === 0 ? group.sumText : null} /> <small>{group.asset}</small>{group.pending > 0 && <em className="pending-pnl-warn">（{group.pending} 笔待同步）</em>}</strong>
              </div>
            ))}
          </div>
        )}
      </div>
      {trades.length > 0 && (
        <div className="daily-snapshot-sublist">
          {trades.map(t => {
            const quote = quoteAsset(t.symbol) || t.symbol;
            return <div className="daily-snapshot-subrow" key={`${t.mode}:${t.symbol}`}>
              <span className="daily-subrow-ident">
                <SnapshotSymbol symbol={t.symbol} />
                <small className="daily-subrow-meta">{tradeSummary.multiMode ? `${modeLabel[t.mode]} · ` : ''}买 {t.buy_count} / 卖 {t.sell_count}</small>
              </span>
              <strong className="daily-subrow-value"><Pnl value={t.pending_pnl_count === 0 ? t.realized_pnl : null} /> <small>{quote}</small>{t.pending_pnl_count > 0 && <em className="pending-pnl-warn"> 待同步</em>}</strong>
            </div>;
          })}
        </div>
      )}
    </div>
  </section>;
}

function DailyPositionsBlock({ positions, pairPositions, sharedScope }: { positions: DailySnapshot['positions']; pairPositions: DailySnapshot['pair_positions']; sharedScope: string | null }) {
  const { openPositions, multiMode } = useMemo(() => {
    const modes = new Set<DailySnapshot['pair_positions'][number]['mode']>();
    const open = pairPositions.flatMap(item => {
      const parsed = parseSignedDecimal(item.size);
      const nonZero = parsed ? parsed.n !== 0n : (numeric(item.size) ?? 0) !== 0;
      if (!nonZero) return [];
      modes.add(item.mode);
      const long = parsed ? parsed.n > 0n : (numeric(item.size) ?? 0) > 0;
      const absSize = typeof item.size === 'string' ? item.size.trim().replace(/^[+-]/, '') : Math.abs(numeric(item.size) ?? 0);
      return [{ item, long, absSize }];
    });
    return { openPositions: open, multiMode: modes.size > 1 };
  }, [pairPositions]);
  return <section className="daily-snapshot-block" aria-labelledby="daily-positions-heading">
    <h4 id="daily-positions-heading">持仓概览</h4>
    {!positions.length ? <div className="daily-snapshot-empty muted">暂无持仓记录</div> : positions.map(pos => {
      const hasLong = isNonZero(pos.long), hasShort = isNonZero(pos.short);
      const quietScope = positions.length === 1 && `${pos.mode}:${pos.asset}` === sharedScope;
      return <div className="daily-snapshot-item" key={`${pos.mode}:${pos.asset}`}>
        <div className="daily-snapshot-item-head">
          <ScopeLabel mode={pos.mode} asset={pos.asset} badge={!quietScope} />
          <span className="daily-snapshot-meta">覆盖 {pos.count} 个策略</span>
        </div>
        <div className="daily-snapshot-metrics daily-snapshot-metrics-hero">
          <div className="daily-snapshot-metric">
            <span className="daily-metric-label">总持仓金额</span>
            <strong>{number(pos.total)} <small>{pos.asset}</small></strong>
          </div>
          <div className="daily-snapshot-metric">
            <span className="daily-metric-label">做多 / 做空</span>
            <strong className="daily-dual-figure daily-dual-exposure">
              <span className={hasLong ? 'side-buy' : undefined}>{hasLong && <span className="daily-dir-arrow" aria-hidden="true">↑</span>}{number(pos.long)}</span>
              <small>/</small>
              <span className={hasShort ? 'side-sell' : undefined}>{hasShort && <span className="daily-dir-arrow" aria-hidden="true">↓</span>}{number(pos.short)}</span>
            </strong>
          </div>
        </div>
      </div>;
    })}
    {openPositions.length > 0 && (
      <div className="daily-snapshot-sublist daily-position-sublist" aria-label="持仓明细">
        {openPositions.map(({ item, long, absSize }) => {
          const quote = quoteAsset(item.symbol) || item.symbol;
          return <div className="daily-snapshot-subrow daily-position-row" key={`${item.mode}:${item.symbol}`}>
            <span className="daily-subrow-ident">
              <SnapshotSymbol symbol={item.symbol} />
              {multiMode && <small className="daily-subrow-meta">{modeLabel[item.mode]}</small>}
              <SideTag side={long ? 'BUY' : 'SELL'} label={`${long ? '做多' : '做空'} ${quantity(absSize)}`} />
            </span>
            <strong className="daily-subrow-value">{number(item.position_amount)} <small>{quote}</small></strong>
            <strong className="daily-subrow-pnl"><small>浮盈</small><Pnl value={item.unrealized_pnl} /></strong>
          </div>;
        })}
      </div>
    )}
  </section>;
}
dayjs.locale('zh-cn');

export const DailySnapshotsSection = memo(function DailySnapshotsSection({ snapshots }: { snapshots: DailySnapshot[] }) {
  const [selectedDate, setSelectedDate] = useState<string>();
  const [pickerOpen, setPickerOpen] = useState(false);
  const active = useMemo(() => snapshots.find(s => s.date === selectedDate) ?? snapshots[0], [snapshots, selectedDate]);
  const { dateSet, monthSet, yearSet } = useMemo(() => {
    const dates = new Set<string>(), months = new Set<string>(), years = new Set<string>();
    for (const s of snapshots) {
      dates.add(s.date);
      months.add(s.date.slice(0, 7));
      years.add(s.date.slice(0, 4));
    }
    return { dateSet: dates, monthSet: months, yearSet: years };
  }, [snapshots]);
  const activeValue = useMemo(() => active ? dayjs(active.date, 'YYYY-MM-DD') : null, [active]);
  if (!active) return null;
  const latestDate = snapshots[0].date;
  const sharedScope = active.accounts.length === 1 ? `${active.accounts[0].mode}:${active.accounts[0].asset}` : null;
  return <div className="history-daily" aria-label="每日运行快照">
    <div className="daily-snapshot-toolbar">
      <div className="daily-snapshot-title">
        <h3>每日运行快照</h3>
        <span>每日 00:00 自动归档前一日数据 · 北京时间</span>
      </div>
      <div className="daily-snapshot-controls">
        <span className="daily-snapshot-generated numeric">生成于 {time(active.generated_at)}</span>
        <DatePicker
          className="daily-snapshot-picker"
          classNames={{ popup: { root: 'daily-snapshot-picker-popup' } }}
          aria-label="选择每日快照日期"
          value={activeValue}
          format="YYYY-MM-DD"
          allowClear={false}
          showNow={false}
          inputReadOnly
          open={pickerOpen}
          onOpenChange={setPickerOpen}
          disabledDate={(current, info) => {
            if (info?.type === 'year') return !yearSet.has(current.format('YYYY'));
            if (info?.type === 'month') return !monthSet.has(current.format('YYYY-MM'));
            return !dateSet.has(current.format('YYYY-MM-DD'));
          }}
          onChange={next => {
            if (next) setSelectedDate(next.format('YYYY-MM-DD'));
          }}
          renderExtraFooter={() => (
            <div className="daily-picker-footer">
              <span>已归档 {snapshots.length} 天 · 北京时间</span>
              {active.date !== latestDate && (
                <button type="button" className="daily-picker-latest" onClick={() => { setSelectedDate(latestDate); setPickerOpen(false); }}>
                  最新（{latestDate}）
                </button>
              )}
            </div>
          )}
        />
      </div>
    </div>
    <div className="daily-snapshot-grid">
      <DailyAccountsBlock accounts={active.accounts} />
      <DailyTradesBlock date={active.date} trades={active.trades} sharedScope={sharedScope} />
      <DailyPositionsBlock positions={active.positions} pairPositions={active.pair_positions} sharedScope={sharedScope} />
    </div>
  </div>;
});
