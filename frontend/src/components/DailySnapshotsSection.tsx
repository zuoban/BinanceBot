import { memo, useMemo, useState } from 'react';
import { Select } from 'antd';
import { parseSignedDecimal } from '../lib/decimal';
import { compactNumber, modeLabel, numeric, quantity, ratioText, time } from '../lib/format';
import { availableRatio, groupModeAssetPnl, quoteAsset, type ModeAssetPnlGroup } from '../lib/portfolio';
import type { DailySnapshot } from '../lib/types';
import { Pnl } from './Common';

function DailyAccountsBlock({ accounts }: { accounts: DailySnapshot['accounts'] }) {
  return <div className="daily-snapshot-block">
    <h4>账户概览</h4>
    {!accounts.length ? <div className="muted">暂无账户记录</div> : accounts.map((acc, idx) => {
      const equity = numeric(acc.equity), available = numeric(acc.available), wallet = numeric(acc.wallet), unrealized = numeric(acc.unrealized);
      const ratio = availableRatio(available, wallet);
      return <div className="daily-snapshot-item" key={`${acc.mode}-${acc.asset}-${acc.account_id ?? idx}`}>
        <div className="daily-snapshot-item-head">
          <strong>{modeLabel[acc.mode]} · {acc.asset}</strong>
          <span className="muted">{acc.count} 个策略</span>
        </div>
        <div className="daily-snapshot-metrics">
          <div><span>账户权益</span><strong>{compactNumber(equity)} <small>{acc.asset}</small></strong></div>
          <div><span>可用余额</span><strong>{compactNumber(available)} <small>({ratioText(ratio, true)})</small></strong></div>
          <div><span>钱包余额</span><strong>{compactNumber(wallet)} <small>{acc.asset}</small></strong></div>
          <div><span>未实现盈亏</span><strong><Pnl value={unrealized} compact /> <small>{acc.asset}</small></strong></div>
        </div>
      </div>;
    })}
  </div>;
}

function DailyTradesBlock({ date, trades }: { date: string; trades: DailySnapshot['trades'] }) {
  const tradeSummary = useMemo(() => {
    let buy = 0, sell = 0;
    for (const item of trades) {
      buy += item.buy_count;
      sell += item.sell_count;
    }
    const pnlGroups: ModeAssetPnlGroup[] = [...groupModeAssetPnl(trades, item => ({ pnl: item.realized_pnl, pending: item.pending_pnl_count })).values()];
    return { buy, sell, total: buy + sell, pnlGroups };
  }, [trades]);
  return <div className="daily-snapshot-block">
    <h4>成交与实现盈亏（{date}）</h4>
    <div className="daily-snapshot-item">
      <div className="daily-snapshot-metrics">
        <div><span>成交笔数</span><strong>{tradeSummary.total} <small>笔</small></strong></div>
        <div><span>买单 / 卖单</span><strong>{tradeSummary.buy} <small>/</small> {tradeSummary.sell}</strong></div>
        <div className="daily-snapshot-span">
          <span>实现盈亏</span>
          {!tradeSummary.pnlGroups.length ? <strong>0</strong> : tradeSummary.pnlGroups.map(group => (
            <div className="daily-snapshot-pnl-row" key={group.key}>
              <span>{modeLabel[group.mode]} {group.asset}</span>
              <strong><Pnl value={group.sum} compact /> <small>{group.asset}</small>{group.pending > 0 && <em className="pending-pnl-warn">（{group.pending} 笔待同步）</em>}</strong>
            </div>
          ))}
        </div>
      </div>
      {trades.length > 0 && (
        <div className="daily-snapshot-sublist">
          {trades.map(t => {
            const quote = quoteAsset(t.symbol) || t.symbol;
            return <div className="daily-snapshot-subrow" key={`${t.mode}:${t.symbol}`}>
              <span>{t.symbol} <small className="muted">{modeLabel[t.mode]} · 买 {t.buy_count} / 卖 {t.sell_count}</small></span>
              <strong><Pnl value={t.realized_pnl} compact /> <small>{quote}</small>{t.pending_pnl_count > 0 && <em className="pending-pnl-warn"> 待同步</em>}</strong>
            </div>;
          })}
        </div>
      )}
    </div>
  </div>;
}

function DailyPositionsBlock({ positions, pairPositions }: { positions: DailySnapshot['positions']; pairPositions: DailySnapshot['pair_positions'] }) {
  const openPositions = useMemo(() => pairPositions.flatMap(item => {
    const parsed = parseSignedDecimal(item.size);
    const nonZero = parsed ? parsed.n !== 0n : (numeric(item.size) ?? 0) !== 0;
    if (!nonZero) return [];
    const long = parsed ? parsed.n > 0n : (numeric(item.size) ?? 0) > 0;
    const absSize = typeof item.size === 'string' ? item.size.trim().replace(/^[+-]/, '') : Math.abs(numeric(item.size) ?? 0);
    return [{ item, long, absSize }];
  }), [pairPositions]);
  return <div className="daily-snapshot-block">
    <h4>持仓概览</h4>
    {!positions.length ? <div className="muted">暂无持仓记录</div> : positions.map(pos => {
      const total = numeric(pos.total), long = numeric(pos.long), short = numeric(pos.short);
      return <div className="daily-snapshot-item" key={`${pos.mode}:${pos.asset}`}>
        <div className="daily-snapshot-item-head">
          <strong>{modeLabel[pos.mode]} · {pos.asset}</strong>
          <span className="muted">覆盖 {pos.count} 个策略</span>
        </div>
        <div className="daily-snapshot-metrics">
          <div><span>总持仓金额</span><strong>{compactNumber(total)} <small>{pos.asset}</small></strong></div>
          <div><span>做多 / 做空</span><strong><span className="position-long">{compactNumber(long)}</span> <small>/</small> <span className="position-short">{compactNumber(short)}</span></strong></div>
        </div>
      </div>;
    })}
    {openPositions.length > 0 && (
      <div className="daily-snapshot-sublist" aria-label="持仓明细">
        {openPositions.map(({ item, long, absSize }) => {
          const quote = quoteAsset(item.symbol) || item.symbol;
          return <div className="daily-snapshot-subrow" key={`${item.mode}:${item.symbol}`}>
            <span>{item.symbol} <small className={long ? 'position-long' : 'position-short'}>{long ? '做多' : '做空'} {quantity(absSize)}</small></span>
            <strong>{compactNumber(item.position_amount)} <small>{quote}</small> · 浮盈 <Pnl value={item.unrealized_pnl} compact /></strong>
          </div>;
        })}
      </div>
    )}
  </div>;
}

export const DailySnapshotsSection = memo(function DailySnapshotsSection({ snapshots }: { snapshots: DailySnapshot[] }) {
  const [selectedDate, setSelectedDate] = useState<string>();
  const active = useMemo(() => snapshots.find(s => s.date === selectedDate) ?? snapshots[0], [snapshots, selectedDate]);
  const options = useMemo(() => snapshots.map(s => ({ value: s.date, label: `${s.date}（北京时间）` })), [snapshots]);
  if (!active) return null;
  return <div className="history-daily" aria-label="每日运行快照">
    <div className="history-caption">
      <div className="history-caption-main"><h3>每日运行快照</h3><span>每日 00:00 自动归档前一日数据 · 北京时间</span></div>
      <span className="numeric">生成于 {time(active.generated_at)}</span>
    </div>
    {options.length > 1 && (
      <div className="history-account-row">
        <Select aria-label="选择每日快照日期" value={active.date} options={options} onChange={setSelectedDate} />
      </div>
    )}
    <div className="daily-snapshot-grid">
      <DailyAccountsBlock accounts={active.accounts} />
      <DailyTradesBlock date={active.date} trades={active.trades} />
      <DailyPositionsBlock positions={active.positions} pairPositions={active.pair_positions} />
    </div>
  </div>;
});
