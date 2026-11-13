import { useLayoutEffect, useMemo, type RefObject } from 'react';
import { Alert, Empty, Space } from 'antd';
import { ArrowDownOutlined, ArrowUpOutlined, CheckCircleOutlined, CloseCircleOutlined, CloseOutlined, ExclamationCircleOutlined, InfoCircleOutlined, ThunderboltOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import { parseSignedDecimal } from '../lib/decimal';
import type { GridConfig, Mode, Status } from '../lib/types';
type Direction = GridConfig['direction'];
import { ageText, compactNumber, directionLabel, modeLabel, number, numeric, sig3Format, statusLabel } from '../lib/format';
import { useRelativeNow } from '../lib/useNow';
export type Signal = 'calm' | 'blue' | 'yellow' | 'orange' | 'red';
const signalIcon: Record<Signal, React.ReactNode> = { calm: <CheckCircleOutlined />, blue: <InfoCircleOutlined />, yellow: <ExclamationCircleOutlined />, orange: <ThunderboltOutlined />, red: <CloseCircleOutlined /> };
function controlResultMeta(result: string): { tier: Signal; alertType: 'error' | 'warning' | 'success' | 'info'; pending: boolean } {
  if (result.startsWith('未提交')) return { tier: 'red', alertType: 'error', pending: false };
  if (result.includes('尚未确认') || result.includes('结果不明')) return { tier: 'yellow', alertType: 'warning', pending: false };
  if (result === '已确认' || result === '反手交易已完成') return { tier: 'calm', alertType: 'success', pending: false };
  return { tier: 'blue', alertType: 'info', pending: result === '正在提交…' };
}
export function ControlResultBanner({ symbol, results, result: directResult, clearResult, variant = 'inline' }: { symbol: string; results?: Record<string, string>; result?: string; clearResult?: (symbol: string) => void; variant?: 'inline' | 'alert' }) {
  const result = directResult ?? results?.[symbol];
  if (!result) return null;
  const meta = controlResultMeta(result);
  if (variant === 'alert') {
    return <Alert showIcon type={meta.alertType} title={result} closable={!meta.pending && !!clearResult} onClose={() => clearResult?.(symbol)} />;
  }
  return <div className={`operation-result signal-${meta.tier}`} role="status">
    <span className="signal-icon" aria-hidden="true">{signalIcon[meta.tier]}</span>
    <span className="operation-result-text">{result}</span>
    {!meta.pending && clearResult && <button type="button" className="operation-result-close" aria-label={`关闭 ${symbol} 操作提示`} onClick={() => clearResult(symbol)}><CloseOutlined aria-hidden="true" /></button>}
  </div>;
}
/** Tiered state chip: colour is never the only channel, every tier carries an icon and words. */
export function SignalChip({ tier, icon, compact = false, children, title }: { tier: Signal; icon?: React.ReactNode; compact?: boolean; children: React.ReactNode; title?: string }) {
  return <span className={`signal signal-${tier}${compact ? ' signal-compact' : ''}`} title={title}><span className="signal-icon" aria-hidden="true">{icon ?? signalIcon[tier]}</span>{children}</span>;
}
/** A signal chip that filters the list below it; the pressed state is exposed to assistive tech. */
export function SignalButton({ tier, active, onClick, children }: { tier: Signal; active: boolean; onClick: () => void; children: React.ReactNode }) {
  return <button type="button" className={`signal signal-${tier} signal-button${active ? ' signal-active' : ''}`} aria-pressed={active} title={active ? '点击取消筛选' : '点击筛选'} onClick={onClick}><span className="signal-icon" aria-hidden="true">{signalIcon[tier]}</span>{children}</button>;
}
export const ModeTag = ({ mode }: { mode: Mode }) => <SignalChip tier={mode === 'LIVE' ? 'orange' : mode === 'UNKNOWN' ? 'yellow' : 'blue'}>{modeLabel[mode]}</SignalChip>;
const statusTier: Record<Status, Signal> = {
  RUNNING: 'calm',
  PAUSED: 'blue',
  STOPPED: 'blue',
  ERROR: 'red',
};
export const StatusTag = ({ status }: { status: Status }) => <SignalChip tier={statusTier[status]} icon={<span className={`status-dot status-dot-${status.toLowerCase()}`} />}>{statusLabel[status]}</SignalChip>;
export function DirectionTag({ direction }: { direction: Direction }) {
  const short = direction === 'SHORT';
  return <span className={`strategy-direction strategy-direction-${short ? 'short' : 'long'}`} role="img" aria-label={`策略方向：${directionLabel(direction)}`}>{short ? <ArrowDownOutlined /> : <ArrowUpOutlined />}{directionLabel(direction)}</span>;
}
export function SideTag({ side, label }: { side: 'BUY' | 'SELL'; label?: string }) {
  const buy = side === 'BUY';
  return <span className={`side-tag side-${buy ? 'buy' : 'sell'}`}><span aria-hidden="true">{buy ? '↑' : '↓'}</span>{label ?? (buy ? '买入' : '卖出')}</span>;
}
export function DirectionText({ direction }: { direction: Direction }) {
  return <SideTag side={direction === 'SHORT' ? 'SELL' : 'BUY'} label={directionLabel(direction)} />;
}
export function Pnl({ value, compact = false }: { value: unknown; compact?: boolean }) {
  if (typeof value === 'string') {
    const parsed = parseSignedDecimal(value);
    if (parsed) {
      const absN = parsed.n < 0n ? -parsed.n : parsed.n;
      const scale10 = 10n ** BigInt(parsed.s);
      if (absN * 1_000_000_000_000n < scale10) {
        return <span className="positive numeric">{compact ? '0' : '0.00'}</span>;
      }
      if (compact && absN > 0n && absN * 100n < scale10) {
        return <span className={parsed.n < 0n ? 'negative numeric' : 'positive numeric'}>{sig3Format.format(Number(value))}</span>;
      }
      const text = compact ? compactNumber(value) : number(value);
      return <span className={text === '--' ? 'numeric' : text.startsWith('-') ? 'negative numeric' : 'positive numeric'}>{text}</span>;
    }
  }
  const raw = numeric(value);
  const n = raw !== null && Math.abs(raw) < 1e-12 ? 0 : raw;
  if (compact && n !== null && Math.abs(n) > 0 && Math.abs(n) < 0.01) {
    return <span className={n < 0 ? 'negative numeric' : 'positive numeric'}>{sig3Format.format(n)}</span>;
  }
  const text = compact ? compactNumber(n) : number(n);
  return <span className={text === '--' ? 'numeric' : text.startsWith('-') ? 'negative numeric' : 'positive numeric'}>{text}</span>;
}
export const NoData = () => <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="暂无数据" />;
export function PageBreadcrumb({ items, disabled = false }: { items: { label: string; to?: string }[]; disabled?: boolean }) {
  return <nav className="page-breadcrumb" aria-label="面包屑"><ol>{items.map((item, index) => {
    const current = index === items.length - 1;
    return <li key={index}>{index > 0 && <span className="page-breadcrumb-separator" aria-hidden="true">/</span>}{current ? <span aria-current="page">{item.label}</span> : item.to && !disabled ? <Link to={item.to}>{item.label}</Link> : <span aria-disabled={disabled || undefined}>{item.label}</span>}</li>;
  })}</ol></nav>;
}
export function PageTitle({ title, description, extra }: { title: string; description?: string; extra?: React.ReactNode }) {
  return <div className="page-heading"><div><h1>{title}</h1>{description && <p>{description}</p>}</div>{extra != null && extra !== false && <Space wrap>{extra}</Space>}</div>;
}
export function useCenterElementScroll<T extends HTMLElement>(ref: RefObject<T | null>, selector: string, triggerKey: string) {
  useLayoutEffect(() => {
    void triggerKey;
    const container = ref.current;
    const target = container?.querySelector<HTMLElement>(selector);
    if (container && target && container.scrollHeight > container.clientHeight) {
      container.scrollTop = Math.max(0, target.offsetTop - container.clientHeight / 2 + target.clientHeight / 2);
    }
  }, [ref, selector, triggerKey]);
}

export function FreshnessText({ updatedAt, prefix }: { updatedAt: number; prefix: string }) {
  const timestamps = useMemo(() => (updatedAt > 0 ? [updatedAt] : []), [updatedAt]);
  const now = useRelativeNow(timestamps, updatedAt > 0);
  return <>{prefix}{ageText(updatedAt, now)}</>;
}
