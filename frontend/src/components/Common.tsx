import { useContext, useRef } from 'react';
import { Alert, App, Button, Empty, Space } from 'antd';
import { ArrowDownOutlined, ArrowUpOutlined, CheckCircleFilled, ClockCircleFilled, CloseCircleFilled, ExclamationCircleFilled, InfoCircleFilled, MinusCircleFilled, ReloadOutlined, ThunderboltFilled } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import { EditContext } from '../lib/session';
import type { GridConfig, Mode, Status } from '../lib/types';
type Direction = GridConfig['direction'];
import { compactNumber, directionLabel, modeLabel, number, numeric, statusLabel } from '../lib/format';

export function ErrorNotice({ error, retry }: { error: Error | null; retry?: () => void }) {
  if (!error) return null;
  return <Alert type="error" showIcon title={error.message} action={retry && <Button size="small" onClick={retry} icon={<ReloadOutlined />}>重试</Button>} />;
}
export type Signal = 'calm' | 'blue' | 'yellow' | 'orange' | 'red';
export const signalIcon: Record<Signal, React.ReactNode> = { calm: <CheckCircleFilled />, blue: <InfoCircleFilled />, yellow: <ExclamationCircleFilled />, orange: <ThunderboltFilled />, red: <CloseCircleFilled /> };
export function controlResultMeta(result: string): { tier: Signal; alertType: 'error' | 'warning' | 'success' | 'info'; pending: boolean } {
  if (result.startsWith('未提交')) return { tier: 'red', alertType: 'error', pending: false };
  if (result.includes('尚未确认') || result.includes('结果不明')) return { tier: 'yellow', alertType: 'warning', pending: false };
  if (result === '已确认') return { tier: 'calm', alertType: 'success', pending: false };
  return { tier: 'blue', alertType: 'info', pending: result === '正在提交…' };
}
/** Tiered state chip: colour is never the only channel, every tier carries an icon and words. */
export function SignalChip({ tier, icon, children, title }: { tier: Signal; icon?: React.ReactNode; children: React.ReactNode; title?: string }) {
  return <span className={`signal signal-${tier}`} title={title}><span className="signal-icon" aria-hidden="true">{icon ?? signalIcon[tier]}</span>{children}</span>;
}
/** A signal chip that filters the list below it; the pressed state is exposed to assistive tech. */
export function SignalButton({ tier, active, onClick, children }: { tier: Signal; active: boolean; onClick: () => void; children: React.ReactNode }) {
  return <button type="button" className={`signal signal-${tier} signal-button${active ? ' signal-active' : ''}`} aria-pressed={active} title={active ? '点击取消筛选' : '点击筛选'} onClick={onClick}><span className="signal-icon" aria-hidden="true">{signalIcon[tier]}</span>{children}</button>;
}
export const ModeTag = ({ mode }: { mode: Mode }) => <SignalChip tier={mode === 'LIVE' ? 'orange' : mode === 'UNKNOWN' ? 'yellow' : 'blue'}>{modeLabel[mode]}</SignalChip>;
const statusMeta: Record<Status, { tier: Signal; icon: React.ReactNode }> = {
  RUNNING: { tier: 'calm', icon: <CheckCircleFilled /> },
  PAUSED: { tier: 'blue', icon: <MinusCircleFilled /> },
  STOPPED: { tier: 'blue', icon: <ClockCircleFilled /> },
  ERROR: { tier: 'red', icon: <CloseCircleFilled /> },
};
export const StatusTag = ({ status }: { status: Status }) => <SignalChip tier={statusMeta[status].tier} icon={statusMeta[status].icon}>{statusLabel[status]}</SignalChip>;
export function DirectionTag({ direction }: { direction: Direction }) {
  const short = direction === 'SHORT';
  return <span className={`strategy-direction strategy-direction-${short ? 'short' : 'long'}`} role="img" aria-label={`策略方向：${directionLabel(direction)}`}>{short ? <ArrowDownOutlined /> : <ArrowUpOutlined />}{directionLabel(direction)}</span>;
}
export function DirectionText({ direction }: { direction: Direction }) {
  const short = direction === 'SHORT';
  return <span className={`side-tag side-${short ? 'sell' : 'buy'}`}><span aria-hidden="true">{short ? '↓' : '↑'}</span>{directionLabel(direction)}</span>;
}
export function Pnl({ value, compact = false }: { value: unknown; compact?: boolean }) {
  const n = numeric(value);
  if (compact && n !== null && Math.abs(n) > 0 && Math.abs(n) < 0.01) {
    return <span className={n < 0 ? 'negative numeric' : 'positive numeric'}>{n.toLocaleString('en-US', { maximumSignificantDigits: 3 })}</span>;
  }
  const text = compact ? compactNumber(value) : number(value);
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
export function isConfigConflict(error: Error | null | undefined, messageFragment: string): boolean {
  return (error as { status?: number } | null | undefined)?.status === 409 || Boolean(error?.message.includes(messageFragment));
}
export function ConflictReloadButton<T>({ label, activeRef, refetch, onLoaded, onReset }: {
  label: string;
  activeRef: { current: unknown };
  refetch: () => Promise<{ data?: T; error: unknown }>;
  onLoaded: (data: NonNullable<T>) => boolean | void;
  onReset: () => void;
}) {
  const { modal, message } = App.useApp();
  const edit = useContext(EditContext);
  const running = useRef(false);
  return <Button className="conflict-reload-btn" disabled={edit.saving} onClick={async () => {
    const current = activeRef.current;
    if (running.current || edit.saving || !current) return;
    running.current = true;
    try {
      if (!await modal.confirm({ title: `重新加载${label}？`, content: '重新加载会放弃当前未保存的修改，请先记录需要保留的内容。', okText: '重新加载', cancelText: '继续编辑' })) return;
      if (activeRef.current !== current) return;
      const result = await refetch();
      if (activeRef.current !== current) return;
      if (result.error) { message.error((result.error as Error).message); return; }
      if (!result.data || onLoaded(result.data) === false) return;
      edit.setDirty(false);
      onReset();
    } finally {
      running.current = false;
    }
  }}>重新加载{label}</Button>;
}
export function FormRevertButton({ activeRef, onRevert, onReset }: {
  activeRef: { current: unknown };
  onRevert: () => void;
  onReset: () => void;
}) {
  const { modal } = App.useApp();
  const edit = useContext(EditContext);
  const running = useRef(false);
  return <Button onClick={async () => {
    const current = activeRef.current;
    if (running.current || !current) return;
    running.current = true;
    try {
      if (edit.dirty && !await modal.confirm({ title: '放弃未保存的修改？', okText: '还原', cancelText: '继续编辑' })) return;
      if (activeRef.current !== current) return;
      onRevert();
      edit.setDirty(false);
      onReset();
    } finally {
      running.current = false;
    }
  }}>还原</Button>;
}
