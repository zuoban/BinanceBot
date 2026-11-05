import { Alert, Button, Empty, Space } from 'antd';
import { CheckCircleFilled, ClockCircleFilled, CloseCircleFilled, ExclamationCircleFilled, InfoCircleFilled, MinusCircleFilled, ReloadOutlined, ThunderboltFilled } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import type { Mode, Status } from '../lib/types';
import { modeLabel, number, statusLabel, trimFractionZeros } from '../lib/format';

export function ErrorNotice({ error, retry }: { error: Error | null; retry?: () => void }) {
  if (!error) return null;
  return <Alert type="error" showIcon title={error.message} action={retry && <Button size="small" onClick={retry} icon={<ReloadOutlined />}>重试</Button>} />;
}
export type Signal = 'calm' | 'blue' | 'yellow' | 'orange' | 'red';
const signalIcon: Record<Signal, React.ReactNode> = { calm: <CheckCircleFilled />, blue: <InfoCircleFilled />, yellow: <ExclamationCircleFilled />, orange: <ThunderboltFilled />, red: <CloseCircleFilled /> };
/** Tiered state chip: colour is never the only channel, every tier carries an icon and words. */
export function SignalChip({ tier, children, title }: { tier: Signal; children: React.ReactNode; title?: string }) {
  return <span className={`signal signal-${tier}`} title={title}><span className="signal-icon" aria-hidden="true">{signalIcon[tier]}</span>{children}</span>;
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
export const StatusTag = ({ status }: { status: Status }) => <span className={`signal signal-${statusMeta[status].tier}`}><span className="signal-icon" aria-hidden="true">{statusMeta[status].icon}</span>{statusLabel[status]}</span>;
export const Pnl = ({ value, compact = false }: { value: unknown; compact?: boolean }) => <span className={Number(value) < 0 ? 'negative numeric' : 'positive numeric'}>{compact ? trimFractionZeros(number(value)) : number(value)}</span>;
export const NoData = () => <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="暂无数据" />;
export function PageBreadcrumb({ items, disabled = false }: { items: { label: string; to?: string }[]; disabled?: boolean }) {
  return <nav className="page-breadcrumb" aria-label="面包屑"><ol>{items.map((item, index) => {
    const current = index === items.length - 1;
    return <li key={index}>{index > 0 && <span className="page-breadcrumb-separator" aria-hidden="true">/</span>}{current ? <span aria-current="page">{item.label}</span> : item.to && !disabled ? <Link to={item.to}>{item.label}</Link> : <span aria-disabled={disabled || undefined}>{item.label}</span>}</li>;
  })}</ol></nav>;
}
export function PageTitle({ title, description, extra }: { title: string; description?: string; extra?: React.ReactNode }) {
  return <div className="page-heading"><div><h1>{title}</h1>{description && <p>{description}</p>}</div><Space wrap>{extra}</Space></div>;
}
