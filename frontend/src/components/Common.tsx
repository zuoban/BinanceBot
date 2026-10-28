import { Alert, Button, Empty, Space, Tag } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import type { Mode, Status } from '../lib/types';
import { modeLabel, number, statusLabel } from '../lib/format';

export function ErrorNotice({ error, retry }: { error: Error | null; retry?: () => void }) {
  if (!error) return null;
  return <Alert type="error" showIcon title={error.message} action={retry && <Button size="small" onClick={retry} icon={<ReloadOutlined />}>重试</Button>} />;
}
export const ModeTag = ({ mode }: { mode: Mode }) => <Tag color={mode === 'LIVE' ? 'orange' : mode === 'TESTNET' ? 'blue' : 'default'}>{modeLabel[mode]}</Tag>;
export const StatusTag = ({ status }: { status: Status }) => <Tag color={status === 'RUNNING' ? 'success' : status === 'ERROR' ? 'error' : 'default'}>{statusLabel[status]}</Tag>;
export const Pnl = ({ value }: { value: unknown }) => <span className={Number(value) < 0 ? 'negative numeric' : 'positive numeric'}>{number(value)}</span>;
export const NoData = () => <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="暂无数据" />;
export function PageTitle({ eyebrow, title, description, extra }: { eyebrow: string; title: string; description?: string; extra?: React.ReactNode }) {
  return <div className="page-heading"><div><div className="eyebrow">{eyebrow}</div><h1>{title}</h1>{description && <p>{description}</p>}</div><Space wrap>{extra}</Space></div>;
}
