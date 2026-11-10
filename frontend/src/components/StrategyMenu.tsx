import { Button, Dropdown, Tooltip } from 'antd';
import { EditOutlined, EyeOutlined, MoreOutlined, PauseOutlined, PlayCircleOutlined, ReloadOutlined, StopOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import type { Action, Pair } from '../lib/types';
import { pendingLabel, useControls } from './Controls';

export function strategySecondaryItems(pair: Pair, stale = false) {
  const running = pair.status === 'RUNNING';
  return [
    { key: 'rebalance', icon: <ReloadOutlined aria-hidden="true" />, label: `重建 ${pair.symbol} 网格`, disabled: stale || !running },
    { key: 'cancel_all', icon: <StopOutlined aria-hidden="true" />, label: `撤单并暂停 ${pair.symbol}`, danger: true },
  ];
}
export function StrategyToggle({ pair, stale = false, size = 'small', className = 'strategy-toggle', withSymbolAriaLabel = true }: { pair: Pair; stale?: boolean; size?: 'small' | 'middle'; className?: string; withSymbolAriaLabel?: boolean }) {
  const { busy, pending, run } = useControls();
  const running = pair.status === 'RUNNING';
  const active = pending[pair.symbol];
  const label = running ? '暂停' : '恢复';
  return <Tooltip title={stale && !running ? '状态过期，恢复前请刷新' : undefined}>
    <Button className={className} size={size} loading={busy.has(pair.symbol)} disabled={stale && !running}
      icon={running ? <PauseOutlined /> : <PlayCircleOutlined />} aria-label={withSymbolAriaLabel ? `${label} ${pair.symbol}` : undefined}
      onClick={() => void run(pair, running ? 'pause' : 'resume')}>{active ? pendingLabel[active] : label}</Button>
  </Tooltip>;
}
export function StrategyMenu({ pair, stale }: { pair: Pair; stale: boolean }) {
  const navigate = useNavigate();
  const { busy, run } = useControls();
  return <Dropdown trigger={['click']} disabled={busy.has(pair.symbol)} menu={{
    items: [
      { key: 'view', icon: <EyeOutlined aria-hidden="true" />, label: '查看策略' },
      { key: 'edit', icon: <EditOutlined aria-hidden="true" />, label: '编辑策略' },
      { type: 'divider' },
      ...strategySecondaryItems(pair, stale),
    ],
    onClick: ({ key }) => {
      if (key === 'view') { navigate(`/pairs/${pair.symbol}`); return; }
      if (key === 'edit') { navigate(`/pairs/${pair.symbol}/config`); return; }
      void run(pair, key as Action);
    },
  }}>
    <Button size="small" icon={<MoreOutlined />} aria-label={`${pair.symbol} 更多操作`} />
  </Dropdown>;
}
