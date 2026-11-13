import { Button, Dropdown, Tooltip } from 'antd';
import { EditOutlined, EyeOutlined, MoreOutlined, PauseOutlined, PlayCircleOutlined, ReloadOutlined, StopOutlined, SwapOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import { pairRoute } from '../lib/api';
import { directionLabel } from '../lib/format';
import type { Action, Pair } from '../lib/types';
import { useControls } from './Controls';

const pendingLabel: Record<Action, string> = { pause: '暂停中…', resume: '恢复中…', cancel_all: '撤单中…', rebalance: '重建中…', reverse: '反手中…' };

export function strategyActionGuard(pair: Pair, stale = false, busy = false) {
  const running = pair.status === 'RUNNING';
  const staleResumeBlocked = !busy && stale && !running;
  const nextDirection: Pair['grid_config']['direction'] = pair.grid_config.direction === 'SHORT' ? 'LONG' : 'SHORT';
  return {
    running,
    staleResumeBlocked,
    nextDirection,
    toggleAction: (running ? 'pause' : 'resume') as 'pause' | 'resume',
    toggleLabel: running ? '暂停' : '恢复',
    toggleDisabled: busy || staleResumeBlocked,
    rebalanceDisabled: busy || stale || !running,
    reverseDisabled: busy || stale || pair.mode === 'UNKNOWN',
    cancelDisabled: busy,
  };
}
export function strategySecondaryItems(pair: Pair, stale = false) {
  const guard = strategyActionGuard(pair, stale);
  return [
    { key: 'rebalance', icon: <ReloadOutlined aria-hidden="true" />, label: `重建 ${pair.symbol} 网格`, disabled: guard.rebalanceDisabled },
    { key: 'reverse', icon: <SwapOutlined aria-hidden="true" />, label: `反手交易（转${directionLabel(guard.nextDirection)}）`, disabled: guard.reverseDisabled },
    { key: 'cancel_all', icon: <StopOutlined aria-hidden="true" />, label: `撤单并暂停 ${pair.symbol}`, danger: true, disabled: guard.cancelDisabled },
  ];
}
export function StrategyToggle({ pair, stale = false, size = 'small', className = 'strategy-toggle', withSymbolAriaLabel = true }: { pair: Pair; stale?: boolean; size?: 'small' | 'middle'; className?: string; withSymbolAriaLabel?: boolean }) {
  const { busy, pending, run } = useControls(pair.symbol);
  const isBusy = busy.has(pair.symbol);
  const guard = strategyActionGuard(pair, stale, isBusy);
  const active = pending[pair.symbol];
  const button = <Button className={className} size={size} loading={isBusy} disabled={guard.toggleDisabled}
    icon={guard.running ? <PauseOutlined /> : <PlayCircleOutlined />} aria-label={withSymbolAriaLabel ? `${guard.toggleLabel} ${pair.symbol}` : undefined}
    onClick={() => void run(pair, guard.toggleAction)}>{active ? pendingLabel[active] : guard.toggleLabel}</Button>;
  return guard.staleResumeBlocked
    ? <Tooltip title="状态过期，恢复前请刷新"><span className="strategy-toggle-tip">{button}</span></Tooltip>
    : button;
}
export function StrategyMenu({ pair, stale }: { pair: Pair; stale: boolean }) {
  const navigate = useNavigate();
  const { busy, run } = useControls(pair.symbol);
  return <Dropdown trigger={['click']} disabled={busy.has(pair.symbol)} menu={{
    items: [
      { key: 'view', icon: <EyeOutlined aria-hidden="true" />, label: '查看策略' },
      { key: 'edit', icon: <EditOutlined aria-hidden="true" />, label: '编辑策略' },
      { type: 'divider' },
      ...strategySecondaryItems(pair, stale),
    ],
    onClick: ({ key }) => {
      if (key === 'view') { navigate(pairRoute(pair.symbol)); return; }
      if (key === 'edit') { navigate(pairRoute(pair.symbol, true)); return; }
      void run(pair, key as Action);
    },
  }}>
    <Button size="small" icon={<MoreOutlined />} aria-label={`${pair.symbol} 更多操作`} />
  </Dropdown>;
}
