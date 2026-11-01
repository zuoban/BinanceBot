import { Button, Dropdown } from 'antd';
import { DeleteOutlined, EditOutlined, EyeOutlined, MoreOutlined, PauseOutlined, PlayCircleOutlined, ReloadOutlined, StopOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import type { Action, Pair } from '../lib/types';
import { useControls } from './Controls';
import { useDeleteStrategy } from './DeleteStrategy';

export function StrategyMenu({ pair, stale }: { pair: Pair; stale: boolean }) {
  const navigate = useNavigate();
  const { busy, run } = useControls();
  const deletion = useDeleteStrategy();
  const running = pair.status === 'RUNNING';
  return <Dropdown trigger={['click']} disabled={busy.has(pair.symbol) || deletion.busy} menu={{
    items: [
      { key: 'view', icon: <EyeOutlined aria-hidden="true" />, label: '查看策略' },
      { key: 'edit', icon: <EditOutlined aria-hidden="true" />, label: '编辑策略' },
      { type: 'divider' },
      { key: running ? 'pause' : 'resume', icon: running ? <PauseOutlined aria-hidden="true" /> : <PlayCircleOutlined aria-hidden="true" />, label: running ? '暂停策略' : stale ? '恢复策略（状态过期）' : '恢复策略', disabled: stale && !running },
      { key: 'cancel_all', icon: <StopOutlined aria-hidden="true" />, label: `撤单并暂停 ${pair.symbol}`, danger: true },
      { key: 'rebalance', icon: <ReloadOutlined aria-hidden="true" />, label: `重建 ${pair.symbol} 网格`, disabled: stale },
      { type: 'divider' },
      { key: 'delete', icon: <DeleteOutlined aria-hidden="true" />, label: '删除策略', danger: true },
    ],
    onClick: ({ key }) => {
      if (key === 'view') { navigate(`/pairs/${pair.symbol}`); return; }
      if (key === 'edit') { navigate(`/pairs/${pair.symbol}/config`); return; }
      if (key === 'delete') void deletion.remove(pair);
      else void run(pair, key as Action);
    },
  }}>
    <Button size="small" icon={<MoreOutlined />} aria-label={`${pair.symbol} 更多操作`} />
  </Dropdown>;
}
