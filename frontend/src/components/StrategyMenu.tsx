import { Button, Dropdown } from 'antd';
import { MoreOutlined } from '@ant-design/icons';
import type { Action, Pair } from '../lib/types';
import { useControls } from './Controls';
import { useDeleteStrategy } from './DeleteStrategy';

export function StrategyMenu({ pair, stale }: { pair: Pair; stale: boolean }) {
  const { busy, run } = useControls();
  const deletion = useDeleteStrategy();
  const running = pair.status === 'RUNNING';
  return <Dropdown trigger={['click']} disabled={busy.has(pair.symbol) || deletion.busy} menu={{
    items: [
      { key: running ? 'pause' : 'resume', label: running ? '暂停策略' : stale ? '恢复策略（状态过期）' : '恢复策略', disabled: stale && !running },
      { key: 'cancel_all', label: `撤单并暂停 ${pair.symbol}`, danger: true },
      { key: 'rebalance', label: `重建 ${pair.symbol} 网格`, disabled: stale },
      { type: 'divider' },
      { key: 'delete', label: '删除策略', danger: true },
    ],
    onClick: ({ key }) => {
      if (key === 'delete') void deletion.remove(pair);
      else void run(pair, key as Action);
    },
  }}>
    <Button size="small" icon={<MoreOutlined />} aria-label={`${pair.symbol} 更多操作`} />
  </Dropdown>;
}
