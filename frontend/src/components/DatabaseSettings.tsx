import { useState } from 'react';
import { Button, Card, Checkbox, Space } from 'antd';
import { useQueryClient } from '@tanstack/react-query';
import { post } from '../lib/api';
import { downloadDatabase, exportSnapshotNotice, exportSnapshotSuccess, useDatabaseAction, type DatabaseTarget } from '../lib/database';
import { invalidateStrategyQueries } from '../lib/queries';

const options: { label: string; value: DatabaseTarget }[] = [
  { label: '成交历史及统计（全部币种、全部模式）', value: 'trade_history' },
  { label: '账户小时与每日快照', value: 'account_hourly_snapshots' },
];

type ClearResult = { deleted_trades: number; deleted_account_hourly_snapshots: number };

export function DatabaseSettings() {
  const client = useQueryClient();
  const [targets, setTargets] = useState<DatabaseTarget[]>([]);
  const { busy, run: runAction } = useDatabaseAction<'selected' | 'all' | 'clear'>();

  const run = (action: 'selected' | 'all' | 'clear') => {
    if (action !== 'all' && targets.length === 0) return;
    if (action === 'clear') {
      const names = options.filter(option => targets.includes(option.value)).map(option => option.label).join('、');
      void runAction('clear', async () => {
        const result = await post<ClearResult>('/api/database/clear', { targets, confirm: true });
        const hasTrades = targets.includes('trade_history');
        const hasHistory = targets.includes('account_hourly_snapshots');
        await invalidateStrategyQueries(client, {
          pairs: false,
          overview: hasTrades,
          snapshot: hasTrades,
          history: hasHistory ? 'invalidate' : undefined,
          cancelFirst: true,
        });
        const counts = [
          ...(targets.includes('trade_history') ? [`${result.deleted_trades} 笔历史成交`] : []),
          ...(targets.includes('account_hourly_snapshots') ? [`${result.deleted_account_hourly_snapshots} 条账户小时与每日快照`] : []),
        ];
        return `已清空 ${counts.join('、')}`;
      }, { title: '清空所选数据库数据？', content: `将永久清空：${names}。此操作无法撤销，建议先导出所选数据。持仓和挂单保留。`, okText: '确认清空', okButtonProps: { danger: true }, cancelText: '取消' });
      return;
    }
    void runAction(action, async signal => {
      await downloadDatabase('/api/database/export', action === 'selected' ? targets : undefined, signal);
      return exportSnapshotSuccess(action === 'all' ? '全库' : '所选数据');
    });
  };

  return <Card title="数据库操作" className="settings-card">
    <div className="database-intro">
      <p className="muted">选择需要导出或清空的数据类别。成交历史及统计覆盖全部币种和全部模式。</p>
      <p className="muted">{exportSnapshotNotice('')}</p>
    </div>
    <Checkbox.Group className="database-targets" options={options} value={targets} disabled={busy !== null} onChange={values => setTargets(values as DatabaseTarget[])} />
    <div className="settings-card-footer database-actions">
      <Space wrap>
        <Button loading={busy === 'selected'} disabled={busy !== null || targets.length === 0} onClick={() => void run('selected')}>导出所选数据分析快照</Button>
        <Button danger loading={busy === 'clear'} disabled={busy !== null || targets.length === 0} onClick={() => void run('clear')}>清空所选数据</Button>
      </Space>
      <Button loading={busy === 'all'} disabled={busy !== null} onClick={() => void run('all')}>导出全库分析快照</Button>
    </div>
  </Card>;
}
