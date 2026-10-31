import { useContext, useEffect, useRef, useState } from 'react';
import { App, Button, Card, Checkbox, Space } from 'antd';
import { useQueryClient } from '@tanstack/react-query';
import { post } from '../lib/api';
import { downloadDatabase, type DatabaseTarget } from '../lib/database';
import { EditContext } from '../lib/session';

const options: { label: string; value: DatabaseTarget }[] = [
  { label: '成交历史及统计（全部币种、全部模式）', value: 'trade_history' },
  { label: '账户小时快照', value: 'account_hourly_snapshots' },
];

type ClearResult = { deleted_trades: number; deleted_account_hourly_snapshots: number };

export function DatabaseSettings() {
  const { modal, message } = App.useApp();
  const client = useQueryClient();
  const edit = useContext(EditContext);
  const [targets, setTargets] = useState<DatabaseTarget[]>([]);
  const [busy, setBusy] = useState<'selected' | 'all' | 'clear' | null>(null);
  const lock = useRef(false);
  useEffect(() => () => edit.setSaving(false), [edit.setSaving]);

  const run = async (action: 'selected' | 'all' | 'clear') => {
    if (lock.current || (action !== 'all' && targets.length === 0)) return;
    lock.current = true; setBusy(action); edit.setSaving(true);
    try {
      if (action === 'clear') {
        const names = options.filter(option => targets.includes(option.value)).map(option => option.label).join('、');
        if (!await modal.confirm({ title: '清空所选数据库数据？', content: `将永久清空：${names}。此操作无法撤销，建议先导出所选数据。持仓和挂单保留。`, okText: '确认清空', okButtonProps: { danger: true }, cancelText: '取消' })) return;
        const result = await post<ClearResult>('/api/database/clear', { targets, confirm: true });
        const keys = [
          ...(targets.includes('trade_history') ? [['snapshot'], ['overview']] : []),
          ...(targets.includes('account_hourly_snapshots') ? [['overview-history']] : []),
        ];
        await Promise.all(keys.map(queryKey => client.cancelQueries({ queryKey })));
        await Promise.all(keys.map(queryKey => client.invalidateQueries({ queryKey })));
        const counts = [
          ...(targets.includes('trade_history') ? [`${result.deleted_trades} 笔历史成交`] : []),
          ...(targets.includes('account_hourly_snapshots') ? [`${result.deleted_account_hourly_snapshots} 条账户小时快照`] : []),
        ];
        message.success(`已清空 ${counts.join('、')}`);
      } else {
        await downloadDatabase('/api/database/export', action === 'selected' ? targets : undefined);
        message.success(action === 'all' ? '全库脱敏分析快照已导出，不能用于完整恢复' : '所选数据脱敏分析快照已导出，不能用于完整恢复');
      }
    } catch (error) { message.error((error as Error).message); }
    finally { lock.current = false; setBusy(null); edit.setSaving(false); }
  };

  return <Card title="数据库操作">
    <p className="muted">选择需要导出或清空的数据类别。成交历史及统计覆盖全部币种和全部模式。</p>
    <p className="muted">导出的是脱敏分析快照：交易所 API 密钥、Telegram 令牌和聊天 ID 会被清空，管理员认证和会话会被移除。快照不能作为完整恢复备份。</p>
    <Checkbox.Group options={options} value={targets} disabled={busy !== null} onChange={values => setTargets(values as DatabaseTarget[])} />
    <div style={{ marginTop: 16 }}><Space wrap>
      <Button loading={busy === 'selected'} disabled={busy !== null || targets.length === 0} onClick={() => void run('selected')}>导出所选数据分析快照</Button>
      <Button danger loading={busy === 'clear'} disabled={busy !== null || targets.length === 0} onClick={() => void run('clear')}>清空所选数据</Button>
      <Button loading={busy === 'all'} disabled={busy !== null} onClick={() => void run('all')}>导出全库分析快照</Button>
    </Space></div>
  </Card>;
}
