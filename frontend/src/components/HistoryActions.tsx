import { Button, Space } from 'antd';
import { DownloadOutlined } from '@ant-design/icons';
import { useQueryClient } from '@tanstack/react-query';
import { post } from '../lib/api';
import { downloadDatabase, exportSnapshotNotice, exportSnapshotSuccess, refreshClearedQueries, useDatabaseAction } from '../lib/database';

export function HistoryActions({ symbol }: { symbol: string }) {
  const client = useQueryClient();
  const { busy, run } = useDatabaseAction<'export' | 'clear'>();
  const path = `/api/pairs/${encodeURIComponent(symbol)}`;
  const download = () => run('export', async () => {
    await downloadDatabase(`${path}/database/export`);
    return exportSnapshotSuccess(`${symbol} 全部模式的成交历史`);
  });
  const clear = () => run('clear', async () => {
    const result = await post<{ deleted_trades: number }>(`${path}/trades/clear`, { confirm: true });
    await refreshClearedQueries(client, [['snapshot'], ['overview']]);
    return `已清理 ${symbol} 的 ${result.deleted_trades} 笔历史成交`;
  }, { title: `清理 ${symbol} 的成交历史？`, content: `将删除 ${symbol} 全部模式的历史成交并重置相关统计。持仓和挂单保留。此操作无法撤销，建议先导出。`, okText: '清理当前币种历史', okButtonProps: { danger: true }, cancelText: '取消' });
  return <div><p className="muted">{exportSnapshotNotice('当前币种全部模式的')}</p><Space wrap><Button icon={<DownloadOutlined />} loading={busy === 'export'} disabled={busy !== null} onClick={() => void download()}>导出当前币种分析快照</Button><Button danger loading={busy === 'clear'} disabled={busy !== null} onClick={() => void clear()}>清理当前币种全部模式</Button></Space></div>;
}
