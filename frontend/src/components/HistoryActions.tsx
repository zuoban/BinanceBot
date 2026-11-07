import { useContext, useEffect, useRef, useState } from 'react';
import { App, Button, Space } from 'antd';
import { DownloadOutlined } from '@ant-design/icons';
import { useQueryClient } from '@tanstack/react-query';
import { post } from '../lib/api';
import { downloadDatabase, exportSnapshotNotice, exportSnapshotSuccess, refreshClearedQueries } from '../lib/database';
import { EditContext } from '../lib/session';

export function HistoryActions({ symbol }: { symbol: string }) {
  const { modal, message } = App.useApp();
  const client = useQueryClient();
  const edit = useContext(EditContext);
  const [busy, setBusy] = useState<'export' | 'clear' | null>(null);
  const lock = useRef(false);
  useEffect(() => () => edit.setSaving(false), [edit.setSaving]);
  const path = `/api/pairs/${encodeURIComponent(symbol)}`;
  const download = async () => {
    if (lock.current) return;
    lock.current = true; setBusy('export'); edit.setSaving(true);
    try {
      await downloadDatabase(`${path}/database/export`);
      message.success(exportSnapshotSuccess(`${symbol} 全部模式的成交历史`));
    } catch (error) { message.error((error as Error).message); }
    finally { lock.current = false; setBusy(null); edit.setSaving(false); }
  };
  const clear = async () => {
    if (lock.current) return;
    lock.current = true;
    try {
      if (!await modal.confirm({ title: `清理 ${symbol} 的成交历史？`, content: `将删除 ${symbol} 全部模式的历史成交并重置相关统计。持仓和挂单保留。此操作无法撤销，建议先导出。`, okText: '清理当前币种历史', okButtonProps: { danger: true }, cancelText: '取消' })) return;
      setBusy('clear'); edit.setSaving(true);
      const result = await post<{ deleted_trades: number }>(`${path}/trades/clear`, { confirm: true });
      await refreshClearedQueries(client, [['snapshot'], ['overview']]);
      message.success(`已清理 ${symbol} 的 ${result.deleted_trades} 笔历史成交`);
    } catch (error) { message.error((error as Error).message); }
    finally { lock.current = false; setBusy(null); edit.setSaving(false); }
  };
  return <div><p className="muted">{exportSnapshotNotice('当前币种全部模式的')}</p><Space wrap><Button icon={<DownloadOutlined />} loading={busy === 'export'} disabled={busy !== null} onClick={() => void download()}>导出当前币种分析快照</Button><Button danger loading={busy === 'clear'} disabled={busy !== null} onClick={() => void clear()}>清理当前币种全部模式</Button></Space></div>;
}
