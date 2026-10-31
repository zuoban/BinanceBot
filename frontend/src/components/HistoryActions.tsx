import { useRef, useState } from 'react';
import { App, Button, Space } from 'antd';
import { DownloadOutlined } from '@ant-design/icons';
import { useQueryClient } from '@tanstack/react-query';
import { post } from '../lib/api';
import { downloadDatabase } from '../lib/database';

export function HistoryActions({ symbol }: { symbol: string }) {
  const { modal, message } = App.useApp();
  const client = useQueryClient();
  const [busy, setBusy] = useState<'export' | 'clear' | null>(null);
  const lock = useRef(false);
  const path = `/api/pairs/${encodeURIComponent(symbol)}`;
  const download = async () => {
    if (lock.current) return;
    lock.current = true; setBusy('export');
    try {
      await downloadDatabase(`${path}/database/export`);
      message.success(`${symbol} 全部模式的成交历史脱敏分析快照已导出，不能用于完整恢复`);
    } catch (error) { message.error((error as Error).message); }
    finally { lock.current = false; setBusy(null); }
  };
  const clear = async () => {
    if (lock.current) return;
    lock.current = true; setBusy('clear');
    try {
      if (!await modal.confirm({ title: `清理 ${symbol} 的成交历史？`, content: `将删除 ${symbol} 全部模式的历史成交并重置相关统计。持仓和挂单保留。此操作无法撤销，建议先导出。`, okText: '清理当前币种历史', okButtonProps: { danger: true }, cancelText: '取消' })) return;
      const result = await post<{ deleted_trades: number }>(`${path}/trades/clear`, { confirm: true });
      await Promise.all([client.cancelQueries({ queryKey: ['snapshot'] }), client.cancelQueries({ queryKey: ['overview'] })]);
      await Promise.all([client.invalidateQueries({ queryKey: ['snapshot'] }), client.invalidateQueries({ queryKey: ['overview'] })]);
      message.success(`已清理 ${symbol} 的 ${result.deleted_trades} 笔历史成交`);
    } catch (error) { message.error((error as Error).message); }
    finally { lock.current = false; setBusy(null); }
  };
  return <div><p className="muted">导出的是当前币种全部模式的脱敏分析快照：交易所 API 密钥、Telegram 令牌和聊天 ID 会被清空，管理员认证和会话会被移除。快照不能作为完整恢复备份。</p><Space wrap><Button icon={<DownloadOutlined />} loading={busy === 'export'} disabled={busy !== null} onClick={() => void download()}>导出当前币种分析快照</Button><Button danger loading={busy === 'clear'} disabled={busy !== null} onClick={() => void clear()}>清理当前币种全部模式</Button></Space></div>;
}
