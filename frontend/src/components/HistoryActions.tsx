import { useRef, useState } from 'react';
import { App, Button, Space } from 'antd';
import { DownloadOutlined } from '@ant-design/icons';
import { useQueryClient } from '@tanstack/react-query';
import { authenticatedFetch, post } from '../lib/api';

export function HistoryActions() {
  const { modal, message } = App.useApp();
  const client = useQueryClient();
  const [busy, setBusy] = useState(false);
  const lock = useRef(false);
  const download = async () => {
    if (lock.current) return;
    lock.current = true; setBusy(true);
    try {
      const response = await authenticatedFetch('/api/database/export', { signal: AbortSignal.timeout(120_000) });
      if (!response.ok) throw new Error('数据库导出失败');
      const url = URL.createObjectURL(await response.blob());
      const a = document.createElement('a'); a.href = url;
      a.download = response.headers.get('Content-Disposition')?.match(/filename="?([^";]+)"?/)?.[1] ?? 'binancebot-analysis.db';
      document.body.append(a); a.click(); a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 10_000);
    } catch (error) { message.error((error as Error).message); }
    finally { lock.current = false; setBusy(false); }
  };
  const clear = async () => {
    if (lock.current) return;
    lock.current = true; setBusy(true);
    try {
      if (!await modal.confirm({ title: '清理全部交易历史？', content: '删除全部交易对、全部模式的历史成交，并重置累计统计。持仓和挂单保留。此操作无法撤销，建议先导出数据库。', okText: '清理全部历史', okButtonProps: { danger: true }, cancelText: '取消' })) return;
      const result = await post<{ deleted_trades: number }>('/api/trades/clear', { confirm: true });
      // Discard in-flight reads so a pre-cleanup snapshot cannot restore old totals.
      await client.cancelQueries({ queryKey: ['snapshot'] });
      await client.cancelQueries({ queryKey: ['overview'] });
      await Promise.all([client.invalidateQueries({ queryKey: ['snapshot'] }), client.invalidateQueries({ queryKey: ['overview'] })]);
      message.success(`已清理 ${result.deleted_trades} 笔历史成交`);
    } catch (error) { message.error((error as Error).message); }
    finally { lock.current = false; setBusy(false); }
  };
  return <Space wrap><Button icon={<DownloadOutlined />} loading={busy} onClick={() => void download()}>导出数据库</Button><Button danger disabled={busy} onClick={() => void clear()}>清理历史记录</Button></Space>;
}
