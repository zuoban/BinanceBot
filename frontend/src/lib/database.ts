import type { QueryClient, QueryKey } from '@tanstack/react-query';
import { authenticatedFetch } from './api';
import { downloadBlob } from './exportCsv';

export type DatabaseTarget = 'trade_history' | 'account_hourly_snapshots';

export const exportSnapshotNotice = (scope = '') =>
  `导出的是${scope}脱敏分析快照：交易所 API 密钥、Telegram 令牌和聊天 ID 会被清空，登录会话会被移除。快照不能作为完整恢复备份。`;

export const exportSnapshotSuccess = (scope: string) => `${scope}脱敏分析快照已导出，不能用于完整恢复`;

export async function refreshClearedQueries(client: QueryClient, keys: QueryKey[]) {
  await Promise.all(keys.map(queryKey => client.cancelQueries({ queryKey })));
  await Promise.all(keys.map(queryKey => client.invalidateQueries({ queryKey })));
}

export async function downloadDatabase(path: string, targets?: DatabaseTarget[]) {
  const response = await authenticatedFetch(path, {
    ...(targets ? { method: 'POST', body: JSON.stringify({ targets }) } : {}),
    timeoutMs: 120_000,
  });
  if (!response.ok) {
    const result = await response.json().catch(() => null) as { message?: string } | null;
    throw new Error(result?.message || '数据库导出失败，请重试');
  }
  const filename = response.headers.get('Content-Disposition')?.match(/filename="?([^";]+)"?/)?.[1] ?? 'binancebot-analysis.db';
  downloadBlob(await response.blob(), filename);
}
