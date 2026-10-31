import { authenticatedFetch } from './api';

export type DatabaseTarget = 'trade_history' | 'account_hourly_snapshots';

export async function downloadDatabase(path: string, targets?: DatabaseTarget[]) {
  const response = await authenticatedFetch(path, {
    ...(targets ? { method: 'POST', body: JSON.stringify({ targets }) } : {}),
    signal: AbortSignal.timeout(120_000),
  });
  if (!response.ok) {
    const result = await response.json().catch(() => null) as { message?: string } | null;
    throw new Error(result?.message || '数据库导出失败，请重试');
  }
  const url = URL.createObjectURL(await response.blob());
  const link = document.createElement('a');
  link.href = url;
  link.download = response.headers.get('Content-Disposition')?.match(/filename="?([^";]+)"?/)?.[1] ?? 'binancebot-analysis.db';
  document.body.append(link);
  link.click();
  link.remove();
  setTimeout(() => URL.revokeObjectURL(url), 10_000);
}
