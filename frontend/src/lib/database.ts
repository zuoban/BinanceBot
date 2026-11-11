import { useContext, useRef, useState } from 'react';
import { App, type ModalFuncProps } from 'antd';
import { ApiError, authenticatedFetch, errorMessage } from './api';
import { useAsyncLock, useMountedRef } from './asyncGuard';
import { useConfirmDialog } from './confirmDialog';
import { downloadBlob } from './exportCsv';
import { EditContext } from './session';
export type DatabaseTarget = 'trade_history' | 'account_hourly_snapshots';

export const exportSnapshotNotice = (scope = '') =>
  `导出的是${scope}脱敏分析快照：交易所 API 密钥、Telegram 令牌和聊天 ID 会被清空，登录会话会被移除。快照不能作为完整恢复备份。`;

export const exportSnapshotSuccess = (scope: string) => `${scope}脱敏分析快照已导出，不能用于完整恢复`;

export async function downloadDatabase(path: string, targets?: DatabaseTarget[], signal?: AbortSignal) {
  const response = await authenticatedFetch(path, {
    ...(targets ? { method: 'POST', body: JSON.stringify({ targets }), readOnly: true } : {}),
    ...(signal ? { signal } : {}),
    timeoutMs: 120_000,
  });
  if (!response.ok) {
    const result = await response.json().catch(() => null) as { message?: string } | null;
    throw new ApiError(result?.message || '数据库导出失败，请重试', response.status);
  }
  const rawFilename = response.headers.get('Content-Disposition')?.match(/filename="?([^";]+)"?/)?.[1]?.trim() || 'binancebot-analysis.db';
  const filename = rawFilename.replace(/[/\\]/g, '_');
  let blob: Blob;
  try {
    blob = await response.blob();
  } catch (error) {
    if (signal?.aborted) throw signal.reason ?? error;
    throw new ApiError('读取导出文件失败，请重试', response.status, 'invalid-response');
  }
  if (signal?.aborted) return;
  downloadBlob(blob, filename);
}

export function useDatabaseAction<T extends string>() {
  const { message } = App.useApp();
  const { confirm: confirmDialog } = useConfirmDialog();
  const edit = useContext(EditContext);
  const [busy, setBusy] = useState<T | null>(null);
  const runLocked = useAsyncLock();
  const abortRef = useRef<AbortController | null>(null);
  const mounted = useMountedRef(() => {
    abortRef.current?.abort();
    abortRef.current = null;
    edit.setSaving(false);
  });
  const run = (action: T, task: (signal: AbortSignal) => Promise<string | void>, confirm?: ModalFuncProps) => {
    if (!mounted.current) return Promise.resolve();
    return runLocked(async () => {
      const controller = new AbortController();
      abortRef.current = controller;
      try {
        if (confirm && !await confirmDialog(confirm)) return;
        if (!mounted.current || controller.signal.aborted) return;
        setBusy(action);
        edit.setSaving(true);
        const text = await task(controller.signal);
        if (mounted.current && !controller.signal.aborted && text) message.success(text);
      } catch (error) {
        if (controller.signal.aborted || ((error instanceof Error || error instanceof DOMException) && error.name === 'AbortError')) return;
        if (mounted.current) message.error(errorMessage(error));
      } finally {
        if (abortRef.current === controller) abortRef.current = null;
        if (mounted.current) {
          setBusy(null);
          edit.setSaving(false);
        }
      }
    });
  };
  return { busy, run };
}
