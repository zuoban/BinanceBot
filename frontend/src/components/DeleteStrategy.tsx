import { useRef, useState } from 'react';
import { App, Input } from 'antd';
import { useQueryClient, type QueryClient } from '@tanstack/react-query';
import { api, ApiError, errorMessage } from '../lib/api';
import { useAsyncLock, useMountedRef } from '../lib/asyncGuard';
import type { Pair } from '../lib/types';
import { useControls } from './Controls';

function invalidateStrategyListQueries(client: QueryClient) {
  void client.invalidateQueries({ queryKey: ['pairs'] });
  void client.invalidateQueries({ queryKey: ['overview'] });
  void client.resetQueries({ queryKey: ['overview-history'] });
}

export function useDeleteStrategy() {
  const { modal, message } = App.useApp();
  const client = useQueryClient();
  const controls = useControls();
  const runDelete = useAsyncLock();
  const [busy, setBusy] = useState(false);
  const dialogRef = useRef<{ destroy?: () => void } | null>(null);
  const mounted = useMountedRef(() => {
    dialogRef.current?.destroy?.();
    dialogRef.current = null;
  });
  const remove = (pair: Pair, beforeConfirm?: () => Promise<boolean>) => runDelete(async () => {
    if (!mounted.current) return false;
    setBusy(true);
    let deleteStarted = false;
    try {
      if (beforeConfirm && !await beforeConfirm()) return false;
      if (!mounted.current) return false;
      const live = pair.mode === 'LIVE';
      const dialog = modal.confirm({
        title: `删除 ${pair.symbol} 策略？`,
        content: <div className="delete-confirm-dialog">
          <p>请先暂停策略、等待挂单撤销并平仓。删除不会自动平仓，成交历史和恢复数据会保留；至少需要保留一个交易对。</p>
          {live && <Input aria-label="输入交易对名称以确认删除" placeholder={`输入 ${pair.symbol} 以确认`} autoComplete="off"
            onChange={event => dialog.update({ okButtonProps: { danger: true, disabled: event.target.value.trim().toUpperCase() !== pair.symbol.toUpperCase() } })} />}
        </div>,
        okText: '确认删除', okButtonProps: { danger: true, disabled: live }, cancelText: '取消',
      });
      dialogRef.current = dialog;
      const accepted = await dialog;
      if (dialogRef.current === dialog) dialogRef.current = null;
      if (!accepted || !mounted.current) return false;
      // Engine shutdown can take 30 seconds, with additional time for failure recovery and persistence.
      deleteStarted = true;
      await api(`/api/pairs/${encodeURIComponent(pair.symbol)}`, { method: 'DELETE', body: JSON.stringify({ confirm: true }), timeoutMs: 90_000 });
      message.success('策略已删除，历史记录保留');
      controls.clearResult(pair.symbol);
      client.removeQueries({ queryKey: ['snapshot', pair.symbol] });
      client.removeQueries({ queryKey: ['config', pair.symbol] });
      invalidateStrategyListQueries(client);
      return true;
    } catch (error) {
      if (deleteStarted) {
        invalidateStrategyListQueries(client);
      }
      const uncertain = deleteStarted && error instanceof ApiError && error.kind === 'invalid-response';
      message.error(`${errorMessage(error)}${uncertain ? '；删除结果可能已生效，请刷新状态或查看日志后再操作' : ''}`);
      return false;
    } finally {
      if (mounted.current) setBusy(false);
    }
  }, false);
  return { remove, busy };
}
