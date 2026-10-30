import { useRef, useState } from 'react';
import { App, Button } from 'antd';
import { useQueryClient } from '@tanstack/react-query';
import { api } from '../lib/api';
import type { Pair } from '../lib/types';

export function useDeleteStrategy() {
  const { modal, message } = App.useApp();
  const client = useQueryClient();
  const locked = useRef(false);
  const [busy, setBusy] = useState(false);
  const remove = async (pair: Pair, beforeConfirm?: () => Promise<boolean>) => {
    if (locked.current) return false;
    locked.current = true;
    setBusy(true);
    try {
      if (beforeConfirm && !await beforeConfirm()) return false;
      const accepted = await modal.confirm({
        title: `删除 ${pair.symbol} 策略？`,
        content: '请先暂停策略并等待挂单撤销。删除不会平仓，已有持仓、成交历史和恢复数据会保留；至少需要保留一个交易对。',
        okText: '确认删除', okButtonProps: { danger: true }, cancelText: '取消',
      });
      if (!accepted) return false;
      await api(`/api/pairs/${encodeURIComponent(pair.symbol)}`, { method: 'DELETE', body: JSON.stringify({ confirm: true }) });
      message.success('策略已删除，持仓和历史记录保留');
      void client.invalidateQueries({ queryKey: ['pairs'] });
      void client.invalidateQueries({ queryKey: ['overview'] });
      return true;
    } catch (error) {
      message.error((error as Error).message);
      return false;
    } finally {
      locked.current = false;
      setBusy(false);
    }
  };
  return { remove, busy };
}

export function DeleteStrategyButton({ pair }: { pair: Pair }) {
  const { remove, busy } = useDeleteStrategy();
  return <Button danger type="text" size="small" aria-label={`删除 ${pair.symbol} 策略`} loading={busy} onClick={() => void remove(pair)}>删除策略</Button>;
}
