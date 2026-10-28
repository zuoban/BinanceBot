import { createContext, useContext, useRef, useState } from 'react';
import { App, Button, Dropdown, Space, Tooltip } from 'antd';
import { MoreOutlined, PauseOutlined, PlayCircleOutlined } from '@ant-design/icons';
import { useQueryClient } from '@tanstack/react-query';
import { api, pairPath, post } from '../lib/api';
import type { Action, Pair, Snapshot } from '../lib/types';

interface ControlState { busy: Set<string>; results: Record<string, string>; run: (pair: Pair, action: Action, confirmed?: boolean) => Promise<boolean> }
export async function runBatchControls(pairs: Pair[], action: 'pause' | 'resume', run: ControlState['run']) {
  if (action === 'pause') {
    const results = await Promise.allSettled(pairs.map(pair => run(pair, action, true)));
    const failure = results.find(result => result.status === 'rejected');
    if (failure?.status === 'rejected') throw failure.reason;
  } else {
    for (const pair of pairs) await run(pair, action, true);
  }
}
const Context = createContext<ControlState>(null!);
export const useControls = () => useContext(Context);
export function ControlProvider({ children }: { children: React.ReactNode }) {
  const { modal, message } = App.useApp();
  const client = useQueryClient();
  const locked = useRef(new Set<string>());
  const [busy, setBusy] = useState(new Set<string>());
  const [results, setResults] = useState<Record<string, string>>({});
  const run = async (pair: Pair, action: Action, confirmed = false) => {
    if (locked.current.has(pair.symbol)) return false;
    locked.current.add(pair.symbol); setBusy(new Set(locked.current));
    try {
      const dangerous = action === 'rebalance' || action === 'cancel_all' || (action === 'resume' && pair.mode === 'LIVE');
      if (dangerous && !confirmed) {
        const accepted = await modal.confirm({
          title: `${pair.symbol} · ${action === 'resume' ? '恢复实盘策略' : action === 'rebalance' ? '重建网格' : '撤单并暂停'}`,
          content: action === 'resume' ? '恢复后将使用真实资金下单，请确认交易对和参数。' : action === 'rebalance' ? '将撤销机器人挂单并重新构建网格。' : '将撤销此交易对的机器人挂单并暂停，保留持仓。',
          okText: '确认执行', cancelText: '取消',
        });
        if (!accepted) return false;
      }
      setResults(r => ({ ...r, [pair.symbol]: '正在提交…' }));
      await post(pairPath(pair, 'control'), { action });
      let confirmedState = action === 'rebalance';
      if (!confirmedState) {
        for (let attempt = 0; attempt < 5; attempt++) {
          await new Promise(resolve => setTimeout(resolve, 800));
          const snapshot = await api<Snapshot>(pairPath(pair, 'status'));
          const matches = action === 'resume' ? snapshot.status === 'RUNNING' : snapshot.status === 'PAUSED' && snapshot.active_orders.length === 0;
          if (matches) { confirmedState = true; break; }
        }
      }
      const result = confirmedState ? (action === 'rebalance' ? '重建请求已提交，请查看挂单及日志' : '已确认') : '已提交，状态尚未确认，请查看日志';
      setResults(r => ({ ...r, [pair.symbol]: result }));
      if (!confirmedState) message.warning(`${pair.symbol}：${result}`);
      return confirmedState;
    } catch (error) {
      const text = (error as Error).message;
      setResults(r => ({ ...r, [pair.symbol]: `失败：${text}` }));
      message.error(`${pair.symbol}：${text}`);
      return false;
    } finally {
      locked.current.delete(pair.symbol); setBusy(new Set(locked.current));
      void client.invalidateQueries({ queryKey: ['pairs'] });
      void client.invalidateQueries({ queryKey: ['overview'] });
      void client.invalidateQueries({ queryKey: ['snapshot', pair.symbol] });
    }
  };
  return <Context.Provider value={{ busy, results, run }}>{children}</Context.Provider>;
}
export function PairControls({ pair, compact = false, stale = false }: { pair: Pair; compact?: boolean; stale?: boolean }) {
  const { busy, run } = useControls();
  const running = pair.status === 'RUNNING';
  return <Space>
    <Tooltip title={stale && !running ? '状态过期，恢复前请刷新' : undefined}>
      <Button size={compact ? 'small' : 'middle'} loading={busy.has(pair.symbol)} disabled={stale && !running}
        icon={running ? <PauseOutlined /> : <PlayCircleOutlined />} onClick={() => void run(pair, running ? 'pause' : 'resume')}>
        {running ? '暂停' : '恢复'}
      </Button>
    </Tooltip>
    <Dropdown trigger={['click']} menu={{ items: [
      { key: 'cancel_all', label: `撤单并暂停 ${pair.symbol}`, danger: true },
      { key: 'rebalance', label: `重建 ${pair.symbol} 网格`, disabled: stale },
    ], onClick: ({ key }) => void run(pair, key as Action) }} disabled={busy.has(pair.symbol)}>
      <Button size={compact ? 'small' : 'middle'} icon={<MoreOutlined />} aria-label={`${pair.symbol} 更多操作`} />
    </Dropdown>
  </Space>;
}
