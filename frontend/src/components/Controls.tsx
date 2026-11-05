import { createContext, useContext, useRef, useState } from 'react';
import { App, Button, Dropdown, Space, Tooltip } from 'antd';
import { MoreOutlined, PauseOutlined, PlayCircleOutlined } from '@ant-design/icons';
import { useQueryClient } from '@tanstack/react-query';
import { api, pairPath, post } from '../lib/api';
import type { Action, Pair, Snapshot } from '../lib/types';

async function confirmControlState(pair: Pair, action: Action): Promise<boolean> {
  if (action === 'rebalance') return true;
  for (let attempt = 0; attempt < 5; attempt++) {
    await new Promise(resolve => setTimeout(resolve, 800));
    const snapshot = await api<Snapshot>(pairPath(pair, 'status'));
    const matches = action === 'resume' ? snapshot.status === 'RUNNING' : snapshot.status === 'PAUSED';
    if (matches) return true;
  }
  return false;
}

export type ControlResult = 'confirmed' | 'cancellation_unconfirmed' | 'state_unconfirmed' | 'request_unconfirmed' | 'failed' | 'not_submitted';
export const pendingLabel: Record<Action, string> = { pause: '暂停中…', resume: '恢复中…', cancel_all: '撤单中…', rebalance: '重建中…' };
interface ControlState { busy: Set<string>; pending: Record<string, Action>; results: Record<string, string>; run: (pair: Pair, action: Action, confirmed?: boolean) => Promise<ControlResult> }
export async function runBatchControls(pairs: Pair[], action: 'pause' | 'resume', run: ControlState['run']): Promise<{ pair: Pair; result: Exclude<ControlResult, 'confirmed'> }[]> {
  const outcomes: { pair: Pair; result: Exclude<ControlResult, 'confirmed'> }[] = [];
  const record = (pair: Pair, result: ControlResult) => {
    if (result !== 'confirmed') outcomes.push({ pair, result });
  };
  if (action === 'pause') {
    const results = await Promise.allSettled(pairs.map(pair => run(pair, action, true)));
    results.forEach((result, index) => record(pairs[index], result.status === 'fulfilled' ? result.value : 'request_unconfirmed'));
  } else {
    for (const pair of pairs) {
      try { record(pair, await run(pair, action, true)); } catch { record(pair, 'request_unconfirmed'); }
    }
  }
  return outcomes;
}
const Context = createContext<ControlState>(null!);
export const useControls = () => useContext(Context);
export function ControlProvider({ children }: { children: React.ReactNode }) {
  const { modal, message } = App.useApp();
  const client = useQueryClient();
  const locked = useRef(new Set<string>());
  const [busy, setBusy] = useState(new Set<string>());
  const [pending, setPending] = useState<Record<string, Action>>({});
  const [results, setResults] = useState<Record<string, string>>({});
  const run = async (pair: Pair, action: Action, confirmed = false): Promise<ControlResult> => {
    if (locked.current.has(pair.symbol)) return 'not_submitted';
    locked.current.add(pair.symbol); setBusy(new Set(locked.current));
    let submitted = false;
    try {
      const dangerous = action === 'rebalance' || action === 'cancel_all' || (action === 'resume' && pair.mode === 'LIVE');
      if (dangerous && !confirmed) {
        const accepted = await modal.confirm({
          title: `${pair.symbol} · ${action === 'resume' ? '恢复策略' : action === 'rebalance' ? '重建网格' : '撤单并暂停'}`,
          content: action === 'resume' ? (pair.mode === 'LIVE' ? '恢复后将使用真实资金下单，请确认交易对和参数。' : '恢复后将按当前参数继续维护网格。') : action === 'rebalance' ? '将撤销机器人挂单并重新构建网格，请确认交易对和参数。' : '将撤销此交易对的机器人挂单并暂停，保留持仓。',
          okText: '确认执行', cancelText: '取消',
        });
        if (!accepted) return 'not_submitted';
      }
      setResults(r => ({ ...r, [pair.symbol]: '正在提交…' }));
      setPending(p => ({ ...p, [pair.symbol]: action }));
      submitted = true;
      await post(pairPath(pair, 'control'), action === 'resume' || action === 'rebalance' ? { action, expected_mode: pair.mode } : { action });
      const observedState = await confirmControlState(pair, action).catch(() => false);
      const cancellationUnverified = action === 'pause' || action === 'cancel_all';
      const confirmedState = observedState && !cancellationUnverified;
      const result = cancellationUnverified
        ? observedState ? '已暂停，撤单结果尚未确认，请检查交易所挂单及日志' : '已提交，暂停状态及撤单结果尚未确认，请检查交易所挂单及日志'
        : confirmedState ? (action === 'rebalance' ? '重建请求已提交，请查看挂单及日志' : '已确认') : '已提交，状态尚未确认，请查看日志';
      setResults(r => ({ ...r, [pair.symbol]: result }));
      if (!confirmedState) message.warning(`${pair.symbol}：${result}`);
      return cancellationUnverified ? (observedState ? 'cancellation_unconfirmed' : 'state_unconfirmed') : (confirmedState ? 'confirmed' : 'state_unconfirmed');
    } catch (error) {
      const text = error instanceof Error ? error.message : String(error);
      if (submitted) {
        const result = `请求结果不明，操作可能已生效：${text}；请刷新状态并核对交易所挂单及日志后再操作`;
        setResults(r => ({ ...r, [pair.symbol]: result }));
        message.warning(`${pair.symbol}：${result}`);
        return 'request_unconfirmed';
      }
      setResults(r => ({ ...r, [pair.symbol]: `未提交：${text}` }));
      message.error(`${pair.symbol}：${text}`);
      return 'failed';
    } finally {
      locked.current.delete(pair.symbol); setBusy(new Set(locked.current));
      setPending(p => { const { [pair.symbol]: _done, ...rest } = p; return rest; });
      void client.invalidateQueries({ queryKey: ['pairs'] });
      void client.invalidateQueries({ queryKey: ['overview'] });
      void client.invalidateQueries({ queryKey: ['snapshot', pair.symbol] });
    }
  };
  return <Context.Provider value={{ busy, pending, results, run }}>{children}</Context.Provider>;
}
export function PairControls({ pair, compact = false, stale = false }: { pair: Pair; compact?: boolean; stale?: boolean }) {
  const { busy, pending, run } = useControls();
  const running = pair.status === 'RUNNING';
  const active = pending[pair.symbol];
  return <Space>
    <Tooltip title={stale && !running ? '状态过期，恢复前请刷新' : undefined}>
      <Button size={compact ? 'small' : 'middle'} loading={busy.has(pair.symbol)} disabled={stale && !running}
        icon={running ? <PauseOutlined /> : <PlayCircleOutlined />} onClick={() => void run(pair, running ? 'pause' : 'resume')}>
        {active ? pendingLabel[active] : running ? '暂停' : '恢复'}
      </Button>
    </Tooltip>
    <Dropdown trigger={['click']} menu={{ items: [
      { key: 'cancel_all', label: `撤单并暂停 ${pair.symbol}`, danger: true },
      { key: 'rebalance', label: `重建 ${pair.symbol} 网格`, disabled: stale || !running },
    ], onClick: ({ key }) => void run(pair, key as Action) }} disabled={busy.has(pair.symbol)}>
      <Button size={compact ? 'small' : 'middle'} icon={<MoreOutlined />} aria-label={`${pair.symbol} 更多操作`}>更多操作</Button>
    </Dropdown>
  </Space>;
}
