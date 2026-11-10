import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import { App, Button, Dropdown, Space } from 'antd';
import { MoreOutlined } from '@ant-design/icons';
import { useQueryClient } from '@tanstack/react-query';
import { api, pairPath, post } from '../lib/api';
import type { Action, Pair, Snapshot } from '../lib/types';
import { StrategyToggle, strategySecondaryItems } from './StrategyMenu';
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
interface ControlState { busy: Set<string>; pending: Record<string, Action>; results: Record<string, string>; clearResult?: (symbol: string) => void; run: (pair: Pair, action: Action, confirmed?: boolean) => Promise<ControlResult> }
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
function controlConfirmCopy(pair: Pair, action: Action) {
  const titleAction = action === 'resume' ? '恢复策略' : action === 'rebalance' ? '重建网格' : '撤单并暂停';
  const content = action === 'resume'
    ? (pair.mode === 'LIVE' ? '恢复后将使用真实资金下单，请确认交易对和参数。' : '恢复后将按当前参数继续维护网格。')
    : action === 'rebalance'
      ? '将撤销机器人挂单并重新构建网格，请确认交易对和参数。'
      : '将撤销此交易对的机器人挂单并暂停，保留持仓。';
  return { title: `${pair.symbol} · ${titleAction}`, content };
}
function resolveControlOutcome(action: Action, observedState: boolean): { resultText: string; confirmedState: boolean; outcome: ControlResult } {
  const cancellationUnverified = action === 'pause' || action === 'cancel_all';
  const confirmedState = observedState && !cancellationUnverified;
  if (cancellationUnverified) {
    return {
      resultText: observedState
        ? '已暂停，撤单结果尚未确认，请检查交易所挂单及日志'
        : '已提交，暂停状态及撤单结果尚未确认，请检查交易所挂单及日志',
      confirmedState: false,
      outcome: observedState ? 'cancellation_unconfirmed' : 'state_unconfirmed',
    };
  }
  return {
    resultText: confirmedState ? (action === 'rebalance' ? '重建请求已提交，请查看挂单及日志' : '已确认') : '已提交，状态尚未确认，请查看日志',
    confirmedState,
    outcome: confirmedState ? 'confirmed' : 'state_unconfirmed',
  };
}
const Context = createContext<ControlState>(null!);
export const useControls = () => useContext(Context);
export function ControlProvider({ pairs, children }: { pairs?: Pair[]; children: React.ReactNode }) {
  const { modal, message } = App.useApp();
  const client = useQueryClient();
  const locked = useRef(new Set<string>());
  const [busy, setBusy] = useState(new Set<string>());
  const [pending, setPending] = useState<Record<string, Action>>({});
  const [results, setResults] = useState<Record<string, string>>({});
  const clearResult = useCallback((symbol: string) => {
    setResults(current => {
      if (!(symbol in current)) return current;
      const { [symbol]: _removed, ...rest } = current;
      return rest;
    });
  }, []);
  useEffect(() => {
    if (!pairs) return;
    const active = new Set(pairs.map(pair => pair.symbol));
    setResults(current => {
      const keys = Object.keys(current);
      if (keys.every(key => active.has(key))) return current;
      return Object.fromEntries(Object.entries(current).filter(([key]) => active.has(key)));
    });
  }, [pairs]);
  const run = useCallback(async (pair: Pair, action: Action, confirmed = false): Promise<ControlResult> => {
    if (locked.current.has(pair.symbol)) return 'not_submitted';
    locked.current.add(pair.symbol); setBusy(new Set(locked.current));
    let submitted = false;
    try {
      const dangerous = action === 'rebalance' || action === 'cancel_all' || (action === 'resume' && pair.mode === 'LIVE');
      if (dangerous && !confirmed) {
        const { title, content } = controlConfirmCopy(pair, action);
        const accepted = await modal.confirm({ title, content, okText: '确认执行', cancelText: '取消' });
        if (!accepted) return 'not_submitted';
      }
      setResults(r => ({ ...r, [pair.symbol]: '正在提交…' }));
      setPending(p => ({ ...p, [pair.symbol]: action }));
      submitted = true;
      await post(pairPath(pair, 'control'), action === 'resume' || action === 'rebalance' ? { action, expected_mode: pair.mode } : { action });
      const observedState = await confirmControlState(pair, action).catch(() => false);
      const { resultText, confirmedState, outcome } = resolveControlOutcome(action, observedState);
      setResults(r => ({ ...r, [pair.symbol]: resultText }));
      if (!confirmedState) message.warning(`${pair.symbol}：${resultText}`);
      return outcome;
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
      if (submitted) {
        void client.invalidateQueries({ queryKey: ['pairs'] });
        void client.invalidateQueries({ queryKey: ['overview'] });
        void client.invalidateQueries({ queryKey: ['snapshot', pair.symbol] });
      }
    }
  }, [client, message, modal]);
  const value = useMemo(() => ({ busy, pending, results, clearResult, run }), [busy, pending, results, clearResult, run]);
  return <Context.Provider value={value}>{children}</Context.Provider>;
}
export function PairControls({ pair, stale = false }: { pair: Pair; stale?: boolean }) {
  const { busy, run } = useControls();
  return <Space>
    <StrategyToggle pair={pair} stale={stale} size="middle" className={undefined} withSymbolAriaLabel={false} />
    <Dropdown trigger={['click']} menu={{ items: strategySecondaryItems(pair, stale), onClick: ({ key }) => void run(pair, key as Action) }} disabled={busy.has(pair.symbol)}>
      <Button size="middle" icon={<MoreOutlined />} aria-label={`${pair.symbol} 更多操作`}>更多操作</Button>
    </Dropdown>
  </Space>;
}
