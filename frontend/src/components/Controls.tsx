import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import { App, Button, Dropdown, Space } from 'antd';
import { MoreOutlined } from '@ant-design/icons';
import { useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, pairPath, post } from '../lib/api';
import { useMountedRef } from '../lib/asyncGuard';
import { useConfirmDialog } from '../lib/confirmDialog';
import { invalidateStrategyQueries } from '../lib/queries';
import type { Action, Pair, Snapshot } from '../lib/types';
import { StrategyToggle, strategySecondaryItems } from './StrategyMenu';
async function confirmControlState(pair: Pair, action: Action, sleep: (ms: number) => Promise<boolean>, signal: AbortSignal): Promise<boolean> {
  if (action === 'rebalance') return true;
  for (let attempt = 0; attempt < 5; attempt++) {
    if (!await sleep(800) || signal.aborted) return false;
    const snapshot = await api<Snapshot>(pairPath(pair, 'status'), { signal });
    if (signal.aborted) return false;
    const matches = action === 'resume' ? snapshot.status === 'RUNNING' : snapshot.status === 'PAUSED';
    if (matches) return true;
  }
  return false;
}

const BATCH_PAUSE_CONCURRENCY = 6;
export type ControlResult = 'confirmed' | 'cancellation_unconfirmed' | 'state_unconfirmed' | 'request_unconfirmed' | 'failed' | 'not_submitted';
interface ControlState { busy: Set<string>; pending: Record<string, Action>; results: Record<string, string>; clearResult: (symbol: string) => void; run: (pair: Pair, action: Action, confirmed?: boolean) => Promise<ControlResult> }
export async function runBatchControls(pairs: Pair[], action: 'pause' | 'resume', run: ControlState['run']): Promise<{ pair: Pair; result: Exclude<ControlResult, 'confirmed'> }[]> {
  const outcomes: { pair: Pair; result: Exclude<ControlResult, 'confirmed'> }[] = [];
  const record = (pair: Pair, result: ControlResult) => {
    if (result !== 'confirmed') outcomes.push({ pair, result });
  };
  if (action === 'pause') {
    const settled = new Array<ControlResult>(pairs.length);
    let nextIndex = 0;
    const worker = async () => {
      while (nextIndex < pairs.length) {
        const index = nextIndex++;
        try { settled[index] = await run(pairs[index], action, true); }
        catch { settled[index] = 'request_unconfirmed'; }
      }
    };
    await Promise.all(Array.from({ length: Math.min(BATCH_PAUSE_CONCURRENCY, pairs.length) }, () => worker()));
    for (let index = 0; index < pairs.length; index++) record(pairs[index], settled[index]);
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
const Context = createContext<ControlState | null>(null);
export function useControls(): ControlState {
  const context = useContext(Context);
  if (!context) throw new Error('useControls must be used within ControlProvider');
  return context;
}
function useAbortableSleep() {
  const wakeups = useRef(new Set<() => void>());
  const abortRef = useRef<AbortController | null>(null);
  if (!abortRef.current || abortRef.current.signal.aborted) abortRef.current = new AbortController();
  useEffect(() => {
    if (!abortRef.current || abortRef.current.signal.aborted) abortRef.current = new AbortController();
    return () => abortRef.current?.abort();
  }, []);
  const mounted = useMountedRef(() => {
    abortRef.current?.abort();
    for (const wake of wakeups.current) wake();
    wakeups.current.clear();
  });
  const sleep = useCallback((ms: number) => new Promise<boolean>(resolve => {
    if (!mounted.current) { resolve(false); return; }
    const wake = () => { window.clearTimeout(timer); wakeups.current.delete(wake); resolve(false); };
    const timer = window.setTimeout(() => { wakeups.current.delete(wake); resolve(mounted.current); }, ms);
    wakeups.current.add(wake);
  }), []);
  return { mounted, abortRef, sleep };
}

function formatControlFailure(submitted: boolean, text: string): { resultText: string; outcome: 'request_unconfirmed' | 'failed'; warn: boolean } {
  if (submitted) {
    return { resultText: `请求结果不明，操作可能已生效：${text}；请刷新状态并核对交易所挂单及日志后再操作`, outcome: 'request_unconfirmed', warn: true };
  }
  return { resultText: `未提交：${text}`, outcome: 'failed', warn: false };
}

export function ControlProvider({ pairs, children }: { pairs?: Pair[]; children: React.ReactNode }) {
  const { message } = App.useApp(), { confirm } = useConfirmDialog(), client = useQueryClient();
  const locked = useRef(new Set<string>());
  const { mounted, abortRef, sleep } = useAbortableSleep();
  const [busy, setBusy] = useState<Set<string>>(new Set());
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
    if (locked.current.has(pair.symbol) || !mounted.current) return 'not_submitted';
    locked.current.add(pair.symbol); setBusy(new Set(locked.current));
    let submitted = false;
    try {
      const dangerous = action === 'rebalance' || action === 'cancel_all' || (action === 'resume' && pair.mode === 'LIVE');
      if (dangerous && !confirmed) {
        const { title, content } = controlConfirmCopy(pair, action);
        if (!await confirm({ title, content, okText: '确认执行', cancelText: '取消' }) || !mounted.current) return 'not_submitted';
      }
      if (!mounted.current) return 'not_submitted';
      setResults(r => ({ ...r, [pair.symbol]: '正在提交…' }));
      setPending(p => ({ ...p, [pair.symbol]: action }));
      submitted = true;
      await post(pairPath(pair, 'control'), action === 'resume' || action === 'rebalance' ? { action, expected_mode: pair.mode } : { action });
      if (!mounted.current) return 'state_unconfirmed';
      const observedState = await confirmControlState(pair, action, sleep, abortRef.current!.signal).catch(() => false);
      const { resultText, confirmedState, outcome } = resolveControlOutcome(action, observedState);
      if (mounted.current) {
        setResults(r => ({ ...r, [pair.symbol]: resultText }));
        if (!confirmedState) message.warning(`${pair.symbol}：${resultText}`);
      }
      return outcome;
    } catch (error) {
      const text = errorMessage(error);
      const { resultText, outcome, warn } = formatControlFailure(submitted, text);
      if (mounted.current) {
        setResults(r => ({ ...r, [pair.symbol]: resultText }));
        if (warn) message.warning(`${pair.symbol}：${resultText}`);
        else message.error(`${pair.symbol}：${text}`);
      }
      return outcome;
    } finally {
      locked.current.delete(pair.symbol);
      if (mounted.current) {
        setBusy(new Set(locked.current));
        setPending(p => { const { [pair.symbol]: _done, ...rest } = p; return rest; });
      }
      if (submitted) void invalidateStrategyQueries(client, { accounts: true, snapshot: pair.symbol });
    }
  }, [abortRef, client, confirm, message, mounted, sleep]);
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
