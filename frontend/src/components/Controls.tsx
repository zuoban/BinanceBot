import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useSyncExternalStore } from 'react';
import { App, Button, Dropdown, Space } from 'antd';
import { MoreOutlined } from '@ant-design/icons';
import { useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, pairPath, post } from '../lib/api';
import { useMountedRef } from '../lib/asyncGuard';
import { useConfirmDialog } from '../lib/confirmDialog';
import { formatDecimal, parseSignedDecimal } from '../lib/decimal';
import { directionLabel, modeLabel, positiveDecimalText, positivePrice, quantity } from '../lib/format';
import { quoteAsset } from '../lib/portfolio';
import { invalidateStrategyQueries } from '../lib/queries';
import type { Action, GridConfig, Mode, Pair, Snapshot, Status } from '../lib/types';
import { StrategyToggle, strategySecondaryItems } from './StrategyMenu';

interface ReverseExpectation {
  targetDirection: GridConfig['direction'];
  expectedStatus: Status;
}

async function confirmControlState(
  pair: Pair,
  action: Action,
  sleep: (ms: number) => Promise<boolean>,
  signal: AbortSignal,
  reverseExpectation?: ReverseExpectation,
): Promise<boolean> {
  if (action === 'rebalance') return true;
  for (let attempt = 0; attempt < 5; attempt++) {
    if (!await sleep(800) || signal.aborted) return false;
    const snapshot = await api<Snapshot>(pairPath(pair, 'status'), { signal });
    if (signal.aborted) return false;
    const matches = action === 'reverse'
      ? Boolean(
          reverseExpectation
          && snapshot.grid_config.direction === reverseExpectation.targetDirection
          && (reverseExpectation.expectedStatus === 'RUNNING' ? snapshot.status === 'RUNNING' : snapshot.status === 'PAUSED'),
        )
      : action === 'resume'
        ? snapshot.status === 'RUNNING'
        : snapshot.status === 'PAUSED';
    if (matches) return true;
  }
  return false;
}

const BATCH_PAUSE_CONCURRENCY = 6;
export type ControlResult = 'confirmed' | 'cancellation_unconfirmed' | 'state_unconfirmed' | 'request_unconfirmed' | 'failed' | 'not_submitted';
interface ControlState { busy: Set<string>; pending: Record<string, Action>; results: Record<string, string>; clearResult: (symbol: string) => void; run: (pair: Pair, action: Action, confirmed?: boolean) => Promise<ControlResult> }
interface ControlStore {
  subscribe: (listener: () => void) => () => void;
  getSnapshot: () => ControlState;
  getSymbolSnapshot: (symbol: string) => ControlState;
}
const emptyBusySet = new Set<string>();
const emptyPendingMap: Record<string, Action> = {};
const emptyResultsMap: Record<string, string> = {};
const noopSubscribe = () => () => {};

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

function buildReversePlan(latest: Snapshot) {
  const fromDirection = latest.grid_config.direction;
  const toDirection: GridConfig['direction'] = fromDirection === 'LONG' ? 'SHORT' : 'LONG';
  const parsedSize = parseSignedDecimal(latest.position.size);
  if (parsedSize && ((fromDirection === 'LONG' && parsedSize.n < 0n) || (fromDirection === 'SHORT' && parsedSize.n > 0n))) {
    throw new Error('当前持仓方向与策略方向不一致，请刷新并核实仓位后再操作');
  }
  const hasPosition = Boolean(parsedSize && parsedSize.n !== 0n);
  const absQtyText = hasPosition && parsedSize ? formatDecimal(parsedSize.n < 0n ? -parsedSize.n : parsedSize.n, parsedSize.s) : '0';
  const formattedQty = hasPosition ? quantity(absQtyText) : '0';
  const currentSideLabel = fromDirection === 'LONG' ? '多头' : '空头';
  const nextOpenVerb = toDirection === 'LONG' ? '开多' : '开空';
  const rawRefPrice = latest.mode === 'PAPER'
    ? (positiveDecimalText(latest.ticker.last_price) ?? positiveDecimalText(latest.ticker.mark_price) ?? positiveDecimalText(latest.position.mark_price))
    : (positiveDecimalText(latest.ticker.mark_price) ?? positiveDecimalText(latest.position.mark_price) ?? positiveDecimalText(latest.ticker.last_price));
  const formattedRefPrice = rawRefPrice ? positivePrice(rawRefPrice) : '--';
  const quote = quoteAsset(latest.symbol, latest.account.asset);
  const stepOneContent = <div className="confirm-changes">
    <p>请核对当前最新仓位与反手执行计划，确认无误后进入最终确认：</p>
    <dl>
      <div className={latest.mode === 'LIVE' ? 'confirm-risky' : undefined}><dt>运行模式</dt><dd><strong>{modeLabel[latest.mode]}</strong></dd></div>
      <div className="confirm-risky"><dt>方向变更</dt><dd><s>{directionLabel(fromDirection)}</s> → <strong>{directionLabel(toDirection)}</strong></dd></div>
      <div><dt>当前持仓</dt><dd><strong>{hasPosition ? `${currentSideLabel} ${formattedQty}` : '空仓（0）'}</strong></dd></div>
      {hasPosition && <div><dt>参考市价</dt><dd>{formattedRefPrice !== '--' ? `${formattedRefPrice}${quote ? ` ${quote}` : ''}` : '待获取'}</dd></div>}
      <div><dt>执行步骤</dt><dd>{hasPosition ? `撤单 → 市价平${currentSideLabel} ${formattedQty} → 切换方向 → 市价${nextOpenVerb} ${formattedQty}` : '撤销策略挂单 → 切换策略方向（不提交市价单）'}</dd></div>
      <div><dt>反手后状态</dt><dd>{latest.status === 'RUNNING' ? '保持运行并按新方向重建网格' : '保持已暂停'}</dd></div>
    </dl>
  </div>;
  const stepTwoContent = <div className="confirm-changes">
    <p>{hasPosition
      ? (latest.mode === 'LIVE'
        ? `即将使用真实资金对 ${latest.symbol} 执行市价平仓并等量反向市价开仓，可能产生滑点与 Taker 手续费，提交后无法撤销。`
        : `即将对 ${latest.symbol}（模拟盘）执行市价平仓并等量反向市价开仓。`)
      : `当前 ${latest.symbol} 为空仓，确认后将撤销策略挂单并将交易方向切换为${directionLabel(toDirection)}。`}</p>
    <dl>
      <div className="confirm-risky"><dt>最终执行</dt><dd><strong>{directionLabel(fromDirection)} → {directionLabel(toDirection)}{hasPosition ? `（等量 ${formattedQty}）` : '（空仓切换）'}</strong></dd></div>
      {hasPosition && <div><dt>异常保护</dt><dd>若中途平仓或反向开仓异常，策略将自动暂停保护</dd></div>}
    </dl>
  </div>;
  return { fromDirection, toDirection, expectedStatus: latest.status, stepOneContent, stepTwoContent };
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
    resultText: confirmedState
      ? (action === 'rebalance' ? '重建请求已提交，请查看挂单及日志' : action === 'reverse' ? '反手交易已完成' : '已确认')
      : '已提交，状态尚未确认，请查看日志',
    confirmedState,
    outcome: confirmedState ? 'confirmed' : 'state_unconfirmed',
  };
}
const Context = createContext<ControlStore | null>(null);
export function useControls(symbol?: string): ControlState {
  const store = useContext(Context);
  if (!store) throw new Error('useControls must be used within ControlProvider');
  return useSyncExternalStore(
    symbol === '' ? noopSubscribe : store.subscribe,
    () => (symbol === undefined ? store.getSnapshot() : store.getSymbolSnapshot(symbol)),
  );
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
  }), [mounted]);
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
  const runRef = useRef<ControlState['run']>(async () => 'not_submitted');
  const store = useMemo(() => {
    const listeners = new Set<() => void>();
    let busy: Set<string> = emptyBusySet;
    let pending: Record<string, Action> = emptyPendingMap;
    let results: Record<string, string> = emptyResultsMap;
    const symbolCache = new Map<string, { isBusy: boolean; pendingAction: Action | undefined; resultText: string | undefined; snapshot: ControlState }>();
    const stableRun: ControlState['run'] = (pair, action, confirmed) => runRef.current(pair, action, confirmed);
    const clearResult = (symbol: string) => {
      if (!(symbol in results)) return;
      const { [symbol]: _removed, ...rest } = results;
      results = Object.keys(rest).length ? rest : emptyResultsMap;
      refreshFull();
    };
    let fullSnapshot: ControlState = { busy, pending, results, clearResult, run: stableRun };
    const emptySnapshot: ControlState = { busy: emptyBusySet, pending: emptyPendingMap, results: emptyResultsMap, clearResult, run: stableRun };
    const notify = () => {
      for (const listener of listeners) listener();
    };
    const refreshFull = () => {
      fullSnapshot = { busy, pending, results, clearResult, run: stableRun };
      notify();
    };
    return {
      subscribe: (listener: () => void) => {
        listeners.add(listener);
        return () => { listeners.delete(listener); };
      },
      getSnapshot: () => fullSnapshot,
      getSymbolSnapshot: (symbol: string) => {
        if (!symbol) return emptySnapshot;
        const isBusy = busy.has(symbol);
        const pendingAction = pending[symbol];
        const resultText = results[symbol];
        const cached = symbolCache.get(symbol);
        if (cached && cached.isBusy === isBusy && cached.pendingAction === pendingAction && cached.resultText === resultText) {
          return cached.snapshot;
        }
        const snapshot: ControlState = {
          busy: isBusy ? new Set([symbol]) : emptyBusySet,
          pending: pendingAction ? { [symbol]: pendingAction } : emptyPendingMap,
          results: resultText !== undefined ? { [symbol]: resultText } : emptyResultsMap,
          clearResult,
          run: stableRun,
        };
        symbolCache.set(symbol, { isBusy, pendingAction, resultText, snapshot });
        return snapshot;
      },
      setBusyFromLocked: (currentLocked: Set<string>) => {
        busy = currentLocked.size ? new Set(currentLocked) : emptyBusySet;
        refreshFull();
      },
      startSubmission: (symbol: string, action: Action) => {
        results = { ...results, [symbol]: '正在提交…' };
        pending = { ...pending, [symbol]: action };
        refreshFull();
      },
      setResult: (symbol: string, text: string) => {
        if (results[symbol] === text) return;
        results = { ...results, [symbol]: text };
        refreshFull();
      },
      finishSymbol: (symbol: string, currentLocked: Set<string>, updateMounted: boolean) => {
        if (!updateMounted) return;
        busy = currentLocked.size ? new Set(currentLocked) : emptyBusySet;
        if (symbol in pending) {
          const { [symbol]: _done, ...rest } = pending;
          pending = Object.keys(rest).length ? rest : emptyPendingMap;
        }
        refreshFull();
      },
      pruneResults: (active: Set<string>) => {
        const keys = Object.keys(results);
        if (keys.every(key => active.has(key))) return;
        const filtered = Object.entries(results).filter(([key]) => active.has(key));
        results = filtered.length ? Object.fromEntries(filtered) : emptyResultsMap;
        for (const cachedSymbol of symbolCache.keys()) {
          if (!active.has(cachedSymbol)) symbolCache.delete(cachedSymbol);
        }
        refreshFull();
      },
    };
  }, []);
  useEffect(() => {
    if (!pairs) return;
    store.pruneResults(new Set(pairs.map(pair => pair.symbol)));
  }, [pairs, store]);
  const run = useCallback(async (pair: Pair, action: Action, confirmed = false): Promise<ControlResult> => {
    if (locked.current.has(pair.symbol) || !mounted.current) return 'not_submitted';
    locked.current.add(pair.symbol);
    store.setBusyFromLocked(locked.current);
    let submitted = false;
    try {
      let expectedMode: Mode = pair.mode;
      let expectedDirection: GridConfig['direction'] = pair.grid_config.direction;
      let reverseExpectation: ReverseExpectation | undefined;
      if (action === 'reverse') {
        if (!confirmed) {
          const latest = await api<Snapshot>(pairPath(pair, 'status'), { signal: abortRef.current!.signal, readOnly: true });
          if (!mounted.current) return 'not_submitted';
          if (!latest || latest.symbol !== pair.symbol) throw new Error('获取交易对最新状态异常，请刷新后重试');
          if (latest.mode === 'UNKNOWN') throw new Error('策略运行模式未知，无法执行反手交易');
          const plan = buildReversePlan(latest);
          expectedMode = latest.mode;
          expectedDirection = plan.fromDirection;
          reverseExpectation = { targetDirection: plan.toDirection, expectedStatus: plan.expectedStatus };
          if (!await confirm({ title: `${latest.symbol} · 反手交易确认（1/2）`, content: plan.stepOneContent, okText: '下一步', cancelText: '取消' }) || !mounted.current) {
            return 'not_submitted';
          }
          if (!await confirm({ title: `${latest.symbol} · 最终执行确认（2/2）`, content: plan.stepTwoContent, okText: '确认反手', okButtonProps: { danger: true }, cancelText: '取消' }) || !mounted.current) {
            return 'not_submitted';
          }
        } else {
          if (expectedMode === 'UNKNOWN') throw new Error('策略运行模式未知，无法执行反手交易');
          reverseExpectation = {
            targetDirection: expectedDirection === 'LONG' ? 'SHORT' : 'LONG',
            expectedStatus: pair.status,
          };
        }
      } else {
        const dangerous = action === 'rebalance' || action === 'cancel_all' || (action === 'resume' && pair.mode === 'LIVE');
        if (dangerous && !confirmed) {
          const { title, content } = controlConfirmCopy(pair, action);
          if (!await confirm({ title, content, okText: '确认执行', cancelText: '取消' }) || !mounted.current) return 'not_submitted';
        }
      }
      if (!mounted.current) return 'not_submitted';
      store.startSubmission(pair.symbol, action);
      submitted = true;
      if (action === 'reverse') {
        await post(pairPath(pair, 'control'), { action: 'reverse', expected_mode: expectedMode, expected_direction: expectedDirection }, { timeoutMs: 60_000 });
      } else {
        await post(pairPath(pair, 'control'), action === 'resume' || action === 'rebalance' ? { action, expected_mode: pair.mode } : { action });
      }
      if (!mounted.current) return 'state_unconfirmed';
      const observedState = await confirmControlState(pair, action, sleep, abortRef.current!.signal, reverseExpectation).catch(() => false);
      const { resultText, confirmedState, outcome } = resolveControlOutcome(action, observedState);
      if (mounted.current) {
        store.setResult(pair.symbol, resultText);
        if (!confirmedState) message.warning(`${pair.symbol}：${resultText}`);
      }
      return outcome;
    } catch (error) {
      const text = errorMessage(error);
      const { resultText, outcome, warn } = formatControlFailure(submitted, text);
      if (mounted.current) {
        store.setResult(pair.symbol, resultText);
        if (warn) message.warning(`${pair.symbol}：${resultText}`);
        else message.error(`${pair.symbol}：${text}`);
      }
      return outcome;
    } finally {
      locked.current.delete(pair.symbol);
      store.finishSymbol(pair.symbol, locked.current, mounted.current);
      if (submitted) {
        void invalidateStrategyQueries(client, {
          accounts: true,
          snapshot: pair.symbol,
          ...(action === 'reverse' ? { configSymbol: pair.symbol } : {}),
        });
      }
    }
  }, [abortRef, client, confirm, message, mounted, sleep, store]);
  runRef.current = run;
  return <Context.Provider value={store}>{children}</Context.Provider>;
}
export function PairControls({ pair, stale = false }: { pair: Pair; stale?: boolean }) {
  const { busy, run } = useControls(pair.symbol);
  return <Space>
    <StrategyToggle pair={pair} stale={stale} size="middle" className={undefined} withSymbolAriaLabel={false} />
    <Dropdown trigger={['click']} menu={{ items: strategySecondaryItems(pair, stale), onClick: ({ key }) => void run(pair, key as Action) }} disabled={busy.has(pair.symbol)}>
      <Button size="middle" icon={<MoreOutlined />} aria-label={`${pair.symbol} 更多操作`}>更多操作</Button>
    </Dropdown>
  </Space>;
}
