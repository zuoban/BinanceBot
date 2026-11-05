import { Button, Drawer } from 'antd';
import { Link } from 'react-router-dom';
import type { Action, Pair } from '../lib/types';
import type { ControlResult } from './Controls';
import { SignalChip, type Signal } from './Common';

export type BatchOutcome = { pair: Pair; result: Exclude<ControlResult, 'confirmed'> };
export interface BatchReport { action: 'pause' | 'resume'; total: number; outcomes: BatchOutcome[] }

const meta = (action: 'pause' | 'resume'): Record<BatchOutcome['result'], { label: string; tier: Signal; retry: boolean }> => ({
  failed: { label: '提交前出错，未提交', tier: 'red', retry: true },
  not_submitted: { label: '操作进行中，未提交', tier: 'yellow', retry: true },
  request_unconfirmed: { label: '请求结果不明，操作可能已生效', tier: 'orange', retry: false },
  state_unconfirmed: { label: action === 'pause' ? '暂停状态及撤单结果未确认' : '恢复状态未确认', tier: 'orange', retry: false },
  cancellation_unconfirmed: { label: '已暂停，撤单结果未确认', tier: 'yellow', retry: false },
});
const order: BatchOutcome['result'][] = ['failed', 'request_unconfirmed', 'not_submitted', 'state_unconfirmed', 'cancellation_unconfirmed'];

/** Per-pair outcome of a batch action. Only strategies that were certainly not submitted offer a retry. */
export function BatchResultDrawer({ report, onClose, onRetry }: { report: BatchReport | null; onClose: () => void; onRetry: (pair: Pair, action: Action) => void }) {
  const labels = report ? meta(report.action) : null;
  const rows = report ? [...report.outcomes].sort((a, b) => order.indexOf(a.result) - order.indexOf(b.result)) : [];
  const verb = report?.action === 'pause' ? '暂停' : '恢复';
  return <Drawer open={!!report} onClose={onClose} size={420} title={report ? `全部${verb}：${report.total - rows.length}/${report.total} 已确认` : ''} aria-label="批量操作结果">
    {report && labels && <>
      <p className="muted">以下 {rows.length} 个交易对需要检查。请核对状态、交易所挂单和日志后再继续操作。</p>
      <ul className="batch-results">
        {rows.map(({ pair, result }) => <li key={pair.symbol}>
          <div className="batch-results-head"><Link to={`/pairs/${pair.symbol}`} onClick={onClose}>{pair.symbol}</Link><SignalChip tier={labels[result].tier}>{labels[result].label}</SignalChip></div>
          {labels[result].retry && <Button size="small" onClick={() => onRetry(pair, report.action)}>重试{verb}</Button>}
        </li>)}
      </ul>
    </>}
  </Drawer>;
}
