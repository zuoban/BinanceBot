import { Button, Drawer } from 'antd';
import { Link } from 'react-router-dom';
import { pairRoute } from '../lib/api';
import type { Action, Pair } from '../lib/types';
import type { ControlResult } from './Controls';
import { DirectionText, SignalChip, type Signal } from './Common';

type BatchOutcome = { pair: Pair; result: Exclude<ControlResult, 'confirmed'> };
export interface BatchReport { id: number; action: 'pause' | 'resume'; total: number; outcomes: BatchOutcome[] }

const meta = (action: 'pause' | 'resume'): Record<BatchOutcome['result'], { label: string; tier: Signal; retry: boolean }> => ({
  failed: { label: '提交前出错，未提交', tier: 'red', retry: true },
  not_submitted: { label: '操作进行中，未提交', tier: 'yellow', retry: true },
  request_unconfirmed: { label: '请求结果不明，操作可能已生效', tier: 'orange', retry: false },
  state_unconfirmed: { label: action === 'pause' ? '暂停状态及撤单结果未确认' : '恢复状态未确认', tier: 'orange', retry: false },
  cancellation_unconfirmed: { label: '已暂停，撤单结果未确认', tier: 'yellow', retry: false },
});
const order: BatchOutcome['result'][] = ['failed', 'request_unconfirmed', 'not_submitted', 'state_unconfirmed', 'cancellation_unconfirmed'];

/** Per-pair outcome of a batch action. Only strategies that were certainly not submitted offer a retry. */
export function BatchResultDrawer({ report, busy, onClose, onRetry }: { report: BatchReport | null; busy?: Set<string>; onClose: () => void; onRetry: (pair: Pair, action: Action) => void }) {
  const labels = report ? meta(report.action) : null;
  const rows = report ? [...report.outcomes].sort((a, b) => order.indexOf(a.result) - order.indexOf(b.result)) : [];
  const verb = report?.action === 'pause' ? '暂停' : '恢复';
  const title = !report ? '' : report.action === 'pause'
    ? `全部暂停：${report.total - report.outcomes.filter(o => o.result !== 'cancellation_unconfirmed').length}/${report.total} 已暂停`
    : `全部恢复：${report.total - rows.length}/${report.total} 已确认`;
  return <Drawer open={!!report} onClose={onClose} size={420} title={title} aria-label="批量操作结果">
    {report && labels && <>
      <p className="muted">以下 {rows.length} 个交易对需要检查。请核对状态、交易所挂单和日志后再继续操作。</p>
      <ul className="batch-results">
        {rows.map(({ pair, result }) => <li key={pair.symbol}>
          <div className="batch-results-head"><div className="batch-results-title"><Link to={pairRoute(pair.symbol)} onClick={onClose}>{pair.symbol}</Link><DirectionText direction={pair.grid_config.direction} /></div><SignalChip tier={labels[result].tier}>{labels[result].label}</SignalChip></div>
          {labels[result].retry && <Button size="small" loading={busy?.has(pair.symbol)} disabled={busy?.has(pair.symbol)} onClick={() => onRetry(pair, report.action)}>重试{verb}</Button>}
        </li>)}
      </ul>
    </>}
  </Drawer>;
}
