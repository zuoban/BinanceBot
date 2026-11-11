import { useContext, useEffect, useLayoutEffect, useState } from 'react';
import { Button, Card, Col, Form, InputNumber, Row, Switch, type FormInstance } from 'antd';
import { describeNewGridConfig, diffGridConfig, type FieldChange } from '../lib/configDiff';
import { positiveDecimalText } from '../lib/format';
import { isPercentDecimal, isPositiveDecimal, priceRangeError } from '../lib/gridPreview';
import { AccountsContext } from '../lib/session';
import type { GridConfig, Mode } from '../lib/types';

export type FormValues = Omit<GridConfig, 'post_only'>;
export const defaults: FormValues = { symbol: '', direction: 'LONG', grid_interval: '0.1', order_amount_usdc: '100', buy_window: 3, sell_window: 3, min_price: null, max_price: null, max_position_usdc: '1000', pause_on_max_position: true, cancel_opening_on_max_position: false, min_liquidation_distance_pct: null };
export const directionOptions = [{ value: 'LONG', label: '做多 · 买入开仓 / 卖出平仓' }, { value: 'SHORT', label: '做空 · 卖出开仓 / 买入平仓' }];

export function AccountMarkBridge({ symbol, mode, onMark }: { symbol: string; mode: Mode | undefined; onMark: (mark: string | undefined) => void }) {
  const accounts = useContext(AccountsContext);
  const next = positiveDecimalText(accounts.find(entry => entry.snapshot.symbol === symbol && entry.mode === mode && !entry.account_observation_stale)?.snapshot.ticker.mark_price) ?? undefined;
  useLayoutEffect(() => { onMark(next); }, [next, onMark]);
  return null;
}

export function LiveConfigConfirmContent({ changes, values }: { changes: FieldChange[] | null; values: FormValues }) {
  const items = changes ?? describeNewGridConfig(values).map(item => ({ key: item.label, label: item.label, before: '', after: item.value, risky: item.risky }));
  return <div className="confirm-changes">
    <p>实盘使用真实资金，请核对以下{changes ? '修改' : '参数'}。</p>
    {changes?.length === 0 && <p className="muted">未检测到参数修改，将按当前参数重新保存。</p>}
    <dl>
      {items.map(item => <div key={item.key} className={item.risky ? 'confirm-risky' : undefined}>
        <dt>{item.label}</dt>
        <dd>{item.before ? <><s>{item.before}</s> → <strong>{item.after}</strong></> : <strong>{item.after}</strong>}</dd>
      </div>)}
    </dl>
  </div>;
}

export function PairRiskAndRangeSection() {
  return <Row gutter={[20, 20]}>
    <Col xs={24} xl={14} className="config-risk-col">
      <Card title="仓位限制">
        <Row gutter={[20, 16]}>
          <Col span={24}><Form.Item name="max_position_usdc" label="最大持仓名义价值" extra={<div className="field-help"><p><strong>注意：</strong>留空表示不设置；选择暂停策略或仅撤销开仓单时须填写大于 0 的上限。</p></div>} dependencies={['pause_on_max_position', 'cancel_opening_on_max_position']} rules={[({ getFieldValue }) => ({ validator: (_, value) => { const pause = Boolean(getFieldValue('pause_on_max_position')); const cancelOpening = Boolean(getFieldValue('cancel_opening_on_max_position')); if (pause || cancelOpening) return isPositiveDecimal(value) ? Promise.resolve() : Promise.reject(new Error(pause ? '暂停策略需要大于 0 的持仓上限' : '撤销开仓单需要大于 0 的持仓上限')); if (value == null || value === '') return Promise.resolve(); return isPositiveDecimal(value) ? Promise.resolve() : Promise.reject(new Error('请输入大于 0 的持仓上限')); } })]}><InputNumber stringMode min="0" className="full-width-input" /></Form.Item></Col>
          <Col span={24}><Form.Item name="pause_on_max_position" label="持仓上限处理方式" extra={<div className="field-help"><p><strong>触发条件：</strong>选择暂停策略时，当前持仓名义价值达到上限，或下一笔完整开仓单成交后预计超过上限。</p><p><strong>执行动作：</strong>暂停策略并尝试撤销机器人挂单；限制开仓会保留平仓订单。</p><p><strong>恢复：</strong>暂停后需手动恢复。</p><p><strong>注意：</strong>撤单可能失败，请检查交易所挂单。仅挂单占用额度不会触发暂停。</p></div>} valuePropName="checked"><Switch checkedChildren="暂停策略" unCheckedChildren="限制开仓" /></Form.Item></Col>
          <Col span={24}><Form.Item name="cancel_opening_on_max_position" label="达到上限时仅撤开仓单" extra={<div className="field-help"><p><strong>触发条件：</strong>开启后，持仓名义价值达到上限。</p><p><strong>执行动作：</strong>只撤销开仓方向的挂单，策略保持运行并继续维护平仓单。</p><p><strong>恢复：</strong>回落到上限以下后自动恢复开仓。</p><p><strong>注意：</strong>与「暂停策略」互斥，开启一项会关闭另一项。</p></div>} valuePropName="checked"><Switch checkedChildren="撤开仓单" unCheckedChildren="关闭" /></Form.Item></Col>
          <Col span={24}><Form.Item name="min_liquidation_distance_pct" label="强平距离阈值（%）" extra={<div className="field-help"><p><strong>触发条件：</strong>持仓的标记价格距强平价不超过该百分比（占标记价）时。</p><p><strong>执行动作：</strong>只撤销开仓方向的挂单并保持运行，继续维护平仓单。</p><p><strong>恢复：</strong>距离回升后自动恢复开仓。</p><p><strong>注意：</strong>留空表示不启用。依赖交易所返回的强平价，模拟盘或无强平价时不生效。</p></div>} rules={[{ validator: (_, value) => value == null || value === '' || isPercentDecimal(value) ? Promise.resolve() : Promise.reject(new Error('请输入大于 0 且小于 100 的百分比')) }]}><InputNumber stringMode min="0" max="100" className="full-width-input" /></Form.Item></Col>
        </Row>
      </Card>
    </Col>
    <Col xs={24} xl={10} className="config-risk-col">
      <Card title="价格范围与订单">
        <Row gutter={[20, 16]}>
          <Col span={24}><Form.Item name="min_price" label="最低价格（可选）" dependencies={['max_price']} rules={[({ getFieldValue }) => ({ validator: (_, value) => { const error = priceRangeError('min', value, getFieldValue('max_price')); return error ? Promise.reject(new Error(error)) : Promise.resolve(); } })]}><InputNumber stringMode min="0" className="full-width-input" /></Form.Item></Col>
          <Col span={24}><Form.Item name="max_price" label="最高价格（可选）" dependencies={['min_price']} rules={[({ getFieldValue }) => ({ validator: (_, value) => { const error = priceRangeError('max', getFieldValue('min_price'), value); return error ? Promise.reject(new Error(error)) : Promise.resolve(); } })]}><InputNumber stringMode min="0" className="full-width-input" /></Form.Item></Col>
        </Row>
        <div className="settings-card-footer config-card-note"><p className="muted">订单仅作为挂单成交（Post Only）；如果订单会立即成交，交易所会拒绝该订单。</p></div>
      </Card>
    </Col>
  </Row>;
}

export function SaveBarStatus({ form, adding, dirty, symbol, baseline }: { form: FormInstance<FormValues>; adding: boolean; dirty: boolean; symbol: string | undefined; baseline: { symbol: string; config: GridConfig } | null }) {
  const watched = Form.useWatch([], form) as FormValues | undefined;
  const [showChanges, setShowChanges] = useState(false);
  useEffect(() => {
    if (!dirty) setShowChanges(false);
  }, [dirty]);
  useEffect(() => {
    setShowChanges(false);
  }, [symbol]);
  const pending = !adding && dirty && watched && baseline && baseline.symbol === symbol ? diffGridConfig(baseline.config, watched) : [];
  const expanded = showChanges && pending.length > 0;
  return <div className="save-bar-status">
    <span className="muted" role="status">{dirty ? (pending.length ? `已修改 ${pending.length} 项` : '有未保存的修改') : adding ? '确认参数后保存并添加交易对' : '所有修改已保存'}</span>
    {pending.length > 0 && <Button type="link" size="small" aria-expanded={expanded} aria-controls="pending-changes" onClick={() => setShowChanges(value => !value)}>{expanded ? '收起' : '查看'}</Button>}
    {expanded && <ul id="pending-changes" className="save-bar-changes">{pending.map(change => <li key={change.key} className={change.risky ? 'confirm-risky' : undefined}><span>{change.label}</span><span><s>{change.before}</s> → <strong>{change.after}</strong></span></li>)}</ul>}
  </div>;
}
