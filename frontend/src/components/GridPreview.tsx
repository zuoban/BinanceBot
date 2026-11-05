import { useState } from 'react';
import { Form, Input } from 'antd';
import type { FormInstance } from 'antd';
import { exceeds, exposure, gridPreview, tickSizeError } from '../lib/gridPreview';
import { trimFractionZeros } from '../lib/format';

/** Live ladder of the orders the engine would place, from the form values and a reference price. */
export function GridPreview<Values>({ form, markPrice, tickSize, quote }: { form: FormInstance<Values>; markPrice?: string | null; tickSize?: string | null; quote: string }) {
  const [typed, setTyped] = useState('');
  const values = Form.useWatch([], form) as Record<string, unknown> | undefined;
  const hasMark = Number(markPrice) > 0;
  const reference = hasMark ? trimFractionZeros(String(markPrice)) : typed;
  const cleanTick = tickSize && Number(tickSize) > 0 ? trimFractionZeros(String(tickSize)) : null;
  const tickError = values && cleanTick ? tickSizeError(values.grid_interval, cleanTick) : null;
  const preview = values && !tickError ? gridPreview({ reference, interval: values.grid_interval, buyWindow: values.buy_window, sellWindow: values.sell_window, min: values.min_price, max: values.max_price }) : null;
  const orders = preview ? preview.buys.length + preview.sells.length : 0;
  const worstCase = preview && values ? exposure(values.order_amount_usdc, preview.buys.length) : null;
  const overLimit = exceeds(worstCase, values?.max_position_usdc);
  return <section className="grid-preview" aria-label="网格预览">
    <div className="grid-preview-head">
      <h3>网格预览</h3>
      <div className="grid-preview-meta">
        {cleanTick && <span className="muted">价格步长 {cleanTick}</span>}
        {hasMark ? <span className="muted">以当前标记价 {reference} 为参考</span>
          : <Input size="small" className="grid-preview-reference" aria-label="预览参考价格" placeholder="输入参考价格" inputMode="decimal" value={typed} onChange={event => setTyped(event.target.value.trim())} />}
      </div>
    </div>
    {tickError ? <p className="grid-preview-warning" role="alert">{tickError}</p>
      : !preview ? <p className="muted grid-preview-empty">填写网格间距、买卖窗口{hasMark ? '' : '和参考价格'}后显示将挂出的价位。</p> : <>
      <ol className="grid-preview-ladder" aria-label="预计挂单价位，由高到低">
        {[...preview.sells].reverse().map(price => <li key={`s${price}`} className="grid-preview-sell"><span>卖</span><strong>{price}</strong></li>)}
        <li className="grid-preview-mark"><span>参考价</span><strong>{reference}</strong></li>
        {preview.buys.map(price => <li key={`b${price}`} className="grid-preview-buy"><span>买</span><strong>{price}</strong></li>)}
      </ol>
      <p className="muted">共 {orders} 个挂单。{(preview.buyClipped || preview.sellClipped) && '价格区间限制了部分价位。'}{worstCase && ` 买单全部成交时新增名义价值约 ${worstCase} ${quote}。`}</p>
      {overLimit && <p className="grid-preview-warning" role="alert">买单全部成交的名义价值超过最大持仓名义价值 {String(values?.max_position_usdc)} {quote}，达到上限后将{values?.pause_on_max_position ? '暂停策略' : '限制开仓'}。</p>}
    </>}
  </section>;
}
