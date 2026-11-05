import { useState } from 'react';
import { Form, Input } from 'antd';
import type { FormInstance } from 'antd';
import { exceeds, exposure, gridPreview, orderSizing, priceDiffPercent, sumDecimals, tickSizeError } from '../lib/gridPreview';
import { trimFractionZeros } from '../lib/format';

/** Live ladder of the orders the engine would place, from the form values and a reference price. */
export function GridPreview<Values>({ form, markPrice, tickSize, stepSize, minQty, minNotional, quote }: { form: FormInstance<Values>; markPrice?: string | null; tickSize?: string | null; stepSize?: string | null; minQty?: string | null; minNotional?: string | null; quote: string }) {
  const [typed, setTyped] = useState('');
  const values = Form.useWatch([], form) as Record<string, unknown> | undefined;
  const hasMark = Number(markPrice) > 0;
  const reference = hasMark ? trimFractionZeros(String(markPrice)) : typed;
  const cleanTick = tickSize && Number(tickSize) > 0 ? trimFractionZeros(String(tickSize)) : null;
  const cleanStep = stepSize && Number(stepSize) > 0 ? trimFractionZeros(String(stepSize)) : null;
  const cleanMinNotional = minNotional && Number(minNotional) > 0 ? trimFractionZeros(String(minNotional)) : null;
  const tickError = values && cleanTick ? tickSizeError(values.grid_interval, cleanTick) : null;
  const preview = values && !tickError ? gridPreview({ reference, interval: values.grid_interval, buyWindow: values.buy_window, sellWindow: values.sell_window, min: values.min_price, max: values.max_price }) : null;
  const sizeAt = (price: string) => cleanStep ? orderSizing(price, values?.order_amount_usdc, { stepSize: cleanStep, minQty, minNotional }) : null;
  const unplaceable = preview && cleanStep ? [...preview.sells, ...preview.buys].flatMap(price => { const s = sizeAt(price); return s?.error ? [s.error] : []; }) : [];
  const short = values?.direction === 'SHORT';
  const openingLabel = short ? '卖单' : '买单';
  const openingPrices = preview ? (short ? preview.sells : preview.buys) : [];
  const orders = preview ? preview.buys.length + preview.sells.length : 0;
  const worstCase = preview && values ? (cleanStep
    ? sumDecimals(openingPrices.flatMap(price => { const s = sizeAt(price); return s && !s.error ? [s.notional] : []; }))
    : exposure(values.order_amount_usdc, openingPrices.length)) : null;
  const overLimit = exceeds(worstCase, values?.max_position_usdc);
  return <section className="grid-preview" aria-label="网格预览">
    <div className="grid-preview-head">
      <h3>网格预览</h3>
      <div className="grid-preview-meta">
        {cleanTick && <span className="muted">价格步长 {cleanTick}</span>}
        {cleanStep && <span className="muted">数量步长 {cleanStep}</span>}
        {cleanMinNotional && <span className="muted">最小名义金额 {cleanMinNotional} {quote}</span>}
        {hasMark ? <span className="muted">以当前标记价 {reference} 为参考</span>
          : <Input size="small" className="grid-preview-reference" aria-label="预览参考价格" placeholder="输入参考价格" inputMode="decimal" value={typed} onChange={event => setTyped(event.target.value.trim())} />}
      </div>
    </div>
    {tickError ? <p className="grid-preview-warning" role="alert">{tickError}</p>
      : !preview ? <p className="muted grid-preview-empty">填写网格间距、买卖窗口{hasMark ? '' : '和参考价格'}后显示将挂出的价位。</p> : <>
      <ol className="grid-preview-ladder" aria-label="预计挂单价位，由高到低">
        {[...preview.sells].reverse().map(price => { const s = sizeAt(price), diff = priceDiffPercent(price, reference); return <li key={`s${price}`} className="grid-preview-sell"><span>卖{s && <small className="grid-preview-qty"> · {s.quantity} ({s.notional} {quote})</small>}</span><span className="grid-preview-price">{diff && <small className="grid-preview-diff">{diff}</small>}<strong>{price}</strong></span></li>; })}
        <li className="grid-preview-mark"><span>参考价</span><strong>{reference}</strong></li>
        {preview.buys.map(price => { const s = sizeAt(price), diff = priceDiffPercent(price, reference); return <li key={`b${price}`} className="grid-preview-buy"><span>买{s && <small className="grid-preview-qty"> · {s.quantity} ({s.notional} {quote})</small>}</span><span className="grid-preview-price">{diff && <small className="grid-preview-diff">{diff}</small>}<strong>{price}</strong></span></li>; })}
      </ol>
      <p className="muted">共 {orders} 个挂单。{(preview.buyClipped || preview.sellClipped) && '价格区间限制了部分价位。'}{worstCase && ` ${openingLabel}全部成交时新增名义价值约 ${worstCase} ${quote}。`}</p>
      {overLimit && <p className="grid-preview-warning" role="alert">{openingLabel}全部成交的名义价值超过最大持仓名义价值 {String(values?.max_position_usdc)} {quote}，达到上限后将{values?.pause_on_max_position ? '暂停策略' : '限制开仓'}。</p>}
      {unplaceable.length > 0 && <p className="grid-preview-warning" role="alert">{unplaceable.length === orders ? `全部 ${orders} 档均无法挂单：` : `${unplaceable.length}/${orders} 档无法挂单：`}{unplaceable[0]}。</p>}
    </>}
  </section>;
}
