import { useState } from 'react';
import { Form, Input } from 'antd';
import type { FormInstance } from 'antd';
import { capCoverage, exceeds, exposure, gridPreview, orderSizing, priceDiffPercent, priceRangeError, spacingBelowRoundTripFee, spacingPercent, sumDecimals, tickSizeError } from '../lib/gridPreview';
import { maxPositionPolicyLabel, trimFractionZeros } from '../lib/format';

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
  const rangeError = values ? priceRangeError('min', values.min_price, values.max_price) ?? priceRangeError('max', values.min_price, values.max_price) : null;
  const preview = values && !tickError && !rangeError ? gridPreview({ reference, interval: values.grid_interval, buyWindow: values.buy_window, sellWindow: values.sell_window, min: values.min_price, max: values.max_price }) : null;
  const sizings = new Map(
    preview && cleanStep ? [...preview.sells, ...preview.buys].map(price => [price, orderSizing(price, values?.order_amount_usdc, { stepSize: cleanStep, minQty, minNotional })] as const) : [],
  );
  const unplaceable = preview && cleanStep ? [...preview.sells, ...preview.buys].flatMap(price => { const s = sizings.get(price); return s?.error ? [s.error] : []; }) : [];
  const short = values?.direction === 'SHORT';
  const openingLabel = short ? '卖单' : '买单';
  const openingPrices = preview ? (short ? preview.sells : preview.buys) : [];
  const orders = preview ? preview.buys.length + preview.sells.length : 0;
  const worstCase = preview && values ? (cleanStep
    ? sumDecimals(openingPrices.flatMap(price => { const s = sizings.get(price); return s && !s.error ? [s.notional] : []; }))
    : exposure(values.order_amount_usdc, openingPrices.length)) : null;
  const overLimit = exceeds(worstCase, values?.max_position_usdc);
  const cap = preview && values ? capCoverage(values.max_position_usdc, values.order_amount_usdc, values.grid_interval, reference) : null;
  const thinSpacing = !!preview && !!values && spacingBelowRoundTripFee(values.grid_interval, reference);
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
    {tickError || rangeError ? <p className="grid-preview-warning" role="alert">{tickError ?? rangeError}</p>
      : !preview ? <p className="muted grid-preview-empty">填写网格间距、买卖窗口{hasMark ? '' : '和参考价格'}后显示将挂出的价位。</p> : <>
      <ol className="grid-preview-ladder" aria-label="预计挂单价位，由高到低">
        {[...preview.sells].reverse().map(price => { const s = sizings.get(price), diff = priceDiffPercent(price, reference); return <li key={`s${price}`} className="grid-preview-sell"><span>卖{s && <small className="grid-preview-qty"> · {s.quantity} ({s.notional} {quote})</small>}</span><span className="grid-preview-price">{diff && <small className="grid-preview-diff">{diff}</small>}<strong>{price}</strong></span></li>; })}
        <li className="grid-preview-mark"><span>参考价</span><strong>{reference}</strong></li>
        {preview.buys.map(price => { const s = sizings.get(price), diff = priceDiffPercent(price, reference); return <li key={`b${price}`} className="grid-preview-buy"><span>买{s && <small className="grid-preview-qty"> · {s.quantity} ({s.notional} {quote})</small>}</span><span className="grid-preview-price">{diff && <small className="grid-preview-diff">{diff}</small>}<strong>{price}</strong></span></li>; })}
      </ol>
      <p className="muted">共 {orders} 个挂单。{(preview.buyClipped || preview.sellClipped) && '价格区间限制了部分价位。'}{worstCase && worstCase !== '0' && ` ${openingLabel}全部成交时新增名义价值约 ${worstCase} ${quote}。`}</p>
      {overLimit && <p className="grid-preview-warning" role="alert">{openingLabel}全部成交的名义价值超过最大持仓名义价值 {String(values?.max_position_usdc)} {quote}，达到上限后将{maxPositionPolicyLabel({ pause_on_max_position: Boolean(values?.pause_on_max_position), cancel_opening_on_max_position: Boolean(values?.cancel_opening_on_max_position) })}。</p>}
      {cap?.tight && <p className="grid-preview-warning" role="alert">按目标金额估算，持仓上限约可容纳 {cap.levels} 格，对应约 {cap.percent}% 的单边价格距离。{values?.pause_on_max_position ? '达到上限或下一笔完整开仓单预计超限时，策略会暂停并尝试撤销挂单。' : values?.cancel_opening_on_max_position ? '达到上限时尝试撤销开仓挂单，平仓单继续按当前网格窗口维护。' : '超过敞口上限的新增开仓单会被跳过，已有挂单继续留在交易所。'}平仓可能产生已实现亏损；请结合可用保证金和可接受敞口检查每格金额与持仓上限。</p>}
      {thinSpacing && <p className="grid-preview-warning" role="alert">网格间距仅为参考价的 {spacingPercent(values?.grid_interval, reference)}%，低于 Maker 往返手续费估算（2 × 0.02% = 0.04%）。按此费率每格成交后净收益为负；USDC 合约 Maker 费率可能为 0，请以账户实际费率为准。</p>}
      {unplaceable.length > 0 && <p className="grid-preview-warning" role="alert">{unplaceable.length === orders ? `全部 ${orders} 档均无法挂单：` : `${unplaceable.length}/${orders} 档无法挂单：`}{unplaceable[0]}。</p>}
    </>}
  </section>;
}
