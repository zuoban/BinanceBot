import { memo, useMemo, useRef, useState } from 'react';
import { Form, Input } from 'antd';
import type { FormInstance } from 'antd';
import { CloseCircleOutlined, WarningOutlined } from '@ant-design/icons';
import { capCoverage, exceeds, exposure, gridIntervalError, gridPreview, orderSizings, priceDiffPercent, priceRangeError, spacingBelowRoundTripFee, spacingPercent, sumDecimals } from '../lib/gridPreview';
import { maxPositionPolicyLabel, positiveDecimalText } from '../lib/format';
import type { GridConfig } from '../lib/types';
import { useCenterElementScroll } from './Common';

type LadderItem = { key: string; side: 'sell' | 'buy'; price: string; sizing: { quantity: string; notional: string; error: string | null } | null | undefined; diff: string | null };

const GridLadderLevel = memo(function GridLadderLevel({ item, quote }: { item: LadderItem; quote: string }) {
  const { side, price, sizing, diff } = item;
  return <li className={`grid-preview-${side}${sizing?.error ? ' grid-preview-unplaceable' : ''}`} title={sizing?.error ?? undefined}><span className="grid-preview-order"><span className="grid-preview-side">{side === 'sell' ? '卖' : '买'}</span>{sizing && <small className="grid-preview-qty"> · {sizing.quantity} ({sizing.notional} {quote})</small>}</span><span className="grid-preview-price">{diff && <small className="grid-preview-diff">{diff}</small>}<strong className="grid-preview-value">{price}</strong></span>{sizing?.error && <span className="sr-only">（无法挂单：{sizing.error}）</span>}</li>;
}, (prev, next) =>
  prev.quote === next.quote
  && prev.item.side === next.item.side
  && prev.item.price === next.item.price
  && prev.item.diff === next.item.diff
  && prev.item.sizing?.quantity === next.item.sizing?.quantity
  && prev.item.sizing?.notional === next.item.sizing?.notional
  && prev.item.sizing?.error === next.item.sizing?.error,
);

/** Live ladder of the orders the engine would place, from the form values and a reference price. */
export function GridPreview<Values extends Partial<Omit<GridConfig, 'post_only'>>>({ form, markPrice, tickSize, stepSize, minQty, minNotional, quote }: { form: FormInstance<Values>; markPrice?: string | null; tickSize?: string | null; stepSize?: string | null; minQty?: string | null; minNotional?: string | null; quote: string }) {
  const [typed, setTyped] = useState('');
  const ladderRef = useRef<HTMLOListElement>(null);
  const direction = Form.useWatch('direction', form) as GridConfig['direction'] | undefined;
  const gridInterval = Form.useWatch('grid_interval', form) as GridConfig['grid_interval'] | undefined;
  const orderAmount = Form.useWatch('order_amount_usdc', form) as GridConfig['order_amount_usdc'] | undefined;
  const buyWindow = Form.useWatch('buy_window', form) as GridConfig['buy_window'] | undefined;
  const sellWindow = Form.useWatch('sell_window', form) as GridConfig['sell_window'] | undefined;
  const minPrice = Form.useWatch('min_price', form) as GridConfig['min_price'] | undefined;
  const maxPrice = Form.useWatch('max_price', form) as GridConfig['max_price'] | undefined;
  const maxPosition = Form.useWatch('max_position_usdc', form) as GridConfig['max_position_usdc'] | undefined;
  const pauseOnMax = Form.useWatch('pause_on_max_position', form) as boolean | undefined;
  const cancelOpeningOnMax = Form.useWatch('cancel_opening_on_max_position', form) as boolean | undefined;
  const hasFormValues = direction !== undefined || gridInterval !== undefined || buyWindow !== undefined || sellWindow !== undefined;
  const cleanMark = positiveDecimalText(markPrice);
  const hasMark = cleanMark !== null;
  const reference = cleanMark ?? typed;
  const cleanTick = positiveDecimalText(tickSize);
  const cleanStep = positiveDecimalText(stepSize);
  const cleanMinNotional = positiveDecimalText(minNotional);
  const tickError = hasFormValues ? gridIntervalError(gridInterval, cleanTick) : null;
  const rangeError = hasFormValues ? priceRangeError('min', minPrice, maxPrice) ?? priceRangeError('max', minPrice, maxPrice) : null;
  const preview = useMemo(
    () => hasFormValues && !tickError && !rangeError ? gridPreview({ reference, interval: gridInterval, buyWindow, sellWindow, min: minPrice, max: maxPrice }) : null,
    [hasFormValues, gridInterval, buyWindow, sellWindow, minPrice, maxPrice, reference, tickError, rangeError],
  );
  const sizings = useMemo(
    () => preview && cleanStep ? orderSizings([...preview.sells, ...preview.buys], orderAmount, { stepSize: cleanStep, minQty, minNotional }) : new Map(),
    [preview, cleanStep, orderAmount, minQty, minNotional],
  );
  const unplaceable = useMemo(() => [...sizings.values()].flatMap(sizing => sizing?.error ? [sizing.error] : []), [sizings]);
  const short = direction === 'SHORT';
  const openingLabel = short ? '卖单' : '买单';
  const orders = preview ? preview.buys.length + preview.sells.length : 0;
  useCenterElementScroll(ladderRef, '.grid-preview-mark', `${preview?.sells.length ?? 0}:${preview?.buys.length ?? 0}`);
  const worstCase = useMemo(() => {
    if (!preview) return null;
    const openingPrices = short ? preview.sells : preview.buys;
    return cleanStep
      ? sumDecimals(openingPrices.flatMap(levelPrice => { const sizing = sizings.get(levelPrice); return sizing && !sizing.error ? [sizing.notional] : []; }))
      : exposure(orderAmount, openingPrices.length);
  }, [preview, short, orderAmount, cleanStep, sizings]);
  const overLimit = exceeds(worstCase, maxPosition);
  const cap = useMemo(
    () => preview ? capCoverage(maxPosition, orderAmount, gridInterval, reference) : null,
    [preview, maxPosition, orderAmount, gridInterval, reference],
  );
  const thinSpacing = !!preview && spacingBelowRoundTripFee(gridInterval, reference);
  const ladder = useMemo(() => {
    if (!preview) return null;
    const build = (side: 'sell' | 'buy', levelPrice: string) => ({ key: `${side[0]}${levelPrice}`, side, price: levelPrice, sizing: sizings.get(levelPrice), diff: priceDiffPercent(levelPrice, reference) });
    return { sells: [...preview.sells].reverse().map(levelPrice => build('sell', levelPrice)), buys: preview.buys.map(levelPrice => build('buy', levelPrice)) };
  }, [preview, reference, sizings]);
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
    {tickError || rangeError ? <p className="grid-preview-notice grid-preview-error" role="alert"><CloseCircleOutlined aria-hidden="true" /><span><strong>参数错误：</strong>{tickError ?? rangeError}</span></p>
      : !preview ? <p className="muted grid-preview-empty">填写网格间距、买卖窗口{hasMark ? '' : '和参考价格'}后显示将挂出的价位。</p> : <>
      <ol ref={ladderRef} className="grid-preview-ladder" aria-label="预计挂单价位，由高到低">
        {ladder!.sells.map(item => <GridLadderLevel key={item.key} item={item} quote={quote} />)}
        <li className="grid-preview-mark"><span className="grid-preview-order">参考价</span><strong className="grid-preview-price grid-preview-value">{reference}</strong></li>
        {ladder!.buys.map(item => <GridLadderLevel key={item.key} item={item} quote={quote} />)}
      </ol>
      <p className="muted">共 {orders} 个挂单。{(preview.buyClipped || preview.sellClipped) && '价格区间限制了部分价位。'}{worstCase && worstCase !== '0' && ` ${openingLabel}全部成交时新增名义价值约 ${worstCase} ${quote}。`}</p>
      {overLimit && <p className="grid-preview-notice grid-preview-warning" role="alert"><WarningOutlined aria-hidden="true" /><span><strong>持仓超限风险：</strong>{openingLabel}全部成交的名义价值超过最大持仓名义价值 {String(maxPosition)} {quote}，达到上限后将{maxPositionPolicyLabel({ pause_on_max_position: Boolean(pauseOnMax), cancel_opening_on_max_position: Boolean(cancelOpeningOnMax) })}。</span></p>}
      {cap?.tight && <p className="grid-preview-notice grid-preview-warning" role="alert"><WarningOutlined aria-hidden="true" /><span><strong>持仓容量风险：</strong>按目标金额估算，持仓上限约可容纳 {cap.levels} 格，对应约 {cap.percent}% 的单边价格距离。{pauseOnMax ? '达到上限或下一笔完整开仓单预计超限时，策略会暂停并尝试撤销挂单。' : cancelOpeningOnMax ? '达到上限时尝试撤销开仓挂单，平仓单继续按当前网格窗口维护。' : '超过敞口上限的新增开仓单会被跳过，已有挂单继续留在交易所。'}平仓可能产生已实现亏损；请结合可用保证金和可接受敞口检查每格金额与持仓上限。</span></p>}
      {thinSpacing && <p className="grid-preview-notice grid-preview-warning" role="alert"><WarningOutlined aria-hidden="true" /><span><strong>手续费风险：</strong>网格间距仅为参考价的 {spacingPercent(gridInterval, reference)}%，低于 Maker 往返手续费估算（2 × 0.02% = 0.04%）。按此费率每格成交后净收益为负；USDC 合约 Maker 费率可能为 0，请以账户实际费率为准。</span></p>}
      {unplaceable.length > 0 && <p className="grid-preview-notice grid-preview-error" role="alert"><CloseCircleOutlined aria-hidden="true" /><span><strong>挂单错误：</strong>{unplaceable.length === orders ? `全部 ${orders} 档均无法挂单：` : `${unplaceable.length}/${orders} 档无法挂单：`}{unplaceable[0]}。</span></p>}
    </>}
  </section>;
}
