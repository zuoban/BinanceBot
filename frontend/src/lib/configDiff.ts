import type { GridConfig } from './types';

type Editable = Omit<GridConfig, 'post_only'>;
export interface FieldChange { key: keyof Editable; label: string; before: string; after: string; risky: boolean }

const fields: { key: keyof Editable; label: string; risky: boolean; show: (config: Editable) => string }[] = [
  { key: 'direction', label: '交易方向', risky: true, show: c => c.direction === 'SHORT' ? '做空' : '做多' },
  { key: 'grid_interval', label: '网格间距', risky: false, show: c => String(c.grid_interval) },
  { key: 'order_amount_usdc', label: '每格金额', risky: true, show: c => String(c.order_amount_usdc) },
  { key: 'buy_window', label: '买单窗口', risky: false, show: c => String(c.buy_window) },
  { key: 'sell_window', label: '卖单窗口', risky: false, show: c => String(c.sell_window) },
  { key: 'min_price', label: '最低价格', risky: true, show: c => c.min_price ? String(c.min_price) : '未设置' },
  { key: 'max_price', label: '最高价格', risky: true, show: c => c.max_price ? String(c.max_price) : '未设置' },
  { key: 'max_position_usdc', label: '最大持仓名义价值', risky: true, show: c => c.max_position_usdc ? String(c.max_position_usdc) : '未设置' },
  { key: 'pause_on_max_position', label: '持仓上限处理方式', risky: true, show: c => c.pause_on_max_position ? '暂停策略' : '限制开仓' },
  { key: 'min_liquidation_distance_pct', label: '强平距离阈值', risky: true, show: c => c.min_liquidation_distance_pct ? `${c.min_liquidation_distance_pct}%` : '未设置' },
  { key: 'cancel_opening_on_max_position', label: '达到上限时仅撤开仓单', risky: true, show: c => c.cancel_opening_on_max_position ? '开启' : '关闭' },
];

const blank = (value: unknown) => value === null || value === undefined || value === '';
const same = (a: unknown, b: unknown) => {
  if (blank(a) || blank(b)) return blank(a) && blank(b);
  if (typeof a === 'string' && typeof b === 'string' && a.trim() !== '' && b.trim() !== '' && !Number.isNaN(Number(a)) && !Number.isNaN(Number(b))) return Number(a) === Number(b);
  return a === b;
};

/** Compare the saved configuration with the submitted one; numeric strings compare by value. */
export function diffGridConfig(before: Editable, after: Editable): FieldChange[] {
  return fields.flatMap(({ key, label, risky, show }) =>
    same(before[key], after[key]) ? [] : [{ key, label, risky, before: show(before), after: show(after) }]);
}

/** Every field of a new strategy, shown as it will be created. */
export function describeNewGridConfig(config: Editable): { label: string; value: string; risky: boolean }[] {
  return fields.map(({ label, risky, show }) => ({ label, risky, value: show(config) }));
}
