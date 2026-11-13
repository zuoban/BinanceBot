import type { Log, Order, Trade } from './types';

function sameMergeSources(leftSources: Order['merge_sources'], rightSources: Order['merge_sources']) {
  if (leftSources === rightSources) return true;
  const left = leftSources ?? [], right = rightSources ?? [];
  if (left.length !== right.length) return false;
  for (let idx = 0; idx < left.length; idx++) {
    if (
      left[idx].client_order_id !== right[idx].client_order_id ||
      left[idx].price !== right[idx].price ||
      left[idx].quantity !== right[idx].quantity
    ) return false;
  }
  return true;
}

export function sameOrders(leftOrders: Order[], rightOrders: Order[]) {
  if (leftOrders === rightOrders) return true;
  if (leftOrders.length !== rightOrders.length) return false;
  for (let idx = 0; idx < leftOrders.length; idx++) {
    const left = leftOrders[idx], right = rightOrders[idx];
    if (
      left.client_order_id !== right.client_order_id ||
      left.side !== right.side ||
      left.price !== right.price ||
      left.quantity !== right.quantity ||
      left.amount_usdc !== right.amount_usdc ||
      left.purpose !== right.purpose ||
      left.is_take_profit !== right.is_take_profit ||
      left.status !== right.status ||
      left.created_at !== right.created_at ||
      !sameMergeSources(left.merge_sources, right.merge_sources)
    ) return false;
  }
  return true;
}

export function sameTrades(leftTrades: Trade[], rightTrades: Trade[]) {
  if (leftTrades === rightTrades) return true;
  if (leftTrades.length !== rightTrades.length) return false;
  for (let idx = 0; idx < leftTrades.length; idx++) {
    const left = leftTrades[idx], right = rightTrades[idx];
    if (
      left.trade_id !== right.trade_id ||
      left.symbol !== right.symbol ||
      left.mode !== right.mode ||
      left.side !== right.side ||
      left.price !== right.price ||
      left.quantity !== right.quantity ||
      left.amount_usdc !== right.amount_usdc ||
      left.realized_pnl !== right.realized_pnl ||
      left.commission !== right.commission ||
      (left.commission_asset ?? null) !== (right.commission_asset ?? null) ||
      left.pnl_verified !== right.pnl_verified ||
      left.is_maker !== right.is_maker ||
      left.timestamp !== right.timestamp ||
      left.note !== right.note
    ) return false;
  }
  return true;
}

export function sameLogs(leftLogs: Log[], rightLogs: Log[]) {
  if (leftLogs === rightLogs) return true;
  if (leftLogs.length !== rightLogs.length) return false;
  for (let idx = 0; idx < leftLogs.length; idx++) {
    const left = leftLogs[idx], right = rightLogs[idx];
    if (left.timestamp !== right.timestamp || left.level !== right.level || left.message !== right.message) return false;
  }
  return true;
}
