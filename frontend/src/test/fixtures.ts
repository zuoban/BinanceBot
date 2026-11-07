import type { Pair, PairSummary, Snapshot } from '../lib/types';

export function snapshot(symbol = 'SOLUSDC', mode: 'PAPER' | 'LIVE' = 'PAPER'): Snapshot {
  const now = new Date().toISOString();
  return {
    symbol, status: 'PAUSED', mode,
    grid_config: { symbol, direction: 'LONG', grid_interval: '0.1', order_amount_usdc: '100', buy_window: 5, sell_window: 5, post_only: true, min_price: null, max_price: null, max_position_usdc: null, pause_on_max_position: false, cancel_opening_on_max_position: false, min_liquidation_distance_pct: null },
    ticker: { last_price: '100', mark_price: '101', change_percent_24h: '0', high_24h: '102', low_24h: '98', update_time: now, mark_update_time: now },
    stats: { total_trades: 0, completed_cycles: 0, total_realized_pnl: '0', total_commission: '0', pending_pnl_trades: 0, total_volume_usdc: '0', uptime_secs: 0, active_buy_orders: 0, active_sell_orders: 0 },
    position: { size: '0', entry_price: '0', mark_price: '101', unrealized_pnl: '5', liquidation_price: '0', leverage: 1 },
    account: { asset: 'USDC', total_wallet_balance: '100', available_balance: '80', margin_balance: '105', unrealized_profit: '5', update_time: now },
    account_cny_rate: null, active_orders: [], recent_trades: [], recent_logs: [],
    hourly_trade_stats: { window_start: now, window_end: now, realized_pnl: '0', pending_pnl_count: 0, buckets: [{ hour_start: now, buy_count: 2, sell_count: 3 }] },
  };
}
export function pair(symbol = 'SOLUSDC'): Pair {
  const s = snapshot(symbol);
  return { symbol, scoped: true, mode: 'PAPER', status: 'PAUSED', grid_config: s.grid_config };
}

export function summarySnapshot(symbol = 'SOLUSDC', mode: 'PAPER' | 'LIVE' = 'PAPER'): PairSummary {
  const s = snapshot(symbol, mode);
  return {
    symbol, mode, status: s.status, grid_config: s.grid_config,
    ticker: { mark_price: s.ticker.mark_price, mark_update_time: s.ticker.mark_update_time },
    position: { size: s.position.size, entry_price: s.position.entry_price, mark_price: s.position.mark_price, unrealized_pnl: s.position.unrealized_pnl },
    stats: { total_realized_pnl: s.stats.total_realized_pnl, active_buy_orders: s.stats.active_buy_orders, active_sell_orders: s.stats.active_sell_orders },
    today_trade_counts: { window_start: s.hourly_trade_stats!.window_start, window_end: s.hourly_trade_stats!.window_end, buy: 2, sell: 3 },
    account: s.account, account_cny_rate: s.account_cny_rate,
  };
}
