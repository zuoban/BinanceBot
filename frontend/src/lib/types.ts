export type Decimal = string;
export type Mode = 'PAPER' | 'TESTNET' | 'LIVE' | 'UNKNOWN';
export type Status = 'RUNNING' | 'PAUSED' | 'STOPPED' | 'ERROR';
export type Action = 'pause' | 'resume' | 'cancel_all' | 'rebalance';
export interface ApiResponse<T> { success: boolean; data: T; message?: string }
export interface AuthStatus { initialized: boolean; authenticated: boolean }
export interface GridConfig {
  symbol: string;
  direction: 'LONG' | 'SHORT';
  grid_interval: Decimal;
  order_amount_usdc: Decimal;
  buy_window: number;
  sell_window: number;
  post_only: boolean;
  dry_run: boolean;
  is_testnet: boolean;
  min_price: Decimal | null;
  max_price: Decimal | null;
  max_position_usdc: Decimal | null;
  pause_on_max_position: boolean;
}
export interface Pair { symbol: string; scoped: boolean; status: Status; mode: Mode; grid_config: GridConfig }
export interface Order {
  client_order_id: string; order_id: number | null; symbol: string; side: 'BUY' | 'SELL';
  price: Decimal; quantity: Decimal; amount_usdc: Decimal; status: string;
  created_at: string; grid_level: number; is_take_profit: boolean; purpose: string;
  merge_sources: { client_order_id: string; price: Decimal; quantity: Decimal }[];
}
export interface Trade {
  trade_id: string; symbol: string; mode: Mode; side: 'BUY' | 'SELL'; price: Decimal;
  quantity: Decimal; amount_usdc: Decimal; realized_pnl: Decimal; commission: Decimal;
  pnl_verified: boolean; is_maker: boolean; timestamp: string; note: string;
}
export interface Log { timestamp: string; level: string; message: string }
export interface Account {
  asset: string; total_wallet_balance: Decimal; available_balance: Decimal;
  margin_balance: Decimal; unrealized_profit: Decimal; update_time: string;
}
export interface Snapshot {
  symbol: string; status: Status; dry_run: boolean; grid_config: GridConfig;
  ticker: { last_price: Decimal; mark_price: Decimal; change_percent_24h: Decimal; high_24h: Decimal; low_24h: Decimal; update_time: string; mark_update_time: string };
  stats: {
    total_trades: number; completed_cycles: number; total_realized_pnl: Decimal;
    total_commission: Decimal; pending_pnl_trades: number; total_volume_usdc: Decimal;
    uptime_secs: number; active_buy_orders: number; active_sell_orders: number;
  };
  position: { size: Decimal; entry_price: Decimal; mark_price: Decimal; unrealized_pnl: Decimal; liquidation_price: Decimal; leverage: number };
  account: Account;
  account_cny_rate: { asset: string; cny_per_unit: Decimal; updated_at: string } | null;
  active_orders: Order[]; recent_trades: Trade[]; recent_logs: Log[];
  hourly_trade_stats: { window_start: string; window_end: string; buckets: { hour_start: string; buy_count: number; sell_count: number }[] } | null;
}
export interface Overview { generated_at: string; pairs: { scoped: boolean; mode: Mode; snapshot: Snapshot }[] }
export interface HourlySnapshot {
  hour: number;
  groups: { mode: Mode; asset: string; equity: Decimal | null; long: Decimal | null; short: Decimal | null }[];
}
export interface GlobalConfig {
  recv_window: number; sync_interval_secs: number; global_max_position_notional: Decimal | null; pause_on_zero_available_margin: boolean; has_api_key: boolean; has_api_secret: boolean;
  api_key_preview: string; telegram_enabled: boolean; has_telegram_bot_token: boolean; telegram_chat_id: string;
}
export interface ServerConfig { saved: { host: string; port: number }; active: { host: string; port: number } }
