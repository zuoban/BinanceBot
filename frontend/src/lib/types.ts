type Decimal = string;
export type Mode = 'PAPER' | 'LIVE' | 'UNKNOWN';
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
  min_price: Decimal | null;
  max_price: Decimal | null;
  max_position_usdc: Decimal | null;
  pause_on_max_position: boolean;
  cancel_opening_on_max_position: boolean;
  min_liquidation_distance_pct: Decimal | null;
}
export interface Pair { symbol: string; scoped: boolean; status: Status; mode: Mode; grid_config: GridConfig; account_scope?: string }
export interface Order {
  client_order_id: string; order_id: number | null; symbol: string; side: 'BUY' | 'SELL';
  price: Decimal; quantity: Decimal; amount_usdc: Decimal; status: string;
  created_at: string; grid_level: number; is_take_profit: boolean; purpose: 'GRID' | 'TAKE_PROFIT' | 'REMAINDER' | 'LEGACY' | 'UNVERIFIED' | (string & {});
  merge_sources: { client_order_id: string; price: Decimal; quantity: Decimal }[];
}
export interface Trade {
  trade_id: string; symbol: string; mode: Mode; side: 'BUY' | 'SELL'; price: Decimal;
  quantity: Decimal; amount_usdc: Decimal; realized_pnl: Decimal; commission: Decimal;
  commission_asset?: string | null;
  pnl_verified: boolean; is_maker: boolean; timestamp: string; note: string;
}
export interface Log { timestamp: string; level: string; message: string }
interface Account {
  asset: string; total_wallet_balance: Decimal; available_balance: Decimal;
  margin_balance: Decimal; unrealized_profit: Decimal; update_time: string;
}
export interface Snapshot {
  generated_at?: string; symbol: string; status: Status; mode: Mode; sync_interval_secs?: number; account_scope?: string; grid_config: GridConfig;
  ticker: { last_price: Decimal; mark_price: Decimal; change_percent_24h: Decimal; high_24h: Decimal; low_24h: Decimal; update_time: string; mark_update_time: string };
  stats: {
    total_trades: number; completed_cycles: number; total_realized_pnl: Decimal;
    total_commission: Decimal; non_quote_commission_trades?: number; pending_pnl_trades: number; total_volume_usdc: Decimal;
    uptime_secs: number; active_buy_orders: number; active_sell_orders: number;
  };
  position: { size: Decimal; entry_price: Decimal; mark_price: Decimal; unrealized_pnl: Decimal; liquidation_price: Decimal; leverage: number };
  account: Account;
  account_cny_rate: { asset: string; cny_per_unit: Decimal; updated_at: string } | null;
  active_orders: Order[]; recent_trades: Trade[]; recent_logs: Log[];
  hourly_trade_stats: { window_start: string; window_end: string; realized_pnl: Decimal; pending_pnl_count: number; buckets: { hour_start: string; buy_count: number; sell_count: number }[] } | null;
}
export type BotSnapshot = Snapshot;
interface AccountSnapshot {
  mode: Mode;
  snapshot: Pick<Snapshot, 'symbol' | 'status' | 'sync_interval_secs' | 'account_scope' | 'account' | 'account_cny_rate'> & {
    position: Pick<Snapshot['position'], 'size' | 'unrealized_pnl'>;
    ticker: Pick<Snapshot['ticker'], 'mark_price' | 'mark_update_time'>;
  };
}
export interface AccountFrame { generated_at: string; pairs: AccountSnapshot[] }
export interface AccountObservation extends AccountSnapshot { account_observation_stale?: boolean; generated_at_ms?: number; received_at_ms?: number }
export type SocketMessage =
  | { type: 'snapshot'; data: Snapshot }
  | { type: 'accounts'; data: AccountFrame };
interface TodayPnl { window_start: string; window_end: string; groups: { symbol: string; mode: Mode; realized_pnl: Decimal; pending_pnl_count: number }[] }
export interface PairSummary {
  symbol: string; status: Status; mode: Mode; sync_interval_secs?: number; account_scope?: string; grid_config: GridConfig;
  ticker: Pick<Snapshot['ticker'], 'mark_price' | 'mark_update_time'>;
  position: Pick<Snapshot['position'], 'size' | 'entry_price' | 'mark_price' | 'unrealized_pnl'>;
  stats: Pick<Snapshot['stats'], 'total_realized_pnl' | 'active_buy_orders' | 'active_sell_orders'>;
  today_trade_counts: { window_start: string; window_end: string; buy: number; sell: number } | null;
  account: Account;
  account_cny_rate: Snapshot['account_cny_rate'];
}
export interface Overview { generated_at: string; pairs: { scoped: boolean; mode: Mode; snapshot: PairSummary }[]; recent_trades: Trade[]; today_pnl?: TodayPnl | null }
export interface HistoryAccount { mode: Mode; asset: string; account_id: string | null }
interface HourlySnapshot {
  hour: number;
  groups: (HistoryAccount & { equity: Decimal | null })[];
}
interface DailyAccountGroup {
  mode: Mode;
  asset: string;
  account_id: string | null;
  count: number;
  equity: Decimal | null;
  available: Decimal | null;
  wallet: Decimal | null;
  unrealized: Decimal | null;
}
interface DailyPositionGroup {
  mode: Mode;
  asset: string;
  count: number;
  long: Decimal | null;
  short: Decimal | null;
  total: Decimal | null;
}
interface DailyPositionItem {
  symbol: string;
  mode: Mode;
  direction: GridConfig['direction'];
  status: Status;
  size: Decimal;
  entry_price: Decimal;
  mark_price: Decimal | null;
  position_amount: Decimal | null;
  unrealized_pnl: Decimal | null;
}
interface DailyTradeGroup {
  symbol: string;
  mode: Mode;
  buy_count: number;
  sell_count: number;
  realized_pnl: Decimal;
  pending_pnl_count: number;
}
export interface DailySnapshot {
  date: string;
  window_start: string;
  window_end: string;
  generated_at: string;
  accounts: DailyAccountGroup[];
  positions: DailyPositionGroup[];
  pair_positions: DailyPositionItem[];
  trades: DailyTradeGroup[];
}
export interface OverviewHistoryData {
  snapshots: HourlySnapshot[];
  current_accounts: HistoryAccount[];
  daily_snapshots?: DailySnapshot[];
}
export interface GlobalConfig {
  version: string;
  recv_window: number; sync_interval_secs: number; global_max_position_notional: Decimal | null; pause_on_zero_available_margin: boolean; min_available_margin_pct: Decimal | null; has_api_key: boolean; has_api_secret: boolean;
  api_key_preview: string; telegram_enabled: boolean; has_telegram_bot_token: boolean; telegram_chat_id: string;
}
export interface ServerConfig { version: string; saved: { host: string; port: number }; active: { host: string; port: number } }
