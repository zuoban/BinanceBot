import { Bar, BarChart, CartesianGrid, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import type { Snapshot } from '../lib/types';
import { price, todayCount } from '../lib/format';
import { NoData } from './Common';

export function HourlyChart({ snapshot }: { snapshot: Snapshot }) {
  if (todayCount(snapshot) === null) return <NoData />;
  const data = [...snapshot.hourly_trade_stats!.buckets].reverse().map(b => ({
    hour: new Date(b.hour_start).toLocaleTimeString('zh-CN', { timeZone: 'Asia/Shanghai', hour: '2-digit', hour12: false }),
    买入: b.buy_count, 卖出: b.sell_count,
  }));
  return <div className="chart" aria-label="北京时间今日每小时成交笔数"><ResponsiveContainer width="100%" height={270}>
    <BarChart data={data} barGap={3} margin={{ top: 15, right: 5, bottom: 5, left: -20 }}>
      <CartesianGrid strokeDasharray="3 5" vertical={false} stroke="#e7ece9" /><XAxis dataKey="hour" tickLine={false} axisLine={false} tick={{ fontSize: 11 }} /><YAxis allowDecimals={false} axisLine={false} tickLine={false} />
      <Tooltip cursor={{ fill: '#f3f6f4' }} /><Bar dataKey="买入" fill="#17745e" radius={[3, 3, 0, 0]} /><Bar dataKey="卖出" fill="#d2825c" radius={[3, 3, 0, 0]} />
    </BarChart>
  </ResponsiveContainer></div>;
}
export function DepthChart({ snapshot }: { snapshot: Snapshot }) {
  const data = [...snapshot.active_orders].sort((a, b) => Number(a.price) - Number(b.price)).map(o => ({
    price: Number(o.price), 买单: o.side === 'BUY' ? Number(o.quantity) : 0, 卖单: o.side === 'SELL' ? Number(o.quantity) : 0,
  }));
  if (!data.length) return <NoData />;
  return <div className="chart" aria-label="实时网格挂单分布"><ResponsiveContainer width="100%" height={270}>
    <BarChart data={data} margin={{ top: 15, right: 15, bottom: 5, left: -20 }}>
      <CartesianGrid strokeDasharray="3 5" vertical={false} stroke="#e7ece9" />
      <XAxis dataKey="price" type="number" domain={['auto', 'auto']} tickFormatter={v => price(v)} tick={{ fontSize: 10 }} />
      <YAxis axisLine={false} tickLine={false} /><Tooltip /><Bar dataKey="买单" stackId="orders" fill="#17745e" /><Bar dataKey="卖单" stackId="orders" fill="#d2825c" />
      {Number(snapshot.ticker.mark_price) > 0 && <ReferenceLine x={Number(snapshot.ticker.mark_price)} stroke="#a98432" strokeDasharray="4 4" label={{ value: '标记价', position: 'insideTop' }} />}
    </BarChart>
  </ResponsiveContainer></div>;
}
