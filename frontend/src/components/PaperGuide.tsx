import { useState } from 'react';
import { Button } from 'antd';
import { CloseOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';

const key = 'binance_bot_paper_guide_dismissed';
const read = () => { try { return localStorage.getItem(key) === '1'; } catch { return false; } };

/** Shown while every strategy runs on paper: restates the validate-first path without blocking the page. */
export function PaperGuide({ firstSymbol }: { firstSymbol: string | undefined }) {
  const [hidden, setHidden] = useState(read);
  if (hidden) return null;
  const dismiss = () => { try { localStorage.setItem(key, '1'); } catch { /* The guide simply reappears next visit. */ } setHidden(true); };
  return <aside className="paper-guide" aria-label="新手指引">
    <div className="paper-guide-head">
      <h2>先在模拟盘验证，再决定是否使用实盘</h2>
      <Button type="text" size="small" icon={<CloseOutlined />} aria-label="关闭新手指引" onClick={dismiss} />
    </div>
    <ol>
      <li><strong>检查参数</strong>：{firstSymbol ? <Link to={`/pairs/${firstSymbol}/config`}>打开 {firstSymbol} 的策略参数</Link> : '打开策略参数'}，用右侧的网格预览确认价位和占用资金。</li>
      <li><strong>观察运行</strong>：模拟盘使用真实行情在本地撮合，不会产生真实订单；在详情页查看挂单、成交和日志。</li>
      <li><strong>再接入实盘</strong>：在<Link to="/settings">全局设置</Link>填写 API Key 并先“测试连接”；实盘默认暂停，需要逐个确认后手动恢复。</li>
    </ol>
    <p className="muted">盈亏统计有明确范围，不保证盈利；资金费和手工交易不计入程序已实现盈亏。</p>
  </aside>;
}
