import { useRef } from 'react';
import { Alert, Button, Form, Input, Typography } from 'antd';
import { LockOutlined } from '@ant-design/icons';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, post, tokenStore } from '../lib/api';
import type { AuthStatus } from '../lib/types';
import { ErrorNotice } from '../components/Common';
import { LoadingScreen } from '../components/Loading';
import { ThemeSwitch } from '../components/ThemeSwitch';

const sellLevels = [
  { y: 40, bar: 148, step: '+2.0%' },
  { y: 62, bar: 116, step: '+1.5%' },
  { y: 84, bar: 88, step: '+1.0%' },
  { y: 106, bar: 62, step: '+0.5%' },
] as const;

const buyLevels = [
  { y: 154, bar: 68, step: '-0.5%' },
  { y: 176, bar: 96, step: '-1.0%' },
  { y: 198, bar: 126, step: '-1.5%' },
  { y: 220, bar: 158, step: '-2.0%' },
] as const;

export default function Login({ onLogin }: { onLogin: () => void }) {
  const client = useQueryClient();
  const status = useQuery({ queryKey: ['auth-status'], queryFn: ({ signal }) => api<AuthStatus>('/api/auth/status', { signal }), retry: false });
  const configured = status.data?.initialized !== false;
  const submitting = useRef(false);
  const mutation = useMutation({
    mutationFn: (values: { password: string }) => post<{ token: string }>('/api/auth/login', values, { readOnly: true }),
    onSuccess: async data => {
      tokenStore.set(data.token);
      await client.cancelQueries({ queryKey: ['auth-status'] });
      client.setQueryData(['auth-status'], { initialized: true, authenticated: true });
      onLogin();
    },
  });
  if (status.isPending) return <LoadingScreen label="正在连接控制台" />;
  return <main className="login-page">
    <div className="login-theme"><ThemeSwitch /></div>
    <aside className="login-brand">
      <div className="login-brand-content">
        <img src="/logo.svg" alt="" className="login-logo" />
        <div><strong>Grid Console</strong><span>Binance Futures</span></div>
      </div>
      <svg className="login-ladder" viewBox="0 0 360 256" aria-hidden="true" focusable="false">
        <text x="16" y="18" className="ladder-caption ladder-caption-sell">↓ 卖单挂单 · SELL</text>
        <text x="344" y="18" textAnchor="end" className="ladder-caption">相对基准</text>
        <line x1="19" x2="19" y1="40" y2="220" className="ladder-axis" />
        {sellLevels.map(({ y, bar, step }) => <g key={y}>
          <line x1="22" x2="292" y1={y} y2={y} className="ladder-sell" />
          <rect x="28" y={y - 3.5} width={bar} height="7" rx="2" className="ladder-sell-depth" />
          <rect x="16" y={y - 3} width="6" height="6" rx="1" className="ladder-sell-mark" />
          <text x="344" y={y + 3.5} textAnchor="end" className="ladder-step ladder-step-sell">{step}</text>
        </g>)}
        <rect x="8" y="117" width="344" height="26" rx="6" className="ladder-mark-band" />
        <line x1="16" x2="262" y1="130" y2="130" className="ladder-mark" />
        <rect x="15.5" y="126.5" width="7" height="7" rx="1.5" transform="rotate(45 19 130)" className="ladder-mark-tag" />
        <rect x="262" y="119" width="82" height="22" rx="4" className="ladder-mark-tag" />
        <text x="303" y="133.5" textAnchor="middle" className="ladder-mark-text">MARK 基准</text>
        {buyLevels.map(({ y, bar, step }) => <g key={y}>
          <line x1="22" x2="292" y1={y} y2={y} className="ladder-buy" />
          <rect x="28" y={y - 3.5} width={bar} height="7" rx="2" className="ladder-buy-depth" />
          <rect x="16" y={y - 3} width="6" height="6" rx="1" className="ladder-buy-mark" />
          <text x="344" y={y + 3.5} textAnchor="end" className="ladder-step ladder-step-buy">{step}</text>
        </g>)}
        <text x="16" y="244" className="ladder-caption ladder-caption-buy">↑ 买单挂单 · BUY</text>
        <text x="344" y="244" textAnchor="end" className="ladder-caption">Post-Only</text>
      </svg>
    </aside>
    <section className="login-form"><div className="login-form-content">
      <div className="login-form-header">
        <Typography.Title level={1}>{configured ? '登录交易控制台' : '无需登录'}</Typography.Title>
        <Typography.Paragraph type="secondary">{configured ? '输入管理员密码，进入策略工作台' : '服务端未设置管理员密码，可直接进入控制台'}</Typography.Paragraph>
      </div>
      <ErrorNotice error={status.error} retry={() => void status.refetch()} /><ErrorNotice error={mutation.error} />
      {!configured && <div className="login-unconfigured"><Alert type="info" showIcon title="未启用管理员密码认证，所有能访问该端口的人都可以操作。" /><Button type="primary" size="large" block autoFocus onClick={onLogin}>进入控制台</Button></div>}
      {configured && <Form layout="vertical" requiredMark={false} onFinish={async values => {
        if (submitting.current) return;
        submitting.current = true;
        try {
          await mutation.mutateAsync(values).catch(() => {});
        } finally {
          submitting.current = false;
        }
      }} disabled={mutation.isPending || !status.data}>
        <Form.Item name="password" label="管理员密码" rules={[{ required: true, message: '请输入密码' }]}>
          <Input.Password prefix={<LockOutlined />} size="large" autoComplete="current-password" autoFocus />
        </Form.Item>
        <Button type="primary" size="large" htmlType="submit" block loading={mutation.isPending}>进入控制台</Button>
      </Form>}
      <p className="login-note">{configured ? '会话仅保存在当前浏览器标签页' : '未启用认证，可直接操作控制台'}</p>
    </div></section>
  </main>;
}
