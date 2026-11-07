import { Alert, Button, Form, Input, Typography } from 'antd';
import { LockOutlined } from '@ant-design/icons';
import { useMutation, useQuery } from '@tanstack/react-query';
import { api, post, tokenStore } from '../lib/api';
import type { AuthStatus } from '../lib/types';
import { ErrorNotice } from '../components/Common';
import { LoadingScreen } from '../components/Loading';
import { ThemeSwitch } from '../components/ThemeSwitch';

export default function Login({ onLogin }: { onLogin: () => void }) {
  const status = useQuery({ queryKey: ['auth-status'], queryFn: () => api<AuthStatus>('/api/auth/status'), retry: false });
  const configured = status.data?.initialized !== false;
  const mutation = useMutation({
    mutationFn: (values: { password: string }) => post<{ token: string }>('/api/auth/login', values),
    onSuccess: data => { tokenStore.set(data.token); onLogin(); },
  });
  if (status.isPending) return <LoadingScreen label="正在连接控制台" />;
  return <main className="login-page">
    <div className="login-theme"><ThemeSwitch /></div>
    <aside className="login-brand">
      <div className="login-brand-content">
        <img src="/logo.svg" alt="" className="login-logo" />
        <div><strong>Grid Console</strong><span>Binance Futures</span></div>
      </div>
      <svg className="login-ladder" viewBox="0 0 320 220" aria-hidden="true" focusable="false">
        {[20, 44, 68, 92].map(y => <g key={y}><line x1="24" x2="296" y1={y} y2={y} className="ladder-sell" /><rect x="24" y={y - 3} width="6" height="6" className="ladder-sell-mark" /></g>)}
        <line x1="0" x2="320" y1="116" y2="116" className="ladder-mark" />
        <rect x="236" y="104" width="84" height="24" rx="4" className="ladder-mark-tag" />
        <text x="278" y="120" textAnchor="middle" className="ladder-mark-text">MARK</text>
        {[140, 164, 188, 212].map(y => <g key={y}><line x1="24" x2="296" y1={y} y2={y} className="ladder-buy" /><rect x="24" y={y - 3} width="6" height="6" className="ladder-buy-mark" /></g>)}
      </svg>
    </aside>
    <section className="login-form"><div className="login-form-content">
      <Typography.Title level={2}>{configured ? '登录交易控制台' : '无需登录'}</Typography.Title>
      <Typography.Paragraph type="secondary">{configured ? '输入管理员密码，进入策略工作台' : '服务端未设置管理员密码，可直接进入控制台'}</Typography.Paragraph>
      <ErrorNotice error={status.error} retry={() => void status.refetch()} /><ErrorNotice error={mutation.error} />
      {!configured && <><Alert type="info" showIcon title="未启用管理员密码认证，所有能访问该端口的人都可以操作。" /><Button type="primary" size="large" block onClick={onLogin}>进入控制台</Button></>}
      {configured && <Form layout="vertical" onFinish={values => mutation.mutate(values)} disabled={mutation.isPending || !status.data}>
        <Form.Item name="password" label="管理员密码" rules={[{ required: true, message: '请输入密码' }]}>
          <Input.Password prefix={<LockOutlined />} size="large" autoComplete="current-password" />
        </Form.Item>
        <Button type="primary" size="large" htmlType="submit" block loading={mutation.isPending}>进入控制台</Button>
      </Form>}
      <p className="login-note">{configured ? '会话仅保存在当前浏览器标签页' : '未启用认证，可直接操作控制台'}</p>
    </div></section>
  </main>;
}
