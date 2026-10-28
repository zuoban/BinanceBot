import { useState } from 'react';
import { Alert, Button, Card, Form, Input, Typography } from 'antd';
import { LockOutlined, SafetyCertificateOutlined } from '@ant-design/icons';
import { useMutation, useQuery } from '@tanstack/react-query';
import { api, post, tokenStore } from '../lib/api';
import type { AuthStatus } from '../lib/types';
import { ErrorNotice } from '../components/Common';

export default function Login({ onLogin }: { onLogin: () => void }) {
  const status = useQuery({ queryKey: ['auth-status'], queryFn: () => api<AuthStatus>('/api/auth/status'), retry: false });
  const [error, setError] = useState<Error | null>(null);
  const setup = status.data?.initialized === false;
  const mutation = useMutation({
    mutationFn: (values: { password: string; setup_code?: string }) => post<{ token: string }>(`/api/auth/${setup ? 'setup' : 'login'}`, values),
    onSuccess: data => { tokenStore.set(data.token); onLogin(); },
    onError: setError,
  });
  return <main className="login-page">
    <section className="login-story"><div className="brand-mark">G<span> / </span>GRID CONSOLE</div><div>
      <div className="eyebrow">BINANCE FUTURES · STRATEGY WORKSPACE</div>
      <h1>让每一格，<br />清晰可见。</h1><p>行情、仓位与策略状态，<br />在一个工作台中掌握。</p>
    </div><div className="login-foot">实时监控 / 独立策略 / 统一管理</div></section>
    <section className="login-form"><Card variant="borderless">
      <SafetyCertificateOutlined className="login-icon" /><Typography.Title level={2}>{setup ? '初始化控制台' : '欢迎回来'}</Typography.Title>
      <Typography.Paragraph type="secondary">{setup ? '使用启动日志中的初始化码，创建管理员密码。' : '输入管理员密码，进入你的策略工作台。'}</Typography.Paragraph>
      <ErrorNotice error={status.error} retry={() => void status.refetch()} /><ErrorNotice error={error} />
      {setup && <Alert type="info" showIcon title="首次启动默认使用模拟盘" />}
      <Form layout="vertical" onFinish={values => { setError(null); mutation.mutate(values); }} disabled={mutation.isPending || !status.data}>
        {setup && <Form.Item name="setup_code" label="初始化码" rules={[{ required: true, message: '请输入初始化码' }]}><Input autoComplete="off" /></Form.Item>}
        <Form.Item name="password" label="管理员密码" rules={[{ required: true, message: '请输入密码' }, ...(setup ? [{ min: 6, message: '至少 6 个字符' }] : [])]}>
          <Input.Password prefix={<LockOutlined />} size="large" autoComplete={setup ? 'new-password' : 'current-password'} />
        </Form.Item>
        {setup && <Form.Item name="confirm" label="确认密码" dependencies={['password']} rules={[{ required: true, message: '请再次输入密码' }, ({ getFieldValue }) => ({ validator: (_, value) => value === getFieldValue('password') ? Promise.resolve() : Promise.reject(new Error('两次密码不一致')) })]}><Input.Password autoComplete="new-password" /></Form.Item>}
        <Button type="primary" size="large" htmlType="submit" block loading={mutation.isPending}>{setup ? '创建并进入控制台' : '进入控制台'}</Button>
      </Form>
    </Card><p className="login-note">会话仅保存在当前浏览器标签页</p></section>
  </main>;
}
