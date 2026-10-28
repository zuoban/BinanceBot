import { useContext, useEffect, useState } from 'react';
import { Alert, App, Button, Card, Checkbox, Col, Form, Input, InputNumber, Row, Space, Switch, Tabs } from 'antd';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, post } from '../lib/api';
import { EditContext, useSession } from '../lib/session';
import type { GlobalConfig, ServerConfig } from '../lib/types';
import { ErrorNotice, PageTitle } from '../components/Common';

type GlobalForm = GlobalConfig & { api_key?: string; api_secret?: string; telegram_bot_token?: string; clear_api_credentials: boolean; clear_telegram_credentials: boolean };
function GlobalSettings() {
  const [form] = Form.useForm<GlobalForm>();
  const edit = useContext(EditContext);
  const [ready, setReady] = useState(false);
  const { message, modal } = App.useApp();
  const client = useQueryClient();
  const query = useQuery({ queryKey: ['global-config'], queryFn: ({ signal }) => api<GlobalConfig>('/api/global/config', { signal }), refetchOnWindowFocus: false });
  const fill = (data: GlobalConfig) => form.setFieldsValue({ ...data, api_key: '', api_secret: '', telegram_bot_token: '', clear_api_credentials: false, clear_telegram_credentials: false });
  useEffect(() => { if (query.data && !ready) { fill(query.data); setReady(true); } }, [query.data, ready]);
  useEffect(() => () => { edit.setDirty(false); edit.setSaving(false); }, [edit.setDirty, edit.setSaving]);
  const mutation = useMutation({
    mutationFn: (v: GlobalForm) => post<GlobalConfig>('/api/global/config', {
      recv_window: v.recv_window, sync_interval_secs: v.sync_interval_secs,
      api_key: v.api_key?.trim() || null, api_secret: v.api_secret?.trim() || null,
      telegram_bot_token: v.telegram_bot_token?.trim() || null, telegram_chat_id: v.telegram_chat_id?.trim() || '',
      telegram_enabled: !!v.telegram_enabled, clear_api_credentials: !!v.clear_api_credentials, clear_telegram_credentials: !!v.clear_telegram_credentials,
    }),
    onMutate: () => edit.setSaving(true),
    onSuccess: data => { client.setQueryData(['global-config'], data); fill(data); edit.setDirty(false); message.success('全局设置已保存，所有交易对立即生效'); },
    onError: error => message.error(error.message),
    onSettled: () => edit.setSaving(false),
  });
  return <>
    <ErrorNotice error={query.error || mutation.error} retry={() => void query.refetch()} />
    <Form form={form} layout="vertical" disabled={!ready || edit.saving} onValuesChange={() => edit.setDirty(true)} onFinish={v => mutation.mutate(v)}>
      <Card title="交易所连接"><Alert type="info" showIcon title={`密钥不会回显，留空将保留已有值。${query.data?.has_api_key ? `当前 API Key：${query.data.api_key_preview}` : '尚未配置 API Key。'}`} />
        <Row gutter={20} className="settings-fields">
          <Col xs={24} md={12}><Form.Item name="api_key" label="API Key"><Input.Password autoComplete="off" placeholder="留空保留已保存的值" /></Form.Item></Col>
          <Col xs={24} md={12}><Form.Item name="api_secret" label="API Secret"><Input.Password autoComplete="new-password" placeholder="留空保留已保存的值" /></Form.Item></Col>
          <Col xs={24} md={12}><Form.Item name="recv_window" label="请求有效窗口（毫秒）" rules={[{ required: true }]}><InputNumber min={1} precision={0} style={{ width: '100%' }} /></Form.Item></Col>
          <Col xs={24} md={12}><Form.Item name="sync_interval_secs" label="同步间隔（秒）" rules={[{ required: true }]}><InputNumber min={1} precision={0} style={{ width: '100%' }} /></Form.Item></Col>
          <Col span={24}><Form.Item name="clear_api_credentials" valuePropName="checked"><Checkbox>清除已保存的 API Key 和 Secret（所有交易对须为模拟盘）</Checkbox></Form.Item></Col>
        </Row>
      </Card>
      <Card title="Telegram 通知" className="form-card"><Row gutter={20}>
        <Col span={24}><Form.Item name="telegram_enabled" label="通知开关" valuePropName="checked"><Switch checkedChildren="开启" unCheckedChildren="关闭" /></Form.Item></Col>
        <Col xs={24} md={12}><Form.Item name="telegram_bot_token" label="Bot Token" extra={query.data?.has_telegram_bot_token ? '已保存 Token，留空保持不变。' : '尚未配置 Token。'}><Input.Password autoComplete="new-password" /></Form.Item></Col>
        <Col xs={24} md={12}><Form.Item name="telegram_chat_id" label="Chat ID"><Input /></Form.Item></Col>
        <Col span={24}><Form.Item name="clear_telegram_credentials" valuePropName="checked"><Checkbox>清除 Telegram 凭证（请先关闭通知）</Checkbox></Form.Item></Col>
      </Row></Card>
      <div className="save-bar"><span className="muted">{edit.dirty ? '有未保存的修改' : '所有修改已保存'}</span><Space><Button onClick={async () => { if (edit.dirty && !await modal.confirm({ title: '放弃未保存的修改？', okText: '还原', cancelText: '继续编辑' })) return; if (query.data) fill(query.data); edit.setDirty(false); }}>还原</Button><Button type="primary" htmlType="submit" loading={mutation.isPending}>保存全局设置</Button></Space></div>
    </Form>
  </>;
}
function ServerSettings() {
  const [form] = Form.useForm<ServerConfig['saved']>();
  const [ready, setReady] = useState(false);
  const edit = useContext(EditContext);
  const { message } = App.useApp();
  const query = useQuery({ queryKey: ['server-config'], queryFn: ({ signal }) => api<ServerConfig>('/api/server/config', { signal }) });
  useEffect(() => { if (query.data && !ready) { form.setFieldsValue(query.data.saved); setReady(true); } }, [query.data, ready, form]);
  useEffect(() => () => { edit.setDirty(false); edit.setSaving(false); }, [edit.setDirty, edit.setSaving]);
  const mutation = useMutation({
    mutationFn: (values: ServerConfig['saved']) => post<ServerConfig>('/api/server/config', values),
    onMutate: () => edit.setSaving(true),
    onSuccess: data => { form.setFieldsValue(data.saved); edit.setDirty(false); message.success('服务设置已保存，重启后生效'); void query.refetch(); },
    onError: error => message.error(error.message),
    onSettled: () => edit.setSaving(false),
  });
  return <><ErrorNotice error={query.error || mutation.error} retry={() => void query.refetch()} />
    <Card title="监听设置"><Alert type="info" title={`当前监听：${query.data?.active.host ?? '--'}:${query.data?.active.port ?? '--'}。修改后重启生效；使用 Docker 时请同时调整代理配置。`} />
      <Form form={form} layout="vertical" className="settings-fields" disabled={!ready || edit.saving} onValuesChange={() => edit.setDirty(true)} onFinish={v => mutation.mutate(v)}>
        <Row gutter={20}><Col xs={24} md={12}><Form.Item name="host" label="监听地址" rules={[{ required: true }]}><Input /></Form.Item></Col>
          <Col xs={24} md={12}><Form.Item name="port" label="端口" rules={[{ required: true }]}><InputNumber min={1} max={65535} precision={0} style={{ width: '100%' }} /></Form.Item></Col></Row>
        <Button type="primary" htmlType="submit" loading={mutation.isPending}>保存监听设置</Button>
      </Form>
    </Card>
  </>;
}
function SecuritySettings() {
  const { message } = App.useApp();
  const session = useSession();
  const client = useQueryClient();
  const [form] = Form.useForm();
  const mutation = useMutation({
    mutationFn: (values: { old_password: string; new_password: string }) => post('/api/auth/change_password', values),
    onSuccess: () => { form.resetFields(); message.success('密码已更新，请重新登录'); client.clear(); void session.logout(); },
    onError: error => message.error(error.message),
  });
  return <Card title="修改管理员密码"><Form form={form} layout="vertical" onFinish={v => mutation.mutate(v)} disabled={mutation.isPending} className="security-form">
    <Form.Item name="old_password" label="当前密码" rules={[{ required: true }]}><Input.Password autoComplete="current-password" /></Form.Item>
    <Form.Item name="new_password" label="新密码" rules={[{ required: true }, { min: 6, message: '至少 6 个字符' }]}><Input.Password autoComplete="new-password" /></Form.Item>
    <Form.Item name="confirm" label="确认新密码" dependencies={['new_password']} rules={[{ required: true }, ({ getFieldValue }) => ({ validator: (_, value) => value === getFieldValue('new_password') ? Promise.resolve() : Promise.reject(new Error('两次密码不一致')) })]}><Input.Password autoComplete="new-password" /></Form.Item>
    <Button type="primary" htmlType="submit" loading={mutation.isPending}>更新密码</Button>
  </Form></Card>;
}
export default function Settings() {
  const edit = useContext(EditContext);
  const { modal } = App.useApp();
  const [active, setActive] = useState('global');
  return <><PageTitle eyebrow="WORKSPACE / SETTINGS" title="全局设置" description="交易所连接、通知和控制台安全设置。" />
    <Tabs activeKey={active} onChange={async key => {
      if (edit.saving) return;
      if (edit.dirty && !await modal.confirm({ title: '放弃未保存的修改？', okText: '放弃修改', cancelText: '继续编辑' })) return;
      edit.setDirty(false); setActive(key);
    }} destroyOnHidden items={[
      { key: 'global', label: '连接与通知', children: <GlobalSettings /> },
      { key: 'server', label: '服务设置', children: <ServerSettings /> },
      { key: 'security', label: '账户安全', children: <SecuritySettings /> },
    ]} />
  </>;
}
