import { useContext, useEffect, useRef, useState } from 'react';
import { flushSync } from 'react-dom';
import { Alert, App, Button, Card, Checkbox, Col, Form, Input, InputNumber, Row, Space, Switch, Tabs } from 'antd';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, post } from '../lib/api';
import { EditContext, useSession } from '../lib/session';
import type { GlobalConfig, ServerConfig } from '../lib/types';
import { ErrorNotice, PageTitle } from '../components/Common';
import { DatabaseSettings } from '../components/DatabaseSettings';

type GlobalForm = GlobalConfig & { api_key?: string; api_secret?: string; telegram_bot_token?: string; clear_api_credentials: boolean; clear_telegram_credentials: boolean };
function GlobalSettings() {
  const [form] = Form.useForm<GlobalForm>();
  const edit = useContext(EditContext);
  const [ready, setReady] = useState(false);
  const { message, modal } = App.useApp();
  const client = useQueryClient();
  const query = useQuery({ queryKey: ['global-config'], queryFn: ({ signal }) => api<GlobalConfig>('/api/global/config', { signal }), refetchOnWindowFocus: false });
  const baseline = useRef<GlobalConfig | null>(null);
  const mounted = useRef(false);
  const submitting = useRef(false);
  const fill = (data: GlobalConfig) => {
    baseline.current = data;
    form.setFieldsValue({ ...data, pause_on_zero_available_margin: data.pause_on_zero_available_margin ?? false, api_key: '', api_secret: '', telegram_bot_token: '', clear_api_credentials: false, clear_telegram_credentials: false });
  };
  useEffect(() => { if (query.data && !ready) { fill(query.data); setReady(true); } }, [query.data, ready]);
  useEffect(() => {
    mounted.current = true;
    return () => { mounted.current = false; edit.setDirty(false); edit.setSaving(false); };
  }, [edit.setDirty, edit.setSaving]);
  const mutation = useMutation({
    mutationFn: (v: GlobalForm) => post<GlobalConfig>('/api/global/config', {
      baseline_version: baseline.current?.version, recv_window: v.recv_window, sync_interval_secs: v.sync_interval_secs,
      global_max_position_notional: v.global_max_position_notional ?? null,
      pause_on_zero_available_margin: v.pause_on_zero_available_margin ?? false,
      api_key: v.api_key?.trim() || null, api_secret: v.api_secret?.trim() || null,
      telegram_bot_token: v.telegram_bot_token?.trim() || null, telegram_chat_id: v.telegram_chat_id?.trim() || '',
      telegram_enabled: !!v.telegram_enabled, clear_api_credentials: !!v.clear_api_credentials, clear_telegram_credentials: !!v.clear_telegram_credentials,
    }),
    onMutate: () => edit.setSaving(true),
    onSuccess: data => { client.setQueryData(['global-config'], data); fill(data); edit.setDirty(false); message.success('全局设置已保存，所有交易对立即生效'); },
    onError: error => message.error(error.message),
    onSettled: () => { submitting.current = false; edit.setSaving(false); },
  });
  const submit = async (values: GlobalForm) => {
    if (submitting.current) return;
    submitting.current = true;
    flushSync(() => edit.setSaving(true));
    const changes: string[] = [];
    if ((values.global_max_position_notional || null) !== (baseline.current?.global_max_position_notional || null)) {
      changes.push(`全局最大持仓金额：${values.global_max_position_notional || '关闭'}`);
    }
    if (!!values.pause_on_zero_available_margin !== !!baseline.current?.pause_on_zero_available_margin) {
      changes.push(`保证金不足时暂停：${values.pause_on_zero_available_margin ? '开启' : '关闭'}`);
    }
    if (values.clear_api_credentials) changes.push('清除已保存的 API Key 和 Secret');
    if (values.clear_telegram_credentials) changes.push('清除已保存的 Telegram 凭证');
    try {
      if (changes.length && !await modal.confirm({
        title: '确认保存全局设置？',
        content: <div><p>以下变更保存后立即生效，全局风控将作用于所有交易对：</p><ul>{changes.map(change => <li key={change}>{change}</li>)}</ul></div>,
        okText: '确认保存', cancelText: '继续编辑',
      })) {
        submitting.current = false;
        if (mounted.current) edit.setSaving(false);
        return;
      }
      if (!mounted.current) { submitting.current = false; return; }
      mutation.mutate(values);
    } catch (error) {
      submitting.current = false;
      if (mounted.current) edit.setSaving(false);
      throw error;
    }
  };
  return <>
    <ErrorNotice error={query.error} retry={() => void query.refetch()} />
    <ErrorNotice error={mutation.error} />
    {mutation.error?.message.includes('全局设置已被其他请求修改') && <Button onClick={async () => {
      if (!await modal.confirm({ title: '重新加载全局设置？', content: '重新加载会放弃当前未保存的修改，请先记录需要保留的内容。', okText: '重新加载', cancelText: '继续编辑' })) return;
      const result = await query.refetch();
      if (!result.error && result.data) { fill(result.data); edit.setDirty(false); mutation.reset(); }
    }}>重新加载全局设置</Button>}
    <Form form={form} layout="vertical" disabled={!ready || edit.saving} onValuesChange={() => edit.setDirty(true)} onFinish={v => void submit(v)}>
      <Card title="交易所连接"><Alert type="info" showIcon title={`密钥不会回显，留空将保留已有值。${query.data?.has_api_key ? `当前 API Key：${query.data.api_key_preview}` : '尚未配置 API Key。'}`} />
        <Row gutter={20} className="settings-fields">
          <Col xs={24} md={12}><Form.Item name="api_key" label="API Key"><Input.Password autoComplete="off" placeholder="留空保留已保存的值" /></Form.Item></Col>
          <Col xs={24} md={12}><Form.Item name="api_secret" label="API Secret"><Input.Password autoComplete="new-password" placeholder="留空保留已保存的值" /></Form.Item></Col>
          <Col xs={24} md={12}><Form.Item name="recv_window" label="请求有效窗口（毫秒）" extra="交易所接受已签名请求的时间窗口。" rules={[{ required: true }]}><InputNumber min={1} precision={0} style={{ width: '100%' }} /></Form.Item></Col>
          <Col xs={24} md={12}><Form.Item name="sync_interval_secs" label="同步间隔（秒）" extra="策略定期同步订单、账户和持仓的间隔。" rules={[{ required: true }]}><InputNumber min={1} precision={0} style={{ width: '100%' }} /></Form.Item></Col>
          <Col span={24}><Form.Item name="clear_api_credentials" valuePropName="checked"><Checkbox>清除已保存的 API Key 和 Secret（所有交易对须为模拟盘）</Checkbox></Form.Item></Col>
        </Row>
      </Card>
      <Card title="全局风控" className="form-card"><Row gutter={20}>
        <Col xs={24} md={12}><Form.Item name="global_max_position_notional" label="全局最大持仓金额" extra="留空表示关闭；所有已配置策略的持仓绝对金额直接相加（包括不同模式和计价资产，不进行汇率换算），达到后暂停所有策略。" rules={[{ validator: (_, value: string | null | undefined) => !value || (/^\d+(\.\d+)?$/.test(value) && /[1-9]/.test(value)) ? Promise.resolve() : Promise.reject(new Error('请输入大于 0 的金额')) }]}><InputNumber stringMode min="0" style={{ width: '100%' }} /></Form.Item></Col>
        <Col xs={24} md={12}><Form.Item name="pause_on_zero_available_margin" label="可用保证金不足时暂停" valuePropName="checked" extra="开启后，任意已配置策略的可用保证金小于或等于零时，暂停所有策略、撤销机器人挂单并保留持仓。"><Switch checkedChildren="开启" unCheckedChildren="关闭" /></Form.Item></Col>
      </Row></Card>
      <Card title="Telegram 通知" className="form-card"><Row gutter={20}>
        <Col span={24}><Form.Item name="telegram_enabled" label="通知开关" valuePropName="checked"><Switch checkedChildren="开启" unCheckedChildren="关闭" /></Form.Item></Col>
        <Col xs={24} md={12}><Form.Item name="telegram_bot_token" label="Bot Token" extra={query.data?.has_telegram_bot_token ? '已保存 Token，留空保持不变。' : '尚未配置 Token。'}><Input.Password autoComplete="new-password" /></Form.Item></Col>
        <Col xs={24} md={12}><Form.Item name="telegram_chat_id" label="Chat ID" extra="向机器人发送消息后，可通过 Telegram Bot API 的 getUpdates 获取会话 ID。"><Input /></Form.Item></Col>
        <Col span={24}><Form.Item name="clear_telegram_credentials" valuePropName="checked"><Checkbox>清除 Telegram 凭证（请先关闭通知）</Checkbox></Form.Item></Col>
      </Row></Card>
      <div className="save-bar"><span className="muted">{edit.dirty ? '有未保存的修改' : '所有修改已保存'}</span><Space wrap><Button onClick={async () => { if (edit.dirty && !await modal.confirm({ title: '放弃未保存的修改？', okText: '还原', cancelText: '继续编辑' })) return; if (query.data) fill(query.data); edit.setDirty(false); }}>还原</Button><Button htmlType="submit" loading={mutation.isPending}>保存全局设置</Button></Space></div>
    </Form>
  </>;
}
function ServerSettings() {
  const [form] = Form.useForm<ServerConfig['saved']>();
  const [ready, setReady] = useState(false);
  const edit = useContext(EditContext);
  const { message, modal } = App.useApp();
  const query = useQuery({ queryKey: ['server-config'], queryFn: ({ signal }) => api<ServerConfig>('/api/server/config', { signal }) });
  const baseline = useRef<string | null>(null);
  useEffect(() => { if (query.data && !ready) { baseline.current = query.data.version; form.setFieldsValue(query.data.saved); setReady(true); } }, [query.data, ready, form]);
  useEffect(() => () => { edit.setDirty(false); edit.setSaving(false); }, [edit.setDirty, edit.setSaving]);
  const mutation = useMutation({
    mutationFn: (values: ServerConfig['saved']) => post<ServerConfig>('/api/server/config', { ...values, baseline_version: baseline.current }),
    onMutate: () => edit.setSaving(true),
    onSuccess: data => { baseline.current = data.version; form.setFieldsValue(data.saved); edit.setDirty(false); message.success('服务设置已保存，重启后生效'); void query.refetch(); },
    onError: error => message.error(error.message),
    onSettled: () => edit.setSaving(false),
  });
  return <><ErrorNotice error={query.error} retry={() => void query.refetch()} /><ErrorNotice error={mutation.error} />
    {mutation.error?.message.includes('服务设置已被其他请求修改') && <Button onClick={async () => {
      if (!await modal.confirm({ title: '重新加载服务设置？', content: '重新加载会放弃当前未保存的修改，请先记录需要保留的内容。', okText: '重新加载', cancelText: '继续编辑' })) return;
      const result = await query.refetch();
      if (!result.error && result.data) { baseline.current = result.data.version; form.setFieldsValue(result.data.saved); edit.setDirty(false); mutation.reset(); }
    }}>重新加载服务设置</Button>}
    <Card title="监听设置"><Alert type="info" title={`当前监听：${query.data?.active.host ?? '--'}:${query.data?.active.port ?? '--'}。修改后重启生效；使用 Docker 时请同时调整代理配置。`} />
      <Form form={form} layout="vertical" className="settings-fields" disabled={!ready || edit.saving} onValuesChange={() => edit.setDirty(true)} onFinish={v => mutation.mutate(v)}>
        <Row gutter={20}><Col xs={24} md={12}><Form.Item name="host" label="监听地址" rules={[{ required: true }]}><Input /></Form.Item></Col>
          <Col xs={24} md={12}><Form.Item name="port" label="端口" rules={[{ required: true }]}><InputNumber min={1} max={65535} precision={0} style={{ width: '100%' }} /></Form.Item></Col></Row>
        <Button htmlType="submit" loading={mutation.isPending}>保存监听设置</Button>
      </Form>
    </Card>
  </>;
}
function SecuritySettings() {
  const edit = useContext(EditContext);
  const { message } = App.useApp();
  const session = useSession();
  const client = useQueryClient();
  const [form] = Form.useForm();
  useEffect(() => () => { edit.setDirty(false); edit.setSaving(false); }, [edit.setDirty, edit.setSaving]);
  const mutation = useMutation({
    mutationFn: (values: { old_password: string; new_password: string }) => post('/api/auth/change_password', values),
    onMutate: () => edit.setSaving(true),
    onSuccess: () => { form.resetFields(); edit.setDirty(false); message.success('密码已更新，请重新登录'); client.clear(); void session.logout(true); },
    onError: error => message.error(error.message),
    onSettled: () => edit.setSaving(false),
  });
  return <Card title="修改管理员密码"><Form form={form} layout="vertical" onValuesChange={(_, values) => edit.setDirty(Object.values(values).some(Boolean))} onFinish={v => mutation.mutate(v)} disabled={mutation.isPending} className="security-form">
    <Form.Item name="old_password" label="当前密码" rules={[{ required: true }]}><Input.Password autoComplete="current-password" /></Form.Item>
    <Form.Item name="new_password" label="新密码" rules={[{ required: true }, { min: 6, message: '至少 6 个字符' }]}><Input.Password autoComplete="new-password" /></Form.Item>
    <Form.Item name="confirm" label="确认新密码" dependencies={['new_password']} rules={[{ required: true }, ({ getFieldValue }) => ({ validator: (_, value) => value === getFieldValue('new_password') ? Promise.resolve() : Promise.reject(new Error('两次密码不一致')) })]}><Input.Password autoComplete="new-password" /></Form.Item>
    <Button htmlType="submit" loading={mutation.isPending}>更新密码</Button>
  </Form></Card>;
}
export default function Settings() {
  const edit = useContext(EditContext);
  const { modal } = App.useApp();
  const [active, setActive] = useState('global');
  return <><PageTitle title="全局设置" description="交易所连接、通知和控制台安全设置。" />
    <Tabs className="settings-tabs" activeKey={active} onChange={async key => {
      if (edit.saving) return;
      if (edit.dirty && !await modal.confirm({ title: '放弃未保存的修改？', okText: '放弃修改', cancelText: '继续编辑' })) return;
      edit.setDirty(false); setActive(key);
    }} destroyOnHidden items={[
      { key: 'global', label: '连接与通知', children: <GlobalSettings /> },
      { key: 'server', label: '服务设置', children: <ServerSettings /> },
      { key: 'database', label: '数据库操作', children: <DatabaseSettings /> },
      { key: 'security', label: '账户安全', children: <SecuritySettings /> },
    ]} />
  </>;
}
