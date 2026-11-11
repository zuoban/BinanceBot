import { Suspense, lazy, useContext, useEffect, useRef, useState } from 'react';
import { flushSync } from 'react-dom';
import { Alert, App, Button, Card, Checkbox, Col, Form, Input, InputNumber, Row, Space, Switch, Tabs } from 'antd';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, post } from '../lib/api';
import { EditContext } from '../lib/session';
import type { GlobalConfig, ServerConfig } from '../lib/types';
import { ConflictReloadButton, ErrorNotice, FormRevertButton, isConfigConflict, PageTitle } from '../components/Common';
import { LoadErrorBoundary } from '../components/LoadErrorBoundary';
import { useUrlParam } from '../lib/urlState';
import { isPercentDecimal, isPositiveDecimal } from '../lib/gridPreview';
import { diffGlobalConfig } from '../lib/configDiff';
const DatabaseSettings = lazy(() => import('../components/DatabaseSettings').then(module => ({ default: module.DatabaseSettings })));

type GlobalForm = GlobalConfig & { api_key?: string; api_secret?: string; telegram_bot_token?: string; clear_api_credentials: boolean; clear_telegram_credentials: boolean };
/** Verify unsaved credentials against the exchange or Telegram; nothing is persisted by the probe. */
function ProbeButton({ label, path, body, disabled, resetKey }: { label: string; path: string; body: () => Record<string, unknown>; disabled?: boolean; resetKey?: string }) {
  const { message } = App.useApp();
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<{ ok: boolean; text: string } | null>(null);
  const running = useRef(false);
  const mounted = useRef(true);
  const activeKey = useRef(resetKey);
  activeKey.current = resetKey;
  useEffect(() => {
    mounted.current = true;
    return () => { mounted.current = false; };
  }, []);
  useEffect(() => { setResult(null); }, [resetKey, disabled]);
  const run = async () => {
    if (running.current || disabled) return;
    running.current = true;
    const key = activeKey.current;
    setBusy(true); setResult(null);
    try {
      const text = await post<string>(path, body());
      if (mounted.current && activeKey.current === key) setResult({ ok: true, text });
    } catch (error) {
      if (mounted.current && activeKey.current === key) {
        const text = errorMessage(error);
        setResult({ ok: false, text });
        message.error(text);
      }
    } finally {
      running.current = false;
      if (mounted.current) setBusy(false);
    }
  };
  return <div className="probe">
    <Button onClick={() => void run()} loading={busy} disabled={disabled}>{label}</Button>
    <span className={`probe-result${result ? (result.ok ? ' probe-ok' : ' probe-fail') : ''}`} role="status">{result ? `${result.ok ? '✓' : '✕'} ${result.text}` : ''}</span>
  </div>;
}
function useSettingsFormLifecycle<T>(data: T | undefined, apply: (data: T) => void) {
  const edit = useContext(EditContext);
  const [ready, setReady] = useState(false);
  const baseline = useRef<T | null>(null);
  const mounted = useRef(true);
  const submitting = useRef(false);
  const applyRef = useRef(apply);
  applyRef.current = apply;
  const fill = (next: T) => {
    baseline.current = next;
    applyRef.current(next);
  };
  useEffect(() => {
    if (data && !ready) {
      baseline.current = data;
      applyRef.current(data);
      setReady(true);
    }
  }, [data, ready]);
  useEffect(() => {
    mounted.current = true;
    return () => { mounted.current = false; edit.setDirty(false); edit.setSaving(false); };
  }, [edit.setDirty, edit.setSaving]);
  return { edit, ready, baseline, mounted, submitting, fill };
}
function GlobalSettings() {
  const [form] = Form.useForm<GlobalForm>();
  const { message, modal } = App.useApp();
  const client = useQueryClient();
  const query = useQuery({ queryKey: ['global-config'], queryFn: ({ signal }) => api<GlobalConfig>('/api/global/config', { signal }), refetchOnWindowFocus: false });
  const { edit, ready, baseline, mounted, submitting, fill } = useSettingsFormLifecycle(query.data, data => {
    form.setFieldsValue({ ...data, pause_on_zero_available_margin: data.pause_on_zero_available_margin ?? false, api_key: '', api_secret: '', telegram_bot_token: '', clear_api_credentials: false, clear_telegram_credentials: false });
  });
  const mutation = useMutation({
    mutationFn: (v: GlobalForm) => post<GlobalConfig>('/api/global/config', {
      baseline_version: baseline.current?.version, recv_window: v.recv_window, sync_interval_secs: v.sync_interval_secs,
      global_max_position_notional: v.global_max_position_notional || null,
      pause_on_zero_available_margin: v.pause_on_zero_available_margin ?? false,
      min_available_margin_pct: v.min_available_margin_pct || null,
      api_key: v.clear_api_credentials ? '' : v.api_key?.trim() || null, api_secret: v.clear_api_credentials ? '' : v.api_secret?.trim() || null,
      telegram_bot_token: v.clear_telegram_credentials ? '' : v.telegram_bot_token?.trim() || null, telegram_chat_id: v.clear_telegram_credentials ? '' : v.telegram_chat_id?.trim() || '',
      telegram_enabled: !!v.telegram_enabled, clear_api_credentials: !!v.clear_api_credentials, clear_telegram_credentials: !!v.clear_telegram_credentials,
    }),
    onMutate: () => edit.setSaving(true),
    onSuccess: (data, values) => {
      client.setQueryData(['global-config'], data);
      if (mounted.current) {
        fill(data);
        edit.setDirty(false);
      }
      message.success('全局设置已保存，所有交易对立即生效');
      if (values.api_key?.trim() || values.api_secret?.trim() || values.clear_api_credentials) {
        void client.resetQueries({ queryKey: ['overview-history'] });
      }
      void client.invalidateQueries({ queryKey: ['pairs'] });
      void client.invalidateQueries({ queryKey: ['overview'] });
      void client.invalidateQueries({ queryKey: ['accounts'] });
      void client.invalidateQueries({ queryKey: ['snapshot'] });
    },
    onError: error => message.error(error.message),
    onSettled: () => {
      submitting.current = false;
      if (mounted.current) edit.setSaving(false);
    },
  });
  const submit = async (values: GlobalForm) => {
    if (submitting.current) return;
    submitting.current = true;
    flushSync(() => edit.setSaving(true));
    const changes = diffGlobalConfig(baseline.current, values);
    try {
      if (changes.length && !await modal.confirm({
        title: '确认保存全局设置？',
        content: <div className="confirm-changes"><p>以下变更保存后立即生效：</p><ul>{changes.map(change => <li key={change}>{change}</li>)}</ul></div>,
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
  const apiKey = Form.useWatch('api_key', form);
  const apiSecret = Form.useWatch('api_secret', form);
  const recvWindow = Form.useWatch('recv_window', form);
  const botToken = Form.useWatch('telegram_bot_token', form);
  const chatId = Form.useWatch('telegram_chat_id', form);
  const clearApi = !!Form.useWatch('clear_api_credentials', form);
  const clearTelegram = !!Form.useWatch('clear_telegram_credentials', form);
  const version = baseline.current?.version ?? query.data?.version ?? '';
  return <>
    <ErrorNotice error={query.error} retry={() => void query.refetch()} />
    <ErrorNotice error={mutation.error} />
    {isConfigConflict(mutation.error, '全局设置已被其他请求修改') && <ConflictReloadButton label="全局设置" activeRef={mounted} refetch={query.refetch} onReset={mutation.reset} onLoaded={fill} />}
    <Form form={form} layout="vertical" disabled={!ready || edit.saving} onValuesChange={() => edit.setDirty(true)} onFinish={v => void submit(v)}>
      <Card title="交易所连接" className="settings-card">
        <Alert type="info" showIcon title={`密钥不会回显，留空将保留已有值。${query.data?.has_api_key ? `当前 API Key：${query.data.api_key_preview}` : '尚未配置 API Key。'}运行模式由 API Key 决定：已配置以实盘运行，未配置以模拟盘运行。`} />
        <Row gutter={[20, 16]} className="settings-fields">
          <Col xs={24} md={12}><Form.Item name="api_key" label="API Key"><Input.Password autoComplete="off" placeholder="留空保留已保存的值" disabled={!ready || edit.saving || clearApi} /></Form.Item></Col>
          <Col xs={24} md={12}><Form.Item name="api_secret" label="API Secret"><Input.Password autoComplete="new-password" placeholder="留空保留已保存的值" disabled={!ready || edit.saving || clearApi} /></Form.Item></Col>
          <Col xs={24} md={12}><Form.Item name="recv_window" label="请求有效窗口（毫秒）" extra="交易所接受已签名请求的时间窗口" rules={[{ required: true }]}><InputNumber min={1} precision={0} className="full-width-input" /></Form.Item></Col>
          <Col xs={24} md={12}><Form.Item name="sync_interval_secs" label="同步间隔（秒）" extra="策略定期同步订单、账户和持仓的间隔" rules={[{ required: true }]}><InputNumber min={1} precision={0} className="full-width-input" /></Form.Item></Col>
        </Row>
        <div className="settings-card-footer">
          <ProbeButton label="测试连接" path="/api/global/test-exchange" disabled={!ready || edit.saving || clearApi}
            resetKey={JSON.stringify([version, apiKey ?? '', apiSecret ?? '', recvWindow ?? '', clearApi])}
            body={() => ({ api_key: form.getFieldValue('api_key'), api_secret: form.getFieldValue('api_secret'), recv_window: form.getFieldValue('recv_window') })} />
          <Form.Item name="clear_api_credentials" valuePropName="checked" className="settings-checkbox-item"><Checkbox>清除已保存的 API Key 和 Secret（清除后所有交易对将以模拟盘运行）</Checkbox></Form.Item>
        </div>
      </Card>
      <Row gutter={[20, 20]} className="settings-grid-row">
        <Col xs={24} xl={12} className="settings-grid-col">
          <Card title="全局风控" className="settings-card settings-card-fill">
            <Row gutter={[20, 16]}>
              <Col span={24}><Form.Item name="global_max_position_notional" label="全局最大持仓金额" extra={<div className="field-help"><p><strong>触发条件：</strong>所有已配置策略的持仓绝对金额直接相加，达到设定金额。</p><p><strong>执行动作：</strong>暂停所有策略。</p><p><strong>注意：</strong>留空表示关闭；计算包括不同模式和计价资产，不进行汇率换算。</p></div>} rules={[{ validator: (_, value: string | null | undefined) => !value || isPositiveDecimal(value) ? Promise.resolve() : Promise.reject(new Error('请输入大于 0 的金额')) }]}><InputNumber stringMode min="0" className="full-width-input" /></Form.Item></Col>
              <Col span={24}><Form.Item name="pause_on_zero_available_margin" label="可用保证金不足时暂停" valuePropName="checked" extra={<div className="field-help"><p><strong>触发条件：</strong>开启后，任意已配置策略的可用保证金小于或等于零。</p><p><strong>执行动作：</strong>暂停所有策略、撤销机器人挂单并保留持仓。</p></div>}><Switch checkedChildren="开启" unCheckedChildren="关闭" /></Form.Item></Col>
              <Col span={24}><Form.Item name="min_available_margin_pct" label="可用保证金比例下限（%）" extra={<div className="field-help"><p><strong>触发条件：</strong>任意已配置策略的可用保证金占保证金余额的比例小于或等于该值。</p><p><strong>执行动作：</strong>暂停所有策略、撤销机器人挂单并保留持仓，处理方式与“可用保证金不足时暂停”相同。</p><p><strong>注意：</strong>留空表示关闭。</p></div>} rules={[{ validator: (_, value: string | null | undefined) => !value || isPercentDecimal(value) ? Promise.resolve() : Promise.reject(new Error('请输入大于 0 且小于 100 的百分比')) }]}><InputNumber stringMode min="0" max="100" className="full-width-input" /></Form.Item></Col>
            </Row>
          </Card>
        </Col>
        <Col xs={24} xl={12} className="settings-grid-col">
          <Card title="Telegram 通知" className="settings-card settings-card-fill">
            <Row gutter={[16, 16]}>
              <Col span={24}><Form.Item name="telegram_enabled" label="通知开关" valuePropName="checked"><Switch checkedChildren="开启" unCheckedChildren="关闭" /></Form.Item></Col>
              <Col xs={24} sm={12}><Form.Item name="telegram_bot_token" label="Bot Token" extra={query.data?.has_telegram_bot_token ? '已保存 Token，留空保持不变' : '尚未配置 Token'}><Input.Password autoComplete="new-password" disabled={!ready || edit.saving || clearTelegram} /></Form.Item></Col>
              <Col xs={24} sm={12}><Form.Item name="telegram_chat_id" label="Chat ID" extra="向机器人发送消息后，可通过 Telegram Bot API 的 getUpdates 获取会话 ID"><Input className="numeric" disabled={!ready || edit.saving || clearTelegram} /></Form.Item></Col>
            </Row>
            <div className="settings-card-footer">
              <ProbeButton label="发送测试消息" path="/api/global/test-telegram" disabled={!ready || edit.saving || clearTelegram}
                resetKey={JSON.stringify([version, botToken ?? '', chatId ?? '', clearTelegram])}
                body={() => ({ bot_token: form.getFieldValue('telegram_bot_token'), chat_id: form.getFieldValue('telegram_chat_id') })} />
              <Form.Item name="clear_telegram_credentials" valuePropName="checked" className="settings-checkbox-item"><Checkbox>清除 Telegram 凭证（请先关闭通知）</Checkbox></Form.Item>
            </div>
          </Card>
        </Col>
      </Row>
      <div className={`save-bar${edit.dirty ? ' save-bar-dirty' : ''}`}><span className="muted" role="status">{edit.dirty ? '有未保存的修改' : '所有修改已保存'}</span><Space wrap><FormRevertButton activeRef={mounted} onReset={mutation.reset} onRevert={() => { if (baseline.current) fill(baseline.current); else if (query.data) fill(query.data); }} /><Button type="primary" htmlType="submit" loading={mutation.isPending}>保存全局设置</Button></Space></div>
    </Form>
  </>;
}
function ServerSettings() {
  const [form] = Form.useForm<ServerConfig['saved']>();
  const { message } = App.useApp();
  const client = useQueryClient();
  const query = useQuery({ queryKey: ['server-config'], queryFn: ({ signal }) => api<ServerConfig>('/api/server/config', { signal }), refetchOnWindowFocus: false });
  const { edit, ready, baseline, mounted, submitting, fill } = useSettingsFormLifecycle(query.data, data => {
    form.setFieldsValue(data.saved);
  });
  const mutation = useMutation({
    mutationFn: (values: ServerConfig['saved']) => post<ServerConfig>('/api/server/config', { ...values, host: values.host.trim(), baseline_version: baseline.current?.version ?? null }),
    onMutate: () => edit.setSaving(true),
    onSuccess: data => {
      client.setQueryData(['server-config'], data);
      if (mounted.current) {
        fill(data);
        edit.setDirty(false);
      }
      message.success('服务设置已保存，重启后生效');
      void query.refetch();
    },
    onError: error => message.error(error.message),
    onSettled: () => {
      submitting.current = false;
      if (mounted.current) edit.setSaving(false);
    },
  });
  return <><ErrorNotice error={query.error} retry={() => void query.refetch()} /><ErrorNotice error={mutation.error} />
    {isConfigConflict(mutation.error, '服务设置已被其他请求修改') && <ConflictReloadButton label="服务设置" activeRef={mounted} refetch={query.refetch} onReset={mutation.reset} onLoaded={fill} />}
    <Card title="监听设置" className="settings-card"><Alert type="info" showIcon title={`当前监听：${query.data?.active.host ?? '--'}:${query.data?.active.port ?? '--'}。修改后重启生效；使用 Docker 时请同时调整代理配置。`} />
      <Form form={form} layout="vertical" className="settings-fields" disabled={!ready || edit.saving} onValuesChange={() => edit.setDirty(true)} onFinish={v => { if (submitting.current) return; submitting.current = true; mutation.mutate(v); }}>
        <Row gutter={[20, 16]}><Col xs={24} md={12}><Form.Item name="host" label="监听地址" rules={[{ required: true, whitespace: true }]}><Input className="numeric" /></Form.Item></Col>
          <Col xs={24} md={12}><Form.Item name="port" label="端口" rules={[{ required: true }]}><InputNumber min={1} max={65535} precision={0} className="full-width-input" /></Form.Item></Col></Row>
        <div className={`settings-card-footer settings-card-actions${edit.dirty ? ' settings-card-actions-dirty' : ''}`}>
          <span className="muted" role="status">{edit.dirty ? '有未保存的修改' : '修改后需重启后端生效'}</span>
          <Space wrap>
            <FormRevertButton activeRef={mounted} onReset={mutation.reset} onRevert={() => { if (baseline.current) fill(baseline.current); }} />
            <Button type="primary" htmlType="submit" loading={mutation.isPending}>保存监听设置</Button>
          </Space>
        </div>
      </Form>
    </Card>
  </>;
}
function SecuritySettings() {
  return <Card title="管理员密码" className="settings-card">
    <Alert type="warning" showIcon title="管理员密码通过服务端启动参数配置，不保存在数据库中。" />
    <dl className="security-guide">
      <div>
        <dt>启动参数与环境变量</dt>
        <dd>使用命令行参数 <code>--admin-password &lt;密码&gt;</code>，或设置环境变量 <code>ADMIN_PASSWORD</code>，然后重启后端。</dd>
      </div>
      <div>
        <dt>访问边界与安全建议</dt>
        <dd>未设置密码时不启用认证，所有能访问该端口的人都可以操作控制台。请仅在受信任网络或本机使用，并优先通过环境变量提供密码，避免密码出现在进程列表中。</dd>
      </div>
      <div>
        <dt>密码轮换与会话失效</dt>
        <dd>修改密码后需重启后端；重启会清除所有登录会话，需要使用新密码重新登录。</dd>
      </div>
    </dl>
  </Card>;
}
const settingsTabs = ['global', 'server', 'database', 'security'] as const;
export default function Settings() {
  const edit = useContext(EditContext);
  const { modal } = App.useApp();
  const [active, setActive] = useUrlParam('tab', 'global', settingsTabs);
  const switching = useRef(false);
  const savingRef = useRef(edit.saving);
  savingRef.current = edit.saving;
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => { mounted.current = false; };
  }, []);
  return <div className="settings-page"><PageTitle title="全局设置" description="交易所连接、通知和控制台安全设置" />
    <Tabs className="settings-tabs" activeKey={active} onChange={async key => {
      const next = settingsTabs.find(t => t === key);
      if (!next || switching.current || savingRef.current) return;
      switching.current = true;
      try {
        if (edit.dirty && !await modal.confirm({ title: '放弃未保存的修改？', okText: '放弃修改', cancelText: '继续编辑' })) return;
        if (!mounted.current || savingRef.current) return;
        edit.setDirty(false); setActive(next);
      } finally {
        switching.current = false;
      }
    }} destroyOnHidden items={[
      { key: 'global', label: '连接与通知', children: <GlobalSettings /> },
      { key: 'server', label: '服务设置', children: <ServerSettings /> },
      { key: 'database', label: '数据库操作', children: <LoadErrorBoundary label="数据库操作"><Suspense fallback={<div className="history-empty" role="status">正在加载数据库操作…</div>}><DatabaseSettings /></Suspense></LoadErrorBoundary> },
      { key: 'security', label: '账户安全', children: <SecuritySettings /> },
    ]} />
  </div>;
}
