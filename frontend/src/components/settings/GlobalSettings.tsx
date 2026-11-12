import { useState } from 'react';
import { Alert, App, Button, Card, Checkbox, Col, Form, Input, InputNumber, Row, Space, Switch, type FormInstance } from 'antd';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, post } from '../../lib/api';
import { diffGlobalConfig } from '../../lib/configDiff';
import { useConfirmDialog } from '../../lib/confirmDialog';
import { exceeds, isPercentDecimal, isPositiveDecimal, MAX_CONFIG_NOTIONAL_OR_PRICE } from '../../lib/gridPreview';
import { invalidateStrategyQueries } from '../../lib/queries';
import type { GlobalConfig } from '../../lib/types';
import { ConflictReloadButton, ErrorNotice, FormRevertButton, isConfigConflict } from '../Common';
import { ProbeButton, useSettingsFormLifecycle, type GlobalForm } from './settingsShared';

const API_KEY_RE = /^[\x21-\x7E]{1,256}$/;
const INVALID_BOT_TOKEN_RE = /[^\x21-\x7E]|[/\\?#%@]/;
const CONTROL_CHAR_RE = /[\x00-\x1F\x7F-\x9F]/;
const validBotToken = (v: string) => v.length > 0 && v.length <= 256 && !INVALID_BOT_TOKEN_RE.test(v);
const validChatId = (v: string) => v.length > 0 && new TextEncoder().encode(v).length <= 128 && !CONTROL_CHAR_RE.test(v);
const CROSS_FIELDS: (keyof GlobalForm)[] = ['api_key', 'api_secret', 'telegram_bot_token', 'telegram_chat_id', 'clear_telegram_credentials'];

function apiCredentialError(side: 'key' | 'secret', keyVal: string | undefined, secretVal: string | undefined, clearApi: boolean, data: GlobalConfig | undefined): string | null {
  if (clearApi) return null;
  const hasKey = Boolean(keyVal?.trim()), hasSecret = Boolean(secretVal?.trim());
  const savedKey = Boolean(data?.has_api_key), savedSecret = Boolean(data?.has_api_secret);
  const missing = (hasKey || hasSecret) ? (side === 'key' ? !hasKey : !hasSecret) : (side === 'key' ? !savedKey && savedSecret : savedKey && !savedSecret);
  if (!missing) return null;
  return savedKey || savedSecret ? '更换或配置 API 凭证时请同时填写 API Key 和 API Secret' : '请同时填写 API Key 和 API Secret';
}

function ExchangeCredentialsCard({ form, data, version, formDisabled, armed }: {
  form: FormInstance<GlobalForm>;
  data: GlobalConfig | undefined;
  version: string;
  formDisabled: boolean;
  armed: boolean;
}) {
  const apiKey = Form.useWatch('api_key', form);
  const apiSecret = Form.useWatch('api_secret', form);
  const recvWindow = Form.useWatch('recv_window', form);
  const clearApi = !!Form.useWatch('clear_api_credentials', form);
  const credDisabled = formDisabled || clearApi;
  const invalidFormat = Boolean((apiKey && (!apiKey.trim() || !API_KEY_RE.test(apiKey.trim()))) || (apiSecret?.trim() && !API_KEY_RE.test(apiSecret.trim())));
  return <Card title="交易所连接" className="settings-card">
    <Alert type="info" showIcon title={`密钥不会回显，留空将保留已有值。${data?.has_api_key ? `当前 API Key：${data.api_key_preview}` : '尚未配置 API Key。'}运行模式由 API Key 决定：已配置以实盘运行，未配置以模拟盘运行。`} />
    <Row gutter={[20, 16]} className="settings-fields">
      <Col xs={24} md={12}><Form.Item name="api_key" label="API Key" rules={[
        ({ getFieldValue }) => ({
          validator: (_, value: string | undefined) => {
            if (getFieldValue('clear_api_credentials') || !value) return Promise.resolve();
            const trimmed = value.trim();
            return trimmed && API_KEY_RE.test(trimmed) ? Promise.resolve() : Promise.reject(new Error('API Key 格式无效'));
          },
        }),
        ({ getFieldValue }) => ({
          validateTrigger: armed ? undefined : 'onSubmit',
          validator: (_, value: string | undefined) => {
            const error = apiCredentialError('key', value, getFieldValue('api_secret'), Boolean(getFieldValue('clear_api_credentials')), data);
            return error ? Promise.reject(new Error(error)) : Promise.resolve();
          },
        }),
      ]}><Input.Password autoComplete="off" placeholder="留空保留已保存的值" disabled={credDisabled} /></Form.Item></Col>
      <Col xs={24} md={12}><Form.Item name="api_secret" label="API Secret" rules={[
        ({ getFieldValue }) => ({
          validator: (_, value: string | undefined) => {
            const trimmed = value?.trim();
            if (getFieldValue('clear_api_credentials') || !trimmed) return Promise.resolve();
            return API_KEY_RE.test(trimmed) ? Promise.resolve() : Promise.reject(new Error('API Secret 格式无效'));
          },
        }),
        ({ getFieldValue }) => ({
          validateTrigger: armed ? undefined : 'onSubmit',
          validator: (_, value: string | undefined) => {
            const error = apiCredentialError('secret', getFieldValue('api_key'), value, Boolean(getFieldValue('clear_api_credentials')), data);
            return error ? Promise.reject(new Error(error)) : Promise.resolve();
          },
        }),
      ]}><Input.Password autoComplete="new-password" placeholder="留空保留已保存的值" disabled={credDisabled} /></Form.Item></Col>
      <Col xs={24} md={12}><Form.Item name="recv_window" label="请求有效窗口（毫秒）" extra="交易所接受已签名请求的时间窗口" rules={[{ required: true }]}><InputNumber min={1} max={60000} precision={0} className="full-width-input" /></Form.Item></Col>
      <Col xs={24} md={12}><Form.Item name="sync_interval_secs" label="同步间隔（秒）" extra="策略定期同步订单、账户和持仓的间隔" rules={[{ required: true }]}><InputNumber min={1} max={300} precision={0} className="full-width-input" /></Form.Item></Col>
    </Row>
    <div className="settings-card-footer">
      <ProbeButton label="测试连接" path="/api/global/test-exchange" disabled={credDisabled || invalidFormat}
        resetKey={JSON.stringify([version, apiKey ?? '', apiSecret ?? '', recvWindow ?? '', clearApi])}
        body={() => ({ api_key: form.getFieldValue('api_key'), api_secret: form.getFieldValue('api_secret'), recv_window: form.getFieldValue('recv_window') })} />
      <Form.Item name="clear_api_credentials" valuePropName="checked" className="settings-checkbox-item"><Checkbox>清除已保存的 API Key 和 Secret（清除后所有交易对将以模拟盘运行）</Checkbox></Form.Item>
    </div>
  </Card>;
}

function GlobalRiskCard() {
  return <Card title="全局风控" className="settings-card settings-card-fill">
    <Row gutter={[20, 16]}>
      <Col span={24}><Form.Item name="global_max_position_notional" label="全局最大持仓金额" extra={<div className="field-help"><p><strong>触发条件：</strong>所有已配置策略的持仓绝对金额直接相加，达到设定金额。</p><p><strong>执行动作：</strong>暂停所有策略。</p><p><strong>注意：</strong>留空表示关闭；计算包括不同模式和计价资产，不进行汇率换算。</p></div>} rules={[{
        validator: (_, value: string | null | undefined) => {
          if (!value) return Promise.resolve();
          if (!isPositiveDecimal(value)) return Promise.reject(new Error('请输入大于 0 的金额'));
          return exceeds(value, MAX_CONFIG_NOTIONAL_OR_PRICE) ? Promise.reject(new Error('全局最大持仓金额超出允许上限')) : Promise.resolve();
        },
      }]}><InputNumber stringMode min="0" className="full-width-input" /></Form.Item></Col>
      <Col span={24}><Form.Item name="pause_on_zero_available_margin" label="可用保证金不足时暂停" valuePropName="checked" extra={<div className="field-help"><p><strong>触发条件：</strong>开启后，任意已配置策略的可用保证金小于或等于零。</p><p><strong>执行动作：</strong>暂停所有策略、撤销机器人挂单并保留持仓。</p></div>}><Switch checkedChildren="开启" unCheckedChildren="关闭" /></Form.Item></Col>
      <Col span={24}><Form.Item name="min_available_margin_pct" label="可用保证金比例下限（%）" extra={<div className="field-help"><p><strong>触发条件：</strong>任意已配置策略的可用保证金占保证金余额的比例小于或等于该值。</p><p><strong>执行动作：</strong>暂停所有策略、撤销机器人挂单并保留持仓，处理方式与“可用保证金不足时暂停”相同。</p><p><strong>注意：</strong>留空表示关闭。</p></div>} rules={[{ validator: (_, value: string | null | undefined) => !value || isPercentDecimal(value) ? Promise.resolve() : Promise.reject(new Error('请输入大于 0 且小于 100 的百分比')) }]}><InputNumber stringMode min="0" max="100" className="full-width-input" /></Form.Item></Col>
    </Row>
  </Card>;
}

function TelegramSettingsCard({ form, hasBotToken, version, formDisabled, armed }: {
  form: FormInstance<GlobalForm>;
  hasBotToken: boolean | undefined;
  version: string;
  formDisabled: boolean;
  armed: boolean;
}) {
  const botToken = Form.useWatch('telegram_bot_token', form);
  const chatId = Form.useWatch('telegram_chat_id', form);
  const clearTelegram = !!Form.useWatch('clear_telegram_credentials', form);
  const credDisabled = formDisabled || clearTelegram;
  const invalidToken = Boolean((botToken && (!botToken.trim() || !validBotToken(botToken.trim()))) || (chatId?.trim() && !validChatId(chatId.trim())));
  return <Card title="Telegram 通知" className="settings-card settings-card-fill">
    <Row gutter={[16, 16]}>
      <Col span={24}><Form.Item name="telegram_enabled" label="通知开关" valuePropName="checked"><Switch checkedChildren="开启" unCheckedChildren="关闭" /></Form.Item></Col>
      <Col xs={24} sm={12}><Form.Item name="telegram_bot_token" label="Bot Token" extra={hasBotToken ? '已保存 Token，留空保持不变' : '尚未配置 Token'} rules={[
        ({ getFieldValue }) => ({
          validator: (_, value: string | undefined) => {
            if (getFieldValue('clear_telegram_credentials') || !value) return Promise.resolve();
            const trimmed = value.trim();
            return trimmed && validBotToken(trimmed) ? Promise.resolve() : Promise.reject(new Error('Telegram Bot Token 格式无效'));
          },
        }),
        ({ getFieldValue }) => ({
          validateTrigger: armed ? undefined : 'onSubmit',
          validator: (_, value: string | undefined) => {
            if (!getFieldValue('telegram_enabled') || getFieldValue('clear_telegram_credentials')) return Promise.resolve();
            return hasBotToken || Boolean(value?.trim()) ? Promise.resolve() : Promise.reject(new Error('请填写 Bot Token'));
          },
        }),
      ]}><Input.Password autoComplete="new-password" disabled={credDisabled} /></Form.Item></Col>
      <Col xs={24} sm={12}><Form.Item name="telegram_chat_id" label="Chat ID" extra="向机器人发送消息后，可通过 Telegram Bot API 的 getUpdates 获取会话 ID" rules={[
        ({ getFieldValue }) => ({
          validator: (_, value: string | undefined) => {
            const trimmed = value?.trim();
            if (getFieldValue('clear_telegram_credentials') || !trimmed) return Promise.resolve();
            return validChatId(trimmed) ? Promise.resolve() : Promise.reject(new Error('Telegram Chat ID 格式无效'));
          },
        }),
        ({ getFieldValue }) => ({
          validateTrigger: armed ? undefined : 'onSubmit',
          validator: (_, value: string | undefined) => {
            if (getFieldValue('clear_telegram_credentials') || !(getFieldValue('telegram_enabled') || Boolean(getFieldValue('telegram_bot_token')?.trim()))) return Promise.resolve();
            return value?.trim() ? Promise.resolve() : Promise.reject(new Error('请填写 Chat ID'));
          },
        }),
      ]}><Input className="numeric" disabled={credDisabled} /></Form.Item></Col>
    </Row>
    <div className="settings-card-footer">
      <ProbeButton label="发送测试消息" path="/api/global/test-telegram" disabled={credDisabled || invalidToken}
        resetKey={JSON.stringify([version, botToken ?? '', chatId ?? '', clearTelegram])}
        body={() => ({ bot_token: form.getFieldValue('telegram_bot_token'), chat_id: form.getFieldValue('telegram_chat_id') })} />
      <Form.Item name="clear_telegram_credentials" valuePropName="checked" className="settings-checkbox-item" rules={[({ getFieldValue }) => ({
        validator: (_, value: boolean | undefined) => value && getFieldValue('telegram_enabled') ? Promise.reject(new Error('请先关闭通知后再清除 Telegram 凭证')) : Promise.resolve(),
      })]}><Checkbox>清除 Telegram 凭证（请先关闭通知）</Checkbox></Form.Item>
    </div>
  </Card>;
}

export function GlobalSettings() {
  const [form] = Form.useForm<GlobalForm>(), { message } = App.useApp(), { confirm } = useConfirmDialog(), client = useQueryClient();
  const query = useQuery({ queryKey: ['global-config'], queryFn: ({ signal }) => api<GlobalConfig>('/api/global/config', { signal }), refetchOnWindowFocus: false });
  const [armedCross, setArmedCross] = useState({ api: false, telegram: false });
  const { edit, ready, baseline, mounted, submitting, fill } = useSettingsFormLifecycle(query.data, data => {
    setArmedCross({ api: false, telegram: false });
    form.setFieldsValue({ ...data, pause_on_zero_available_margin: data.pause_on_zero_available_margin ?? false, api_key: '', api_secret: '', telegram_bot_token: '', clear_api_credentials: false, clear_telegram_credentials: false });
  });
  const mutation = useMutation({
    mutationFn: async (v: GlobalForm) => {
      if (!baseline.current?.version) throw new Error('无法确认设置版本，请重新加载设置');
      return post<GlobalConfig>('/api/global/config', {
        baseline_version: baseline.current.version, recv_window: v.recv_window, sync_interval_secs: v.sync_interval_secs,
        global_max_position_notional: v.global_max_position_notional || null, pause_on_zero_available_margin: v.pause_on_zero_available_margin ?? false, min_available_margin_pct: v.min_available_margin_pct || null,
        api_key: v.clear_api_credentials ? '' : v.api_key?.trim() || null, api_secret: v.clear_api_credentials ? '' : v.api_secret?.trim() || null,
        telegram_bot_token: v.clear_telegram_credentials ? '' : v.telegram_bot_token?.trim() || null, telegram_chat_id: v.clear_telegram_credentials ? '' : v.telegram_chat_id?.trim() || '',
        telegram_enabled: !!v.telegram_enabled, clear_api_credentials: !!v.clear_api_credentials, clear_telegram_credentials: !!v.clear_telegram_credentials,
      });
    },
    onMutate: () => edit.setSaving(true),
    onSuccess: (data, values) => {
      client.setQueryData(['global-config'], data);
      if (mounted.current) { fill(data); edit.setDirty(false); }
      message.success('全局设置已保存，所有交易对立即生效');
      const credentialsChanged = Boolean(values.api_key?.trim() || values.api_secret?.trim() || values.clear_api_credentials);
      void invalidateStrategyQueries(client, { accounts: true, snapshot: true, history: credentialsChanged ? 'reset' : undefined });
    },
    onError: error => message.error(error.message),
    onSettled: () => { submitting.current = null; if (mounted.current) edit.setSaving(false); },
  });
  const submit = async (values: GlobalForm) => {
    if (submitting.current) return;
    const token = {};
    submitting.current = token;
    const changes = diffGlobalConfig(baseline.current, values);
    try {
      if (changes.length && !await confirm({ title: '确认保存全局设置？', content: <div className="confirm-changes"><p>以下变更保存后立即生效：</p><ul>{changes.map(change => <li key={change}>{change}</li>)}</ul></div>, okText: '确认保存', cancelText: '继续编辑' })) {
        if (submitting.current === token) submitting.current = null;
        return;
      }
      if (!mounted.current || submitting.current !== token) {
        if (submitting.current === token) submitting.current = null;
        return;
      }
      edit.setSaving(true); mutation.mutate(values);
    } catch (error) { if (submitting.current === token) submitting.current = null; throw error; }
  };
  const activeData = baseline.current ?? query.data;
  const formDisabled = !ready || edit.saving, version = activeData?.version ?? '';
  return <>
    <ErrorNotice error={query.error} retry={() => void query.refetch()} />
    <ErrorNotice error={mutation.error} />
    {isConfigConflict(mutation.error, '全局设置已被其他请求修改', '无法确认设置版本') && <ConflictReloadButton form={form} label="全局设置" activeRef={mounted} refetch={query.refetch} onReset={mutation.reset} onLoaded={fill} />}
    <Form form={form} layout="vertical" disabled={formDisabled} onValuesChange={changed => {
      submitting.current = null;
      const apiTouched = 'api_key' in changed || 'api_secret' in changed || 'clear_api_credentials' in changed;
      const telegramTouched = 'telegram_bot_token' in changed || 'telegram_chat_id' in changed || 'telegram_enabled' in changed || 'clear_telegram_credentials' in changed;
      const uncheckApi = 'clear_api_credentials' in changed && !changed.clear_api_credentials;
      const uncheckTelegram = 'clear_telegram_credentials' in changed && !changed.clear_telegram_credentials;
      const invalidUncheckedFormat = (name: keyof GlobalForm) => {
        const raw = form.getFieldValue(name) as string | undefined;
        if (name === 'api_key') return Boolean(raw && (!raw.trim() || !API_KEY_RE.test(raw.trim())));
        if (name === 'api_secret') return Boolean(raw?.trim() && !API_KEY_RE.test(raw.trim()));
        if (name === 'telegram_bot_token') return Boolean(raw && (!raw.trim() || !validBotToken(raw.trim())));
        if (name === 'telegram_chat_id') return Boolean(raw?.trim() && !validChatId(raw.trim()));
        return false;
      };
      const revalidate = CROSS_FIELDS.filter(name => {
        if (name in changed) return false;
        if (form.getFieldError(name).length > 0 || form.isFieldValidating(name)) return true;
        if ((name === 'api_key' || name === 'api_secret') && ((armedCross.api && apiTouched) || (uncheckApi && invalidUncheckedFormat(name)))) return true;
        if ((name === 'telegram_bot_token' || name === 'telegram_chat_id') && ((armedCross.telegram && telegramTouched) || (uncheckTelegram && invalidUncheckedFormat(name)))) return true;
        return name === 'clear_telegram_credentials' && 'telegram_enabled' in changed && Boolean(form.getFieldValue('clear_telegram_credentials'));
      });
      if (revalidate.length) void form.validateFields(revalidate).catch(() => {});
      edit.setDirty(true);
    }} onFinishFailed={({ errorFields }) => {
      const failed = new Set(errorFields.flatMap(f => f.errors.length > 0 ? [f.name[0] as keyof GlobalForm] : []));
      const apiErr = failed.has('api_key') || failed.has('api_secret'), tgErr = failed.has('telegram_bot_token') || failed.has('telegram_chat_id');
      if (apiErr || tgErr) setArmedCross(prev => ({ api: prev.api || apiErr, telegram: prev.telegram || tgErr }));
    }} onFinish={v => void submit(v)}>
      <ExchangeCredentialsCard form={form} data={activeData} version={version} formDisabled={formDisabled} armed={armedCross.api} />
      <Row gutter={[20, 20]} className="settings-grid-row">
        <Col xs={24} xl={12} className="settings-grid-col"><GlobalRiskCard /></Col>
        <Col xs={24} xl={12} className="settings-grid-col"><TelegramSettingsCard form={form} hasBotToken={activeData?.has_telegram_bot_token} version={version} formDisabled={formDisabled} armed={armedCross.telegram} /></Col>
      </Row>
      <div className={`save-bar${edit.dirty ? ' save-bar-dirty' : ''}`}><span className="muted" role="status">{edit.dirty ? '有未保存的修改' : '所有修改已保存'}</span><Space wrap><FormRevertButton form={form} activeRef={mounted} onReset={mutation.reset} onRevert={() => { submitting.current = null; setArmedCross({ api: false, telegram: false }); const source = baseline.current ?? query.data; if (source) fill(source); }} /><Button type="primary" htmlType="submit" loading={mutation.isPending}>保存全局设置</Button></Space></div>
    </Form>
  </>;
}
