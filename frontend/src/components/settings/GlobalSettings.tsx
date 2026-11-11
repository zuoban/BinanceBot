import { flushSync } from 'react-dom';
import { Alert, App, Button, Card, Checkbox, Col, Form, Input, InputNumber, Row, Space, Switch } from 'antd';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, post } from '../../lib/api';
import { diffGlobalConfig } from '../../lib/configDiff';
import { isPercentDecimal, isPositiveDecimal } from '../../lib/gridPreview';
import type { GlobalConfig } from '../../lib/types';
import { ConflictReloadButton, ErrorNotice, FormRevertButton, isConfigConflict } from '../Common';
import { ProbeButton, useSettingsFormLifecycle, type GlobalForm } from './settingsShared';

export function GlobalSettings() {
  const [form] = Form.useForm<GlobalForm>();
  const { message, modal } = App.useApp();
  const client = useQueryClient();
  const query = useQuery({ queryKey: ['global-config'], queryFn: ({ signal }) => api<GlobalConfig>('/api/global/config', { signal }), refetchOnWindowFocus: false });
  const { edit, ready, baseline, mounted, submitting, fill } = useSettingsFormLifecycle(query.data, data => {
    form.setFieldsValue({ ...data, pause_on_zero_available_margin: data.pause_on_zero_available_margin ?? false, api_key: '', api_secret: '', telegram_bot_token: '', clear_api_credentials: false, clear_telegram_credentials: false });
  });
  const mutation = useMutation({
    mutationFn: async (v: GlobalForm) => {
      if (!baseline.current?.version) throw new Error('无法确认设置版本，请重新加载设置');
      return post<GlobalConfig>('/api/global/config', {
        baseline_version: baseline.current.version, recv_window: v.recv_window, sync_interval_secs: v.sync_interval_secs,
        global_max_position_notional: v.global_max_position_notional || null,
        pause_on_zero_available_margin: v.pause_on_zero_available_margin ?? false,
        min_available_margin_pct: v.min_available_margin_pct || null,
        api_key: v.clear_api_credentials ? '' : v.api_key?.trim() || null, api_secret: v.clear_api_credentials ? '' : v.api_secret?.trim() || null,
        telegram_bot_token: v.clear_telegram_credentials ? '' : v.telegram_bot_token?.trim() || null, telegram_chat_id: v.clear_telegram_credentials ? '' : v.telegram_chat_id?.trim() || '',
        telegram_enabled: !!v.telegram_enabled, clear_api_credentials: !!v.clear_api_credentials, clear_telegram_credentials: !!v.clear_telegram_credentials,
      });
    },
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
    {(isConfigConflict(mutation.error, '全局设置已被其他请求修改') || isConfigConflict(mutation.error, '无法确认设置版本')) && <ConflictReloadButton label="全局设置" activeRef={mounted} refetch={query.refetch} onReset={mutation.reset} onLoaded={fill} />}
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
