import { Alert, App, Button, Card, Col, Form, Input, InputNumber, Row, Space } from 'antd';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, post } from '../../lib/api';
import type { ServerConfig } from '../../lib/types';
import { ConflictReloadButton, ErrorNotice, FormRevertButton, isConfigConflict } from '../Common';
import { useSettingsFormLifecycle } from './settingsShared';

export function ServerSettings() {
  const [form] = Form.useForm<ServerConfig['saved']>();
  const { message } = App.useApp();
  const client = useQueryClient();
  const query = useQuery({ queryKey: ['server-config'], queryFn: ({ signal }) => api<ServerConfig>('/api/server/config', { signal }), refetchOnWindowFocus: false });
  const { edit, ready, baseline, mounted, submitting, fill } = useSettingsFormLifecycle(query.data, data => {
    form.setFieldsValue(data.saved);
  });
  const mutation = useMutation({
    mutationFn: async (values: ServerConfig['saved']) => {
      if (!baseline.current?.version) throw new Error('无法确认设置版本，请重新加载设置');
      return post<ServerConfig>('/api/server/config', { ...values, host: values.host.trim(), baseline_version: baseline.current.version });
    },
    onMutate: () => edit.setSaving(true),
    onSuccess: data => {
      client.setQueryData(['server-config'], data);
      if (mounted.current) {
        fill(data);
        edit.setDirty(false);
      }
      message.success('服务设置已保存，重启后生效');
    },
    onError: error => message.error(error.message),
    onSettled: () => {
      submitting.current = false;
      if (mounted.current) edit.setSaving(false);
    },
  });
  return <><ErrorNotice error={query.error} retry={() => void query.refetch()} /><ErrorNotice error={mutation.error} />
    {isConfigConflict(mutation.error, '服务设置已被其他请求修改', '无法确认设置版本') && <ConflictReloadButton label="服务设置" activeRef={mounted} refetch={query.refetch} onReset={mutation.reset} onLoaded={fill} />}
    <Card title="监听设置" className="settings-card"><Alert type="info" showIcon title={`当前监听：${query.data?.active.host ?? '--'}:${query.data?.active.port ?? '--'}。修改后重启生效；使用 Docker 时请同时调整代理配置。`} />
      <Form form={form} layout="vertical" className="settings-fields" disabled={!ready || edit.saving} onValuesChange={() => edit.setDirty(true)} onFinish={v => { if (submitting.current) return; submitting.current = true; mutation.mutate(v); }}>
        <Row gutter={[20, 16]}><Col xs={24} md={12}><Form.Item name="host" label="监听地址" rules={[{ required: true, whitespace: true }]}><Input className="numeric" /></Form.Item></Col>
          <Col xs={24} md={12}><Form.Item name="port" label="端口" rules={[{ required: true }]}><InputNumber min={1} max={65535} precision={0} className="full-width-input" /></Form.Item></Col></Row>
        <div className={`settings-card-footer settings-card-actions${edit.dirty ? ' settings-card-actions-dirty' : ''}`}>
          <span className="muted" role="status">{edit.dirty ? '有未保存的修改' : '修改后需重启后端生效'}</span>
          <Space wrap>
            <FormRevertButton form={form} activeRef={mounted} onReset={mutation.reset} onRevert={() => { if (baseline.current) fill(baseline.current); }} />
            <Button type="primary" htmlType="submit" loading={mutation.isPending}>保存监听设置</Button>
          </Space>
        </div>
      </Form>
    </Card>
  </>;
}
