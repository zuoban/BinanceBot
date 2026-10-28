import { useContext, useEffect, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { Alert, App, Button, Card, Checkbox, Col, Form, Input, InputNumber, Row, Select, Space, Switch } from 'antd';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, pairPath, post } from '../lib/api';
import { usePairs } from '../lib/queries';
import { EditContext } from '../lib/session';
import type { GridConfig } from '../lib/types';
import { ErrorNotice, PageTitle } from '../components/Common';

type FormValues = Omit<GridConfig, 'dry_run' | 'is_testnet'> & { mode: 'PAPER' | 'TESTNET' | 'LIVE' };
const defaults: FormValues = { symbol: '', direction: 'LONG', grid_interval: '0.1', order_amount_usdc: '100', buy_window: 5, sell_window: 5, post_only: true, mode: 'PAPER', min_price: null, max_price: null, max_position_usdc: '2000', pause_on_max_position: false };
function toForm(c: GridConfig): FormValues {
  return { ...c, mode: c.dry_run ? 'PAPER' : c.is_testnet ? 'TESTNET' : 'LIVE' };
}
export default function PairConfig() {
  const { symbol } = useParams();
  const adding = !symbol;
  const navigate = useNavigate();
  const pairs = usePairs();
  const pair = pairs.data?.find(p => p.symbol === symbol);
  const [form] = Form.useForm<FormValues>();
  const edit = useContext(EditContext);
  const client = useQueryClient();
  const { message, modal } = App.useApp();
  const [ready, setReady] = useState(adding);
  const selectedMode = Form.useWatch('mode', form);
  const config = useQuery({ queryKey: ['config', symbol, pair?.scoped], queryFn: ({ signal }) => api<GridConfig>(pairPath(pair!, 'config'), { signal }), enabled: !!pair, refetchOnWindowFocus: false });
  useEffect(() => {
    setReady(adding);
    form.resetFields();
    if (adding) form.setFieldsValue(defaults);
  }, [symbol, adding, form]);
  useEffect(() => {
    if (config.data && !ready && !adding) { form.setFieldsValue(toForm(config.data)); setReady(true); }
  }, [config.data, ready, adding, form]);
  useEffect(() => () => { edit.setDirty(false); edit.setSaving(false); }, [edit.setDirty, edit.setSaving]);
  const mutation = useMutation({
    mutationFn: (values: FormValues) => {
      const { mode, ...fields } = values;
      return post<GridConfig | string>(adding ? '/api/pairs' : pairPath(pair!, 'config'), {
        ...fields, symbol: fields.symbol.trim().toUpperCase(),
        dry_run: mode === 'PAPER', is_testnet: mode === 'TESTNET',
        min_price: fields.min_price || null, max_price: fields.max_price || null, max_position_usdc: fields.max_position_usdc || null,
      });
    },
    onMutate: () => edit.setSaving(true),
    onSuccess: async (data, values) => {
      edit.setDirty(false);
      edit.setSaving(false);
      if (!adding && typeof data !== 'string') form.setFieldsValue(toForm(data));
      message.success(adding ? '交易对已添加，策略保持暂停，请检查配置后手动恢复' : '策略配置已保存并生效');
      await Promise.all([client.invalidateQueries({ queryKey: ['pairs'] }), client.invalidateQueries({ queryKey: ['overview'] }), client.invalidateQueries({ queryKey: ['config', symbol] })]);
      if (adding) navigate(`/pairs/${typeof data === 'string' ? data : values.symbol.toUpperCase()}`, { replace: true });
    },
    onError: error => message.error(error.message),
    onSettled: () => edit.setSaving(false),
  });
  const remove = async () => {
    if (!pair) return;
    if (edit.dirty && !await modal.confirm({ title: '放弃未保存的修改？', content: '移除交易对会放弃当前表单修改。', okText: '继续', cancelText: '取消' })) return;
    if (!await modal.confirm({ title: `移除 ${symbol}？`, content: '请先暂停策略并等待挂单撤销。已有持仓、成交历史和恢复数据会保留；至少需要保留一个交易对。', okText: '确认移除', okButtonProps: { danger: true }, cancelText: '取消' })) return;
    edit.setSaving(true);
    try {
      await api(`/api/pairs/${encodeURIComponent(pair.symbol)}`, { method: 'DELETE', body: JSON.stringify({ confirm: true }) });
      edit.setDirty(false);
      edit.setSaving(false);
      await Promise.all([client.invalidateQueries({ queryKey: ['pairs'] }), client.invalidateQueries({ queryKey: ['overview'] })]);
      message.success('交易对已移除，持仓和历史记录保留'); navigate('/');
    } catch (error) { message.error((error as Error).message); }
    finally { edit.setSaving(false); }
  };
  const submit = async (values: FormValues) => {
    if (values.mode === 'LIVE' && !await modal.confirm({ title: '保存实盘配置？', content: '请核对交易对、方向、网格和持仓限制。实盘使用真实资金。', okText: '确认保存', cancelText: '取消' })) return;
    mutation.mutate(values);
  };
  return <>
    <PageTitle eyebrow="WORKSPACE / CONFIGURATION" title={adding ? '新增交易对' : `${symbol} 策略参数`} description={adding ? '新策略创建后保持暂停，检查参数后手动恢复。' : '参数保存后立即生效，请先核对策略设置。'} />
    <ErrorNotice error={pairs.error || config.error || mutation.error} retry={() => { if (!adding) void config.refetch(); }} />
    {pairs.data && !pair && !adding && <Alert type="warning" title="该交易对不存在或已移除" />}
    <Form<FormValues> className="config-form" form={form} layout="vertical" onFinish={values => void submit(values)} onValuesChange={() => edit.setDirty(true)} disabled={!ready || edit.saving}>
      <Card title="基本设置" extra={<span className="muted">STRATEGY / 01</span>}>
        <Row gutter={20}>
          <Col xs={24} md={12}><Form.Item name="symbol" label="合约交易对" rules={[{ required: true, message: '请输入交易对' }, { pattern: /^[a-zA-Z0-9]+$/, message: '仅支持字母和数字' }]}><Input disabled={!adding || edit.saving} placeholder="例如 BTCUSDT" autoComplete="off" /></Form.Item></Col>
          <Col xs={24} md={12}><Form.Item name="direction" label="交易方向" rules={[{ required: true }]}><Select options={[{ value: 'LONG', label: '做多 · 买入开仓 / 卖出平仓' }, { value: 'SHORT', label: '做空 · 卖出开仓 / 买入平仓' }]} /></Form.Item></Col>
          <Col xs={24} md={12}><Form.Item name="mode" label="运行模式" rules={[{ required: true }]} extra="模拟盘使用真实行情进行本地撮合。"><Select options={[{ value: 'PAPER', label: '模拟盘 / Paper Trading' }, { value: 'TESTNET', label: '测试网 / Testnet' }, { value: 'LIVE', label: '实盘 / Live' }]} /></Form.Item></Col>
        </Row>
        {selectedMode === 'LIVE' && <Alert type="warning" showIcon title="实盘使用真实资金，请核对交易方向和风险限制。" />}
      </Card>
      <Card title="网格参数" className="form-card" extra={<span className="muted">STRATEGY / 02</span>}>
        <Row gutter={20}>
          <Col xs={24} md={12}><Form.Item name="grid_interval" label="网格间距" extra="相邻网格价格的固定差值，单位为报价资产。" rules={[{ required: true, message: '请输入网格间距' }]}><InputNumber stringMode min="0.0000000001" style={{ width: '100%' }} /></Form.Item></Col>
          <Col xs={24} md={12}><Form.Item name="order_amount_usdc" label="每格金额（报价资产）" rules={[{ required: true, message: '请输入每格金额' }]}><InputNumber stringMode min="0.0000000001" style={{ width: '100%' }} /></Form.Item></Col>
          <Col xs={24} md={12}><Form.Item name="buy_window" label="买单窗口" extra="当前价格下方预挂的买单数量，范围为 1–50。" rules={[{ required: true }]}><InputNumber min={1} max={50} precision={0} style={{ width: '100%' }} /></Form.Item></Col>
          <Col xs={24} md={12}><Form.Item name="sell_window" label="卖单窗口" extra="当前价格上方预挂的卖单数量，范围为 1–50。" rules={[{ required: true }]}><InputNumber min={1} max={50} precision={0} style={{ width: '100%' }} /></Form.Item></Col>
        </Row>
      </Card>
      <Card title="仓位限制" className="form-card" extra={<span className="muted">STRATEGY / 03</span>}><Row gutter={20}>
          <Col xs={24} md={12}><Form.Item name="max_position_usdc" label="最大持仓名义价值" extra="留空表示不设置。"><InputNumber stringMode min="0" style={{ width: '100%' }} /></Form.Item></Col>
          <Col xs={24} md={12}><Form.Item name="pause_on_max_position" label="达到持仓上限时" extra="限制开仓会保留平仓订单；暂停策略会撤销机器人挂单。" valuePropName="checked"><Switch checkedChildren="暂停策略" unCheckedChildren="限制开仓" /></Form.Item></Col>
        </Row>
      </Card>
      <Card title="价格范围与订单" className="form-card" extra={<span className="muted">STRATEGY / 04</span>}><Row gutter={20}>
          <Col xs={24} md={12}><Form.Item name="min_price" label="最低价格（可选）"><InputNumber stringMode min="0" style={{ width: '100%' }} /></Form.Item></Col>
          <Col xs={24} md={12}><Form.Item name="max_price" label="最高价格（可选）"><InputNumber stringMode min="0" style={{ width: '100%' }} /></Form.Item></Col>
          <Col span={24}><Form.Item name="post_only" valuePropName="checked"><Checkbox>仅挂单（Post Only）</Checkbox></Form.Item></Col>
        </Row>
      </Card>
      <div className="save-bar"><span className="muted">{edit.dirty ? '有未保存的修改' : '所有修改已保存'}</span><Space wrap>
        <Button onClick={async () => { if (edit.dirty && !await modal.confirm({ title: '放弃未保存的修改？', okText: '还原', cancelText: '继续编辑' })) return; if (adding) form.setFieldsValue(defaults); else if (config.data) form.setFieldsValue(toForm(config.data)); edit.setDirty(false); }}>还原</Button>
        <Button type="primary" htmlType="submit" loading={mutation.isPending}>{adding ? '保存并添加' : '保存并立即生效'}</Button>
      </Space></div>
      {!adding && <Card className="danger-zone" title="移除交易对"><p>移除前请暂停策略并等待挂单撤销；已有持仓和历史记录会保留。</p><Button danger disabled={edit.saving} onClick={() => void remove()}>移除交易对</Button></Card>}
    </Form>
  </>;
}
