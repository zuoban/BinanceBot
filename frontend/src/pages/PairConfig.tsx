import { useContext, useEffect, useLayoutEffect, useRef, useState } from 'react';
import { flushSync } from 'react-dom';
import { UNSAFE_DataRouterContext, useLocation, useNavigate, useParams } from 'react-router-dom';
import { Alert, App, Button, Card, Col, Form, Input, InputNumber, Row, Select, Space, Switch, type FormInstance } from 'antd';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, pairPath, post } from '../lib/api';
import { usePairs } from '../lib/queries';
import { AccountsContext, EditContext } from '../lib/session';
import type { GlobalConfig, GridConfig } from '../lib/types';
import { ConflictReloadButton, ErrorNotice, FormRevertButton, isConfigConflict, PageBreadcrumb, PageTitle } from '../components/Common';
import { useDeleteStrategy } from '../components/DeleteStrategy';
import { GridPreview } from '../components/GridPreview';
import { defaultGridInterval, isPercentDecimal, isPositiveDecimal, orderAmountError, priceRangeError, tickSizeError } from '../lib/gridPreview';
import { positiveDecimalText, quoteAsset } from '../lib/format';
import { describeNewGridConfig, diffGridConfig, type FieldChange } from '../lib/configDiff';

type FormValues = Omit<GridConfig, 'post_only'>;
const defaults: FormValues = { symbol: '', direction: 'LONG', grid_interval: '0.1', order_amount_usdc: '100', buy_window: 3, sell_window: 3, min_price: null, max_price: null, max_position_usdc: '1000', pause_on_max_position: true, cancel_opening_on_max_position: false, min_liquidation_distance_pct: null };
function LiveConfigConfirmContent({ changes, values }: { changes: FieldChange[] | null; values: FormValues }) {
  const items = changes ?? describeNewGridConfig(values).map(item => ({ key: item.label, label: item.label, before: '', after: item.value, risky: item.risky }));
  return <div className="confirm-changes">
    <p>实盘使用真实资金，请核对以下{changes ? '修改' : '参数'}。</p>
    {changes?.length === 0 && <p className="muted">未检测到参数修改，将按当前参数重新保存。</p>}
    <dl>
      {items.map(item => <div key={item.key} className={item.risky ? 'confirm-risky' : undefined}>
        <dt>{item.label}</dt>
        <dd>{item.before ? <><s>{item.before}</s> → <strong>{item.after}</strong></> : <strong>{item.after}</strong>}</dd>
      </div>)}
    </dl>
  </div>;
}
function PairRiskAndRangeSection() {
  return <Row gutter={[20, 20]} className="config-risk-row">
    <Col xs={24} xl={14} className="config-risk-col">
      <Card title="仓位限制">
        <Row gutter={[20, 16]}>
          <Col span={24}><Form.Item name="max_position_usdc" label="最大持仓名义价值" extra={<div className="field-help"><p><strong>注意：</strong>留空表示不设置；选择暂停策略或仅撤销开仓单时须填写大于 0 的上限。</p></div>} dependencies={['pause_on_max_position', 'cancel_opening_on_max_position']} rules={[({ getFieldValue }) => ({ validator: (_, value) => !(getFieldValue('pause_on_max_position') || getFieldValue('cancel_opening_on_max_position')) || isPositiveDecimal(value) ? Promise.resolve() : Promise.reject(new Error(getFieldValue('pause_on_max_position') ? '暂停策略需要大于 0 的持仓上限' : '撤销开仓单需要大于 0 的持仓上限')) })]}><InputNumber stringMode min="0" className="full-width-input" /></Form.Item></Col>
          <Col span={24}><Form.Item name="pause_on_max_position" label="持仓上限处理方式" extra={<div className="field-help"><p><strong>触发条件：</strong>选择暂停策略时，当前持仓名义价值达到上限，或下一笔完整开仓单成交后预计超过上限。</p><p><strong>执行动作：</strong>暂停策略并尝试撤销机器人挂单；限制开仓会保留平仓订单。</p><p><strong>恢复：</strong>暂停后需手动恢复。</p><p><strong>注意：</strong>撤单可能失败，请检查交易所挂单。仅挂单占用额度不会触发暂停。</p></div>} valuePropName="checked"><Switch checkedChildren="暂停策略" unCheckedChildren="限制开仓" /></Form.Item></Col>
          <Col span={24}><Form.Item name="cancel_opening_on_max_position" label="达到上限时仅撤开仓单" extra={<div className="field-help"><p><strong>触发条件：</strong>开启后，持仓名义价值达到上限。</p><p><strong>执行动作：</strong>只撤销开仓方向的挂单，策略保持运行并继续维护平仓单。</p><p><strong>恢复：</strong>回落到上限以下后自动恢复开仓。</p><p><strong>注意：</strong>与「暂停策略」互斥，开启一项会关闭另一项。</p></div>} valuePropName="checked"><Switch checkedChildren="撤开仓单" unCheckedChildren="关闭" /></Form.Item></Col>
          <Col span={24}><Form.Item name="min_liquidation_distance_pct" label="强平距离阈值（%）" extra={<div className="field-help"><p><strong>触发条件：</strong>持仓的标记价格距强平价不超过该百分比（占标记价）时。</p><p><strong>执行动作：</strong>只撤销开仓方向的挂单并保持运行，继续维护平仓单。</p><p><strong>恢复：</strong>距离回升后自动恢复开仓。</p><p><strong>注意：</strong>留空表示不启用。依赖交易所返回的强平价，模拟盘或无强平价时不生效。</p></div>} rules={[{ validator: (_, value) => value == null || value === '' || isPercentDecimal(value) ? Promise.resolve() : Promise.reject(new Error('请输入大于 0 且小于 100 的百分比')) }]}><InputNumber stringMode min="0" max="100" className="full-width-input" /></Form.Item></Col>
        </Row>
      </Card>
    </Col>
    <Col xs={24} xl={10} className="config-risk-col">
      <Card title="价格范围与订单">
        <Row gutter={[20, 16]}>
          <Col span={24}><Form.Item name="min_price" label="最低价格（可选）" dependencies={['max_price']} rules={[({ getFieldValue }) => ({ validator: (_, value) => { const error = priceRangeError('min', value, getFieldValue('max_price')); return error ? Promise.reject(new Error(error)) : Promise.resolve(); } })]}><InputNumber stringMode min="0" className="full-width-input" /></Form.Item></Col>
          <Col span={24}><Form.Item name="max_price" label="最高价格（可选）" dependencies={['min_price']} rules={[({ getFieldValue }) => ({ validator: (_, value) => { const error = priceRangeError('max', getFieldValue('min_price'), value); return error ? Promise.reject(new Error(error)) : Promise.resolve(); } })]}><InputNumber stringMode min="0" className="full-width-input" /></Form.Item></Col>
        </Row>
        <div className="settings-card-footer config-card-note"><p className="muted">订单仅作为挂单成交（Post Only）；如果订单会立即成交，交易所会拒绝该订单。</p></div>
      </Card>
    </Col>
  </Row>;
}
export default function PairConfig() {
  const { symbol } = useParams();
  const adding = !symbol;
  const navigate = useNavigate();
  const { pathname } = useLocation();
  const router = useContext(UNSAFE_DataRouterContext)?.router;
  const pairs = usePairs();
  const accounts = useContext(AccountsContext);
  const pair = pairs.data?.find(p => p.symbol === symbol);
  const markPrice = accounts.find(entry => entry.snapshot.symbol === symbol && entry.mode === pair?.mode && !entry.account_observation_stale)?.snapshot.ticker.mark_price;
  const [form] = Form.useForm<FormValues>();
  const edit = useContext(EditContext);
  const client = useQueryClient();
  const { message, modal } = App.useApp();
  const [ready, setReady] = useState(adding);
  const deletion = useDeleteStrategy();
  const removing = useRef(false);
  const submission = useRef<object | null>(null);
  const page = useRef<object | null>(null);
  useLayoutEffect(() => {
    const current = {};
    page.current = current;
    const invalidate = () => { if (page.current === current) { page.current = null; submission.current = null; } };
    // Router state can change before React commits the new route and runs cleanup.
    const unsubscribe = router?.subscribe(state => { if (state.location.pathname !== pathname) invalidate(); });
    if (router && router.state.location.pathname !== pathname) invalidate();
    return () => { unsubscribe?.(); invalidate(); };
  }, [pathname, router]);
  const baseline = useRef<{ symbol: string; config: GridConfig & { config_version: string } } | null>(null);
  const customInterval = useRef(false);
  const appliedSymbol = useRef<string | undefined>(undefined);
  const resetMutation = useRef(() => {});
  const global = useQuery({ queryKey: ['global-config'], queryFn: ({ signal }) => api<GlobalConfig>('/api/global/config', { signal }), refetchOnWindowFocus: false });
  // Unknown state (for example a failed global-config request) is confirmed as live.
  const live = global.data ? global.data.has_api_key && global.data.has_api_secret : pair ? pair.mode === 'LIVE' : true;
  const symbols = useQuery({ queryKey: ['exchange-symbols'], queryFn: ({ signal }) => api<string[]>('/api/exchange/symbols?environment=mainnet', { signal }), enabled: adding, retry: false, refetchOnWindowFocus: false });
  const config = useQuery({ queryKey: ['config', symbol, pair?.scoped], queryFn: ({ signal }) => api<GridConfig & { config_version: string; tick_size?: string; step_size?: string; min_qty?: string; min_notional?: string }>(pairPath(pair!, 'config'), { signal }), enabled: !!pair, refetchOnWindowFocus: false });
  useEffect(() => {
    setReady(adding);
    baseline.current = null;
    customInterval.current = false;
    appliedSymbol.current = undefined;
    resetMutation.current();
    edit.setDirty(false);
    edit.setSaving(false);
    form.resetFields();
    if (adding) form.setFieldsValue(defaults);
  }, [symbol, adding, form, edit.setDirty, edit.setSaving]);
  useEffect(() => {
    if (config.data && config.data.symbol === symbol && !ready && !adding && symbol) { baseline.current = { symbol, config: config.data }; form.setFieldsValue(config.data); setReady(true); }
  }, [config.data, ready, adding, form, symbol]);
  useEffect(() => () => { edit.setDirty(false); edit.setSaving(false); }, [edit.setDirty, edit.setSaving]);
  const mutation = useMutation({
    mutationFn: (values: FormValues) => {
      if (!adding && (typeof baseline.current?.config.config_version !== 'string' || !baseline.current.config.config_version || baseline.current.symbol !== symbol)) throw new Error('无法确认配置版本，请重新加载策略配置');
      return post<GridConfig | string>(adding ? '/api/pairs' : pairPath(pair!, 'config'), {
        ...values, ...(!adding && { baseline_version: baseline.current?.config.config_version }), symbol: values.symbol.trim().toUpperCase(),
        post_only: true,
        min_price: values.min_price || null, max_price: values.max_price || null, max_position_usdc: values.max_position_usdc || null, min_liquidation_distance_pct: values.min_liquidation_distance_pct || null,
      });
    },
    onMutate: () => edit.setSaving(true),
    onSuccess: (data, values) => {
      flushSync(() => { edit.setDirty(false); edit.setSaving(false); });
      if (!adding && typeof data !== 'string') form.setFieldsValue(data);
      message.success(adding ? '交易对已添加，策略保持暂停，请检查配置后手动恢复' : '策略配置已保存并生效');
      const targetSymbol = adding ? (typeof data === 'string' ? data : values.symbol.trim().toUpperCase()) : symbol;
      void client.invalidateQueries({ queryKey: ['snapshot', targetSymbol] });
      navigate(`/pairs/${targetSymbol}`, { replace: true });
      void client.invalidateQueries({ queryKey: ['pairs'] });
      void client.invalidateQueries({ queryKey: ['overview'] });
      if (adding) void client.resetQueries({ queryKey: ['overview-history'] });
      if (!adding) void client.invalidateQueries({ queryKey: ['config', symbol] });
    },
    onError: error => message.error(error.message),
    onSettled: () => edit.setSaving(false),
  });
  resetMutation.current = mutation.reset;
  const remove = async () => {
    if (removing.current || submission.current || !symbol || !pair || !page.current) return;
    const currentPage = page.current;
    removing.current = true;
    edit.setSaving(true);
    let removed = false;
    try {
      removed = await deletion.remove(
        { ...pair, mode: live ? 'LIVE' : pair.mode },
        async () => (!edit.dirty || await modal.confirm({ title: '放弃未保存的修改？', content: '删除策略会放弃当前表单修改。', okText: '继续', cancelText: '取消' })) && page.current === currentPage,
      );
    } finally {
      removing.current = false;
      if (page.current === currentPage) {
        // Update the shell's blocker before navigating away from a deleted strategy.
        flushSync(() => {
          if (removed) edit.setDirty(false);
          edit.setSaving(false);
        });
      }
    }
    if (removed && page.current === currentPage) navigate('/');
  };
  const submit = async (values: FormValues) => {
    if (removing.current || submission.current || !page.current) return;
    const currentPage = page.current;
    const currentSubmission = {};
    submission.current = currentSubmission;
    try {
      const latest = await global.refetch();
      if (page.current !== currentPage || submission.current !== currentSubmission) return;
      const currentLive = latest.data ? latest.data.has_api_key && latest.data.has_api_secret : pair ? pair.mode === 'LIVE' : true;
      const changes = adding || !baseline.current ? null : diffGridConfig(baseline.current.config, values);
      if (currentLive && !await modal.confirm({
        title: adding ? '添加实盘交易对？' : '保存实盘配置？',
        content: <LiveConfigConfirmContent changes={changes} values={values} />,
        okText: '确认保存', cancelText: '取消',
      })) return;
      if (page.current !== currentPage || submission.current !== currentSubmission) return;
      await mutation.mutateAsync(values).catch(() => { /* Mutation errors are reported by onError. */ });
    } finally {
      if (submission.current === currentSubmission) submission.current = null;
    }
  };
  const formDisabled = !ready || edit.saving || deletion.busy;
  const watchedSymbol = Form.useWatch('symbol', form) as string | undefined;
  const selectedSymbol = adding ? watchedSymbol?.trim().toUpperCase() : symbol;
  const quote = quoteAsset(selectedSymbol ?? '');
  const symbolPreview = useQuery({ queryKey: ['symbol-preview', selectedSymbol], queryFn: ({ signal }) => api<{ symbol: string; last_price?: string | null; mark_price?: string | null; tick_size?: string; step_size?: string; min_qty?: string; min_notional?: string }>(`/api/exchange/symbol-preview?environment=mainnet&symbol=${encodeURIComponent(selectedSymbol!)}`, { signal }), enabled: adding && !!selectedSymbol, retry: false, refetchOnWindowFocus: false });
  const previewLast = positiveDecimalText(symbolPreview.data?.last_price);
  const previewMark = positiveDecimalText(symbolPreview.data?.mark_price);
  const latestPrice = adding ? (previewLast ?? previewMark) : null;
  const effectiveMark = (adding ? (previewMark ?? previewLast) : positiveDecimalText(markPrice)) ?? undefined;
  const tickSize = adding ? symbolPreview.data?.tick_size : config.data?.tick_size;
  const stepSize = adding ? symbolPreview.data?.step_size : config.data?.step_size;
  const minQty = adding ? symbolPreview.data?.min_qty : config.data?.min_qty;
  const minNotional = adding ? symbolPreview.data?.min_notional : config.data?.min_notional;
  useEffect(() => {
    if (!adding || !ready || edit.saving || submission.current || !selectedSymbol || !effectiveMark) return;
    if (customInterval.current || appliedSymbol.current === selectedSymbol) return;
    const nextInterval = defaultGridInterval(effectiveMark, tickSize);
    if (nextInterval) {
      appliedSymbol.current = selectedSymbol;
      form.setFieldValue('grid_interval', nextInterval);
      void form.validateFields(['grid_interval', 'order_amount_usdc']).catch(() => {});
    }
  }, [adding, ready, edit.saving, selectedSymbol, effectiveMark, tickSize, form]);
  useEffect(() => { if ((tickSize || stepSize || minQty || minNotional) && ready) void form.validateFields(['grid_interval', 'order_amount_usdc']).catch(() => {}); }, [tickSize, stepSize, minQty, minNotional, effectiveMark, ready, form]);
  return <div className="config-page">
    <PageBreadcrumb disabled={edit.saving} items={adding
      ? [{ label: '策略总览', to: '/' }, { label: '新增交易对' }]
      : [{ label: '策略总览', to: '/' }, { label: symbol, to: `/pairs/${symbol}` }, { label: '策略参数' }]} />
    <PageTitle title={adding ? '新增交易对' : `${symbol} 策略参数`} description={adding ? '新策略创建后保持暂停，检查参数后手动恢复' : '参数保存后立即生效，请先核对策略设置'} />
    <ErrorNotice error={pairs.error || config.error} retry={() => { if (pairs.error) void pairs.refetch(); if (!adding && pair) void config.refetch(); }} />
    <ErrorNotice error={mutation.error} />
    {!adding && isConfigConflict(mutation.error, '无法确认配置版本') && <ConflictReloadButton label="策略配置" activeRef={page} refetch={config.refetch} onReset={mutation.reset} onLoaded={data => {
      if (!symbol || data.symbol !== symbol) return false;
      baseline.current = { symbol, config: data };
      form.setFieldsValue(data);
    }} />}
    {pairs.data && !pair && !adding && <Alert type="warning" title="该交易对不存在或已移除" />}
    <Form<FormValues>
      className="config-form"
      form={form}
      layout="vertical"
      onFinish={values => void submit(values)}
      onValuesChange={changed => {
        if ('symbol' in changed) {
          form.setFieldValue('grid_interval', defaults.grid_interval);
          customInterval.current = false;
          appliedSymbol.current = undefined;
        } else if ('grid_interval' in changed) {
          customInterval.current = true;
        }
        if (changed.pause_on_max_position) {
          form.setFieldValue('cancel_opening_on_max_position', false);
          void form.validateFields(['max_position_usdc']).catch(() => {});
        } else if (changed.cancel_opening_on_max_position) {
          form.setFieldValue('pause_on_max_position', false);
          void form.validateFields(['max_position_usdc']).catch(() => {});
        }
        edit.setDirty(true);
      }}
      disabled={formDisabled}
    >
      <Card title="基本设置">
        {live && <Alert type="warning" showIcon className="config-live-alert" title="实盘使用真实资金，请核对交易方向和风险限制。" />}
        <Row gutter={[20, 16]}>
          <Col xs={24} md={12}>
            <Form.Item
              name="symbol"
              label="合约交易对"
              rules={[{ required: true, message: '请选择交易对' }]}
              getValueProps={value => ({ value: value || undefined })}
              extra={
                adding && symbols.isError ? <span>交易对加载失败 <Button type="link" onClick={() => void symbols.refetch()}>重试</Button></span>
                : adding && selectedSymbol ? (symbolPreview.isFetching ? '正在获取最新价格...' : latestPrice ? `最新价格：${latestPrice}${quote ? ` ${quote}` : ''}` : undefined)
                : undefined
              }
            >
              {adding ? (
                <Select
                  className="symbol-field"
                  showSearch
                  optionFilterProp="label"
                  placeholder={symbols.isFetching ? '正在加载交易对...' : '搜索并选择交易对'}
                  loading={symbols.isFetching}
                  disabled={formDisabled || symbols.isFetching || symbols.isError}
                  notFoundContent="没有匹配的交易对"
                  options={(symbols.data ?? []).map(value => ({ value, label: value }))}
                />
              ) : (
                <Input className="symbol-field" disabled />
              )}
            </Form.Item>
          </Col>
          <Col xs={24} md={12}><Form.Item name="direction" label="交易方向" rules={[{ required: true }]}><Select options={[{ value: 'LONG', label: '做多 · 买入开仓 / 卖出平仓' }, { value: 'SHORT', label: '做空 · 卖出开仓 / 买入平仓' }]} /></Form.Item></Col>
        </Row>
      </Card>
      <div className="config-editor-grid">
      <Card title="网格参数">
        <Row gutter={[20, 16]}>
          <Col xs={24} md={12}><Form.Item name="grid_interval" label="网格间距" dependencies={['symbol']} extra="相邻网格价格的固定差值，单位为报价资产" rules={[{ required: true, message: '请输入网格间距' }, () => ({ validator: (_, value) => { const error = tickSize ? tickSizeError(value, tickSize) : null; return error ? Promise.reject(new Error(error)) : Promise.resolve(); } })]}><InputNumber stringMode min="0.0000000001" className="full-width-input" /></Form.Item></Col>
          <Col xs={24} md={12}><Form.Item name="order_amount_usdc" label="每格金额（报价资产）" dependencies={['symbol']} rules={[{ required: true, message: '请输入每格金额' }, () => ({ validator: (_, value) => { const error = orderAmountError(value, effectiveMark, { stepSize, minQty, minNotional }); return error ? Promise.reject(new Error(error)) : Promise.resolve(); } })]}><InputNumber stringMode min="0.0000000001" className="full-width-input" /></Form.Item></Col>
          <Col xs={24} md={12}><Form.Item name="buy_window" label="买单窗口" extra="当前价格下方预挂的买单数量，范围为 1–50" rules={[{ required: true }]}><InputNumber min={1} max={50} precision={0} className="full-width-input" /></Form.Item></Col>
          <Col xs={24} md={12}><Form.Item name="sell_window" label="卖单窗口" extra="当前价格上方预挂的卖单数量，范围为 1–50" rules={[{ required: true }]}><InputNumber min={1} max={50} precision={0} className="full-width-input" /></Form.Item></Col>
        </Row>
      </Card>
      <GridPreview key={selectedSymbol ?? 'new'} form={form} markPrice={effectiveMark} tickSize={tickSize} stepSize={stepSize} minQty={minQty} minNotional={minNotional} quote={quote || '报价资产'} />
      </div>
      <PairRiskAndRangeSection />
      <div className={`save-bar${edit.dirty ? ' save-bar-dirty' : ''}`}>
        <SaveBarStatus form={form} adding={adding} dirty={edit.dirty} symbol={symbol} baseline={baseline.current} />
        <Space wrap>
        <FormRevertButton activeRef={page} onReset={mutation.reset} onRevert={() => { if (adding) { customInterval.current = false; appliedSymbol.current = undefined; form.setFieldsValue(defaults); } else if (baseline.current?.symbol === symbol) form.setFieldsValue(baseline.current.config); }} />
        <Button type="primary" htmlType="submit" loading={mutation.isPending}>{adding ? '保存并添加' : '保存并立即生效'}</Button>
      </Space></div>
      {!adding && <Card className="danger-zone" title="删除策略"><p>删除前请暂停策略、等待挂单撤销并平仓；删除不会自动平仓，历史记录会保留。</p><Button danger disabled={edit.saving} loading={deletion.busy} onClick={() => void remove()}>删除策略</Button></Card>}
    </Form>
  </div>;
}

function SaveBarStatus({ form, adding, dirty, symbol, baseline }: { form: FormInstance<FormValues>; adding: boolean; dirty: boolean; symbol: string | undefined; baseline: { symbol: string; config: GridConfig } | null }) {
  const watched = Form.useWatch([], form) as FormValues | undefined;
  const [showChanges, setShowChanges] = useState(false);
  useEffect(() => {
    if (!dirty) setShowChanges(false);
  }, [dirty]);
  useEffect(() => {
    setShowChanges(false);
  }, [symbol]);
  const pending = !adding && dirty && watched && baseline && baseline.symbol === symbol ? diffGridConfig(baseline.config, watched) : [];
  const expanded = showChanges && pending.length > 0;
  return <div className="save-bar-status">
    <span className="muted" role="status">{dirty ? (pending.length ? `已修改 ${pending.length} 项` : '有未保存的修改') : adding ? '确认参数后保存并添加交易对' : '所有修改已保存'}</span>
    {pending.length > 0 && <Button type="link" size="small" aria-expanded={expanded} aria-controls="pending-changes" onClick={() => setShowChanges(value => !value)}>{expanded ? '收起' : '查看'}</Button>}
    {expanded && <ul id="pending-changes" className="save-bar-changes">{pending.map(change => <li key={change.key} className={change.risky ? 'confirm-risky' : undefined}><span>{change.label}</span><span><s>{change.before}</s> → <strong>{change.after}</strong></span></li>)}</ul>}
  </div>;
}
