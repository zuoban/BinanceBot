import { useContext, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { flushSync } from 'react-dom';
import { UNSAFE_DataRouterContext, useLocation, useNavigate, useParams } from 'react-router-dom';
import { Alert, App, Button, Card, Col, Form, Input, InputNumber, Row, Select, Space } from 'antd';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, pairPath, pairRoute, post } from '../lib/api';
import { usePairs } from '../lib/queries';
import { EditContext } from '../lib/session';
import type { GlobalConfig, GridConfig } from '../lib/types';
import { ConflictReloadButton, ErrorNotice, FormRevertButton, isConfigConflict, PageBreadcrumb, PageTitle } from '../components/Common';
import { useDeleteStrategy } from '../components/DeleteStrategy';
import { GridPreview } from '../components/GridPreview';
import { AccountMarkBridge, defaults, directionOptions, LiveConfigConfirmContent, PairRiskAndRangeSection, SaveBarStatus, type FormValues } from '../components/PairConfigSections';
import { defaultGridInterval, isPositiveDecimal, orderAmountError, tickSizeError } from '../lib/gridPreview';
import { positiveDecimalText } from '../lib/format';
import { quoteAsset } from '../lib/portfolio';
import { diffGridConfig } from '../lib/configDiff';
export default function PairConfig() {
  const { symbol } = useParams();
  const adding = !symbol;
  const navigate = useNavigate();
  const { pathname } = useLocation();
  const router = useContext(UNSAFE_DataRouterContext)?.router;
  const pairs = usePairs();
  const pair = pairs.data?.find(p => p.symbol === symbol);
  const [liveMark, setLiveMark] = useState<string | undefined>(undefined);
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
    setLiveMark(undefined);
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
      const targetSymbol = adding ? (typeof data === 'string' ? data : values.symbol.trim().toUpperCase()) : symbol!;
      void client.invalidateQueries({ queryKey: ['pairs'] });
      void client.invalidateQueries({ queryKey: ['snapshot', targetSymbol] });
      navigate(pairRoute(targetSymbol), { replace: true });
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
  const effectiveMark = (adding ? (previewMark ?? previewLast) : liveMark) ?? undefined;
  const symbolOptions = useMemo(() => (symbols.data ?? []).map(value => ({ value, label: value })), [symbols.data]);
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
    {!adding && symbol && <AccountMarkBridge symbol={symbol} mode={pair?.mode} onMark={setLiveMark} />}
    <PageBreadcrumb disabled={edit.saving} items={!symbol
      ? [{ label: '策略总览', to: '/' }, { label: '新增交易对' }]
      : [{ label: '策略总览', to: '/' }, { label: symbol, to: pairRoute(symbol) }, { label: '策略参数' }]} />
    <PageTitle title={adding ? '新增交易对' : `${symbol} 策略参数`} description={adding ? '新策略创建后保持暂停，检查参数后手动恢复' : '参数保存后立即生效，请先核对策略设置'} />
    <ErrorNotice error={pairs.error || config.error} retry={() => { if (pairs.error) void pairs.refetch(); if (!adding && pair) void config.refetch(); }} />
    <ErrorNotice error={mutation.error} />
    {!adding && isConfigConflict(mutation.error, '无法确认配置版本') && <ConflictReloadButton label="策略配置" activeRef={page} refetch={config.refetch} onReset={mutation.reset} onLoaded={data => {
      if (!symbol || data.symbol !== symbol) return false;
      baseline.current = { symbol, config: data };
      form.setFieldsValue(data);
    }} />}
    {pairs.data && !pair && !adding && !pairs.isFetching && <Alert type="warning" title="该交易对不存在或已移除" />}
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
                  options={symbolOptions}
                />
              ) : (
                <Input className="symbol-field" disabled />
              )}
            </Form.Item>
          </Col>
          <Col xs={24} md={12}><Form.Item name="direction" label="交易方向" rules={[{ required: true }]}><Select options={directionOptions} /></Form.Item></Col>
        </Row>
      </Card>
      <div className="config-editor-grid">
      <Card title="网格参数">
        <Row gutter={[20, 16]}>
          <Col xs={24} md={12}><Form.Item name="grid_interval" label="网格间距" dependencies={['symbol']} extra="相邻网格价格的固定差值，单位为报价资产" rules={[{ required: true, message: '请输入网格间距' }, () => ({ validator: (_, value) => { if (value == null || value === '') return Promise.resolve(); if (!isPositiveDecimal(value)) return Promise.reject(new Error('网格间距必须大于 0')); const error = tickSize ? tickSizeError(value, tickSize) : null; return error ? Promise.reject(new Error(error)) : Promise.resolve(); } })]}><InputNumber stringMode min="0" className="full-width-input" /></Form.Item></Col>
          <Col xs={24} md={12}><Form.Item name="order_amount_usdc" label="每格金额（报价资产）" dependencies={['symbol']} rules={[{ required: true, message: '请输入每格金额' }, () => ({ validator: (_, value) => { if (value == null || value === '') return Promise.resolve(); if (!isPositiveDecimal(value)) return Promise.reject(new Error('每格金额必须大于 0')); const error = orderAmountError(value, effectiveMark, { stepSize, minQty, minNotional }); return error ? Promise.reject(new Error(error)) : Promise.resolve(); } })]}><InputNumber stringMode min="0" className="full-width-input" /></Form.Item></Col>
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
