import { useContext, useEffect, useLayoutEffect, useMemo, useRef, useState, type MutableRefObject } from 'react';
import { flushSync } from 'react-dom';
import { useLocation, useNavigate, useParams, type NavigateFunction } from 'react-router-dom';
import { Alert, App, Button, Card, Form, Space, type FormInstance } from 'antd';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, pairPath, pairRoute, post } from '../lib/api';
import { useConfirmDialog } from '../lib/confirmDialog';
import { invalidateStrategyQueries, usePairs } from '../lib/queries';
import { EditContext } from '../lib/session';
import type { GlobalConfig, GridConfig, Mode, Pair } from '../lib/types';
import { ConflictReloadButton, ErrorNotice, FormRevertButton, isConfigConflict, PageBreadcrumb, PageTitle } from '../components/Common';
import { useDeleteStrategy } from '../components/DeleteStrategy';
import { GridPreview } from '../components/GridPreview';
import { defaults, LiveConfigConfirmContent, PairBasicSection, PairGridParamsSection, PairRiskAndRangeSection, SaveBarStatus, useLiveSymbolMark, type FormValues } from '../components/PairConfigSections';
import { defaultGridInterval, isPositiveDecimal, orderAmountError } from '../lib/gridPreview';
import { positiveDecimalText } from '../lib/format';
import { quoteAsset } from '../lib/portfolio';
import { diffGridConfig } from '../lib/configDiff';

type BaselineState = { symbol: string; config: GridConfig & { config_version: string } };
type LoadedGridConfig = GridConfig & { config_version: string; tick_size?: string; step_size?: string; min_qty?: string; min_notional?: string };

function usePageSubmissionGuard(pathname: string) {
  const { subscribeRouteLeave } = useContext(EditContext);
  const removing = useRef(false), submission = useRef<object | null>(null), page = useRef<object | null>(null), activePreflight = useRef<object | null>(null);
  const [preflightPending, setPreflightPending] = useState(false);
  useLayoutEffect(() => {
    const current = {};
    page.current = current;
    const invalidate = () => {
      if (page.current === current) {
        page.current = null;
        submission.current = null;
        if (activePreflight.current) { activePreflight.current = null; setPreflightPending(false); }
      }
    };
    const unsubscribe = subscribeRouteLeave?.(nextPathname => {
      if (nextPathname !== pathname) invalidate();
    });
    return () => { unsubscribe?.(); invalidate(); };
  }, [pathname, subscribeRouteLeave]);
  return { page, removing, submission, activePreflight, preflightPending, setPreflightPending };
}

function usePairConfigActions({ adding, symbol, pair, baseline, form, refetchGlobal, pathname, navigate }: {
  adding: boolean; symbol: string | undefined; pair: Pair | undefined;
  baseline: React.RefObject<BaselineState | null>; form: FormInstance<FormValues>;
  refetchGlobal: () => Promise<{ data?: GlobalConfig; isError?: boolean; error?: unknown }>; pathname: string; navigate: NavigateFunction;
}) {
  const { page, removing, submission, activePreflight, preflightPending, setPreflightPending } = usePageSubmissionGuard(pathname);
  const edit = useContext(EditContext), client = useQueryClient(), { message } = App.useApp();
  const { confirm } = useConfirmDialog(symbol), deletion = useDeleteStrategy(symbol);
  const mutation = useMutation({
    mutationFn: (values: FormValues) => {
      let targetPath = '/api/pairs';
      if (!adding) {
        if (!pair) throw new Error('该交易对不存在或已移除');
        if (typeof baseline.current?.config.config_version !== 'string' || !baseline.current.config.config_version || baseline.current.symbol !== symbol) {
          throw new Error('无法确认配置版本，请重新加载策略配置');
        }
        targetPath = pairPath(pair, 'config');
      }
      return post<GridConfig | string>(targetPath, {
        ...values, ...(!adding && { baseline_version: baseline.current?.config.config_version }), symbol: values.symbol.trim().toUpperCase(),
        post_only: true,
        min_price: values.min_price || null, max_price: values.max_price || null, max_position_usdc: values.max_position_usdc || null, min_liquidation_distance_pct: values.min_liquidation_distance_pct || null,
      });
    },
    onMutate: () => { edit.setSaving(true); return { submitPage: page.current }; },
    onSuccess: (data, values, context) => {
      if (!page.current || context?.submitPage !== page.current) return;
      flushSync(() => { edit.setDirty(false); edit.setSaving(false); });
      if (!adding && typeof data !== 'string') form.setFieldsValue(data);
      message.success(adding ? '交易对已添加，策略保持暂停，请检查配置后手动恢复' : '策略配置已保存并生效');
      const targetSymbol = adding || !symbol ? (typeof data === 'string' ? data : values.symbol.trim().toUpperCase()) : symbol;
      navigate(pairRoute(targetSymbol), { replace: true });
      void invalidateStrategyQueries(client, { accounts: adding, snapshot: targetSymbol, configSymbol: adding ? undefined : symbol, history: adding ? 'reset' : undefined });
    },
    onError: (error, _values, context) => {
      if (page.current && context?.submitPage === page.current) message.error(error.message);
    },
    onSettled: (_data, _error, _values, context) => {
      if (page.current && context?.submitPage === page.current) edit.setSaving(false);
    },
  });
  const remove = async () => {
    if (removing.current || submission.current || !symbol || !pair || !page.current) return;
    const currentPage = page.current;
    removing.current = true; edit.setSaving(true);
    let removed = false;
    try {
      if (edit.dirty && (!await confirm({ title: '放弃未保存的修改？', content: '删除策略会放弃当前表单修改。', okText: '继续', cancelText: '取消' }) || page.current !== currentPage)) return;
      const latest = await refetchGlobal();
      if (page.current !== currentPage) return;
      removed = await deletion.remove(
        { ...pair, mode: (!latest.isError && latest.data ? Boolean(latest.data.has_api_key && latest.data.has_api_secret) : true) ? 'LIVE' : pair.mode },
        async () => page.current === currentPage,
      );
    } finally {
      removing.current = false;
      if (page.current === currentPage) flushSync(() => { if (removed) edit.setDirty(false); edit.setSaving(false); });
    }
    if (removed && page.current === currentPage) navigate('/');
  };
  const submit = async (values: FormValues) => {
    if (removing.current || submission.current || !page.current) return;
    const currentPage = page.current, currentSubmission = {}, preflightToken = {};
    submission.current = currentSubmission;
    activePreflight.current = preflightToken;
    setPreflightPending(true);
    try {
      let latest: { data?: GlobalConfig; isError?: boolean; error?: unknown };
      try {
        latest = await refetchGlobal();
      } finally {
        if (activePreflight.current === preflightToken) {
          activePreflight.current = null;
          setPreflightPending(false);
        }
      }
      if (page.current !== currentPage || submission.current !== currentSubmission) return;
      const currentLive = !latest.isError && latest.data ? Boolean(latest.data.has_api_key && latest.data.has_api_secret) : true;
      const changes = adding || !baseline.current ? null : diffGridConfig(baseline.current.config, values);
      if (currentLive && !await confirm({ title: adding ? '添加实盘交易对？' : '保存实盘配置？', content: <LiveConfigConfirmContent changes={changes} values={values} />, okText: '确认保存', cancelText: '取消' })) return;
      if (page.current !== currentPage || submission.current !== currentSubmission) return;
      await mutation.mutateAsync(values).catch(() => { /* Mutation errors are reported by onError. */ });
    } finally {
      if (submission.current === currentSubmission) submission.current = null;
    }
  };
  return { page, submission, preflightPending, edit, mutation, deletionBusy: deletion.busy, remove, submit };
}

function PairConfigSaveBar({ form, adding, dirty, saving, isPending, symbol, page, baseline, hasPair, deletionBusy, onReset, onRevert, onRemove }: {
  form: FormInstance<FormValues>;
  adding: boolean;
  dirty: boolean;
  saving: boolean;
  isPending: boolean;
  symbol: string | undefined;
  page: { current: unknown };
  baseline: BaselineState | null;
  hasPair: boolean;
  deletionBusy: boolean;
  onReset: () => void;
  onRevert: () => void;
  onRemove: () => void;
}) {
  return <>
    <div className={`save-bar${dirty ? ' save-bar-dirty' : ''}`}>
      <SaveBarStatus form={form} adding={adding} dirty={dirty} symbol={symbol} baseline={baseline} />
      <Space wrap>
        <FormRevertButton form={form} activeRef={page} resetKey={symbol} onReset={onReset} onRevert={onRevert} />
        <Button type="primary" htmlType="submit" loading={isPending}>{adding ? '保存并添加' : '保存并立即生效'}</Button>
      </Space>
    </div>
    {!adding && <Card className="danger-zone" title="删除策略"><p>删除前请暂停策略、等待挂单撤销并平仓；删除不会自动平仓，历史记录会保留。</p><Button danger disabled={saving || isPending || !hasPair} loading={deletionBusy} onClick={onRemove}>删除策略</Button></Card>}
  </>;
}

function PairConfigPreviewPane({
  adding,
  ready,
  symbol,
  selectedSymbol,
  mode,
  previewMark,
  effectiveMarkRef,
  editedFields,
  form,
  tickSize,
  stepSize,
  minQty,
  minNotional,
  quote,
}: {
  adding: boolean;
  ready: boolean;
  symbol: string | undefined;
  selectedSymbol: string | undefined;
  mode: Mode | undefined;
  previewMark: string | undefined;
  effectiveMarkRef: MutableRefObject<string | undefined>;
  editedFields: MutableRefObject<Set<string>>;
  form: FormInstance<FormValues>;
  tickSize: string | undefined;
  stepSize: string | undefined;
  minQty: string | undefined;
  minNotional: string | undefined;
  quote: string;
}) {
  const liveMark = useLiveSymbolMark(symbol, mode, !adding && !!symbol);
  const effectiveMark = adding ? previewMark : liveMark;
  effectiveMarkRef.current = effectiveMark;
  useEffect(() => {
    if (!ready || (!stepSize && !minQty && !minNotional)) return;
    const currentErrors = form.getFieldError('order_amount_usdc');
    if (!editedFields.current.has('order_amount_usdc') && currentErrors.length === 0) return;
    const amountValue = form.getFieldValue('order_amount_usdc');
    if (amountValue == null || amountValue === '' || !isPositiveDecimal(amountValue)) return;
    const nextError = orderAmountError(amountValue, effectiveMark, { stepSize, minQty, minNotional });
    if (nextError !== (currentErrors[0] ?? null)) {
      void form.validateFields(['order_amount_usdc']).catch(() => {});
    }
  }, [stepSize, minQty, minNotional, effectiveMark, ready, form, editedFields]);
  return <GridPreview key={selectedSymbol ?? 'new'} form={form} markPrice={effectiveMark} tickSize={tickSize} stepSize={stepSize} minQty={minQty} minNotional={minNotional} quote={quote || '报价资产'} />;
}

export default function PairConfig() {
  const { symbol } = useParams(), adding = !symbol, navigate = useNavigate(), { pathname } = useLocation();
  const pairs = usePairs(), pair = pairs.data?.find(p => p.symbol === symbol);
  const [ready, setReady] = useState(adding);
  const [form] = Form.useForm<FormValues>();
  const baseline = useRef<BaselineState | null>(null), customInterval = useRef(false), appliedSymbol = useRef<string | undefined>(undefined), editedFields = useRef(new Set<string>()), effectiveMarkRef = useRef<string | undefined>(undefined), resetMutation = useRef(() => {});
  const global = useQuery({ queryKey: ['global-config'], queryFn: ({ signal }) => api<GlobalConfig>('/api/global/config', { signal }), refetchOnWindowFocus: false });
  const live = global.data ? global.data.has_api_key && global.data.has_api_secret : pair ? pair.mode === 'LIVE' : true;
  const symbols = useQuery({ queryKey: ['exchange-symbols'], queryFn: ({ signal }) => api<string[]>('/api/exchange/symbols?environment=mainnet', { signal }), enabled: adding, retry: false, refetchOnWindowFocus: false });
  const config = useQuery({ queryKey: ['config', symbol, pair?.scoped], queryFn: ({ signal }) => { if (!pair) throw new Error('该交易对不存在或已移除'); return api<LoadedGridConfig>(pairPath(pair, 'config'), { signal }); }, enabled: !!pair, refetchOnWindowFocus: false });
  const { page, submission, preflightPending, edit, mutation, deletionBusy, remove, submit } = usePairConfigActions({ adding, symbol, pair, baseline, form, refetchGlobal: global.refetch, pathname, navigate });
  const { dirty: editDirty, saving: editSaving, setDirty: setEditDirty, setSaving: setEditSaving } = edit;
  resetMutation.current = mutation.reset;
  useEffect(() => {
    setReady(adding); effectiveMarkRef.current = undefined; baseline.current = null; customInterval.current = false; appliedSymbol.current = undefined; editedFields.current.clear();
    resetMutation.current(); setEditDirty(false); setEditSaving(false); form.resetFields();
    if (adding) form.setFieldsValue(defaults);
  }, [symbol, adding, form, setEditDirty, setEditSaving]);
  useEffect(() => {
    if (config.data && config.data.symbol === symbol && !ready && !adding && symbol) { baseline.current = { symbol, config: config.data }; form.setFieldsValue(config.data); setReady(true); }
  }, [config.data, ready, adding, form, symbol]);
  useEffect(() => () => { setEditDirty(false); setEditSaving(false); }, [setEditDirty, setEditSaving]);
  const disabled = !ready || editSaving || deletionBusy || (!adding && !pair);
  const formDisabled = disabled || preflightPending || mutation.isPending;
  const watchedSymbol = Form.useWatch('symbol', form) as string | undefined;
  const selectedSymbol = adding ? watchedSymbol?.trim().toUpperCase() : symbol, quote = quoteAsset(selectedSymbol ?? '');
  const symbolPreview = useQuery({ queryKey: ['symbol-preview', selectedSymbol], queryFn: ({ signal }) => api<{ symbol: string; last_price?: string | null; mark_price?: string | null; tick_size?: string; step_size?: string; min_qty?: string; min_notional?: string }>(`/api/exchange/symbol-preview?environment=mainnet&symbol=${encodeURIComponent(selectedSymbol ?? '')}`, { signal }), enabled: adding && !!selectedSymbol, retry: false, refetchOnWindowFocus: false });
  const previewLast = positiveDecimalText(symbolPreview.data?.last_price), previewMark = positiveDecimalText(symbolPreview.data?.mark_price);
  const latestPrice = adding ? (previewLast ?? previewMark) : null, previewEffectiveMark = (previewMark ?? previewLast) ?? undefined;
  if (adding) effectiveMarkRef.current = previewEffectiveMark;
  const symbolOptions = useMemo(() => (symbols.data ?? []).map(value => ({ value, label: value })), [symbols.data]);
  const tickSize = adding ? symbolPreview.data?.tick_size : config.data?.tick_size, stepSize = adding ? symbolPreview.data?.step_size : config.data?.step_size;
  const minQty = adding ? symbolPreview.data?.min_qty : config.data?.min_qty, minNotional = adding ? symbolPreview.data?.min_notional : config.data?.min_notional;
  useEffect(() => {
    if (!adding || !ready || editSaving || submission.current || !selectedSymbol || !previewEffectiveMark || customInterval.current || appliedSymbol.current === selectedSymbol) return;
    const nextInterval = defaultGridInterval(previewEffectiveMark, tickSize);
    if (nextInterval) { appliedSymbol.current = selectedSymbol; editedFields.current.add('grid_interval'); form.setFieldValue('grid_interval', nextInterval); void form.validateFields(['grid_interval', 'order_amount_usdc']).catch(() => {}); }
  }, [adding, ready, editSaving, selectedSymbol, previewEffectiveMark, tickSize, form, submission]);
  useEffect(() => {
    if (tickSize && ready && (editedFields.current.has('grid_interval') || form.getFieldError('grid_interval').length > 0)) void form.validateFields(['grid_interval']).catch(() => {});
  }, [tickSize, ready, form]);
  return <div className="config-page">
    <PageBreadcrumb disabled={editSaving} items={!symbol ? [{ label: '策略总览', to: '/' }, { label: '新增交易对' }] : [{ label: '策略总览', to: '/' }, { label: symbol, to: pairRoute(symbol) }, { label: '策略参数' }]} />
    <PageTitle title={adding ? '新增交易对' : `${symbol} 策略参数`} description={adding ? '新策略创建后保持暂停，检查参数后手动恢复' : '参数保存后立即生效，请先核对策略设置'} />
    <ErrorNotice error={pairs.error || config.error} retry={() => { if (pairs.error) void pairs.refetch(); if (!adding && pair) void config.refetch(); }} />
    <ErrorNotice error={mutation.error} />
    {!adding && isConfigConflict(mutation.error, '交易对设置已被其他请求修改', '无法确认配置版本') && <ConflictReloadButton form={form} label="策略配置" activeRef={page} resetKey={symbol} refetch={config.refetch} onReset={mutation.reset} onLoaded={data => { if (!symbol || data.symbol !== symbol) return false; submission.current = null; editedFields.current.clear(); baseline.current = { symbol, config: data }; form.setFieldsValue(data); }} />}
    {pairs.data && !pair && !adding && !pairs.isFetching && <Alert type="warning" title="该交易对不存在或已移除" />}
    <Form<FormValues> className="config-form" form={form} layout="vertical" disabled={formDisabled} onFinish={values => void submit(values)} onValuesChange={changed => {
      submission.current = null;
      for (const key of Object.keys(changed)) editedFields.current.add(key);
      if ('symbol' in changed) { editedFields.current.delete('grid_interval'); form.setFieldValue('grid_interval', defaults.grid_interval); customInterval.current = false; appliedSymbol.current = undefined; }
      else if ('grid_interval' in changed) customInterval.current = true;
      if (changed.pause_on_max_position) { form.setFieldValue('cancel_opening_on_max_position', false); void form.validateFields(['max_position_usdc']).catch(() => {}); }
      else if (changed.cancel_opening_on_max_position) { form.setFieldValue('pause_on_max_position', false); void form.validateFields(['max_position_usdc']).catch(() => {}); }
      setEditDirty(true);
    }}>
      <PairBasicSection live={live} adding={adding} formDisabled={formDisabled} selectedSymbol={selectedSymbol} latestPrice={latestPrice} quote={quote}
        symbolOptions={symbolOptions} symbolsFetching={symbols.isFetching} symbolsError={symbols.isError} previewFetching={symbolPreview.isFetching}
        previewError={adding ? symbolPreview.error : null} onRetrySymbols={() => void symbols.refetch()} onRetryPreview={() => void symbolPreview.refetch()} />
      <div className="config-editor-grid">
        <PairGridParamsSection effectiveMarkRef={effectiveMarkRef} tickSize={tickSize} stepSize={stepSize} minQty={minQty} minNotional={minNotional} />
        <PairConfigPreviewPane adding={adding} ready={ready} symbol={symbol} selectedSymbol={selectedSymbol} mode={global.data ? (live ? 'LIVE' : 'PAPER') : pair?.mode} previewMark={previewEffectiveMark} effectiveMarkRef={effectiveMarkRef} editedFields={editedFields} form={form} tickSize={tickSize} stepSize={stepSize} minQty={minQty} minNotional={minNotional} quote={quote} />
      </div>
      <PairRiskAndRangeSection />
      <PairConfigSaveBar form={form} adding={adding} dirty={editDirty} saving={editSaving} isPending={preflightPending || mutation.isPending} symbol={symbol} page={page} baseline={baseline.current} hasPair={!!pair} deletionBusy={deletionBusy} onReset={mutation.reset} onRemove={() => void remove()}
        onRevert={() => { submission.current = null; editedFields.current.clear(); if (adding) { customInterval.current = false; appliedSymbol.current = undefined; form.setFieldsValue(defaults); } else if (baseline.current?.symbol === symbol) { form.setFieldsValue(baseline.current.config); } }} />
    </Form>
  </div>;
}
