import { useContext, useEffect, useRef, useState } from 'react';
import { App, Button } from 'antd';
import { errorMessage, post } from '../../lib/api';
import { useAsyncLock, useMountedRef } from '../../lib/asyncGuard';
import { EditContext } from '../../lib/session';
import type { GlobalConfig } from '../../lib/types';

export type GlobalForm = GlobalConfig & {
  api_key?: string;
  api_secret?: string;
  telegram_bot_token?: string;
  clear_api_credentials: boolean;
  clear_telegram_credentials: boolean;
};

/** Verify unsaved credentials against the exchange or Telegram; nothing is persisted by the probe. */
export function ProbeButton({ label, path, body, disabled, resetKey }: { label: string; path: string; body: () => Record<string, unknown>; disabled?: boolean; resetKey?: string }) {
  const { message } = App.useApp();
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<{ ok: boolean; text: string } | null>(null);
  const runProbe = useAsyncLock();
  const mounted = useMountedRef();
  const activeKey = useRef(resetKey);
  activeKey.current = resetKey;
  useEffect(() => { setResult(null); }, [resetKey, disabled]);
  const run = () => {
    if (disabled) return;
    void runProbe(async () => {
      const key = activeKey.current;
      setBusy(true); setResult(null);
      try {
        const text = await post<string>(path, body(), { readOnly: true });
        if (mounted.current && activeKey.current === key) setResult({ ok: true, text });
      } catch (error) {
        if (mounted.current && activeKey.current === key) {
          const text = errorMessage(error);
          setResult({ ok: false, text });
          message.error(text);
        }
      } finally {
        if (mounted.current) setBusy(false);
      }
    });
  };
  return <div className="probe">
    <Button onClick={run} loading={busy} disabled={disabled}>{label}</Button>
    <span className={`probe-result${result ? (result.ok ? ' probe-ok' : ' probe-fail') : ''}`} role="status">{result ? `${result.ok ? '✓' : '✕'} ${result.text}` : ''}</span>
  </div>;
}

export function useSettingsFormLifecycle<T>(data: T | undefined, apply: (data: T) => void) {
  const edit = useContext(EditContext);
  const [ready, setReady] = useState(false);
  const baseline = useRef<T | null>(null);
  const submitting = useRef<object | null>(null);
  const applyRef = useRef(apply);
  applyRef.current = apply;
  const mounted = useMountedRef(() => {
    edit.setDirty(false);
    edit.setSaving(false);
  });
  const fill = (next: T) => {
    submitting.current = null;
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
  return { edit, ready, baseline, mounted, submitting, fill };
}
