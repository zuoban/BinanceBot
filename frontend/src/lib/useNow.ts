import { useEffect, useMemo, useState } from 'react';
import { accountGroups, nextAccountExpiry } from './portfolio';
import type { AccountObservation } from './types';

/** A wall clock that re-renders its caller every `intervalMs`. */
export function useNow(intervalMs = 1000, enabled = true) {
  const [now, setNow] = useState(Date.now);
  useEffect(() => {
    if (!enabled) return;
    let timer: number | undefined;
    const start = () => {
      window.clearInterval(timer);
      setNow(Date.now());
      if (typeof document === 'undefined' || document.visibilityState !== 'hidden') {
        timer = window.setInterval(() => setNow(Date.now()), intervalMs);
      }
    };
    start();
    const onVisibility = () => start();
    if (typeof document !== 'undefined') document.addEventListener('visibilitychange', onVisibility);
    return () => {
      window.clearInterval(timer);
      if (typeof document !== 'undefined') document.removeEventListener('visibilitychange', onVisibility);
    };
  }, [intervalMs, enabled]);
  return now;
}

const MAX_TIMEOUT_MS = 2_147_483_647;

export function nextAgeBoundaryDelay(timestamps: readonly number[], now = Date.now()): number {
  let minDelay = Infinity;
  for (const ts of timestamps) {
    if (!Number.isFinite(ts)) continue;
    const elapsed = now - ts;
    let nextAt: number;
    if (elapsed < 2_000) {
      nextAt = ts + 2_000;
    } else if (elapsed < 60_000) {
      nextAt = ts + (Math.floor(elapsed / 1_000) + 1) * 1_000;
    } else if (elapsed < 3_600_000) {
      nextAt = ts + (Math.floor(elapsed / 60_000) + 1) * 60_000;
    } else if (elapsed < 86_400_000) {
      nextAt = ts + (Math.floor(elapsed / 3_600_000) + 1) * 3_600_000;
    } else {
      nextAt = ts + (Math.floor(elapsed / 86_400_000) + 1) * 86_400_000;
    }
    const delay = Math.max(50, nextAt - now + 5);
    if (delay < minDelay) minDelay = delay;
  }
  return minDelay;
}

export function useRelativeNow(timestamps: readonly number[], enabled = true) {
  const [now, setNow] = useState(Date.now);
  useEffect(() => {
    if (!enabled || !timestamps.length) return;
    let timer: number | undefined;
    const schedule = () => {
      window.clearTimeout(timer);
      const current = Date.now();
      setNow(current);
      if (typeof document !== 'undefined' && document.visibilityState === 'hidden') return;
      const delay = nextAgeBoundaryDelay(timestamps, current);
      if (Number.isFinite(delay)) {
        timer = window.setTimeout(schedule, Math.min(delay, MAX_TIMEOUT_MS));
      }
    };
    schedule();
    const onVisibility = () => { if (document.visibilityState === 'visible') schedule(); else window.clearTimeout(timer); };
    if (typeof document !== 'undefined') document.addEventListener('visibilitychange', onVisibility);
    return () => {
      window.clearTimeout(timer);
      if (typeof document !== 'undefined') document.removeEventListener('visibilitychange', onVisibility);
    };
  }, [timestamps, enabled]);
  return now;
}

/** Re-render at an expiry boundary instead of on every clock tick. */
export function useExpired(deadline: number) {
  const [tick, refresh] = useState(0);
  const expired = !Number.isFinite(deadline) || Date.now() > deadline;
  useEffect(() => {
    if (expired) return;
    const delay = Math.min(Math.max(0, deadline - Date.now() + 1), MAX_TIMEOUT_MS);
    const timer = window.setTimeout(() => refresh(value => value + 1), delay);
    return () => window.clearTimeout(timer);
  }, [deadline, expired, tick]);
  return expired;
}

const BEIJING_OFFSET_MS = 8 * 3_600_000;
const DAY_MS = 86_400_000;

export function serverAlignedNow(generatedAt?: string, receivedAt?: number): number | undefined {
  const genMs = generatedAt ? Date.parse(generatedAt) : NaN;
  if (!Number.isFinite(genMs)) return undefined;
  return genMs + (typeof receivedAt === 'number' && receivedAt > 0 ? Math.max(0, Date.now() - receivedAt) : 0);
}

function useServerBoundaryBucket(periodMs: number, offsetMs: number, enabled: boolean, generatedAt?: string, receivedAt?: number) {
  const genMs = generatedAt ? Date.parse(generatedAt) : NaN;
  const hasServer = Number.isFinite(genMs);
  const hasReceived = typeof receivedAt === 'number' && receivedAt > 0;
  const now = hasServer ? genMs + (hasReceived ? Math.max(0, Date.now() - receivedAt) : 0) : Date.now();
  const bucket = Math.floor((now + offsetMs) / periodMs);
  const nextBoundaryMs = (bucket + 1) * periodMs - offsetMs;
  useExpired(!enabled ? Infinity : !hasServer ? nextBoundaryMs - 1 : hasReceived ? receivedAt + (nextBoundaryMs - genMs) - 1 : Infinity);
  return bucket;
}

/** Re-render only when Beijing midnight (00:00 UTC+8) passes, returning that day's start timestamp. */
export function useBeijingDay(generatedAt?: string, receivedAt?: number) {
  return useServerBoundaryBucket(DAY_MS, BEIJING_OFFSET_MS, true, generatedAt, receivedAt) * DAY_MS - BEIJING_OFFSET_MS;
}

/** Return the current UTC hour index, re-rendering only when the hour turns over. */
export function useUtcHour(enabled = true, generatedAt?: string, receivedAt?: number) {
  return useServerBoundaryBucket(3_600_000, 0, enabled, generatedAt, receivedAt);
}

/** Recompute `accountGroups` on data changes and exact freshness threshold expiries without a 1 Hz clock. */
export function useAccountGroups(entries: AccountObservation[], enabled = true) {
  const deadline = enabled ? nextAccountExpiry(entries, Date.now()) : Infinity;
  const expired = useExpired(deadline);
  return useMemo(() => {
    void deadline;
    void expired;
    return enabled ? accountGroups(entries, Date.now()) : [];
  }, [entries, enabled, deadline, expired]);
}
