import { useEffect, useMemo, useState } from 'react';
import { accountGroups, nextAccountExpiry } from './portfolio';
import type { AccountObservation } from './types';

/** A wall clock that re-renders its caller every `intervalMs`. */
export function useNow(intervalMs = 1000, enabled = true) {
  const [now, setNow] = useState(Date.now);
  useEffect(() => {
    if (!enabled) return;
    setNow(Date.now());
    const timer = window.setInterval(() => setNow(Date.now()), intervalMs);
    return () => window.clearInterval(timer);
  }, [intervalMs, enabled]);
  return now;
}

const MAX_TIMEOUT_MS = 2_147_483_647;

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

/** Re-render only when Beijing midnight (00:00 UTC+8) passes, returning that day's start timestamp. */
export function useBeijingDay(generatedAt?: string, receivedAt?: number) {
  const genMs = generatedAt ? Date.parse(generatedAt) : NaN;
  const hasServer = Number.isFinite(genMs);
  const hasReceived = typeof receivedAt === 'number' && receivedAt > 0;
  const now = hasServer ? genMs + (hasReceived ? Math.max(0, Date.now() - receivedAt) : 0) : Date.now();
  const day = Math.floor((now + BEIJING_OFFSET_MS) / DAY_MS);
  const startMs = day * DAY_MS - BEIJING_OFFSET_MS;
  useExpired(!hasServer ? startMs + DAY_MS - 1 : hasReceived ? receivedAt + (startMs + DAY_MS - genMs) - 1 : Infinity);
  return startMs;
}

/** Return the current UTC hour index, re-rendering only when the hour turns over. */
export function useUtcHour(enabled = true, generatedAt?: string, receivedAt?: number) {
  const genMs = generatedAt ? Date.parse(generatedAt) : NaN;
  const hasServer = Number.isFinite(genMs);
  const hasReceived = typeof receivedAt === 'number' && receivedAt > 0;
  const now = hasServer ? genMs + (hasReceived ? Math.max(0, Date.now() - receivedAt) : 0) : Date.now();
  const hour = Math.floor(now / 3_600_000);
  const nextHourMs = (hour + 1) * 3_600_000;
  useExpired(!enabled ? Infinity : !hasServer ? nextHourMs - 1 : hasReceived ? receivedAt + (nextHourMs - genMs) - 1 : Infinity);
  return hour;
}

/** Recompute `accountGroups` on data changes and exact freshness threshold expiries without a 1 Hz clock. */
export function useAccountGroups(entries: AccountObservation[], enabled = true) {
  const deadline = enabled ? nextAccountExpiry(entries, Date.now()) : Infinity;
  const expired = useExpired(deadline);
  return useMemo(() => enabled ? accountGroups(entries, Date.now()) : [], [entries, enabled, deadline, expired]);
}
