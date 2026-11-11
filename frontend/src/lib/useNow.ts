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

/** Re-render only when Beijing midnight (00:00 UTC+8) passes, returning that day's start timestamp. */
export function useBeijingDay() {
  const day = Math.floor((Date.now() + BEIJING_OFFSET_MS) / DAY_MS);
  const startMs = day * DAY_MS - BEIJING_OFFSET_MS;
  useExpired(startMs + DAY_MS - 1);
  return startMs;
}

/** Return the current UTC hour index, re-rendering only when the hour turns over. */
export function useUtcHour(enabled = true) {
  const hour = Math.floor(Date.now() / 3_600_000);
  useExpired(enabled ? (hour + 1) * 3_600_000 - 1 : Infinity);
  return hour;
}

/** Recompute `accountGroups` on data changes and exact freshness threshold expiries without a 1 Hz clock. */
export function useAccountGroups(entries: AccountObservation[], enabled = true) {
  const deadline = enabled ? nextAccountExpiry(entries, Date.now()) : Infinity;
  const expired = useExpired(deadline);
  return useMemo(() => enabled ? accountGroups(entries, Date.now()) : [], [entries, enabled, deadline, expired]);
}
