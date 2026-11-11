import { useEffect, useMemo, useState } from 'react';
import { accountGroups, nextAccountExpiry } from './format';
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

/** Re-render only when Beijing midnight (00:00 UTC+8) passes. */
export function useBeijingDay() {
  const now = Date.now();
  const day = Math.floor((now + 8 * 3600_000) / 86_400_000);
  useExpired((day + 1) * 86_400_000 - 8 * 3600_000 - 1);
  return Date.now();
}

/** Recompute `accountGroups` on data changes and exact freshness threshold expiries without a 1 Hz clock. */
export function useAccountGroups(entries: AccountObservation[], enabled = true) {
  const deadline = enabled ? nextAccountExpiry(entries, Date.now()) : Infinity;
  const expired = useExpired(deadline);
  return useMemo(() => enabled ? accountGroups(entries, Date.now()) : [], [entries, enabled, deadline, expired]);
}
