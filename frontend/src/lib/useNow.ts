import { useEffect, useState } from 'react';

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

/** Re-render at an expiry boundary instead of on every clock tick. */
export function useExpired(deadline: number) {
  const [, refresh] = useState(0);
  useEffect(() => {
    const remaining = deadline - Date.now();
    if (!Number.isFinite(remaining)) return;
    const timer = window.setTimeout(() => refresh(value => value + 1), Math.max(0, remaining + 1));
    return () => window.clearTimeout(timer);
  }, [deadline]);
  return !Number.isFinite(deadline) || Date.now() > deadline;
}
