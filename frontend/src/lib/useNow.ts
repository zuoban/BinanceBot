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
