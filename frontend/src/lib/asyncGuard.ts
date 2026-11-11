import { useCallback, useEffect, useRef } from 'react';

export function useMountedRef(onUnmount?: () => void) {
  const mounted = useRef(true);
  const cleanup = useRef(onUnmount);
  cleanup.current = onUnmount;
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
      cleanup.current?.();
    };
  }, []);
  return mounted;
}

interface RunLocked {
  <T>(fn: () => Promise<T>, fallback: T): Promise<T>;
  <T = void>(fn: () => Promise<T>): Promise<T | undefined>;
}

export function useAsyncLock(): RunLocked {
  const locked = useRef(false);
  return useCallback(async <T>(fn: () => Promise<T>, fallback?: T): Promise<T | undefined> => {
    if (locked.current) return fallback;
    locked.current = true;
    try {
      return await fn();
    } finally {
      locked.current = false;
    }
  }, []) as RunLocked;
}
