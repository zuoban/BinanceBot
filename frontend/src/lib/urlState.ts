import { useCallback, useRef } from 'react';
import { useSearchParams } from 'react-router-dom';

interface ParamSpec { fallback: string; allowed?: readonly string[] }

/**
 * Several query-string parameters as state. Defaults are omitted from the URL and changes replace history.
 * Router updaters do not queue, so every change to one event must go through a single `update(patch)` call.
 */
export function useUrlParams<S extends Record<string, ParamSpec>>(spec: S): [{ [K in keyof S]: string }, (patch: Partial<{ [K in keyof S]: string }>) => void] {
  const [params, setParams] = useSearchParams();
  const specRef = useRef(spec);
  specRef.current = spec;
  const values = Object.fromEntries(Object.entries(spec).map(([key, { fallback, allowed }]) => {
    const raw = params.get(key);
    return [key, raw !== null && (!allowed || allowed.includes(raw)) ? raw : fallback];
  })) as { [K in keyof S]: string };
  const update = useCallback((patch: Partial<{ [K in keyof S]: string }>) => {
    setParams(current => {
      const next = new URLSearchParams(current);
      for (const [key, value] of Object.entries(patch) as [string, string | undefined][]) {
        if (value === undefined || !(key in specRef.current)) continue;
        if (value === specRef.current[key].fallback || value === '') next.delete(key); else next.set(key, value);
      }
      return next;
    }, { replace: true });
  }, [setParams]);
  return [values, update];
}

/** One query-string parameter as state. */
export function useUrlParam<T extends string>(key: string, fallback: T, allowed?: readonly T[]): [T, (value: T) => void] {
  const [values, update] = useUrlParams({ [key]: { fallback, allowed } });
  return [values[key] as T, useCallback((value: T) => update({ [key]: value }), [update, key])];
}
