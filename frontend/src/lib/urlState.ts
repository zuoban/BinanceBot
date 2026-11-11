import { useCallback, useRef } from 'react';
import { useSearchParams } from 'react-router-dom';

interface ParamSpec<T extends string = string> {
  readonly fallback: NoInfer<T>;
  readonly allowed?: readonly T[];
}

type ParamValues<S extends Record<string, ParamSpec>> = {
  [K in keyof S]: S[K] extends { readonly allowed: readonly (infer T extends string)[] } ? T : string;
};

/**
 * Several query-string parameters as state. Defaults are omitted from the URL and changes replace history.
 * Router updaters do not queue, so every change to one event must go through a single `update(patch)` call.
 */
export function useUrlParams<const S extends Record<string, ParamSpec>>(spec: S): [ParamValues<S>, (patch: Partial<ParamValues<S>>) => void] {
  const [params, setParams] = useSearchParams();
  const specRef = useRef(spec);
  specRef.current = spec;
  const values = Object.fromEntries(Object.entries(spec).map(([key, { fallback, allowed }]) => {
    const raw = params.get(key);
    return [key, raw !== null && (!allowed || allowed.includes(raw)) ? raw : fallback];
  })) as ParamValues<S>;
  const update = useCallback((patch: Partial<ParamValues<S>>) => {
    setParams(current => {
      const next = new URLSearchParams(current);
      for (const [key, value] of Object.entries(patch) as [string, string | undefined][]) {
        if (value === undefined || !(key in specRef.current)) continue;
        const { fallback, allowed } = specRef.current[key];
        if (value === fallback || value === '') next.delete(key);
        else if (!allowed || allowed.includes(value)) next.set(key, value);
      }
      return next;
    }, { replace: true });
  }, [setParams]);
  return [values, update];
}

/** One query-string parameter as state. */
export function useUrlParam<const T extends string>(key: string, fallback: NoInfer<T>, allowed: readonly T[]): [T, (value: T) => void];
export function useUrlParam(key: string, fallback: string): [string, (value: string) => void];
export function useUrlParam<const T extends string>(key: string, fallback: T, allowed?: readonly T[]): [T, (value: T) => void] {
  const [values, update] = useUrlParams({ [key]: allowed ? { fallback, allowed } : { fallback } });
  return [values[key] as T, useCallback((value: T) => update({ [key]: value }), [update, key])];
}
