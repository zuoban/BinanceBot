import { createContext, useContext } from 'react';
import type { AccountObservation } from './types';

export const SessionContext = createContext<{ logout: () => Promise<void>; authEnabled: boolean }>({ logout: async () => {}, authEnabled: true });
export const useSession = () => useContext(SessionContext);
export const AccountsContext = createContext<AccountObservation[]>([]);
export type RouteLeaveListener = (nextPathname: string) => void;
export const EditContext = createContext<{
  dirty: boolean;
  setDirty: (dirty: boolean) => void;
  saving: boolean;
  setSaving: (saving: boolean) => void;
  subscribeRouteLeave?: (listener: RouteLeaveListener) => () => void;
}>({
  dirty: false,
  setDirty: () => {},
  saving: false,
  setSaving: () => {},
  subscribeRouteLeave: () => () => {},
});
