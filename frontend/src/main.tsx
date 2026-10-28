import React, { Suspense, lazy, useCallback, useEffect, useState } from 'react';
import ReactDOM from 'react-dom/client';
import { App as AntApp } from 'antd';
import { QueryClient, QueryClientProvider, useQuery } from '@tanstack/react-query';
import { createBrowserRouter, RouterProvider } from 'react-router-dom';
import { api, post, tokenStore } from './lib/api';
import type { AuthStatus } from './lib/types';
import { SessionContext } from './lib/session';
import Shell from './components/Shell';
import Login from './pages/Login';
import { LoadingScreen, PageLoading } from './components/Loading';
import { ThemeProvider } from './lib/theme';
import './style.css';

const Overview = lazy(() => import('./pages/Overview'));
const PairDetail = lazy(() => import('./pages/PairDetail'));
const PairConfig = lazy(() => import('./pages/PairConfig'));
const Settings = lazy(() => import('./pages/Settings'));
const page = (component: React.ReactNode) => <Suspense fallback={<PageLoading />}>{component}</Suspense>;
const client = new QueryClient({ defaultOptions: { queries: { retry: 1, refetchOnWindowFocus: true }, mutations: { retry: false } } });
const router = createBrowserRouter([{ element: <Shell />, children: [
  { path: '/', element: page(<Overview />) },
  { path: '/pairs/new', element: page(<PairConfig />) },
  { path: '/pairs/:symbol', element: page(<PairDetail />) },
  { path: '/pairs/:symbol/config', element: page(<PairConfig />) },
  { path: '/settings', element: page(<Settings />) },
] }]);

function Root() {
  const [loggedIn, setLoggedIn] = useState(!!tokenStore.get());
  const status = useQuery({ queryKey: ['verify-session', loggedIn], queryFn: () => api<AuthStatus>('/api/auth/status'), enabled: loggedIn, retry: false });
  const logout = useCallback(async () => {
    try { if (tokenStore.get()) await post('/api/auth/logout', {}); } catch { /* Local logout still succeeds when offline. */ }
    tokenStore.clear(); client.clear(); setLoggedIn(false);
  }, []);
  useEffect(() => {
    const expire = () => { tokenStore.clear(); client.clear(); setLoggedIn(false); };
    window.addEventListener('auth-expired', expire);
    return () => window.removeEventListener('auth-expired', expire);
  }, []);
  useEffect(() => { if (status.data && !status.data.authenticated) { tokenStore.clear(); client.clear(); setLoggedIn(false); } }, [status.data]);
  const session = { logout };
  if (loggedIn && status.isPending) return <LoadingScreen label="正在验证会话" />;
  if (loggedIn && status.error) return <div className="loading-screen"><div>无法连接后端服务：{status.error.message}<br /><button onClick={() => void status.refetch()}>重试</button></div></div>;
  return <SessionContext.Provider value={session}>{loggedIn && status.data?.authenticated ? <RouterProvider router={router} /> : <Login onLogin={() => { client.clear(); setLoggedIn(true); }} />}</SessionContext.Provider>;
}

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode><ThemeProvider>
    <AntApp><QueryClientProvider client={client}><Root /></QueryClientProvider></AntApp>
  </ThemeProvider></React.StrictMode>,
);
