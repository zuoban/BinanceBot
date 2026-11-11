import React, { Suspense, lazy, useCallback, useEffect, useMemo, useState } from 'react';
import ReactDOM from 'react-dom/client';
import { App as AntApp, Button, Modal } from 'antd';
import { QueryClient, QueryClientProvider, useQuery } from '@tanstack/react-query';
import { createBrowserRouter, Navigate, RouterProvider, useLocation } from 'react-router-dom';
import { api, ApiError, post, tokenStore } from './lib/api';
import type { AuthStatus } from './lib/types';
import { SessionContext } from './lib/session';
const Login = lazy(() => import('./pages/Login'));
import { DetailPageLoading, LoadingScreen, OverviewPageLoading, PageLoading } from './components/Loading';
import { LoadErrorBoundary } from './components/LoadErrorBoundary';
import { ThemeProvider } from './lib/theme';
import './style.css';

const Shell = lazy(() => import('./components/Shell'));
const Overview = lazy(() => import('./pages/Overview'));
const PairDetail = lazy(() => import('./pages/PairDetail'));
const PairConfig = lazy(() => import('./pages/PairConfig'));
const Settings = lazy(() => import('./pages/Settings'));
function RouteBoundary({ component, label = '页面', fallback = <PageLoading /> }: { component: React.ReactNode; label?: string; fallback?: React.ReactNode }) {
  const { pathname } = useLocation();
  return <LoadErrorBoundary label={label} resetKey={pathname}><Suspense fallback={fallback}>{component}</Suspense></LoadErrorBoundary>;
}
const client = new QueryClient({ defaultOptions: { queries: { retry: 1, refetchOnWindowFocus: true }, mutations: { retry: false } } });
const router = createBrowserRouter([{ element: <RouteBoundary component={<Shell />} label="控制台" fallback={<LoadingScreen label="正在加载控制台" />} />, children: [
  { path: '/', element: <RouteBoundary component={<Overview />} label="策略总览" fallback={<OverviewPageLoading />} /> },
  { path: '/pairs/new', element: <RouteBoundary component={<PairConfig />} label="新增交易对" /> },
  { path: '/pairs/:symbol', element: <RouteBoundary component={<PairDetail />} label="策略详情" fallback={<DetailPageLoading />} /> },
  { path: '/pairs/:symbol/config', element: <RouteBoundary component={<PairConfig />} label="策略参数" /> },
  { path: '/settings', element: <RouteBoundary component={<Settings />} label="全局设置" /> },
  { path: '*', element: <Navigate to="/" replace /> },
] }]);

function Root() {
  const [loggedIn, setLoggedIn] = useState(!!tokenStore.get());
  const authStatus = useQuery({ queryKey: ['auth-status'], queryFn: ({ signal }) => api<AuthStatus>('/api/auth/status', { signal }), retry: false });
  const authEnabled = authStatus.data?.initialized !== false;
  const logout = useCallback(async () => {
    const token = tokenStore.get();
    if (token) {
      try { await post('/api/auth/logout', {}); }
      catch (error) { if (!(error instanceof ApiError && error.status === 401)) throw error; }
    }
    if (tokenStore.get() !== token) return;
    Modal.destroyAll(); tokenStore.clear(); client.clear(); setLoggedIn(false);
  }, []);
  useEffect(() => {
    const expire = () => { Modal.destroyAll(); tokenStore.clear(); client.clear(); setLoggedIn(false); };
    window.addEventListener('auth-expired', expire);
    return () => window.removeEventListener('auth-expired', expire);
  }, []);
  useEffect(() => { if (loggedIn && authEnabled && authStatus.data && !authStatus.data.authenticated) { Modal.destroyAll(); tokenStore.clear(); client.removeQueries({ predicate: query => query.queryKey[0] !== 'auth-status' }); setLoggedIn(false); } }, [loggedIn, authEnabled, authStatus.data]);
  const session = useMemo(() => ({ logout, authEnabled }), [logout, authEnabled]);
  if (authStatus.isPending) return <LoadingScreen label={loggedIn ? '正在验证会话' : '正在连接控制台'} />;
  if (authStatus.error && !authStatus.data) return <div className="loading-screen" role="alert"><div className="loading-brand"><img className="brand-symbol" src="/logo.svg" alt="" /><span>Grid Console<small>Strategy Workspace</small></span></div><p>无法连接后端服务：{authStatus.error.message}</p><Button onClick={() => void authStatus.refetch()}>重试</Button></div>;
  if (!authEnabled) return <SessionContext.Provider value={session}><RouterProvider router={router} /></SessionContext.Provider>;
  return <SessionContext.Provider value={session}>{loggedIn && authStatus.data?.authenticated ? <RouterProvider router={router} /> : <LoadErrorBoundary label="登录页"><Suspense fallback={<LoadingScreen label="正在加载登录页" />}><Login onLogin={async () => { await client.cancelQueries({ queryKey: ['auth-status'] }); Modal.destroyAll(); client.clear(); client.setQueryData(['auth-status'], { initialized: true, authenticated: true }); setLoggedIn(true); }} /></Suspense></LoadErrorBoundary>}</SessionContext.Provider>;
}

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode><ThemeProvider>
    <AntApp><QueryClientProvider client={client}><Root /></QueryClientProvider></AntApp>
  </ThemeProvider></React.StrictMode>,
);
