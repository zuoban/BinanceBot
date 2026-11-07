import React, { Suspense, lazy, useCallback, useEffect, useState } from 'react';
import ReactDOM from 'react-dom/client';
import { App as AntApp } from 'antd';
import { QueryClient, QueryClientProvider, useQuery } from '@tanstack/react-query';
import { createBrowserRouter, RouterProvider } from 'react-router-dom';
import { api, ApiError, post, tokenStore } from './lib/api';
import type { AuthStatus } from './lib/types';
import { SessionContext } from './lib/session';
import Login from './pages/Login';
import { DetailPageLoading, LoadingScreen, OverviewPageLoading, PageLoading } from './components/Loading';
import { LoadErrorBoundary } from './components/LoadErrorBoundary';
import { ThemeProvider } from './lib/theme';
import './style.css';

const Shell = lazy(() => import('./components/Shell'));
const Overview = lazy(() => import('./pages/Overview'));
const PairDetail = lazy(() => import('./pages/PairDetail'));
const PairConfig = lazy(() => import('./pages/PairConfig'));
const Settings = lazy(() => import('./pages/Settings'));
const page = (component: React.ReactNode, label = '页面', fallback: React.ReactNode = <PageLoading />) => <LoadErrorBoundary label={label}><Suspense fallback={fallback}>{component}</Suspense></LoadErrorBoundary>;
const client = new QueryClient({ defaultOptions: { queries: { retry: 1, refetchOnWindowFocus: true }, mutations: { retry: false } } });
const router = createBrowserRouter([{ element: <LoadErrorBoundary label="控制台"><Suspense fallback={<LoadingScreen label="正在加载控制台" />}><Shell /></Suspense></LoadErrorBoundary>, children: [
  { path: '/', element: page(<Overview />, '策略总览', <OverviewPageLoading />) },
  { path: '/pairs/new', element: page(<PairConfig />, '新增交易对') },
  { path: '/pairs/:symbol', element: page(<PairDetail />, '策略详情', <DetailPageLoading />) },
  { path: '/pairs/:symbol/config', element: page(<PairConfig />, '策略参数') },
  { path: '/settings', element: page(<Settings />, '全局设置') },
] }]);

function Root() {
  const [loggedIn, setLoggedIn] = useState(!!tokenStore.get());
  const authStatus = useQuery({ queryKey: ['auth-status'], queryFn: () => api<AuthStatus>('/api/auth/status'), retry: false });
  const authEnabled = authStatus.data?.initialized !== false;
  const logout = useCallback(async (revoked = false) => {
    const token = tokenStore.get();
    if (!revoked && token) {
      try { await post('/api/auth/logout', {}); }
      catch (error) { if (!(error instanceof ApiError && error.status === 401)) throw error; }
    }
    if (tokenStore.get() !== token) return;
    tokenStore.clear(); client.clear(); setLoggedIn(false);
  }, []);
  useEffect(() => {
    const expire = () => { tokenStore.clear(); client.clear(); setLoggedIn(false); };
    window.addEventListener('auth-expired', expire);
    return () => window.removeEventListener('auth-expired', expire);
  }, []);
  useEffect(() => { if (loggedIn && authEnabled && authStatus.data && !authStatus.data.authenticated) { tokenStore.clear(); client.removeQueries({ predicate: query => query.queryKey[0] !== 'auth-status' }); setLoggedIn(false); } }, [loggedIn, authEnabled, authStatus.data]);
  const session = { logout, authEnabled };
  if (authStatus.isPending) return <LoadingScreen label={loggedIn ? '正在验证会话' : '正在连接控制台'} />;
  if (authStatus.error) return <div className="loading-screen"><div>无法连接后端服务：{authStatus.error.message}<br /><button type="button" onClick={() => void authStatus.refetch()}>重试</button></div></div>;
  if (!authEnabled) return <SessionContext.Provider value={session}><RouterProvider router={router} /></SessionContext.Provider>;
  return <SessionContext.Provider value={session}>{loggedIn && authStatus.data?.authenticated ? <RouterProvider router={router} /> : <Login onLogin={() => { client.clear(); setLoggedIn(true); }} />}</SessionContext.Provider>;
}

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode><ThemeProvider>
    <AntApp><QueryClientProvider client={client}><Root /></QueryClientProvider></AntApp>
  </ThemeProvider></React.StrictMode>,
);
