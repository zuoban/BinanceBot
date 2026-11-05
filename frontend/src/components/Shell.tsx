import { useCallback, useEffect, useMemo, useState } from 'react';
import { Link, Outlet, useBlocker, useLocation, useNavigate } from 'react-router-dom';
import { App, Button, Drawer } from 'antd';
import { AppstoreOutlined, ArrowDownOutlined, ArrowUpOutlined, BulbOutlined, EditOutlined, LineChartOutlined, LogoutOutlined, MenuOutlined, PlusOutlined, SearchOutlined, SettingOutlined } from '@ant-design/icons';
import { useLiveAccounts, useOverview, usePairs } from '../lib/queries';
import { AccountsContext, EditContext, useSession } from '../lib/session';
import { accountGroups, number, statusLabel, trimFractionZeros } from '../lib/format';
import type { Mode, Pair } from '../lib/types';
import { ControlProvider } from './Controls';
import { CommandPalette, type Command } from './CommandPalette';
import { ThemeSegmented, themeLabels } from './ThemeSwitch';
import { useTheme, type ThemePreference } from '../lib/theme';
import { defaultTitle, useDocumentTitle } from '../lib/documentTitle';
import { ModeTag } from './Common';

export default function Shell() {
  const navigate = useNavigate(), location = useLocation();
  const pairs = usePairs();
  const overview = useOverview();
  const accounts = useLiveAccounts(overview.data, overview.dataUpdatedAt);
  const [dirty, setDirty] = useState(false), [saving, setSaving] = useState(false);
  const { modal, message } = App.useApp();
  const { logout, authEnabled } = useSession();
  const { preference, setPreference } = useTheme();
  const groups = accountGroups(accounts);
  useDocumentTitle(location.pathname.match(/^\/pairs\/(?!new\/?$)[^/]+\/?$/) ? null : groups.length ? groups.map(group => {
    return `${trimFractionZeros(number(group.equity))} | ${group.ratio === null ? '--' : `${trimFractionZeros(number(group.ratio))}%`}`;
  }).join(' / ') : defaultTitle);
  const blocker = useBlocker(({ currentLocation, nextLocation }) => (dirty || saving) && currentLocation.pathname !== nextLocation.pathname);
  useEffect(() => {
    if (blocker.state !== 'blocked') return;
    if (saving) { blocker.reset(); return; }
    let active = true;
    void (async () => {
      const confirmed = await modal.confirm({ title: '放弃未保存的修改？', content: '当前表单的修改尚未保存。', okText: '放弃修改', cancelText: '继续编辑' });
      if (!active) return;
      if (confirmed) { setDirty(false); blocker.proceed(); } else blocker.reset();
    })();
    return () => { active = false; };
  }, [blocker, saving, modal]);
  useEffect(() => {
    if (!dirty && !saving) return;
    const beforeUnload = (e: BeforeUnloadEvent) => { e.preventDefault(); e.returnValue = ''; };
    window.addEventListener('beforeunload', beforeUnload);
    return () => window.removeEventListener('beforeunload', beforeUnload);
  }, [dirty, saving]);
  const symbol = location.pathname.match(/^\/pairs\/([^/]+)/)?.[1];
  const context = useMemo(() => ({ dirty, setDirty, saving, setSaving }), [dirty, saving]);
  const logOut = useCallback(async () => {
    if (saving) return;
    if (dirty && !await modal.confirm({ title: '放弃未保存的修改并退出？', okText: '退出', cancelText: '继续编辑' })) return;
    try { await logout(); } catch { message.error('退出失败，会话可能仍然有效，请重试'); }
  }, [dirty, saving, logout, modal, message]);
  const [drawer, setDrawer] = useState(false), [palette, setPalette] = useState(false);
  useEffect(() => { setDrawer(false); }, [location.pathname]);
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') { event.preventDefault(); setPalette(open => !open); }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);
  const commands = useMemo<Command[]>(() => [
    { key: 'go-overview', group: '页面', label: '策略总览', keywords: 'overview home 总览 首页', icon: <AppstoreOutlined />, run: () => navigate('/') },
    { key: 'go-settings', group: '页面', label: '全局设置', keywords: 'settings 设置 api telegram', icon: <SettingOutlined />, run: () => navigate('/settings') },
    { key: 'go-new', group: '页面', label: '新增交易对', keywords: 'new add 新增 添加', icon: <PlusOutlined />, run: () => navigate('/pairs/new') },
    ...(pairs.data ?? []).flatMap(pair => [
      { key: `pair-${pair.symbol}`, group: '交易对', label: pair.symbol, hint: `${statusLabel[pair.status]} · ${pair.grid_config.direction === 'SHORT' ? '做空' : '做多'}`, keywords: '策略 查看', icon: <LineChartOutlined />, run: () => navigate(`/pairs/${pair.symbol}`) },
      { key: `config-${pair.symbol}`, group: '交易对', label: `${pair.symbol} 策略参数`, hint: '编辑', keywords: 'config 配置 参数', icon: <EditOutlined />, run: () => navigate(`/pairs/${pair.symbol}/config`) },
    ]),
    ...(Object.entries(themeLabels) as [ThemePreference, string][]).map(([key, label]) => ({ key: `theme-${key}`, group: '外观', label: `主题：${label}`, hint: preference === key ? '当前' : undefined, keywords: 'theme dark light 主题 深色 浅色', icon: <BulbOutlined />, run: () => setPreference(key) })),
    ...(authEnabled ? [{ key: 'logout', group: '账户', label: '退出登录', keywords: 'logout 退出', icon: <LogoutOutlined />, disabled: saving, run: () => void logOut() }] : []),
  ], [pairs.data, navigate, preference, setPreference, authEnabled, saving, logOut]);
  const mode = pairs.data?.[0]?.mode;
  const sidebarProps = { pairs: pairs.data, pending: pairs.isPending, symbol, pathname: location.pathname, mode, authEnabled, saving, onSearch: () => { setDrawer(false); setPalette(true); }, onLogout: () => void logOut() };
  return <EditContext.Provider value={context}><AccountsContext.Provider value={accounts}><ControlProvider><div className="app-layout">
    <aside className="sidebar"><SidebarBody idPrefix="rail" {...sidebarProps} /></aside>
    <header className="mobilebar">
      <Link className="sidebar-brand" to="/" aria-label="Grid Console 首页"><img className="sidebar-logo" src="/logo.svg" alt="" /><span>Grid Console</span></Link>
      <div className="mobilebar-actions">
        {mode && <ModeTag mode={mode} />}
        <Button type="text" icon={<SearchOutlined />} aria-label="打开命令面板" onClick={() => setPalette(true)} />
        <Button type="text" icon={<MenuOutlined />} aria-label="打开主导航" onClick={() => setDrawer(true)} />
      </div>
    </header>
    <Drawer open={drawer} onClose={() => setDrawer(false)} placement="left" size={280} closable={false} className="sidebar-drawer" title={null} styles={{ body: { padding: 0 } }} aria-label="主导航"><SidebarBody idPrefix="drawer" {...sidebarProps} /></Drawer>
    <main className="content" id="main"><Outlet /></main>
    <CommandPalette open={palette} onClose={() => setPalette(false)} commands={commands} />
  </div></ControlProvider></AccountsContext.Provider></EditContext.Provider>;
}

interface SidebarProps { idPrefix: string; pairs: Pair[] | undefined; pending: boolean; symbol: string | undefined; pathname: string; mode: Mode | undefined; authEnabled: boolean; saving: boolean; onSearch: () => void; onLogout: () => void }

function SidebarBody({ idPrefix, pairs, pending, symbol, pathname, mode, authEnabled, saving, onSearch, onLogout }: SidebarProps) {
  const isMac = typeof navigator !== 'undefined' && /Mac|iPhone|iPad/.test(navigator.platform);
  const page = pathname.startsWith('/settings') ? 'settings' : pathname.startsWith('/pairs/') ? 'pair' : 'overview';
  return <div className="sidebar-inner">
    <Link className="sidebar-brand" to="/" aria-label="Grid Console 首页"><img className="sidebar-logo" src="/logo.svg" alt="" /><span className="sidebar-brand-name"><strong>Grid Console</strong><small>Binance Futures</small></span></Link>
    <button type="button" className="sidebar-search" onClick={onSearch} aria-label="打开命令面板"><SearchOutlined aria-hidden="true" /><span>搜索与跳转</span><kbd aria-hidden="true">{isMac ? '⌘K' : 'Ctrl K'}</kbd></button>
    <nav className="sidebar-nav" aria-label="主导航">
      <Link to="/" className={`sidebar-link${page === 'overview' ? ' sidebar-link-active' : ''}`} aria-current={page === 'overview' ? 'page' : undefined}><AppstoreOutlined aria-hidden="true" />策略总览</Link>
      <Link to="/settings" className={`sidebar-link${page === 'settings' ? ' sidebar-link-active' : ''}`} aria-current={page === 'settings' ? 'page' : undefined}><SettingOutlined aria-hidden="true" />全局设置</Link>
    </nav>
    <div className="sidebar-section"><span id={`${idPrefix}-pairs-label`}>交易对</span><Link to="/pairs/new" className="sidebar-add" aria-label="新增交易对" title="新增交易对"><PlusOutlined aria-hidden="true" /></Link></div>
    <nav className="sidebar-pairs" aria-labelledby={`${idPrefix}-pairs-label`}>
      {pending && !pairs && [0, 1, 2].map(item => <div className="sidebar-pair-skeleton" key={item} aria-hidden="true" />)}
      {pairs?.map(pair => {
        const short = pair.grid_config.direction === 'SHORT';
        const active = pair.symbol === symbol;
        return <Link key={pair.symbol} to={`/pairs/${pair.symbol}${pathname.endsWith('/config') ? '/config' : ''}`} className={`sidebar-pair${active ? ' sidebar-pair-active' : ''}`} aria-current={active ? 'page' : undefined}>
          <span className={`status-dot status-dot-${pair.status.toLowerCase()}`} aria-hidden="true" />
          <span className="sidebar-pair-symbol">{pair.symbol}</span>
          <span className={`sidebar-pair-side ${short ? 'short' : 'long'}`} aria-hidden="true">{short ? <ArrowDownOutlined /> : <ArrowUpOutlined />}</span>
          <span className="sr-only">{statusLabel[pair.status]}，{short ? '做空' : '做多'}</span>
        </Link>;
      })}
      {pairs && !pairs.length && <p className="sidebar-empty">暂无交易对</p>}
    </nav>
    <div className="sidebar-foot">
      <div className="sidebar-foot-row">{mode ? <ModeTag mode={mode} /> : <span />}<ThemeSegmented /></div>
      <div className="sidebar-foot-row">
        <small>v{import.meta.env.VITE_APP_VERSION}</small>
        {authEnabled && <Button type="text" size="small" icon={<LogoutOutlined />} disabled={saving} onClick={onLogout}>退出</Button>}
      </div>
    </div>
  </div>;
}
