import { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Link, Outlet, useBlocker, useLocation } from 'react-router-dom';
import { App, Button, Drawer } from 'antd';
import { AppstoreOutlined, ArrowDownOutlined, ArrowUpOutlined, LineChartOutlined, LogoutOutlined, MenuOutlined, PlusOutlined, SearchOutlined, SettingOutlined } from '@ant-design/icons';
import { pairRoute } from '../lib/api';
import { useAsyncLock } from '../lib/asyncGuard';
import { useConfirmDialog } from '../lib/confirmDialog';
import { useLiveAccounts, usePairs } from '../lib/queries';
import { AccountsContext, EditContext, useSession } from '../lib/session';
import { compactNumber, directionLabel, ratioText, statusLabel } from '../lib/format';
import { PAIRS_STALE_MS } from '../lib/portfolio';
import type { AccountObservation, Mode, Pair } from '../lib/types';
import { useAccountGroups, useExpired } from '../lib/useNow';
import { ControlProvider } from './Controls';
import { PaletteHost, routeSymbol } from './CommandPalette';
import { ThemeSegmented } from './ThemeSwitch';
import { defaultTitle, useDocumentTitle } from '../lib/documentTitle';
import { ModeTag } from './Common';

function ShellTitle({ accounts, alert, active }: { accounts: AccountObservation[]; alert: boolean; active: boolean }) {
  const groups = useAccountGroups(accounts, active);
  const title = groups.length ? groups.map(group => `${compactNumber(group.equity)} | ${ratioText(group.ratio, true)}`).join(' / ') : defaultTitle;
  useDocumentTitle(active ? `${alert ? '[!] ' : ''}${title}` : null);
  return null;
}

function useLivePairStatuses(pairsData: Pair[] | undefined, accounts: AccountObservation[]) {
  const accountPairs = useMemo(() => new Map(accounts.map(entry => [entry.snapshot.symbol, entry])), [accounts]);
  const pairCache = useRef<{ overrides: WeakMap<Pair, Pair>; current?: Pair[] }>({ overrides: new WeakMap() });
  const currentPairs = useMemo(() => {
    if (!pairsData) return undefined;
    let changed = false;
    const mapped = pairsData.map(pair => {
      const observation = accountPairs.get(pair.symbol);
      if (observation?.mode === pair.mode && !observation.account_observation_stale && observation.snapshot.status !== pair.status) {
        changed = true;
        const cached = pairCache.current.overrides.get(pair);
        if (cached?.status === observation.snapshot.status) return cached;
        const updated = { ...pair, status: observation.snapshot.status };
        pairCache.current.overrides.set(pair, updated);
        return updated;
      }
      return pair;
    });
    const previous = pairCache.current.current;
    const result = !changed ? pairsData : previous?.length === mapped.length && mapped.every((pair, index) => pair === previous[index]) ? previous : mapped;
    pairCache.current.current = result;
    return result;
  }, [pairsData, accountPairs]);
  const pairModeChanged = useMemo(() => pairsData?.some(pair => {
    const observation = accountPairs.get(pair.symbol);
    return observation && observation.mode !== pair.mode;
  }), [pairsData, accountPairs]);
  const mode = useMemo<Mode | undefined>(() => {
    const pairModes = currentPairs?.map(pair => accountPairs.get(pair.symbol)?.mode ?? pair.mode) ?? [];
    const observed = accounts.length ? [...accounts.map(entry => entry.mode), ...pairModes] : pairModes;
    if (!observed.length) return undefined;
    if (observed.includes('LIVE')) return 'LIVE';
    if (observed.includes('UNKNOWN')) return 'UNKNOWN';
    return 'PAPER';
  }, [accounts, accountPairs, currentPairs]);
  return { currentPairs, pairModeChanged, mode };
}

function useDirtyNavigationBlocker(dirty: boolean, saving: boolean, setDirty: (value: boolean) => void) {
  const blockerDialog = useConfirmDialog();
  const blocker = useBlocker(({ currentLocation, nextLocation }) => (dirty || saving) && currentLocation.pathname !== nextLocation.pathname);
  useEffect(() => {
    if (blocker.state !== 'blocked') return;
    if (saving) { blocker.reset(); return; }
    let active = true;
    void (async () => {
      const confirmed = await blockerDialog.confirm({ title: '放弃未保存的修改？', content: '当前表单的修改尚未保存。', okText: '放弃修改', cancelText: '继续编辑' });
      if (!active) return;
      if (confirmed) { setDirty(false); blocker.proceed(); } else blocker.reset();
    })();
    return () => { active = false; blockerDialog.destroyAll(); };
  }, [blocker, saving, setDirty, blockerDialog.confirm, blockerDialog.destroyAll]);
  useEffect(() => {
    if (!dirty && !saving) return;
    const beforeUnload = (e: BeforeUnloadEvent) => { e.preventDefault(); e.returnValue = ''; };
    window.addEventListener('beforeunload', beforeUnload);
    return () => window.removeEventListener('beforeunload', beforeUnload);
  }, [dirty, saving]);
}

export default function Shell() {
  const location = useLocation(), pairs = usePairs(), liveAccounts = useLiveAccounts();
  const accounts = liveAccounts.accounts;
  const { currentPairs, pairModeChanged, mode } = useLivePairStatuses(pairs.data, accounts);
  const [dirty, setDirty] = useState(false), [saving, setSaving] = useState(false);
  const savingRef = useRef(false);
  savingRef.current = saving;
  const updateSaving = useCallback((value: boolean) => { savingRef.current = value; setSaving(value); }, []);
  useDirtyNavigationBlocker(dirty, saving, setDirty);
  const context = useMemo(() => ({ dirty, setDirty, saving, setSaving: updateSaving }), [dirty, saving, updateSaving]);
  const runLogout = useAsyncLock(), { message } = App.useApp(), logoutDialog = useConfirmDialog(), { logout, authEnabled } = useSession();
  const logOut = useCallback(() => runLogout(async () => {
    if (savingRef.current) return;
    if (dirty && !await logoutDialog.confirm({ title: '放弃未保存的修改并退出？', okText: '退出', cancelText: '继续编辑' })) return;
    if (savingRef.current) return;
    try { await logout(); } catch { message.error('退出失败，会话可能仍然有效，请重试'); }
  }), [dirty, logout, logoutDialog.confirm, message, runLogout]);
  const pairsStale = useExpired(pairs.data ? pairs.dataUpdatedAt + PAIRS_STALE_MS : -Infinity);
  const alert = !!liveAccounts.error || liveAccounts.stale || accounts.some(entry => entry.account_observation_stale || entry.snapshot.status === 'ERROR');
  const symbol = routeSymbol(location.pathname);
  const [drawer, setDrawer] = useState(false), [palette, setPalette] = useState(false);
  useEffect(() => { setDrawer(false); }, [location.pathname]);
  const page: SidebarProps['page'] = location.pathname.startsWith('/settings') ? 'settings' : location.pathname.startsWith('/pairs/') ? 'pair' : 'overview';
  const closeDrawer = useCallback(() => setDrawer(false), []);
  const openPalette = useCallback(() => { if (saving) return; setDrawer(false); setPalette(true); }, [saving]);
  const handleLogout = useCallback(() => void logOut(), [logOut]);
  const sidebarProps = { pairs: currentPairs, pending: pairs.isPending, symbol, page, pathname: location.pathname, mode, authEnabled, saving, onNavigate: closeDrawer, onSearch: openPalette, onLogout: handleLogout };
  return <EditContext.Provider value={context}><AccountsContext.Provider value={accounts}><ControlProvider pairs={currentPairs}><div className="app-layout">
    <ShellTitle accounts={accounts} alert={alert} active={!symbol || location.pathname.endsWith('/config')} />
    <a className="skip-link" href="#main">跳到主内容</a>
    <aside className="sidebar"><SidebarBody idPrefix="rail" {...sidebarProps} /></aside>
    <header className="mobilebar">
      <Link className="sidebar-brand" to="/" aria-label="Grid Console 首页"><img className="sidebar-logo" src="/logo.svg" alt="" /><span>Grid Console</span></Link>
      <div className="mobilebar-actions">
        {mode && <ModeTag mode={mode} />}
        <Button type="text" icon={<SearchOutlined />} aria-label="打开命令面板" disabled={saving} onClick={openPalette} />
        <Button type="text" icon={<MenuOutlined />} aria-label="打开主导航" aria-expanded={drawer} onClick={() => setDrawer(true)} />
      </div>
    </header>
    <Drawer open={drawer} onClose={closeDrawer} destroyOnHidden placement="left" size={280} closable={false} className="sidebar-drawer" title={null} styles={{ body: { padding: 0 } }} aria-label="主导航"><SidebarBody idPrefix="drawer" {...sidebarProps} /></Drawer>
    <main className="content" id="main"><Outlet /></main>
    <nav className="bottombar" aria-label="底部导航">
      <Link to="/" className={`bottombar-item${page === 'overview' ? ' bottombar-item-active' : ''}`} aria-current={page === 'overview' ? 'page' : undefined}><AppstoreOutlined aria-hidden="true" /><span>总览</span></Link>
      <button type="button" className={`bottombar-item${page === 'pair' ? ' bottombar-item-active' : ''}`} aria-label="切换或查看交易对" aria-expanded={drawer} onClick={() => setDrawer(open => !open)}><LineChartOutlined aria-hidden="true" /><span>交易对</span></button>
      <Link to="/settings" className={`bottombar-item${page === 'settings' ? ' bottombar-item-active' : ''}`} aria-current={page === 'settings' ? 'page' : undefined}><SettingOutlined aria-hidden="true" /><span>设置</span></Link>
    </nav>
    <PaletteHost open={palette} setOpen={setPalette} pairs={currentPairs} stale={!!pairs.error || !!liveAccounts.error || liveAccounts.stale || !!pairModeChanged || pairsStale} authEnabled={authEnabled} saving={saving} onLogout={logOut} />
  </div></ControlProvider></AccountsContext.Provider></EditContext.Provider>;
}

interface SidebarProps { idPrefix: string; pairs: Pair[] | undefined; pending: boolean; symbol: string | undefined; page: 'overview' | 'pair' | 'settings'; pathname: string; mode: Mode | undefined; authEnabled: boolean; saving: boolean; onNavigate: () => void; onSearch: () => void; onLogout: () => void }

const SidebarBody = memo(function SidebarBody({ idPrefix, pairs, pending, symbol, page, pathname, mode, authEnabled, saving, onNavigate, onSearch, onLogout }: SidebarProps) {
  const isMac = typeof navigator !== 'undefined' && /Mac|iPhone|iPad/i.test((navigator as Navigator & { userAgentData?: { platform?: string } }).userAgentData?.platform ?? navigator.platform);
  const pairsRef = useRef<HTMLElement>(null);
  useEffect(() => {
    const container = pairsRef.current;
    const activeNode = container?.querySelector<HTMLElement>('[aria-current="page"]');
    if (!container || !activeNode || container.scrollHeight <= container.clientHeight) return;
    const top = activeNode.offsetTop, bottom = top + activeNode.offsetHeight;
    if (top < container.scrollTop) container.scrollTop = top;
    else if (bottom > container.scrollTop + container.clientHeight) container.scrollTop = bottom - container.clientHeight;
  }, [symbol, pairs?.length]);
  return <div className="sidebar-inner">
    <Link className="sidebar-brand" to="/" aria-label="Grid Console 首页" onClick={onNavigate}><img className="sidebar-logo" src="/logo.svg" alt="" /><span className="sidebar-brand-name"><strong>Grid Console</strong><small>Binance Futures</small></span></Link>
    <button type="button" className="sidebar-search" disabled={saving} onClick={onSearch} aria-label="打开命令面板"><SearchOutlined aria-hidden="true" /><span>搜索与跳转</span><kbd aria-hidden="true">{isMac ? '⌘K' : 'Ctrl K'}</kbd></button>
    <nav className="sidebar-nav" aria-label="主导航">
      <Link to="/" onClick={onNavigate} className={`sidebar-link${page === 'overview' ? ' sidebar-link-active' : ''}`} aria-current={page === 'overview' ? 'page' : undefined}><AppstoreOutlined aria-hidden="true" />策略总览</Link>
      <Link to="/settings" onClick={onNavigate} className={`sidebar-link${page === 'settings' ? ' sidebar-link-active' : ''}`} aria-current={page === 'settings' ? 'page' : undefined}><SettingOutlined aria-hidden="true" />全局设置</Link>
    </nav>
    <div className="sidebar-section"><span id={`${idPrefix}-pairs-label`}>交易对</span><Link to="/pairs/new" onClick={onNavigate} className="sidebar-add" aria-label="新增交易对" aria-current={pathname === '/pairs/new' ? 'page' : undefined} title="新增交易对"><PlusOutlined aria-hidden="true" /></Link></div>
    <nav ref={pairsRef} className="sidebar-pairs" aria-labelledby={`${idPrefix}-pairs-label`}>
      {pending && !pairs && [0, 1, 2].map(item => <div className="sidebar-pair-skeleton" key={item} aria-hidden="true" />)}
      {pairs?.map(pair => {
        const short = pair.grid_config.direction === 'SHORT';
        const active = pair.symbol === symbol;
        return <Link key={pair.symbol} to={pairRoute(pair.symbol, pathname.endsWith('/config'))} onClick={onNavigate} className={`sidebar-pair${active ? ' sidebar-pair-active' : ''}`} aria-current={active ? 'page' : undefined} title={`${pair.symbol} · ${statusLabel[pair.status]} · ${directionLabel(pair.grid_config.direction)}`}>
          <span className="sidebar-icon-slot" aria-hidden="true"><span className={`status-dot status-dot-${pair.status.toLowerCase()}`} /></span>
          <span className="sidebar-pair-symbol">{pair.symbol}</span>
          <span className={`sidebar-pair-side ${short ? 'short' : 'long'}`} aria-hidden="true">{short ? <ArrowDownOutlined /> : <ArrowUpOutlined />}</span>
          <span className="sr-only">{statusLabel[pair.status]}，{directionLabel(pair.grid_config.direction)}</span>
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
});
