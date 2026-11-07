import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Link, Outlet, useBlocker, useLocation, useNavigate } from 'react-router-dom';
import { App, Button, Drawer, Modal } from 'antd';
import { AppstoreOutlined, ArrowDownOutlined, ArrowUpOutlined, BulbOutlined, EditOutlined, LineChartOutlined, LogoutOutlined, MenuOutlined, PauseOutlined, PlayCircleOutlined, PlusOutlined, QuestionCircleOutlined, ReloadOutlined, SearchOutlined, SettingOutlined, StopOutlined } from '@ant-design/icons';
import { useLiveAccounts, usePairs } from '../lib/queries';
import { AccountsContext, EditContext, useSession } from '../lib/session';
import { accountGroups, compactNumber, ratioText, statusLabel } from '../lib/format';
import type { Mode, Pair } from '../lib/types';
import { ControlProvider, useControls } from './Controls';
import { CommandPalette, type Command } from './CommandPalette';
import { ThemeSegmented, themeLabels } from './ThemeSwitch';
import { useTheme, type ThemePreference } from '../lib/theme';
import { defaultTitle, useDocumentTitle } from '../lib/documentTitle';
import { ModeTag } from './Common';


function routeSymbol(pathname: string) {
  const symbol = pathname.match(/^\/pairs\/([^/]+)/)?.[1];
  return symbol && symbol !== 'new' ? symbol : undefined;
}
export default function Shell() {
  const location = useLocation();
  const pairs = usePairs();
  const liveAccounts = useLiveAccounts();
  const accounts = liveAccounts.accounts;
  const accountPairs = useMemo(() => new Map(accounts.map(entry => [entry.snapshot.symbol, entry])), [accounts]);
  const currentPairs = useMemo(() => pairs.data?.map(pair => {
    const observation = accountPairs.get(pair.symbol);
    return observation?.mode === pair.mode && !observation.account_observation_stale ? { ...pair, status: observation.snapshot.status } : pair;
  }), [pairs.data, accountPairs]);
  const pairModeChanged = useMemo(() => pairs.data?.some(pair => {
    const observation = accountPairs.get(pair.symbol);
    return observation && observation.mode !== pair.mode;
  }), [pairs.data, accountPairs]);
  const [dirty, setDirty] = useState(false), [saving, setSaving] = useState(false);
  const { modal, message } = App.useApp();
  const { logout, authEnabled } = useSession();
  const groups = accountGroups(accounts, liveAccounts.now);
  const alert = !!liveAccounts.error || liveAccounts.stale || accounts.some(entry => entry.account_observation_stale || entry.snapshot.status === 'ERROR');
  const withAlert = (title: string) => alert ? `⚠ ${title}` : title;
  useDocumentTitle(location.pathname.match(/^\/pairs\/(?!new\/?$)[^/]+\/?$/) ? null : withAlert(groups.length ? groups.map(group => {
    return `${compactNumber(group.equity)} | ${ratioText(group.ratio, true)}`;
  }).join(' / ') : defaultTitle));
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
  const symbol = routeSymbol(location.pathname);
  const context = useMemo(() => ({ dirty, setDirty, saving, setSaving }), [dirty, saving]);
  const logOut = useCallback(async () => {
    if (saving) return;
    if (dirty && !await modal.confirm({ title: '放弃未保存的修改并退出？', okText: '退出', cancelText: '继续编辑' })) return;
    try { await logout(); } catch { message.error('退出失败，会话可能仍然有效，请重试'); }
  }, [dirty, saving, logout, modal, message]);
  const [drawer, setDrawer] = useState(false), [palette, setPalette] = useState(false);
  useEffect(() => { setDrawer(false); }, [location.pathname]);
  const mode = pairs.data?.[0]?.mode;
  const page: SidebarProps['page'] = location.pathname.startsWith('/settings') ? 'settings' : location.pathname.startsWith('/pairs/') ? 'pair' : 'overview';
  const sidebarProps = { pairs: currentPairs, pending: pairs.isPending, symbol, page, pathname: location.pathname, mode, authEnabled, saving, onNavigate: () => setDrawer(false), onSearch: () => { setDrawer(false); setPalette(true); }, onLogout: () => void logOut() };
  return <EditContext.Provider value={context}><AccountsContext.Provider value={accounts}><ControlProvider pairs={currentPairs}><div className="app-layout">
    <a className="skip-link" href="#main">跳到主内容</a>
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
    <nav className="bottombar" aria-label="底部导航">
      <Link to="/" className={`bottombar-item${page === 'overview' ? ' bottombar-item-active' : ''}`} aria-current={page === 'overview' ? 'page' : undefined}><AppstoreOutlined aria-hidden="true" /><span>总览</span></Link>
      <button type="button" className={`bottombar-item${page === 'pair' ? ' bottombar-item-active' : ''}`} aria-label="切换或查看交易对" aria-expanded={drawer} onClick={() => setDrawer(open => !open)}><LineChartOutlined aria-hidden="true" /><span>交易对</span></button>
      <Link to="/settings" className={`bottombar-item${page === 'settings' ? ' bottombar-item-active' : ''}`} aria-current={page === 'settings' ? 'page' : undefined}><SettingOutlined aria-hidden="true" /><span>设置</span></Link>
    </nav>
    <PaletteHost open={palette} setOpen={setPalette} pairs={currentPairs} stale={!!pairs.error || !!liveAccounts.error || liveAccounts.stale || !!pairModeChanged || liveAccounts.now - pairs.dataUpdatedAt > 10_000} authEnabled={authEnabled} saving={saving} onLogout={logOut} />
  </div></ControlProvider></AccountsContext.Provider></EditContext.Provider>;
}

const recentKey = 'binance_bot_recent_commands';
const readRecent = (): string[] => { try { const value = JSON.parse(localStorage.getItem(recentKey) ?? '[]'); return Array.isArray(value) ? value.filter((item): item is string => typeof item === 'string').slice(0, 5) : []; } catch { return []; } };

interface HostProps { open: boolean; setOpen: (open: boolean | ((value: boolean) => boolean)) => void; pairs: Pair[] | undefined; stale: boolean; authEnabled: boolean; saving: boolean; onLogout: () => void | Promise<void> }

/** Owns the command list, the global shortcuts and the recently-used order. Lives inside ControlProvider to reuse its guarded actions. */
function PaletteHost({ open, setOpen, pairs, stale, authEnabled, saving, onLogout }: HostProps) {
  const navigate = useNavigate();
  const { pathname } = useLocation();
  const { run } = useControls();
  const { preference, setPreference } = useTheme();
  const [recent, setRecent] = useState<string[]>(readRecent);
  const [help, setHelp] = useState(false);
  const remember = useCallback((key: string) => {
    setRecent(current => {
      const next = [key, ...current.filter(item => item !== key)].slice(0, 5);
      try { localStorage.setItem(recentKey, JSON.stringify(next)); } catch { /* Recent commands are a convenience only. */ }
      return next;
    });
  }, []);
  const symbols = useMemo(() => pairs?.map(pair => pair.symbol) ?? [], [pairs]);
  const current = routeSymbol(pathname);
  const lead = useRef(0);
  const shortcutState = useRef({ symbols, current, pathname, saving });
  shortcutState.current = { symbols, current, pathname, saving };
  useEffect(() => {
    const typing = (target: EventTarget | null) => target instanceof HTMLElement && (target.isContentEditable || ['INPUT', 'TEXTAREA', 'SELECT'].includes(target.tagName));
    const onKey = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key?.toLowerCase() === 'k') {
        event.preventDefault();
        if (shortcutState.current.saving) return;
        setOpen(value => value ? false : !document.querySelector('.ant-modal-root, .ant-drawer-open'));
        return;
      }
      if (event.metaKey || event.ctrlKey || event.altKey || typing(event.target) || document.querySelector('.ant-modal-root, .ant-drawer-open')) return;
      if (event.key === '?') { event.preventDefault(); setHelp(true); return; }
      if (shortcutState.current.saving) return;
      if (event.key === 'g') { lead.current = Date.now(); return; }
      if (lead.current && Date.now() - lead.current < 1200) {
        lead.current = 0;
        if (event.key === 'o') { navigate('/'); return; }
        if (event.key === 's') { navigate('/settings'); return; }
        if (event.key === 'n') { navigate('/pairs/new'); return; }
      }
      const { symbols: currentSymbols, current: activeSymbol, pathname: currentPathname } = shortcutState.current;
      if ((event.key === '[' || event.key === ']') && currentSymbols.length > 1 && activeSymbol) {
        const index = currentSymbols.indexOf(activeSymbol);
        if (index < 0) return;
        const next = currentSymbols[(index + (event.key === ']' ? 1 : -1) + currentSymbols.length) % currentSymbols.length];
        navigate(`/pairs/${next}${currentPathname.endsWith('/config') ? '/config' : ''}`);
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [navigate, setOpen]);
  const commands = useMemo<Command[]>(() => {
    const all: Command[] = [
      { key: 'go-overview', group: '页面', label: '策略总览', hint: 'g o', keywords: 'overview home 总览 首页', icon: <AppstoreOutlined />, run: () => navigate('/') },
      { key: 'go-settings', group: '页面', label: '全局设置', hint: 'g s', keywords: 'settings 设置 api telegram', icon: <SettingOutlined />, run: () => navigate('/settings') },
      { key: 'go-new', group: '页面', label: '新增交易对', hint: 'g n', keywords: 'new add 新增 添加', icon: <PlusOutlined />, run: () => navigate('/pairs/new') },
      ...(pairs ?? []).flatMap(pair => {
        const running = pair.status === 'RUNNING';
        return [
          { key: `pair-${pair.symbol}`, group: '交易对', label: pair.symbol, hint: `${statusLabel[pair.status]} · ${pair.grid_config.direction === 'SHORT' ? '做空' : '做多'}`, keywords: '策略 查看', icon: <LineChartOutlined />, run: () => navigate(`/pairs/${pair.symbol}`) },
          { key: `config-${pair.symbol}`, group: '交易对', label: `${pair.symbol} 策略参数`, hint: '编辑', keywords: 'config 配置 参数', icon: <EditOutlined />, run: () => navigate(`/pairs/${pair.symbol}/config`) },
          { key: `toggle-${pair.symbol}`, group: '操作', label: `${running ? '暂停' : '恢复'} ${pair.symbol}`, hint: pair.mode === 'LIVE' && !running ? '实盘 · 需确认' : undefined, keywords: running ? 'pause 暂停 停止' : 'resume 恢复 启动', icon: running ? <PauseOutlined /> : <PlayCircleOutlined />, disabled: stale && !running, run: () => void run(pair, running ? 'pause' : 'resume') },
          { key: `rebalance-${pair.symbol}`, group: '操作', label: `重建 ${pair.symbol} 网格`, hint: '需确认', keywords: 'rebalance 重建 网格', icon: <ReloadOutlined />, disabled: stale || !running, run: () => void run(pair, 'rebalance') },
          { key: `cancel-${pair.symbol}`, group: '操作', label: `撤单并暂停 ${pair.symbol}`, hint: '需确认', keywords: 'cancel 撤单', icon: <StopOutlined />, run: () => void run(pair, 'cancel_all') },
        ];
      }),
      ...(Object.entries(themeLabels) as [ThemePreference, string][]).map(([key, label]) => ({ key: `theme-${key}`, group: '外观', label: `主题：${label}`, hint: preference === key ? '当前' : undefined, keywords: 'theme dark light 主题 深色 浅色', icon: <BulbOutlined />, run: () => setPreference(key) })),
      ...(authEnabled ? [{ key: 'logout', group: '账户', label: '退出登录', keywords: 'logout 退出', icon: <LogoutOutlined />, disabled: saving, run: () => void onLogout() }] : []),
      { key: 'shortcuts', group: '帮助', label: '键盘快捷键', hint: '?', keywords: 'help shortcuts 快捷键 帮助', icon: <QuestionCircleOutlined />, run: () => setHelp(true) },
    ];
    const wrapped = all.map(command => ({ ...command, run: () => { remember(command.key); command.run(); } }));
    const byKey = new Map(wrapped.map(command => [command.key, command]));
    const recents = recent.flatMap(key => { const command = byKey.get(key); return command && !command.disabled ? [{ ...command, group: '最近使用', key: `recent-${command.key}` }] : []; });
    return [...recents, ...wrapped];
  }, [pairs, navigate, run, preference, setPreference, authEnabled, saving, onLogout, stale, recent, remember]);
  return <>
    <CommandPalette open={open} onClose={() => setOpen(false)} commands={commands} />
    <Modal open={help} onCancel={() => setHelp(false)} footer={null} title="键盘快捷键" width={440} destroyOnHidden>
      <dl className="shortcut-list">
        <div><dt>打开命令面板</dt><dd><kbd>⌘K</kbd> / <kbd>Ctrl K</kbd></dd></div>
        <div><dt>查看快捷键说明</dt><dd><kbd>?</kbd></dd></div>
        <div><dt>前往策略总览</dt><dd><kbd>g</kbd> <kbd>o</kbd></dd></div>
        <div><dt>前往全局设置</dt><dd><kbd>g</kbd> <kbd>s</kbd></dd></div>
        <div><dt>前往新增交易对</dt><dd><kbd>g</kbd> <kbd>n</kbd></dd></div>
        <div><dt>切换上 / 下一个交易对</dt><dd><kbd>[</kbd> / <kbd>]</kbd></dd></div>
      </dl>
    </Modal>
  </>;
}

interface SidebarProps { idPrefix: string; pairs: Pair[] | undefined; pending: boolean; symbol: string | undefined; page: 'overview' | 'pair' | 'settings'; pathname: string; mode: Mode | undefined; authEnabled: boolean; saving: boolean; onNavigate: () => void; onSearch: () => void; onLogout: () => void }

function SidebarBody({ idPrefix, pairs, pending, symbol, page, pathname, mode, authEnabled, saving, onNavigate, onSearch, onLogout }: SidebarProps) {
  const isMac = typeof navigator !== 'undefined' && /Mac|iPhone|iPad/i.test((navigator as Navigator & { userAgentData?: { platform?: string } }).userAgentData?.platform ?? navigator.platform);
  return <div className="sidebar-inner">
    <Link className="sidebar-brand" to="/" aria-label="Grid Console 首页" onClick={onNavigate}><img className="sidebar-logo" src="/logo.svg" alt="" /><span className="sidebar-brand-name"><strong>Grid Console</strong><small>Binance Futures</small></span></Link>
    <button type="button" className="sidebar-search" onClick={onSearch} aria-label="打开命令面板"><SearchOutlined aria-hidden="true" /><span>搜索与跳转</span><kbd aria-hidden="true">{isMac ? '⌘K' : 'Ctrl K'}</kbd></button>
    <nav className="sidebar-nav" aria-label="主导航">
      <Link to="/" onClick={onNavigate} className={`sidebar-link${page === 'overview' ? ' sidebar-link-active' : ''}`} aria-current={page === 'overview' ? 'page' : undefined}><AppstoreOutlined aria-hidden="true" />策略总览</Link>
      <Link to="/settings" onClick={onNavigate} className={`sidebar-link${page === 'settings' ? ' sidebar-link-active' : ''}`} aria-current={page === 'settings' ? 'page' : undefined}><SettingOutlined aria-hidden="true" />全局设置</Link>
    </nav>
    <div className="sidebar-section"><span id={`${idPrefix}-pairs-label`}>交易对</span><Link to="/pairs/new" onClick={onNavigate} className="sidebar-add" aria-label="新增交易对" title="新增交易对"><PlusOutlined aria-hidden="true" /></Link></div>
    <nav className="sidebar-pairs" aria-labelledby={`${idPrefix}-pairs-label`}>
      {pending && !pairs && [0, 1, 2].map(item => <div className="sidebar-pair-skeleton" key={item} aria-hidden="true" />)}
      {pairs?.map(pair => {
        const short = pair.grid_config.direction === 'SHORT';
        const active = pair.symbol === symbol;
        return <Link key={pair.symbol} to={`/pairs/${pair.symbol}${pathname.endsWith('/config') ? '/config' : ''}`} onClick={onNavigate} className={`sidebar-pair${active ? ' sidebar-pair-active' : ''}`} aria-current={active ? 'page' : undefined}>
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
