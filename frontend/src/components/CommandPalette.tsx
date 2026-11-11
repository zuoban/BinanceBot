import { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useLocation, useNavigate, type NavigateFunction } from 'react-router-dom';
import { Input, Modal } from 'antd';
import type { InputRef } from 'antd';
import { AppstoreOutlined, BulbOutlined, EditOutlined, LineChartOutlined, LogoutOutlined, PauseOutlined, PlayCircleOutlined, PlusOutlined, QuestionCircleOutlined, ReloadOutlined, SearchOutlined, SettingOutlined, StopOutlined } from '@ant-design/icons';
import { pairRoute } from '../lib/api';
import { directionLabel, statusLabel } from '../lib/format';
import { useTheme, type ThemePreference } from '../lib/theme';
import type { Pair } from '../lib/types';
import { useControls } from './Controls';
import { strategyActionGuard } from './StrategyMenu';
import { themeLabels } from './ThemeSwitch';
interface Command {
  key: string;
  label: string;
  group: string;
  hint?: string;
  icon?: React.ReactNode;
  keywords?: string;
  disabled?: boolean;
  run: () => void;
}

function CommandPalette({ open, onClose, commands }: { open: boolean; onClose: () => void; commands: Command[] }) {
  const [query, setQuery] = useState('');
  const [active, setActive] = useState(0);
  const input = useRef<InputRef>(null);
  const list = useRef<HTMLUListElement>(null);
  const results = useMemo(() => {
    const needle = query.trim().toLowerCase();
    return commands.filter(command => !command.disabled && (!needle || (command.group !== '最近使用' && `${command.label} ${command.hint ?? ''} ${command.keywords ?? ''} ${command.group}`.toLowerCase().includes(needle))));
  }, [commands, query]);
  const currentActive = Math.min(active, Math.max(0, results.length - 1));
  useEffect(() => { if (open) { setQuery(''); setActive(0); } }, [open]);
  useEffect(() => { setActive(0); }, [query]);
  useEffect(() => { list.current?.querySelector('[aria-selected="true"]')?.scrollIntoView?.({ block: 'nearest' }); }, [currentActive]);
  const choose = (command: Command | undefined) => {
    if (!command) return;
    onClose();
    command.run();
  };
  const onKeyDown = (event: React.KeyboardEvent) => {
    // Enter that commits an IME candidate must not run a command.
    if (event.nativeEvent.isComposing || event.keyCode === 229) return;
    if (event.key === 'ArrowDown') { event.preventDefault(); setActive(results.length ? (currentActive + 1) % results.length : 0); }
    else if (event.key === 'ArrowUp') { event.preventDefault(); setActive(results.length ? (currentActive - 1 + results.length) % results.length : 0); }
    else if (event.key === 'Home' && results.length) { event.preventDefault(); setActive(0); }
    else if (event.key === 'End' && results.length) { event.preventDefault(); setActive(results.length - 1); }
    else if (event.key === 'Enter') { event.preventDefault(); choose(results[currentActive]); }
  };
  let lastGroup = '';
  return <Modal open={open} aria-label="命令面板" onCancel={onClose} footer={null} closable={false} destroyOnHidden width={560} className="palette" afterOpenChange={visible => { if (visible) input.current?.focus(); }}>
    <div className="palette-search">
      <SearchOutlined aria-hidden="true" />
      <Input ref={input} variant="borderless" aria-label="搜索命令" placeholder="搜索交易对、页面或操作" value={query} onChange={event => setQuery(event.target.value)} onKeyDown={onKeyDown} role="combobox" aria-expanded={results.length > 0} aria-controls="palette-list" aria-activedescendant={results[currentActive] ? `palette-${results[currentActive].key}` : undefined} autoComplete="off" />
      <button type="button" className="palette-esc" onClick={onClose} aria-label="关闭命令面板"><kbd>Esc</kbd></button>
    </div>
    <ul className="palette-list" id="palette-list" role="listbox" aria-label="命令列表" ref={list}>
      {results.map((command, index) => {
        const heading = command.group !== lastGroup ? command.group : null;
        lastGroup = command.group;
        return <li key={command.key} role="presentation">
          {heading && <div className="palette-group">{heading}</div>}
          <div id={`palette-${command.key}`} role="option" aria-selected={index === currentActive} className={`palette-item${index === currentActive ? ' palette-item-active' : ''}`} onMouseMove={() => setActive(index)} onClick={() => choose(command)}>
            <span className="palette-icon" aria-hidden="true">{command.icon}</span>
            <span className="palette-label">{command.label}</span>
            {command.hint && <span className="palette-hint">{command.hint}</span>}
          </div>
        </li>;
      })}
      {!results.length && <li className="palette-empty" role="presentation">没有匹配的命令</li>}
    </ul>
  </Modal>;
}

export function routeSymbol(pathname: string) {
  const symbol = pathname.match(/^\/pairs\/([^/]+)/)?.[1];
  if (!symbol) return undefined;
  try {
    const decoded = decodeURIComponent(symbol);
    return decoded && decoded !== 'new' ? decoded : undefined;
  } catch {
    return undefined;
  }
}

const recentKey = 'binance_bot_recent_commands';
const readRecent = (): string[] => { try { const value = JSON.parse(localStorage.getItem(recentKey) ?? '[]'); return Array.isArray(value) ? value.filter((item): item is string => typeof item === 'string').slice(0, 5) : []; } catch { return []; } };

interface PaletteHostProps {
  open: boolean;
  setOpen: (open: boolean | ((value: boolean) => boolean)) => void;
  pairs: Pair[] | undefined;
  stale: boolean;
  authEnabled: boolean;
  saving: boolean;
  onLogout: () => void | Promise<void>;
}

function useGlobalShortcuts({ symbols, current, pathname, saving, navigate, setOpen, setHelp }: {
  symbols: string[];
  current: string | undefined;
  pathname: string;
  saving: boolean;
  navigate: NavigateFunction;
  setOpen: PaletteHostProps['setOpen'];
  setHelp: (open: boolean) => void;
}) {
  const lead = useRef(0);
  const shortcutState = useRef({ symbols, current, pathname, saving });
  shortcutState.current = { symbols, current, pathname, saving };
  useEffect(() => { if (saving) setOpen(false); }, [saving, setOpen]);
  useEffect(() => {
    const typing = (target: EventTarget | null) => target instanceof HTMLElement && (target.isContentEditable || ['INPUT', 'TEXTAREA', 'SELECT'].includes(target.tagName) || Boolean(target.closest('[role="menu"], [role="listbox"]')));
    const go = (target: string) => {
      if (shortcutState.current.pathname !== target) navigate(target);
    };
    const onKey = (event: KeyboardEvent) => {
      if (event.isComposing || event.keyCode === 229 || event.defaultPrevented) return;
      if ((event.metaKey || event.ctrlKey) && !event.shiftKey && !event.altKey && event.key?.toLowerCase() === 'k') {
        event.preventDefault();
        setOpen(value => value ? false : (!shortcutState.current.saving && !document.querySelector('.ant-modal-root, .ant-drawer-open')));
        return;
      }
      if (event.metaKey || event.ctrlKey || event.altKey || typing(event.target) || document.querySelector('.ant-modal-root, .ant-drawer-open')) {
        lead.current = 0;
        return;
      }
      if (event.key === '?') { lead.current = 0; event.preventDefault(); setHelp(true); return; }
      if (shortcutState.current.saving) { lead.current = 0; return; }
      if (event.key === 'g') { lead.current = Date.now(); return; }
      if (lead.current && Date.now() - lead.current < 1200) {
        lead.current = 0;
        if (event.key === 'o') { go('/'); return; }
        if (event.key === 's') { go('/settings'); return; }
        if (event.key === 'n') { go('/pairs/new'); return; }
      }
      const { symbols: currentSymbols, current: activeSymbol, pathname: currentPathname } = shortcutState.current;
      if ((event.key === '[' || event.key === ']') && currentSymbols.length > 1 && activeSymbol) {
        const index = currentSymbols.indexOf(activeSymbol);
        if (index < 0) return;
        const next = currentSymbols[(index + (event.key === ']' ? 1 : -1) + currentSymbols.length) % currentSymbols.length];
        go(pairRoute(next, currentPathname.endsWith('/config')));
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [navigate, setOpen, setHelp]);
}

function ShortcutHelpModal({ open, onClose }: { open: boolean; onClose: () => void }) {
  return <Modal open={open} onCancel={onClose} footer={null} title="键盘快捷键" width={440} destroyOnHidden>
    <dl className="shortcut-list">
      <div><dt>打开命令面板</dt><dd><kbd>⌘K</kbd> / <kbd>Ctrl K</kbd></dd></div>
      <div><dt>查看快捷键说明</dt><dd><kbd>?</kbd></dd></div>
      <div><dt>前往策略总览</dt><dd><kbd>g</kbd> <kbd>o</kbd></dd></div>
      <div><dt>前往全局设置</dt><dd><kbd>g</kbd> <kbd>s</kbd></dd></div>
      <div><dt>前往新增交易对</dt><dd><kbd>g</kbd> <kbd>n</kbd></dd></div>
      <div><dt>切换上 / 下一个交易对</dt><dd><kbd>[</kbd> / <kbd>]</kbd></dd></div>
    </dl>
  </Modal>;
}

/** Owns the command list, the global shortcuts and the recently-used order. Lives inside ControlProvider to reuse its guarded actions. */
export const PaletteHost = memo(function PaletteHost({ open, setOpen, pairs, stale, authEnabled, saving, onLogout }: PaletteHostProps) {
  const navigate = useNavigate(), { pathname } = useLocation();
  const { busy, run } = useControls(), { preference, setPreference } = useTheme();
  const [recent, setRecent] = useState<string[]>(readRecent), [help, setHelp] = useState(false);
  const remember = useCallback((key: string) => {
    setRecent(current => {
      const next = [key, ...current.filter(item => item !== key)].slice(0, 5);
      try { localStorage.setItem(recentKey, JSON.stringify(next)); } catch { /* Recent commands are a convenience only. */ }
      return next;
    });
  }, []);
  const symbols = useMemo(() => pairs?.map(pair => pair.symbol) ?? [], [pairs]);
  const current = routeSymbol(pathname);
  useGlobalShortcuts({ symbols, current, pathname, saving, navigate, setOpen, setHelp });
  const closePalette = useCallback(() => setOpen(false), [setOpen]);
  const closeHelp = useCallback(() => setHelp(false), []);
  const commands = useMemo<Command[]>(() => {
    if (!open) return [];
    const go = (target: string) => { if (pathname !== target) navigate(target); };
    const all: Command[] = [
      { key: 'go-overview', group: '页面', label: '策略总览', hint: 'g o', keywords: 'overview home 总览 首页', icon: <AppstoreOutlined />, run: () => go('/') },
      { key: 'go-settings', group: '页面', label: '全局设置', hint: 'g s', keywords: 'settings 设置 api telegram', icon: <SettingOutlined />, run: () => go('/settings') },
      { key: 'go-new', group: '页面', label: '新增交易对', hint: 'g n', keywords: 'new add 新增 添加', icon: <PlusOutlined />, run: () => go('/pairs/new') },
      ...(pairs ?? []).flatMap(pair => {
        const guard = strategyActionGuard(pair, stale, Boolean(busy?.has(pair.symbol)));
        return [
          { key: `pair-${pair.symbol}`, group: '交易对', label: pair.symbol, hint: `${statusLabel[pair.status]} · ${directionLabel(pair.grid_config.direction)}`, keywords: '策略 查看', icon: <LineChartOutlined />, run: () => go(pairRoute(pair.symbol)) },
          { key: `config-${pair.symbol}`, group: '交易对', label: `${pair.symbol} 策略参数`, hint: '编辑', keywords: 'config 配置 参数', icon: <EditOutlined />, run: () => go(pairRoute(pair.symbol, true)) },
          { key: `toggle-${pair.symbol}`, group: '操作', label: `${guard.toggleLabel} ${pair.symbol}`, hint: pair.mode === 'LIVE' && !guard.running ? '实盘 · 需确认' : undefined, keywords: guard.running ? 'pause 暂停 停止' : 'resume 恢复 启动', icon: guard.running ? <PauseOutlined /> : <PlayCircleOutlined />, disabled: guard.toggleDisabled, run: () => void run(pair, guard.toggleAction) },
          { key: `rebalance-${pair.symbol}`, group: '操作', label: `重建 ${pair.symbol} 网格`, hint: '需确认', keywords: 'rebalance 重建 网格', icon: <ReloadOutlined />, disabled: guard.rebalanceDisabled, run: () => void run(pair, 'rebalance') },
          { key: `cancel-${pair.symbol}`, group: '操作', label: `撤单并暂停 ${pair.symbol}`, hint: '需确认', keywords: 'cancel 撤单', icon: <StopOutlined />, disabled: guard.cancelDisabled, run: () => void run(pair, 'cancel_all') },
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
  }, [open, pathname, pairs, navigate, busy, run, preference, setPreference, authEnabled, saving, onLogout, stale, recent, remember]);
  return <>
    <CommandPalette open={open} onClose={closePalette} commands={commands} />
    <ShortcutHelpModal open={help} onClose={closeHelp} />
  </>;
});
