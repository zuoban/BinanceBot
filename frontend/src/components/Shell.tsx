import { useCallback, useEffect, useMemo, useState } from 'react';
import { Link, Outlet, useBlocker, useLocation, useNavigate } from 'react-router-dom';
import { App, Button, Dropdown, Layout, Select, Space } from 'antd';
import { AppstoreOutlined, BulbOutlined, LogoutOutlined, MenuOutlined, SettingOutlined } from '@ant-design/icons';
import { useLiveAccounts, useOverview, usePairs } from '../lib/queries';
import { AccountsContext, EditContext, useSession } from '../lib/session';
import { accountGroups, number } from '../lib/format';
import { ControlProvider } from './Controls';
import { themeLabels } from './ThemeSwitch';
import { useTheme, type ThemePreference } from '../lib/theme';
import { defaultTitle, useDocumentTitle } from '../lib/documentTitle';

export default function Shell() {
  const navigate = useNavigate(), location = useLocation();
  const pairs = usePairs();
  const overview = useOverview();
  const accounts = useLiveAccounts(overview.data);
  const [dirty, setDirty] = useState(false), [saving, setSaving] = useState(false);
  const { modal } = App.useApp();
  const { logout } = useSession();
  const { preference, setPreference } = useTheme();
  const groups = accountGroups(accounts);
  useDocumentTitle(groups.length ? groups.map(group => `${number(group.equity)} | ${number(group.available)} ${group.asset}`).join(' / ') : defaultTitle);
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
  const selected = location.pathname.startsWith('/settings') ? 'settings' : location.pathname.startsWith('/pairs/') ? 'pair' : 'overview';
  const symbol = location.pathname.match(/^\/pairs\/([^/]+)/)?.[1];
  const context = useMemo(() => ({ dirty, setDirty, saving, setSaving }), [dirty, saving]);
  const logOut = useCallback(async () => {
    if (saving) return;
    if (dirty && !await modal.confirm({ title: '放弃未保存的修改并退出？', okText: '退出', cancelText: '继续编辑' })) return;
    await logout();
  }, [dirty, saving, logout, modal]);
  return <EditContext.Provider value={context}><AccountsContext.Provider value={accounts}><ControlProvider><Layout className="app-layout">
    <Layout.Header className="topbar">
      <div className="topbar-main">
        <Link className="topbar-brand" to="/" aria-label="Grid Console 首页"><span className="topbar-brand-name"><strong>GRID CONSOLE</strong><small>BINANCE FUTURES</small></span></Link>
      </div>
      <Space className="topbar-actions">{selected === 'pair' && symbol !== 'new' && <Select className="topbar-pair-select" aria-label="切换交易对" title={symbol} popupMatchSelectWidth={false} value={symbol} options={pairs.data?.map(p => ({ value: p.symbol, label: p.symbol }))} onChange={value => navigate(`/pairs/${value}${location.pathname.endsWith('/config') ? '/config' : ''}`)} />}
        <Dropdown placement="bottomRight" trigger={['click']} menu={{
          items: [
            { key: 'overview', icon: <AppstoreOutlined aria-hidden="true" />, label: '策略总览' }, { key: 'settings', icon: <SettingOutlined aria-hidden="true" />, label: '全局设置' },
            { type: 'divider' },
            { key: 'theme', icon: <BulbOutlined aria-hidden="true" />, label: `主题：${themeLabels[preference]}`, children: Object.entries(themeLabels).map(([key, label]) => ({ key: `theme-${key}`, label: `${preference === key ? '✓ ' : ''}${label}` })) },
            { key: 'logout', icon: <LogoutOutlined aria-hidden="true" />, label: '退出', disabled: saving },
          ],
          selectedKeys: selected === 'pair' ? [] : [selected],
          onClick: ({ key }) => {
            if (key === 'logout') void logOut();
            else if (key.startsWith('theme-')) setPreference(key.slice(6) as ThemePreference);
            else navigate(key === 'overview' ? '/' : '/settings');
          },
        }}>
          <Button className="topbar-menu" type="text" icon={<MenuOutlined />} aria-label="打开主导航" />
        </Dropdown>
      </Space>
    </Layout.Header>
    <Layout.Content className="content"><Outlet /></Layout.Content>
    <Layout.Footer className="footer"><span>所有时间默认显示北京时间</span><span>版本 v{import.meta.env.VITE_APP_VERSION}</span></Layout.Footer>
  </Layout></ControlProvider></AccountsContext.Provider></EditContext.Provider>;
}
