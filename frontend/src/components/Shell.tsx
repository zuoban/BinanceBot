import { useCallback, useEffect, useMemo, useState } from 'react';
import { Link, Outlet, useBlocker, useLocation, useNavigate } from 'react-router-dom';
import { App, Button, Drawer, Layout, Menu, Select, Space } from 'antd';
import { AppstoreOutlined, MenuOutlined, LogoutOutlined, SettingOutlined, PlusOutlined } from '@ant-design/icons';
import { useLiveAccounts, useOverview, usePairs } from '../lib/queries';
import { AccountsContext, EditContext, useSession } from '../lib/session';
import { accountGroups, number } from '../lib/format';
import { ControlProvider } from './Controls';

export default function Shell() {
  const navigate = useNavigate(), location = useLocation();
  const pairs = usePairs();
  const overview = useOverview();
  const accounts = useLiveAccounts(overview.data);
  const [dirty, setDirty] = useState(false), [saving, setSaving] = useState(false), [open, setOpen] = useState(false);
  const { modal } = App.useApp();
  const { logout } = useSession();
  useEffect(() => {
    const groups = accountGroups(accounts);
    document.title = groups.length ? groups.map(group => `${number(group.equity)} | ${number(group.available)} ${group.asset}`).join(' / ') : 'Grid Console · 网格交易控制台';
    return () => { document.title = 'Grid Console · 网格交易控制台'; };
  }, [accounts]);
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
  useEffect(() => {
    if (symbol && symbol !== 'new' && pairs.data?.some(p => p.symbol === symbol)) sessionStorage.setItem('binance_bot_selected_pair', symbol);
  }, [symbol, pairs.data]);
  const lastPair = pairs.data?.find(p => p.symbol === symbol) || pairs.data?.find(p => p.symbol === sessionStorage.getItem('binance_bot_selected_pair')) || pairs.data?.[0];
  const menu = <Menu mode="inline" selectedKeys={[selected]} items={[
    { key: 'overview', icon: <AppstoreOutlined />, label: <Link to="/">策略总览</Link> },
    ...(lastPair ? [{ key: 'pair', icon: <span className="menu-grid-icon">▦</span>, label: <Link to={`/pairs/${lastPair.symbol}`}>策略详情</Link> }] : []),
    { key: 'settings', icon: <SettingOutlined />, label: <Link to="/settings">全局设置</Link> },
  ]} onClick={() => setOpen(false)} />;
  const context = useMemo(() => ({ dirty, setDirty, saving, setSaving }), [dirty, saving]);
  const logOut = useCallback(async () => {
    if (saving) return;
    if (dirty && !await modal.confirm({ title: '放弃未保存的修改并退出？', okText: '退出', cancelText: '继续编辑' })) return;
    await logout();
  }, [dirty, saving, logout, modal]);
  const sidebar = <><div className="sidebar-brand"><div className="brand-symbol">G</div><div><b>GRID CONSOLE</b><small>BINANCE FUTURES</small></div></div>
    <div className="sidebar-caption">WORKSPACE</div>{menu}<div className="sidebar-footer"><span className="online-dot" /> 策略控制台 <small>操作前请核对运行模式</small></div></>;
  return <EditContext.Provider value={context}><AccountsContext.Provider value={accounts}><ControlProvider><Layout className="app-layout">
    <Layout.Sider width={232} className="desktop-sider">{sidebar}</Layout.Sider>
    <Drawer title="GRID CONSOLE" placement="left" open={open} onClose={() => setOpen(false)} styles={{ body: { padding: 0 } }} size={260}>{menu}<div className="drawer-actions"><Link to="/pairs/new" onClick={() => setOpen(false)}><Button icon={<PlusOutlined />}>新增交易对</Button></Link></div></Drawer>
    <Layout>
      <Layout.Header className="topbar">
        <Space><Button className="mobile-menu" icon={<MenuOutlined />} aria-label="打开导航" onClick={() => setOpen(true)} />
          <span className="topbar-label">STRATEGY WORKSPACE</span><span className="topbar-separator">/</span><span>{selected === 'overview' ? '策略总览' : selected === 'settings' ? '全局设置' : '策略详情'}</span>
        </Space>
        <Space>{selected === 'pair' && symbol !== 'new' && <Select aria-label="切换交易对" value={symbol} options={pairs.data?.map(p => ({ value: p.symbol, label: p.symbol }))} onChange={value => navigate(`/pairs/${value}${location.pathname.endsWith('/config') ? '/config' : ''}`)} style={{ minWidth: 135 }} />}
          <Button type="text" icon={<LogoutOutlined />} onClick={() => void logOut()}>退出</Button></Space>
      </Layout.Header>
      <Layout.Content className="content"><Outlet /></Layout.Content>
      <Layout.Footer className="footer"><span>GRID CONSOLE · 策略工作台</span><span>所有时间默认显示北京时间</span></Layout.Footer>
    </Layout>
  </Layout></ControlProvider></AccountsContext.Provider></EditContext.Provider>;
}
