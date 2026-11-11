import { Suspense, lazy, useContext, useRef } from 'react';
import { flushSync } from 'react-dom';
import { Tabs } from 'antd';
import { useAsyncLock, useMountedRef } from '../lib/asyncGuard';
import { useConfirmDialog } from '../lib/confirmDialog';
import { EditContext } from '../lib/session';
import { useUrlParam } from '../lib/urlState';
import { PageTitle } from '../components/Common';
import { LoadErrorBoundary } from '../components/LoadErrorBoundary';
import { GlobalSettings } from '../components/settings/GlobalSettings';
import { SecuritySettings } from '../components/settings/SecuritySettings';
import { ServerSettings } from '../components/settings/ServerSettings';
const DatabaseSettings = lazy(() => import('../components/DatabaseSettings').then(module => ({ default: module.DatabaseSettings })));

const settingsTabs = ['global', 'server', 'database', 'security'] as const;
export default function Settings() {
  const edit = useContext(EditContext);
  const { confirm } = useConfirmDialog();
  const [active, setActive] = useUrlParam('tab', 'global', settingsTabs);
  const runSwitch = useAsyncLock();
  const savingRef = useRef(edit.saving);
  savingRef.current = edit.saving;
  const mounted = useMountedRef();
  return <div className="settings-page"><PageTitle title="全局设置" description="交易所连接、通知和控制台安全设置" />
    <Tabs className="settings-tabs" activeKey={active} onChange={async key => {
      const next = settingsTabs.find(t => t === key);
      if (!next || savingRef.current) return;
      await runSwitch(async () => {
        if (edit.dirty && !await confirm({ title: '放弃未保存的修改？', okText: '放弃修改', cancelText: '继续编辑' })) return;
        if (!mounted.current || savingRef.current) return;
        flushSync(() => edit.setDirty(false)); setActive(next);
      });
    }} destroyOnHidden items={[
      { key: 'global', label: '连接与通知', children: <GlobalSettings /> },
      { key: 'server', label: '服务设置', children: <ServerSettings /> },
      { key: 'database', label: '数据库操作', children: <LoadErrorBoundary label="数据库操作" resetKey={active}><Suspense fallback={<div className="history-empty" role="status">正在加载数据库操作…</div>}><DatabaseSettings /></Suspense></LoadErrorBoundary> },
      { key: 'security', label: '账户安全', children: <SecuritySettings /> },
    ]} />
  </div>;
}
