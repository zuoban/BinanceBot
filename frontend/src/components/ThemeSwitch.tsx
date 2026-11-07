import { Button, Dropdown, Segmented } from 'antd';
import { BulbOutlined, DesktopOutlined, MoonOutlined, SunOutlined } from '@ant-design/icons';
import { useTheme, type ThemePreference } from '../lib/theme';

export const themeLabels: Record<ThemePreference, string> = { system: '跟随系统', light: '浅色', dark: '深色' };

export function ThemeSwitch() {
  const { preference, setPreference } = useTheme();
  return <Dropdown trigger={['click']} menu={{
    selectable: true, selectedKeys: [preference],
    items: Object.entries(themeLabels).map(([key, label]) => ({ key, label })),
    onClick: ({ key }) => setPreference(key as ThemePreference),
  }}>
    <Button type="text" icon={<BulbOutlined />} aria-label={`主题：${themeLabels[preference]}`} title={`主题：${themeLabels[preference]}`} />
  </Dropdown>;
}

const themeIcons: Record<ThemePreference, React.ReactNode> = { system: <DesktopOutlined />, light: <SunOutlined />, dark: <MoonOutlined /> };

export function ThemeSegmented() {
  const { preference, setPreference } = useTheme();
  return <Segmented<ThemePreference> className="theme-segmented" size="small" aria-label="主题" value={preference} onChange={setPreference}
    options={(Object.keys(themeLabels) as ThemePreference[]).map(value => ({ value, label: <span role="img" aria-label={`主题：${themeLabels[value]}`} title={themeLabels[value]}>{themeIcons[value]}</span> }))} />;
}
