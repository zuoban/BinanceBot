import { Button, Dropdown } from 'antd';
import { BulbOutlined } from '@ant-design/icons';
import { useTheme, type ThemePreference } from '../lib/theme';

export const themeLabels: Record<ThemePreference, string> = { system: '跟随系统', light: '浅色', dark: '深色' };

export function ThemeSwitch() {
  const { preference, setPreference } = useTheme();
  const labels = themeLabels;
  return <Dropdown trigger={['click']} menu={{
    selectable: true, selectedKeys: [preference],
    items: Object.entries(labels).map(([key, label]) => ({ key, label })),
    onClick: ({ key }) => setPreference(key as ThemePreference),
  }}>
    <Button type="text" icon={<BulbOutlined />} aria-label={`主题：${labels[preference]}`} title={`主题：${labels[preference]}`} />
  </Dropdown>;
}
