import { createContext, useContext, useEffect, useState } from 'react';
import { ConfigProvider, theme as antdTheme } from 'antd';
import zhCN from 'antd/locale/zh_CN';

export type ThemePreference = 'system' | 'light' | 'dark';
const storageKey = 'binance_bot_theme';
const isPreference = (value: string | null): value is ThemePreference => value === 'system' || value === 'light' || value === 'dark';
const readPreference = (): ThemePreference => {
  try {
    const value = localStorage.getItem(storageKey);
    return isPreference(value) ? value : 'system';
  } catch { return 'system'; }
};
const systemDark = () => typeof window.matchMedia === 'function' && window.matchMedia('(prefers-color-scheme: dark)').matches;

const ThemeContext = createContext<{ preference: ThemePreference; setPreference: (value: ThemePreference) => void }>({
  preference: 'system', setPreference: () => {},
});

export const useTheme = () => useContext(ThemeContext);

export function ThemeProvider({ children }: { children: React.ReactNode }) {
  const [preference, setPreferenceState] = useState<ThemePreference>(readPreference);
  const [darkSystem, setDarkSystem] = useState(systemDark);
  const dark = preference === 'dark' || (preference === 'system' && darkSystem);
  const setPreference = (value: ThemePreference) => {
    setPreferenceState(value);
    try { localStorage.setItem(storageKey, value); } catch { /* Keep the choice for this session. */ }
  };

  useEffect(() => {
    if (typeof window.matchMedia !== 'function') return;
    const media = window.matchMedia('(prefers-color-scheme: dark)');
    const update = () => setDarkSystem(media.matches);
    update();
    media.addEventListener('change', update);
    return () => media.removeEventListener('change', update);
  }, []);
  useEffect(() => {
    const update = (event: StorageEvent) => {
      if (event.key === storageKey || event.key === null) setPreferenceState(readPreference());
    };
    window.addEventListener('storage', update);
    return () => window.removeEventListener('storage', update);
  }, []);
  useEffect(() => {
    document.documentElement.dataset.theme = dark ? 'dark' : 'light';
    document.documentElement.style.colorScheme = dark ? 'dark' : 'light';
  }, [dark]);

  return <ThemeContext.Provider value={{ preference, setPreference }}>
    <ConfigProvider locale={zhCN} theme={{
      algorithm: dark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
      token: {
        colorPrimary: dark ? '#6ea0ff' : '#1d4ed8',
        colorInfo: dark ? '#6ea0ff' : '#1d4ed8',
        colorInfoBg: dark ? '#0f1f3d' : '#eef3ff',
        colorInfoBorder: dark ? '#1e3a6e' : '#c7d7fb',
        colorSuccess: dark ? '#3fcf96' : '#0f7a52',
        colorSuccessBg: dark ? '#0d2a21' : '#ecf8f2',
        colorSuccessBorder: dark ? '#1b4d3b' : '#bde5d1',
        colorError: dark ? '#ff7a66' : '#b4382c',
        colorErrorBg: dark ? '#2e1613' : '#fdf0ee',
        colorErrorBorder: dark ? '#5a2a24' : '#f3c6bf',
        colorWarning: dark ? '#f0b94d' : '#a35a07',
        colorWarningBg: dark ? '#2b2010' : '#fff7e8',
        colorWarningBorder: dark ? '#55401c' : '#f4d9a6',
        borderRadius: 8,
        borderRadiusSM: 6,
        borderRadiusLG: 12,
        controlHeight: 36,
        controlHeightSM: 28,
        fontFamily: 'system-ui, -apple-system, "PingFang SC", "Microsoft YaHei", "Segoe UI", sans-serif',
        fontFamilyCode: 'ui-monospace, "SF Mono", Menlo, Consolas, monospace',
        colorBgLayout: dark ? '#0a1220' : '#f2f5f9',
        colorBgContainer: dark ? '#101a2b' : '#ffffff',
        colorBgElevated: dark ? '#16223a' : '#ffffff',
        colorBorder: dark ? '#2b3d5c' : '#cbd5e1',
        colorBorderSecondary: dark ? '#1f2e47' : '#e1e7ef',
        colorText: dark ? '#e6edf7' : '#0f1a2b',
        colorTextSecondary: dark ? '#a7b5c9' : '#44546a',
        colorTextTertiary: dark ? '#8394ad' : '#5f6f85',
        colorTextPlaceholder: dark ? '#8394ad' : '#5f6f85',
        colorTextDisabled: dark ? '#7a8ba5' : '#6b7a90',
        colorBgContainerDisabled: dark ? '#16223a' : '#eef2f7',
        controlItemBgHover: dark ? '#17253f' : '#f2f5f9',
        controlItemBgActive: dark ? 'rgba(110, 160, 255, 0.16)' : 'rgba(29, 78, 216, 0.08)',
        controlItemBgActiveHover: dark ? 'rgba(110, 160, 255, 0.22)' : 'rgba(29, 78, 216, 0.13)',
      },
      components: {
        Card: { borderRadiusLG: 12, headerFontSize: 15, headerHeight: 52 },
        Table: {
          headerBg: dark ? '#0c1626' : '#f6f8fb',
          headerColor: dark ? '#a7b5c9' : '#44546a',
          headerSplitColor: 'transparent',
          rowHoverBg: dark ? '#14213a' : '#f6f9fe',
          borderColor: dark ? '#1f2e47' : '#e1e7ef',
        },
        Dropdown: { paddingBlock: 6, borderRadiusLG: 12, borderRadiusSM: 6 },
        Select: {
          optionPadding: '8px 12px',
          optionSelectedFontWeight: 600,
          optionSelectedColor: dark ? '#6ea0ff' : '#1d4ed8',
        },
        // White on the light dark-theme fills is about 2.6:1; dark ink is about 7.3:1.
        Button: dark ? { primaryColor: '#0a1220', dangerColor: '#0a1220' } : {},
        Tabs: { horizontalMargin: '0 0 20px', titleFontSize: 14 },
        Segmented: { trackBg: dark ? '#0c1626' : '#e9eef5' },
      },
    }}>{children}</ConfigProvider>
  </ThemeContext.Provider>;
}
