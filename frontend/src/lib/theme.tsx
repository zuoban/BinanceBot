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
        colorPrimary: dark ? '#69bb98' : '#176e59',
        colorInfo: dark ? '#69bb98' : '#176e59',
        colorInfoBg: dark ? '#162420' : '#f0f7f4',
        colorInfoBorder: dark ? '#26453b' : '#cce3da',
        colorSuccess: dark ? '#4cc38a' : '#16794f',
        colorSuccessBg: dark ? '#15241e' : '#f0f8f4',
        colorSuccessBorder: dark ? '#244536' : '#c6e6d6',
        colorError: dark ? '#e5484d' : '#c83e3b',
        colorErrorBg: dark ? '#261617' : '#fdf2f2',
        colorErrorBorder: dark ? '#4d2628' : '#f2c9c8',
        colorWarning: dark ? '#e5bc70' : '#9a6700',
        colorWarningBg: dark ? '#241e12' : '#fdf8ed',
        colorWarningBorder: dark ? '#47391c' : '#f2ddb6',
        borderRadius: 8,
        borderRadiusSM: 4,
        borderRadiusLG: 12,
        fontFamily: '"DM Sans", "PingFang SC", "Microsoft YaHei", sans-serif',
        colorBgLayout: dark ? '#141415' : '#fafafa',
        colorBgContainer: dark ? '#19191b' : '#ffffff',
        colorBgElevated: dark ? '#232325' : '#ffffff',
        colorBorder: dark ? '#3a3a40' : '#d4d4d8',
        colorBorderSecondary: dark ? '#28282b' : '#e6e6e9',
        colorText: dark ? '#ededed' : '#171717',
        colorTextSecondary: dark ? '#a1a1a6' : '#525252',
        colorTextTertiary: dark ? '#85858b' : '#737373',
        colorTextPlaceholder: dark ? '#85858b' : '#737373',
        colorTextDisabled: dark ? '#85858b' : '#737373',
        colorBgContainerDisabled: dark ? '#232325' : '#f4f4f5',
        controlItemBgHover: dark ? '#2c2c30' : '#f4f4f5',
        controlItemBgActive: dark ? 'rgba(105, 187, 152, 0.14)' : 'rgba(23, 110, 89, 0.09)',
        controlItemBgActiveHover: dark ? 'rgba(105, 187, 152, 0.20)' : 'rgba(23, 110, 89, 0.14)',
      },
      components: {
        Card: { borderRadiusLG: 12 },
        Dropdown: {
          paddingBlock: 6,
          borderRadiusLG: 12,
          borderRadiusSM: 6,
        },
        Select: {
          optionPadding: '8px 12px',
          optionSelectedFontWeight: 500,
          optionSelectedColor: dark ? '#69bb98' : '#176e59',
        },
        Menu: {
          itemSelectedBg: dark ? 'rgba(105, 187, 152, 0.14)' : 'rgba(23, 110, 89, 0.09)',
          itemSelectedColor: dark ? '#69bb98' : '#176e59',
          itemHoverBg: dark ? '#2c2c30' : '#f4f4f5',
          subMenuItemBg: dark ? '#232325' : '#ffffff',
        },
      },
    }}>{children}</ConfigProvider>
  </ThemeContext.Provider>;
}
