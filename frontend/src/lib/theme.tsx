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
      token: { colorPrimary: dark ? '#69bb98' : '#176e59', colorInfo: dark ? '#69bb98' : '#176e59', colorSuccess: dark ? '#69bb98' : '#17745e', colorError: dark ? '#ed907a' : '#bd5c4b', borderRadius: 8, fontFamily: '"Avenir Next", "PingFang SC", "Microsoft YaHei", sans-serif', colorBgLayout: dark ? '#101a17' : '#f4f6f3', ...(dark ? { colorBgContainer: '#192723', colorBgElevated: '#20312b', colorBorderSecondary: '#31443b' } : {}) },
      components: { Card: { borderRadiusLG: 12 }, Menu: { itemSelectedBg: dark ? '#28483a' : '#e5f0eb', itemSelectedColor: dark ? '#a2dfbb' : '#176e59' } },
    }}>{children}</ConfigProvider>
  </ThemeContext.Provider>;
}
