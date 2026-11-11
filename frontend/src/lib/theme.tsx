import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react';
import { ConfigProvider, theme as antdTheme } from 'antd';
import zhCN from 'antd/locale/zh_CN';
import { componentColors, dimensions, fonts, palettes } from './designTokens';

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
  const palette = palettes[dark ? 'dark' : 'light'];
  const colors = componentColors[dark ? 'dark' : 'light'];
  const setPreference = useCallback((value: ThemePreference) => {
    setPreferenceState(value);
    try { localStorage.setItem(storageKey, value); } catch { /* Keep the choice for this session. */ }
  }, []);
  const value = useMemo(() => ({ preference, setPreference }), [preference, setPreference]);

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
    document.querySelector<HTMLMetaElement>('meta[name="theme-color"]')?.setAttribute('content', palette['--bg-page']);
  }, [dark, palette]);

  return <ThemeContext.Provider value={value}>
    <ConfigProvider locale={zhCN} theme={{
      algorithm: dark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
      token: {
        colorPrimary: palette['--accent'],
        colorInfo: palette['--text-secondary'],
        colorInfoBg: palette['--bg-raised'],
        colorInfoBorder: colors.borderSecondary,
        colorSuccess: palette['--positive'],
        colorSuccessBg: palette['--sig-calm-bg'],
        colorSuccessBorder: palette['--sig-calm-line'],
        colorError: palette['--negative'],
        colorErrorBg: palette['--sig-red-bg'],
        colorErrorBorder: palette['--sig-red-line'],
        colorWarning: palette['--sig-yellow'],
        colorWarningBg: palette['--sig-yellow-bg'],
        colorWarningBorder: palette['--sig-yellow-line'],
        borderRadius: dimensions.radiusControl,
        borderRadiusSM: dimensions.radiusChip,
        borderRadiusLG: dimensions.radiusPanel,
        controlHeight: dimensions.controlHeight,
        controlHeightSM: dimensions.controlHeightSM,
        controlHeightLG: dimensions.controlTouch,
        fontSize: dimensions.textBase,
        fontSizeSM: dimensions.textXs,
        fontSizeLG: dimensions.textSection,
        fontFamily: fonts.sans,
        fontFamilyCode: fonts.mono,
        colorBgLayout: palette['--bg-page'],
        colorBgContainer: palette['--bg-card'],
        colorBgElevated: colors.elevated,
        colorBorder: colors.border,
        colorBorderSecondary: colors.borderSecondary,
        colorText: palette['--text-primary'],
        colorTextSecondary: palette['--text-secondary'],
        colorTextTertiary: palette['--text-muted'],
        colorTextPlaceholder: palette['--text-muted'],
        colorTextDisabled: colors.disabledText,
        colorBgContainerDisabled: colors.disabledBg,
        controlItemBgHover: colors.hover,
        controlItemBgActive: colors.active,
        controlItemBgActiveHover: colors.activeHover,
      },
      components: {
        Card: { borderRadiusLG: dimensions.radiusPanel, headerFontSize: dimensions.textSection, headerHeight: 52 },
        Table: {
          headerBg: colors.tableHeader,
          headerColor: palette['--text-secondary'],
          headerSplitColor: 'transparent',
          rowHoverBg: colors.tableHover,
          borderColor: palette['--border-hairline'],
        },
        Dropdown: { paddingBlock: dimensions.radiusChip, borderRadiusLG: dimensions.radiusPanel, borderRadiusSM: dimensions.radiusChip },
        Select: {
          optionPadding: `${dimensions.space2}px ${dimensions.space3}px`,
          optionSelectedFontWeight: 500,
          optionSelectedColor: palette['--text-primary'],
        },
        // High-contrast neutral primary actions keep the chrome calm while accent marks selection and moments.
        Button: {
          colorPrimary: colors.primaryBg,
          colorPrimaryHover: colors.primaryHover,
          colorPrimaryActive: colors.primaryActive,
          primaryColor: colors.buttonForeground,
          dangerColor: colors.dangerForeground,
        },
        Tabs: { horizontalMargin: `0 0 ${dimensions.space5}px`, titleFontSize: dimensions.textBase },
        Segmented: { trackBg: colors.segmentedTrack },
      },
    }}>{children}</ConfigProvider>
  </ThemeContext.Provider>;
}
