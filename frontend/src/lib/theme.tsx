import type { ReactNode } from 'react';
import { ConfigProvider, theme as antdTheme } from 'antd';
import zhCN from 'antd/locale/zh_CN';
import { componentColors, dimensions, fonts, palettes } from './designTokens';
import { ThemeStateProvider, useHasOuterThemeState, useThemeDark } from './themeState';
export { useTheme, type ThemePreference } from './themeState';

function buildAntdTheme(dark: boolean) {
  const palette = palettes[dark ? 'dark' : 'light'];
  const colors = componentColors[dark ? 'dark' : 'light'];
  return {
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
      controlOutline: palette['--accent-soft'],
      controlOutlineWidth: 2,
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
      Checkbox: { colorWhite: colors.buttonForeground },
      DatePicker: { colorTextLightSolid: colors.buttonForeground },
      Switch: { colorPrimary: '#9a6700', colorPrimaryHover: dark ? '#9f6b00' : '#8f5f00' },
      Tabs: { horizontalMargin: `0 0 ${dimensions.space5}px`, titleFontSize: dimensions.textBase },
      Segmented: { trackBg: colors.segmentedTrack },
    },
  };
}
const antdThemes = { light: buildAntdTheme(false), dark: buildAntdTheme(true) };

function AntdThemeBridge({ children }: { children: ReactNode }) {
  const dark = useThemeDark();
  return <ConfigProvider locale={zhCN} theme={antdThemes[dark ? 'dark' : 'light']}>{children}</ConfigProvider>;
}

export function ThemeProvider({ children }: { children: ReactNode }) {
  const hasOuterState = useHasOuterThemeState();
  const bridged = <AntdThemeBridge>{children}</AntdThemeBridge>;
  return hasOuterState ? bridged : <ThemeStateProvider>{bridged}</ThemeStateProvider>;
}
