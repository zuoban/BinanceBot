export type ColorMode = 'light' | 'dark';

const lightPalette = {
  '--bg-page': '#f2f5f9',
  '--bg-card': '#ffffff',
  '--bg-raised': '#f6f8fb',
  '--border-hairline': '#e1e7ef',
  '--border-strong': '#cbd5e1',
  '--text-primary': '#0f1a2b',
  '--text-secondary': '#44546a',
  '--text-muted': '#5f6f85',
  '--accent': '#1d4ed8',
  '--accent-soft': 'rgba(29, 78, 216, 0.08)',
  '--positive': '#0f7a52',
  '--negative': '#b4382c',
  '--danger-border': '#f0c4bd',
  '--history-equity': '#1d4ed8',
  '--sig-calm': '#0f7a52', '--sig-calm-bg': '#ecf8f2', '--sig-calm-line': '#bde5d1',
  '--sig-blue': '#1d4ed8', '--sig-blue-bg': '#eef3ff', '--sig-blue-line': '#c7d7fb',
  '--sig-yellow': '#8a5a00', '--sig-yellow-bg': '#fff7e0', '--sig-yellow-line': '#f1dc9b',
  '--sig-orange': '#b04a05', '--sig-orange-bg': '#fff0e3', '--sig-orange-line': '#f6c9a0',
  '--sig-red': '#b4382c', '--sig-red-bg': '#fdf0ee', '--sig-red-line': '#f3c6bf',
  '--rail-bg': '#0b1424', '--rail-line': '#1b2a44', '--rail-text': '#e6edf7',
  '--rail-muted': '#9aaac1', '--rail-hover': '#14223a', '--rail-active': '#1b2f55', '--rail-accent': '#8fb3ff',
  '--rail-active-text': '#ffffff', '--rail-border-strong': '#2b3d5c',
  '--rail-positive': '#5fdcaa', '--rail-negative': '#ff9a88',
  '--rail-running': '#3fcf96', '--rail-error': '#ff7a66',
  '--rail-info-line': '#1e3a6e', '--rail-live': '#ff9f57', '--rail-live-line': '#5e3a1a',
  '--rail-warning': '#f0cf63', '--rail-warning-line': '#55481c',
  '--selection-bg': 'rgba(29, 78, 216, 0.08)', '--action-foreground': '#ffffff',
  '--savebar-shadow': '0 8px 24px rgba(15, 26, 43, 0.12)',
  '--overlay-shadow': '0 12px 32px rgba(15, 26, 43, 0.14)',
} as const;

type Palette = { readonly [Key in keyof typeof lightPalette]: string };
export const palettes = {
  light: lightPalette,
  dark: {
    ...lightPalette,
    '--bg-page': '#0a1220', '--bg-card': '#101a2b', '--bg-raised': '#16223a',
    '--border-hairline': '#1f2e47', '--border-strong': '#2b3d5c',
    '--text-primary': '#e6edf7', '--text-secondary': '#a7b5c9', '--text-muted': '#8394ad',
    '--accent': '#6ea0ff', '--accent-soft': 'rgba(110, 160, 255, 0.14)',
    '--positive': '#3fcf96', '--negative': '#ff7a66', '--danger-border': '#5a2a24', '--history-equity': '#6ea0ff',
    '--sig-calm': '#3fcf96', '--sig-calm-bg': '#0d2a21', '--sig-calm-line': '#1b4d3b',
    '--sig-blue': '#8fb3ff', '--sig-blue-bg': '#0f1f3d', '--sig-blue-line': '#1e3a6e',
    '--sig-yellow': '#f0cf63', '--sig-yellow-bg': '#2b2410', '--sig-yellow-line': '#55481c',
    '--sig-orange': '#ff9f57', '--sig-orange-bg': '#2e1c0e', '--sig-orange-line': '#5e3a1a',
    '--sig-red': '#ff7a66', '--sig-red-bg': '#2e1613', '--sig-red-line': '#5a2a24',
    '--rail-bg': '#070d18', '--overlay-shadow': '0 16px 40px rgba(0, 0, 0, 0.55)',
    '--selection-bg': 'rgba(110, 160, 255, 0.28)', '--action-foreground': '#0a1220',
    '--savebar-shadow': '0 8px 24px rgba(0, 0, 0, 0.5)',
  },
} as const satisfies Record<ColorMode, Palette>;

export const fonts = {
  sans: 'system-ui, -apple-system, "PingFang SC", "Microsoft YaHei", "Segoe UI", sans-serif',
  mono: 'ui-monospace, "SF Mono", Menlo, Consolas, monospace',
} as const;

export const dimensions = {
  radiusControl: 8, radiusPanel: 12, radiusChip: 6,
  space1: 4, space2: 8, space3: 12, space4: 16, space5: 20, space6: 24, space8: 32,
  textXs: 12, textSm: 13, textBase: 14, textSection: 16, textTitle: 26,
  controlHeight: 36, controlHeightSM: 28, controlTouch: 44,
} as const;

export const sharedCssTokens = {
  '--font-sans': fonts.sans, '--font-mono': fonts.mono,
  '--radius-control': `${dimensions.radiusControl}px`, '--radius-panel': `${dimensions.radiusPanel}px`, '--radius-chip': `${dimensions.radiusChip}px`,
  '--space-1': `${dimensions.space1}px`, '--space-2': `${dimensions.space2}px`, '--space-3': `${dimensions.space3}px`,
  '--space-4': `${dimensions.space4}px`, '--space-5': `${dimensions.space5}px`, '--space-6': `${dimensions.space6}px`, '--space-8': `${dimensions.space8}px`,
  '--text-xs': `${dimensions.textXs}px`, '--text-sm': `${dimensions.textSm}px`, '--text-base': `${dimensions.textBase}px`,
  '--text-section': `${dimensions.textSection}px`, '--text-title': `${dimensions.textTitle}px`, '--control-touch': `${dimensions.controlTouch}px`,
} as const;

// Ant Design-only surfaces and interaction states share the palette's semantic values.
export const componentColors = {
  light: {
    elevated: palettes.light['--bg-card'], disabledText: '#6b7a90', disabledBg: '#eef2f7',
    hover: palettes.light['--bg-page'], active: palettes.light['--accent-soft'], activeHover: 'rgba(29, 78, 216, 0.13)',
    tableHeader: palettes.light['--bg-raised'], tableHover: '#f6f9fe', segmentedTrack: '#e9eef5',
    buttonForeground: palettes.light['--action-foreground'],
  },
  dark: {
    elevated: palettes.dark['--bg-raised'], disabledText: '#7a8ba5', disabledBg: palettes.dark['--bg-raised'],
    hover: '#17253f', active: 'rgba(110, 160, 255, 0.16)', activeHover: 'rgba(110, 160, 255, 0.22)',
    tableHeader: '#0c1626', tableHover: '#14213a', segmentedTrack: '#0c1626',
    buttonForeground: palettes.dark['--action-foreground'],
  },
} as const;

const declarations = (tokens: Readonly<Record<string, string>>) => Object.entries(tokens).map(([name, value]) => `${name}:${value};`).join('');
export const designTokensCss = `:root{${declarations(sharedCssTokens)}${declarations(palettes.light)}}[data-theme="dark"]{${declarations(palettes.dark)}}`;
