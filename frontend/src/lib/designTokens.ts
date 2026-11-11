type ColorMode = 'light' | 'dark';

const lightPalette = {
  '--bg-page': '#f7f7f8',
  '--bg-card': '#ffffff',
  '--bg-raised': '#f1f2f4',
  '--border-hairline': 'rgba(23, 24, 28, 0.08)',
  '--border-strong': 'rgba(23, 24, 28, 0.16)',
  '--text-primary': '#17181c',
  '--text-secondary': '#52555d',
  '--text-muted': '#70747e',
  '--accent': '#9a6700',
  '--accent-soft': 'rgba(154, 103, 0, 0.08)',
  '--positive': '#15803d',
  '--negative': '#dc2626',
  '--danger-border': 'rgba(220, 38, 38, 0.28)',
  '--history-equity': '#b47800',
  '--sig-calm': '#15803d', '--sig-calm-bg': 'rgba(21, 128, 61, 0.07)', '--sig-calm-line': 'rgba(21, 128, 61, 0.2)',
  '--sig-blue': '#52555d', '--sig-blue-bg': '#f1f2f4', '--sig-blue-line': 'rgba(23, 24, 28, 0.12)',
  '--sig-yellow': '#9a6700', '--sig-yellow-bg': 'rgba(180, 120, 0, 0.08)', '--sig-yellow-line': 'rgba(180, 120, 0, 0.24)',
  '--sig-orange': '#b45309', '--sig-orange-bg': 'rgba(180, 83, 9, 0.08)', '--sig-orange-line': 'rgba(180, 83, 9, 0.24)',
  '--sig-red': '#dc2626', '--sig-red-bg': 'rgba(220, 38, 38, 0.07)', '--sig-red-line': 'rgba(220, 38, 38, 0.22)',
  '--rail-bg': '#141518', '--rail-line': 'rgba(237, 238, 240, 0.08)', '--rail-text': '#edeef0',
  '--rail-muted': '#8e929c', '--rail-hover': '#1c1e22', '--rail-active': '#23262c', '--rail-accent': '#e5a910',
  '--rail-active-text': '#f5f6f7', '--rail-border-strong': 'rgba(237, 238, 240, 0.16)',
  '--rail-positive': '#4cc38a', '--rail-negative': '#e5484d',
  '--rail-live': '#f0b90b', '--rail-live-line': 'rgba(240, 185, 11, 0.28)',
  '--rail-warning': '#e5a910', '--rail-warning-line': 'rgba(229, 169, 16, 0.26)',
  '--selection-bg': 'rgba(180, 120, 0, 0.14)', '--action-foreground': '#ffffff',
  '--savebar-shadow': '0 8px 24px rgba(0, 0, 0, 0.06)',
  '--overlay-shadow': '0 8px 24px rgba(0, 0, 0, 0.08)',
} as const;

type Palette = { readonly [Key in keyof typeof lightPalette]: string };
export const palettes = {
  light: lightPalette,
  dark: {
    ...lightPalette,
    '--bg-page': '#131416', '--bg-card': '#18191c', '--bg-raised': '#212328',
    '--border-hairline': 'rgba(237, 238, 240, 0.08)', '--border-strong': 'rgba(237, 238, 240, 0.16)',
    '--text-primary': '#edeef0', '--text-secondary': '#a0a3ab', '--text-muted': '#848892',
    '--accent': '#e5a910', '--accent-soft': 'rgba(229, 169, 16, 0.12)',
    '--positive': '#4cc38a', '--negative': '#e5484d', '--danger-border': 'rgba(229, 72, 77, 0.32)',
    '--history-equity': '#e5a910',
    '--sig-calm': '#4cc38a', '--sig-calm-bg': 'rgba(76, 195, 138, 0.1)', '--sig-calm-line': 'rgba(76, 195, 138, 0.22)',
    '--sig-blue': '#a0a3ab', '--sig-blue-bg': '#212328', '--sig-blue-line': 'rgba(237, 238, 240, 0.12)',
    '--sig-yellow': '#e5a910', '--sig-yellow-bg': 'rgba(229, 169, 16, 0.1)', '--sig-yellow-line': 'rgba(229, 169, 16, 0.26)',
    '--sig-orange': '#f59e0b', '--sig-orange-bg': 'rgba(245, 158, 11, 0.12)', '--sig-orange-line': 'rgba(245, 158, 11, 0.28)',
    '--sig-red': '#e5484d', '--sig-red-bg': 'rgba(229, 72, 77, 0.1)', '--sig-red-line': 'rgba(229, 72, 77, 0.26)',
    '--rail-bg': '#0f1012', '--overlay-shadow': '0 12px 32px rgba(0, 0, 0, 0.48)',
    '--selection-bg': 'rgba(229, 169, 16, 0.22)', '--action-foreground': '#131416',
    '--savebar-shadow': '0 8px 24px rgba(0, 0, 0, 0.44)',
  },
} as const satisfies Record<ColorMode, Palette>;

export const fonts = {
  sans: 'Inter, "IBM Plex Sans", system-ui, -apple-system, "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif',
  mono: '"JetBrains Mono", "IBM Plex Mono", "SF Mono", ui-monospace, Menlo, Consolas, monospace',
} as const;

export const dimensions = {
  radiusControl: 8, radiusPanel: 12, radiusChip: 6,
  space1: 4, space2: 8, space3: 12, space4: 16, space5: 20,
  textXs: 12, textSm: 13, textBase: 14, textSection: 16, textTitle: 28,
  controlHeight: 36, controlHeightSM: 28, controlTouch: 44,
} as const;

const sharedCssTokens = {
  '--font-sans': fonts.sans, '--font-mono': fonts.mono,
  '--radius-control': `${dimensions.radiusControl}px`, '--radius-panel': `${dimensions.radiusPanel}px`, '--radius-chip': `${dimensions.radiusChip}px`,
  '--space-1': `${dimensions.space1}px`, '--space-2': `${dimensions.space2}px`, '--space-3': `${dimensions.space3}px`,
  '--space-4': `${dimensions.space4}px`, '--space-5': `${dimensions.space5}px`,
  '--text-xs': `${dimensions.textXs}px`, '--text-sm': `${dimensions.textSm}px`, '--text-base': `${dimensions.textBase}px`,
  '--text-section': `${dimensions.textSection}px`, '--text-title': `${dimensions.textTitle}px`, '--control-touch': `${dimensions.controlTouch}px`,
} as const;

// Ant Design-only surfaces and interaction states share the palette's semantic values.
export const componentColors = {
  light: {
    elevated: palettes.light['--bg-card'], disabledText: '#8a8e98', disabledBg: '#f1f2f4',
    border: '#d8dadf', borderSecondary: '#e9eaee',
    hover: '#f1f2f4', active: palettes.light['--accent-soft'], activeHover: 'rgba(154, 103, 0, 0.13)',
    tableHeader: palettes.light['--bg-raised'], tableHover: '#f7f7f8', segmentedTrack: '#eceef1',
    primaryBg: '#17181c', primaryHover: '#2a2c33', primaryActive: '#0f1012',
    buttonForeground: '#ffffff', dangerForeground: '#ffffff',
  },
  dark: {
    elevated: palettes.dark['--bg-raised'], disabledText: '#686c76', disabledBg: '#1d1f24',
    border: '#34373f', borderSecondary: '#25272d',
    hover: '#23252b', active: 'rgba(229, 169, 16, 0.14)', activeHover: 'rgba(229, 169, 16, 0.2)',
    tableHeader: '#151619', tableHover: '#1d1f24', segmentedTrack: '#141518',
    primaryBg: '#edeef0', primaryHover: '#ffffff', primaryActive: '#d6d8dd',
    buttonForeground: '#131416', dangerForeground: '#ffffff',
  },
} as const;

const declarations = (tokens: Readonly<Record<string, string>>) => Object.entries(tokens).map(([name, value]) => `${name}:${value};`).join('');
export const designTokensCss = `:root{${declarations(sharedCssTokens)}${declarations(palettes.light)}}[data-theme="dark"]{${declarations(palettes.dark)}}`;
