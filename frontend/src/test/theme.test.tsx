import { readFileSync } from 'node:fs';
import { act, fireEvent, render, screen } from '@testing-library/react';
import { Checkbox, Switch, theme as antdTheme } from 'antd';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { ThemeProvider, useTheme } from '../lib/theme';
import { componentColors, palettes } from '../lib/designTokens';

function relativeLuminance(hex: string) {
  const normalized = hex.replace('#', '');
  const [r, g, b] = [0, 2, 4].map(index => {
    const channel = Number.parseInt(normalized.slice(index, index + 2), 16) / 255;
    return channel <= 0.03928 ? channel / 12.92 : ((channel + 0.055) / 1.055) ** 2.4;
  });
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

function contrastRatio(foreground: string, background: string) {
  const l1 = relativeLuminance(foreground);
  const l2 = relativeLuminance(background);
  return (Math.max(l1, l2) + 0.05) / (Math.min(l1, l2) + 0.05);
}

function TokenProbe() {
  const { token } = antdTheme.useToken();
  return <span data-testid="color-primary">{token.colorPrimary}</span>;
}

function Controls() {
  const { preference, setPreference } = useTheme();
  return <><span data-testid="preference">{preference}</span>
    <button onClick={() => setPreference('light')}>light</button>
    <button onClick={() => setPreference('system')}>system</button>
    <button onClick={() => setPreference('dark')}>dark</button></>;
}

let matches = false;
let listeners: Set<() => void>;
beforeEach(() => {
  localStorage.clear();
  matches = false;
  listeners = new Set();
  vi.stubGlobal('matchMedia', () => ({
    get matches() { return matches; },
    addEventListener: (_: string, callback: () => void) => listeners.add(callback),
    removeEventListener: (_: string, callback: () => void) => listeners.delete(callback),
  }));
});
afterEach(() => {
  vi.unstubAllGlobals();
  localStorage.clear();
  delete document.documentElement.dataset.theme;
  document.documentElement.style.colorScheme = '';
  document.head.querySelectorAll('meta[name="theme-color"]').forEach(node => node.remove());
});

it('follows system changes until a manual theme is selected', () => {
  render(<ThemeProvider><Controls /></ThemeProvider>);
  expect(document.documentElement.dataset.theme).toBe('light');
  act(() => { matches = true; listeners.forEach(listener => listener()); });
  expect(document.documentElement.dataset.theme).toBe('dark');
  fireEvent.click(screen.getByText('light'));
  expect(localStorage.getItem('binance_bot_theme')).toBe('light');
  expect(document.documentElement.dataset.theme).toBe('light');
  act(() => { matches = false; listeners.forEach(listener => listener()); });
  act(() => { matches = true; listeners.forEach(listener => listener()); });
  expect(document.documentElement.dataset.theme).toBe('light');
  fireEvent.click(screen.getByText('system'));
  expect(document.documentElement.dataset.theme).toBe('dark');
});

it('restores the saved preference and follows changes from other tabs', () => {
  localStorage.setItem('binance_bot_theme', 'dark');
  render(<ThemeProvider><Controls /></ThemeProvider>);
  expect(document.documentElement.dataset.theme).toBe('dark');
  localStorage.setItem('binance_bot_theme', 'light');
  act(() => window.dispatchEvent(new StorageEvent('storage', { key: 'binance_bot_theme' })));
  expect(screen.getByTestId('preference')).toHaveTextContent('light');
  expect(document.documentElement.dataset.theme).toBe('light');
});

it('updates theme-color meta symmetrically in theme-init.js and survives a throwing matchMedia', () => {
  const script = readFileSync('public/theme-init.js', 'utf8');
  const meta = document.createElement('meta');
  meta.setAttribute('name', 'theme-color');
  meta.setAttribute('content', '#000000');
  meta.setAttribute('data-light', palettes.light['--bg-page']);
  meta.setAttribute('data-dark', palettes.dark['--bg-page']);
  document.head.appendChild(meta);

  localStorage.setItem('binance_bot_theme', 'dark');
  (0, eval)(script);
  expect(document.documentElement.dataset.theme).toBe('dark');
  expect(meta.getAttribute('content')).toBe(palettes.dark['--bg-page']);

  localStorage.setItem('binance_bot_theme', 'light');
  (0, eval)(script);
  expect(document.documentElement.dataset.theme).toBe('light');
  expect(meta.getAttribute('content')).toBe(palettes.light['--bg-page']);

  localStorage.removeItem('binance_bot_theme');
  vi.stubGlobal('matchMedia', () => { throw new Error('matchMedia blocked'); });
  expect(() => (0, eval)(script)).not.toThrow();
  expect(document.documentElement.dataset.theme).toBe('light');
  expect(meta.getAttribute('content')).toBe(palettes.light['--bg-page']);
});

it('maintains WCAG AA >= 4.5:1 contrast across light and dark semantic tokens and keeps live contrast above stale', () => {
  for (const mode of ['light', 'dark'] as const) {
    const p = palettes[mode];
    const pairs: [string, string, string][] = [
      [`${mode} text-primary / bg-card`, p['--text-primary'], p['--bg-card']],
      [`${mode} text-secondary / bg-card`, p['--text-secondary'], p['--bg-card']],
      [`${mode} text-secondary / bg-raised`, p['--text-secondary'], p['--bg-raised']],
      [`${mode} text-muted / bg-card`, p['--text-muted'], p['--bg-card']],
      [`${mode} text-muted / bg-raised`, p['--text-muted'], p['--bg-raised']],
      [`${mode} positive / bg-card`, p['--positive'], p['--bg-card']],
      [`${mode} positive / bg-raised`, p['--positive'], p['--bg-raised']],
      [`${mode} positive-stale / bg-card`, p['--positive-stale'], p['--bg-card']],
      [`${mode} positive-stale / bg-raised`, p['--positive-stale'], p['--bg-raised']],
      [`${mode} negative / bg-card`, p['--negative'], p['--bg-card']],
      [`${mode} negative / bg-raised`, p['--negative'], p['--bg-raised']],
      [`${mode} negative-stale / bg-card`, p['--negative-stale'], p['--bg-card']],
      [`${mode} negative-stale / bg-raised`, p['--negative-stale'], p['--bg-raised']],
      [`${mode} sig-calm / sig-calm-bg`, p['--sig-calm'], p['--sig-calm-bg']],
      [`${mode} sig-blue / sig-blue-bg`, p['--sig-blue'], p['--sig-blue-bg']],
      [`${mode} sig-yellow / sig-yellow-bg`, p['--sig-yellow'], p['--sig-yellow-bg']],
      [`${mode} sig-yellow / sig-yellow-hover`, p['--sig-yellow'], p['--sig-yellow-hover']],
      [`${mode} sig-orange / sig-orange-bg`, p['--sig-orange'], p['--sig-orange-bg']],
      [`${mode} sig-red / sig-red-bg`, p['--sig-red'], p['--sig-red-bg']],
      [`${mode} sig-red / sig-red-hover`, p['--sig-red'], p['--sig-red-hover']],
      [`${mode} rail-muted / rail-hover`, p['--rail-muted'], p['--rail-hover']],
      [`${mode} rail-positive / rail-active`, p['--rail-positive'], p['--rail-active']],
      [`${mode} rail-negative / rail-active`, p['--rail-negative'], p['--rail-active']],
    ];
    for (const [label, fg, bg] of pairs) {
      expect(contrastRatio(fg, bg), label).toBeGreaterThanOrEqual(4.5);
    }
    expect(contrastRatio(p['--positive'], p['--bg-card'])).toBeGreaterThan(contrastRatio(p['--positive-stale'], p['--bg-card']));
    expect(contrastRatio(p['--negative'], p['--bg-card'])).toBeGreaterThan(contrastRatio(p['--negative-stale'], p['--bg-card']));
  }
  expect(contrastRatio('#ffffff', '#9a6700')).toBeGreaterThanOrEqual(4.5);
  expect(contrastRatio('#ffffff', '#9f6b00')).toBeGreaterThanOrEqual(4.5);
  expect(contrastRatio('#ffffff', '#8f5f00')).toBeGreaterThanOrEqual(4.5);
  expect(contrastRatio(componentColors.dark.buttonForeground, palettes.dark['--accent'])).toBeGreaterThanOrEqual(4.5);
});

it('applies dark Ant Design component tokens for Switch and Checkbox without custom dark CSS overrides', () => {
  localStorage.setItem('binance_bot_theme', 'dark');
  const { container } = render(
    <ThemeProvider>
      <TokenProbe />
      <Checkbox checked onChange={() => {}}>确认</Checkbox>
      <Switch checked checkedChildren="开启" unCheckedChildren="关闭" />
    </ThemeProvider>,
  );
  const derivedPrimary = screen.getByTestId('color-primary').textContent ?? '';
  expect(derivedPrimary).toMatch(/^#[0-9a-f]{6}$/i);
  expect(contrastRatio(componentColors.dark.buttonForeground, derivedPrimary)).toBeGreaterThanOrEqual(4.5);
  const switchEl = container.querySelector<HTMLElement>('.ant-switch-checked');
  expect(switchEl).not.toBeNull();
  expect(getComputedStyle(switchEl!).backgroundColor).toBe('rgb(154, 103, 0)');
});
