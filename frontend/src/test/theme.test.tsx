import { act, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { ThemeProvider, useTheme } from '../lib/theme';

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
