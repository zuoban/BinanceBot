import { readFileSync } from 'node:fs';
import { render, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { HourlyChart } from '../components/Charts';
import OverviewHistoryChart from '../components/OverviewHistoryChart';
import { designTokensCss } from '../lib/designTokens';
import { snapshot } from './fixtures';

let styleEl: HTMLStyleElement;

beforeEach(() => {
  styleEl = document.createElement('style');
  styleEl.textContent = `${designTokensCss}\n${readFileSync('src/styles/base.css', 'utf8')}\n${readFileSync('src/styles/detail-forms.css', 'utf8')}`;
  document.head.appendChild(styleEl);
  vi.stubGlobal('ResizeObserver', class {
    private readonly callback: ResizeObserverCallback;
    constructor(callback: ResizeObserverCallback) { this.callback = callback; }
    observe(target: Element) {
      this.callback([{ target, contentRect: { width: 640, height: 236 } as DOMRectReadOnly } as ResizeObserverEntry], this as unknown as ResizeObserver);
    }
    unobserve() {}
    disconnect() {}
  });
});

afterEach(() => {
  styleEl.remove();
  vi.unstubAllGlobals();
});

it('suppresses focus outlines on Recharts wrapper, surface, and zIndex layers while keeping control focus rings', async () => {
  const baseHour = 1_762_905_600;
  const data = [
    { hour: baseHour - 3600, equity: 2180 },
    { hour: baseHour, equity: 2340.84 },
  ];
  const now = Date.parse('2026-11-05T00:30:00Z');
  const hourlySnapshot = snapshot();
  hourlySnapshot.hourly_trade_stats = {
    window_start: '2026-11-04T16:00:00Z',
    window_end: new Date(now).toISOString(),
    realized_pnl: '0',
    pending_pnl_count: 0,
    buckets: [{ hour_start: '2026-11-05T00:00:00Z', buy_count: 2, sell_count: 1 }],
  };

  const { container } = render(
    <div>
      <button type="button">普通按钮</button>
      <div className="history-plot chart">
        <OverviewHistoryChart
          data={data}
          ticks={[baseHour - 3600, baseHour]}
          asset="USDC"
          formatValue={value => String(value)}
          formatDate={hour => String(hour)}
        />
      </div>
      <HourlyChart snapshot={hourlySnapshot} now={now} />
    </div>,
  );

  const button = container.querySelector<HTMLButtonElement>('button')!;
  button.focus();
  expect(getComputedStyle(button).outlineStyle).toBe('solid');

  await waitFor(() => expect(container.querySelectorAll('.recharts-wrapper')).toHaveLength(2));
  const wrappers = container.querySelectorAll<HTMLElement>('.recharts-wrapper');
  expect(wrappers).toHaveLength(2);

  for (const wrapper of wrappers) {
    const surface = wrapper.querySelector<SVGElement>('.recharts-surface');
    const zIndexLayer = wrapper.querySelector<SVGGElement>('[class*="recharts-zIndex-layer_"][tabindex="-1"]');
    expect(surface).not.toBeNull();
    expect(zIndexLayer).not.toBeNull();
    expect(getComputedStyle(wrapper).outlineStyle).toBe('none');

    surface!.focus();
    expect(document.activeElement).toBe(surface);
    expect(getComputedStyle(surface!).outlineStyle).toBe('none');

    zIndexLayer!.focus();
    expect(document.activeElement).toBe(zIndexLayer);
    expect(getComputedStyle(zIndexLayer!).outlineStyle).toBe('none');
  }
});
