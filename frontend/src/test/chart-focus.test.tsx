import { readFileSync } from 'node:fs';
import { render, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { HourlyChart } from '../components/Charts';
import OverviewHistoryChart from '../components/OverviewHistoryChart';
import { designTokensCss, palettes } from '../lib/designTokens';
import { snapshot } from './fixtures';

let styleEl: HTMLStyleElement;

beforeEach(() => {
  styleEl = document.createElement('style');
  styleEl.textContent = `${designTokensCss}\n${readFileSync('src/styles/base.css', 'utf8')}\n${readFileSync('src/styles/overview.css', 'utf8')}\n${readFileSync('src/styles/detail-forms.css', 'utf8')}`;
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

function hexToRgb(hex: string) {
  const v = hex.replace('#', '');
  return `rgb(${Number.parseInt(v.slice(0, 2), 16)}, ${Number.parseInt(v.slice(2, 4), 16)}, ${Number.parseInt(v.slice(4, 6), 16)})`;
}

it('preserves log-row-error and log-row-warn backgrounds over Ant Design hover cells and resolves semantic colors without !important', () => {
  const antHoverStyle = document.createElement('style');
  antHoverStyle.textContent = '.ant-table-wrapper .ant-table-tbody > tr > td.ant-table-cell-row-hover { background: rgb(247, 247, 248); }';
  document.head.prepend(antHoverStyle);
  try {
    const { container } = render(
      <div>
        <div className="ant-table-wrapper">
          <table>
            <tbody className="ant-table-tbody">
              <tr className="log-row-error"><td className="ant-table-cell-row-hover">异常</td></tr>
              <tr className="log-row-warn"><td className="ant-table-cell-row-hover">告警</td></tr>
            </tbody>
          </table>
        </div>
        <div className="today-trade-summary">
          <div>
            <span>已实现盈亏</span>
            <strong><span className="positive">+12.5</span> / <span className="negative">-3.2</span></strong>
            <small className="pending-pnl-warn">盈亏待同步</small>
            <small className="plain-small">笔</small>
          </div>
        </div>
        <div className="finance-metrics">
          <div><span>未实现盈亏</span><strong><span className="positive finance-pos">+24.00</span></strong></div>
        </div>
        <div className="strategy-metrics">
          <div><span>已实现盈亏</span><strong><span className="negative strategy-neg">-6.50</span></strong></div>
        </div>
        <div className="is-stale">
          <span className="positive stale-pos">+8.0</span>
          <span className="negative stale-neg">-1.5</span>
        </div>
      </div>,
    );

    const errorCell = container.querySelector<HTMLElement>('tr.log-row-error > td')!;
    const warnCell = container.querySelector<HTMLElement>('tr.log-row-warn > td')!;
    expect(getComputedStyle(errorCell).backgroundColor).toBe(hexToRgb(palettes.light['--sig-red-bg']));
    expect(getComputedStyle(warnCell).backgroundColor).toBe(hexToRgb(palettes.light['--sig-yellow-bg']));

    expect(getComputedStyle(container.querySelector<HTMLElement>('.positive')!).color).toBe(hexToRgb(palettes.light['--positive']));
    expect(getComputedStyle(container.querySelector<HTMLElement>('.negative')!).color).toBe(hexToRgb(palettes.light['--negative']));
    expect(getComputedStyle(container.querySelector<HTMLElement>('.pending-pnl-warn')!).color).toBe(hexToRgb(palettes.light['--sig-yellow']));
    expect(getComputedStyle(container.querySelector<HTMLElement>('.plain-small')!).color).toBe(hexToRgb(palettes.light['--text-secondary']));
    const financePos = container.querySelector<HTMLElement>('.finance-pos')!;
    const strategyNeg = container.querySelector<HTMLElement>('.strategy-neg')!;
    expect(getComputedStyle(financePos).color).toBe(hexToRgb(palettes.light['--positive']));
    expect(getComputedStyle(financePos).fontSize).toBe('14px');
    expect(getComputedStyle(strategyNeg).color).toBe(hexToRgb(palettes.light['--negative']));
    expect(getComputedStyle(strategyNeg).fontSize).toBe('14px');
    expect(getComputedStyle(container.querySelector<HTMLElement>('.stale-pos')!).color).toBe(hexToRgb(palettes.light['--positive-stale']));
    expect(getComputedStyle(container.querySelector<HTMLElement>('.stale-neg')!).color).toBe(hexToRgb(palettes.light['--negative-stale']));
  } finally {
    antHoverStyle.remove();
  }
});
