import { useLayoutEffect, useState, type RefObject } from 'react';

export interface PlotPoint {
  x: number;
  y: number;
}

export function fmtCoord(value: number): string {
  return Number(value.toFixed(2)).toString();
}

export function useChartWidth(ref: RefObject<HTMLElement | null>, fallback = 640): number {
  const [width, setWidth] = useState(fallback);
  useLayoutEffect(() => {
    const element = ref.current;
    if (!element) return;
    const initial = Math.round(element.getBoundingClientRect().width || element.clientWidth);
    if (initial > 0) setWidth(initial);
    if (typeof ResizeObserver !== 'function') return;
    const observer = new ResizeObserver(entries => {
      const rectWidth = Math.round(entries[0]?.contentRect.width ?? 0);
      if (rectWidth > 0) setWidth(prev => (prev === rectWidth ? prev : rectWidth));
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, [ref]);
  return width;
}

export function estimateYAxisWidth(labels: readonly string[], minWidth = 28): number {
  let maxUnits = 0;
  for (const label of labels) {
    let units = 0;
    for (const ch of label) {
      units += ch.charCodeAt(0) > 127 ? 1.7 : 1;
    }
    if (units > maxUnits) maxUnits = units;
  }
  return Math.max(minWidth, Math.ceil(maxUnits * 6.8) + 10);
}

export function niceIntegerTicks(maxValue: number, targetCount = 4): { ticks: number[]; domainMax: number } {
  const safeMax = Number.isFinite(maxValue) && maxValue > 0 ? Math.ceil(maxValue) : 0;
  if (safeMax === 0) return { ticks: [0, 1, 2, 3, 4], domainMax: 4 };
  const rawStep = safeMax / Math.max(1, targetCount);
  const magnitude = 10 ** Math.floor(Math.log10(Math.max(1, rawStep)));
  const residual = rawStep / magnitude;
  const niceBase = residual <= 1 ? 1 : residual <= 2 ? 2 : residual <= 5 ? 5 : 10;
  const step = Math.max(1, niceBase * magnitude);
  const count = Math.max(1, Math.ceil(safeMax / step));
  const domainMax = count * step;
  const ticks = Array.from({ length: count + 1 }, (_, idx) => idx * step);
  return { ticks, domainMax };
}

function cleanFloat(value: number): number {
  return Math.abs(value) < 1e-12 ? 0 : Number(value.toPrecision(12));
}

export function niceLinearTicks(minValue: number, maxValue: number, targetCount: number): { ticks: number[]; min: number; max: number } {
  if (!Number.isFinite(minValue) || !Number.isFinite(maxValue)) {
    return { ticks: [0, 1], min: 0, max: 1 };
  }
  let low = Math.min(minValue, maxValue);
  let high = Math.max(minValue, maxValue);
  if (low === high) {
    if (low === 0) return { ticks: [0, 1, 2], min: 0, max: 2 };
    const pad = Math.abs(low) * 0.02 || 1;
    low = low >= 0 ? Math.max(0, low - pad) : low - pad;
    high = high + pad;
  }
  const span = high - low;
  const rawStep = span / Math.max(1, targetCount - 1);
  const magnitude = 10 ** Math.floor(Math.log10(rawStep));
  const residual = rawStep / magnitude;
  const niceBase = residual <= 1 ? 1 : residual <= 2 ? 2 : residual <= 2.5 ? 2.5 : residual <= 5 ? 5 : 10;
  const step = niceBase * magnitude;
  let niceMin = Math.floor(low / step) * step;
  if (minValue >= 0 && niceMin < 0) niceMin = 0;
  const niceMax = Math.ceil(high / step) * step;
  const finalMax = niceMax > niceMin ? niceMax : niceMin + step;
  const count = Math.max(1, Math.round((finalMax - niceMin) / step));
  const ticks = Array.from({ length: count + 1 }, (_, idx) => cleanFloat(niceMin + idx * step));
  return { ticks, min: ticks[0], max: ticks[ticks.length - 1] };
}

export function buildMonotoneCurve(segment: readonly PlotPoint[], baselineY?: number): { linePath: string; areaPath: string | null } {
  const count = segment.length;
  if (count < 2) return { linePath: '', areaPath: null };
  if (count === 2) {
    const [first, second] = segment;
    const linePath = `M${fmtCoord(first.x)},${fmtCoord(first.y)}L${fmtCoord(second.x)},${fmtCoord(second.y)}`;
    const areaPath = baselineY !== undefined
      ? `${linePath}L${fmtCoord(second.x)},${fmtCoord(baselineY)}L${fmtCoord(first.x)},${fmtCoord(baselineY)}Z`
      : null;
    return { linePath, areaPath };
  }
  const dx = new Array<number>(count - 1);
  const slopes = new Array<number>(count - 1);
  for (let idx = 0; idx < count - 1; idx++) {
    const spanX = segment[idx + 1].x - segment[idx].x;
    dx[idx] = spanX;
    slopes[idx] = spanX > 0 ? (segment[idx + 1].y - segment[idx].y) / spanX : 0;
  }
  const tangents = new Array<number>(count);
  tangents[0] = slopes[0];
  tangents[count - 1] = slopes[count - 2];
  for (let idx = 1; idx < count - 1; idx++) {
    const prevSlope = slopes[idx - 1];
    const nextSlope = slopes[idx];
    if (prevSlope * nextSlope <= 0) {
      tangents[idx] = 0;
    } else {
      const prevDx = dx[idx - 1];
      const nextDx = dx[idx];
      const common = prevDx + nextDx;
      tangents[idx] = (3 * common) / ((common + nextDx) / prevSlope + (common + prevDx) / nextSlope);
    }
  }
  let linePath = `M${fmtCoord(segment[0].x)},${fmtCoord(segment[0].y)}`;
  for (let idx = 0; idx < count - 1; idx++) {
    const cur = segment[idx];
    const next = segment[idx + 1];
    const third = dx[idx] / 3;
    const cp1x = cur.x + third;
    const cp1y = cur.y + tangents[idx] * third;
    const cp2x = next.x - third;
    const cp2y = next.y - tangents[idx + 1] * third;
    linePath += `C${fmtCoord(cp1x)},${fmtCoord(cp1y)} ${fmtCoord(cp2x)},${fmtCoord(cp2y)} ${fmtCoord(next.x)},${fmtCoord(next.y)}`;
  }
  const first = segment[0];
  const last = segment[count - 1];
  const areaPath = baselineY !== undefined
    ? `${linePath}L${fmtCoord(last.x)},${fmtCoord(baselineY)}L${fmtCoord(first.x)},${fmtCoord(baselineY)}Z`
    : null;
  return { linePath, areaPath };
}

export function topRoundedBarPath(barX: number, barY: number, barWidth: number, barHeight: number, radius = 2): string {
  if (barHeight <= 0 || barWidth <= 0) return '';
  const corner = Math.min(radius, barWidth / 2, barHeight);
  const right = barX + barWidth;
  const bottom = barY + barHeight;
  return `M${fmtCoord(barX)},${fmtCoord(bottom)}L${fmtCoord(barX)},${fmtCoord(barY + corner)}Q${fmtCoord(barX)},${fmtCoord(barY)} ${fmtCoord(barX + corner)},${fmtCoord(barY)}L${fmtCoord(right - corner)},${fmtCoord(barY)}Q${fmtCoord(right)},${fmtCoord(barY)} ${fmtCoord(right)},${fmtCoord(barY + corner)}L${fmtCoord(right)},${fmtCoord(bottom)}Z`;
}
