import { memo, useEffect, useId, useMemo, useRef, useState, type KeyboardEvent, type MouseEvent } from 'react';
import { buildMonotoneCurve, estimateYAxisWidth, niceLinearTicks, useChartWidth, type PlotPoint } from '../lib/chartMath';
import { isValidEpochMs } from '../lib/format';

type HistoryPoint = { hour: number; equity: number | null };
const hourFormat = new Intl.DateTimeFormat('zh-CN', { timeZone: 'Asia/Shanghai', hour: '2-digit', minute: '2-digit', hour12: false });
const dayFormat = new Intl.DateTimeFormat('zh-CN', { timeZone: 'Asia/Shanghai', month: '2-digit', day: '2-digit' });
const wanFormat = new Intl.NumberFormat('en-US', { maximumFractionDigits: 6 });
const formatHour = (hour: number) => isValidEpochMs(hour * 1000) ? hourFormat.format(hour * 1000) : '--';
const formatDay = (hour: number) => isValidEpochMs(hour * 1000) ? dayFormat.format(hour * 1000) : '--';

export default memo(function OverviewHistoryChart({ data, ticks, asset, formatValue, formatDate, daily = false, discrete = false }: {
  data: HistoryPoint[];
  ticks: number[];
  asset: string | undefined;
  formatValue: (value: unknown) => string;
  formatDate: (hour: number) => string;
  daily?: boolean;
  discrete?: boolean;
}) {
  const wrapRef = useRef<HTMLDivElement>(null);
  const width = useChartWidth(wrapRef);
  const rectRef = useRef<DOMRect | null>(null);
  const [hoverIndex, setHoverIndex] = useState<number | null>(null);
  const [focusIndex, setFocusIndex] = useState<number | null>(null);
  useEffect(() => { rectRef.current = null; }, [width]);
  const rawId = useId().replace(/[^a-zA-Z0-9_-]/g, '');
  const fillId = `equityAreaFill-${rawId}`;
  const useWan = data.some(point => point.equity !== null && Math.abs(point.equity) >= 100_000);
  const lineColor = daily ? 'var(--text-secondary)' : 'var(--history-equity)';
  const chartHeight = daily ? 196 : 236;
  const marginRight = daily ? 16 : 12;
  const marginTop = 12;
  const marginBottom = 24;
  const marginLeft = 4;

  const { yTicks, minVal, maxVal, yLabels, yAxisWidth } = useMemo(() => {
    let minFound = Infinity;
    let maxFound = -Infinity;
    for (const point of data) {
      if (point.equity !== null && Number.isFinite(point.equity)) {
        if (point.equity < minFound) minFound = point.equity;
        if (point.equity > maxFound) maxFound = point.equity;
      }
    }
    const domain = minFound <= maxFound ? niceLinearTicks(minFound, maxFound, daily ? 3 : 4) : { ticks: [0, 1], min: 0, max: 1 };
    const labels = domain.ticks.map(val => (!Number.isFinite(val) ? '--' : useWan ? `${wanFormat.format(val / 10000)}万` : formatValue(val)));
    return {
      yTicks: domain.ticks,
      minVal: domain.min,
      maxVal: domain.max,
      yLabels: labels,
      yAxisWidth: estimateYAxisWidth(labels, 34),
    };
  }, [data, daily, useWan, formatValue]);

  const plotLeft = marginLeft + yAxisWidth;
  const plotRight = Math.max(plotLeft + 40, width - marginRight);
  const plotWidth = plotRight - plotLeft;
  const plotTop = marginTop;
  const plotBottom = chartHeight - marginBottom;
  const plotHeight = Math.max(1, plotBottom - plotTop);
  const domainSpan = Math.max(1e-9, maxVal - minVal);

  const { segments, plottedPoints, validIndices } = useMemo(() => {
    const runs: PlotPoint[][] = [];
    let currentRun: PlotPoint[] = [];
    const points: ({ index: number; hour: number; equity: number; cx: number; cy: number; isolated: boolean })[] = [];
    const indices: number[] = [];
    for (let idx = 0; idx < data.length; idx++) {
      const item = data[idx];
      if (item.equity !== null && Number.isFinite(item.equity)) {
        const cx = data.length <= 1 ? plotLeft + plotWidth / 2 : plotLeft + (idx / (data.length - 1)) * plotWidth;
        const cy = plotBottom - ((item.equity - minVal) / domainSpan) * plotHeight;
        const isolated = discrete || (data[idx - 1]?.equity == null && data[idx + 1]?.equity == null);
        points.push({ index: idx, hour: item.hour, equity: item.equity, cx, cy, isolated });
        indices.push(idx);
        currentRun.push({ x: cx, y: cy });
      } else if (currentRun.length > 0) {
        runs.push(currentRun);
        currentRun = [];
      }
    }
    if (currentRun.length > 0) runs.push(currentRun);
    const curves = discrete ? [] : runs.filter(run => run.length >= 2).map(run => buildMonotoneCurve(run, !daily ? plotBottom : undefined));
    return { segments: curves, plottedPoints: points, validIndices: indices };
  }, [data, discrete, daily, plotLeft, plotWidth, plotBottom, plotHeight, minVal, domainSpan]);

  const visibleXTicks = useMemo(() => {
    const hourToIndex = new Map<number, number>();
    for (let idx = 0; idx < data.length; idx++) hourToIndex.set(data[idx].hour, idx);
    const positioned = ticks.flatMap(hour => {
      const idx = hourToIndex.get(hour);
      if (idx === undefined) return [];
      const posX = data.length <= 1 ? plotLeft + plotWidth / 2 : plotLeft + (idx / (data.length - 1)) * plotWidth;
      return [{ hour, idx, x: posX, label: (daily ? formatDay : formatHour)(hour) }];
    });
    if (positioned.length <= 1) return positioned;
    const minGap = daily ? 38 : 34;
    const selected: typeof positioned = [];
    let nextKeptX = Infinity;
    for (let pos = positioned.length - 1; pos >= 0; pos--) {
      const candidate = positioned[pos];
      if (nextKeptX - candidate.x >= minGap) {
        selected.push(candidate);
        nextKeptX = candidate.x;
      }
    }
    return selected.reverse();
  }, [data, ticks, daily, plotLeft, plotWidth]);

  const activeIndex = hoverIndex ?? focusIndex;
  const activePlot = useMemo(
    () => (activeIndex !== null ? plottedPoints.find(point => point.index === activeIndex) : undefined),
    [activeIndex, plottedPoints],
  );

  const handleMouseMove = (event: MouseEvent<SVGSVGElement>) => {
    if (!plottedPoints.length) return;
    const rect = rectRef.current ?? (rectRef.current = event.currentTarget.getBoundingClientRect());
    const cursorX = event.clientX - rect.left;
    if (cursorX < plotLeft - 8 || cursorX > plotRight + 8) {
      setHoverIndex(null);
      return;
    }
    let bestPoint = plottedPoints[0];
    let bestDist = Math.abs(bestPoint.cx - cursorX);
    for (let idx = 1; idx < plottedPoints.length; idx++) {
      const dist = Math.abs(plottedPoints[idx].cx - cursorX);
      if (dist < bestDist) {
        bestDist = dist;
        bestPoint = plottedPoints[idx];
      }
    }
    setHoverIndex(bestPoint.index);
  };

  const handleKeyDown = (event: KeyboardEvent<SVGSVGElement>) => {
    if (!validIndices.length) return;
    if (event.key === 'Escape') {
      setFocusIndex(null);
      return;
    }
    const currentPos = focusIndex !== null ? validIndices.indexOf(focusIndex) : -1;
    let nextPos = -1;
    if (event.key === 'ArrowRight' || event.key === 'ArrowDown') {
      nextPos = currentPos < 0 ? 0 : Math.min(validIndices.length - 1, currentPos + 1);
    } else if (event.key === 'ArrowLeft' || event.key === 'ArrowUp') {
      nextPos = currentPos < 0 ? validIndices.length - 1 : Math.max(0, currentPos - 1);
    } else if (event.key === 'Home') {
      nextPos = 0;
    } else if (event.key === 'End') {
      nextPos = validIndices.length - 1;
    }
    if (nextPos >= 0) {
      event.preventDefault();
      setHoverIndex(null);
      setFocusIndex(validIndices[nextPos]);
    }
  };

  const placeTooltipLeft = activePlot ? activePlot.cx > width * 0.55 : false;

  return <div ref={wrapRef} className="chart-wrap" style={{ height: chartHeight }} onMouseEnter={() => { rectRef.current = null; }} onMouseLeave={() => { rectRef.current = null; setHoverIndex(null); }}>
    <svg
      className="chart-surface"
      width={width}
      height={chartHeight}
      viewBox={`0 0 ${width} ${chartHeight}`}
      tabIndex={0}
      onMouseMove={handleMouseMove}
      onKeyDown={handleKeyDown}
      onBlur={() => setFocusIndex(null)}
    >
      {!daily && !discrete && <defs>
        <linearGradient id={fillId} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="var(--history-equity)" stopOpacity={0.18} />
          <stop offset="100%" stopColor="var(--history-equity)" stopOpacity={0} />
        </linearGradient>
      </defs>}
      <g className="chart-grid" aria-hidden="true">
        {yTicks.map(tick => {
          const tickY = plotBottom - ((tick - minVal) / domainSpan) * plotHeight;
          return <line key={tick} x1={plotLeft} x2={plotRight} y1={tickY} y2={tickY} stroke="var(--border-hairline)" />;
        })}
      </g>
      <g className="chart-axis chart-axis-y" aria-hidden="true">
        {yTicks.map((tick, idx) => {
          const tickY = plotBottom - ((tick - minVal) / domainSpan) * plotHeight;
          return <text key={tick} x={plotLeft - 6} y={tickY + 4} textAnchor="end" className="chart-tick">{yLabels[idx]}</text>;
        })}
      </g>
      <g className="chart-axis chart-axis-x" aria-hidden="true">
        {visibleXTicks.map(tick => {
          const anchor = tick.idx === 0 ? 'start' : tick.idx === data.length - 1 ? 'end' : 'middle';
          return <text key={tick.hour} x={tick.x} y={chartHeight - 6} textAnchor={anchor} className="chart-tick">{tick.label}</text>;
        })}
      </g>
      <g className="chart-plot-layer" tabIndex={-1}>
        {activePlot && (
          <line
            x1={activePlot.cx}
            x2={activePlot.cx}
            y1={plotTop}
            y2={plotBottom}
            stroke="var(--border-strong)"
            strokeDasharray="3 4"
          />
        )}
        {segments.map((segment, idx) => <g key={idx}>
          {segment.areaPath && <path d={segment.areaPath} fill={`url(#${fillId})`} />}
          {segment.linePath && <path d={segment.linePath} fill="none" stroke={lineColor} strokeWidth={daily ? 1.5 : 2} />}
        </g>)}
        {plottedPoints.map(point => (
          <circle
            key={point.hour}
            cx={point.cx}
            cy={point.cy}
            r={2}
            fill={lineColor}
            className={point.hour % (4 * 3600) === 0 || point.isolated ? undefined : 'history-point-hourly'}
          />
        ))}
        {activePlot && (
          <circle
            cx={activePlot.cx}
            cy={activePlot.cy}
            r={4}
            fill={daily ? 'var(--text-secondary)' : 'var(--history-equity)'}
          />
        )}
      </g>
    </svg>
    {activePlot && (
      <div
        className="chart-tooltip"
        role="status"
        aria-live="polite"
        style={placeTooltipLeft
          ? { right: Math.max(8, Math.round(width - activePlot.cx + 10)), top: marginTop }
          : { left: Math.min(Math.max(8, Math.round(activePlot.cx + 10)), Math.max(8, width - 190)), top: marginTop }}
      >
        <div className="chart-tooltip-title">{formatDate(activePlot.hour)} · 北京时间</div>
        <div className="chart-tooltip-row"><span>账户权益</span><strong>{formatValue(activePlot.equity)}{asset ? ` ${asset}` : ''}</strong></div>
      </div>
    )}
  </div>;
});
