import { memo, useEffect, useMemo, useRef, useState, type KeyboardEvent, type MouseEvent } from 'react';
import { estimateYAxisWidth, niceIntegerTicks, topRoundedBarPath, useChartWidth } from '../lib/chartMath';

export type HourlyChartPoint = { hour: string; 买入: number | null; 卖出: number | null };

const CHART_HEIGHT = 264;
const MARGIN_TOP = 12;
const MARGIN_RIGHT = 12;
const MARGIN_BOTTOM = 26;
const MARGIN_LEFT = 4;
const BAR_GAP = 2;
const MAX_BAR_SIZE = 12;

export default memo(function HourlyChartView({ data }: { data: HourlyChartPoint[] }) {
  const wrapRef = useRef<HTMLDivElement>(null);
  const width = useChartWidth(wrapRef);
  const rectRef = useRef<DOMRect | null>(null);
  const [hoverIndex, setHoverIndex] = useState<number | null>(null);
  const [focusIndex, setFocusIndex] = useState<number | null>(null);
  useEffect(() => { rectRef.current = null; }, [width]);

  const { yTicks, domainMax, yAxisWidth } = useMemo(() => {
    let maxCount = 0;
    for (const point of data) {
      if (point.买入 !== null && point.买入 > maxCount) maxCount = point.买入;
      if (point.卖出 !== null && point.卖出 > maxCount) maxCount = point.卖出;
    }
    const { ticks, domainMax: computedMax } = niceIntegerTicks(maxCount, 4);
    return {
      yTicks: ticks,
      domainMax: computedMax,
      yAxisWidth: estimateYAxisWidth(ticks.map(String), 24),
    };
  }, [data]);

  const plotLeft = MARGIN_LEFT + yAxisWidth;
  const plotRight = Math.max(plotLeft + 40, width - MARGIN_RIGHT);
  const plotWidth = plotRight - plotLeft;
  const plotTop = MARGIN_TOP;
  const plotBottom = CHART_HEIGHT - MARGIN_BOTTOM;
  const plotHeight = Math.max(1, plotBottom - plotTop);
  const slotCount = Math.max(1, data.length);
  const slotWidth = plotWidth / slotCount;
  const barWidth = Math.max(2, Math.min(MAX_BAR_SIZE, (slotWidth * 0.72 - BAR_GAP) / 2));
  const groupWidth = barWidth * 2 + BAR_GAP;

  const xTickIndices = useMemo(() => {
    if (!data.length) return [];
    if (data.length === 1) return [0];
    const minGap = 40;
    const lastIdx = data.length - 1;
    const endX = plotLeft + (lastIdx + 0.5) * slotWidth;
    const kept = [0];
    let prevX = plotLeft + 0.5 * slotWidth;
    for (let idx = 1; idx < lastIdx; idx++) {
      const curX = plotLeft + (idx + 0.5) * slotWidth;
      if (curX - prevX >= minGap && endX - curX >= minGap * 0.75) {
        kept.push(idx);
        prevX = curX;
      }
    }
    kept.push(lastIdx);
    return kept;
  }, [data.length, plotLeft, slotWidth]);

  const activeIndices = useMemo(
    () => data.flatMap((point, idx) => (point.买入 !== null || point.卖出 !== null ? [idx] : [])),
    [data],
  );

  const activeIndex = hoverIndex ?? focusIndex;
  const activePoint = activeIndex !== null ? data[activeIndex] : undefined;
  const hasActiveValues = Boolean(activePoint && (activePoint.买入 !== null || activePoint.卖出 !== null));

  const handleMouseMove = (event: MouseEvent<SVGSVGElement>) => {
    if (!data.length) return;
    const rect = rectRef.current ?? (rectRef.current = event.currentTarget.getBoundingClientRect());
    const offsetX = event.clientX - rect.left - plotLeft;
    if (offsetX < 0 || offsetX > plotWidth) {
      setHoverIndex(null);
      return;
    }
    const nextIdx = Math.min(data.length - 1, Math.max(0, Math.floor(offsetX / slotWidth)));
    setHoverIndex(nextIdx);
  };

  const handleKeyDown = (event: KeyboardEvent<SVGSVGElement>) => {
    if (!activeIndices.length) return;
    if (event.key === 'Escape') {
      setFocusIndex(null);
      return;
    }
    const currentPos = focusIndex !== null ? activeIndices.indexOf(focusIndex) : -1;
    let nextPos = -1;
    if (event.key === 'ArrowRight' || event.key === 'ArrowDown') {
      nextPos = currentPos < 0 ? 0 : Math.min(activeIndices.length - 1, currentPos + 1);
    } else if (event.key === 'ArrowLeft' || event.key === 'ArrowUp') {
      nextPos = currentPos < 0 ? activeIndices.length - 1 : Math.max(0, currentPos - 1);
    } else if (event.key === 'Home') {
      nextPos = 0;
    } else if (event.key === 'End') {
      nextPos = activeIndices.length - 1;
    }
    if (nextPos >= 0) {
      event.preventDefault();
      setHoverIndex(null);
      setFocusIndex(activeIndices[nextPos]);
    }
  };

  const activeCenterX = activeIndex !== null ? plotLeft + (activeIndex + 0.5) * slotWidth : 0;
  const placeTooltipLeft = activeCenterX > width * 0.55;

  return <div ref={wrapRef} className="chart-wrap" style={{ height: CHART_HEIGHT }} onMouseEnter={() => { rectRef.current = null; }} onMouseLeave={() => { rectRef.current = null; setHoverIndex(null); }}>
    <svg
      className="chart-surface"
      width={width}
      height={CHART_HEIGHT}
      viewBox={`0 0 ${width} ${CHART_HEIGHT}`}
      tabIndex={0}
      onMouseMove={handleMouseMove}
      onKeyDown={handleKeyDown}
      onBlur={() => setFocusIndex(null)}
    >
      <g className="chart-grid" aria-hidden="true">
        {yTicks.map(tick => {
          const tickY = plotBottom - (tick / domainMax) * plotHeight;
          return <line key={tick} x1={plotLeft} x2={plotRight} y1={tickY} y2={tickY} stroke="var(--border-hairline)" />;
        })}
      </g>
      <g className="chart-axis chart-axis-y" aria-hidden="true">
        {yTicks.map(tick => {
          const tickY = plotBottom - (tick / domainMax) * plotHeight;
          return <text key={tick} x={plotLeft - 6} y={tickY + 4} textAnchor="end" className="chart-tick chart-tick-hourly">{tick}</text>;
        })}
      </g>
      <g className="chart-axis chart-axis-x" aria-hidden="true">
        {xTickIndices.map(idx => {
          const tickX = plotLeft + (idx + 0.5) * slotWidth;
          const anchor = idx === 0 ? 'start' : idx === data.length - 1 ? 'end' : 'middle';
          const alignedX = idx === 0 ? Math.max(plotLeft, tickX - slotWidth * 0.4) : idx === data.length - 1 ? Math.min(plotRight, tickX + slotWidth * 0.4) : tickX;
          return <text key={data[idx].hour} x={alignedX} y={CHART_HEIGHT - 6} textAnchor={anchor} className="chart-tick chart-tick-hourly">{data[idx].hour}</text>;
        })}
      </g>
      <g className="chart-plot-layer" tabIndex={-1}>
        {activeIndex !== null && hasActiveValues && (
          <rect
            x={plotLeft + activeIndex * slotWidth}
            y={plotTop}
            width={slotWidth}
            height={plotHeight}
            fill="var(--bg-raised)"
            opacity={0.75}
          />
        )}
        {data.map((point, idx) => {
          const groupStart = plotLeft + (idx + 0.5) * slotWidth - groupWidth / 2;
          const buyHeight = point.买入 && point.买入 > 0 ? (point.买入 / domainMax) * plotHeight : 0;
          const sellHeight = point.卖出 && point.卖出 > 0 ? (point.卖出 / domainMax) * plotHeight : 0;
          if (buyHeight <= 0 && sellHeight <= 0) return null;
          return <g key={point.hour}>
            {buyHeight > 0 && <path d={topRoundedBarPath(groupStart, plotBottom - buyHeight, barWidth, buyHeight)} fill="var(--positive)" />}
            {sellHeight > 0 && <path d={topRoundedBarPath(groupStart + barWidth + BAR_GAP, plotBottom - sellHeight, barWidth, sellHeight)} fill="var(--negative)" />}
          </g>;
        })}
      </g>
    </svg>
    {activePoint && hasActiveValues && (
      <div
        className="chart-tooltip"
        role="status"
        aria-live="polite"
        style={placeTooltipLeft
          ? { right: Math.max(8, Math.round(width - activeCenterX + 10)), top: MARGIN_TOP }
          : { left: Math.min(Math.max(8, Math.round(activeCenterX + 10)), Math.max(8, width - 150)), top: MARGIN_TOP }}
      >
        <div className="chart-tooltip-title">{activePoint.hour}</div>
        {activePoint.买入 !== null && <div className="chart-tooltip-row"><span className="positive">买入</span><strong>{activePoint.买入}</strong></div>}
        {activePoint.卖出 !== null && <div className="chart-tooltip-row"><span className="negative">卖出</span><strong>{activePoint.卖出}</strong></div>}
      </div>
    )}
  </div>;
});
