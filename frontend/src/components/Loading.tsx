export function LoadingScreen({ label = '正在准备工作台' }: { label?: string }) {
  return <div className="loading-screen" role="status" aria-live="polite">
    <div className="loading-brand"><img className="brand-symbol" src="/logo.svg" alt="" /><span>GRID CONSOLE<small>STRATEGY WORKSPACE</small></span></div>
    <div className="loading-indicator"><span /><span /><span /></div>
    <p>{label}</p>
  </div>;
}

function SkeletonLine({ width, size }: { width: string; size?: 'large' | 'small' }) {
  return <div className={`skeleton-line${size ? ` skeleton-line-${size}` : ''}`} style={{ width }} />;
}

function SkeletonCard({ variant = 'metric' }: { variant?: 'metric' | 'strategy' | 'chart' }) {
  return <div className={`skeleton-card skeleton-card-${variant}`}>
    <div className="skeleton-card-heading"><SkeletonLine width="36%" size="small" /><SkeletonLine width="18%" size="small" /></div>
    <SkeletonLine width={variant === 'chart' ? '28%' : '48%'} size="large" />
    {variant === 'metric' ? <SkeletonLine width="62%" size="small" /> :
      <div className={`skeleton-card-content skeleton-card-content-${variant}`}>
        {[0, 1, 2, 3].map(item => <div key={item}><SkeletonLine width="65%" size="small" /><SkeletonLine width="85%" /></div>)}
      </div>}
  </div>;
}

export function PageLoading() {
  return <div className="page-placeholder" role="status" aria-label="正在加载页面">
    <div className="skeleton-page-heading"><SkeletonLine width="170px" size="large" /><SkeletonLine width="240px" /></div>
    <div className="skeleton-card skeleton-page-panel"><SkeletonLine width="32%" /><SkeletonLine width="55%" size="large" /><SkeletonLine width="42%" /></div>
    <div className="skeleton-grid skeleton-grid-metrics">{[0, 1, 2, 3].map(item => <SkeletonCard key={item} />)}</div>
    <span className="sr-only">正在加载页面</span>
  </div>;
}

export function OverviewLoading() {
  return <div className="overview-placeholder" role="status" aria-label="正在同步策略数据">
    <div className="skeleton-section-heading"><SkeletonLine width="110px" /></div>
    <div className="skeleton-grid skeleton-grid-finances">{[0, 1].map(item => <SkeletonCard key={item} variant="strategy" />)}</div>
    <div className="skeleton-section-heading"><SkeletonLine width="110px" /><SkeletonLine width="112px" /></div>
    <div className="skeleton-filters"><SkeletonLine width="180px" /><SkeletonLine width="140px" /></div>
    <div className="skeleton-grid skeleton-grid-strategies">{[0, 1, 2].map(item => <SkeletonCard key={item} variant="strategy" />)}</div>
    <span className="sr-only">正在同步策略数据</span>
  </div>;
}

export function DetailLoading() {
  return <div className="detail-placeholder" role="status" aria-label="正在加载策略详情">
    <div className="skeleton-grid skeleton-grid-metrics">{[0, 1, 2, 3].map(item => <SkeletonCard key={item} />)}</div>
    <div className="skeleton-tabs"><SkeletonLine width="72px" /><SkeletonLine width="64px" /><SkeletonLine width="64px" /></div>
    <div className="skeleton-grid skeleton-grid-charts">{[0, 1].map(item => <SkeletonCard key={item} variant="chart" />)}</div>
    <span className="sr-only">正在加载策略详情</span>
  </div>;
}
