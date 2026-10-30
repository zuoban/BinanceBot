export function LoadingScreen({ label = '正在准备工作台' }: { label?: string }) {
  return <div className="loading-screen" role="status" aria-live="polite">
    <div className="loading-brand"><img className="brand-symbol" src="/logo.svg" alt="" /><span>GRID CONSOLE<small>STRATEGY WORKSPACE</small></span></div>
    <div className="loading-indicator"><span /><span /><span /></div>
    <p>{label}</p>
  </div>;
}

export function PageLoading() {
  return <div className="page-placeholder" role="status" aria-label="正在加载页面">
    <div className="skeleton-line skeleton-eyebrow" />
    <div className="skeleton-line skeleton-title" />
    <div className="skeleton-line skeleton-caption" />
    <div className="skeleton-block skeleton-hero" />
    <div className="placeholder-grid"><div className="skeleton-block" /><div className="skeleton-block" /><div className="skeleton-block" /></div>
    <span className="sr-only">正在加载页面</span>
  </div>;
}

export function OverviewLoading() {
  return <div className="overview-placeholder" role="status" aria-label="正在同步策略数据">
    <div className="placeholder-grid"><div className="skeleton-block" /><div className="skeleton-block" /><div className="skeleton-block" /></div>
    <div className="skeleton-line skeleton-section" />
    <div className="placeholder-filters"><div className="skeleton-line" /><div className="skeleton-line" /><div className="skeleton-line" /></div>
    <div className="placeholder-grid placeholder-strategies"><div className="skeleton-block" /><div className="skeleton-block" /></div>
    <span className="sr-only">正在同步策略数据</span>
  </div>;
}
