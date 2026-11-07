import { Component, type ReactNode } from 'react';

type Props = { children: ReactNode; label: string; onReload?: () => void };

export class LoadErrorBoundary extends Component<Props, { failed: boolean }> {
  state = { failed: false };

  static getDerivedStateFromError() {
    return { failed: true };
  }

  render() {
    if (this.state.failed) return <div className="history-empty" role="alert">
      <span>{this.props.label}加载失败，请刷新页面重试。</span>
      <button type="button" onClick={this.props.onReload ?? (() => window.location.reload())}>刷新页面</button>
    </div>;
    return this.props.children;
  }
}
