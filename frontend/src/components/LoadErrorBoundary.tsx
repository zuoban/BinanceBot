import { Component, type ReactNode } from 'react';
import { Button } from 'antd';
type Props = { children: ReactNode; label: string; onReload?: () => void };

export class LoadErrorBoundary extends Component<Props, { failed: boolean }> {
  state = { failed: false };

  static getDerivedStateFromError() {
    return { failed: true };
  }

  componentDidCatch(error: unknown) {
    console.error(`${this.props.label}加载失败`, error);
  }

  render() {
    if (this.state.failed) return <div className="history-empty" role="alert">
      <span>{this.props.label}加载失败，请刷新页面重试。</span>
      <Button size="small" onClick={this.props.onReload ?? (() => window.location.reload())}>刷新页面</Button>
    </div>;
    return this.props.children;
  }
}
