import { Component, type ReactNode } from 'react';
type Props = { children: ReactNode; label: string; resetKey?: unknown; onReload?: () => void };

export class LoadErrorBoundary extends Component<Props, { failed: boolean }> {
  state = { failed: false };

  static getDerivedStateFromError() {
    return { failed: true };
  }

  componentDidUpdate(prevProps: Props) {
    if (this.state.failed && !Object.is(prevProps.resetKey, this.props.resetKey)) {
      this.setState({ failed: false });
    }
  }

  componentDidCatch(error: unknown) {
    console.error(`${this.props.label}加载失败`, error);
  }

  render() {
    if (this.state.failed) return <div className="history-empty" role="alert">
      <span>{this.props.label}加载失败，请刷新页面重试。</span>
      <button type="button" className="ui-action-btn ui-action-btn-sm" onClick={this.props.onReload ?? (() => window.location.reload())}>刷新页面</button>
    </div>;
    return this.props.children;
  }
}
