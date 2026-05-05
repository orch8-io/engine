import { Component, type ReactNode } from "react";

interface Props {
  children: ReactNode;
}

interface State {
  error: Error | null;
}

export class ErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = { error: null };
  }

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  override render() {
    if (this.state.error) {
      return (
        <div className="min-h-screen flex items-center justify-center bg-bg text-ink px-6">
          <div className="max-w-lg w-full space-y-6">
            <div className="text-[11px] font-mono uppercase tracking-[0.16em] text-warn">
              Runtime error
            </div>
            <h1 className="text-[22px] font-semibold tracking-tight">
              Something went wrong
            </h1>
            <p className="text-[13px] text-muted leading-relaxed">
              The dashboard hit an unexpected error. This is usually a bug in
              the UI code, not the engine itself. Try refreshing the page.
            </p>
            <pre className="well text-[12px] font-mono text-warn whitespace-pre-wrap">
              {this.state.error.message}
            </pre>
            <button
              onClick={() => window.location.reload()}
              className="text-[13px] text-signal hover:underline"
            >
              Reload page →
            </button>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}
