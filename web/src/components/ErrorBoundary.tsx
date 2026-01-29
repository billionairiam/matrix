import React, { ReactNode, ReactElement } from 'react'

interface Props {
  children: ReactNode
}

interface State {
  hasError: boolean
  error?: Error
}

export class ErrorBoundary extends React.Component<Props, State> {
  constructor(props: Props) {
    super(props)
    this.state = { hasError: false }
  }

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error }
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
    console.error('ErrorBoundary caught:', error, errorInfo)
  }

  render(): ReactElement {
    if (this.state.hasError) {
      return (
        <div
          className="min-h-screen flex items-center justify-center"
          style={{ background: '#0B0E11' }}
        >
          <div className="text-center max-w-md mx-auto px-6">
            <div className="text-6xl mb-4">⚠️</div>
            <h1
              className="text-2xl font-bold mb-2"
              style={{ color: '#EAECEF' }}
            >
              Something went wrong
            </h1>
            <p className="mb-6" style={{ color: '#848E9C' }}>
              {this.state.error?.message || 'An unexpected error occurred'}
            </p>
            <button
              onClick={() => {
                this.setState({ hasError: false, error: undefined })
                window.location.reload()
              }}
              className="px-6 py-2 rounded font-semibold transition-all hover:opacity-90"
              style={{
                background: 'linear-gradient(135deg, #F0B90B 0%, #FCD535 100%)',
                color: '#0B0E11',
              }}
            >
              Reload Page
            </button>
          </div>
        </div>
      )
    }

    return this.props.children as ReactElement
  }
}
