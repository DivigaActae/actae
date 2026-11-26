// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import React from 'react'

export class ErrorBoundary extends React.Component<
  { children: React.ReactNode },
  { hasError: boolean; error: Error | null }
> {
  constructor(props: { children: React.ReactNode }) {
    super(props)
    this.state = { hasError: false, error: null }
  }

  static getDerivedStateFromError(error: Error) {
    return { hasError: true, error }
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="flex items-center justify-center min-h-screen bg-gray-950">
          <div className="bg-gray-900 border border-gray-800 p-8 rounded-lg max-w-md w-full">
            <h2 className="text-xl font-bold text-red-400 mb-4">Something went wrong</h2>
            <p className="text-gray-300 mb-4">
              {this.state.error?.message || 'An unexpected error occurred'}
            </p>
            <button
              onClick={() => window.location.reload()}
              className="w-full px-4 py-2 bg-actae-500 text-white rounded hover:bg-actae-600"
            >
              Reload Page
            </button>
          </div>
        </div>
      )
    }
    return this.props.children
  }
}
