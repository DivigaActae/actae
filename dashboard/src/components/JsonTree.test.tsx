// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import JsonTree from './JsonTree'

describe('JsonTree', () => {
  it('renders null', () => {
    render(<JsonTree data={null} />)
    expect(screen.getByText('null')).toHaveClass('text-gray-500')
  })

  it('renders undefined as null', () => {
    render(<JsonTree data={undefined} />)
    expect(screen.getByText('null')).toBeInTheDocument()
  })

  it('renders strings quoted', () => {
    render(<JsonTree data="hello" />)
    expect(screen.getByText('"hello"')).toHaveClass('text-green-400')
  })

  it('renders numbers unquoted', () => {
    render(<JsonTree data={42} />)
    expect(screen.getByText('42')).toHaveClass('text-blue-400')
  })

  it('renders booleans', () => {
    render(<JsonTree data={false} />)
    expect(screen.getByText('false')).toHaveClass('text-purple-400')
  })

  it('renders empty arrays as []', () => {
    render(<JsonTree data={[]} />)
    expect(screen.getByText('[]')).toBeInTheDocument()
  })

  it('renders arrays with elements and commas', () => {
    render(<JsonTree data={[1, 2]} />)
    expect(screen.getByText('[')).toBeInTheDocument()
    expect(screen.getByText(']')).toBeInTheDocument()
    expect(screen.getAllByText(',')).toHaveLength(1)
  })

  it('renders empty objects as {}', () => {
    render(<JsonTree data={{}} />)
    expect(screen.getByText('{}')).toBeInTheDocument()
  })

  it('renders object keys in red with quoted keys', () => {
    render(<JsonTree data={{ a: 1, b: 'x' }} />)
    expect(screen.getByText('"a":')).toHaveClass('text-red-400')
    expect(screen.getByText('"b":')).toBeInTheDocument()
    expect(screen.getAllByText(',')).toHaveLength(1)
  })

  it('renders nested structures recursively', () => {
    render(<JsonTree data={{ agent: { model: 'gpt-4', flags: [true, null] } }} />)
    expect(screen.getByText('"model":')).toBeInTheDocument()
    expect(screen.getByText('"gpt-4"')).toBeInTheDocument()
    expect(screen.getByText('true')).toBeInTheDocument()
    expect(screen.getByText('null')).toBeInTheDocument()
  })

  it('falls back to String() for exotic types', () => {
    render(<JsonTree data={Symbol('s') as any} />)
    expect(screen.getByText('Symbol(s)')).toBeInTheDocument()
  })
})
