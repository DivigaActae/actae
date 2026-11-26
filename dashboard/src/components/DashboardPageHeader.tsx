// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import type { ReactNode } from 'react'

export default function DashboardPageHeader({
  eyebrow,
  title,
  description,
  actions,
  meta,
}: {
  eyebrow: string
  title: string
  description: string
  actions?: ReactNode
  meta?: ReactNode
}) {
  return (
    <header className="console-page-header">
      <div className="console-page-header__copy">
        <div className="console-eyebrow"><span />{eyebrow}</div>
        <h1>{title}</h1>
        <p>{description}</p>
      </div>
      {(actions || meta) && (
        <div className="console-page-header__aside">
          {meta && <div className="console-page-header__meta">{meta}</div>}
          {actions && <div className="console-page-header__actions">{actions}</div>}
        </div>
      )}
    </header>
  )
}
