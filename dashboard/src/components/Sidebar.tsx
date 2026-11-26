// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'
import { useState } from 'react'
import { useAuth } from '@/lib/auth'
import { useUiStore } from '@/lib/ui-store'
import { useIsMac, shortcutLabel } from '@/hooks/useIsMac'

interface NavItem {
  href: string
  label: string
  desc: string
  icon: (props: { active: boolean }) => JSX.Element
  /** Extra path prefixes that should highlight this item (detail views). */
  match?: string[]
}

const NAV_GROUPS: { label: string; items: NavItem[] }[] = [
  {
    label: 'Monitor',
    items: [
      { href: '/dashboard/', label: 'Overview', desc: 'System status and recent activity', icon: MonitorIcon },
      { href: '/dashboard/runs/', label: 'Runs', desc: 'Every agent run, newest first', icon: RunsIcon, match: ['/dashboard/flow/'] },
      { href: '/dashboard/events/', label: 'Events', desc: 'Search the raw event ledger', icon: EventsIcon },
    ],
  },
  {
    label: 'Compare',
    items: [
      { href: '/dashboard/experiments/', label: 'Forks', desc: 'Compare and promote forked runs', icon: ForkIcon },
    ],
  },
  {
    label: 'Review',
    items: [
      { href: '/dashboard/approvals/', label: 'Approvals', desc: 'Decisions waiting on a person', icon: ApprovalIcon },
    ],
  },
]

const ADVANCED_ITEMS: NavItem[] = [
  { href: '/dashboard/groups/', label: 'Coordination', desc: 'Multi-agent groups and messages', icon: GroupsIcon },
]

// Admin-only: shown in the Advanced section when the signed-in user is an admin.
const TEAM_ITEM: NavItem = {
  href: '/dashboard/team/',
  label: 'Team',
  desc: 'Manage dashboard users and roles',
  icon: TeamIcon,
}

const SITE_URL = process.env.NEXT_PUBLIC_SITE_URL || 'https://actae.dev'

export default function Sidebar({ demoMode = false }: { demoMode?: boolean }) {
  const pathname = usePathname()
  const { user, logout, logoutAll } = useAuth()
  const isMac = useIsMac()
  const [advancedOpen, setAdvancedOpen] = useState(false)

  function isActive(item: NavItem) {
    const prefixes = [item.href, ...(item.match ?? [])]
    return prefixes.some(prefix => {
      if (prefix === '/dashboard/') return pathname === '/dashboard/' || pathname === '/dashboard'
      return pathname === prefix || pathname.startsWith(prefix)
    })
  }

  const basePath = demoMode ? '/showcase' : '/dashboard'
  const withBase = (item: NavItem) => ({ ...item, href: item.href.replace('/dashboard', basePath) })
  const groups = NAV_GROUPS.map(group => ({ ...group, items: group.items.map(withBase) }))
  const advanced = (user?.role === 'admin' ? [...ADVANCED_ITEMS, TEAM_ITEM] : ADVANCED_ITEMS).map(withBase)

  function renderItem(item: NavItem) {
    const active = isActive({ ...item, href: item.href })
    return (
      <Link
        key={item.href}
        href={item.href}
        title={item.desc}
        className={`flex items-center gap-2.5 px-3 py-2 rounded-lg mb-0.5 transition-all duration-150 ${
          active
            ? 'bg-white/10 text-white border border-actae-400/20 shadow-[inset_3px_0_0_#c8ff3d]'
            : 'text-surface-400 hover:text-white hover:bg-white/5 border border-transparent'
        }`}
      >
        <item.icon active={active} />
        <span className="min-w-0 flex-1">
          <span className="block text-[13px] font-medium leading-tight">{item.label}</span>
          <span className="block text-[10px] leading-tight text-surface-500 mt-0.5">{item.desc}</span>
        </span>
      </Link>
    )
  }

  return (
    <aside className="actae-sidebar w-[236px] bg-surface-900 flex flex-col h-screen z-20 relative">
      {/* Brand — links back to the Actae site */}
      <div className="px-5 pt-5 pb-5 border-b border-white/10">
        <a href={SITE_URL} target="_blank" rel="noreferrer" className="flex items-center gap-3" aria-label="Actae home">
          <img src="/android-chrome-192x192.png" alt="" width={36} height={36} className="h-9 w-9 shrink-0 rounded-lg" />
          <div>
            <h1 className="text-[14px] font-black text-white leading-none tracking-[0.12em]">ACTAE</h1>
            <p className="text-[8px] text-surface-500 font-semibold tracking-[0.15em] uppercase mt-1">Execution console</p>
          </div>
        </a>
      </div>

      {/* Navigation */}
      <nav className="flex-1 overflow-y-auto px-3 py-3 space-y-4">
        {groups.map(group => (
          <div key={group.label}>
            <p className="text-[10px] font-semibold text-surface-500 uppercase tracking-widest px-3 mb-1.5">
              {group.label}
            </p>
            {group.items.map(renderItem)}
          </div>
        ))}

        {/* Advanced: demoted, collapsed by default */}
        <div>
          <button
            type="button"
            onClick={() => setAdvancedOpen(open => !open)}
            aria-expanded={advancedOpen}
            className="flex w-full items-center justify-between px-3 mb-1.5 text-[10px] font-semibold text-surface-500 uppercase tracking-widest hover:text-surface-300 transition-colors"
          >
            <span className="sidebar-tool-label">Advanced</span>
            <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" className={`transition-transform ${advancedOpen ? 'rotate-180' : ''}`}>
              <polyline points="6 9 12 15 18 9" />
            </svg>          </button>
          {advancedOpen && advanced.map(renderItem)}
        </div>
      </nav>

      {/* User footer */}
      <div className="px-4 py-4 border-t border-white/10 bg-black/20">
        {demoMode ? (
          <div className="rounded-lg border border-actae-400/20 bg-actae-400/5 px-3 py-2">
            <p className="text-[10px] font-bold uppercase tracking-widest text-actae-400">Demo mode</p>
            <p className="mt-1 text-[11px] leading-4 text-surface-400">Synthetic data · read only</p>
          </div>
        ) : (
        <>
        <div className="mb-3 flex items-center gap-1.5">
          <button
            type="button"
            onClick={() => useUiStore.getState().openCommand()}
            className="flex flex-1 items-center gap-2 rounded-lg border border-white/10 bg-white/5 px-2.5 py-1.5 text-[11px] text-surface-400 hover:text-white hover:bg-white/10 transition-colors"
            title="Search runs and events"
          >
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/></svg>
            <span className="sidebar-tool-label flex-1 text-left">Search</span>
            <kbd className="rounded border border-white/10 px-1 text-[9px] text-surface-500">{shortcutLabel(isMac)}</kbd>
          </button>
          <button
            type="button"
            onClick={() => useUiStore.getState().openHelp()}
            className="flex h-7 w-7 shrink-0 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-surface-400 hover:text-white hover:bg-white/10 transition-colors"
            title="Help and glossary"
            aria-label="Help and glossary"
          >
            <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10"/><path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
          </button>
        </div>
        <div className="flex items-center gap-2.5">
          <div className="w-7 h-7 rounded-full bg-actae-400 flex items-center justify-center text-surface-950 text-[11px] font-black shrink-0">
            {user?.email?.[0]?.toUpperCase() || '?'}
          </div>
          <div className="min-w-0 flex-1">
            <p className="text-[12px] text-surface-300 font-medium truncate leading-tight">{user?.email}</p>
          </div>
          <button
            onClick={logoutAll}
            className="shrink-0 text-surface-500 hover:text-white transition-colors p-1"
            title="Sign out everywhere"
            aria-label="Sign out everywhere"
          >
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M12 2a7 7 0 0 0-7 7c0 3 2 4.5 2 7h10c0-2.5 2-4 2-7a7 7 0 0 0-7-7z"/>
              <line x1="9" y1="21" x2="15" y2="21"/>
            </svg>
          </button>
          <button
            onClick={logout}
            className="shrink-0 text-surface-500 hover:text-white transition-colors p-1 -mr-1"
            title="Logout"
          >
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/>
              <polyline points="16 17 21 12 16 7"/>
              <line x1="21" y1="12" x2="9" y2="12"/>
            </svg>
          </button>
        </div>
        </>
        )}
      </div>
    </aside>
  )
}

/*  SVG Icons (inline to avoid icon library dependency)  */

function RunsIcon({ active }: { active: boolean }) {
  const c = active ? '#c8ff3d' : '#697069'
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke={c} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <line x1="8" y1="6" x2="21" y2="6"/><line x1="8" y1="12" x2="21" y2="12"/><line x1="8" y1="18" x2="21" y2="18"/>
      <circle cx="3.5" cy="6" r="1.2" fill={c} stroke="none"/><circle cx="3.5" cy="12" r="1.2" fill={c} stroke="none"/><circle cx="3.5" cy="18" r="1.2" fill={c} stroke="none"/>
    </svg>
  )
}

function ForkIcon({ active }: { active: boolean }) {
  const c = active ? '#c8ff3d' : '#697069'
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke={c} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <line x1="6" y1="3" x2="6" y2="15"/>
      <circle cx="18" cy="6" r="3"/><circle cx="6" cy="18" r="3"/>
      <path d="M6 9a6 6 0 0 0 6 6h3"/>
    </svg>
  )
}

function MonitorIcon({ active }: { active: boolean }) {
  const c = active ? '#c8ff3d' : '#697069'
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke={c} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <rect x="2" y="3" width="20" height="14" rx="2" ry="2"/>
      <line x1="8" y1="21" x2="16" y2="21"/>
      <line x1="12" y1="17" x2="12" y2="21"/>
    </svg>
  )
}

function EventsIcon({ active }: { active: boolean }) {
  const c = active ? '#c8ff3d' : '#697069'
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke={c} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/>
      <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/>
    </svg>
  )
}

function GroupsIcon({ active }: { active: boolean }) {
  const c = active ? '#c8ff3d' : '#697069'
  return <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke={c} strokeWidth="2"><circle cx="7" cy="7" r="3"/><circle cx="17" cy="7" r="3"/><path d="M2 21v-2a5 5 0 0 1 10 0v2M12 19a5 5 0 0 1 10 0v2"/></svg>
}

function ApprovalIcon({ active }: { active: boolean }) {
  const c = active ? '#c8ff3d' : '#697069'
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke={c} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M9 11l3 3L22 4"/>
      <path d="M21 12v7a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11"/>
    </svg>
  )
}

function TeamIcon({ active }: { active: boolean }) {
  const c = active ? '#c8ff3d' : '#697069'
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke={c} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="8" r="3.5"/>
      <path d="M5 21v-1a7 7 0 0 1 14 0v1"/>
    </svg>
  )
}
