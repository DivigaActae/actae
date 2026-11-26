// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

import { create } from 'zustand'

interface UiState {
  commandOpen: boolean
  helpOpen: boolean
  openCommand: () => void
  closeCommand: () => void
  setCommandOpen: (open: boolean) => void
  openHelp: () => void
  closeHelp: () => void
  setHelpOpen: (open: boolean) => void
}

/** Small shell-level UI store for the command palette and help dialog.
 * Kept separate from the flow store so the shell can drive overlays without
 * pulling in the whole workspace state. */
export const useUiStore = create<UiState>((set) => ({
  commandOpen: false,
  helpOpen: false,
  openCommand: () => set({ commandOpen: true, helpOpen: false }),
  closeCommand: () => set({ commandOpen: false }),
  setCommandOpen: (open) => set({ commandOpen: open }),
  openHelp: () => set({ helpOpen: true, commandOpen: false }),
  closeHelp: () => set({ helpOpen: false }),
  setHelpOpen: (open) => set({ helpOpen: open }),
}))
