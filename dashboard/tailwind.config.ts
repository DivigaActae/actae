// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

import type { Config } from 'tailwindcss'

const config: Config = {
  darkMode: 'class',
  content: [
    './src/pages/**/*.{js,ts,jsx,tsx,mdx}',
    './src/components/**/*.{js,ts,jsx,tsx,mdx}',
    './src/app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      fontFamily: {
        sans: ['var(--font-inter)', 'system-ui', 'sans-serif'],
        mono: ['JetBrains Mono', 'Fira Code', 'monospace'],
      },
      colors: {
        gray: {
          50: '#f7f7f2', 100: '#efefe8', 200: '#dadcd4', 300: '#bcc1b9',
          400: '#929990', 500: '#697069', 600: '#4b514b', 700: '#303530',
          800: '#1b1f1c', 900: '#0d100f', 950: '#090b0a',
        },
        actae: {
          50: '#f9ffe9',
          100: '#efffc2',
          200: '#e2ff87',
          300: '#d4ff5c',
          400: '#c8ff3d',
          500: '#b5ec28',
          600: '#91c20f',
          700: '#6f9411',
          800: '#587416',
          900: '#485f17',
          950: '#243506',
        },
        surface: {
          50: '#f8fafc',
          100: '#f1f5f9',
          200: '#e2e8f0',
          300: '#cbd5e1',
          400: '#929990',
          500: '#697069',
          600: '#4b514b',
          700: '#303530',
          800: '#1b1f1c',
          900: '#0d100f',
          950: '#090b0a',
        },
      },
      boxShadow: {
        'card': '0 1px 3px 0 rgba(0, 0, 0, 0.04), 0 1px 2px -1px rgba(0, 0, 0, 0.06)',
        'card-hover': '0 4px 6px -1px rgba(0, 0, 0, 0.06), 0 2px 4px -2px rgba(0, 0, 0, 0.05)',
        'card-lg': '0 10px 15px -3px rgba(0, 0, 0, 0.06), 0 4px 6px -4px rgba(0, 0, 0, 0.04)',
        'modal': '0 20px 60px -12px rgba(0, 0, 0, 0.18)',
        'sidebar': '2px 0 10px 0 rgba(0, 0, 0, 0.08)',
      },
      borderRadius: {
        'xl': '0.875rem',
        '2xl': '1rem',
      },
      keyframes: {
        'fade-in': {
          '0%': { opacity: '0', transform: 'translateY(4px)' },
          '100%': { opacity: '1', transform: 'translateY(0)' },
        },
        'fade-in-up': {
          '0%': { opacity: '0', transform: 'translateY(12px)' },
          '100%': { opacity: '1', transform: 'translateY(0)' },
        },
        'slide-in-right': {
          '0%': { opacity: '0', transform: 'translateX(16px)' },
          '100%': { opacity: '1', transform: 'translateX(0)' },
        },
        'scale-in': {
          '0%': { opacity: '0', transform: 'scale(0.95)' },
          '100%': { opacity: '1', transform: 'scale(1)' },
        },
        'pulse-soft': {
          '0%, 100%': { opacity: '1' },
          '50%': { opacity: '0.5' },
        },
        'shimmer': {
          '0%': { backgroundPosition: '-200% 0' },
          '100%': { backgroundPosition: '200% 0' },
        },
      },
      animation: {
        'fade-in': 'fade-in 0.2s ease-out',
        'fade-in-up': 'fade-in-up 0.3s ease-out',
        'slide-in-right': 'slide-in-right 0.2s ease-out',
        'scale-in': 'scale-in 0.2s ease-out',
        'pulse-soft': 'pulse-soft 2s ease-in-out infinite',
        'shimmer': 'shimmer 1.5s ease-in-out infinite',
      },
      transitionTimingFunction: {
        'spring': 'cubic-bezier(0.34, 1.56, 0.64, 1)',
      },
    },
  },
  plugins: [],
}
export default config
