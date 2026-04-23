/** @type {import('tailwindcss').Config} */
export default {
  content: ['./src/**/*.{astro,html,js,jsx,md,mdx,ts,tsx}'],
  // The theme switch is implemented by flipping a class on <html> (theme-light
  // / theme-dark). All color tokens are CSS variables that change with the
  // class, so Tailwind color utilities that reference them follow the theme
  // without needing dark: variants.
  darkMode: ['class', '.theme-dark'],
  theme: {
    extend: {
      colors: {
        bg: 'rgb(var(--bg) / <alpha-value>)',
        surface: 'rgb(var(--surface) / <alpha-value>)',
        'surface-2': 'rgb(var(--surface-2) / <alpha-value>)',
        border: 'rgb(var(--border) / <alpha-value>)',
        text: 'rgb(var(--text) / <alpha-value>)',
        muted: 'rgb(var(--muted) / <alpha-value>)',
        brand: {
          DEFAULT: 'rgb(var(--brand) / <alpha-value>)',
          hover: 'rgb(var(--brand-hover) / <alpha-value>)',
          dim: 'rgb(var(--brand-dim) / <alpha-value>)',
        },
        warn: 'rgb(var(--warn) / <alpha-value>)',
        danger: 'rgb(var(--danger) / <alpha-value>)',
      },
      fontFamily: {
        sans: ['"IBM Plex Sans"', 'system-ui', 'sans-serif'],
        mono: ['"JetBrains Mono"', 'ui-monospace', 'monospace'],
        'sans-condensed': ['"IBM Plex Sans Condensed"', '"IBM Plex Sans"', 'system-ui', 'sans-serif'],
      },
      fontSize: {
        'display-xl': ['clamp(2.75rem, 6vw, 5rem)', { lineHeight: '1.02', letterSpacing: '-0.02em' }],
        'display-lg': ['clamp(2rem, 4vw, 3.25rem)', { lineHeight: '1.08', letterSpacing: '-0.018em' }],
        'display': ['clamp(1.4rem, 2.4vw, 1.9rem)', { lineHeight: '1.18', letterSpacing: '-0.012em' }],
        'lead': ['1.125rem', { lineHeight: '1.55' }],
      },
      letterSpacing: {
        stamp: '0.12em',
        data: '0.08em',
      },
      maxWidth: {
        'prose-wide': '72ch',
        'prose-tech': '68ch',
      },
      boxShadow: {
        card: '0 1px 0 rgb(var(--border) / 1)',
        elevated:
          '0 1px 0 rgb(var(--border) / 1), 0 8px 24px -12px rgb(var(--bg) / 0.45)',
      },
    },
  },
  plugins: [],
};
