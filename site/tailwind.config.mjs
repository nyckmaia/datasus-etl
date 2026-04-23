/** @type {import('tailwindcss').Config} */
export default {
  content: ['./src/**/*.{astro,html,js,jsx,md,mdx,ts,tsx}'],
  theme: {
    extend: {
      colors: {
        // Editorial paper + deep navy — evokes a scientific journal crossed
        // with Brazilian civic design. No gradient blobs here.
        paper: {
          DEFAULT: '#f3ece0', // warm cream
          50: '#faf6ef',
          100: '#f3ece0',
          200: '#e9dfca',
          300: '#d9cbad',
        },
        ink: {
          DEFAULT: '#0b1e33',
          800: '#13283f',
          600: '#2a3f57',
          400: '#5b6e82',
          200: '#a0adbc',
        },
        clinic: {
          red: '#c4364d',
          sage: '#5a7a64',
          amber: '#d19a3b',
        },
      },
      fontFamily: {
        display: ['Fraunces', 'Georgia', 'serif'],
        sans: ['"Public Sans"', 'system-ui', 'sans-serif'],
        mono: ['"JetBrains Mono"', 'ui-monospace', 'monospace'],
      },
      fontSize: {
        'display-xl': ['clamp(3.5rem, 7vw, 7rem)', { lineHeight: '0.95', letterSpacing: '-0.035em' }],
        'display-lg': ['clamp(2.5rem, 5vw, 4.5rem)', { lineHeight: '1.02', letterSpacing: '-0.03em' }],
        'display': ['clamp(1.75rem, 3vw, 2.75rem)', { lineHeight: '1.1', letterSpacing: '-0.02em' }],
        'lead': ['1.2rem', { lineHeight: '1.55' }],
      },
      letterSpacing: {
        stamp: '0.22em', // for small-caps labels
      },
      maxWidth: {
        'prose-wide': '72ch',
      },
      backgroundImage: {
        // Subtle paper grain — rendered as a data URI so there's no extra
        // network request at first paint.
        grain:
          "url(\"data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='220' height='220'><filter id='n'><feTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='2' stitchTiles='stitch'/><feColorMatrix values='0 0 0 0 0.043 0 0 0 0 0.118 0 0 0 0 0.2 0 0 0 0.08 0'/></filter><rect width='100%' height='100%' filter='url(%23n)'/></svg>\")",
      },
    },
  },
  plugins: [],
};
