// Astro config — produces a fully static site suitable for GitHub Pages.
// The `base` matches the GitHub Pages project URL nyckmaia.github.io/datasus-etl/.

import { defineConfig } from 'astro/config';
import tailwind from '@astrojs/tailwind';

export default defineConfig({
  site: 'https://nyckmaia.github.io',
  base: '/datasus-etl',
  trailingSlash: 'ignore',
  // Emit directly into the repo-root /docs folder. GitHub Pages is
  // configured as "Deploy from a branch" with source = main, folder =
  // /docs, so the built files live in git alongside the source.
  outDir: '../docs',
  integrations: [
    tailwind({
      applyBaseStyles: false, // we load our own base in src/styles/global.css
    }),
  ],
  build: {
    format: 'directory',
  },
  vite: {
    resolve: {
      alias: {
        '~': new URL('./src', import.meta.url).pathname,
      },
    },
  },
});
