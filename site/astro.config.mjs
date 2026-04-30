// Astro config — produces a fully static site suitable for GitHub Pages.
// Served from the custom domain datasus-etl.com.br at the root path, so no
// `base` prefix is needed (and adding one would 404 every CSS/JS asset).

import { defineConfig } from 'astro/config';
import tailwind from '@astrojs/tailwind';

export default defineConfig({
  site: 'https://datasus-etl.com.br',
  trailingSlash: 'ignore',
  // Emit into the repo-root /docs folder. The directory is gitignored —
  // the site is rebuilt on every push that touches site/** or VERSION,
  // then deployed via actions/upload-pages-artifact + deploy-pages.
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
