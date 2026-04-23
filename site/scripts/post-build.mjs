// Trim Astro's unused content-collection stubs from the committed build.
// They're empty Maps Astro emits at the outDir root even when no content
// collections are defined. Safe to remove; regenerated on each build.

import { rmSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const OUT_DIR = resolve(HERE, '..', '..', 'docs');

for (const name of ['content-assets.mjs', 'content-modules.mjs']) {
  try {
    rmSync(resolve(OUT_DIR, name), { force: true });
  } catch {
    // already absent — fine
  }
}
