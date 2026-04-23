// Single source of truth for download URLs. The CI exports SITE_VERSION
// when building for a release deploy; for local `astro dev` we fall back to
// reading the repo-root VERSION file. SITE_REPO lets the workflow override
// the repo slug (useful for fork previews) without touching code.

import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(HERE, '..', '..', '..');

export type PlatformId = 'windows-x64' | 'macos-arm64' | 'linux-x64';

export interface PlatformAsset {
  id: PlatformId;
  label: string;
  extension: 'exe' | 'dmg' | 'AppImage';
  architecture: string;
  note: string;
}

function readVersion(): string {
  const envVersion = process.env.SITE_VERSION?.trim();
  if (envVersion) return envVersion;
  try {
    return readFileSync(resolve(ROOT, 'VERSION'), 'utf8').trim();
  } catch {
    return '0.0.0';
  }
}

export const VERSION = readVersion();
export const REPO = process.env.SITE_REPO ?? 'nyckmaia/datasus-etl';

export const PLATFORMS: PlatformAsset[] = [
  {
    id: 'windows-x64',
    label: 'Windows',
    extension: 'exe',
    architecture: '64-bit · 10 / 11',
    note: 'Inno Setup · creates a Start Menu shortcut',
  },
  {
    id: 'macos-arm64',
    label: 'macOS (Apple Silicon)',
    extension: 'dmg',
    architecture: 'arm64 · 11+',
    note: 'Drag-to-Applications · right-click “Open” the first time',
  },
  {
    id: 'linux-x64',
    label: 'Linux',
    extension: 'AppImage',
    architecture: 'x86_64 · glibc 2.35+',
    note: 'chmod +x then double-click · no root required',
  },
];

export function downloadUrl(platform: PlatformAsset, version = VERSION): string {
  const filename = `datasus-etl-${version}-${platform.id}.${platform.extension}`;
  return `https://github.com/${REPO}/releases/download/v${version}/${filename}`;
}

export function releaseUrl(version = VERSION): string {
  return `https://github.com/${REPO}/releases/tag/v${version}`;
}

export function releasesIndexUrl(): string {
  return `https://github.com/${REPO}/releases`;
}

export function repoUrl(): string {
  return `https://github.com/${REPO}`;
}

export function issuesUrl(): string {
  return `https://github.com/${REPO}/issues`;
}
