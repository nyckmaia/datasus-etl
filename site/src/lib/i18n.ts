// Locale helpers used by layouts and components. The runtime locale switch
// (first-visit auto-detect + persisted choice) lives in the LanguageToggle
// client script — this module is compile-time only.

export type Locale = 'en' | 'pt';

export interface PagePath {
  en: string;   // path without locale prefix, rooted at site base
  pt?: string;  // Present only for bilingual pages.
}

export const NAV: { key: string; path: PagePath; labels: Record<Locale, string> }[] = [
  {
    key: 'download',
    path: { en: '/download', pt: '/pt/download' },
    labels: { en: 'Download', pt: 'Baixar' },
  },
  {
    key: 'tutorial',
    path: { en: '/tutorial', pt: '/pt/tutorial' },
    labels: { en: 'Tutorial', pt: 'Tutorial' },
  },
  {
    key: 'docs',
    path: { en: '/docs' },
    labels: { en: 'Docs', pt: 'Docs (EN)' },
  },
  {
    key: 'changelog',
    path: { en: '/changelog' },
    labels: { en: 'Changelog', pt: 'Changelog (EN)' },
  },
];

export function localePath(locale: Locale, path: string, base: string): string {
  const clean = path.replace(/^\/+/, '/');
  if (locale === 'en') {
    // EN is the default locale; URLs are unprefixed.
    return `${base}${clean}`.replace(/\/+/g, '/');
  }
  // PT lives under /pt/ — if the path is already under /pt/, don't double it.
  const withPt = clean.startsWith('/pt/') ? clean : `/pt${clean}`;
  return `${base}${withPt}`.replace(/\/+/g, '/');
}
