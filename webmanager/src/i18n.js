import { createI18n } from 'vue-i18n';

import en from './locales/en.js';
import zhCN from './locales/zh-CN.js';

export const LOCALE_STORAGE_KEY = 'via.manager.locale';

export function normalizeLocale(value) {
  const locale = String(value || '').toLowerCase();
  if (locale === 'zh' || locale.startsWith('zh-')) return 'zh-CN';
  if (locale === 'en' || locale.startsWith('en-')) return 'en';
  return null;
}

export function resolveInitialLocale({ storage, languages = [] } = {}) {
  try {
    const stored = normalizeLocale(storage?.getItem(LOCALE_STORAGE_KEY));
    if (stored) return stored;
  } catch {
    // Browser privacy settings may block storage access.
  }
  for (const language of languages) {
    const locale = normalizeLocale(language);
    if (locale) return locale;
  }
  return 'en';
}

export function persistLocale(locale, storage) {
  const normalized = normalizeLocale(locale);
  if (!normalized) return false;
  try {
    storage?.setItem(LOCALE_STORAGE_KEY, normalized);
    return Boolean(storage);
  } catch {
    return false;
  }
}

export function createManagerI18n(locale) {
  return createI18n({
    legacy: false,
    locale: normalizeLocale(locale) || 'en',
    fallbackLocale: 'en',
    messages: { 'zh-CN': zhCN, en },
  });
}