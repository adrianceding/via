import assert from 'node:assert/strict';
import test from 'node:test';

import {
  LOCALE_STORAGE_KEY,
  normalizeLocale,
  persistLocale,
  resolveInitialLocale,
} from '../src/i18n.js';
import en from '../src/locales/en.js';
import zhCN from '../src/locales/zh-CN.js';

function messageKeys(value, prefix = '') {
  return Object.entries(value).flatMap(([key, child]) => {
    const path = prefix ? `${prefix}.${key}` : key;
    return child && typeof child === 'object' ? messageKeys(child, path) : [path];
  }).sort();
}

test('normalizeLocale accepts Chinese and English language tags', () => {
  assert.equal(normalizeLocale('zh-CN'), 'zh-CN');
  assert.equal(normalizeLocale('zh-HK'), 'zh-CN');
  assert.equal(normalizeLocale('en-US'), 'en');
  assert.equal(normalizeLocale('fr'), null);
  assert.equal(normalizeLocale(''), null);
});

test('resolveInitialLocale prefers stored choice then browser language', () => {
  const storage = { getItem: () => 'en' };
  assert.equal(resolveInitialLocale({ storage, languages: ['zh-CN'] }), 'en');
  assert.equal(resolveInitialLocale({ storage: null, languages: ['zh-HK', 'en'] }), 'zh-CN');
  assert.equal(resolveInitialLocale({ storage: null, languages: ['fr-FR'] }), 'en');
  assert.equal(resolveInitialLocale({ storage: null, languages: [] }), 'en');
});

test('locale storage failures do not prevent startup or switching', () => {
  const brokenStorage = {
    getItem: () => { throw new Error('blocked'); },
    setItem: () => { throw new Error('blocked'); },
  };
  assert.equal(resolveInitialLocale({ storage: brokenStorage, languages: ['zh-CN'] }), 'zh-CN');
  assert.equal(persistLocale('en', brokenStorage), false);

  const writes = [];
  assert.equal(persistLocale('zh-CN', { setItem: (...args) => writes.push(args) }), true);
  assert.deepEqual(writes, [[LOCALE_STORAGE_KEY, 'zh-CN']]);
  assert.equal(persistLocale('fr', { setItem: () => assert.fail('unexpected write') }), false);
});

test('Chinese and English message catalogs have matching keys', () => {
  assert.deepEqual(messageKeys(zhCN), messageKeys(en));
  for (const key of [
    'app.title',
    'locale.label',
    'header.refresh',
    'header.pause',
    'status.connecting',
    'status.healthy',
    'errors.authentication',
  ]) {
    assert.ok(messageKeys(en).includes(key), `missing core message: ${key}`);
  }
});

test('throughput chart labels separate current and last measured capacity by direction', () => {
  assert.match(zhCN.charts.uplinkTitle, /上行/);
  assert.match(zhCN.charts.downlinkTitle, /下行/);
  assert.match(zhCN.charts.lastCapacityReferenceTitle, /过期.*参考/);
  assert.match(en.charts.uplinkTitle, /uplink/i);
  assert.match(en.charts.downlinkTitle, /downlink/i);
  assert.match(en.charts.lastCapacityReferenceTitle, /expired reference/i);
});