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
    'status.transportUnavailable',
    'health.noSessions',
    'health.noReadySessions',
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

test('directional capacity and allocation copy keeps downlink semantics separate', () => {
  const zhDownlinkAggregation = Object.values(zhCN.charts.aggregationDirections.downlink).join(' ');
  const enDownlinkAggregation = Object.values(en.charts.aggregationDirections.downlink).join(' ');
  assert.match(zhCN.charts.aggregationDirections.uplink.highestCapacityNote, /上行 DATA/);
  assert.match(zhCN.charts.aggregationDirections.downlink.highestCapacityNote, /下行 DATA/);
  assert.match(zhCN.charts.aggregationDirections.uplink.lift, /上行/);
  assert.match(zhCN.charts.aggregationDirections.downlink.lift, /下行/);
  assert.doesNotMatch(zhDownlinkAggregation, /上行 DATA|发送容量提升|发送单路/);
  assert.match(zhCN.charts.downlinkShareSummary, /下行 DATA/);
  assert.match(zhCN.charts.downlinkShareEmpty, /下行分配/);
  assert.match(en.charts.aggregationDirections.downlink.highestCapacityNote, /downlink DATA/i);
  assert.match(en.charts.aggregationDirections.downlink.lift, /downlink/i);
  assert.doesNotMatch(enDownlinkAggregation, /uplink DATA|send capacity lift|send link/i);
  assert.match(en.charts.downlinkShareSummary, /downlink DATA/i);
});

test('transport-unavailable status has explicit bilingual presentation copy', () => {
  assert.equal(zhCN.status.transportUnavailable, '线路不可用');
  assert.equal(en.status.transportUnavailable, 'Transport unavailable');
  assert.match(zhCN.health.noSessions, /传输线路/);
  assert.match(zhCN.health.noReadySessions, /传输会话.*尚未就绪/);
});
