import assert from 'node:assert/strict';
import test from 'node:test';

import { formatBytes, formatCounter, formatMicros, formatPercent, formatRate, formatTimestamp } from '../src/format.js';

test('formatBytes uses bounded binary units', () => {
  assert.equal(formatBytes(0), '0 B');
  assert.equal(formatBytes(1024), '1.00 KiB');
  assert.equal(formatBytes(10 * 1024), '10.0 KiB');
  assert.equal(formatBytes(1024 ** 5), '1024 TiB');
});

test('formatCounter distinguishes unavailable counters from a real zero', () => {
  assert.equal(formatCounter(0), '0 B');
  assert.equal(formatCounter(undefined), '--');
  assert.equal(formatCounter(null), '--');
  assert.equal(formatCounter(Number.NaN), '--');
  assert.equal(formatCounter(-1), '--');
});

test('formatMicros selects readable units', () => {
  assert.equal(formatMicros(0), '--');
  assert.equal(formatMicros(999), '999 us');
  assert.equal(formatMicros(1000), '1.00 ms');
  assert.equal(formatMicros(1_000_000), '1.00 s');
});

test('formatRate and formatPercent reject unavailable or invalid values', () => {
  assert.equal(formatRate(null), '--');
  assert.equal(formatRate(2048), '2.00 KiB/s');
  assert.equal(formatPercent(5, 0), '--');
  assert.equal(formatPercent(5, 100), '5.00%');
  assert.equal(formatPercent(200, 100), '200%');
});

test('formatTimestamp rejects missing and invalid values', () => {
  assert.equal(formatTimestamp(''), '--');
  assert.equal(formatTimestamp('invalid'), '--');
  assert.equal(formatTimestamp('2026-08-01T10:00:00Z', 'en-US'), new Date('2026-08-01T10:00:00Z').toLocaleString('en-US', { hour12: false }));
  assert.equal(formatTimestamp('2026-08-01T10:00:00Z', 'zh-CN'), new Date('2026-08-01T10:00:00Z').toLocaleString('zh-CN', { hour12: false }));
});
