import assert from 'node:assert/strict';
import test from 'node:test';

import { createSessionTrendStore } from '../src/trends.js';

function session(id, written) {
  return {
    connection_id: id,
    interface: id,
    local_endpoint: '192.0.2.10:1000',
    remote_endpoint: '198.51.100.20:2000',
    quality: { written_data_payload_bytes: written },
  };
}

test('trend store samples received DATA separately for downlink', () => {
  const store = createSessionTrendStore();
  store.update([
    { ...session('a', 100), quality: { written_data_payload_bytes: 100, received_data_payload_bytes: 200 } },
  ], '2026-08-01T10:00:00Z');
  store.update([
    { ...session('a', 150), quality: { written_data_payload_bytes: 150, received_data_payload_bytes: 260 } },
  ], '2026-08-01T10:00:01Z');

  const series = store.snapshot()[0];
  assert.equal(series.samples[0], 50);
  assert.equal(series.rxSamples[0], 60);
});

test('trend store ignores missing or invalid received DATA counters', () => {
  const store = createSessionTrendStore();
  store.update([
    { ...session('a', 100), quality: { written_data_payload_bytes: 100, received_data_payload_bytes: 200 } },
    { ...session('b', 100), quality: { written_data_payload_bytes: 100, received_data_payload_bytes: Number.NaN } },
  ], '2026-08-01T10:00:00Z');
  store.update([
    { ...session('a', 120), quality: { written_data_payload_bytes: 120, received_data_payload_bytes: 220 } },
    { ...session('b', 120), quality: { written_data_payload_bytes: 120, received_data_payload_bytes: Number.NaN } },
  ], '2026-08-01T10:00:01Z');

  const byID = Object.fromEntries(store.snapshot().map((series) => [series.id, series]));
  assert.deepEqual(byID.a.rxSamples, [20]);
  assert.deepEqual(byID.b.rxSamples, []);
});

test('trend store bounds series and samples', () => {
  const store = createSessionTrendStore();
  for (let sampleIndex = 0; sampleIndex < 140; sampleIndex += 1) {
    const sessions = Array.from({ length: 20 }, (_, index) => session(`connection-${index}`, (sampleIndex + 1) * 1000));
    store.update(sessions, new Date((sampleIndex + 1) * 1000).toISOString());
  }

  const series = store.snapshot();
  assert.equal(series.length, 12);
  assert.equal(series[0].samples.length, 120);
  assert.equal(series[0].samples[0], 1000);
  assert.equal(series[0].localEndpoint, '192.0.2.10:1000');
  assert.equal(series[0].remoteEndpoint, '198.51.100.20:2000');
});

test('trend aggregate includes sessions beyond the visible series limit', () => {
  const store = createSessionTrendStore();
  const initial = Array.from({ length: 13 }, (_, index) => session(`connection-${index}`, 100));
  const updated = initial.map((item, index) => session(item.connection_id, index === 12 ? 200 : 101));
  store.update(initial, '2026-08-01T10:00:00Z');
  store.update(updated, '2026-08-01T10:00:01Z');

  assert.equal(store.snapshot().length, 12);
  assert.deepEqual(store.aggregateSnapshot()?.samples, [112]);
});

test('trend store removes disconnected series and admits replacements', () => {
  const store = createSessionTrendStore();
  store.update([session('a', 100), session('b', 100)], '2026-08-01T10:00:00Z');
  store.update([session('b', 121), session('c', 100)], '2026-08-01T10:00:01Z');

  assert.deepEqual(store.snapshot().map((series) => series.id), ['b']);
  assert.deepEqual(store.snapshot()[0].samples, [21]);
});

test('trend store ignores missing and invalid DATA counters', () => {
  const store = createSessionTrendStore();
  store.update([
    { connection_id: 'a', quality: {} },
    { connection_id: 'b', quality: { written_data_payload_bytes: Number.NaN } },
    { quality: { written_data_payload_bytes: 10 } },
  ], '2026-08-01T10:00:00Z');
  assert.deepEqual(store.snapshot(), []);
});