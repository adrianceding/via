import assert from 'node:assert/strict';
import test from 'node:test';

import { createSessionTrendStore, directionalTrend } from '../src/trends.js';

function session(id, written) {
  return {
    connection_id: id,
    interface: id,
    local_endpoint: '192.0.2.10:1000',
    remote_endpoint: '198.51.100.20:2000',
    quality: { written_data_payload_bytes: written },
  };
}

function lane(id, interfaceName, written, received = 0) {
  return {
    ...session(id, written),
    interface: interfaceName,
    state: 3,
    quality: {
      written_data_payload_bytes: written,
      received_data_payload_bytes: received,
      capacity_bytes_sec: 100,
      data_sample_fresh: true,
    },
  };
}

test('directional trend maps local and peer sends by runtime role', () => {
  const trend = {
    samples: [10], rxSamples: [20], capacity: 100, lastCapacity: 90,
    peerCapacity: 200, peerLastCapacity: 180,
  };
  assert.deepEqual(directionalTrend(trend, 1), {
    uplink: { samples: [10], capacity: 100, lastCapacity: 90 },
    downlink: { samples: [20], capacity: 200, lastCapacity: 180 },
  });
  assert.deepEqual(directionalTrend(trend, 2), {
    uplink: { samples: [20], capacity: 200, lastCapacity: 180 },
    downlink: { samples: [10], capacity: 100, lastCapacity: 90 },
  });
});

test('trend store samples received DATA separately for downlink', () => {
  const store = createSessionTrendStore();
  store.update([
    { ...session('a', 100), quality: { written_data_payload_bytes: 100, received_data_payload_bytes: 200 } },
  ], '2026-08-01T10:00:00Z');
  store.update([
    { ...session('a', 150), quality: { written_data_payload_bytes: 150, received_data_payload_bytes: 260 } },
  ], '2026-08-01T10:00:01Z');

  const series = store.snapshot()[0];
  assert.equal(series.id, 'interface:a');
  assert.equal(series.samples[0], 50);
  assert.equal(series.rxSamples[0], 60);
});

test('trend store aggregates only complete fresh peer send capacity', () => {
  const store = createSessionTrendStore();
  const peerQuality = {
    peer_send_capacity_bytes_sec: 400,
    peer_send_data_sample_at: '2026-08-01T10:00:00Z',
    peer_send_data_sample_expires_at: '2026-08-01T10:00:03Z',
  };
  store.update([
    { ...lane('a-1', 'eth0', 90), quality: { ...lane('a-1', 'eth0', 90).quality, ...peerQuality } },
    { ...lane('a-2', 'eth0', 90), quality: { ...lane('a-2', 'eth0', 90).quality, ...peerQuality } },
  ], '2026-08-01T10:00:01Z');
  store.update([
    { ...lane('a-1', 'eth0', 100), quality: { ...lane('a-1', 'eth0', 100).quality, ...peerQuality } },
    { ...lane('a-2', 'eth0', 100), quality: { ...lane('a-2', 'eth0', 100).quality, ...peerQuality } },
  ], '2026-08-01T10:00:02Z');
  assert.equal(store.snapshot()[0].peerCapacity, 800);
  assert.equal(store.snapshot()[0].peerLastCapacity, 800);

  store.update([
    { ...lane('a-1', 'eth0', 100), quality: { ...lane('a-1', 'eth0', 100).quality, ...peerQuality } },
    lane('a-2', 'eth0', 100),
  ], '2026-08-01T10:00:02.500Z');
  assert.equal(store.snapshot()[0].peerCapacity, 0);
  assert.equal(store.snapshot()[0].peerLastCapacity, 0);

  store.update([
    { ...lane('a-1', 'eth0', 100), quality: { ...lane('a-1', 'eth0', 100).quality, ...peerQuality } },
    { ...lane('a-2', 'eth0', 100), quality: { ...lane('a-2', 'eth0', 100).quality, ...peerQuality } },
  ], '2026-08-01T10:00:03.001Z');
  assert.equal(store.snapshot()[0].peerCapacity, 0);
  assert.equal(store.snapshot()[0].peerLastCapacity, 800);
});

test('trend store requires complete local capacity and retains fresh values as reference', () => {
  const store = createSessionTrendStore({ now: () => 1_000, slots: 4 });
  store.update([
    lane('a', 'eth0', 100),
    lane('b', 'eth0', 100),
  ], '2026-08-01T10:00:02Z');
  store.update([
    { ...lane('a', 'eth0', 200), quality: { ...lane('a', 'eth0', 200).quality, capacity_bytes_sec: 400, data_sample_fresh: true } },
    { ...lane('b', 'eth0', 200), quality: { ...lane('b', 'eth0', 200).quality, capacity_bytes_sec: 600, data_sample_fresh: false } },
  ], '2026-08-01T10:00:03Z');

  assert.equal(store.snapshot()[0].capacity, 0);
  assert.equal(store.snapshot()[0].lastCapacity, 0);

  store.update([
    { ...lane('a', 'eth0', 300), quality: { ...lane('a', 'eth0', 300).quality, capacity_bytes_sec: 400, data_sample_fresh: true } },
    { ...lane('b', 'eth0', 300), quality: { ...lane('b', 'eth0', 300).quality, capacity_bytes_sec: 600, data_sample_fresh: true } },
  ], '2026-08-01T10:00:04Z');
  assert.equal(store.snapshot()[0].capacity, 1_000);
  assert.equal(store.snapshot()[0].lastCapacity, 1_000);
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
  assert.deepEqual(byID['interface:a'].rxSamples, [20]);
  assert.deepEqual(byID['interface:b'].rxSamples, []);
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
  assert.equal(series[0].localEndpoint, 'connection-0');
  assert.equal(series[0].remoteEndpoint, '');
});

test('trend store aggregates every lane into one interface series', () => {
  const store = createSessionTrendStore();
  store.update([
    lane('a-1', 'eth0', 100, 200),
    lane('a-2', 'eth0', 300, 400),
    lane('b-1', 'wlan0', 500, 600),
  ], '2026-08-01T10:00:00Z');
  store.update([
    lane('a-1', 'eth0', 120, 230),
    lane('a-2', 'eth0', 350, 460),
    lane('b-1', 'wlan0', 510, 620),
  ], '2026-08-01T10:00:01Z');

  const byID = Object.fromEntries(store.snapshot().map((series) => [series.id, series]));
  assert.deepEqual(byID['interface:eth0'].samples, [70]);
  assert.deepEqual(byID['interface:eth0'].rxSamples, [90]);
  assert.equal(byID['interface:eth0'].capacity, 200);
  assert.deepEqual(byID['interface:wlan0'].samples, [10]);
});

test('trend store resets the baseline when interface lane membership changes', () => {
  const store = createSessionTrendStore();
  store.update([lane('a-1', 'eth0', 100)], '2026-08-01T10:00:00Z');
  store.update([lane('a-1', 'eth0', 120), lane('a-2', 'eth0', 10)], '2026-08-01T10:00:01Z');
  store.update([lane('a-1', 'eth0', 150), lane('a-2', 'eth0', 30)], '2026-08-01T10:00:02Z');

  assert.deepEqual(store.snapshot()[0].samples, [50]);
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

  assert.deepEqual(store.snapshot().map((series) => series.id), ['interface:b']);
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