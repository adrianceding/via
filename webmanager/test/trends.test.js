import assert from 'node:assert/strict';
import test from 'node:test';

import { createSessionTrendStore } from '../src/trends.js';

function session(id, rtt) {
  return {
    connection_id: id,
    interface: id,
    local_endpoint: '192.0.2.10:1000',
    remote_endpoint: '198.51.100.20:2000',
    quality: { smoothed_rtt_micros: rtt },
  };
}

test('trend store bounds series and samples', () => {
  const store = createSessionTrendStore();
  const sessions = Array.from({ length: 20 }, (_, index) => session(`connection-${index}`, index + 1));
  for (let sampleIndex = 0; sampleIndex < 140; sampleIndex += 1) store.update(sessions);

  const series = store.snapshot();
  assert.equal(series.length, 12);
  assert.equal(series[0].samples.length, 120);
  assert.equal(series[0].samples[0], 1);
  assert.equal(series[0].localEndpoint, '192.0.2.10:1000');
  assert.equal(series[0].remoteEndpoint, '198.51.100.20:2000');
});

test('trend store removes disconnected series and admits replacements', () => {
  const store = createSessionTrendStore();
  store.update([session('a', 10), session('b', 20)]);
  store.update([session('b', 21), session('c', 30)]);

  assert.deepEqual(store.snapshot().map((series) => series.id), ['b', 'c']);
  assert.deepEqual(store.snapshot()[0].samples, [20, 21]);
});

test('trend store ignores missing and invalid RTT samples', () => {
  const store = createSessionTrendStore();
  store.update([session('a', 0), session('b', Number.NaN), { quality: { smoothed_rtt_micros: 10 } }]);
  assert.deepEqual(store.snapshot(), []);
});