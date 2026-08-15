import assert from 'node:assert/strict';
import test from 'node:test';

import { summarizeSessionObservations } from '../src/session-observations.js';

function lane(id, options = {}) {
  return {
    id,
    connection_id: id,
    interface: options.interface ?? 'eth0',
    path_group_id: options.pathGroupID ?? 'group-a',
    lane: options.lane ?? 1,
    state: options.state ?? 3,
    reconnects: options.reconnects ?? 0,
    quality: {
      smoothed_rtt_micros: options.rtt ?? 0,
      capacity_bytes_sec: options.capacity ?? 0,
      data_sample_fresh: options.fresh ?? false,
      data_sample_age_ms: options.age ?? null,
      last_data_capacity_bytes_sec: options.lastCapacity ?? 0,
      peer_send_capacity_bytes_sec: options.peerCapacity ?? 0,
      peer_send_data_sample_at: options.peerSampleAt,
      peer_send_data_sample_expires_at: options.peerExpiresAt,
      queued_bytes: options.queued ?? 0,
      in_flight_bytes: options.inFlight ?? 0,
    },
  };
}

test('session observations summarize lanes by interface', () => {
  const observations = summarizeSessionObservations([
    lane('a', { lane: 1, rtt: 40, capacity: 100, fresh: true, queued: 2 }),
    lane('b', { lane: 2, rtt: 20, capacity: 200, fresh: true, inFlight: 3 }),
    lane('c', { interface: 'wlan0', pathGroupID: 'group-b', state: 4 }),
  ]);

  assert.deepEqual(observations.map((item) => item.label), ['eth0', 'wlan0']);
  assert.equal(observations[0].laneCount, 2);
  assert.equal(observations[0].readyCount, 2);
  assert.equal(observations[0].rtt, 30);
  assert.equal(observations[0].uplink.capacity, 300);
  assert.equal(observations[0].queuedBytes, 2);
  assert.equal(observations[0].inFlightBytes, 3);
});

test('session observations separate complete current and stale reference capacity by role', () => {
  const observations = summarizeSessionObservations([
    lane('a', {
      lane: 1, fresh: false, age: 8_000, lastCapacity: 100,
      peerCapacity: 400, peerSampleAt: '2026-08-01T10:00:00Z', peerExpiresAt: '2026-08-01T10:00:03Z',
    }),
    lane('b', {
      lane: 2, fresh: false, age: 7_000, lastCapacity: 200,
      peerCapacity: 600, peerSampleAt: '2026-08-01T10:00:01Z', peerExpiresAt: '2026-08-01T10:00:04Z',
    }),
  ], 'name', 2, '2026-08-01T10:00:08Z');

  assert.equal(observations[0].uplink.capacity, null);
  assert.equal(observations[0].uplink.lastCapacity, 1_000);
  assert.equal(observations[0].downlink.capacity, null);
  assert.equal(observations[0].downlink.lastCapacity, 300);
  assert.equal(observations[0].lanes[0].capacityDirections.uplink.state, 'stale');
  assert.equal(observations[0].lanes[0].capacityDirections.downlink.ageMs, 8_000);
});

test('session observations do not report partial interface capacity as current total', () => {
  const observations = summarizeSessionObservations([
    lane('fresh', { lane: 1, fresh: true, capacity: 100 }),
    lane('missing', { lane: 2 }),
  ], 'name', 1);

  assert.equal(observations[0].uplink.freshCount, 1);
  assert.equal(observations[0].uplink.capacity, null);
  assert.equal(observations[0].uplink.lastCapacity, null);
});

test('session observations fall back to path group without interface', () => {
  const observations = summarizeSessionObservations([
    lane('a', { interface: '', pathGroupID: 'group-a' }),
    lane('b', { interface: '', pathGroupID: 'group-a', lane: 2 }),
    lane('c', { interface: '', pathGroupID: 'group-b' }),
  ]);

  assert.deepEqual(observations.map((item) => item.key), ['path_group:group-a', 'path_group:group-b']);
  assert.equal(observations[0].laneCount, 2);
});

test('session observation sorting uses average group RTT', () => {
  const sessions = [
    lane('slow', { interface: 'eth0', rtt: 80 }),
    lane('fast', { interface: 'eth0', lane: 2, rtt: 20 }),
    lane('middle', { interface: 'wlan0', pathGroupID: 'group-b', rtt: 40 }),
    lane('down', { interface: 'wwan0', pathGroupID: 'group-c', state: 4 }),
  ];

  assert.deepEqual(summarizeSessionObservations(sessions, 'rtt').map((item) => item.label), ['wlan0', 'eth0', 'wwan0']);
});