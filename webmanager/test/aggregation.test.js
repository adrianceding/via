import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import { directionalAggregation, summarizeAggregation } from '../src/aggregation.js';

function session(id, options = {}) {
  return {
    id,
    interface: options.interface ?? id,
    principal: options.principal,
    path_group_id: options.pathGroupID,
    lane: options.lane,
    state: options.state ?? 3,
    fastest: options.fastest,
    quality: {
      capacity_bytes_sec: options.capacity ?? 1_048_576,
      data_sample_fresh: options.fresh ?? false,
      data_sample_age_ms: options.age ?? (options.fresh ? 0 : null),
      last_data_capacity_bytes_sec: options.lastCapacity ?? 0,
      smoothed_rtt_micros: options.rtt ?? 0,
      eligible_acked_data_payload_bytes: options.eligible ?? 0,
      peer_send_capacity_bytes_sec: options.peerCapacity ?? 0,
      peer_send_data_sample_at: options.peerSampleAt,
      peer_send_data_sample_expires_at: options.peerExpiresAt,
    },
  };
}

test('aggregation separates fresh uplink and downlink capacities', () => {
  const generatedAt = '2026-08-01T10:00:02Z';
  const summary = summarizeAggregation([
    session('lane-1', {
      interface: 'eth0', capacity: 2_000_000, fresh: true, peerCapacity: 8_000_000,
      peerSampleAt: '2026-08-01T10:00:01.900Z', peerExpiresAt: '2026-08-01T10:00:04.900Z',
    }),
    session('lane-2', {
      interface: 'eth0', capacity: 3_000_000, fresh: true, peerCapacity: 12_000_000,
      peerSampleAt: '2026-08-01T10:00:01.800Z', peerExpiresAt: '2026-08-01T10:00:04.800Z',
    }),
  ], 'name', generatedAt);

  assert.equal(summary.totalCapacity, 5_000_000);
  assert.equal(summary.peerTotalCapacity, 20_000_000);
  assert.equal(summary.peerFreshCount, 2);
  assert.deepEqual(summary.groups[0].peerCapacityRange, {
    min: 8_000_000,
    average: 10_000_000,
    max: 12_000_000,
  });
});

test('aggregation does not add lane capacities measured outside one sample window', () => {
  const generatedAt = '2026-08-01T10:00:02Z';
  const summary = summarizeAggregation([
    session('lane-1', {
      interface: 'eth0', capacity: 2_000_000, fresh: true, age: 100,
      peerCapacity: 8_000_000, peerSampleAt: '2026-08-01T10:00:01.900Z',
      peerExpiresAt: '2026-08-01T10:00:04.900Z',
    }),
    session('lane-2', {
      interface: 'eth0', capacity: 3_000_000, fresh: true, age: 500,
      peerCapacity: 12_000_000, peerSampleAt: '2026-08-01T10:00:01.500Z',
      peerExpiresAt: '2026-08-01T10:00:04.500Z',
    }),
  ], 'name', generatedAt);

  assert.equal(summary.totalCapacity, null);
  assert.equal(summary.lastTotalCapacity, null);
  assert.equal(summary.peerTotalCapacity, null);
  assert.equal(summary.peerLastTotalCapacity, null);
  assert.equal(summary.groups[0].totalCapacity, null);
  assert.equal(summary.groups[0].peerTotalCapacity, null);
  assert.equal(summary.highestCapacity, 3_000_000);
  assert.equal(summary.peerHighestCapacity, 12_000_000);
});

test('aggregation excludes expired or incomplete downlink capacity samples', () => {
  const summary = summarizeAggregation([
    session('stale', {
      peerCapacity: 8_000_000,
      peerSampleAt: '2026-08-01T10:00:00Z', peerExpiresAt: '2026-08-01T10:00:03Z',
    }),
    session('unmeasured'),
  ], 'name', '2026-08-01T10:00:03.001Z');

  assert.equal(summary.peerFreshCount, 0);
  assert.equal(summary.peerTotalCapacity, null);
  assert.deepEqual(summary.rows.map((row) => row.peerSampleState), ['stale', 'unmeasured']);
  assert.ok(summary.rows.every((row) => row.peerCapacity == null));
});

test('aggregation keeps expired capacities as reference only', () => {
  const summary = summarizeAggregation([
    session('uplink-stale', { capacity: 1_048_576, age: 8_000, lastCapacity: 6_000_000 }),
    session('downlink-stale', {
      peerCapacity: 20_000_000,
      peerSampleAt: '2026-08-01T10:00:00Z', peerExpiresAt: '2026-08-01T10:00:03Z',
    }),
  ], 'name', '2026-08-01T10:00:08Z');

  assert.equal(summary.totalCapacity, null);
  assert.equal(summary.peerTotalCapacity, null);
  const uplink = summary.rows.find((row) => row.session.id === 'uplink-stale');
  const downlink = summary.rows.find((row) => row.session.id === 'downlink-stale');
  assert.equal(uplink.capacity, null);
  assert.equal(uplink.lastCapacity, 6_000_000);
  assert.equal(downlink.peerCapacity, null);
  assert.equal(downlink.peerLastCapacity, 20_000_000);
  assert.equal(summary.groups.find((group) => group.label === 'uplink-stale').referenceCapacityRange.max, 6_000_000);
  assert.equal(summary.groups.find((group) => group.label === 'downlink-stale').peerReferenceCapacityRange.max, 20_000_000);
});

test('aggregation reports complete totals for one ready lane but leaves lift unavailable', () => {
  const summary = summarizeAggregation([
    session('only', {
      interface: 'eth0', capacity: 4_000_000, fresh: true,
      peerCapacity: 20_000_000, peerSampleAt: '2026-08-01T10:00:00Z',
      peerExpiresAt: '2026-08-01T10:00:03Z',
    }),
  ], 'name', '2026-08-01T10:00:08Z');

  assert.equal(summary.totalCapacity, 4_000_000);
  assert.equal(summary.lastTotalCapacity, 4_000_000);
  assert.equal(summary.peerTotalCapacity, null);
  assert.equal(summary.peerLastTotalCapacity, 20_000_000);
  assert.equal(summary.groups[0].totalCapacity, 4_000_000);
  assert.equal(summary.groups[0].peerLastTotalCapacity, 20_000_000);
  assert.equal(summary.lift, null);
  assert.equal(summary.peerLift, null);
});

test('directional aggregation keeps stale references separate and maps server directions', () => {
  const summary = summarizeAggregation([
    session('lane-1', {
      interface: 'eth0', fresh: false, age: 8_000, lastCapacity: 6_000_000,
      peerCapacity: 20_000_000, peerSampleAt: '2026-08-01T10:00:00Z',
      peerExpiresAt: '2026-08-01T10:00:03Z',
    }),
    session('lane-2', {
      interface: 'eth0', fresh: false, age: 7_900, lastCapacity: 8_000_000,
      peerCapacity: 30_000_000, peerSampleAt: '2026-08-01T10:00:00.100Z',
      peerExpiresAt: '2026-08-01T10:00:03.100Z',
    }),
  ], 'name', '2026-08-01T10:00:08Z');
  const directions = directionalAggregation(summary, 2);

  assert.equal(directions.uplink.totalCapacity, null);
  assert.equal(directions.uplink.lastTotalCapacity, 50_000_000);
  assert.equal(directions.uplink.lastHighestCapacity, 30_000_000);
  assert.equal(directions.uplink.groups[0].lanes.find((row) => row.session.id === 'lane-2').sampleAgeMs, 7_900);
  assert.equal(directions.downlink.totalCapacity, null);
  assert.equal(directions.downlink.lastTotalCapacity, 14_000_000);
  assert.equal(directions.downlink.lastHighestCapacity, 8_000_000);
  assert.equal(directions.downlink.groups[0].lanes.find((row) => row.session.id === 'lane-2').sampleAgeMs, 7_900);
});

test('aggregation excludes unmeasured fallback capacities', () => {
  const summary = summarizeAggregation([
    session('first', { rtt: 500_000 }),
    session('second', { rtt: 70_000 }),
  ]);

  assert.equal(summary.readyCount, 2);
  assert.equal(summary.freshCount, 0);
  assert.equal(summary.totalCapacity, null);
  assert.equal(summary.highestCapacity, null);
  assert.equal(summary.lift, null);
  assert.equal(summary.highestShare, null);
  assert.deepEqual(summary.rows.map((row) => row.sampleState), ['unmeasured', 'unmeasured']);
  assert.ok(summary.rows.every((row) => row.capacity === null && !row.isHighestCapacity));
});

test('aggregation requires fresh samples from every ready session', () => {
  const summary = summarizeAggregation([
    session('fresh', { capacity: 4_000_000, fresh: true, eligible: 100 }),
    session('stale', { capacity: 8_000_000, age: 4_000, eligible: 300 }),
    session('not-ready', { state: 4, capacity: 50_000_000, fresh: true, eligible: 5_000 }),
  ]);

  assert.equal(summary.readyCount, 2);
  assert.equal(summary.freshCount, 1);
  assert.equal(summary.totalCapacity, null);
  assert.equal(summary.highestCapacity, 4_000_000);
  assert.equal(summary.lift, null);
  assert.equal(summary.highestShare, 25);
  assert.deepEqual(summary.rows.map((row) => row.sampleState), ['fresh', 'stale']);
});

test('aggregation computes complete fresh capacity estimates', () => {
  const summary = summarizeAggregation([
    session('policy-fastest', { capacity: 4_000_000, fresh: true, fastest: true, rtt: 50_000, eligible: 100 }),
    session('highest-capacity', { capacity: 8_000_000, fresh: true, rtt: 80_000, eligible: 300 }),
  ]);

  assert.equal(summary.totalCapacity, 12_000_000);
  assert.equal(summary.highestCapacity, 8_000_000);
  assert.equal(summary.lift, 50);
  assert.equal(summary.highestShare, 75);
  assert.equal(summary.rows.find((row) => row.isHighestCapacity).session.id, 'highest-capacity');
});

test('aggregation breaks equal-capacity ties by RTT then stable identifier', () => {
  const summary = summarizeAggregation([
    session('zeta', { interface: 'eth0', capacity: 2_000_000, fresh: true, rtt: 80_000 }),
    session('alpha', { interface: 'eth0', capacity: 2_000_000, fresh: true, rtt: 80_000 }),
    session('lower-rtt', { interface: 'eth0', capacity: 2_000_000, fresh: true, rtt: 60_000 }),
  ]);

  assert.deepEqual(summary.rows.map((row) => row.session.id), ['lower-rtt', 'alpha', 'zeta']);
  assert.equal(summary.rows.filter((row) => row.isHighestCapacity).length, 1);
});

test('aggregation leaves uplift and share unavailable at their boundaries', () => {
  const empty = summarizeAggregation([]);
  const single = summarizeAggregation([session('only', { capacity: 0, fresh: true })]);

  assert.equal(empty.totalCapacity, null);
  assert.equal(empty.highestCapacity, null);
  assert.equal(single.totalCapacity, 0);
  assert.equal(single.highestCapacity, 0);
  assert.equal(single.lift, null);
  assert.equal(single.highestShare, null);
});

test('aggregation observes interfaces and keeps path groups as lane detail', () => {
  const summary = summarizeAggregation([
    session('group-a-lane-1', { interface: 'eth0', pathGroupID: 'group-a', lane: 1, capacity: 2_000_000, fresh: true }),
    session('group-a-lane-2', { interface: 'eth0', pathGroupID: 'group-a', lane: 2, capacity: 3_000_000, fresh: true }),
    session('group-b-lane-1', { interface: 'eth0', pathGroupID: 'group-b', lane: 1, capacity: 4_000_000, fresh: true }),
  ]);

  assert.deepEqual(summary.groups.map((group) => group.key), ['interface:eth0']);
  assert.equal(summary.groups[0].laneCount, 3);
  assert.equal(summary.groups[0].interface, 'eth0');
  assert.equal(summary.groupCount, 1);
  assert.equal(summary.highestCapacity, 4_000_000);
  assert.equal(summary.totalCapacity, 9_000_000);
  assert.equal(summary.lift, 125);
});

test('aggregation summarizes fresh capacity and valid RTT distributions by interface', () => {
  const summary = summarizeAggregation([
    session('fresh-low', { interface: 'eth0', capacity: 2_000_000, fresh: true, rtt: 20_000 }),
    session('fresh-high', { interface: 'eth0', capacity: 8_000_000, fresh: true, rtt: 80_000 }),
    session('stale', { interface: 'eth0', capacity: 50_000_000, age: 4_000, rtt: 0 }),
  ]);

  assert.deepEqual(summary.groups[0].capacityRange, {
    min: 2_000_000,
    average: 5_000_000,
    max: 8_000_000,
  });
  assert.deepEqual(summary.groups[0].rttRange, {
    min: 20_000,
    average: 50_000,
    max: 80_000,
  });
});

test('aggregation sorts interfaces by name or average RTT', () => {
  const sessions = [
    session('alpha-fast', { interface: 'alpha', rtt: 20_000 }),
    session('alpha-slow', { interface: 'alpha', rtt: 80_000 }),
    session('beta', { interface: 'beta', rtt: 40_000 }),
  ];

  assert.deepEqual(summarizeAggregation(sessions).groups.map((group) => group.label), ['alpha', 'beta']);
  assert.deepEqual(summarizeAggregation(sessions, 'rtt').groups.map((group) => group.label), ['beta', 'alpha']);
});

test('aggregation leaves Lane distributions unavailable without valid samples', () => {
  const summary = summarizeAggregation([
    session('unmeasured', { interface: 'eth0', rtt: 0 }),
  ]);

  assert.deepEqual(summary.groups[0].capacityRange, { min: null, average: null, max: null });
  assert.deepEqual(summary.groups[0].rttRange, { min: null, average: null, max: null });
});

test('aggregation falls back to interface, principal, then session and preserves every lane', () => {
  const sessions = [
    ...Array.from({ length: 8 }, (_, index) => session(`lane-${index}`, {
      interface: 'eth0', lane: index + 1, capacity: index + 1, fresh: true,
    })),
    session('principal-a', { interface: '', principal: 'principal-a', capacity: 10, fresh: true }),
    session('session-fallback', { interface: '', principal: '', capacity: 11, fresh: true }),
  ];
  const summary = summarizeAggregation(sessions);

  assert.equal(summary.groups[0].key, 'interface:eth0');
  assert.equal(summary.groups[0].laneCount, 8);
  assert.equal(summary.groups[0].lanes.length, 8);
  assert.equal(summary.groups[0].lanes[0].session.id, 'lane-7');
  assert.equal(summary.rows.length, 8 + 1 + 1);
  assert.equal(summary.groups[0].totalCapacity, 36);
  assert.equal(summary.totalCapacity, 57);
  assert.equal(summary.highestCapacity, 11);
});

test('aggregation keeps stale and unmeasured capacity out of effective totals', () => {
  const summary = summarizeAggregation([
    session('fresh', { interface: 'eth0', capacity: 4_000_000, fresh: true }),
    session('stale', { interface: 'wlan0', capacity: 8_000_000, age: 4_000 }),
  ]);

  assert.equal(summary.totalCapacity, null);
  assert.equal(summary.groups[0].highestCapacity, 4_000_000);
  assert.equal(summary.groups[1].highestCapacity, null);
  assert.equal(summary.groups[1].lanes[0].capacity, null);
});

test('aggregation panel consumes the shared summary and marks the measured-capacity leader', async () => {
  const component = await readFile(new URL('../src/components/AggregationPanel.vue', import.meta.url), 'utf8');
  const styles = await readFile(new URL('../src/styles.css', import.meta.url), 'utf8');

  assert.ok(component.includes("import { directionalAggregation, summarizeAggregation } from '../aggregation.js';"));
  assert.ok(component.includes('summarizeAggregation(props.sessions, props.sort, props.snapshotAt)'));
  assert.ok(component.includes('directionalAggregation(summary.value, props.role)'));
  assert.ok(component.includes('charts.aggregationDirections.${direction}.${key}'));
  assert.ok(component.includes("directionText(name, 'currentTotal')"));
  assert.ok(component.includes("directionText(name, row.isHighestCapacity ? 'highestNote' : 'measuredNote'"));
  assert.ok(component.includes('row.isHighestCapacity'));
  assert.ok(component.includes('formatRange(group.rttRange, formatMicros)'));
  assert.ok(component.includes('formatRange(group.capacityRange, formatRate)'));
  assert.ok(component.includes('formatRange(group.referenceCapacityRange, formatRate)'));
  assert.ok(component.includes('direction.totalCapacity == null'));
  assert.ok(component.includes('direction.lastTotalCapacity == null'));
  assert.ok(component.includes("directionText(name, 'staleReferenceNote'"));
  assert.ok(!component.includes('aggregationHiddenLanes'));
  assert.ok(!component.includes("t('charts.aggregationAck')"));
  assert.ok(!component.includes('index === 0'));
  assert.ok(!component.includes('const fastest = computed'));
  assert.match(styles, /\.agg-body\s*\{[^}]*grid-template-columns:\s*minmax\(0, 1fr\)/);
});
