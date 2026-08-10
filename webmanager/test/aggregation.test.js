import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import { summarizeAggregation } from '../src/aggregation.js';

function session(id, options = {}) {
  return {
    id,
    interface: id,
    state: options.state ?? 3,
    fastest: options.fastest,
    quality: {
      capacity_bytes_sec: options.capacity ?? 1_048_576,
      data_sample_fresh: options.fresh ?? false,
      data_sample_age_ms: options.age ?? null,
      smoothed_rtt_micros: options.rtt ?? 0,
      eligible_acked_data_payload_bytes: options.eligible ?? 0,
    },
  };
}

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
    session('zeta', { capacity: 2_000_000, fresh: true, rtt: 80_000 }),
    session('alpha', { capacity: 2_000_000, fresh: true, rtt: 80_000 }),
    session('lower-rtt', { capacity: 2_000_000, fresh: true, rtt: 60_000 }),
  ]);

  assert.deepEqual(summary.rows.map((row) => row.session.id), ['lower-rtt', 'alpha', 'zeta']);
  assert.equal(summary.rows.filter((row) => row.isHighestCapacity).length, 1);
});

test('aggregation leaves uplift and share unavailable at their boundaries', () => {
  const empty = summarizeAggregation([]);
  const single = summarizeAggregation([session('only', { capacity: 0, fresh: true })]);

  assert.equal(empty.totalCapacity, null);
  assert.equal(empty.highestCapacity, null);
  assert.equal(single.totalCapacity, null);
  assert.equal(single.highestCapacity, 0);
  assert.equal(single.lift, null);
  assert.equal(single.highestShare, null);
});

test('aggregation panel consumes the shared summary and marks the measured-capacity leader', async () => {
  const component = await readFile(new URL('../src/components/AggregationPanel.vue', import.meta.url), 'utf8');

  assert.ok(component.includes("import { summarizeAggregation } from '../aggregation.js';"));
  assert.ok(component.includes('summarizeAggregation(props.sessions)'));
  assert.ok(component.includes('row.isHighestCapacity'));
  assert.ok(component.includes("summary.highestShare == null ? '--'"));
  assert.ok(!component.includes('index === 0'));
  assert.ok(!component.includes('const fastest = computed'));
});
