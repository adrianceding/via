import assert from 'node:assert/strict';
import test from 'node:test';

import { calculateRates, describeFreshness, evaluateHealth } from '../src/observability.js';

test('calculateRates derives bounded rates from consecutive successful snapshots', () => {
  const previous = {
    generated_at: '2026-08-01T10:00:00Z',
    counters: { bytes_sent: 1000, bytes_received: 2000 },
  };
  const next = {
    generated_at: '2026-08-01T10:00:02Z',
    counters: { bytes_sent: 5000, bytes_received: 5000 },
  };

  assert.deepEqual(calculateRates(previous, next), { sent: 2000, received: 1500 });
  assert.deepEqual(calculateRates(next, previous), { sent: null, received: null });
  assert.deepEqual(calculateRates(previous, {
    generated_at: '2026-08-01T10:00:02Z',
    counters: { bytes_sent: 10, bytes_received: 20 },
  }), { sent: null, received: null });
});

test('evaluateHealth reports current degradation without treating historical drops as permanent', () => {
  const snapshot = {
    summary: {
      healthy: true,
      resources: { recovering_flows: 1 },
      counters: { dropped_status_events: 9 },
    },
    sessions: [{ state: 3 }, { state: 4 }],
  };

  assert.deepEqual(evaluateHealth(snapshot, 0), {
    level: 'degraded',
    reasons: [
      { code: 'recoveringFlows', count: 1 },
      { code: 'unavailableSessions', count: 1 },
    ],
  });
  assert.deepEqual(evaluateHealth({
    summary: { healthy: true, resources: {}, counters: { dropped_status_events: 9 } },
    sessions: [{ state: 3 }],
  }, 0), { level: 'healthy', reasons: [] });
  assert.deepEqual(evaluateHealth({
    summary: { healthy: true, resources: {}, counters: { dropped_status_events: 10 } },
    sessions: [{ state: 3 }],
  }, 1), { level: 'degraded', reasons: [{ code: 'droppedEvents', count: 1 }] });
});

test('describeFreshness distinguishes fresh, stale, and paused snapshots', () => {
  const lastSuccessAt = Date.parse('2026-08-01T10:00:00Z');
  assert.deepEqual(describeFreshness(lastSuccessAt, lastSuccessAt + 3000, false), {
    stale: false,
    state: 'fresh',
    ageSeconds: 3,
  });
  assert.deepEqual(describeFreshness(lastSuccessAt, lastSuccessAt + 7000, false), {
    stale: true,
    state: 'stale',
    ageSeconds: 7,
  });
  assert.deepEqual(describeFreshness(lastSuccessAt, lastSuccessAt + 7000, true), {
    stale: true,
    state: 'paused',
    ageSeconds: 7,
  });
  assert.deepEqual(describeFreshness(null, lastSuccessAt, false), {
    stale: true,
    state: 'unavailable',
    ageSeconds: 0,
  });
});