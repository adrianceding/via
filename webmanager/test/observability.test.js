import assert from 'node:assert/strict';
import test from 'node:test';

import {
  calculateRates,
  describeFreshness,
  evaluateHealth,
  healthStatusKey,
  resolveStatusPresentation,
} from '../src/observability.js';

test('calculateRates derives bounded rates from consecutive successful snapshots', () => {
  const previous = {
    generated_at: '2026-08-01T10:00:00Z',
    counters: { bytes_sent: 9000, bytes_received: 10000, data_payload_bytes_sent: 1000, data_payload_bytes_received: 2000 },
  };
  const next = {
    generated_at: '2026-08-01T10:00:02Z',
    counters: { bytes_sent: 15000, bytes_received: 17000, data_payload_bytes_sent: 5000, data_payload_bytes_received: 5000 },
  };

  assert.deepEqual(calculateRates(previous, next), { sent: 2000, received: 1500 });
  assert.deepEqual(calculateRates(next, previous), { sent: null, received: null });
  assert.deepEqual(calculateRates(previous, {
    generated_at: '2026-08-01T10:00:02Z',
    counters: { bytes_sent: 10, bytes_received: 20, data_payload_bytes_sent: 10, data_payload_bytes_received: 20 },
  }), { sent: null, received: null });

  assert.deepEqual(calculateRates(previous, {
    generated_at: '2026-08-01T10:00:02Z',
    counters: { bytes_sent: 15000, bytes_received: 17000 },
  }), { sent: null, received: null });

  assert.deepEqual(calculateRates({ ...previous, counters: { ...previous.counters, data_payload_bytes_received: null } }, next), {
    sent: null,
    received: null,
  });

  assert.deepEqual(calculateRates(previous, {
    generated_at: '2026-08-01T10:00:02Z',
    counters: { bytes_sent: 15000, bytes_received: 17000, data_payload_bytes_sent: 1000, data_payload_bytes_received: 2000 },
  }), { sent: 0, received: 0 });

  assert.deepEqual(calculateRates(previous, {
    generated_at: '2026-08-01T10:00:02Z',
    counters: { bytes_sent: 15000, bytes_received: 17000, data_payload_bytes_sent: 900, data_payload_bytes_received: 1900 },
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

test('evaluateHealth applies role-aware ready session rules and preserves full summary counts', () => {
  assert.deepEqual(evaluateHealth({
    summary: { healthy: true, role: 1, sessions: 0, ready_sessions: 0 },
    sessions: [],
  }), { level: 'unhealthy', reasons: [{ code: 'noSessions' }] });
  assert.deepEqual(evaluateHealth({
    summary: { healthy: true, role: 1, sessions: 3, ready_sessions: 0 },
    sessions: [{ state: 4 }],
  }), { level: 'unhealthy', reasons: [{ code: 'noReadySessions', count: 3 }] });
  const unavailableWithSignals = evaluateHealth({
    summary: { healthy: true, role: 1, sessions: 3, ready_sessions: 0, resources: { recovering_flows: 2 } },
    sessions: [{ state: 4 }],
  }, 4);
  assert.deepEqual(unavailableWithSignals, {
    level: 'unhealthy',
    reasons: [
      { code: 'noReadySessions', count: 3 },
      { code: 'recoveringFlows', count: 2 },
      { code: 'droppedEvents', count: 4 },
    ],
  });
  assert.equal(healthStatusKey(unavailableWithSignals), 'transportUnavailable');
  assert.deepEqual(evaluateHealth({
    summary: { healthy: true, role: 1, sessions: 3, ready_sessions: 2, resources: {} },
    sessions: [{ state: 3 }],
  }), { level: 'degraded', reasons: [{ code: 'unavailableSessions', count: 1 }] });
  assert.deepEqual(evaluateHealth({
    summary: { healthy: true, role: 1, sessions: 2, ready_sessions: 2, resources: {} },
    sessions: [],
  }), { level: 'healthy', reasons: [] });
  assert.deepEqual(evaluateHealth({
    summary: { healthy: true, role: 2, sessions: 0, ready_sessions: 0, resources: {} },
    sessions: [],
  }), { level: 'healthy', reasons: [] });
  assert.deepEqual(evaluateHealth({
    summary: { healthy: true, role: 2, sessions: 3, ready_sessions: 2, resources: {} },
    sessions: [],
  }), { level: 'degraded', reasons: [{ code: 'unavailableSessions', count: 1 }] });
  assert.deepEqual(evaluateHealth({
    summary: { healthy: true, role: 1, sessions: 4, resources: {} },
    sessions: [{ state: 3 }, { state: 4 }],
  }), { level: 'degraded', reasons: [{ code: 'unavailableSessions', count: 1 }] });
  assert.deepEqual(evaluateHealth({
    summary: { healthy: true, role: 1, sessions: 0, ready_sessions: -1, resources: {} },
    sessions: [],
  }), { level: 'unhealthy', reasons: [{ code: 'noSessions' }] });
  assert.deepEqual(evaluateHealth({
    summary: { healthy: false, role: 1, sessions: 0, ready_sessions: 0 },
    sessions: [],
  }), { level: 'unhealthy', reasons: [] });
});

test('healthStatusKey gives client transport loss an explicit presentation key', () => {
  assert.equal(healthStatusKey({ level: 'unhealthy', reasons: [{ code: 'noSessions' }] }), 'transportUnavailable');
  assert.equal(healthStatusKey({ level: 'unhealthy', reasons: [{ code: 'noReadySessions', count: 2 }] }), 'transportUnavailable');
  assert.equal(healthStatusKey({ level: 'unhealthy', reasons: [] }), 'unhealthy');
  assert.equal(healthStatusKey({ level: 'degraded', reasons: [] }), 'degraded');
});

test('status presentation keeps error, unavailable, and stale precedence', () => {
  const transportHealth = { level: 'unhealthy', reasons: [{ code: 'noSessions' }] };
  assert.deepEqual(resolveStatusPresentation({ error: new Error('unauthorized'), freshnessState: 'fresh', health: transportHealth }), {
    level: 'error', key: null,
  });
  assert.deepEqual(resolveStatusPresentation({ error: null, freshnessState: 'unavailable', health: transportHealth }), {
    level: 'connecting', key: 'connecting',
  });
  assert.deepEqual(resolveStatusPresentation({ error: null, freshnessState: 'stale', health: transportHealth }), {
    level: 'unhealthy', key: 'stale',
  });
  assert.deepEqual(resolveStatusPresentation({ error: null, freshnessState: 'fresh', health: transportHealth }), {
    level: 'unhealthy', key: 'transportUnavailable',
  });
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
