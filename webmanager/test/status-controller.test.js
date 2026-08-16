import assert from 'node:assert/strict';
import test from 'node:test';

import { createStatusController } from '../src/status-controller.js';

test('status controller atomically publishes a successful snapshot', async () => {
  const trendUpdates = [];
  let now = 1000;
  const controller = createStatusController({
    fetchSnapshot: async () => ({
      summary: { healthy: true, role: 1, generated_at: '2026-08-01T10:00:00Z' },
      interfaces: { items: [] },
      sessions: { generated_at: '2026-08-01T10:00:01Z', items: [{ id: 'session-a', state: 3 }], total: 1 },
      flows: { items: [], terminals: [] },
    }),
    trendStore: {
      update: (sessions, generatedAt) => trendUpdates.push({ sessions, generatedAt }),
      snapshot: () => [{ id: 'session-a', label: 'eth0', samples: [1200] }],
    },
    now: () => now,
  });

  assert.equal(controller.refreshing.value, false);
  await controller.refresh();

  assert.equal(controller.snapshot.value.sessionTotal, 1);
  assert.equal(controller.health.value.level, 'healthy');
  assert.equal(controller.lastSuccessAt.value, 1000);
  assert.deepEqual(controller.rates.value, { sent: null, received: null });
  assert.equal(controller.refreshing.value, false);
  assert.equal(controller.error.value, null);
  assert.deepEqual(trendUpdates, [{
    sessions: [{ id: 'session-a', state: 3 }],
    generatedAt: '2026-08-01T10:00:01Z',
  }]);
  assert.equal(controller.trends.value.length, 1);
});

test('status controller preserves the last snapshot after a refresh failure', async () => {
  let shouldFail = false;
  const controller = createStatusController({
    fetchSnapshot: async () => {
      if (shouldFail) throw Object.assign(new Error('unauthorized'), { status: 401 });
      return {
        summary: { healthy: false, role: 2 },
        interfaces: {},
        sessions: { items: [{ id: 'session-a' }] },
        flows: {},
      };
    },
  });

  await controller.refresh();
  shouldFail = true;
  await controller.refresh();

  assert.equal(controller.snapshot.value.sessions[0].id, 'session-a');
  assert.equal(controller.error.value.status, 401);
});

test('status controller derives rates and current dropped-event degradation', async () => {
  let refresh = 0;
  const snapshots = [
    { generated_at: '2026-08-01T10:00:00Z', healthy: true, counters: { bytes_sent: 1000, bytes_received: 2000, data_payload_bytes_sent: 1000, data_payload_bytes_received: 2000, dropped_status_events: 4 } },
    { generated_at: '2026-08-01T10:00:02Z', healthy: true, counters: { bytes_sent: 5000, bytes_received: 5000, data_payload_bytes_sent: 5000, data_payload_bytes_received: 5000, dropped_status_events: 5 } },
  ];
  const controller = createStatusController({
    fetchSnapshot: async () => ({
      summary: snapshots[refresh++],
      interfaces: {},
      sessions: { items: [{ id: 'session-a', state: 3 }] },
      flows: {},
    }),
    now: () => refresh * 1000,
  });

  await controller.refresh();
  await controller.refresh();

  assert.deepEqual(controller.rates.value, { sent: 2000, received: 1500 });
  assert.deepEqual(controller.health.value, {
    level: 'degraded',
    reasons: [{ code: 'droppedEvents', count: 1 }],
  });
  assert.equal(controller.lastSuccessAt.value, 2000);
});
