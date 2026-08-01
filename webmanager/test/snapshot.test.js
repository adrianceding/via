import assert from 'node:assert/strict';
import test from 'node:test';

import { normalizeSnapshot } from '../src/snapshot.js';

test('normalizeSnapshot supplies bounded empty collections', () => {
  const snapshot = normalizeSnapshot({ summary: {}, interfaces: {}, sessions: {}, flows: {} });
  assert.deepEqual(snapshot.interfaces, []);
  assert.deepEqual(snapshot.sessions, []);
  assert.deepEqual(snapshot.flows, []);
  assert.deepEqual(snapshot.terminals, []);
  assert.equal(snapshot.truncated, false);
});

test('normalizeSnapshot preserves totals and reports any truncated response', () => {
  const snapshot = normalizeSnapshot({
    summary: { role: 1 },
    interfaces: { items: [{ name: 'eth0' }], truncated: true },
    sessions: { items: [{ id: 'session-a' }], total: 3 },
    flows: { items: [{ id: 'flow-a' }], terminals: [{ id: 'terminal-a' }], total: 2, terminal_total: 4 },
  });
  assert.equal(snapshot.truncated, true);
  assert.equal(snapshot.sessionTotal, 3);
  assert.equal(snapshot.flowTotal, 2);
  assert.equal(snapshot.terminalTotal, 4);
});