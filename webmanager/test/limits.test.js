import assert from 'node:assert/strict';
import test from 'node:test';

import { flowRejectionNoteParams } from '../src/observability.js';

test('flow rejection note preserves totals and normalizes optional classifications', () => {
  assert.deepEqual(flowRejectionNoteParams({
    flows: 7,
    flow_rate_limited: 2,
    flow_opening_capacity: 3,
    flow_target_dial_capacity: 1,
  }), { count: 7, rate: 2, opening: 3, target: 1 });
  assert.deepEqual(flowRejectionNoteParams({ flows: 4 }), { count: 4, rate: 0, opening: 0, target: 0 });
  assert.deepEqual(flowRejectionNoteParams({ flows: -1, flow_rate_limited: 'invalid' }), { count: 0, rate: 0, opening: 0, target: 0 });
});