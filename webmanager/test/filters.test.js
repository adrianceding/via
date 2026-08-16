import assert from 'node:assert/strict';
import test from 'node:test';

import {
  flowMatchesFilter,
  interfaceMatchesFilter,
  isFlowAbnormal,
  isInterfaceAbnormal,
  isSessionAbnormal,
  isTerminalAbnormal,
  sessionMatchesFilter,
  terminalMatchesFilter,
} from '../src/filters.js';

test('session filter searches correlation and endpoint fields', () => {
  const session = {
    state: 3,
    connection_id: 'connection-a',
    path_group_id: 'path-group-a',
    lane: 3,
    interface: 'eth0',
    remote_endpoint: '192.0.2.1:38473',
    quality: { stall_penalty_micros: 0 },
  };
  assert.equal(sessionMatchesFilter(session, 'connection-a', false), true);
  assert.equal(sessionMatchesFilter(session, 'path-group-a', false), true);
  assert.equal(sessionMatchesFilter(session, '3', false), true);
  assert.equal(sessionMatchesFilter(session, '192.0.2.1', false), true);
  assert.equal(sessionMatchesFilter(session, '', true), false);
  assert.equal(sessionMatchesFilter({ ...session, state: 4 }, '', true), true);
  assert.equal(isSessionAbnormal(session), false);
  assert.equal(isSessionAbnormal({ ...session, quality: { stall_penalty_micros: 1 } }), true);
});

test('active flow filter treats recovery stages as abnormal', () => {
  const flow = { state: 3, adaptive_state: 1, flow_id: 'flow-a', preferred_connection_id: 'connection-a' };
  assert.equal(flowMatchesFilter(flow, 'flow-a', false), true);
  assert.equal(flowMatchesFilter({ ...flow, adaptive_state: 2 }, '定向补发', false), true);
  assert.equal(flowMatchesFilter({ ...flow, adaptive_state: 2 }, 'targeted retry', false), true);
  assert.equal(flowMatchesFilter(flow, '', true), false);
  assert.equal(flowMatchesFilter({ ...flow, adaptive_state: 2 }, '', true), true);
  assert.equal(isFlowAbnormal(flow), false);
});

test('terminal filter only treats reset as abnormal', () => {
  const closed = { state: 6, flow_id: 'flow-a', reason: 11 };
  assert.equal(terminalMatchesFilter(closed, '', true), false);
  assert.equal(terminalMatchesFilter({ ...closed, state: 8 }, '', true), true);
  assert.equal(terminalMatchesFilter(closed, '正常完成', false), true);
  assert.equal(terminalMatchesFilter(closed, 'completed normally', false), true);
  assert.equal(isTerminalAbnormal(closed), false);
  assert.equal(isTerminalAbnormal({ ...closed, state: 8 }), true);
});

test('interface abnormality follows reason semantics and treats unknown values conservatively', () => {
  for (const reason of [1, 2, 3]) assert.equal(isInterfaceAbnormal({ reason }), false, `reason ${reason}`);
  for (const reason of [4, 5, 6]) assert.equal(isInterfaceAbnormal({ reason }), true, `reason ${reason}`);
  for (const reason of [undefined, null, 0, 7, 2.5, '4']) {
    assert.equal(isInterfaceAbnormal({ reason }), true, `unknown reason ${String(reason)}`);
  }
});

test('interface filter keeps strategy exclusions searchable unless anomaly-only is enabled', () => {
  const item = { name: 'eth0', addresses: ['192.0.2.10'], reason: 1 };
  assert.equal(interfaceMatchesFilter(item, 'eth0', false), true);
  assert.equal(interfaceMatchesFilter(item, '192.0.2', false), true);
  assert.equal(interfaceMatchesFilter(item, '', true), false);
  const excluded = { name: 'lo', addresses: ['127.0.0.1'], reason: 3 };
  assert.equal(interfaceMatchesFilter(excluded, 'lo', false), true);
  assert.equal(interfaceMatchesFilter(excluded, '', true), false);
  assert.equal(interfaceMatchesFilter(excluded, 'lo', true), false);
  assert.equal(interfaceMatchesFilter({ ...item, reason: 4 }, 'eth0', true), true);
  assert.equal(interfaceMatchesFilter({ ...item, reason: 4 }, 'missing', true), false);
});
