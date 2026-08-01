import assert from 'node:assert/strict';
import test from 'node:test';

import { buildViewURL, parseViewState, serializeViewState } from '../src/view-state.js';

test('view state parses supported URL parameters with bounded query text', () => {
  const state = parseViewState(`?q=${'x'.repeat(300)}&anomaly=1&flow=terminal&sort=rtt`);
  assert.equal(state.query.length, 256);
  assert.deepEqual({ ...state, query: 'query' }, {
    query: 'query',
    onlyAnomalies: true,
    flowTab: 'terminal',
    sessionSort: 'rtt',
  });
});

test('view state rejects unknown options and omits defaults when serialized', () => {
  assert.deepEqual(parseViewState('?anomaly=no&flow=other&sort=random'), {
    query: '',
    onlyAnomalies: false,
    flowTab: 'active',
    sessionSort: 'source',
  });
  assert.equal(serializeViewState(parseViewState('')), '');
  assert.equal(serializeViewState({
    query: ' connection-a ',
    onlyAnomalies: true,
    flowTab: 'terminal',
    sessionSort: 'state',
  }), '?q=connection-a&anomaly=1&flow=terminal&sort=state');
});

test('shared view URL preserves location while replacing supported state', () => {
  assert.equal(buildViewURL('http://127.0.0.1:19090/manager?old=1#flows', {
    query: 'connection-a',
    onlyAnomalies: true,
    flowTab: 'terminal',
    sessionSort: 'rtt',
  }), 'http://127.0.0.1:19090/manager?q=connection-a&anomaly=1&flow=terminal&sort=rtt#flows');
});