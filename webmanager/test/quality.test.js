import assert from 'node:assert/strict';
import test from 'node:test';

import { selectFastestSession, sortSessions } from '../src/quality.js';

test('fastest session prefers a ready backend selection', () => {
  const selected = selectFastestSession([
    { id: 'lower-rtt', state: 3, quality: { smoothed_rtt_micros: 900 } },
    { id: 'marked', state: 3, fastest: true, quality: { smoothed_rtt_micros: 1200 } },
  ]);
  assert.equal(selected.id, 'marked');
});

test('fastest session falls back to the lowest valid ready RTT', () => {
  const selected = selectFastestSession([
    { id: 'not-ready', state: 4, quality: { smoothed_rtt_micros: 100 } },
    { id: 'unknown-rtt', state: 3, quality: { smoothed_rtt_micros: 0 } },
    { id: 'slower', state: 3, quality: { smoothed_rtt_micros: 2300 } },
    { id: 'faster', state: 3, quality: { smoothed_rtt_micros: 1700 } },
  ]);
  assert.equal(selected.id, 'faster');
  assert.equal(selectFastestSession([]), null);
});

test('session sorting remains stable when the fastest marker changes', () => {
  const sessions = [
    { connection_id: 'b', interface: 'eth1', fastest: true, state: 3, quality: { smoothed_rtt_micros: 100 } },
    { connection_id: 'a', interface: 'eth0', state: 4, quality: { smoothed_rtt_micros: 300 } },
  ];
  assert.deepEqual(sortSessions(sessions, 'source').map((session) => session.connection_id), ['a', 'b']);
  assert.deepEqual(sortSessions(sessions, 'rtt').map((session) => session.connection_id), ['b', 'a']);
  assert.deepEqual(sortSessions(sessions, 'state').map((session) => session.connection_id), ['a', 'b']);
  assert.deepEqual(sessions.map((session) => session.connection_id), ['b', 'a']);
});