import assert from 'node:assert/strict';
import test from 'node:test';

import { createPoller } from '../src/poller.js';

test('poller schedules only after the current refresh settles', async () => {
  let finishRefresh;
  const scheduled = [];
  const poller = createPoller(
    () => new Promise((resolve) => { finishRefresh = resolve; }),
    (callback, delay) => { scheduled.push([callback, delay]); return 7; },
    () => {},
  );

  const started = poller.start();
  assert.equal(scheduled.length, 0);
  finishRefresh();
  await started;
  assert.equal(scheduled.length, 1);
  assert.equal(scheduled[0][1], 3000);
});

test('stopped poller does not schedule another refresh', async () => {
  let scheduled = false;
  const poller = createPoller(
    async () => {},
    () => { scheduled = true; return 8; },
    () => {},
  );
  poller.stop();
  await poller.start();
  assert.equal(scheduled, false);
});

test('pausing an in-flight refresh prevents the next schedule', async () => {
  let finishRefresh;
  const scheduled = [];
  const cancelled = [];
  const poller = createPoller(
    () => new Promise((resolve) => { finishRefresh = resolve; }),
    (callback, delay) => { scheduled.push([callback, delay]); return 9; },
    (timer) => cancelled.push(timer),
  );

  const started = poller.start();
  poller.pause();
  finishRefresh();
  await started;

  assert.equal(poller.paused, true);
  assert.equal(poller.refreshing, false);
  assert.deepEqual(scheduled, []);
  assert.deepEqual(cancelled, []);
});

test('manual refresh while paused runs once and remains paused', async () => {
  let refreshes = 0;
  const scheduled = [];
  const poller = createPoller(
    async () => { refreshes += 1; },
    (callback, delay) => { scheduled.push([callback, delay]); return 10; },
    () => {},
  );

  poller.pause();
  await poller.refreshNow();

  assert.equal(refreshes, 1);
  assert.equal(poller.paused, true);
  assert.deepEqual(scheduled, []);
});

test('repeated manual refreshes reuse the in-flight request', async () => {
  let finishRefresh;
  let refreshes = 0;
  const poller = createPoller(() => {
    refreshes += 1;
    return new Promise((resolve) => { finishRefresh = resolve; });
  });

  const first = poller.refreshNow();
  const second = poller.refreshNow();
  assert.equal(refreshes, 1);
  assert.equal(poller.refreshing, true);
  finishRefresh();
  await Promise.all([first, second]);
  poller.stop();
});

test('resume refreshes immediately and restores one timer', async () => {
  let refreshes = 0;
  const scheduled = [];
  const poller = createPoller(
    async () => { refreshes += 1; },
    (callback, delay) => { scheduled.push([callback, delay]); return 11; },
    () => {},
  );

  poller.pause();
  await poller.resume();

  assert.equal(refreshes, 1);
  assert.equal(poller.paused, false);
  assert.equal(scheduled.length, 1);
  assert.equal(scheduled[0][1], 3000);
  poller.stop();
});