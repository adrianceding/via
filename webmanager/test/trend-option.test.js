import assert from 'node:assert/strict';
import test from 'node:test';

import { buildTrendOption, selectionBySeriesID, toggleTrendIsolation } from '../src/trend-option.js';

test('trend option right-aligns bounded DATA throughput samples', () => {
  const option = buildTrendOption([
    { id: 'connection-a', label: 'eth0', localEndpoint: '192.0.2.10:1000', remoteEndpoint: '198.51.100.20:2000', samples: [1000, 2500] },
  ]);

  assert.equal(option.animation, false);
  assert.equal(option.xAxis.data.length, 120);
  assert.equal(option.series.length, 1);
  assert.equal(option.series[0].id, 'connection-a');
  assert.equal(option.series[0].name, 'eth0');
  assert.equal(option.series[0].data.length, 120);
  assert.deepEqual(option.series[0].data.slice(-3), [null, 1000, 2500]);
  assert.equal(option.series[0].triggerEvent, 'line');
  assert.equal(option.series[0].localEndpoint, '192.0.2.10:1000');
  assert.equal(option.yAxis.name, 'DATA throughput (bytes/s)');
  assert.match(option.tooltip.formatter([{ seriesName: option.series[0].name, data: 2500, seriesId: 'connection-a' }]), /2\.44 KiB\/s/);
});

test('trend option remains empty without valid series', () => {
  const option = buildTrendOption([]);
  assert.deepEqual(option.series, []);
});

test('trend option preserves hidden legend series by stable ID across translations', () => {
  const trends = [
    { id: 'connection-a', label: 'eth0', samples: [1000] },
    { id: 'connection-b', label: 'wlan0', samples: [2000] },
  ];
  const refreshed = buildTrendOption(trends, { 'connection-a': false }, '连接');
  const hiddenName = refreshed.series[0].name;

  assert.equal(refreshed.legend.selected[hiddenName], false);
  assert.equal(refreshed.legend.selected[refreshed.series[1].name], true);
});

test('trend option localizes the fallback series label without changing its ID', () => {
  const trends = [{ id: 'connection-a', label: '', samples: [1000] }];
  const chinese = buildTrendOption(trends, { 'connection-a': false }, '连接');
  const english = buildTrendOption(trends, { 'connection-a': false }, 'Connection');

  assert.equal(chinese.series[0].id, english.series[0].id);
  assert.notEqual(chinese.series[0].name, english.series[0].name);
  assert.equal(chinese.legend.selected[chinese.series[0].name], false);
  assert.equal(english.legend.selected[english.series[0].name], false);
});

test('trend interaction keeps IDs internal and groups duplicate interface names', () => {
  const series = [
    { id: 'connection-a', name: 'eth0' },
    { id: 'connection-b', name: 'eth0' },
    { id: 'connection-c', name: 'wlan0' },
  ];
  const hidden = selectionBySeriesID(series, { eth0: false, wlan0: true });
  assert.deepEqual(hidden, { 'connection-a': false, 'connection-b': false, 'connection-c': true });

  const isolated = toggleTrendIsolation(series, hidden, 'eth0');
  assert.deepEqual(isolated, { 'connection-a': true, 'connection-b': true, 'connection-c': false });
  assert.deepEqual(toggleTrendIsolation(series, isolated, 'eth0'), {
    'connection-a': true,
    'connection-b': true,
    'connection-c': true,
  });
});