import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import { summarizeDirectionalShareData, summarizeShareData } from '../src/traffic-chart.js';

function session(id, eligible, capacity = 0, options = {}) {
  return {
    id,
    state: 3,
    interface: options.interface ?? id,
    path_group_id: options.pathGroupID,
    quality: {
      eligible_acked_data_payload_bytes: eligible,
      received_data_payload_bytes: options.received ?? 0,
      capacity_bytes_sec: capacity,
    },
  };
}

test('share summary uses cumulative acknowledged DATA instead of capacity', () => {
  const summary = summarizeShareData([
    session('fast', 128, 10_000_000),
    session('slow', 64, 1_000_000),
  ]);

  assert.equal(summary.totalEligible, 192);
  assert.deepEqual(summary.data.map((item) => item.value), [128, 64]);
  assert.notEqual(summary.totalEligible, 11_000_000);
});

test('directional share maps acknowledged and received DATA by runtime role', () => {
  const sessions = [
    session('eth-lane', 100, 0, { interface: 'eth0', received: 400 }),
    session('wifi-lane', 300, 0, { interface: 'wlan0', received: 600 }),
  ];
  const client = summarizeDirectionalShareData(sessions, 1);
  const server = summarizeDirectionalShareData(sessions, 2);

  assert.equal(client.uplink.totalBytes, 400);
  assert.equal(client.downlink.totalBytes, 1_000);
  assert.deepEqual(server.uplink, client.downlink);
  assert.deepEqual(server.downlink, client.uplink);
});

test('share summary reports an explicit empty state without acknowledged DATA', () => {
  const summary = summarizeShareData([
    session('unmeasured', 0, 50_000_000),
    session('missing'),
  ]);

  assert.equal(summary.hasData, false);
  assert.equal(summary.totalEligible, 0);
  assert.deepEqual(summary.data, []);
});

test('share summary includes all visible sessions beyond trend limits and bounds groups', () => {
  const sessions = Array.from({ length: 14 }, (_, index) => session(`connection-${index}`, index + 1));
  const summary = summarizeShareData(sessions, 4);

  assert.equal(summary.totalEligible, 105);
  assert.equal(summary.groupCount, 14);
  assert.equal(summary.hiddenGroupCount, 11);
  assert.deepEqual(summary.data.map((item) => item.value), [14, 13, 12, 66]);
  assert.equal(summary.data.at(-1).label, 'Other');
});

test('share summary groups lanes by interface before path group', () => {
  const summary = summarizeShareData([
    session('lane-a', 10, 0, { interface: 'eth0', pathGroupID: 'group-a' }),
    session('lane-b', 20, 0, { interface: 'eth0', pathGroupID: 'group-a' }),
    session('lane-c', 30, 0, { interface: 'eth0', pathGroupID: 'group-b' }),
  ]);

  assert.deepEqual(summary.data.map((item) => [item.label, item.value]), [['eth0', 60]]);
});

test('share summary falls back to path group without interface observations', () => {
  const summary = summarizeShareData([
    session('lane-a', 10, 0, { interface: '', pathGroupID: 'group-a' }),
    session('lane-b', 20, 0, { interface: '', pathGroupID: 'group-a' }),
    session('lane-c', 30, 0, { interface: '', pathGroupID: 'group-b' }),
  ]);

  assert.deepEqual(summary.data.map((item) => [item.label, item.value]), [['group-a', 30], ['group-b', 30]]);
});

test('traffic charts use localized headings and do not render an empty share canvas', async () => {
  const component = await readFile(new URL('../src/components/TrafficCharts.vue', import.meta.url), 'utf8');

  assert.ok(component.includes('summarizeDirectionalShareData('));
  assert.ok(component.includes("formatBytes(totalBytes)"));
  assert.ok(component.includes("v-if=\"directionalShares.uplink.hasData\""));
  assert.ok(component.includes("v-if=\"directionalShares.downlink.hasData\""));
  assert.ok(component.includes('aria-labelledby="uplink-share-chart-heading"'));
  assert.ok(component.includes('aria-labelledby="downlink-share-chart-heading"'));
  assert.ok(component.includes('aria-labelledby="uplink-chart-heading"'));
  assert.ok(component.includes('aria-labelledby="downlink-chart-heading"'));
  assert.ok(component.includes("directionalTrend(trend, props.role)"));
  assert.ok(component.includes("buildThroughputOption('uplink')"));
  assert.ok(component.includes("buildThroughputOption('downlink')"));
  assert.ok(component.includes('values.capacity > 0 ? values.capacity : values.lastCapacity'));
  assert.ok(component.includes("t('charts.shareEmpty')"));
  assert.ok(component.includes('disposeDirectionChart(\'uplink\')'));
  assert.ok(component.includes('disposeDirectionChart(\'downlink\')'));
  assert.ok(component.includes("disposeShareChart('uplink')"));
  assert.ok(component.includes("disposeShareChart('downlink')"));
  assert.ok(component.includes('Showing {shown} / {total} trends') === false);
  assert.ok(!component.includes(':aria-label="t(\'charts.shareAria\')"'));
});