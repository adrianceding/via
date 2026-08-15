import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

test('overview active flow count follows the filtered flow response', async () => {
  const component = await readFile(new URL('../src/components/OverviewMetrics.vue', import.meta.url), 'utf8');
  const app = await readFile(new URL('../src/App.vue', import.meta.url), 'utf8');
  assert.ok(component.includes('activeFlows: { type: Number, required: true }'));
  assert.ok(component.includes('String(props.activeFlows)'));
  assert.ok(app.includes(':active-flows="controller.snapshot.value.flowTotal"'));
  assert.ok(app.includes('const trendTotal = computed(() => normalizedQuery.value || onlyAnomalies.value'));
  assert.ok(app.includes(':total="trendTotal"'));
});