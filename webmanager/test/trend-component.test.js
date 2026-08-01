import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

test('trend component renders options without deferring the first paint', async () => {
  const component = await readFile(new URL('../src/components/SessionTrend.vue', import.meta.url), 'utf8');
  assert.ok(component.includes("{ notMerge: true }"));
  assert.ok(!component.includes('lazyUpdate'));
  assert.ok(component.includes('const currentSeries = chart.getOption().series'));
  assert.ok(component.includes('selectionBySeriesID(currentSeries, selected)'));
  assert.ok(component.includes('toggleTrendIsolation(currentSeries, legendSelection, seriesName)'));
  assert.ok(!component.includes("emit('filter'"));
  assert.ok(component.includes('watch([() => props.trends, locale], render)'));
});