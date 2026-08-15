import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

test('trend component renders options without deferring the first paint', async () => {
  const component = await readFile(new URL('../src/components/SessionTrend.vue', import.meta.url), 'utf8');
  const app = await readFile(new URL('../src/App.vue', import.meta.url), 'utf8');
  assert.ok(component.includes("{ notMerge: true }"));
  assert.ok(!component.includes('lazyUpdate'));
  assert.ok(component.includes('const currentSeries = chart.getOption().series'));
  assert.ok(component.includes('selectionBySeriesID(currentSeries, selected)'));
  assert.ok(component.includes('toggleTrendIsolation(currentSeries, legendSelection, seriesName)'));
  assert.ok(component.includes("label: t('trend.aggregate')"));
  assert.ok(!component.includes("emit('filter'"));
    assert.ok(component.includes('watch([() => props.aggregate, () => props.trends, locale, hasData]'));
    assert.ok(component.includes('if (!hasData.value)'));
    assert.ok(component.includes('disposeChart()'));
  assert.ok(app.includes('controller.trends.value.filter((trend) => visibleObservationIDs.value.has(trend.id))'));
  assert.ok(!app.includes('trend.label} ${trend.id} ${trend.localEndpoint} ${trend.remoteEndpoint}'));
  assert.ok(!app.includes("import SessionTrend from './components/SessionTrend.vue';"));
  assert.ok(!app.includes('<SessionTrend'));
  assert.ok(!app.includes('visibleAggregateTrend'));
  assert.ok(app.includes('<TrafficCharts'));
});