<script setup>
import { GridComponent, LegendComponent, TooltipComponent } from 'echarts/components';
import { LineChart } from 'echarts/charts';
import { init, use } from 'echarts/core';
import { CanvasRenderer } from 'echarts/renderers';
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { useI18n } from 'vue-i18n';

import { buildTrendOption, selectionBySeriesID, toggleTrendIsolation } from '../trend-option.js';

use([CanvasRenderer, GridComponent, LegendComponent, LineChart, TooltipComponent]);

const props = defineProps({
  aggregate: { type: Object, default: null },
  trends: { type: Array, required: true },
});

const { locale, t } = useI18n();

const container = ref(null);
let chart = null;
let resizeObserver = null;
let legendSelection = {};
const hasData = computed(() => Boolean(props.aggregate || props.trends.length));
const summaryLabel = computed(() => t('trend.summary', {
  connections: props.trends.length,
  aggregate: props.aggregate ? 1 : 0,
}));

function render() {
  if (!chart) return;
  const trends = props.aggregate
    ? [{ ...props.aggregate, label: t('trend.aggregate') }, ...props.trends]
    : props.trends;
  chart.setOption(buildTrendOption(trends, legendSelection, t('trend.connection'), t('trend.axis')), { notMerge: true });
}

function disposeChart() {
  resizeObserver?.unobserve(container.value);
  chart?.off('legendselectchanged');
  chart?.off('click');
  chart?.dispose();
  chart = null;
  legendSelection = {};
}

function ensureChart() {
  if (!hasData.value || chart || !container.value) return;
  chart = init(container.value, null, { renderer: 'canvas' });
  chart.on('legendselectchanged', ({ selected }) => {
    const currentSeries = chart.getOption().series;
    legendSelection = selectionBySeriesID(currentSeries, selected);
  });
  chart.on('click', ({ componentType, seriesName }) => {
    if (componentType !== 'series' || !seriesName) return;
    const currentSeries = chart.getOption().series;
    legendSelection = toggleTrendIsolation(currentSeries, legendSelection, seriesName);
    render();
  });
  resizeObserver?.observe(container.value);
  render();
}

onMounted(() => {
  resizeObserver = new ResizeObserver(() => chart?.resize());
  ensureChart();
});

watch([() => props.aggregate, () => props.trends, locale, hasData], async () => {
  if (!hasData.value) {
    disposeChart();
    return;
  }
  await nextTick();
  ensureChart();
  render();
});

onBeforeUnmount(() => {
  resizeObserver?.disconnect();
  disposeChart();
  resizeObserver = null;
});
</script>

<template>
  <section class="trend-section" aria-labelledby="trend-heading">
    <header class="section-heading">
      <div><p class="eyebrow">{{ t('trend.eyebrow') }}</p><h2 id="trend-heading">{{ t('trend.title') }}</h2></div>
      <span class="count">{{ trends.length }}</span>
    </header>
    <div class="trend-chart-wrap">
      <div
        v-if="hasData"
        ref="container"
        class="trend-chart"
        role="img"
        aria-labelledby="trend-heading"
        aria-describedby="trend-summary"
      />
      <p v-else class="trend-empty">{{ t('trend.empty') }}</p>
    </div>
    <p id="trend-summary" class="visually-hidden" aria-live="polite">{{ summaryLabel }}</p>
  </section>
</template>