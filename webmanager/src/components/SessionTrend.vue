<script setup>
import { GridComponent, LegendComponent, TooltipComponent } from 'echarts/components';
import { LineChart } from 'echarts/charts';
import { init, use } from 'echarts/core';
import { CanvasRenderer } from 'echarts/renderers';
import { onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { useI18n } from 'vue-i18n';

import { buildTrendOption, selectionBySeriesID, toggleTrendIsolation } from '../trend-option.js';

use([CanvasRenderer, GridComponent, LegendComponent, LineChart, TooltipComponent]);

const props = defineProps({
  trends: { type: Array, required: true },
});

const { locale, t } = useI18n();

const container = ref(null);
let chart = null;
let resizeObserver = null;
let legendSelection = {};

function render() {
  if (!chart) return;
  chart.setOption(buildTrendOption(props.trends, legendSelection, t('trend.connection')), { notMerge: true });
}

onMounted(() => {
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
  resizeObserver = new ResizeObserver(() => chart?.resize());
  resizeObserver.observe(container.value);
  render();
});

watch([() => props.trends, locale], render);

onBeforeUnmount(() => {
  resizeObserver?.disconnect();
  chart?.off('legendselectchanged');
  chart?.off('click');
  chart?.dispose();
  resizeObserver = null;
  chart = null;
  legendSelection = {};
});
</script>

<template>
  <section class="trend-section" aria-labelledby="trend-heading">
    <header class="section-heading">
      <div><p class="eyebrow">{{ t('trend.eyebrow') }}</p><h2 id="trend-heading">{{ t('trend.title') }}</h2></div>
      <span class="count">{{ trends.length }}</span>
    </header>
    <div class="trend-chart-wrap">
      <div ref="container" class="trend-chart" role="img" :aria-label="t('trend.aria')" />
    </div>
  </section>
</template>