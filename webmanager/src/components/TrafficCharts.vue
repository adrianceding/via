<script setup>
import { GridComponent, LegendComponent, MarkLineComponent, TooltipComponent } from 'echarts/components';
import { LineChart, PieChart } from 'echarts/charts';
import { init, use } from 'echarts/core';
import { CanvasRenderer } from 'echarts/renderers';
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch, watchEffect } from 'vue';
import { useI18n } from 'vue-i18n';

import { formatBytes, formatRate } from '../format.js';
import { MAX_SHARE_GROUPS, summarizeDirectionalShareData } from '../traffic-chart.js';
import { directionalTrend } from '../trends.js';

use([CanvasRenderer, GridComponent, LegendComponent, LineChart, MarkLineComponent, PieChart, TooltipComponent]);

const MAX_SERIES = 12;
const TREND_SLOTS = 120;

const props = defineProps({
  loading: { type: Boolean, default: false },
  role: { type: Number, default: 0 },
  sessions: { type: Array, required: true },
  total: { type: Number, default: 0 },
  trends: { type: Array, required: true },
});

const { locale, t } = useI18n();

const uplinkContainer = ref(null);
const downlinkContainer = ref(null);
const uplinkShareContainer = ref(null);
const downlinkShareContainer = ref(null);
let uplinkChart = null;
let downlinkChart = null;
let uplinkShareChart = null;
let downlinkShareChart = null;
let resizeObserver = null;
let uplinkObservedElement = null;
let downlinkObservedElement = null;
let uplinkShareObservedElement = null;
let downlinkShareObservedElement = null;

const visibleLinks = computed(() => props.trends
  .map((trend) => {
    const directions = directionalTrend(trend, props.role);
    return {
      id: trend.id,
      name: trend.label || trend.id,
      localEndpoint: trend.localEndpoint || '',
      remoteEndpoint: trend.remoteEndpoint || '',
      uplink: {
        ...directions.uplink,
        samples: directions.uplink.samples.slice(-TREND_SLOTS).map(Number),
      },
      downlink: {
        ...directions.downlink,
        samples: directions.downlink.samples.slice(-TREND_SLOTS).map(Number),
      },
    };
  })
  .filter((link) => link.uplink.samples.length > 0 || link.downlink.samples.length > 0));
const uplinkLinks = computed(() => directionLinks('uplink'));
const downlinkLinks = computed(() => directionLinks('downlink'));
const directionalShares = computed(() => summarizeDirectionalShareData(
  props.sessions,
  props.role,
  MAX_SHARE_GROUPS,
  t('charts.shareOther'),
));
const trendScope = computed(() => ({ shown: visibleLinks.value.length, total: props.total || props.sessions.length }));
const throughputSummary = computed(() => t('charts.throughputSummary', {
  shown: trendScope.value.shown,
  total: trendScope.value.total,
  omitted: Math.max(0, trendScope.value.total - trendScope.value.shown),
}));
function shareSummaryLabel(direction) {
  const summary = directionalShares.value[direction];
  return summary.hasData
    ? t('charts.shareSummary', {
      groups: summary.groupCount,
      value: formatBytes(summary.totalBytes),
    })
    : t('charts.shareEmpty');
}

const palette = ['#0e7f6e', '#3a63c7', '#b0760a', '#8b99ad', '#7a4d9e', '#c2473d'];

function pad(series) {
  return Array(TREND_SLOTS - series.length).fill(null).concat(series);
}

function directionLinks(direction) {
  return visibleLinks.value
    .filter((link) => link[direction].samples.length > 0)
    .sort((left, right) => right[direction].capacity - left[direction].capacity
      || right[direction].lastCapacity - left[direction].lastCapacity)
    .slice(0, MAX_SERIES);
}

function buildThroughputOption(direction) {
  const links = direction === 'uplink' ? uplinkLinks.value : downlinkLinks.value;
  const series = [];
  const markLines = [];
  links.forEach((link, index) => {
    const color = palette[index % palette.length];
    const values = link[direction];
    series.push({
      name: link.name,
      type: 'line',
      showSymbol: false,
      connectNulls: false,
      data: pad(values.samples),
      lineStyle: { width: 2, color },
      itemStyle: { color },
    });
    const reference = values.capacity > 0 ? values.capacity : values.lastCapacity;
    if (reference > 0) {
      const current = values.capacity > 0;
      markLines.push({
        name: t(current ? 'charts.currentCapacity' : 'charts.lastCapacityReference'),
        type: 'line',
        data: [{ yAxis: reference }],
        lineStyle: { type: current ? 'dashed' : 'dotted', width: 1.4, color, opacity: current ? 0.7 : 0.45 },
        label: { show: false },
        tooltip: { formatter: () => `${t(current ? 'charts.currentCapacityTitle' : 'charts.lastCapacityReferenceTitle', { name: link.name })}<br>${formatRate(reference)}` },
        silent: true,
      });
    }
  });
  return {
    animation: false,
    grid: { left: 62, right: 16, top: 34, bottom: 26 },
    tooltip: {
      trigger: 'axis',
      valueFormatter: (value) => formatRate(value),
      formatter: (parameters) => parameters.map((parameter) => {
        const value = parameter.data == null ? '--' : formatRate(parameter.data);
        const link = links[parameter.seriesIndex];
        const endpoint = link ? `${link.localEndpoint || '--'} → ${link.remoteEndpoint || '--'}` : '';
        return `${escapeHTML(parameter.seriesName)}<br>${value}${endpoint ? `<br>${escapeHTML(endpoint)}` : ''}`;
      }).join('<br><br>'),
    },
    legend: { top: 4, itemWidth: 14, itemHeight: 8, textStyle: { fontSize: 11 }, type: 'scroll' },
    xAxis: {
      type: 'category', boundaryGap: false,
      data: Array.from({ length: TREND_SLOTS }, (_value, index) => index - TREND_SLOTS + 1),
      axisLabel: { show: false }, axisTick: { show: false },
    },
    yAxis: { type: 'value', min: 0, splitNumber: 3, axisLabel: { formatter: (value) => formatBytes(value) } },
    series: [...series, ...markLines],
  };
}

function escapeHTML(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function buildShareOption(direction) {
  const summary = directionalShares.value[direction];
  const totalBytes = summary.totalBytes;
  return {
    animation: false,
    tooltip: {
      trigger: 'item',
      valueFormatter: (value) => formatBytes(value),
      formatter: (parameter) => `${escapeHTML(parameter.name)}<br>${formatBytes(parameter.value)} (${parameter.percent}%)`,
    },
    legend: { bottom: 0, itemWidth: 12, itemHeight: 12, textStyle: { fontSize: 11 } },
    series: [{
      type: 'pie',
      radius: ['52%', '74%'],
      center: ['50%', '44%'],
      label: { show: false },
      labelLine: { show: false },
      emphasis: { scale: false },
      itemStyle: { borderWidth: 3, borderColor: 'transparent' },
      data: summary.data.map((item, index) => ({
        name: item.label,
        value: item.value,
        itemStyle: { color: palette[index % palette.length] },
      })),
    }],
    graphic: [
      { type: 'text', left: 'center', top: '36%', style: { text: formatBytes(totalBytes), fontSize: 17, fontWeight: 700, fill: 'currentColor' } },
      { type: 'text', left: 'center', top: '48%', style: { text: t('charts.shareTotal'), fontSize: 10, fill: 'currentColor', opacity: 0.6 } },
    ],
  };
}

function disposeShareChart(direction) {
  const uplink = direction === 'uplink';
  const observed = uplink ? uplinkShareObservedElement : downlinkShareObservedElement;
  if (observed) resizeObserver?.unobserve(observed);
  if (uplink) {
    uplinkShareObservedElement = null;
    uplinkShareChart?.dispose();
    uplinkShareChart = null;
  } else {
    downlinkShareObservedElement = null;
    downlinkShareChart?.dispose();
    downlinkShareChart = null;
  }
}

function disposeDirectionChart(direction) {
  const uplink = direction === 'uplink';
  const observed = uplink ? uplinkObservedElement : downlinkObservedElement;
  if (observed) resizeObserver?.unobserve(observed);
  if (uplink) {
    uplinkObservedElement = null;
    uplinkChart?.dispose();
    uplinkChart = null;
  } else {
    downlinkObservedElement = null;
    downlinkChart?.dispose();
    downlinkChart = null;
  }
}

function render() {
  if (uplinkChart) uplinkChart.setOption(buildThroughputOption('uplink'), { notMerge: true });
  if (downlinkChart) downlinkChart.setOption(buildThroughputOption('downlink'), { notMerge: true });
  if (uplinkShareChart && directionalShares.value.uplink.hasData) uplinkShareChart.setOption(buildShareOption('uplink'), { notMerge: true });
  if (downlinkShareChart && directionalShares.value.downlink.hasData) downlinkShareChart.setOption(buildShareOption('downlink'), { notMerge: true });
}

// 吞吐容器按 visibleLinks 条件渲染：图表延迟到容器实际存在后再初始化。
watchEffect(() => {
  const links = visibleLinks.value;
  const shares = directionalShares.value;
  void nextTick(() => {
    if (!uplinkChart && uplinkContainer.value) {
      uplinkChart = init(uplinkContainer.value, null, { renderer: 'canvas' });
      uplinkObservedElement = uplinkContainer.value;
      resizeObserver?.observe(uplinkContainer.value);
    }
    if (!downlinkChart && downlinkContainer.value) {
      downlinkChart = init(downlinkContainer.value, null, { renderer: 'canvas' });
      downlinkObservedElement = downlinkContainer.value;
      resizeObserver?.observe(downlinkContainer.value);
    }
    if (!uplinkLinks.value.length) disposeDirectionChart('uplink');
    if (!downlinkLinks.value.length) disposeDirectionChart('downlink');
    for (const direction of ['uplink', 'downlink']) {
      const summary = shares[direction];
      const container = direction === 'uplink' ? uplinkShareContainer.value : downlinkShareContainer.value;
      const chart = direction === 'uplink' ? uplinkShareChart : downlinkShareChart;
      if (!summary.hasData) disposeShareChart(direction);
      if (summary.hasData && !chart && container) {
        const nextChart = init(container, null, { renderer: 'canvas' });
        resizeObserver?.observe(container);
        if (direction === 'uplink') {
          uplinkShareChart = nextChart;
          uplinkShareObservedElement = container;
        } else {
          downlinkShareChart = nextChart;
          downlinkShareObservedElement = container;
        }
      }
    }
    if (links !== undefined) render();
  });
});

onMounted(() => {
  resizeObserver = new ResizeObserver(() => {
    uplinkChart?.resize();
    downlinkChart?.resize();
    uplinkShareChart?.resize();
    downlinkShareChart?.resize();
  });
  if (uplinkObservedElement) resizeObserver.observe(uplinkObservedElement);
  if (downlinkObservedElement) resizeObserver.observe(downlinkObservedElement);
  if (uplinkShareObservedElement) resizeObserver.observe(uplinkShareObservedElement);
  if (downlinkShareObservedElement) resizeObserver.observe(downlinkShareObservedElement);
});

watch([() => props.trends, () => props.sessions, locale], render);

onBeforeUnmount(() => {
  resizeObserver?.disconnect();
  disposeDirectionChart('uplink');
  disposeDirectionChart('downlink');
  disposeShareChart('uplink');
  disposeShareChart('downlink');
  resizeObserver = null;
});
</script>

<template>
  <section class="chart-grid">
    <div class="direction-chart-row">
      <article class="panel chart-panel">
        <header class="panel-head">
          <h2 id="uplink-chart-heading">{{ t('charts.uplinkTitle') }}</h2>
          <span class="badge tone-neutral">{{ t('charts.uplinkBadge') }} · {{ t('charts.throughputScope', { shown: uplinkLinks.length, total: trendScope.total }) }}</span>
        </header>
        <div v-if="uplinkLinks.length" ref="uplinkContainer" class="chart" role="img" aria-labelledby="uplink-chart-heading" aria-describedby="throughput-chart-summary" />
        <p v-else class="empty-block">{{ loading ? t('charts.throughputLoading') : t('charts.throughputEmpty') }}</p>
      </article>
      <article class="panel chart-panel share-panel">
        <header class="panel-head">
          <h2 id="uplink-share-chart-heading">{{ t('charts.uplinkShareTitle') }}</h2>
          <span class="badge tone-neutral">{{ t('charts.uplinkShareBadge') }}</span>
        </header>
        <div v-if="directionalShares.uplink.hasData" ref="uplinkShareContainer" class="chart" role="img" aria-labelledby="uplink-share-chart-heading" aria-describedby="uplink-share-chart-summary" />
        <p v-else class="empty-block">{{ loading ? t('charts.shareLoading') : t('charts.shareEmpty') }}</p>
        <p id="uplink-share-chart-summary" class="visually-hidden" aria-live="polite">{{ shareSummaryLabel('uplink') }}</p>
      </article>
    </div>
    <div class="direction-chart-row">
      <article class="panel chart-panel">
        <header class="panel-head">
          <h2 id="downlink-chart-heading">{{ t('charts.downlinkTitle') }}</h2>
          <span class="badge tone-neutral">{{ t('charts.downlinkBadge') }} · {{ t('charts.throughputScope', { shown: downlinkLinks.length, total: trendScope.total }) }}</span>
        </header>
        <div v-if="downlinkLinks.length" ref="downlinkContainer" class="chart" role="img" aria-labelledby="downlink-chart-heading" aria-describedby="throughput-chart-summary" />
        <p v-else class="empty-block">{{ loading ? t('charts.throughputLoading') : t('charts.throughputEmpty') }}</p>
      </article>
      <article class="panel chart-panel share-panel">
        <header class="panel-head">
          <h2 id="downlink-share-chart-heading">{{ t('charts.downlinkShareTitle') }}</h2>
          <span class="badge tone-neutral">{{ t('charts.downlinkShareBadge') }}</span>
        </header>
        <div v-if="directionalShares.downlink.hasData" ref="downlinkShareContainer" class="chart" role="img" aria-labelledby="downlink-share-chart-heading" aria-describedby="downlink-share-chart-summary" />
        <p v-else class="empty-block">{{ loading ? t('charts.shareLoading') : t('charts.shareEmpty') }}</p>
        <p id="downlink-share-chart-summary" class="visually-hidden" aria-live="polite">{{ shareSummaryLabel('downlink') }}</p>
      </article>
    </div>
    <p id="throughput-chart-summary" class="visually-hidden" aria-live="polite">{{ throughputSummary }}</p>
  </section>
</template>
