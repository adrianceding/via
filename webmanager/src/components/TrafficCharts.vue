<script setup>
import { GridComponent, LegendComponent, MarkLineComponent, TooltipComponent } from 'echarts/components';
import { LineChart, PieChart } from 'echarts/charts';
import { init, use } from 'echarts/core';
import { CanvasRenderer } from 'echarts/renderers';
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch, watchEffect } from 'vue';
import { useI18n } from 'vue-i18n';

import { formatBytes, formatRate } from '../format.js';

use([CanvasRenderer, GridComponent, LegendComponent, LineChart, MarkLineComponent, PieChart, TooltipComponent]);

const MAX_SERIES = 12;
const TREND_SLOTS = 120;

const props = defineProps({
  sessions: { type: Array, required: true },
  trends: { type: Array, required: true },
});

const { locale, t } = useI18n();

const throughputContainer = ref(null);
const shareContainer = ref(null);
let throughputChart = null;
let shareChart = null;
let resizeObserver = null;

// 每条线路占 2 个系列（上行 + 下行），容量作为当前值 markLine，不占用系列预算。
const visibleLinks = computed(() => props.trends
  .map((trend) => {
    const session = props.sessions.find((item) => (item.connection_id || item.id) === trend.id);
    return {
      id: trend.id,
      name: trend.label || trend.id,
      localEndpoint: trend.localEndpoint || '',
      remoteEndpoint: trend.remoteEndpoint || '',
      samples: trend.samples.slice(-TREND_SLOTS).map(Number),
      rxSamples: (trend.rxSamples || []).slice(-TREND_SLOTS).map(Number),
      capacity: Number(session?.quality?.capacity_bytes_sec) || 0,
      eligible: Number(session?.quality?.eligible_acked_data_payload_bytes) || 0,
    };
  })
  .filter((link) => link.samples.length > 0)
  .sort((left, right) => right.capacity - left.capacity)
  .slice(0, Math.floor(MAX_SERIES / 2)));

const palette = ['#0e7f6e', '#3a63c7', '#b0760a', '#8b99ad', '#7a4d9e', '#c2473d'];

function pad(series) {
  return Array(TREND_SLOTS - series.length).fill(null).concat(series);
}

function buildThroughputOption() {
  const links = visibleLinks.value;
  const series = [];
  const markLines = [];
  links.forEach((link, index) => {
    const color = palette[index % palette.length];
    series.push({
      name: t('charts.throughputTx', { name: link.name }),
      type: 'line',
      showSymbol: false,
      connectNulls: false,
      data: pad(link.samples),
      lineStyle: { width: 2, color },
      itemStyle: { color },
    });
    series.push({
      name: t('charts.throughputRx', { name: link.name }),
      type: 'line',
      showSymbol: false,
      connectNulls: false,
      data: pad(link.rxSamples),
      lineStyle: { width: 2, color: link.id === links[0]?.id ? color : shade(color, -18), type: 'solid' },
      itemStyle: { color: link.id === links[0]?.id ? color : shade(color, -18) },
    });
    if (link.capacity > 0) {
      markLines.push({
        name: t('charts.throughputCapacity'),
        type: 'line',
        data: [{ yAxis: link.capacity }],
        lineStyle: { type: 'dashed', width: 1.4, color, opacity: 0.65 },
        label: { show: false },
        tooltip: { formatter: () => `${t('charts.throughputCapacityTitle', { name: link.name })}<br>${formatRate(link.capacity)}` },
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
        const link = links[parameter.seriesIndex >> 1];
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

function shade(hex, percent) {
  const value = hex.replace('#', '');
  const channel = (offset) => {
    const parsed = parseInt(value.slice(offset, offset + 2), 16);
    const shifted = Math.max(0, Math.min(255, parsed + percent));
    return shifted.toString(16).padStart(2, '0');
  };
  return `#${channel(0)}${channel(2)}${channel(4)}`;
}

function buildShareOption() {
  const links = visibleLinks.value;
  const totalEligible = links.reduce((total, link) => total + link.eligible, 0);
  const totalCapacity = links.reduce((total, link) => total + link.capacity, 0);
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
      data: links.map((link, index) => ({
        name: link.name,
        value: link.eligible,
        itemStyle: { color: palette[index % palette.length] },
      })),
    }],
    graphic: [
      { type: 'text', left: 'center', top: '36%', style: { text: formatRate(totalCapacity), fontSize: 17, fontWeight: 700, fill: 'currentColor' } },
      { type: 'text', left: 'center', top: '48%', style: { text: t('charts.shareTotal'), fontSize: 10, fill: 'currentColor', opacity: 0.6 } },
    ],
  };
}

function render() {
  if (throughputChart) throughputChart.setOption(buildThroughputOption(), { notMerge: true });
  if (shareChart) shareChart.setOption(buildShareOption(), { notMerge: true });
}

// 吞吐容器按 visibleLinks 条件渲染：图表延迟到容器实际存在后再初始化。
watchEffect(() => {
  const links = visibleLinks.value;
  void nextTick(() => {
    if (!throughputChart && throughputContainer.value) {
      throughputChart = init(throughputContainer.value, null, { renderer: 'canvas' });
      resizeObserver?.observe(throughputContainer.value);
    }
    if (!shareChart && shareContainer.value) {
      shareChart = init(shareContainer.value, null, { renderer: 'canvas' });
      resizeObserver?.observe(shareContainer.value);
    }
    if (links !== undefined) render();
  });
});

onMounted(() => {
  resizeObserver = new ResizeObserver(() => {
    throughputChart?.resize();
    shareChart?.resize();
  });
  if (shareContainer.value) resizeObserver.observe(shareContainer.value);
});

watch([() => props.trends, () => props.sessions, locale], render);

onBeforeUnmount(() => {
  resizeObserver?.disconnect();
  throughputChart?.dispose();
  shareChart?.dispose();
  resizeObserver = null;
  throughputChart = null;
  shareChart = null;
});
</script>

<template>
  <section class="chart-grid">
    <article class="panel chart-panel">
      <header class="panel-head">
        <h2>{{ t('charts.throughputTitle') }}</h2>
        <span class="badge tone-neutral">{{ t('charts.throughputBadge') }}</span>
      </header>
      <div v-if="visibleLinks.length" ref="throughputContainer" class="chart" role="img" :aria-label="t('charts.throughputAria')" />
      <p v-else class="empty-block">{{ t('charts.throughputEmpty') }}</p>
    </article>
    <article class="panel chart-panel">
      <header class="panel-head">
        <h2>{{ t('charts.shareTitle') }}</h2>
        <span class="badge tone-neutral">{{ t('charts.shareBadge') }}</span>
      </header>
      <div ref="shareContainer" class="chart" role="img" :aria-label="t('charts.shareAria')" />
    </article>
  </section>
</template>
