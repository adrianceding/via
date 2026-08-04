const TREND_SLOTS = 120;

import { formatRate } from './format.js';

function escapeHTML(value) {
  return String(value || '').replace(/[&<>"']/g, (character) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  })[character]);
}

export function selectionBySeriesID(series, selectedByName) {
  return Object.fromEntries(series.map((item) => [item.id, selectedByName[item.name] !== false]));
}

export function toggleTrendIsolation(series, selectedByID, selectedName) {
  const isolated = series.every((item) => (selectedByID[item.id] !== false) === (item.name === selectedName));
  return Object.fromEntries(series.map((item) => [item.id, isolated || item.name === selectedName]));
}

export function buildTrendOption(
  trends,
  legendSelection = {},
  fallbackLabel = 'Connection',
  axisLabel = 'DATA throughput (bytes/s)',
) {
  const labels = Array.from({ length: TREND_SLOTS }, (_value, index) => index - TREND_SLOTS + 1);
  const series = trends.slice(0, 12).map((trend) => {
    const samples = trend.samples.slice(-TREND_SLOTS).map((value) => Number(value));
    return {
      id: trend.id,
      name: trend.label || fallbackLabel,
      localEndpoint: trend.localEndpoint || '',
      remoteEndpoint: trend.remoteEndpoint || '',
      type: 'line',
        triggerEvent: 'line',
      showSymbol: false,
      connectNulls: false,
      data: Array(TREND_SLOTS - samples.length).fill(null).concat(samples),
    };
  });
  const selected = Object.fromEntries(series.map((item) => [item.name, legendSelection[item.id] !== false]));
  const seriesByID = new Map(series.map((item) => [item.id, item]));
  return {
    animation: false,
    color: ['#137a50', '#1769aa', '#b05a16', '#8a4d9c', '#b43b32', '#54732c'],
    grid: { top: 50, right: 18, bottom: 30, left: 58 },
    legend: { type: 'scroll', top: 4, left: 8, right: 8, selected, textStyle: { color: '#46514c', fontSize: 11 } },
    tooltip: {
      trigger: 'axis',
      formatter: (parameters) => parameters.map((parameter) => {
        const item = seriesByID.get(parameter.seriesId);
        const value = parameter.data == null ? '--' : formatRate(parameter.data);
        return `${escapeHTML(parameter.seriesName)}<br>${value}<br>${escapeHTML(item?.localEndpoint || '--')} → ${escapeHTML(item?.remoteEndpoint || '--')}`;
      }).join('<br><br>'),
    },
    xAxis: { type: 'category', boundaryGap: false, data: labels, axisLabel: { show: false }, axisTick: { show: false } },
    yAxis: { type: 'value', min: 0, name: axisLabel, nameTextStyle: { color: '#65706b' }, splitNumber: 3 },
    series,
  };
}