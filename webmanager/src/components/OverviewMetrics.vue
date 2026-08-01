<script setup>
import { computed } from 'vue';
import { useI18n } from 'vue-i18n';

import { formatBytes, formatPercent, formatRate } from '../format.js';

const props = defineProps({
  sessions: { type: Array, required: true },
  summary: { type: Object, required: true },
  rates: { type: Object, required: true },
});
const { t } = useI18n();

const metrics = computed(() => {
  const resources = props.summary.resources || {};
  const counters = props.summary.counters || {};
  return [
    [t('metrics.sessions'), String(resources.sessions ?? props.sessions.length), t('common.currentResources')],
    [t('metrics.flows'), String(resources.flows ?? 0), t('common.currentResources')],
    [t('metrics.applicationConnections'), String(resources.socks_connections ?? 0), t('common.currentResources')],
    [t('metrics.sendRate'), formatRate(props.rates.sent), t('common.total', { value: formatBytes(counters.bytes_sent) })],
    [t('metrics.receiveRate'), formatRate(props.rates.received), t('common.total', { value: formatBytes(counters.bytes_received) })],
    [
      t('metrics.overhead'),
      `${formatBytes(counters.retransmitted_bytes)} / ${formatBytes(counters.redundant_bytes)}`,
      t('metrics.sendShare', { value: formatPercent(Number(counters.retransmitted_bytes || 0) + Number(counters.redundant_bytes || 0), counters.bytes_sent) }),
    ],
  ];
});
</script>

<template>
  <section class="metrics" :aria-label="t('metrics.label')">
    <article v-for="([label, value, detail]) in metrics" :key="label">
      <span>{{ label }}</span>
      <strong>{{ value }}</strong>
      <small>{{ detail }}</small>
    </article>
  </section>
</template>