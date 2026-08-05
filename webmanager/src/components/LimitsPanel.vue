<script setup>
import { computed } from 'vue';
import { useI18n } from 'vue-i18n';

const props = defineProps({
  summary: { type: Object, required: true },
});

const { t } = useI18n();

const rows = computed(() => {
  const resources = props.summary.resources || {};
  const rejected = props.summary.rejected || {};
  const entries = [
    { label: t('charts.limitsSessions'), value: Number(resources.sessions) || 0, max: Number(resources.max_sessions) || 0, note: t('charts.limitsRejected', { count: rejected.sessions || 0 }) },
    { label: t('charts.limitsFlows'), value: Number(resources.flows) || 0, max: Number(resources.max_flows) || 0, note: t('charts.limitsRejected', { count: rejected.flows || 0 }) },
    { label: t('charts.limitsSOCKS'), value: Number(resources.socks_connections) || 0, max: Number(resources.max_socks_connections) || 0, note: t('charts.limitsRejected', { count: rejected.socks_connections || 0 }) },
    { label: t('charts.limitsTerminals'), value: Number(resources.tombstones) || 0, max: Number(resources.max_tombstones) || 0, note: t('charts.limitsTerminalsNote') },
  ];
  return entries.map((entry) => {
    const ratio = entry.max > 0 ? entry.value / entry.max : 0;
    return {
      ...entry,
      ratio,
      tone: ratio > 0.8 ? 'tone-bad' : ratio > 0.5 ? 'tone-warning' : 'tone-good',
    };
  });
});
</script>

<template>
  <section class="panel limits" aria-label="resource limits">
    <header class="panel-head">
      <h2>{{ t('charts.limitsTitle') }}</h2>
      <span class="badge tone-neutral">{{ t('charts.limitsBadge') }}</span>
    </header>
    <div class="limits-body">
      <div v-for="row in rows" :key="row.label" class="limit-row">
        <div class="limit-top">
          <span>{{ row.label }}</span>
          <em>{{ row.value }} / {{ row.max }}</em>
        </div>
        <div class="bar">
          <i class="bar-fill" :class="row.tone" :style="{ width: `${Math.min(100, row.ratio * 100).toFixed(1)}%` }" />
        </div>
        <small>{{ row.note }}</small>
      </div>
    </div>
  </section>
</template>
