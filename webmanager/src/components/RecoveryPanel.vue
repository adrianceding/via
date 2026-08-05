<script setup>
import { computed } from 'vue';
import { useI18n } from 'vue-i18n';

import { formatBytes, formatMicros } from '../format.js';

const props = defineProps({
  flows: { type: Array, required: true },
  sessions: { type: Array, required: true },
  terminals: { type: Array, required: true },
});

const { t } = useI18n();

const reconnects = computed(() => props.sessions.reduce((total, session) => total + (Number(session.reconnects) || 0), 0));
const rows = computed(() => [
  ...props.flows
    .filter((flow) => Number(flow.recovery_count) > 0)
    .map((flow) => ({
      id: flow.flow_id || flow.id,
      count: Number(flow.recovery_count) || 0,
      micros: Number(flow.recovery_micros) || 0,
      retransmitted: Number(flow.retransmitted_bytes) || 0,
      redundant: Number(flow.redundant_bytes) || 0,
    })),
  ...props.terminals
    .filter((terminal) => Number(terminal.recovery_count) > 0)
    .map((terminal) => ({
      id: terminal.flow_id || terminal.id,
      count: Number(terminal.recovery_count) || 0,
      micros: Number(terminal.recovery_micros) || 0,
      retransmitted: Number(terminal.retransmitted_bytes) || 0,
      redundant: Number(terminal.redundant_bytes) || 0,
    })),
]);
</script>

<template>
  <section class="panel recovery" aria-label="recovery statistics">
    <header class="panel-head">
      <h2>{{ t('charts.recoveryTitle') }}</h2>
      <span
        class="badge tone-warning"
        :title="t('charts.recoveryReconnectsTitle')"
      >{{ t('charts.recoveryReconnects', { count: reconnects }) }}</span>
    </header>
    <div class="recovery-list">
      <p v-if="!rows.length" class="empty-block">{{ t('charts.recoveryEmpty') }}</p>
      <div v-for="row in rows" :key="row.id" class="recovery-item">
        <div class="recovery-top">
          <span class="recovery-id">{{ row.id }}</span>
          <span class="badge tone-warning">{{ t('charts.recoveryCount', { count: row.count }) }}</span>
        </div>
        <div class="recovery-meta">
          <span>{{ t('charts.recoveryDuration', { duration: formatMicros(row.micros) }) }}</span>
          <span>{{ t('charts.recoveryRetransmitted', { bytes: formatBytes(row.retransmitted) }) }}</span>
          <span>{{ t('charts.recoveryRedundant', { bytes: formatBytes(row.redundant) }) }}</span>
        </div>
      </div>
    </div>
  </section>
</template>
