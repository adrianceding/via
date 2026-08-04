<script setup>
import { computed } from 'vue';
import { useI18n } from 'vue-i18n';

import { sessionMatchesFilter } from '../filters.js';
import { formatBytes, formatMicros, formatRate } from '../format.js';
import { sortSessions } from '../quality.js';
import { sessionStates } from '../status.js';
import CopyIdentifierButton from './CopyIdentifierButton.vue';

const props = defineProps({
  onlyAnomalies: { type: Boolean, required: true },
  query: { type: String, required: true },
  role: { type: Number, default: 0 },
  sessions: { type: Array, required: true },
  sort: { type: String, required: true },
  total: { type: Number, required: true },
});

defineEmits(['filter', 'update:sort']);
const { t } = useI18n();

const client = computed(() => props.role === 1);
const visibleSessions = computed(() => sortSessions(
  props.sessions.filter((session) => sessionMatchesFilter(session, props.query.trim().toLowerCase(), props.onlyAnomalies)),
  props.sort,
));

function stateClass(state) {
  if (state === 3) return 'good';
  if ([1, 2, 4].includes(state)) return 'warning';
  return 'neutral';
}

function formatCapacity(value) {
  const capacity = Number(value);
  return Number.isFinite(capacity) && capacity > 0 ? formatRate(capacity) : '--';
}

function formatSampleAge(value) {
  const milliseconds = Number(value);
  if (!Number.isFinite(milliseconds) || milliseconds < 0) return '--';
  if (milliseconds < 1000) return `${Math.round(milliseconds)} ms`;
  return `${(milliseconds / 1000).toFixed(milliseconds < 10_000 ? 1 : 0)} s`;
}

function dataSampleText(quality) {
  if (quality?.data_sample_fresh) {
    return quality.data_sample_age_ms == null
      ? t('sessions.dataFresh')
      : t('sessions.dataFreshAge', { age: formatSampleAge(quality.data_sample_age_ms) });
  }
  if (quality?.data_sample_age_ms == null) return t('sessions.dataUnavailable');
  return t('sessions.dataStaleAge', { age: formatSampleAge(quality.data_sample_age_ms) });
}

function dataSampleClass(quality) {
  if (quality?.data_sample_fresh) return 'good';
  return quality?.data_sample_age_ms == null ? 'neutral' : 'warning';
}
</script>

<template>
  <section class="data-section">
    <header class="section-heading">
      <div><p class="eyebrow">{{ t('sessions.eyebrow') }}</p><h2>{{ t('sessions.title') }}</h2></div>
      <div class="section-actions">
        <label class="compact-select">{{ t('sessions.sort') }}
          <select :value="sort" @change="$emit('update:sort', $event.target.value)">
            <option value="source">{{ t('sessions.sortSource') }}</option>
            <option value="rtt">{{ t('sessions.sortRtt') }}</option>
            <option value="state">{{ t('sessions.sortState') }}</option>
          </select>
        </label>
        <span class="count">{{ visibleSessions.length }} / {{ total }}</span>
      </div>
    </header>
    <div class="table-wrap">
      <table class="sessions-table">
        <colgroup>
          <col class="session-col-source">
          <col class="session-col-endpoints">
          <col class="session-col-state">
          <col class="session-col-rtt">
          <col class="session-col-retry">
          <col class="session-col-stall">
          <col class="session-col-queue">
          <col class="session-col-role">
        </colgroup>
        <thead><tr>
          <th>{{ client ? t('sessions.clientSource') : t('sessions.serverSource') }}</th><th>{{ t('sessions.endpoints') }}</th><th>{{ t('sessions.state') }}</th>
          <th :title="t('sessions.smoothedRttTitle')">{{ t('sessions.smoothedRtt') }}</th><th :title="t('sessions.retryTitle')">{{ t('sessions.retry') }}</th><th :title="t('sessions.stallTitle')">{{ t('sessions.stall') }}</th><th :title="t('sessions.queueTitle')">{{ t('sessions.queue') }}</th><th>{{ client ? t('sessions.reconnect') : t('sessions.transport') }}</th>
        </tr></thead>
        <tbody>
          <tr v-if="visibleSessions.length === 0"><td colspan="8" class="empty-row">{{ t('sessions.empty') }}</td></tr>
          <tr v-for="session in visibleSessions" :key="session.id">
            <td :data-label="t('sessions.session')">
              <div class="primary-cell">{{ client ? session.interface : session.principal }}<span v-if="session.fastest" class="fastest-badge">{{ t('sessions.fastest') }}</span></div>
              <small class="mono">{{ t('common.local') }} {{ String(session.id || '').slice(0, 12) }}</small>
              <div class="identifier-cell session-identifier"><button class="identifier-link mono" type="button" @click="$emit('filter', session.connection_id)">{{ session.connection_id || '--' }}</button><CopyIdentifierButton :label="t('common.connectionId')" :value="session.connection_id" /></div>
            </td>
            <td :data-label="t('sessions.endpoints')"><div class="primary-cell mono">{{ session.local_endpoint || session.local_address || '--' }}</div><small class="mono">{{ session.remote_endpoint || '--' }}</small></td>
            <td :data-label="t('sessions.state')"><span class="state" :class="stateClass(session.state)">{{ sessionStates[session.state] ? t(sessionStates[session.state]) : t('common.unknown') }}</span></td>
            <td :data-label="t('sessions.smoothedRtt')">{{ formatMicros(session.quality?.smoothed_rtt_micros) }}</td>
            <td :data-label="t('sessions.retry')">{{ formatMicros(session.quality?.retry_micros) }}</td>
            <td :data-label="t('sessions.stall')">{{ formatMicros(session.quality?.stall_penalty_micros) }}</td>
            <td :data-label="t('sessions.queue')">
              <div class="metric-lines session-quality-lines">
                <span><small>{{ t('sessions.capacity') }}</small>{{ formatCapacity(session.quality?.capacity_bytes_sec) }}</span>
                <span><small>{{ t('sessions.dataSample') }}</small><strong class="data-sample-state" :class="dataSampleClass(session.quality)">{{ dataSampleText(session.quality) }}</strong></span>
                <span><small>{{ t('sessions.queue') }}</small>{{ formatBytes(session.quality?.queued_bytes) }} / {{ formatBytes(session.quality?.in_flight_bytes) }}</span>
              </div>
            </td>
            <td :data-label="client ? t('sessions.reconnect') : t('sessions.transport')">{{ client ? String(session.reconnects || 0) : (session.transport || '--') }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>
</template>