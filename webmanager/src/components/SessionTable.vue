<script setup>
import { computed, ref } from 'vue';
import { useI18n } from 'vue-i18n';

import { sessionMatchesFilter } from '../filters.js';
import { formatBytes, formatMicros, formatRate } from '../format.js';
import { summarizeSessionObservations } from '../session-observations.js';
import { sessionStates } from '../status.js';
import CopyIdentifierButton from './CopyIdentifierButton.vue';

const props = defineProps({
  onlyAnomalies: { type: Boolean, required: true },
  filtered: { type: Boolean, default: false },
  query: { type: String, required: true },
  role: { type: Number, default: 0 },
  sessions: { type: Array, required: true },
  sort: { type: String, required: true },
  snapshotAt: { type: String, default: '' },
  total: { type: Number, required: true },
});

defineEmits(['filter', 'update:sort']);
const { t } = useI18n();

const client = computed(() => props.role === 1);
const expandedObservations = ref(new Set());
const matchingSessions = computed(() => props.sessions.filter((session) => sessionMatchesFilter(
  session,
  props.query.trim().toLowerCase(),
  props.onlyAnomalies,
)));
const observations = computed(() => summarizeSessionObservations(
  matchingSessions.value,
  props.sort,
  props.role,
  props.snapshotAt,
));
const observationScope = computed(() => t(client.value ? 'sessions.interfaceScope' : 'sessions.pathGroupScope', {
  observations: observations.value.length,
  shown: matchingSessions.value.length,
  total: props.total,
}));

function observationTypeLabel(type) {
  return t(`charts.aggregationGroupType.${type}`);
}

function observationStateClass(observation) {
  if (observation.readyCount === observation.laneCount) return 'good';
  if (observation.readyCount > 0) return 'warning';
  return 'neutral';
}

function observationExpanded(key) {
  return props.filtered || expandedObservations.value.has(key);
}

function handleObservationToggle(key, event) {
  if (props.filtered) return;
  const next = new Set(expandedObservations.value);
  if (event.currentTarget.open) next.add(key);
  else next.delete(key);
  expandedObservations.value = next;
}

function stateClass(state) {
  if (state === 3) return 'good';
  if ([1, 2, 4].includes(state)) return 'warning';
  return 'neutral';
}

function formatCapacity(value) {
  const capacity = Number(value);
  return Number.isFinite(capacity) && capacity > 0 ? formatRate(capacity) : '--';
}

function displayCapacity(sample) {
  return formatCapacity(sample?.capacity ?? sample?.lastCapacity);
}

function observationCapacity(direction) {
  return formatCapacity(direction?.capacity ?? direction?.lastCapacity);
}

function formatSampleAge(value) {
  const milliseconds = Number(value);
  if (!Number.isFinite(milliseconds) || milliseconds < 0) return '--';
  if (milliseconds < 1000) return `${Math.round(milliseconds)} ms`;
  return `${(milliseconds / 1000).toFixed(milliseconds < 10_000 ? 1 : 0)} s`;
}

function capacitySampleText(sample) {
  if (sample?.state === 'fresh') return t('sessions.capacityCurrent');
  if (sample?.state === 'stale') {
    return sample.ageMs == null
      ? t('sessions.capacityLastMeasured')
      : t('sessions.capacityLastMeasuredAge', { age: formatSampleAge(sample.ageMs) });
  }
  return t('sessions.dataUnavailable');
}

function capacitySampleClass(sample) {
  if (sample?.state === 'fresh') return 'good';
  return sample?.state === 'stale' ? 'warning' : 'neutral';
}
</script>

<template>
  <section class="data-section">
    <header class="section-heading">
      <div><p class="eyebrow">{{ t('sessions.eyebrow') }}</p><h2>{{ t('sessions.title') }}</h2></div>
      <div class="section-actions">
        <label class="compact-select">{{ t('sessions.sort') }}
          <select :value="sort" @change="$emit('update:sort', $event.target.value)">
            <option value="name">{{ t('sessions.sortName') }}</option>
            <option value="rtt">{{ t('sessions.sortRtt') }}</option>
          </select>
        </label>
        <span class="count observation-count">{{ observationScope }}</span>
      </div>
    </header>
    <p v-if="observations.length === 0" class="empty-block">{{ t('sessions.empty') }}</p>
    <div v-else class="session-observations">
      <details
        v-for="observation in observations"
        :key="observation.key"
        class="session-observation"
        :open="observationExpanded(observation.key)"
        @toggle="handleObservationToggle(observation.key, $event)"
      >
        <summary class="session-observation-summary">
          <span class="observation-name">
            <strong>{{ observation.label }}</strong>
            <small>{{ observationTypeLabel(observation.type) }} · {{ t('sessions.laneCount', { count: observation.laneCount }) }}</small>
          </span>
          <span class="observation-metric"><small>{{ t('sessions.ready') }}</small><strong class="state" :class="observationStateClass(observation)">{{ observation.readyCount }} / {{ observation.laneCount }}</strong></span>
          <span class="observation-metric"><small>{{ t('sessions.smoothedRtt') }}</small><strong>{{ formatMicros(observation.rtt) }}</strong></span>
          <span class="observation-metric"><small>{{ t('sessions.uplinkCapacity') }}</small><strong>{{ observationCapacity(observation.uplink) }}</strong><small>{{ observation.uplink.freshCount }} / {{ observation.readyCount }} {{ t('sessions.currentSamples') }}</small></span>
          <span class="observation-metric"><small>{{ t('sessions.downlinkCapacity') }}</small><strong>{{ observationCapacity(observation.downlink) }}</strong><small>{{ observation.downlink.freshCount }} / {{ observation.readyCount }} {{ t('sessions.currentSamples') }}</small></span>
          <span class="observation-metric"><small>{{ t('sessions.queue') }}</small><strong>{{ formatBytes(observation.queuedBytes) }} / {{ formatBytes(observation.inFlightBytes) }}</strong></span>
        </summary>
        <div v-if="observationExpanded(observation.key)" class="table-wrap lane-detail">
          <table class="sessions-table">
            <thead><tr>
              <th>{{ t('sessions.laneDetail') }}</th><th>{{ t('sessions.endpoints') }}</th><th>{{ t('sessions.state') }}</th>
              <th :title="t('sessions.smoothedRttTitle')">{{ t('sessions.smoothedRtt') }}</th><th :title="t('sessions.retryTitle')">{{ t('sessions.retry') }}</th><th :title="t('sessions.stallTitle')">{{ t('sessions.stall') }}</th><th :title="t('sessions.queueTitle')">{{ t('sessions.queue') }}</th><th>{{ client ? t('sessions.reconnect') : t('sessions.transport') }}</th>
            </tr></thead>
            <tbody>
              <tr v-for="session in observation.lanes" :key="session.id">
                <td :data-label="t('sessions.laneDetail')">
                  <div class="primary-cell">{{ session.lane ? t('sessions.lane', { lane: session.lane }) : t('sessions.session') }}<span v-if="session.fastest" class="fastest-badge">{{ t('sessions.fastest') }}</span></div>
                  <small class="mono">{{ t('common.local') }} {{ String(session.id || '').slice(0, 12) }}</small>
                  <div v-if="session.path_group_id" class="identifier-cell session-identifier"><button class="identifier-link mono" type="button" @click="$emit('filter', session.path_group_id)">{{ t('sessions.pathGroup') }} {{ String(session.path_group_id).slice(0, 12) }}</button></div>
                  <div class="identifier-cell session-identifier"><button class="identifier-link mono" type="button" @click="$emit('filter', session.connection_id)">{{ session.connection_id || '--' }}</button><CopyIdentifierButton :label="t('common.connectionId')" :value="session.connection_id" /></div>
                </td>
                <td :data-label="t('sessions.endpoints')"><div class="primary-cell mono">{{ session.local_endpoint || session.local_address || '--' }}</div><small class="mono">{{ session.remote_endpoint || '--' }}</small></td>
                <td :data-label="t('sessions.state')"><span class="state" :class="stateClass(session.state)">{{ sessionStates[session.state] ? t(sessionStates[session.state]) : t('common.unknown') }}</span></td>
                <td :data-label="t('sessions.smoothedRtt')">{{ formatMicros(session.quality?.smoothed_rtt_micros) }}</td>
                <td :data-label="t('sessions.retry')">{{ formatMicros(session.quality?.retry_micros) }}</td>
                <td :data-label="t('sessions.stall')">{{ formatMicros(session.quality?.stall_penalty_micros) }}</td>
                <td :data-label="t('sessions.queue')">
                  <div class="metric-lines session-quality-lines">
                    <span><small>{{ t('sessions.uplinkCapacity') }}</small>{{ displayCapacity(session.capacityDirections.uplink) }} <strong class="data-sample-state" :class="capacitySampleClass(session.capacityDirections.uplink)">{{ capacitySampleText(session.capacityDirections.uplink) }}</strong></span>
                    <span><small>{{ t('sessions.downlinkCapacity') }}</small>{{ displayCapacity(session.capacityDirections.downlink) }} <strong class="data-sample-state" :class="capacitySampleClass(session.capacityDirections.downlink)">{{ capacitySampleText(session.capacityDirections.downlink) }}</strong></span>
                    <span><small>{{ t('sessions.localSendQueue') }}</small>{{ formatBytes(session.quality?.queued_bytes) }} / {{ formatBytes(session.quality?.in_flight_bytes) }}</span>
                  </div>
                </td>
                <td :data-label="client ? t('sessions.reconnect') : t('sessions.transport')">{{ client ? String(session.reconnects || 0) : (session.transport || '--') }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </details>
    </div>
  </section>
</template>