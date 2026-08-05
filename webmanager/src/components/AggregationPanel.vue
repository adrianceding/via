<script setup>
import { computed } from 'vue';
import { useI18n } from 'vue-i18n';

import { formatMicros, formatPercent, formatRate } from '../format.js';

const props = defineProps({
  sessions: { type: Array, required: true },
});

const { t } = useI18n();

const ready = computed(() => props.sessions.filter((session) => session.state === 3));
const fastest = computed(() => ready.value.reduce((best, session) => {
  const capacity = Number(session.quality?.capacity_bytes_sec) || 0;
  return !best || capacity > (Number(best.quality?.capacity_bytes_sec) || 0) ? session : best;
}, null));
const rows = computed(() => ready.value
  .map((session) => {
    const capacity = Number(session.quality?.capacity_bytes_sec) || 0;
    return {
      session,
      capacity,
      rtt: Number(session.quality?.smoothed_rtt_micros) || 0,
      stall: Number(session.quality?.stall_penalty_micros) || 0,
    };
  })
  .sort((left, right) => right.capacity - left.capacity));
const totalCapacity = computed(() => rows.value.reduce((total, row) => total + row.capacity, 0));
const fastestCapacity = computed(() => fastest.value ? Number(fastest.value.quality?.capacity_bytes_sec) || 0 : 0);
const lift = computed(() => {
  if (fastestCapacity.value <= 0 || rows.value.length < 2) return null;
  return (totalCapacity.value - fastestCapacity.value) / fastestCapacity.value * 100;
});
const eligibleTotal = computed(() => rows.value.reduce((total, row) => total + (Number(row.session.quality?.eligible_acked_data_payload_bytes) || 0), 0));
</script>

<template>
  <section class="panel agg" aria-label="bandwidth aggregation">
    <header class="panel-head">
      <h2>{{ t('charts.aggregationTitle') }}</h2>
      <span class="badge" :class="rows.length > 1 ? 'tone-good' : 'tone-neutral'">{{ t('charts.aggregationBadge') }}</span>
    </header>
    <div v-if="rows.length" class="agg-body">
      <div class="agg-bars">
        <div v-for="(row, index) in rows" :key="row.session.connection_id || row.session.id" class="bar-row">
          <label>{{ row.session.interface || row.session.principal || '--' }}</label>
          <div class="bar">
            <i
              class="bar-fill"
              :class="index === 0 ? 'tone-good' : 'tone-info'"
              :style="{ width: formatPercent(row.capacity, totalCapacity) }"
            />
          </div>
          <em>{{ formatRate(row.capacity) }}</em>
          <small v-if="index === 0">{{ t('charts.aggregationFastest', { rtt: formatMicros(row.rtt) }) }}</small>
          <small v-else>{{ t('charts.aggregationNote', { rtt: formatMicros(row.rtt), stall: formatMicros(row.stall) }) }}</small>
        </div>
      </div>
      <dl class="agg-stats">
        <div class="agg-stat">
          <dt>{{ t('charts.aggregationTotal') }}</dt>
          <dd>{{ formatRate(totalCapacity) }}</dd>
          <small>{{ t('charts.aggregationTotalNote') }}</small>
        </div>
        <div class="agg-stat">
          <dt>{{ t('charts.aggregationFastestOnly') }}</dt>
          <dd>{{ formatRate(fastestCapacity) }}</dd>
          <small>{{ t('charts.aggregationFastestOnlyNote') }}</small>
        </div>
        <div class="agg-stat">
          <dt>{{ t('charts.aggregationLift') }}</dt>
          <dd>{{ lift == null ? '--' : `+${lift.toFixed(1)}%` }}</dd>
          <small>{{ t('charts.aggregationLiftNote') }}</small>
        </div>
        <div class="agg-stat">
          <dt>{{ t('charts.aggregationShare') }}</dt>
          <dd>{{ formatPercent(fastest?.quality?.eligible_acked_data_payload_bytes, eligibleTotal) }}</dd>
          <small>{{ t('charts.aggregationShareNote') }}</small>
        </div>
      </dl>
    </div>
    <p v-else class="empty-block">{{ t('charts.aggregationEmpty') }}</p>
  </section>
</template>
