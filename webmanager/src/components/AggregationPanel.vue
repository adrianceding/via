<script setup>
import { computed } from 'vue';
import { useI18n } from 'vue-i18n';

import { directionalAggregation, summarizeAggregation } from '../aggregation.js';
import { formatMicros, formatPercent, formatRate } from '../format.js';

const props = defineProps({
  sessions: { type: Array, required: true },
  total: { type: Number, default: 0 },
  filtered: { type: Boolean, default: false },
  loading: { type: Boolean, default: false },
  role: { type: Number, default: 0 },
  snapshotAt: { type: String, default: '' },
  sort: { type: String, default: 'name' },
  truncated: { type: Boolean, default: false },
});

defineEmits(['update:sort']);
const { t } = useI18n();

const summary = computed(() => summarizeAggregation(props.sessions, props.sort, props.snapshotAt));
const directions = computed(() => directionalAggregation(summary.value, props.role));
function directionText(direction, key, params) {
  return t(`charts.aggregationDirections.${direction}.${key}`, params);
}
const scopeLabel = computed(() => t('charts.aggregationScope', {
  shown: props.sessions.length,
  total: props.total || props.sessions.length,
}));
function sampleStateLabel(direction, name) {
  if (direction.readyCount === 0) return t('charts.aggregationNoReady');
  if (direction.freshCount === 0) return directionText(name, 'noSample');
  return directionText(name, 'sampleScope', {
    fresh: direction.freshCount,
    ready: direction.readyCount,
  });
}

function formatRange(range, formatter) {
  if (range.min == null || range.average == null || range.max == null) return t('charts.aggregationPending');
  return [range.min, range.average, range.max].map(formatter).join(' / ');
}

function formatSampleAge(value) {
  const milliseconds = Number(value);
  if (!Number.isFinite(milliseconds) || milliseconds < 0) return t('charts.aggregationAgeUnknown');
  if (milliseconds < 1000) return `${Math.round(milliseconds)} ms`;
  return `${(milliseconds / 1000).toFixed(milliseconds < 10_000 ? 1 : 0)} s`;
}
</script>

<template>
  <section class="panel agg" aria-labelledby="aggregation-heading">
    <header class="panel-head">
      <div>
        <h2 id="aggregation-heading">{{ t('charts.aggregationTitle') }}</h2>
        <small class="panel-scope">{{ scopeLabel }}<template v-if="truncated"> · {{ t('charts.partialResults') }}</template></small>
      </div>
      <div class="section-actions">
        <label class="compact-select">{{ t('sessions.sort') }}
          <select :value="sort" @change="$emit('update:sort', $event.target.value)">
            <option value="name">{{ t('sessions.sortName') }}</option>
            <option value="rtt">{{ t('sessions.sortRtt') }}</option>
          </select>
        </label>
      </div>
    </header>
    <div v-if="summary.groups.length" class="agg-body">
      <article v-for="(direction, name) in directions" :key="name" class="agg-direction">
        <header class="agg-direction-head">
          <div><p class="eyebrow">{{ t(`charts.${name}Direction`) }}</p><h3>{{ t(`charts.${name}AggregationTitle`) }}</h3></div>
          <span class="badge" :class="direction.readyCount === 0 ? 'tone-neutral' : direction.freshCount === direction.readyCount ? 'tone-good' : 'tone-warning'">{{ sampleStateLabel(direction, name) }}</span>
        </header>
        <dl class="agg-stats">
          <div class="agg-stat"><dt>{{ directionText(name, 'currentTotal') }}</dt><dd>{{ direction.totalCapacity == null ? t('charts.aggregationPending') : formatRate(direction.totalCapacity) }}</dd><small>{{ directionText(name, 'totalNote') }}</small></div>
          <div class="agg-stat"><dt>{{ directionText(name, 'currentHighest') }}</dt><dd>{{ direction.highestCapacity == null ? t('charts.aggregationPending') : formatRate(direction.highestCapacity) }}</dd><small>{{ directionText(name, 'highestCapacityNote') }}</small></div>
          <div class="agg-stat"><dt>{{ directionText(name, 'lift') }}</dt><dd>{{ direction.lift == null ? t('charts.aggregationPending') : `+${direction.lift.toFixed(1)}%` }}</dd><small>{{ directionText(name, 'liftNote') }}</small></div>
          <div class="agg-stat agg-stat-reference"><dt>{{ directionText(name, 'lastMeasured') }}</dt><dd>{{ direction.lastTotalCapacity == null ? t('charts.aggregationPending') : formatRate(direction.lastTotalCapacity) }}</dd><small>{{ directionText(name, 'lastMeasuredNote') }}</small></div>
        </dl>
        <div class="agg-groups">
          <details v-for="group in direction.groups" :key="`${name}:${group.key}`" class="agg-group">
            <summary class="agg-group-summary">
              <span class="agg-group-name"><strong>{{ group.label }}</strong><small>{{ t(`charts.aggregationGroupType.${group.type}`) }} · {{ group.laneCount }} {{ t('charts.aggregationLanes') }}</small></span>
              <span class="agg-group-metrics">
                <span class="agg-group-metric"><small>{{ directionText(name, 'currentTotal') }}</small><strong>{{ group.totalCapacity == null ? t('charts.aggregationPending') : formatRate(group.totalCapacity) }}</strong></span>
                <span class="agg-group-metric"><small>{{ directionText(name, 'lastMeasured') }}</small><strong>{{ group.lastTotalCapacity == null ? t('charts.aggregationPending') : formatRate(group.lastTotalCapacity) }}</strong></span>
                <span class="agg-group-metric"><small>{{ t('charts.aggregationSamples') }}</small><strong>{{ group.freshCount }} / {{ group.readyCount }}</strong></span>
                <span class="agg-group-metric agg-group-range"><small>{{ directionText(name, 'rttRange') }}</small><strong>{{ formatRange(group.rttRange, formatMicros) }}</strong></span>
                <span class="agg-group-metric agg-group-range"><small>{{ directionText(name, 'currentRange') }}</small><strong>{{ formatRange(group.capacityRange, formatRate) }}</strong></span>
                <span class="agg-group-metric agg-group-range"><small>{{ directionText(name, 'referenceRange') }}</small><strong>{{ formatRange(group.referenceCapacityRange, formatRate) }}</strong></span>
              </span>
            </summary>
            <div class="agg-group-detail"><div class="agg-bars">
              <div v-for="row in group.lanes" :key="row.session.connection_id || row.session.id" class="bar-row">
                <label>{{ row.session.lane ? t('sessions.lane', { lane: row.session.lane }) : row.session.id || '--' }}</label>
                <div class="bar"><i v-if="row.capacity != null && group.highestCapacity > 0" class="bar-fill" :class="row.isHighestCapacity ? 'tone-good' : 'tone-info'" :style="{ width: formatPercent(row.capacity, group.highestCapacity) }" /></div>
                <em>{{ formatRate(row.capacity ?? row.lastCapacity) }}</em>
                <small v-if="row.sampleState === 'fresh'">{{ directionText(name, row.isHighestCapacity ? 'highestNote' : 'measuredNote', { rtt: formatMicros(row.rtt), stall: formatMicros(row.stall) }) }}</small>
                <small v-else-if="row.sampleState === 'stale'">{{ directionText(name, 'staleReferenceNote', { age: formatSampleAge(row.sampleAgeMs), rtt: formatMicros(row.rtt) }) }}</small>
                <small v-else>{{ directionText(name, 'waitingNote', { rtt: formatMicros(row.rtt) }) }}</small>
              </div>
            </div></div>
          </details>
        </div>
      </article>
      <p v-if="summary.hiddenGroupCount" class="agg-hidden">{{ t('charts.aggregationHiddenGroups', { count: summary.hiddenGroupCount }) }}</p>
    </div>
    <p v-else-if="loading" class="empty-block">{{ t('charts.aggregationLoading') }}</p>
    <p v-else-if="filtered" class="empty-block">{{ t('charts.aggregationNoMatch') }}</p>
    <p v-else-if="sessions.length === 0" class="empty-block">{{ t('charts.aggregationNoSample') }}</p>
    <p v-else class="empty-block">{{ t('charts.aggregationNoReady') }}</p>
  </section>
</template>
