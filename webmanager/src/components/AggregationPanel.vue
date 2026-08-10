<script setup>
import { computed } from 'vue';
import { useI18n } from 'vue-i18n';

import { summarizeAggregation } from '../aggregation.js';
import { formatMicros, formatPercent, formatRate } from '../format.js';

const props = defineProps({
  sessions: { type: Array, required: true },
});

const { t } = useI18n();

const summary = computed(() => summarizeAggregation(props.sessions));
</script>

<template>
  <section class="panel agg" aria-label="bandwidth aggregation">
    <header class="panel-head">
      <h2>{{ t('charts.aggregationTitle') }}</h2>
      <span
        class="badge"
        :class="summary.readyCount === 0 ? 'tone-neutral' : summary.freshCount === summary.readyCount ? 'tone-good' : 'tone-warning'"
      >{{ t('charts.aggregationBadge', { fresh: summary.freshCount, ready: summary.readyCount }) }}</span>
    </header>
    <div v-if="summary.rows.length" class="agg-body">
      <div class="agg-bars">
        <div v-for="row in summary.rows" :key="row.session.connection_id || row.session.id" class="bar-row">
          <label>{{ row.session.interface || row.session.principal || '--' }}</label>
          <div class="bar">
            <i
              v-if="row.capacity != null && summary.highestCapacity > 0"
              class="bar-fill"
              :class="row.isHighestCapacity ? 'tone-good' : 'tone-info'"
              :style="{ width: formatPercent(row.capacity, summary.highestCapacity) }"
            />
          </div>
          <em>{{ formatRate(row.capacity) }}</em>
          <small v-if="row.sampleState === 'fresh'">
            {{ t(row.isHighestCapacity ? 'charts.aggregationHighestNote' : 'charts.aggregationMeasuredNote', { rtt: formatMicros(row.rtt), stall: formatMicros(row.stall) }) }}
          </small>
          <small v-else-if="row.sampleState === 'stale'">{{ t('charts.aggregationStaleNote', { rtt: formatMicros(row.rtt) }) }}</small>
          <small v-else>{{ t('charts.aggregationWaitingNote', { rtt: formatMicros(row.rtt) }) }}</small>
        </div>
      </div>
      <dl class="agg-stats">
        <div class="agg-stat">
          <dt>{{ t('charts.aggregationTotal') }}</dt>
          <dd>{{ formatRate(summary.totalCapacity) }}</dd>
          <small>{{ t('charts.aggregationTotalNote') }}</small>
        </div>
        <div class="agg-stat">
          <dt>{{ t('charts.aggregationHighest') }}</dt>
          <dd>{{ formatRate(summary.highestCapacity) }}</dd>
          <small>{{ t('charts.aggregationHighestCapacityNote') }}</small>
        </div>
        <div class="agg-stat">
          <dt>{{ t('charts.aggregationLift') }}</dt>
          <dd>{{ summary.lift == null ? '--' : `+${summary.lift.toFixed(1)}%` }}</dd>
          <small>{{ t('charts.aggregationLiftNote') }}</small>
        </div>
        <div class="agg-stat">
          <dt>{{ t('charts.aggregationShare') }}</dt>
          <dd>{{ summary.highestShare == null ? '--' : formatPercent(summary.highestShare, 100) }}</dd>
          <small>{{ t('charts.aggregationShareNote') }}</small>
        </div>
      </dl>
    </div>
    <p v-else class="empty-block">{{ t('charts.aggregationEmpty') }}</p>
  </section>
</template>
