<script setup>
import { computed } from 'vue';
import { useI18n } from 'vue-i18n';

import { formatMicros } from '../format.js';
import { fastestEndpointValues } from '../focus.js';
import { selectFastestSession } from '../quality.js';

const props = defineProps({
  sessions: { type: Array, required: true },
  summary: { type: Object, required: true },
});
const { t } = useI18n();

const fastest = computed(() => selectFastestSession(props.sessions));
const readySessions = computed(() => props.sessions.filter((session) => session.state === 3).length);
const resources = computed(() => props.summary.resources || {});
const counters = computed(() => props.summary.counters || {});
const endpoints = computed(() => fastestEndpointValues(fastest.value));
</script>

<template>
  <section class="focus-grid">
    <article class="focus-panel">
      <header class="section-heading">
        <div><p class="eyebrow">{{ t('focus.livePath') }}</p><h2>{{ t('focus.fastest') }}</h2></div>
        <span class="state" :class="fastest ? 'good' : 'neutral'">{{ fastest ? t('focus.fastest') : t('focus.waiting') }}</span>
      </header>
      <div v-if="fastest" class="fastest-content">
        <strong class="fastest-name">{{ fastest.interface || fastest.principal || 'listener' }}</strong>
        <strong class="fastest-rtt">{{ formatMicros(fastest.quality?.smoothed_rtt_micros) }}</strong>
        <p class="fastest-meta">
          <span class="mono">{{ t('focus.localEndpoint', { value: endpoints.local }) }}</span>
          <span class="mono">{{ t('focus.remoteEndpoint', { value: endpoints.remote }) }}</span>
          <span>{{ fastest.transport || '--' }}</span>
          <span>{{ t('focus.retry', { value: formatMicros(fastest.quality?.retry_micros) }) }}</span>
        </p>
      </div>
      <p v-else class="empty-block">{{ t('focus.noPath') }}</p>
    </article>
    <article class="focus-panel">
      <header class="section-heading">
        <div><p class="eyebrow">{{ t('focus.quality') }}</p><h2>{{ t('focus.runtime') }}</h2></div>
      </header>
      <dl class="quality-summary">
        <div><dt>{{ t('focus.readySessions') }}</dt><dd>{{ readySessions }}</dd></div>
        <div><dt>{{ t('focus.recoveringFlows') }}</dt><dd>{{ resources.recovering_flows || 0 }}</dd></div>
        <div><dt>{{ t('focus.droppedEvents') }}</dt><dd>{{ counters.dropped_status_events || 0 }}</dd></div>
      </dl>
    </article>
  </section>
</template>
