<script setup>
import { computed, defineAsyncComponent, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { useI18n } from 'vue-i18n';

import ConnectionFocus from './components/ConnectionFocus.vue';
import EmptyTrend from './components/EmptyTrend.vue';
import FilterBar from './components/FilterBar.vue';
import FlowSection from './components/FlowSection.vue';
import InterfaceList from './components/InterfaceList.vue';
import OverviewMetrics from './components/OverviewMetrics.vue';
import RuntimeHeader from './components/RuntimeHeader.vue';
import SessionTable from './components/SessionTable.vue';
import {
  flowMatchesFilter,
  interfaceMatchesFilter,
  isFlowAbnormal,
  isSessionAbnormal,
  isTerminalAbnormal,
  sessionMatchesFilter,
  terminalMatchesFilter,
} from './filters.js';
import { statusErrorCode } from './api.js';
import { persistLocale } from './i18n.js';
import { describeFreshness } from './observability.js';
import { createPoller } from './poller.js';
import { createStatusController } from './status-controller.js';
import { roles } from './status.js';
import { buildViewURL, parseViewState, serializeViewState } from './view-state.js';

const controller = createStatusController();
const { locale, t } = useI18n();
const poller = createPoller(controller.refresh);
const initialView = parseViewState(window.location.search);
const query = ref(initialView.query);
const onlyAnomalies = ref(initialView.onlyAnomalies);
const flowTab = ref(initialView.flowTab);
const sessionSort = ref(initialView.sessionSort);
const paused = ref(false);
const currentTime = ref(Date.now());
let ageTimer = null;
const SessionTrend = defineAsyncComponent(() => import('./components/SessionTrend.vue'));

const freshness = computed(() => describeFreshness(controller.lastSuccessAt.value, currentTime.value, paused.value));
const freshnessLabel = computed(() => t(`freshness.${freshness.value.state}`, { count: freshness.value.ageSeconds }));
const normalizedQuery = computed(() => query.value.trim().toLowerCase());
const visibleSessions = computed(() => controller.snapshot.value.sessions.filter((session) => sessionMatchesFilter(session, normalizedQuery.value, onlyAnomalies.value)));
const visibleFlows = computed(() => controller.snapshot.value.flows.filter((flow) => flowMatchesFilter(flow, normalizedQuery.value, onlyAnomalies.value)));
const visibleTerminals = computed(() => controller.snapshot.value.terminals.filter((flow) => terminalMatchesFilter(flow, normalizedQuery.value, onlyAnomalies.value)));
const visibleInterfaces = computed(() => controller.snapshot.value.interfaces.filter((item) => interfaceMatchesFilter(item, normalizedQuery.value, onlyAnomalies.value)));
const visibleSessionIDs = computed(() => new Set(visibleSessions.value.map((session) => session.connection_id || session.id)));
const visibleTrends = computed(() => controller.trends.value.filter((trend) => visibleSessionIDs.value.has(trend.id)
  && (!normalizedQuery.value || `${trend.label} ${trend.id} ${trend.localEndpoint} ${trend.remoteEndpoint}`.toLowerCase().includes(normalizedQuery.value))));
const anomalyCount = computed(() => controller.snapshot.value.sessions.filter(isSessionAbnormal).length
  + controller.snapshot.value.flows.filter(isFlowAbnormal).length
  + controller.snapshot.value.terminals.filter(isTerminalAbnormal).length
  + controller.snapshot.value.interfaces.filter((item) => item.reason !== 1).length);
const currentViewURL = computed(() => buildViewURL(window.location.href, {
  query: query.value,
  onlyAnomalies: onlyAnomalies.value,
  flowTab: flowTab.value,
  sessionSort: sessionSort.value,
}));
const healthLevel = computed(() => {
  if (controller.error.value || freshness.value.stale && !paused.value) return 'error';
  return controller.health.value.level;
});
const connectionLabel = computed(() => controller.error.value
  ? t(`errors.${statusErrorCode(controller.error.value)}`)
  : t(`status.${controller.health.value.level}`));
const healthReasons = computed(() => controller.health.value.reasons.map((reason) => t(`health.${reason.code}`, { count: reason.count })));
const runtimeTitle = computed(() => t('header.title', {
  role: t(roles[controller.snapshot.value.summary.role] || 'status.role.runtime'),
}));

function refreshNow() {
  void poller.refreshNow();
}

function togglePause() {
  paused.value = !paused.value;
  if (paused.value) poller.pause();
  else void poller.resume();
}

function clearFilters() {
  query.value = '';
  onlyAnomalies.value = false;
}

function filterBy(value) {
  query.value = String(value || '');
}

function changeLocale(nextLocale) {
  locale.value = nextLocale;
  persistLocale(nextLocale, window.localStorage);
  document.documentElement.lang = nextLocale;
  document.title = t('app.title');
}

watch([query, onlyAnomalies, flowTab, sessionSort], () => {
  const search = serializeViewState({
    query: query.value,
    onlyAnomalies: onlyAnomalies.value,
    flowTab: flowTab.value,
    sessionSort: sessionSort.value,
  });
  window.history.replaceState(null, '', `${window.location.pathname}${search}${window.location.hash}`);
});

onMounted(() => {
  void poller.start();
  ageTimer = setInterval(() => { currentTime.value = Date.now(); }, 1000);
});
onBeforeUnmount(() => {
  poller.stop();
  clearInterval(ageTimer);
});
</script>

<template>
  <RuntimeHeader
    :connection-label="connectionLabel"
    :freshness-label="freshnessLabel"
    :generated-at="controller.snapshot.value.summary.generated_at"
    :health-level="healthLevel"
    :health-reasons="healthReasons"
    :locale="locale"
    :paused="paused"
    :refreshing="controller.refreshing.value"
    :role="controller.snapshot.value.summary.role"
    :title="runtimeTitle"
    @change-locale="changeLocale"
    @refresh="refreshNow"
    @toggle-pause="togglePause"
  />
  <main class="manager-main">
    <OverviewMetrics
      :rates="controller.rates.value"
      :sessions="controller.snapshot.value.sessions"
      :summary="controller.snapshot.value.summary"
    />
    <FilterBar
      v-model:only-anomalies="onlyAnomalies"
      v-model:query="query"
      :anomaly-count="anomalyCount"
      :share-url="currentViewURL"
      :truncated="controller.snapshot.value.truncated"
      @clear="clearFilters"
    />
    <ConnectionFocus
      :sessions="visibleSessions"
      :summary="controller.snapshot.value.summary"
    />
    <SessionTrend v-if="visibleTrends.length" :trends="visibleTrends" />
    <EmptyTrend v-else />
    <SessionTable
      v-model:sort="sessionSort"
      :only-anomalies="false"
      query=""
      :role="controller.snapshot.value.summary.role"
      :sessions="visibleSessions"
      :total="controller.snapshot.value.sessionTotal"
      @filter="filterBy"
    />
    <FlowSection
      v-model:active-tab="flowTab"
      :flows="visibleFlows"
      :flow-total="controller.snapshot.value.flowTotal"
      :terminals="visibleTerminals"
      :terminal-total="controller.snapshot.value.terminalTotal"
      @filter="filterBy"
    />
    <InterfaceList
      :filtered="Boolean(normalizedQuery) || onlyAnomalies"
      :interfaces="visibleInterfaces"
      :sessions="visibleSessions"
      :total="controller.snapshot.value.interfaces.length"
      @filter="filterBy"
    />
  </main>
</template>