<script setup>
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { useI18n } from 'vue-i18n';
import { computeActiveSection, computeNavigateTop } from './app-nav.js';

import AggregationPanel from './components/AggregationPanel.vue';
import ConnectionFocus from './components/ConnectionFocus.vue';
import FilterBar from './components/FilterBar.vue';
import FlowSection from './components/FlowSection.vue';
import InterfaceList from './components/InterfaceList.vue';
import LimitsPanel from './components/LimitsPanel.vue';
import OverviewMetrics from './components/OverviewMetrics.vue';
import RecoveryPanel from './components/RecoveryPanel.vue';
import RuntimeHeader from './components/RuntimeHeader.vue';
import SessionTable from './components/SessionTable.vue';
import TrafficCharts from './components/TrafficCharts.vue';
import {
  flowMatchesFilter,
  interfaceMatchesFilter,
  isFlowAbnormal,
  isSessionAbnormal,
  isTerminalAbnormal,
  sessionMatchesFilter,
  terminalMatchesFilter,
} from './filters.js';
import { aggregationGroupIdentity } from './aggregation.js';
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
const activeSection = ref('overview');
let ageTimer = null;

const freshness = computed(() => describeFreshness(controller.lastSuccessAt.value, currentTime.value, paused.value));
const freshnessLabel = computed(() => t(`freshness.${freshness.value.state}`, { count: freshness.value.ageSeconds }));
const initialLoading = computed(() => controller.lastSuccessAt.value === null);
const normalizedQuery = computed(() => query.value.trim().toLowerCase());
const visibleSessions = computed(() => controller.snapshot.value.sessions.filter((session) => sessionMatchesFilter(session, normalizedQuery.value, onlyAnomalies.value)));
const visibleFlows = computed(() => controller.snapshot.value.flows.filter((flow) => flowMatchesFilter(flow, normalizedQuery.value, onlyAnomalies.value)));
const visibleTerminals = computed(() => controller.snapshot.value.terminals.filter((flow) => terminalMatchesFilter(flow, normalizedQuery.value, onlyAnomalies.value)));
const visibleInterfaces = computed(() => controller.snapshot.value.interfaces.filter((item) => interfaceMatchesFilter(item, normalizedQuery.value, onlyAnomalies.value)));
const visibleObservationIDs = computed(() => new Set(visibleSessions.value.map((session, index) => {
  const identity = aggregationGroupIdentity(session, index);
  return `${identity.type}:${identity.value}`;
})));
const visibleTrends = computed(() => controller.trends.value.filter((trend) => visibleObservationIDs.value.has(trend.id)));
const trendTotal = computed(() => normalizedQuery.value || onlyAnomalies.value
  ? visibleObservationIDs.value.size
  : new Set(controller.snapshot.value.sessions.map((session, index) => {
    const identity = aggregationGroupIdentity(session, index);
    return `${identity.type}:${identity.value}`;
  })).size);
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
const healthReasons = computed(() => controller.health.value.reasons.map((reason) => t(`health.${reason.code}`, { count: reason.count })));
const statusPresentation = computed(() => {
  if (controller.error.value) {
    return { level: 'error', label: t(`errors.${statusErrorCode(controller.error.value)}`) };
  }
  if (freshness.value.state === 'unavailable') {
    return { level: 'connecting', label: t('status.connecting') };
  }
  if (freshness.value.state === 'stale') {
    return { level: 'unhealthy', label: t('status.stale') };
  }
  const level = controller.health.value.level;
  return { level, label: t(`status.${level}`) };
});
const healthLevel = computed(() => statusPresentation.value.level);
const connectionLabel = computed(() => statusPresentation.value.label);
const runtimeTitle = computed(() => t('header.title', {
  role: t(roles[controller.snapshot.value.summary.role] || 'status.role.runtime'),
}));
const navSections = computed(() => [
  { id: 'overview', label: t('nav.overview') },
  { id: 'bandwidth', label: t('nav.bandwidth') },
  { id: 'interfaces', label: t('nav.interfaces') },
  { id: 'flows', label: t('nav.flows') },
  { id: 'limits', label: t('nav.limits') },
]);
const roleLabel = computed(() => t(roles[controller.snapshot.value.summary.role] || 'status.role.runtime'));

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

function updateActiveSection() {
  const sections = navSections.value
    .map((section) => {
      const el = document.getElementById(section.id);
      return el ? { id: section.id, top: el.getBoundingClientRect().top } : null;
    })
    .filter(Boolean);
  activeSection.value = computeActiveSection(sections, {
    scrollY: window.scrollY,
    innerHeight: window.innerHeight,
    scrollHeight: document.documentElement.scrollHeight,
  });
}

function navigateTo(sectionID) {
  const target = document.getElementById(sectionID);
  if (!target) return;
  window.scrollTo({
    top: computeNavigateTop(target.getBoundingClientRect().top, window.scrollY),
    behavior: 'smooth',
  });
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
  window.addEventListener('scroll', updateActiveSection, { passive: true });
  updateActiveSection();
});
onBeforeUnmount(() => {
  poller.stop();
  clearInterval(ageTimer);
  window.removeEventListener('scroll', updateActiveSection);
});
</script>

<template>
  <div class="app">
    <aside class="sidebar">
      <div class="side-brand">
        <span class="side-logo">V</span>
        <span class="side-name">Via</span>
      </div>
      <nav class="side-nav">
        <a
          v-for="section in navSections"
          :key="section.id"
          class="side-link"
          :class="{ active: activeSection === section.id }"
          :href="`#${section.id}`"
          @click.prevent="navigateTo(section.id)"
        >{{ section.label }}</a>
      </nav>
      <div class="side-foot">
        <div class="side-role">{{ roleLabel }} · tcp</div>
        <div class="side-health">
          <span class="dot" :class="healthLevel" />
          <span>{{ connectionLabel }}</span>
        </div>
      </div>
    </aside>
    <div class="shell">
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
        <section id="overview" class="main-section">
          <OverviewMetrics
            :active-flows="controller.snapshot.value.flowTotal"
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
        </section>
        <section id="bandwidth" class="main-section">
          <AggregationPanel
            v-model:sort="sessionSort"
            :filtered="Boolean(normalizedQuery) || onlyAnomalies"
            :loading="initialLoading"
            :role="controller.snapshot.value.summary.role"
            :sessions="visibleSessions"
            :snapshot-at="controller.snapshot.value.sessionsGeneratedAt"
            :total="controller.snapshot.value.sessionTotal"
            :truncated="controller.snapshot.value.truncated"
          />
          <TrafficCharts
            :role="controller.snapshot.value.summary.role"
            :sessions="visibleSessions"
            :total="trendTotal"
            :trends="visibleTrends"
          />
        </section>
        <section id="interfaces" class="main-section">
          <SessionTable
            v-model:sort="sessionSort"
            :filtered="Boolean(normalizedQuery) || onlyAnomalies"
            :only-anomalies="false"
            query=""
            :role="controller.snapshot.value.summary.role"
            :sessions="visibleSessions"
            :snapshot-at="controller.snapshot.value.sessionsGeneratedAt"
            :total="controller.snapshot.value.sessionTotal"
            @filter="filterBy"
          />
          <InterfaceList
            :filtered="Boolean(normalizedQuery) || onlyAnomalies"
            :interfaces="visibleInterfaces"
            :sessions="visibleSessions"
            :total="controller.snapshot.value.interfaces.length"
            @filter="filterBy"
          />
        </section>
        <section id="flows" class="main-section">
          <FlowSection
            v-model:active-tab="flowTab"
            :flows="visibleFlows"
            :flow-total="controller.snapshot.value.flowTotal"
            :terminals="visibleTerminals"
            :terminal-total="controller.snapshot.value.terminalTotal"
            @filter="filterBy"
          />
        </section>
        <section id="limits" class="main-section">
          <div class="panel-grid">
            <LimitsPanel :summary="controller.snapshot.value.summary" />
            <RecoveryPanel
              :flows="visibleFlows"
              :sessions="visibleSessions"
              :terminals="visibleTerminals"
            />
          </div>
        </section>
      </main>
    </div>
  </div>
</template>