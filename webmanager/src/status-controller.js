import { ref, shallowRef } from 'vue';

import { fetchStatusSnapshot } from './api.js';
import { calculateRates, evaluateHealth } from './observability.js';
import { normalizeSnapshot } from './snapshot.js';
import { createSessionTrendStore } from './trends.js';

const emptySnapshot = normalizeSnapshot({
  summary: {},
  interfaces: {},
  sessions: {},
  flows: {},
});

export function createStatusController({
  fetchSnapshot = fetchStatusSnapshot,
  trendStore = createSessionTrendStore(),
  now = Date.now,
} = {}) {
  const snapshot = shallowRef(emptySnapshot);
  const trends = shallowRef([]);
  const aggregateTrend = shallowRef(null);
  const error = shallowRef(null);
  const health = shallowRef({ level: 'connecting', reasons: [] });
  const rates = shallowRef({ sent: null, received: null });
  const lastSuccessAt = ref(null);
  const refreshing = ref(false);

  async function refresh() {
    refreshing.value = true;
    try {
      const nextSnapshot = normalizeSnapshot(await fetchSnapshot());
      const previousSummary = snapshot.value.summary;
      const previousDrops = Number(previousSummary.counters?.dropped_status_events || 0);
      const nextDrops = Number(nextSnapshot.summary.counters?.dropped_status_events || 0);
      const droppedDelta = lastSuccessAt.value === null ? 0 : Math.max(0, nextDrops - previousDrops);
      trendStore.update(nextSnapshot.sessions, nextSnapshot.sessionsGeneratedAt);
      const nextTrends = trendStore.snapshot();
      const nextAggregateTrend = trendStore.aggregateSnapshot ? trendStore.aggregateSnapshot() : null;
      const nextRates = lastSuccessAt.value === null
        ? { sent: null, received: null }
        : calculateRates(previousSummary, nextSnapshot.summary);
      const nextHealth = evaluateHealth(nextSnapshot, droppedDelta);
      snapshot.value = nextSnapshot;
      trends.value = nextTrends;
      aggregateTrend.value = nextAggregateTrend;
      rates.value = nextRates;
      health.value = nextHealth;
      lastSuccessAt.value = now();
      error.value = null;
    } catch (refreshError) {
      error.value = refreshError;
    } finally {
      refreshing.value = false;
    }
  }

  return { snapshot, trends, aggregateTrend, error, health, rates, lastSuccessAt, refreshing, refresh };
}