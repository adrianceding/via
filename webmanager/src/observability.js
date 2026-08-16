const STALE_AFTER_MS = 6000;

function counter(summary, name) {
  const raw = summary?.counters?.[name];
  if (typeof raw !== 'number' || !Number.isFinite(raw) || raw < 0) return null;
  return raw;
}

function rejection(rejected, name) {
  const value = Number(rejected?.[name]);
  return Number.isFinite(value) && value >= 0 ? value : 0;
}

export function flowRejectionNoteParams(rejected) {
  return {
    count: rejection(rejected, 'flows'),
    rate: rejection(rejected, 'flow_rate_limited'),
    opening: rejection(rejected, 'flow_opening_capacity'),
    target: rejection(rejected, 'flow_target_dial_capacity'),
  };
}

export function calculateRates(previous, next) {
  const previousAt = Date.parse(previous?.generated_at || '');
  const nextAt = Date.parse(next?.generated_at || '');
  const elapsedSeconds = (nextAt - previousAt) / 1000;
  const previousSent = counter(previous, 'data_payload_bytes_sent');
  const nextSent = counter(next, 'data_payload_bytes_sent');
  const previousReceived = counter(previous, 'data_payload_bytes_received');
  const nextReceived = counter(next, 'data_payload_bytes_received');
  if (previousSent == null || nextSent == null || previousReceived == null || nextReceived == null) {
    return { sent: null, received: null };
  }
  const sentDelta = nextSent - previousSent;
  const receivedDelta = nextReceived - previousReceived;
  if (!Number.isFinite(elapsedSeconds) || elapsedSeconds <= 0 || sentDelta < 0 || receivedDelta < 0) {
    return { sent: null, received: null };
  }
  return { sent: sentDelta / elapsedSeconds, received: receivedDelta / elapsedSeconds };
}

export function evaluateHealth(snapshot, droppedDelta = 0) {
  if (!snapshot.summary?.healthy) return { level: 'unhealthy', reasons: [] };
  const summary = snapshot.summary || {};
  const sessions = Array.isArray(snapshot.sessions) ? snapshot.sessions : [];
  const summarySessions = Number.isSafeInteger(summary.sessions) && summary.sessions >= 0
    ? summary.sessions
    : null;
  const summaryReadySessions = Number.isSafeInteger(summary.ready_sessions) && summary.ready_sessions >= 0
    ? summary.ready_sessions
    : null;
  const hasCompleteCounts = summarySessions !== null
    && summaryReadySessions !== null
    && summaryReadySessions <= summarySessions;
  const listedReadySessions = sessions.filter((session) => session.state === 3).length;
  const sessionCounts = hasCompleteCounts
    ? {
      total: summarySessions,
      ready: summaryReadySessions,
      unavailable: Math.max(0, summarySessions - summaryReadySessions),
    }
    : {
      total: sessions.length,
      ready: listedReadySessions,
      unavailable: sessions.length - listedReadySessions,
    };

  const reasons = [];
  const transportUnavailable = summary.role === 1 && sessionCounts.ready === 0;
  if (transportUnavailable) {
    reasons.push(sessionCounts.total === 0
      ? { code: 'noSessions' }
      : { code: 'noReadySessions', count: sessionCounts.total });
  }
  const recovering = Number(summary.resources?.recovering_flows || 0);
  const unavailableSessions = sessionCounts.unavailable;
  if (recovering > 0) reasons.push({ code: 'recoveringFlows', count: recovering });
  if (!transportUnavailable && unavailableSessions > 0) {
    reasons.push({ code: 'unavailableSessions', count: unavailableSessions });
  }
  if (droppedDelta > 0) reasons.push({ code: 'droppedEvents', count: droppedDelta });
  return reasons.length > 0
    ? { level: transportUnavailable ? 'unhealthy' : 'degraded', reasons }
    : { level: 'healthy', reasons: [] };
}

export function healthStatusKey(health) {
  if (health?.reasons?.some((reason) => reason.code === 'noSessions' || reason.code === 'noReadySessions')) {
    return 'transportUnavailable';
  }
  return health?.level || 'healthy';
}

export function resolveStatusPresentation({ error, freshnessState, health }) {
  if (error) return { level: 'error', key: null };
  if (freshnessState === 'unavailable') return { level: 'connecting', key: 'connecting' };
  if (freshnessState === 'stale') return { level: 'unhealthy', key: 'stale' };
  const level = health?.level || 'healthy';
  return { level, key: healthStatusKey(health) };
}

export function describeFreshness(lastSuccessAt, now, paused) {
  if (!Number.isFinite(lastSuccessAt)) return { stale: true, state: 'unavailable', ageSeconds: 0 };
  const ageSeconds = Math.max(0, Math.floor((now - lastSuccessAt) / 1000));
  if (paused) return { stale: ageSeconds * 1000 > STALE_AFTER_MS, state: 'paused', ageSeconds };
  if (ageSeconds * 1000 > STALE_AFTER_MS) return { stale: true, state: 'stale', ageSeconds };
  return { stale: false, state: 'fresh', ageSeconds };
}
