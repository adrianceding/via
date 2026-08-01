const STALE_AFTER_MS = 6000;

function counter(summary, name) {
  const value = Number(summary?.counters?.[name]);
  return Number.isFinite(value) && value >= 0 ? value : 0;
}

export function calculateRates(previous, next) {
  const previousAt = Date.parse(previous?.generated_at || '');
  const nextAt = Date.parse(next?.generated_at || '');
  const elapsedSeconds = (nextAt - previousAt) / 1000;
  const sentDelta = counter(next, 'bytes_sent') - counter(previous, 'bytes_sent');
  const receivedDelta = counter(next, 'bytes_received') - counter(previous, 'bytes_received');
  if (!Number.isFinite(elapsedSeconds) || elapsedSeconds <= 0 || sentDelta < 0 || receivedDelta < 0) {
    return { sent: null, received: null };
  }
  return { sent: sentDelta / elapsedSeconds, received: receivedDelta / elapsedSeconds };
}

export function evaluateHealth(snapshot, droppedDelta = 0) {
  if (!snapshot.summary?.healthy) return { level: 'unhealthy', reasons: [] };
  const reasons = [];
  const recovering = Number(snapshot.summary.resources?.recovering_flows || 0);
  const unavailableSessions = snapshot.sessions.filter((session) => session.state !== 3).length;
  if (recovering > 0) reasons.push({ code: 'recoveringFlows', count: recovering });
  if (unavailableSessions > 0) reasons.push({ code: 'unavailableSessions', count: unavailableSessions });
  if (droppedDelta > 0) reasons.push({ code: 'droppedEvents', count: droppedDelta });
  return reasons.length > 0
    ? { level: 'degraded', reasons }
    : { level: 'healthy', reasons: [] };
}

export function describeFreshness(lastSuccessAt, now, paused) {
  if (!Number.isFinite(lastSuccessAt)) return { stale: true, state: 'unavailable', ageSeconds: 0 };
  const ageSeconds = Math.max(0, Math.floor((now - lastSuccessAt) / 1000));
  if (paused) return { stale: ageSeconds * 1000 > STALE_AFTER_MS, state: 'paused', ageSeconds };
  if (ageSeconds * 1000 > STALE_AFTER_MS) return { stale: true, state: 'stale', ageSeconds };
  return { stale: false, state: 'fresh', ageSeconds };
}