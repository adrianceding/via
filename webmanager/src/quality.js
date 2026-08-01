export function selectFastestSession(sessions) {
  const marked = sessions.find((session) => session.fastest && session.state === 3);
  if (marked) return marked;
  return sessions
    .filter((session) => session.state === 3 && Number(session.quality?.smoothed_rtt_micros) > 0)
    .sort((left, right) => left.quality.smoothed_rtt_micros - right.quality.smoothed_rtt_micros)[0] || null;
}

export function sortSessions(sessions, mode) {
  const sourceOrder = (left, right) => String(left.interface || left.principal || '')
    .localeCompare(String(right.interface || right.principal || ''))
    || String(left.connection_id || left.id || '').localeCompare(String(right.connection_id || right.id || ''));
  return [...sessions].sort((left, right) => {
    if (mode === 'rtt') {
      const leftRTT = Number(left.quality?.smoothed_rtt_micros) || Number.POSITIVE_INFINITY;
      const rightRTT = Number(right.quality?.smoothed_rtt_micros) || Number.POSITIVE_INFINITY;
      return leftRTT - rightRTT || sourceOrder(left, right);
    }
    if (mode === 'state') {
      const leftReady = left.state === 3 ? 1 : 0;
      const rightReady = right.state === 3 ? 1 : 0;
      return leftReady - rightReady || Number(left.state || 0) - Number(right.state || 0) || sourceOrder(left, right);
    }
    return sourceOrder(left, right);
  });
}