const MAX_TREND_SAMPLES = 120;
const MAX_TREND_SERIES = 12;

export function createSessionTrendStore() {
  const trends = new Map();

  return {
    update(sessions) {
      const activeIDs = new Set();
      sessions.forEach((session) => {
        const id = session.connection_id || session.id;
        if (id) activeIDs.add(id);
      });
      const selectedIDs = new Set(
        [...trends.keys()].filter((id) => activeIDs.has(id)).slice(0, MAX_TREND_SERIES),
      );
      sessions.forEach((session) => {
        const id = session.connection_id || session.id;
        if (id && selectedIDs.size < MAX_TREND_SERIES) selectedIDs.add(id);
      });
      trends.forEach((_trend, id) => {
        if (!selectedIDs.has(id)) trends.delete(id);
      });

      const sampledIDs = new Set();
      sessions.forEach((session) => {
        const id = session.connection_id || session.id;
        if (!selectedIDs.has(id) || sampledIDs.has(id)) return;
        sampledIDs.add(id);
        const rtt = Number(session.quality?.smoothed_rtt_micros);
        if (!Number.isFinite(rtt) || rtt <= 0) return;
        const existing = trends.get(id);
        const samples = [...(existing?.samples || []), rtt].slice(-MAX_TREND_SAMPLES);
        trends.set(id, {
          id,
          label: session.interface || session.principal || '',
          localEndpoint: session.local_endpoint || session.local_address || '',
          remoteEndpoint: session.remote_endpoint || '',
          samples,
        });
      });
    },
    snapshot() {
      return [...trends.values()].map((trend) => ({ ...trend, samples: [...trend.samples] }));
    },
  };
}