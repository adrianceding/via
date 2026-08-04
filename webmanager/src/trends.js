const MAX_TREND_SAMPLES = 120;
const MAX_TREND_SERIES = 12;
const AGGREGATE_ID = '__aggregate__';

export function createSessionTrendStore() {
  const trends = new Map();
  const aggregate = { id: AGGREGATE_ID, label: '', samples: [], lastWritten: null, lastAt: null, memberIDs: [] };

  return {
    update(sessions, generatedAt) {
      const sampledAt = Date.parse(generatedAt || '');
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
        const existing = trends.get(id);
        const written = Number(session.quality?.written_data_payload_bytes);
        const previousWritten = existing?.lastWritten;
        const previousAt = existing?.lastAt;
        const elapsedSeconds = (sampledAt - previousAt) / 1000;
        const rate = Number.isFinite(written) && written >= 0 && Number.isFinite(previousWritten)
          && previousWritten >= 0 && Number.isFinite(elapsedSeconds) && elapsedSeconds > 0
          && written >= previousWritten ? (written - previousWritten) / elapsedSeconds : null;
        const samples = rate == null ? [...(existing?.samples || [])] : [...(existing?.samples || []), rate].slice(-MAX_TREND_SAMPLES);
        trends.set(id, {
          id,
          label: session.interface || session.principal || '',
          localEndpoint: session.local_endpoint || session.local_address || '',
          remoteEndpoint: session.remote_endpoint || '',
          samples,
          lastWritten: Number.isFinite(written) && written >= 0 ? written : null,
          lastAt: Number.isFinite(sampledAt) ? sampledAt : null,
        });
      });

      if (Number.isFinite(sampledAt)) {
        const currentIDs = [...activeIDs].sort();
        const membershipChanged = currentIDs.length !== aggregate.memberIDs.length
          || currentIDs.some((id, index) => id !== aggregate.memberIDs[index]);
        const writtenValues = currentIDs.map((id) => {
          const session = sessions.find((item) => (item.connection_id || item.id) === id);
          const written = Number(session?.quality?.written_data_payload_bytes);
          return Number.isFinite(written) && written >= 0 ? written : null;
        });
        const hasCompleteCounters = writtenValues.every((written) => written != null);
        const writtenTotal = hasCompleteCounters ? writtenValues.reduce((total, written) => total + written, 0) : null;
        const elapsedSeconds = (sampledAt - aggregate.lastAt) / 1000;
        const aggregateRate = !membershipChanged && writtenTotal != null && Number.isFinite(aggregate.lastWritten)
          && Number.isFinite(elapsedSeconds) && elapsedSeconds > 0 && writtenTotal >= aggregate.lastWritten
          ? (writtenTotal - aggregate.lastWritten) / elapsedSeconds : null;
        if (aggregateRate != null) aggregate.samples = [...aggregate.samples, aggregateRate].slice(-MAX_TREND_SAMPLES);
        aggregate.lastWritten = writtenTotal;
        aggregate.lastAt = sampledAt;
        aggregate.memberIDs = currentIDs;
      }
    },
    snapshot() {
      return [...trends.values()].map((trend) => ({
        id: trend.id, label: trend.label, localEndpoint: trend.localEndpoint, remoteEndpoint: trend.remoteEndpoint,
        samples: [...trend.samples],
      })).filter((trend) => trend.samples.length > 0);
    },
    aggregateSnapshot() {
      return aggregate.samples.length === 0 ? null : { id: aggregate.id, label: '', samples: [...aggregate.samples] };
    },
  };
}