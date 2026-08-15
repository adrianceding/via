import { aggregationGroupIdentity } from './aggregation.js';
import { concurrentCapacityTotal } from './capacity-samples.js';

const MAX_TREND_SAMPLES = 120;
const MAX_TREND_SERIES = 12;
const AGGREGATE_ID = '__aggregate__';

export function directionalTrend(trend, role) {
  const localSend = {
    samples: [...(trend?.samples || [])],
    capacity: Number(trend?.capacity) || 0,
    lastCapacity: Number(trend?.lastCapacity) || 0,
  };
  const peerSend = {
    samples: [...(trend?.rxSamples || [])],
    capacity: Number(trend?.peerCapacity) || 0,
    lastCapacity: Number(trend?.peerLastCapacity) || 0,
  };
  return role === 2
    ? { uplink: peerSend, downlink: localSend }
    : { uplink: localSend, downlink: peerSend };
}

function peerCapacitySample(quality, sampledAt) {
  const capacity = Number(quality?.peer_send_capacity_bytes_sec);
  const sampleAt = Date.parse(quality?.peer_send_data_sample_at || '');
  const expiresAt = Date.parse(quality?.peer_send_data_sample_expires_at || '');
  if (!Number.isFinite(capacity) || capacity <= 0 || !Number.isFinite(sampleAt)
    || !Number.isFinite(expiresAt) || expiresAt < sampleAt) return { current: null, last: null, ageMs: null };
  const current = Number.isFinite(sampledAt) && sampledAt <= expiresAt ? capacity : null;
  const ageMs = Number.isFinite(sampledAt) ? Math.max(0, sampledAt - sampleAt) : null;
  return { current, last: capacity, ageMs };
}

export function createSessionTrendStore() {
  const trends = new Map();
  const aggregate = { id: AGGREGATE_ID, label: '', samples: [], lastWritten: null, lastAt: null, memberIDs: [] };

  return {
    update(sessions, generatedAt) {
      const sampledAt = Date.parse(generatedAt || '');
      const observations = new Map();
      sessions.forEach((session, index) => {
        const identity = aggregationGroupIdentity(session, index);
        const id = `${identity.type}:${identity.value}`;
        if (!observations.has(id)) observations.set(id, { ...identity, id, sessions: [] });
        observations.get(id).sessions.push(session);
      });
      const activeIDs = new Set(observations.keys());
      const selectedIDs = new Set(
        [...trends.keys()].filter((id) => activeIDs.has(id)).slice(0, MAX_TREND_SERIES),
      );
      observations.forEach(({ id }) => {
        if (id && selectedIDs.size < MAX_TREND_SERIES) selectedIDs.add(id);
      });
      trends.forEach((_trend, id) => {
        if (!selectedIDs.has(id)) trends.delete(id);
      });

      observations.forEach((observation, id) => {
        if (!selectedIDs.has(id)) return;
        const existing = trends.get(id);
        const memberIDs = observation.sessions.map((session, index) => String(session.connection_id || session.id || `${id}:${index}`)).sort();
        const membershipChanged = memberIDs.length !== (existing?.memberIDs || []).length
          || memberIDs.some((memberID, index) => memberID !== existing.memberIDs[index]);
        const writtenValues = observation.sessions.map((session) => Number(session.quality?.written_data_payload_bytes));
        const written = writtenValues.every((value) => Number.isFinite(value) && value >= 0)
          ? writtenValues.reduce((total, value) => total + value, 0)
          : null;
        const previousWritten = existing?.lastWritten;
        const previousAt = existing?.lastAt;
        const elapsedSeconds = (sampledAt - previousAt) / 1000;
        const rate = !membershipChanged && Number.isFinite(written) && written >= 0 && Number.isFinite(previousWritten)
          && previousWritten >= 0 && Number.isFinite(elapsedSeconds) && elapsedSeconds > 0
          && written >= previousWritten ? (written - previousWritten) / elapsedSeconds : null;
        const samples = rate == null ? [...(existing?.samples || [])] : [...(existing?.samples || []), rate].slice(-MAX_TREND_SAMPLES);
        const receivedValues = observation.sessions.map((session) => Number(session.quality?.received_data_payload_bytes));
        const received = receivedValues.every((value) => Number.isFinite(value) && value >= 0)
          ? receivedValues.reduce((total, value) => total + value, 0)
          : null;
        const previousReceived = existing?.lastRxWritten;
        const rxRate = !membershipChanged && Number.isFinite(received) && received >= 0 && Number.isFinite(previousReceived)
          && previousReceived >= 0 && Number.isFinite(elapsedSeconds) && elapsedSeconds > 0
          && received >= previousReceived ? (received - previousReceived) / elapsedSeconds : null;
        const rxSamples = rxRate == null ? [...(existing?.rxSamples || [])] : [...(existing?.rxSamples || []), rxRate].slice(-MAX_TREND_SAMPLES);
        const ready = observation.sessions.filter((session) => session.state === 3);
        const measured = ready.filter((session) => session.quality?.data_sample_fresh === true);
        const localSamples = ready.map((session) => ({
          capacity: session.quality?.data_sample_fresh === true
            ? Math.max(0, Number(session.quality?.capacity_bytes_sec) || 0)
            : null,
          lastCapacity: session.quality?.data_sample_fresh === true
            ? Number(session.quality?.capacity_bytes_sec)
            : Number(session.quality?.last_data_capacity_bytes_sec) > 0
              ? Number(session.quality.last_data_capacity_bytes_sec)
              : null,
          ageMs: session.quality?.data_sample_age_ms,
        }));
        const capacity = measured.length === ready.length
          ? concurrentCapacityTotal(localSamples) ?? 0
          : 0;
        const lastCapacity = concurrentCapacityTotal(localSamples, 'lastCapacity') ?? 0;
        const peerSamples = ready.map((session) => peerCapacitySample(session.quality, sampledAt));
        const peerCapacity = concurrentCapacityTotal(peerSamples, 'current') ?? 0;
        const peerLastCapacity = concurrentCapacityTotal(peerSamples, 'last') ?? 0;
        trends.set(id, {
          id,
          label: observation.value,
          localEndpoint: observation.interfaceName || observation.pathGroupID || observation.principal || '',
          remoteEndpoint: '',
          samples,
          rxSamples,
          capacity,
          lastCapacity,
          peerCapacity,
          peerLastCapacity,
          lastWritten: Number.isFinite(written) && written >= 0 ? written : null,
          lastRxWritten: Number.isFinite(received) && received >= 0 ? received : null,
          lastAt: Number.isFinite(sampledAt) ? sampledAt : null,
          memberIDs,
        });
      });

      if (Number.isFinite(sampledAt)) {
        const currentIDs = sessions.map((session, index) => String(
          session.connection_id || session.id || `session-${index}`,
        )).sort();
        const membershipChanged = currentIDs.length !== aggregate.memberIDs.length
          || currentIDs.some((id, index) => id !== aggregate.memberIDs[index]);
        const writtenValues = sessions.map((session) => {
          const written = Number(session.quality?.written_data_payload_bytes);
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
        samples: [...trend.samples], rxSamples: [...(trend.rxSamples || [])], capacity: trend.capacity,
        lastCapacity: trend.lastCapacity, peerCapacity: trend.peerCapacity, peerLastCapacity: trend.peerLastCapacity,
      })).filter((trend) => trend.samples.length > 0);
    },
    aggregateSnapshot() {
      return aggregate.samples.length === 0 ? null : { id: aggregate.id, label: '', samples: [...aggregate.samples] };
    },
  };
}