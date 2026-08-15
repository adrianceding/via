import { aggregationGroupIdentity } from './aggregation.js';
import { concurrentCapacityTotal } from './capacity-samples.js';
import { sortSessions } from './quality.js';

function validNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) && number >= 0 ? number : 0;
}

function positiveNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) && number > 0 ? number : null;
}

function localSendCapacity(quality) {
  const current = quality?.data_sample_fresh === true
    ? positiveNumber(quality?.capacity_bytes_sec)
    : null;
  const last = current ?? positiveNumber(quality?.last_data_capacity_bytes_sec);
  return {
    state: current != null ? 'fresh' : last != null ? 'stale' : 'unmeasured',
    capacity: current,
    lastCapacity: last,
    ageMs: quality?.data_sample_age_ms == null ? null : validNumber(quality.data_sample_age_ms),
  };
}

function peerSendCapacity(quality, generatedAt) {
  const capacity = positiveNumber(quality?.peer_send_capacity_bytes_sec);
  const sampleAt = Date.parse(quality?.peer_send_data_sample_at || '');
  const expiresAt = Date.parse(quality?.peer_send_data_sample_expires_at || '');
  const observedAt = Date.parse(generatedAt || '');
  if (capacity == null || !Number.isFinite(sampleAt) || !Number.isFinite(expiresAt) || expiresAt < sampleAt) {
    return { state: 'unmeasured', capacity: null, lastCapacity: null, ageMs: null };
  }
  const ageMs = Number.isFinite(observedAt) ? Math.max(0, observedAt - sampleAt) : null;
  const current = Number.isFinite(observedAt) && observedAt <= expiresAt ? capacity : null;
  return { state: current != null ? 'fresh' : 'stale', capacity: current, lastCapacity: capacity, ageMs };
}

export function directionalSessionCapacity(session, role, generatedAt) {
  const localSend = localSendCapacity(session?.quality);
  const peerSend = peerSendCapacity(session?.quality, generatedAt);
  return role === 2
    ? { uplink: peerSend, downlink: localSend }
    : { uplink: localSend, downlink: peerSend };
}

function summarizeDirection(ready, direction) {
  const samples = ready.map((session) => session.capacityDirections[direction]);
  return {
    freshCount: samples.filter((sample) => sample.state === 'fresh').length,
    capacity: concurrentCapacityTotal(samples),
    lastCapacity: concurrentCapacityTotal(samples, 'lastCapacity'),
  };
}

function compareObservations(left, right, mode) {
  if (mode === 'rtt') {
    return (left.rtt ?? Number.POSITIVE_INFINITY) - (right.rtt ?? Number.POSITIVE_INFINITY)
      || left.label.localeCompare(right.label);
  }
  if (mode === 'state') {
    return left.readyCount / left.laneCount - right.readyCount / right.laneCount
      || right.anomalyCount - left.anomalyCount
      || left.label.localeCompare(right.label);
  }
  return left.label.localeCompare(right.label) || left.key.localeCompare(right.key);
}

export function summarizeSessionObservations(sessions, mode = 'source', role = 0, generatedAt = '') {
  const groups = new Map();
  sessions.forEach((session, index) => {
    const identity = aggregationGroupIdentity(session, index);
    const key = `${identity.type}:${identity.value}`;
    if (!groups.has(key)) groups.set(key, { ...identity, key, sessions: [] });
    groups.get(key).sessions.push({
      ...session,
      capacityDirections: directionalSessionCapacity(session, role, generatedAt),
    });
  });

  return [...groups.values()].map((group) => {
    const lanes = sortSessions(group.sessions, mode);
    const ready = lanes.filter((session) => session.state === 3);
    const rtts = ready.map((session) => positiveNumber(session.quality?.smoothed_rtt_micros)).filter((value) => value != null);
    return {
      ...group,
      label: group.value,
      lanes,
      laneCount: lanes.length,
      readyCount: ready.length,
      anomalyCount: lanes.length - ready.length,
      uplink: summarizeDirection(ready, 'uplink'),
      downlink: summarizeDirection(ready, 'downlink'),
      rtt: rtts.length ? rtts.reduce((total, rtt) => total + rtt, 0) / rtts.length : null,
      queuedBytes: lanes.reduce((total, session) => total + validNumber(session.quality?.queued_bytes), 0),
      inFlightBytes: lanes.reduce((total, session) => total + validNumber(session.quality?.in_flight_bytes), 0),
      reconnects: lanes.reduce((total, session) => total + validNumber(session.reconnects), 0),
      fastest: lanes.some((session) => session.fastest),
    };
  }).sort((left, right) => compareObservations(left, right, mode));
}