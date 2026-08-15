export const AGGREGATION_MAX_GROUPS = 12;

function sessionKey(session, fallback = '') {
  return String(session.connection_id || session.id || fallback);
}

function validNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) && number >= 0 ? number : 0;
}

function summarizeValues(values) {
  if (values.length === 0) return { min: null, average: null, max: null };
  return {
    min: Math.min(...values),
    average: values.reduce((total, value) => total + value, 0) / values.length,
    max: Math.max(...values),
  };
}

function peerCapacitySample(quality, generatedAt) {
  const capacity = Number(quality?.peer_send_capacity_bytes_sec);
  const sampleAt = Date.parse(quality?.peer_send_data_sample_at || '');
  const expiresAt = Date.parse(quality?.peer_send_data_sample_expires_at || '');
  const observedAt = Date.parse(generatedAt || '');
  if (!Number.isFinite(capacity) || capacity <= 0 || !Number.isFinite(sampleAt)
    || !Number.isFinite(expiresAt) || expiresAt < sampleAt) {
    return { state: 'unmeasured', capacity: null, lastCapacity: null, ageMs: null };
  }
  const ageMs = Number.isFinite(observedAt) ? Math.max(0, observedAt - sampleAt) : null;
  if (!Number.isFinite(observedAt) || observedAt > expiresAt) {
    return { state: 'stale', capacity: null, lastCapacity: capacity, ageMs };
  }
  return { state: 'fresh', capacity, lastCapacity: capacity, ageMs };
}

export function aggregationGroupIdentity(session, index = 0) {
  const interfaceName = String(session.interface || '');
  const pathGroupID = String(session.path_group_id || '');
  if (interfaceName) return {
    type: 'interface', value: interfaceName, interfaceName, pathGroupID,
  };
  if (pathGroupID) return { type: 'path_group', value: pathGroupID, pathGroupID };
  const principal = String(session.principal || '');
  if (principal) return { type: 'principal', value: principal, principal };
  return { type: 'session', value: sessionKey(session, `session-${index}`) };
}

function compareRows(left, right) {
  if (left.capacity != null && right.capacity == null) return -1;
  if (left.capacity == null && right.capacity != null) return 1;
  if (left.capacity !== right.capacity) return (right.capacity || 0) - (left.capacity || 0);
  const leftRTT = left.rtt > 0 ? left.rtt : Number.POSITIVE_INFINITY;
  const rightRTT = right.rtt > 0 ? right.rtt : Number.POSITIVE_INFINITY;
  return leftRTT - rightRTT || sessionKey(left.session).localeCompare(sessionKey(right.session));
}

function summarizeGroup(group) {
  const lanes = group.rows.sort(compareRows);
  const capacityRange = summarizeValues(lanes.map((lane) => lane.capacity).filter((capacity) => capacity != null));
  const peerCapacityRange = summarizeValues(lanes.map((lane) => lane.peerCapacity).filter((capacity) => capacity != null));
  const referenceCapacityRange = summarizeValues(lanes.map((lane) => lane.lastCapacity).filter((capacity) => capacity != null));
  const peerReferenceCapacityRange = summarizeValues(lanes.map((lane) => lane.peerLastCapacity).filter((capacity) => capacity != null));
  const rttRange = summarizeValues(lanes.map((lane) => lane.rtt).filter((rtt) => rtt > 0));
  const freshCount = lanes.filter((lane) => lane.sampleState === 'fresh').length;
  const peerFreshCount = lanes.filter((lane) => lane.peerSampleState === 'fresh').length;
  const totalCapacity = freshCount === lanes.length
    ? lanes.reduce((total, lane) => total + lane.capacity, 0)
    : null;
  const peerTotalCapacity = peerFreshCount === lanes.length
    ? lanes.reduce((total, lane) => total + lane.peerCapacity, 0)
    : null;
  const lastTotalCapacity = lanes.every((lane) => lane.lastCapacity != null)
    ? lanes.reduce((total, lane) => total + lane.lastCapacity, 0)
    : null;
  const peerLastTotalCapacity = lanes.every((lane) => lane.peerLastCapacity != null)
    ? lanes.reduce((total, lane) => total + lane.peerLastCapacity, 0)
    : null;
  const highestRow = lanes.find((lane) => lane.capacity != null) || null;
  const highestCapacity = highestRow?.capacity ?? null;
  const peerHighestCapacity = Math.max(...lanes.map((lane) => lane.peerCapacity).filter((capacity) => capacity != null), -1);
  const lastHighestCapacity = Math.max(...lanes.map((lane) => lane.lastCapacity).filter((capacity) => capacity != null), -1);
  const peerLastHighestCapacity = Math.max(...lanes.map((lane) => lane.peerLastCapacity).filter((capacity) => capacity != null), -1);
  const eligibleBytes = lanes.reduce((total, lane) => total + lane.eligibleBytes, 0);
  return {
    key: `${group.type}:${group.value}`,
    type: group.type,
    label: group.value,
    pathGroupID: group.pathGroupID || '',
    interface: group.interfaceName || '',
    principal: group.principal || '',
    laneCount: lanes.length,
    readyCount: lanes.length,
    freshCount,
    peerFreshCount,
    lanes,
    totalCapacity,
    peerTotalCapacity,
    lastTotalCapacity,
    peerLastTotalCapacity,
    highestCapacity,
    peerHighestCapacity: peerHighestCapacity < 0 ? null : peerHighestCapacity,
    lastHighestCapacity: lastHighestCapacity < 0 ? null : lastHighestCapacity,
    peerLastHighestCapacity: peerLastHighestCapacity < 0 ? null : peerLastHighestCapacity,
    capacity: totalCapacity ?? highestCapacity,
    capacityRange,
    peerCapacityRange,
    referenceCapacityRange,
    peerReferenceCapacityRange,
    rttRange,
    eligibleBytes,
    eligibleAckedBytes: eligibleBytes,
    queuedBytes: lanes.reduce((total, lane) => total + lane.queuedBytes, 0),
    inFlightBytes: lanes.reduce((total, lane) => total + lane.inFlightBytes, 0),
  };
}

function compareGroups(left, right, mode) {
  if (mode === 'rtt') {
    return (left.rttRange.average ?? Number.POSITIVE_INFINITY)
      - (right.rttRange.average ?? Number.POSITIVE_INFINITY)
      || left.label.localeCompare(right.label)
      || left.key.localeCompare(right.key);
  }
  return left.label.localeCompare(right.label) || left.key.localeCompare(right.key);
}

export function summarizeAggregation(sessions, mode = 'name', generatedAt = '') {
  const groupsByKey = new Map();
  sessions.forEach((session, index) => {
    if (session.state !== 3) return;
    const identity = aggregationGroupIdentity(session, index);
    const key = `${identity.type}:${identity.value}`;
    if (!groupsByKey.has(key)) groupsByKey.set(key, { ...identity, rows: [] });
    const fresh = session.quality?.data_sample_fresh === true;
    const peerSample = peerCapacitySample(session.quality, generatedAt);
    const lastCapacity = fresh
      ? validNumber(session.quality?.capacity_bytes_sec)
      : Number(session.quality?.last_data_capacity_bytes_sec) > 0
        ? validNumber(session.quality?.last_data_capacity_bytes_sec)
        : null;
    groupsByKey.get(key).rows.push({
      session,
      sampleState: fresh ? 'fresh' : session.quality?.data_sample_age_ms == null ? 'unmeasured' : 'stale',
      capacity: fresh ? validNumber(session.quality?.capacity_bytes_sec) : null,
      lastCapacity,
      peerSampleState: peerSample.state,
      peerCapacity: peerSample.capacity,
      peerLastCapacity: peerSample.lastCapacity,
      peerSampleAgeMs: peerSample.ageMs,
      sampleAgeMs: session.quality?.data_sample_age_ms == null
        ? null
        : validNumber(session.quality.data_sample_age_ms),
      rtt: validNumber(session.quality?.smoothed_rtt_micros),
      stall: validNumber(session.quality?.stall_penalty_micros),
      eligibleBytes: validNumber(session.quality?.eligible_acked_data_payload_bytes),
      queuedBytes: validNumber(session.quality?.queued_bytes),
      inFlightBytes: validNumber(session.quality?.in_flight_bytes),
      isHighestCapacity: false,
    });
  });

  const rawGroups = [...groupsByKey.values()];
  const allGroups = rawGroups.map(summarizeGroup).sort((left, right) => compareGroups(left, right, mode));
  const groups = allGroups.slice(0, AGGREGATION_MAX_GROUPS);
  const readyRows = rawGroups.flatMap((group) => group.rows);
  const readyCount = readyRows.length;
  const freshCount = readyRows.filter((row) => row.sampleState === 'fresh').length;
  const peerFreshCount = readyRows.filter((row) => row.peerSampleState === 'fresh').length;
  const complete = readyCount > 0 && freshCount === readyCount;
  const peerComplete = readyCount > 0 && peerFreshCount === readyCount;
  const totalCapacity = complete ? readyRows.reduce((total, row) => total + row.capacity, 0) : null;
  const peerTotalCapacity = peerComplete ? readyRows.reduce((total, row) => total + row.peerCapacity, 0) : null;
  const lastTotalCapacity = readyCount > 0 && readyRows.every((row) => row.lastCapacity != null)
    ? readyRows.reduce((total, row) => total + row.lastCapacity, 0)
    : null;
  const peerLastTotalCapacity = readyCount > 0 && readyRows.every((row) => row.peerLastCapacity != null)
    ? readyRows.reduce((total, row) => total + row.peerLastCapacity, 0)
    : null;
  const highestRow = readyRows.slice().sort(compareRows).find((row) => row.capacity != null) || null;
  const highestCapacity = highestRow?.capacity ?? null;
  const lift = readyCount >= 2 && totalCapacity != null && highestCapacity > 0
    ? (totalCapacity - highestCapacity) / highestCapacity * 100
    : null;
  const peerHighestCapacity = Math.max(...readyRows.map((row) => row.peerCapacity).filter((capacity) => capacity != null), -1);
  const normalizedPeerHighestCapacity = peerHighestCapacity < 0 ? null : peerHighestCapacity;
  const lastHighestCapacity = Math.max(...readyRows.map((row) => row.lastCapacity).filter((capacity) => capacity != null), -1);
  const peerLastHighestCapacity = Math.max(...readyRows.map((row) => row.peerLastCapacity).filter((capacity) => capacity != null), -1);
  const peerLift = readyCount >= 2 && peerTotalCapacity != null && normalizedPeerHighestCapacity > 0
    ? (peerTotalCapacity - normalizedPeerHighestCapacity) / normalizedPeerHighestCapacity * 100
    : null;
  const eligibleTotal = allGroups.reduce((total, group) => total + group.eligibleBytes, 0);
  const highestShare = highestRow && eligibleTotal > 0
    ? highestRow.eligibleBytes / eligibleTotal * 100
    : null;
  if (highestRow) highestRow.isHighestCapacity = true;

  return {
    rows: groups.flatMap((group) => group.lanes),
    groups,
    groupCount: allGroups.length,
    displayedGroupCount: groups.length,
    hiddenGroupCount: Math.max(0, allGroups.length - AGGREGATION_MAX_GROUPS),
    readyCount,
    freshCount,
    peerFreshCount,
    totalCapacity,
    peerTotalCapacity,
    lastTotalCapacity,
    peerLastTotalCapacity,
    highestCapacity,
    peerHighestCapacity: normalizedPeerHighestCapacity,
    lastHighestCapacity: lastHighestCapacity < 0 ? null : lastHighestCapacity,
    peerLastHighestCapacity: peerLastHighestCapacity < 0 ? null : peerLastHighestCapacity,
    lift,
    peerLift,
    highestShare,
  };
}

function directionSummary(summary, peer) {
  const field = (local, remote) => (peer ? summary[remote] : summary[local]);
  return {
    readyCount: summary.readyCount,
    freshCount: field('freshCount', 'peerFreshCount'),
    totalCapacity: field('totalCapacity', 'peerTotalCapacity'),
    lastTotalCapacity: field('lastTotalCapacity', 'peerLastTotalCapacity'),
    highestCapacity: field('highestCapacity', 'peerHighestCapacity'),
    lastHighestCapacity: field('lastHighestCapacity', 'peerLastHighestCapacity'),
    lift: field('lift', 'peerLift'),
    groups: summary.groups.map((group) => {
      const capacity = peer ? group.peerHighestCapacity : group.highestCapacity;
      let highestAssigned = false;
      const lanes = group.lanes.map((row) => {
        const rowCapacity = peer ? row.peerCapacity : row.capacity;
        const isHighestCapacity = !highestAssigned && rowCapacity != null && rowCapacity === capacity;
        if (isHighestCapacity) highestAssigned = true;
        return {
          ...row,
          sampleState: peer ? row.peerSampleState : row.sampleState,
          capacity: rowCapacity,
          lastCapacity: peer ? row.peerLastCapacity : row.lastCapacity,
          sampleAgeMs: peer ? row.peerSampleAgeMs : row.sampleAgeMs,
          isHighestCapacity,
        };
      }).sort(compareRows);
      return {
        ...group,
        freshCount: peer ? group.peerFreshCount : group.freshCount,
        totalCapacity: peer ? group.peerTotalCapacity : group.totalCapacity,
        lastTotalCapacity: peer ? group.peerLastTotalCapacity : group.lastTotalCapacity,
        highestCapacity: capacity,
        lastHighestCapacity: peer ? group.peerLastHighestCapacity : group.lastHighestCapacity,
        capacityRange: peer ? group.peerCapacityRange : group.capacityRange,
        referenceCapacityRange: peer ? group.peerReferenceCapacityRange : group.referenceCapacityRange,
        lanes,
      };
    }),
  };
}

export function directionalAggregation(summary, role) {
  const localSend = directionSummary(summary, false);
  const peerSend = directionSummary(summary, true);
  return role === 2
    ? { uplink: peerSend, downlink: localSend }
    : { uplink: localSend, downlink: peerSend };
}
