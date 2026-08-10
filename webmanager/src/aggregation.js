function sessionKey(session) {
  return String(session.interface || session.principal || '')
    + '\u0000'
    + String(session.connection_id || session.id || '');
}

function validNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) && number >= 0 ? number : 0;
}

export function summarizeAggregation(sessions) {
  const ready = sessions.filter((session) => session.state === 3);
  const rows = ready.map((session) => {
    const fresh = session.quality?.data_sample_fresh === true;
    return {
      session,
      sampleState: fresh ? 'fresh' : session.quality?.data_sample_age_ms == null ? 'unmeasured' : 'stale',
      capacity: fresh ? validNumber(session.quality?.capacity_bytes_sec) : null,
      rtt: validNumber(session.quality?.smoothed_rtt_micros),
      stall: validNumber(session.quality?.stall_penalty_micros),
      eligibleBytes: validNumber(session.quality?.eligible_acked_data_payload_bytes),
      isHighestCapacity: false,
    };
  }).sort((left, right) => {
    if (left.capacity != null && right.capacity == null) return -1;
    if (left.capacity == null && right.capacity != null) return 1;
    if (left.capacity !== right.capacity) return (right.capacity || 0) - (left.capacity || 0);
    const leftRTT = left.rtt > 0 ? left.rtt : Number.POSITIVE_INFINITY;
    const rightRTT = right.rtt > 0 ? right.rtt : Number.POSITIVE_INFINITY;
    return leftRTT - rightRTT || sessionKey(left.session).localeCompare(sessionKey(right.session));
  });

  const highestRow = rows.find((row) => row.capacity != null) || null;
  if (highestRow) highestRow.isHighestCapacity = true;

  const freshCount = rows.filter((row) => row.sampleState === 'fresh').length;
  const complete = rows.length >= 2 && freshCount === rows.length;
  const totalCapacity = complete ? rows.reduce((total, row) => total + row.capacity, 0) : null;
  const highestCapacity = highestRow?.capacity ?? null;
  const lift = totalCapacity != null && highestCapacity > 0
    ? (totalCapacity - highestCapacity) / highestCapacity * 100
    : null;
  const eligibleTotal = rows.reduce((total, row) => total + row.eligibleBytes, 0);
  const highestShare = highestRow && eligibleTotal > 0
    ? highestRow.eligibleBytes / eligibleTotal * 100
    : null;

  return {
    rows,
    readyCount: rows.length,
    freshCount,
    totalCapacity,
    highestCapacity,
    lift,
    highestShare,
  };
}
