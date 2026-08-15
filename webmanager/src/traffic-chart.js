import { aggregationGroupIdentity } from './aggregation.js';

export const MAX_SHARE_GROUPS = 12;

function validBytes(value) {
  const number = Number(value);
  return Number.isFinite(number) && number > 0 ? number : 0;
}

export function summarizeShareData(
  sessions,
  maxGroups = MAX_SHARE_GROUPS,
  otherLabel = 'Other',
  counter = 'eligible_acked_data_payload_bytes',
) {
  const groups = new Map();
  sessions.forEach((session, index) => {
    const value = validBytes(session.quality?.[counter]);
    if (value <= 0) return;
    const identity = aggregationGroupIdentity(session, index);
    const key = `${identity.type}:${identity.value}`;
    const existing = groups.get(key) || {
      key,
      label: identity.value,
      value: 0,
    };
    existing.value += value;
    groups.set(key, existing);
  });

  const allGroups = [...groups.values()].sort((left, right) => right.value - left.value || left.key.localeCompare(right.key));
  const totalEligible = allGroups.reduce((total, group) => total + group.value, 0);
  const boundedCount = Math.max(1, Math.floor(maxGroups));
  const visibleCount = allGroups.length > boundedCount ? Math.max(0, boundedCount - 1) : boundedCount;
  const visible = allGroups.slice(0, visibleCount);
  const hidden = allGroups.slice(visibleCount);
  if (hidden.length) {
    visible.push({
      key: '__other__',
      label: otherLabel,
      value: hidden.reduce((total, group) => total + group.value, 0),
      hiddenGroupCount: hidden.length,
    });
  }

  return {
    data: visible,
    totalBytes: totalEligible,
    totalEligible,
    hasData: totalEligible > 0,
    groupCount: allGroups.length,
    displayedGroupCount: visible.length,
    hiddenGroupCount: hidden.length,
  };
}

export function summarizeDirectionalShareData(
  sessions,
  role,
  maxGroups = MAX_SHARE_GROUPS,
  otherLabel = 'Other',
) {
  const localSend = summarizeShareData(
    sessions,
    maxGroups,
    otherLabel,
    'eligible_acked_data_payload_bytes',
  );
  const peerSend = summarizeShareData(
    sessions,
    maxGroups,
    otherLabel,
    'received_data_payload_bytes',
  );
  return role === 2
    ? { uplink: peerSend, downlink: localSend }
    : { uplink: localSend, downlink: peerSend };
}