import { adaptiveStates, adaptiveTransitions, flowStates, transitionReasons } from './status.js';
import en from './locales/en.js';
import zhCN from './locales/zh-CN.js';

function message(catalog, key) {
  return key.split('.').reduce((value, segment) => value?.[segment], catalog) || '';
}

function aliases(key) {
  return key ? [message(zhCN, key), message(en, key)] : [];
}

function includesQuery(values, query) {
  return values.some((value) => String(value || '').toLowerCase().includes(query));
}

export function isSessionAbnormal(session) {
  return session.state !== 3 || Number(session.quality?.stall_penalty_micros) > 0;
}

export function isFlowAbnormal(flow) {
  return flow.state !== 3 || [2, 3, 4].includes(flow.adaptive_state);
}

export function isTerminalAbnormal(flow) {
  return flow.state === 8;
}

export function sessionMatchesFilter(session, query, onlyAnomalies) {
  if (onlyAnomalies && !isSessionAbnormal(session)) return false;
  if (!query) return true;
  return includesQuery([
    session.connection_id,
    session.id,
    session.interface,
    session.principal,
    session.local_endpoint,
    session.remote_endpoint,
  ], query);
}

export function flowMatchesFilter(flow, query, onlyAnomalies) {
  if (onlyAnomalies && !isFlowAbnormal(flow)) return false;
  if (!query) return true;
  return includesQuery([
    flow.flow_id,
    flow.id,
    flow.preferred_connection_id,
    ...aliases(adaptiveStates[flow.adaptive_state]),
    ...aliases(adaptiveTransitions[flow.adaptive_transition]),
  ], query);
}

export function terminalMatchesFilter(flow, query, onlyAnomalies) {
  if (onlyAnomalies && !isTerminalAbnormal(flow)) return false;
  if (!query) return true;
  return includesQuery([
    flow.flow_id,
    flow.id,
    ...aliases(flowStates[flow.state]),
    ...aliases(transitionReasons[flow.reason]),
  ], query);
}

export function interfaceMatchesFilter(item, query, onlyAnomalies) {
  if (onlyAnomalies && item.reason === 1) return false;
  if (!query) return true;
  return includesQuery([item.name, ...(item.addresses || [])], query);
}