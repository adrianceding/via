const FLOW_TABS = new Set(['active', 'terminal']);
const SESSION_SORTS = new Set(['source', 'rtt', 'state']);

export function parseViewState(search) {
  const parameters = new URLSearchParams(search);
  const flowTab = parameters.get('flow');
  const sessionSort = parameters.get('sort');
  return {
    query: String(parameters.get('q') || '').slice(0, 256),
    onlyAnomalies: parameters.get('anomaly') === '1',
    flowTab: FLOW_TABS.has(flowTab) ? flowTab : 'active',
    sessionSort: SESSION_SORTS.has(sessionSort) ? sessionSort : 'source',
  };
}

export function serializeViewState(state) {
  const parameters = new URLSearchParams();
  const query = String(state.query || '').trim().slice(0, 256);
  if (query) parameters.set('q', query);
  if (state.onlyAnomalies) parameters.set('anomaly', '1');
  if (state.flowTab === 'terminal') parameters.set('flow', 'terminal');
  if (state.sessionSort === 'rtt' || state.sessionSort === 'state') parameters.set('sort', state.sessionSort);
  const serialized = parameters.toString();
  return serialized ? `?${serialized}` : '';
}

export function buildViewURL(location, state) {
  const url = new URL(location);
  url.search = serializeViewState(state);
  return url.toString();
}