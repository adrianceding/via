function items(payload) {
  return Array.isArray(payload?.items) ? payload.items : [];
}

export function normalizeSnapshot(payloads) {
  const interfaces = items(payloads.interfaces);
  const sessions = items(payloads.sessions);
  const flows = items(payloads.flows);
  const terminals = Array.isArray(payloads.flows?.terminals) ? payloads.flows.terminals : [];
  return {
    summary: payloads.summary || {},
    interfaces,
    sessions,
    sessionsGeneratedAt: payloads.sessions?.generated_at || payloads.summary?.generated_at || '',
    flows,
    terminals,
    sessionTotal: payloads.sessions?.total ?? sessions.length,
    flowTotal: payloads.flows?.total ?? flows.length,
    terminalTotal: payloads.flows?.terminal_total ?? terminals.length,
    truncated: [payloads.interfaces, payloads.sessions, payloads.flows].some((payload) => Boolean(payload?.truncated)),
  };
}