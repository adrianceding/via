async function requestJSON(fetcher, path) {
  const response = await fetcher(path, { cache: 'no-store' });
  if (!response.ok) {
    const error = new Error(`HTTP ${response.status}`);
    error.status = response.status;
    throw error;
  }
  return response.json();
}

export async function fetchStatusSnapshot(fetcher = fetch) {
  const [summary, interfaces, sessions, flows] = await Promise.all([
    requestJSON(fetcher, '/api/v1/summary'),
    requestJSON(fetcher, '/api/v1/interfaces'),
    requestJSON(fetcher, '/api/v1/sessions'),
    requestJSON(fetcher, '/api/v1/flows'),
  ]);
  return { summary, interfaces, sessions, flows };
}

export function statusErrorCode(error) {
  if (error?.status === 401) return 'authentication';
  if (Number.isInteger(error?.status)) return 'response';
  return 'network';
}