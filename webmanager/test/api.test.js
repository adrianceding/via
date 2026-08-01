import assert from 'node:assert/strict';
import test from 'node:test';

import { fetchStatusSnapshot, statusErrorCode } from '../src/api.js';

function response(body, status = 200) {
  return { ok: status >= 200 && status < 300, status, json: async () => body };
}

test('fetchStatusSnapshot reads the four existing endpoints', async () => {
  const paths = [];
  const fetcher = async (path, options) => {
    paths.push([path, options.cache]);
    return response({ path });
  };
  const snapshot = await fetchStatusSnapshot(fetcher);
  assert.deepEqual(paths, [
    ['/api/v1/summary', 'no-store'],
    ['/api/v1/interfaces', 'no-store'],
    ['/api/v1/sessions', 'no-store'],
    ['/api/v1/flows', 'no-store'],
  ]);
  assert.equal(snapshot.summary.path, '/api/v1/summary');
});

test('statusErrorCode distinguishes authentication, HTTP, and network failures', async () => {
  await assert.rejects(
    fetchStatusSnapshot(async () => response({}, 401)),
    (error) => statusErrorCode(error) === 'authentication',
  );
  await assert.rejects(
    fetchStatusSnapshot(async () => response({}, 503)),
    (error) => statusErrorCode(error) === 'response',
  );
  assert.equal(statusErrorCode(new TypeError('offline')), 'network');
});