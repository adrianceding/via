import assert from 'node:assert/strict';
import test from 'node:test';

import { copyIdentifier } from '../src/clipboard.js';

test('copyIdentifier prefers the Clipboard API', async () => {
  const copied = [];
  const result = await copyIdentifier('connection-a', {
    clipboard: { writeText: async (value) => copied.push(value) },
  });
  assert.equal(result, true);
  assert.deepEqual(copied, ['connection-a']);
});

test('copyIdentifier falls back when Clipboard API is unavailable', async () => {
  const copied = [];
  const result = await copyIdentifier('flow-a', {
    clipboard: null,
    fallback: (value) => copied.push(value),
  });
  assert.equal(result, true);
  assert.deepEqual(copied, ['flow-a']);
});