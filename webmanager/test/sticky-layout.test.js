import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

test('runtime header and filters remain visible without overlapping', async () => {
  const header = await readFile(new URL('../src/components/RuntimeHeader.vue', import.meta.url), 'utf8');
  const styles = await readFile(new URL('../src/styles.css', import.meta.url), 'utf8');
  assert.match(styles, /\.runtime-header \{[\s\S]*?position: sticky;[\s\S]*?top: 0;[\s\S]*?z-index: 10;/);
  assert.match(styles, /\.manager-tools \{[\s\S]*?position: sticky;[\s\S]*?top: 84px;/);
  assert.ok(styles.includes('@media (max-width: 320px)'));
  assert.ok(header.includes(':aria-label="`${connectionLabel}，${freshnessLabel}`"'));
});