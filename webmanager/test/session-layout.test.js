import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

test('session table merges connection identifiers without horizontal overflow', async () => {
  const component = await readFile(new URL('../src/components/SessionTable.vue', import.meta.url), 'utf8');
  const styles = await readFile(new URL('../src/styles.css', import.meta.url), 'utf8');

  assert.equal(component.match(/<col class="session-col-/g)?.length, 8);
  assert.ok(component.includes("client ? t('sessions.clientSource') : t('sessions.serverSource')"));
  assert.ok(!component.includes("<th>{{ t('common.connectionId') }}</th>"));
  assert.ok(component.includes('colspan="8"'));
  for (const titleKey of [
    'sessions.smoothedRttTitle',
    'sessions.retryTitle',
    'sessions.stallTitle',
    'sessions.queueTitle',
  ]) {
    assert.ok(component.includes(`:title="t('${titleKey}')"`), `missing session header description: ${titleKey}`);
  }
  assert.equal(component.match(/:data-label=/g)?.length, 8);
  assert.ok(styles.includes('content: attr(data-label);'));
  assert.ok(styles.includes('.sessions-table { min-width: 100%; }'));
  for (const rule of [
    '.session-col-source { width: 28%; }',
    '.session-col-endpoints { width: 20%; }',
    '.session-col-state { width: 8%; }',
    '.session-col-rtt { width: 9%; }',
    '.session-col-retry { width: 9%; }',
    '.session-col-stall { width: 9%; }',
    '.session-col-queue { width: 11%; }',
    '.session-col-role { width: 6%; }',
  ]) {
    assert.ok(styles.includes(rule), `missing session column rule: ${rule}`);
  }
});