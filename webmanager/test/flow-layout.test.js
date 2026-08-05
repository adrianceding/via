import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

test('flow views use compact semantic columns without fixed overflow widths', async () => {
  const active = await readFile(new URL('../src/components/ActiveFlowTable.vue', import.meta.url), 'utf8');
  const terminal = await readFile(new URL('../src/components/TerminalFlowTable.vue', import.meta.url), 'utf8');
  const section = await readFile(new URL('../src/components/FlowSection.vue', import.meta.url), 'utf8');

  assert.equal(active.match(/<col class="active-flow-col-/g)?.length, 5);
  assert.equal(terminal.match(/<col class="terminal-flow-col-/g)?.length, 3);
  assert.ok(active.includes('v-if="flows.length === 0" class="empty-block compact-empty"'));
  assert.ok(terminal.includes('v-if="terminals.length === 0" class="empty-block compact-empty"'));
  assert.ok(section.includes('role="tablist"'));
});