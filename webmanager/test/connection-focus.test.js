import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import { fastestEndpointValues } from '../src/focus.js';
import en from '../src/locales/en.js';
import zhCN from '../src/locales/zh-CN.js';

test('fastest endpoint display prefers endpoints and falls back safely', () => {
  assert.deepEqual(fastestEndpointValues({
    local_endpoint: '192.0.2.10:41000',
    local_address: '192.0.2.10',
    remote_endpoint: '198.51.100.20:9443',
  }), {
    local: '192.0.2.10:41000',
    remote: '198.51.100.20:9443',
  });
  assert.deepEqual(fastestEndpointValues({ local_address: '192.0.2.10' }), {
    local: '192.0.2.10',
    remote: '--',
  });
  assert.deepEqual(fastestEndpointValues({ local_endpoint: '', local_address: '' }), {
    local: '--',
    remote: '--',
  });
});

test('ConnectionFocus renders endpoint fields before transport and retry', async () => {
  const component = await readFile(new URL('../src/components/ConnectionFocus.vue', import.meta.url), 'utf8');
  const local = component.indexOf("t('focus.localEndpoint'");
  const remote = component.indexOf("t('focus.remoteEndpoint'");
  const transport = component.indexOf("{{ fastest.transport || '--' }}");
  const retry = component.indexOf("t('focus.retry'");
  assert.ok(local >= 0 && local < remote && remote < transport && transport < retry);
  assert.ok(component.includes('fastestEndpointValues(fastest.value)'));
  assert.ok(component.includes('class="mono"'));

  const styles = await readFile(new URL('../src/styles.css', import.meta.url), 'utf8');
  assert.ok(styles.includes('.fastest-meta > .mono { min-width: 0; overflow-wrap: anywhere; }'));
});

test('endpoint labels are aligned and explicit in both catalogs', () => {
  assert.equal(zhCN.focus.localEndpoint, '本地 {value}');
  assert.equal(zhCN.focus.remoteEndpoint, '远端 {value}');
  assert.equal(en.focus.localEndpoint, 'Local {value}');
  assert.equal(en.focus.remoteEndpoint, 'Remote {value}');
});
