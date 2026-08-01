<script setup>
import { useI18n } from 'vue-i18n';

import { formatTimestamp } from '../format.js';
import { deliveryModes, flowStates, pathSelections, transitionReasons } from '../status.js';
import CopyIdentifierButton from './CopyIdentifierButton.vue';

defineProps({
  terminals: { type: Array, required: true },
});

defineEmits(['filter']);
const { locale, t } = useI18n();
</script>

<template>
  <p v-if="terminals.length === 0" class="empty-block compact-empty">{{ t('flows.emptyTerminal') }}</p>
  <div v-else class="table-wrap">
    <table class="terminals-table">
      <colgroup>
        <col class="terminal-flow-col-identity">
        <col class="terminal-flow-col-result">
        <col class="terminal-flow-col-time">
      </colgroup>
        <thead><tr><th>{{ t('flows.identityPolicy') }}</th><th>{{ t('flows.result') }}</th><th>{{ t('flows.time') }}</th></tr></thead>
        <tbody>
          <tr v-for="flow in terminals" :key="flow.id">
            <td :data-label="t('flows.identityPolicy')">
              <div class="identifier-cell mono"><button class="identifier-link" type="button" @click="$emit('filter', flow.flow_id)">{{ flow.flow_id || '--' }}</button><CopyIdentifierButton :label="t('common.flowId')" :value="flow.flow_id" /></div>
              <small class="mono">{{ t('common.local') }} {{ String(flow.id || '').slice(0, 12) }}</small>
              <small>{{ deliveryModes[flow.delivery_mode] ? t(deliveryModes[flow.delivery_mode]) : t('common.unknownMode') }} · {{ pathSelections[flow.path_selection] ? t(pathSelections[flow.path_selection]) : '--' }}</small>
            </td>
            <td :data-label="t('flows.result')"><span class="state neutral">{{ flowStates[flow.state] ? t(flowStates[flow.state]) : t('common.unknown') }}</span><small>{{ transitionReasons[flow.reason] ? t(transitionReasons[flow.reason]) : t('common.unknown') }}</small></td>
            <td class="metric-lines" :data-label="t('flows.time')">
              <span><small>{{ t('flows.started') }}</small>{{ formatTimestamp(flow.started_at, locale) }}</span>
              <span><small>{{ t('flows.finished') }}</small>{{ formatTimestamp(flow.finished_at, locale) }}</span>
            </td>
          </tr>
        </tbody>
    </table>
  </div>
</template>