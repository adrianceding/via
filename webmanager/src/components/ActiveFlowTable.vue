<script setup>
import { useI18n } from 'vue-i18n';

import { formatBytes } from '../format.js';
import { adaptiveStates, adaptiveTransitions, deliveryModes, flowStates, pathSelections } from '../status.js';
import CopyIdentifierButton from './CopyIdentifierButton.vue';

defineProps({
  flows: { type: Array, required: true },
});

defineEmits(['filter']);
const { t } = useI18n();

function stateClass(state) {
  if (state === 3) return 'good';
  if ([1, 2, 4, 5, 7].includes(state)) return 'warning';
  return 'neutral';
}
</script>

<template>
  <p v-if="flows.length === 0" class="empty-block compact-empty">{{ t('flows.emptyActive') }}</p>
  <div v-else class="table-wrap">
    <table class="flows-table">
      <colgroup>
        <col class="active-flow-col-identity">
        <col class="active-flow-col-policy">
        <col class="active-flow-col-route">
        <col class="active-flow-col-progress">
        <col class="active-flow-col-overhead">
      </colgroup>
        <thead><tr>
          <th>{{ t('flows.identityState') }}</th><th>{{ t('flows.policyAdaptive') }}</th><th>{{ t('flows.route') }}</th><th>{{ t('flows.progress') }}</th><th>{{ t('flows.overhead') }}</th>
        </tr></thead>
        <tbody>
          <tr v-for="flow in flows" :key="flow.id">
            <td :data-label="t('flows.identityState')">
              <div class="identifier-cell mono"><button class="identifier-link" type="button" @click="$emit('filter', flow.flow_id)">{{ flow.flow_id || '--' }}</button><CopyIdentifierButton :label="t('common.flowId')" :value="flow.flow_id" /></div>
              <small class="mono">{{ t('common.local') }} {{ String(flow.id || '').slice(0, 12) }}</small>
              <span class="state flow-state" :class="stateClass(flow.state)">{{ flowStates[flow.state] ? t(flowStates[flow.state]) : t('common.unknown') }}</span>
            </td>
            <td :data-label="t('flows.policyAdaptive')">
              <div class="primary-cell">{{ deliveryModes[flow.delivery_mode] ? t(deliveryModes[flow.delivery_mode]) : t('common.unknownMode') }}</div>
              <small>{{ pathSelections[flow.path_selection] ? t(pathSelections[flow.path_selection]) : '--' }} · {{ adaptiveStates[flow.adaptive_state] ? t(adaptiveStates[flow.adaptive_state]) : '--' }}</small>
              <small v-if="adaptiveTransitions[flow.adaptive_transition]">{{ t(adaptiveTransitions[flow.adaptive_transition]) }}</small>
            </td>
            <td :data-label="t('flows.route')">
              <div>{{ t('flows.attachments', { published: Number(flow.published_attachments || 0), policy: Number(flow.policy_attachments || 0) }) }}</div>
              <button v-if="flow.preferred_connection_id" class="identifier-link mono" type="button" @click="$emit('filter', flow.preferred_connection_id)">{{ flow.preferred_connection_id }}</button>
              <small v-else>{{ t('flows.noPreferredConnection') }}</small>
            </td>
            <td class="metric-lines" :data-label="t('flows.progress')">
              <span><small>{{ t('flows.unacknowledged') }}</small>{{ formatBytes(flow.unacknowledged_bytes) }}</span>
              <span><small>{{ t('flows.acknowledged') }}</small>{{ formatBytes(flow.tx_acknowledged_offset) }}</span>
              <span><small>{{ t('flows.received') }}</small>{{ formatBytes(flow.rx_written_offset) }}</span>
            </td>
            <td class="metric-lines" :data-label="t('flows.overhead')">
              <span><small>{{ t('flows.retransmitted') }}</small>{{ formatBytes(flow.retransmitted_bytes) }}</span>
              <span><small>{{ t('flows.redundant') }}</small>{{ formatBytes(flow.redundant_bytes) }}</span>
            </td>
          </tr>
        </tbody>
    </table>
  </div>
</template>