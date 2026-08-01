<script setup>
import { useI18n } from 'vue-i18n';

import ActiveFlowTable from './ActiveFlowTable.vue';
import TerminalFlowTable from './TerminalFlowTable.vue';

defineProps({
  flows: { type: Array, required: true },
  flowTotal: { type: Number, required: true },
  terminals: { type: Array, required: true },
  terminalTotal: { type: Number, required: true },
});

defineEmits(['filter']);

const activeTab = defineModel('activeTab', { type: String, required: true });
const { t } = useI18n();
</script>

<template>
  <section class="data-section flow-section">
    <header class="section-heading flow-heading">
      <div><p class="eyebrow">{{ t('flows.eyebrow') }}</p><h2>{{ t('flows.title') }}</h2></div>
      <div class="flow-tabs" role="tablist" :aria-label="t('flows.views')">
        <button type="button" role="tab" :aria-selected="activeTab === 'active'" @click="activeTab = 'active'">
          {{ t('flows.active') }} <span>{{ flows.length }} / {{ flowTotal }}</span>
        </button>
        <button type="button" role="tab" :aria-selected="activeTab === 'terminal'" @click="activeTab = 'terminal'">
          {{ t('flows.terminal') }} <span>{{ terminals.length }} / {{ terminalTotal }}</span>
        </button>
      </div>
    </header>
    <ActiveFlowTable v-if="activeTab === 'active'" :flows="flows" @filter="$emit('filter', $event)" />
    <TerminalFlowTable v-else :terminals="terminals" @filter="$emit('filter', $event)" />
  </section>
</template>