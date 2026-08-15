<script setup>
import { nextTick, ref } from 'vue';
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
const activeTabButton = ref(null);
const terminalTabButton = ref(null);

function selectTab(tab) {
  activeTab.value = tab;
}

async function handleTabKeydown(event, tab) {
  let nextTab = null;
  if (event.key === 'ArrowRight' || event.key === 'ArrowDown') {
    event.preventDefault();
    nextTab = tab === 'active' ? 'terminal' : 'active';
  } else if (event.key === 'ArrowLeft' || event.key === 'ArrowUp') {
    event.preventDefault();
    nextTab = tab === 'terminal' ? 'active' : 'terminal';
  }
  if (!nextTab) return;
  selectTab(nextTab);
  await nextTick();
  (nextTab === 'active' ? activeTabButton.value : terminalTabButton.value)?.focus();
}
</script>

<template>
  <section class="data-section flow-section">
    <header class="section-heading flow-heading">
      <div><p class="eyebrow">{{ t('flows.eyebrow') }}</p><h2>{{ t('flows.title') }}</h2></div>
      <div class="flow-tabs" role="tablist" :aria-label="t('flows.views')">
        <button
          ref="activeTabButton"
          id="flow-tab-active"
          type="button"
          role="tab"
          aria-controls="flow-panel-active"
          :aria-selected="activeTab === 'active'"
          :tabindex="activeTab === 'active' ? 0 : -1"
          @click="selectTab('active')"
          @keydown="handleTabKeydown($event, 'active')"
        >
          {{ t('flows.active') }} <span>{{ flows.length }} / {{ flowTotal }}</span>
        </button>
        <button
          ref="terminalTabButton"
          id="flow-tab-terminal"
          type="button"
          role="tab"
          aria-controls="flow-panel-terminal"
          :aria-selected="activeTab === 'terminal'"
          :tabindex="activeTab === 'terminal' ? 0 : -1"
          @click="selectTab('terminal')"
          @keydown="handleTabKeydown($event, 'terminal')"
        >
          {{ t('flows.terminal') }} <span>{{ terminals.length }} / {{ terminalTotal }}</span>
        </button>
      </div>
    </header>
    <div
      v-if="activeTab === 'active'"
      id="flow-panel-active"
      role="tabpanel"
      aria-labelledby="flow-tab-active"
      tabindex="0"
    >
      <ActiveFlowTable :flows="flows" @filter="$emit('filter', $event)" />
    </div>
    <div
      v-else
      id="flow-panel-terminal"
      role="tabpanel"
      aria-labelledby="flow-tab-terminal"
      tabindex="0"
    >
      <TerminalFlowTable :terminals="terminals" @filter="$emit('filter', $event)" />
    </div>
  </section>
</template>