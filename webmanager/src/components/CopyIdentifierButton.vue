<script setup>
import { Check, Copy } from '@lucide/vue';
import { onBeforeUnmount, ref } from 'vue';
import { useI18n } from 'vue-i18n';

import { copyIdentifier } from '../clipboard.js';

const props = defineProps({
  label: { type: String, required: true },
  value: { type: String, default: '' },
  variant: { type: String, default: 'compact' },
});

const copied = ref(false);
const { t } = useI18n();
let resetTimer = null;

async function copy() {
  if (!await copyIdentifier(props.value)) return;
  copied.value = true;
  clearTimeout(resetTimer);
  resetTimer = setTimeout(() => { copied.value = false; }, 1200);
}

onBeforeUnmount(() => clearTimeout(resetTimer));
</script>

<template>
  <button
    :class="variant === 'tool' ? 'tool-icon-button' : 'copy-button'"
    type="button"
    :disabled="!value"
    :title="t(copied ? 'common.copied' : 'common.copy', { label })"
    @click="copy"
  >
    <Check v-if="copied" :size="14" aria-hidden="true" />
    <Copy v-else :size="14" aria-hidden="true" />
    <span class="visually-hidden" aria-live="polite">{{ t(copied ? 'common.copied' : 'common.copy', { label }) }}</span>
  </button>
</template>