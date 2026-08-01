<script setup>
import { X } from '@lucide/vue';
import { useI18n } from 'vue-i18n';

import CopyIdentifierButton from './CopyIdentifierButton.vue';

defineProps({
  anomalyCount: { type: Number, required: true },
  shareUrl: { type: String, required: true },
  truncated: { type: Boolean, default: false },
});

defineEmits(['clear']);

const query = defineModel('query', { type: String, required: true });
const onlyAnomalies = defineModel('onlyAnomalies', { type: Boolean, required: true });
const { t } = useI18n();
</script>

<template>
  <section class="manager-tools" :aria-label="t('filter.label')">
    <label class="search-field">
      <span>{{ t('filter.label') }}</span>
      <input v-model="query" type="search" autocomplete="off" :placeholder="t('filter.placeholder')">
    </label>
    <label class="anomaly-filter">
      <input v-model="onlyAnomalies" type="checkbox">
      <span>{{ t('filter.anomalies', { count: anomalyCount }) }}</span>
    </label>
    <CopyIdentifierButton :label="t('filter.share')" :value="shareUrl" variant="tool" />
    <button v-if="query || onlyAnomalies" class="tool-icon-button" type="button" :title="t('filter.clear')" :aria-label="t('filter.clear')" @click="$emit('clear')">
      <X :size="17" aria-hidden="true" />
    </button>
    <p v-if="truncated" class="truncation-warning" role="status">
      {{ t('filter.truncated') }}
    </p>
  </section>
</template>