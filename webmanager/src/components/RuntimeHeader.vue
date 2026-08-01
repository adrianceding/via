<script setup>
import { Languages, Pause, Play, RefreshCw } from '@lucide/vue';
import { computed } from 'vue';
import { useI18n } from 'vue-i18n';

const props = defineProps({
  connectionLabel: { type: String, required: true },
  freshnessLabel: { type: String, required: true },
  generatedAt: { type: String, default: '' },
  healthLevel: { type: String, required: true },
  healthReasons: { type: Array, required: true },
  locale: { type: String, required: true },
  paused: { type: Boolean, required: true },
  refreshing: { type: Boolean, required: true },
  role: { type: Number, default: 0 },
  title: { type: String, required: true },
});

defineEmits(['change-locale', 'refresh', 'toggle-pause']);

const { t } = useI18n();

const generatedLabel = computed(() => {
  if (!props.generatedAt) return '--';
  const date = new Date(props.generatedAt);
  return Number.isNaN(date.valueOf()) ? '--' : date.toLocaleString(props.locale, { hour12: false });
});
</script>

<template>
  <header class="runtime-header">
    <div>
      <p class="product-name">VIA</p>
      <h1>{{ title }}</h1>
    </div>
    <div class="runtime-actions">
        <div class="connection-state" :aria-label="`${connectionLabel}，${freshnessLabel}`" aria-live="polite">
      <span class="health-dot" :class="healthLevel" aria-hidden="true" />
      <div>
        <strong :title="healthReasons.join('; ')">{{ connectionLabel }}</strong>
        <time :datetime="generatedAt" :title="generatedLabel">{{ freshnessLabel }}</time>
      </div>
      </div>
      <label class="locale-control" :title="t('locale.label')">
        <Languages :size="17" aria-hidden="true" />
        <span class="visually-hidden">{{ t('locale.label') }}</span>
        <select :value="locale" :aria-label="t('locale.label')" @change="$emit('change-locale', $event.target.value)">
          <option value="zh-CN">{{ t('locale.chinese') }}</option>
          <option value="en">{{ t('locale.english') }}</option>
        </select>
      </label>
      <button class="header-icon-button" type="button" :disabled="refreshing" :title="t('header.refresh')" :aria-label="t('header.refresh')" @click="$emit('refresh')">
        <RefreshCw :size="17" :class="{ spinning: refreshing }" aria-hidden="true" />
      </button>
      <button class="header-icon-button" type="button" :title="paused ? t('header.resume') : t('header.pause')" :aria-label="paused ? t('header.resume') : t('header.pause')" @click="$emit('toggle-pause')">
        <Play v-if="paused" :size="17" aria-hidden="true" />
        <Pause v-else :size="17" aria-hidden="true" />
      </button>
    </div>
  </header>
</template>