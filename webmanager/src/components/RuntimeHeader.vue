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
  <header class="topbar">
    <h1 class="topbar-title">{{ title }}</h1>
    <div class="topbar-meta">
      <span class="chip" :class="healthLevel">
        <span class="dot" :class="healthLevel" aria-hidden="true" />
        <strong>{{ connectionLabel }}</strong>
        <span v-if="healthReasons.length" class="chip-detail" :title="healthReasons.join('; ')">{{ healthReasons[0] }}</span>
        <time :datetime="generatedAt" :title="generatedLabel">{{ freshnessLabel }}</time>
      </span>
      <label class="locale-control" :title="t('locale.label')">
        <Languages :size="15" aria-hidden="true" />
        <span class="visually-hidden">{{ t('locale.label') }}</span>
        <select :value="locale" :aria-label="t('locale.label')" @change="$emit('change-locale', $event.target.value)">
          <option value="zh-CN">{{ t('locale.chinese') }}</option>
          <option value="en">{{ t('locale.english') }}</option>
        </select>
      </label>
      <button class="icon-btn" type="button" :disabled="refreshing" :title="t('header.refresh')" :aria-label="t('header.refresh')" @click="$emit('refresh')">
        <RefreshCw :size="15" :class="{ spinning: refreshing }" aria-hidden="true" />
      </button>
      <button class="icon-btn" type="button" :title="paused ? t('header.resume') : t('header.pause')" :aria-label="paused ? t('header.resume') : t('header.pause')" @click="$emit('toggle-pause')">
        <Play v-if="paused" :size="15" aria-hidden="true" />
        <Pause v-else :size="15" aria-hidden="true" />
      </button>
    </div>
  </header>
</template>