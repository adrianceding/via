<script setup>
import { ChevronDown, ChevronUp } from '@lucide/vue';
import { computed, ref } from 'vue';
import { useI18n } from 'vue-i18n';

import { selectFastestSession } from '../quality.js';
import { interfaceReasons } from '../status.js';

const props = defineProps({
  interfaces: { type: Array, required: true },
  filtered: { type: Boolean, required: true },
  sessions: { type: Array, required: true },
  total: { type: Number, required: true },
});

defineEmits(['filter']);
const { t } = useI18n();

const fastest = computed(() => selectFastestSession(props.sessions));
const expanded = ref(false);
const sortedInterfaces = computed(() => [...props.interfaces].sort((left, right) => Number(left.reason === 1) - Number(right.reason === 1)
  || String(left.name).localeCompare(String(right.name))));
const visibleInterfaces = computed(() => {
  if (expanded.value || props.filtered || sortedInterfaces.value.length <= 3) return sortedInterfaces.value;
  const visible = sortedInterfaces.value.slice(0, 3);
  const fastestItem = sortedInterfaces.value.find((item) => item.name === fastest.value?.interface);
  if (fastestItem && !visible.includes(fastestItem)) visible.push(fastestItem);
  return visible;
});

function readySessions(name) {
  return props.sessions.filter((session) => session.interface === name && session.state === 3).length;
}
</script>

<template>
  <section class="data-section interfaces-section">
    <header class="section-heading">
      <div><p class="eyebrow">{{ t('interfaces.eyebrow') }}</p><h2>{{ t('interfaces.title') }}</h2></div>
      <div class="section-actions">
        <button v-if="!filtered && sortedInterfaces.length > 3" class="tool-icon-button" type="button" :title="expanded ? t('interfaces.collapse') : t('interfaces.expand')" :aria-label="expanded ? t('interfaces.collapse') : t('interfaces.expand')" @click="expanded = !expanded">
          <ChevronUp v-if="expanded" :size="17" aria-hidden="true" />
          <ChevronDown v-else :size="17" aria-hidden="true" />
        </button>
        <span class="count">{{ visibleInterfaces.length }} / {{ total }}</span>
      </div>
    </header>
    <p v-if="interfaces.length === 0" class="empty-block">{{ t('interfaces.serverEmpty') }}</p>
    <div v-else class="interface-list">
      <article
        v-for="item in visibleInterfaces"
        :key="item.index"
        class="interface-item"
        :class="{ selected: fastest?.interface === item.name }"
      >
        <div>
          <div class="primary-cell"><button class="identifier-link" type="button" @click="$emit('filter', item.name)">{{ item.name }}</button><span v-if="fastest?.interface === item.name" class="fastest-badge">{{ t('interfaces.fastest') }}</span></div>
          <p class="mono">{{ item.addresses?.length ? item.addresses.join(', ') : '--' }}</p>
        </div>
        <dl>
          <div><dt>{{ t('interfaces.filter') }}</dt><dd>{{ interfaceReasons[item.reason] ? t(interfaceReasons[item.reason]) : t('common.unknown') }}</dd></div>
          <div><dt>{{ t('interfaces.readySessions') }}</dt><dd>{{ readySessions(item.name) }}</dd></div>
        </dl>
      </article>
    </div>
  </section>
</template>