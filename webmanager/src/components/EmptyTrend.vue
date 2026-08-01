<script setup>
import { useI18n } from 'vue-i18n';

const { t } = useI18n();
</script>

<template>
  <section class="trend-section" aria-labelledby="trend-heading">
    <header class="section-heading">
      <div><p class="eyebrow">{{ t('trend.eyebrow') }}</p><h2 id="trend-heading">{{ t('trend.title') }}</h2></div>
      <span class="count">0</span>
    </header>
    <p class="empty-block compact-empty">{{ t('trend.empty') }}</p>
  </section>
</template>