import { createApp } from 'vue';

import App from './App.vue';
import { createManagerI18n, resolveInitialLocale } from './i18n.js';
import './styles.css';

const initialLocale = resolveInitialLocale({
	storage: window.localStorage,
	languages: navigator.languages,
});
const i18n = createManagerI18n(initialLocale);

document.documentElement.lang = initialLocale;
document.title = i18n.global.t('app.title');

createApp(App).use(i18n).mount('#app');