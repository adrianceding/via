import { fileURLToPath, URL } from 'node:url';

import vue from '@vitejs/plugin-vue';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [vue()],
  build: {
    outDir: fileURLToPath(new URL('../internal/status/webmanager/dist', import.meta.url)),
    emptyOutDir: true,
  },
});