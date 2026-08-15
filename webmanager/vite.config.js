import { fileURLToPath, URL } from 'node:url';

import vue from '@vitejs/plugin-vue';
import { defineConfig } from 'vite';

const statusAPITarget = process.env.VIA_STATUS_API_TARGET?.trim();

export default defineConfig({
  plugins: [vue()],
  server: statusAPITarget ? {
    proxy: {
      '/api': {
        target: statusAPITarget,
        changeOrigin: true,
      },
    },
  } : undefined,
  build: {
    outDir: fileURLToPath(new URL('../internal/status/webmanager/dist', import.meta.url)),
    emptyOutDir: true,
  },
});