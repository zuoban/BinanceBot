import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';
import { readFileSync } from 'node:fs';

import { designTokensCss, palettes } from './src/lib/designTokens.ts';
const cargoToml = readFileSync(new URL('../Cargo.toml', import.meta.url), 'utf8');
const version = cargoToml.match(/^version\s*=\s*"([^"]+)"/m)?.[1];
if (!version) throw new Error('无法从 Cargo.toml 读取项目版本号');

export default defineConfig({
  plugins: [react(), {
    name: 'design-tokens',
    transformIndexHtml() {
      return [
        { tag: 'meta', attrs: { charset: 'UTF-8' }, injectTo: 'head-prepend' as const },
        { tag: 'style', attrs: { id: 'design-tokens' }, children: designTokensCss, injectTo: 'head-prepend' as const },
        { tag: 'meta', attrs: { name: 'theme-color', content: palettes.light['--bg-page'], 'data-dark': palettes.dark['--bg-page'] }, injectTo: 'head-prepend' as const },
      ];
    },
  }],
  define: { 'import.meta.env.VITE_APP_VERSION': JSON.stringify(version) },
  build: {
    rollupOptions: {
      output: {
        manualChunks(id) {
          const normalized = id.replaceAll('\\', '/');
          if (!normalized.includes('/node_modules/')) return;
          if (normalized.includes('/@ant-design/icons') || normalized.includes('/@ant-design/cssinjs')) return 'antd-core';
          if (normalized.includes('/react/') || normalized.includes('/react-dom/') || normalized.includes('/react-router') || normalized.includes('/@tanstack/')) return 'vendor';
        },
      },
    },
  },
  server: {
    proxy: {
      '/api': { target: process.env.API_PROXY_TARGET || 'http://127.0.0.1:8080', ws: true },
      '/ws': { target: process.env.API_PROXY_TARGET || 'http://127.0.0.1:8080', ws: true },
    },
  },
  test: { environment: 'jsdom', setupFiles: ['./src/test/setup.ts'], restoreMocks: true, testTimeout: 30000, maxWorkers: 2 },
});
