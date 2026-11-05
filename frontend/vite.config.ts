import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';
import { readFileSync } from 'node:fs';

const cargoToml = readFileSync(new URL('../Cargo.toml', import.meta.url), 'utf8');
const version = cargoToml.match(/^version\s*=\s*"([^"]+)"/m)?.[1];
if (!version) throw new Error('无法从 Cargo.toml 读取项目版本号');

export default defineConfig({
  plugins: [react()],
  define: { 'import.meta.env.VITE_APP_VERSION': JSON.stringify(version) },
  server: {
    proxy: {
      '/api': { target: process.env.API_PROXY_TARGET || 'http://127.0.0.1:8080', ws: true },
      '/ws': { target: process.env.API_PROXY_TARGET || 'http://127.0.0.1:8080', ws: true },
    },
  },
  build: {
    rolldownOptions: {
      output: {
        manualChunks(id) {
          if (!id.includes('node_modules')) return undefined;
          if (id.includes('recharts') || id.includes('d3-')) return 'charts';
          if (id.includes('rc-table') || id.includes('antd/es/table') || id.includes('@rc-component/table')) return 'antd-table';
          if (id.includes('rc-select') || id.includes('rc-tree-select') || id.includes('rc-cascader') || id.includes('antd/es/select') || id.includes('@rc-component/select')) return 'antd-select';
          if (id.includes('rc-field-form') || id.includes('antd/es/form') || id.includes('@rc-component/form')) return 'antd-form';
          if (id.includes('@ant-design/icons')) return 'icons';
          if (id.includes('/react/') || id.includes('/react-dom/') || id.includes('react-router') || id.includes('@tanstack/react-query')) return 'framework';
          return undefined;
        },
      },
    },
  },
  test: { environment: 'jsdom', setupFiles: ['./src/test/setup.ts'], restoreMocks: true, testTimeout: 30000, maxWorkers: 2 },
});
