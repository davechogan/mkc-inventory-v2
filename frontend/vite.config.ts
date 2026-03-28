import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  base: '/static/dist/',
  server: {
    port: 5173,
    proxy: {
      '/api': { target: 'http://localhost:8008', changeOrigin: true },
      '/static': { target: 'http://localhost:8008', changeOrigin: true },
    },
  },
  build: {
    outDir: '../static/dist',
    emptyOutDir: true,
  },
});
