/// <reference types="vitest/config" />
import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';

const BACKEND = 'http://127.0.0.1:8080';

export default defineConfig({
  plugins: [svelte()],
  // Relative asset URLs so the SPA works under any mount point when embedded.
  base: './',
  build: {
    outDir: 'dist',
  },
  server: {
    proxy: {
      // ws entry must come first and carry ws:true.
      '/api/v1/ws': { target: BACKEND, ws: true },
      '/api': { target: BACKEND, changeOrigin: true },
    },
  },
  test: {
    // format.ts is pure logic; no DOM needed.
    environment: 'node',
    include: ['src/**/*.test.ts'],
  },
});
