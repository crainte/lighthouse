/// <reference types="vitest/config" />
import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';
import { svelteTesting } from '@testing-library/svelte/vite';

const BACKEND = process.env.WARDEN_BACKEND ?? 'http://127.0.0.1:8080';

export default defineConfig({
  plugins: [svelte(), svelteTesting()],
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
    // `lib/` is pure logic and needs no DOM; components obviously do. Every
    // frontend test used to live in the first group, which meant a template
    // that referenced an undefined variable — a guaranteed crash on render —
    // was unreachable by the whole suite. `environment: 'jsdom'` costs little
    // and makes the rendering half testable at all.
    environment: 'jsdom',
    include: ['src/**/*.test.ts', 'src/**/*.test.svelte.ts'],
    setupFiles: ['src/test/setup.ts'],
  },
});
