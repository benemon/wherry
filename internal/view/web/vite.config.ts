import react from '@vitejs/plugin-react';
import { defineConfig } from 'vitest/config';
import { notices } from './notices.ts';

// The page is embedded in the wherry binary and served from its origin alone, so every asset is a file under
// /assets: nothing is inlined as a data: URI, which the page's policy allows for images only.
export default defineConfig({
  plugins: [react(), notices()],
  build: {
    outDir: '../static',
    emptyOutDir: true,
    assetsInlineLimit: 0,
    sourcemap: false,
    reportCompressedSize: false,
    chunkSizeWarningLimit: 2048,
  },
  test: {
    environment: 'node',
    include: ['src/**/*.test.ts', 'src/**/*.test.tsx'],
  },
});
