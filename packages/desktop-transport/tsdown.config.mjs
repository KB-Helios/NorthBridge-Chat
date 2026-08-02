import { defineConfig } from 'tsdown';

export default defineConfig({
  entry: ['src/index.ts'],
  format: ['esm', 'cjs'],
  platform: 'browser',
  dts: { oxc: true },
  outDir: 'dist',
  sourcemap: true,
  fixedExtension: true,
  deps: {
    neverBundle: true,
  },
});
