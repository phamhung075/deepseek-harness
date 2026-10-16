import tsconfigPaths from 'vite-tsconfig-paths'
import { defineConfig } from 'vitest/config'

// Fork tests are outside the upstream workspace globs; run with
// `pnpm exec vitest run --config fork/vitest.config.ts`.
export default defineConfig({
  root: new URL('..', import.meta.url).pathname,
  plugins: [tsconfigPaths({ projects: ['./tsconfig.base.json'] })],
  test: { include: ['fork/**/tests/**/*.spec.ts'] },
})
