# fork/

Fork-only plugins that edit no upstream file, so `upstream/master` merges do not conflict with them. They sit outside the upstream workspace globs and gates.

| Plugin | Role |
|---|---|
| [`live-sync`](live-sync/src/index.ts) | Publishes root Agents' live Assistant frames to `ctx.sessionStreams` |

Load a plugin with its overlay: `pnpm dsh --profile <profile> --patch fork/live-sync/live-sync.overlay.yml`.

Test: `pnpm exec vitest run --config fork/vitest.config.ts`. Type check: `cd fork/live-sync && pnpm exec tsc -p tsconfig.json 2>&1 | grep '^src/'` (errors under `vendor/` are expected without project references).
