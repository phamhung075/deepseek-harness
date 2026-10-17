# fork/

Fork-only plugins. They edit no upstream file, so merging `upstream/master` does not conflict with them. They sit outside the upstream workspace globs (`packages/*/*`), so upstream package gates, lockfile and registries do not apply.

## Plugins

| Plugin | What it does |
|---|---|
| [`live-sync`](live-sync/src/index.ts) | Publishes each root Agent's live Assistant frames to `ctx.sessionStreams`, so another Host can follow its tokens. Subagents are skipped unless `publishSubagents: true`. It does nothing when no `sessionStreams` service is registered. |
| [`job-control`](job-control/src/index.ts) | Steers a running root Agent from another process and exposes what it holds. Lines appended to `<session>.control.jsonl` (`{"text": "..."}`) go to `agent.steer(...)` at the next step; lines present at attach time are ignored. After each completed reply it writes `<session>.usage.json` (`totalTokens`, `surfaceTokens`, `nodes`, `logRevision`) from `ctx.tokenMeter`. Needs a source launch (`pnpm dsh`, tsx), because it imports `@deepseek-ai/dsh-llm` at runtime. |

## Launch with the plugins

Plain `dsh` does not load them. Add each plugin's overlay with `--patch`; the flag can repeat.

```sh
pnpm dsh web --patch fork/live-sync/live-sync.overlay.yml
pnpm dsh --profile acp --patch fork/live-sync/live-sync.overlay.yml
make dev-web ARGS='--patch fork/live-sync/live-sync.overlay.yml'
```

`deepseek-offload`'s bridge adds every `fork/<name>/<name>.overlay.yml` to each ACP child automatically (`DSH_OFFLOAD_FORK_OVERLAYS=0` disables it). Then `node .agents/deepseek-offload/.agents/skills/deepseek-offload/scripts/session-steer.mjs <jobId> "text"` steers a job, and `... <jobId> --usage` prints its usage snapshot.

To check composition without starting the app, add `--dump-config` and look for `fork-live-sync`.

## Test and type check

```sh
pnpm exec vitest run --config fork/vitest.config.ts
cd fork/live-sync && pnpm exec tsc -p tsconfig.json 2>&1 | grep '^src/'
```

The `grep` hides errors under `vendor/`, which appear because the fork tsconfig has no project references. Only `src/` errors count.

## Add a plugin

1. Create `fork/<name>/src/index.ts` exporting `apply(ctx, config)` (and `Config` if it takes options). Use only `import type` from `@deepseek-ai/*` packages so the plugin needs no `package.json` or workspace entry.
2. Register listeners with `ctx.on` and cleanup with `ctx.effect`. Do not edit upstream files.
3. Add `fork/<name>/<name>.overlay.yml` with an `- insert:` entry whose `name` is `./src/index.ts` (resolved relative to the overlay file).
4. Add tests under `fork/<name>/tests/` and a row in the table above.

## Still edited in place in upstream files

The jsonl provider wiring, session-controller (`history.ts`, `list.ts`, `index.ts`), the session-format anchored decode and released codecs, and the `session-persistence` seam exports are not yet extracted. Expect conflicts there on merge.

Merge helper: `~/.local/bin/dsh-sync-upstream` merges `upstream/master`, regenerates generated docs on conflict, and stops on source conflicts.
