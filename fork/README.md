# fork/

English | [中文](README.zh.md)

Fork-only plugins. They edit no upstream file, so merging `upstream/master` does not conflict with them. They sit outside the upstream workspace globs (`packages/*/*`), so upstream package gates, lockfile and registries do not apply.

## Plugins

| Plugin | What it does |
|---|---|
| [`live-sync`](live-sync/src/index.ts) | Publishes each root Agent's live Assistant frames to `ctx.sessionStreams`, so another Host can follow its tokens. Subagents are skipped unless `publishSubagents: true`. It does nothing when no `sessionStreams` service is registered. |
| [`live-writer`](live-writer/src/index.ts) | Presents a Session another process is still writing as it is stored. A cold read cannot tell a live writer from a dead one, so it balances an open turn with synthetic `interruptedTurnClosers`; against a running job that fabricated a stop after every reply and moved the follower's cursor past appends the writer had not made. While the writer's kernel lock is held (`/proc/locks`, Linux), the plugin exposes the stored prefix and its exact cursor instead. It wraps `sessionQuery` after that service resolves and restores it on unload. |
| [`job-control`](job-control/src/index.ts) | Steers a running root Agent from another process and exposes what it holds. Lines appended to `<session>.control.jsonl` (`{"text": "..."}`) go to `agent.steer(...)` at the next step; lines present at attach time are ignored. After each completed reply it writes `<session>.usage.json` (`totalTokens`, `surfaceTokens`, `nodes`, `logRevision`) from `ctx.tokenMeter`. Its entry points are `.ts` sources, so it needs a source launch (`pnpm dsh`, tsx). |
| [`session-watch`](session-watch/src/index.ts) | Announces stored Sessions another process creates, so a dashboard that listed the corpus once adds their rows without a reload. Every `pollIntervalMs` (default `5000`) it re-reads the stored corpus; for each new identity it publishes the `api-session/added` summary the list endpoint returns. It needs `sessionController` and `sessionQuery`, and does not mount without them. |

## Install as a profile bundle

`fork/` is itself a bundle: [`cordis.patch.yml`](cordis.patch.yml) inserts every plugin, and [`package.json`](package.json) names that patch as `dsh.bundle.patch`. Install it once per profile instead of passing `--patch` on every launch.

In the Web UI open "Add plugin" and enter `link:` followed by this directory. `<repo>` is the absolute path of this checkout:

```text
link:<repo>/fork
```

The same from a shell (the `--profile` flag belongs to `plugin`, not to `dsh`):

```sh
pnpm dsh plugin --profile <name> add link:<repo>/fork
```

Either path runs `pnpm add` in `~/.dsh/profiles/<name>/`, records the dependency as `link:...`, and appends `@phamhung075/dsh-fork` to `dsh.profile.bundles`. A bundle mounts on every launch of that profile, so afterwards `pnpm dsh` / `pnpm dsh web` load every plugin with no flags. `pnpm dsh --profile <name> --dump-config` lists them as `fork-live-sync`, `fork-live-writer`, `fork-job-control`, and `fork-session-watch`.

A link install reads this directory in place, so edits under `fork/*/src` apply on the next launch; only `package.json` and `cordis.patch.yml` changes need a re-install. The entry points are `.ts` sources, so the bundle runs under the source launch: `pnpm dsh` and `pnpm dsh web` from the repo root.

## Launch with the plugins

Plain `dsh` does not load them. Add each plugin's overlay with `--patch`; the flag can repeat. Run from the repo root, because overlay paths are relative to it.

Build the client bundles first, and again after every `git merge` of upstream:

```sh
pnpm run build:artifacts
```

Without it the browser shows "Failed to load plugins" and every `client-ui-*` entry waits for `sessions`, because `packages/*/lib/client*.js` is missing or stale. `make dev-web` runs this step itself.

Put `--patch` before app flags such as `--no-open` or `--port`: the launcher passes everything after its first unknown flag to the app, which rejects `--patch`.

Web GUI, every plugin (daily use):

```sh
pnpm dsh web --patch fork/live-sync/live-sync.overlay.yml --patch fork/live-writer/live-writer.overlay.yml --patch fork/job-control/job-control.overlay.yml --patch fork/session-watch/session-watch.overlay.yml
```

Web GUI while editing the client UI:

```sh
make dev-web ARGS='--patch fork/live-sync/live-sync.overlay.yml --patch fork/live-writer/live-writer.overlay.yml --patch fork/job-control/job-control.overlay.yml --patch fork/session-watch/session-watch.overlay.yml'
```

Headless ACP child, manual run:

```sh
pnpm dsh --profile acp --patch fork/live-sync/live-sync.overlay.yml --patch fork/job-control/job-control.overlay.yml
```

- `pnpm dsh web` starts from source (tsx), which `job-control` needs. `make dev-web` runs `build:artifacts` first and rebuilds client bundles on edits, so use it only for client UI work.
- A GUI that only follows `deepseek-offload` jobs needs `live-sync`, `live-writer`, and `session-watch`. `job-control` adds steering and usage snapshots.
- `session-watch` matters when a job is started by another process while the dashboard is open: without it the new row appears only after a page reload, because the controller announces only Sessions its own Host creates. It publishes the list endpoint's own summary, so the new row carries the same title, workspace, running flag, and projections a reload would show. It costs one header listing per cadence and one full list only when an identity is new.
- You normally do not start the ACP child yourself: `deepseek-offload`'s bridge adds every `fork/<name>/<name>.overlay.yml` to each child automatically (`DSH_OFFLOAD_FORK_OVERLAYS=0` disables it). Then `dsh-offload update <jobId> "text"` steers a job through the control inbox, and `job-control` rewrites `<session log>.usage.json` beside the session log after each Assistant attempt.

To check composition without starting the app, add `--dump-config` and look for `fork-live-sync`, `fork-live-writer`, `fork-job-control`, and `fork-session-watch`.

`live-writer` reads the JSONL backend's `<session dir>/session.lock` through `/proc/locks`, so it observes a live writer only on Linux with a readable lock table; elsewhere it answers unknown and the Host keeps its own presentation. It reads the sessions root from `$DSH_HOME/sessions` unless the bundle sets `sessionsRoot`, and it needs no engine package at runtime.

## Test and type check

```sh
pnpm exec vitest run --config fork/vitest.config.ts
(cd fork/live-sync && pnpm exec tsc -p tsconfig.json 2>&1 | grep '^src/')
(cd fork/live-writer && pnpm exec tsc -p tsconfig.json 2>&1 | grep '^src/')
(cd fork/job-control && pnpm exec tsc -p tsconfig.json 2>&1 | grep '^src/')
(cd fork/session-watch && pnpm exec tsc -p tsconfig.json 2>&1 | grep '^src/')
```

The `grep` hides errors under `vendor/`, which appear because the fork tsconfig has no project references. Only `src/` errors count.

## Add a plugin

1. Create `fork/<name>/src/index.ts` exporting `apply(ctx, config)` (and `Config` if it takes options). Use only `import type` from `@deepseek-ai/*` packages so the plugin needs no `package.json` or workspace entry.
2. Register listeners with `ctx.on` and cleanup with `ctx.effect`. Do not edit upstream files.
3. Add `fork/<name>/<name>.overlay.yml` with an `- insert:` entry whose `name` is `./src/index.ts` (resolved relative to the overlay file), for the `--patch` route.
4. Add the same entry, named `@phamhung075/dsh-fork/<name>`, to [`cordis.patch.yml`](cordis.patch.yml), and the matching `"./<name>"` export to [`package.json`](package.json), for the bundle route.
5. Add tests under `fork/<name>/tests/` and a row in the table above.

## Still edited in place in upstream files

The jsonl provider wiring, session-controller (`history.ts`, `list.ts`, `index.ts`), the session-format anchored decode and released codecs, and the `session-persistence` seam exports are not yet extracted. Expect conflicts there on merge.

Merge helper: `~/.local/bin/dsh-sync-upstream` merges `upstream/master`, regenerates generated docs on conflict, and stops on source conflicts.
