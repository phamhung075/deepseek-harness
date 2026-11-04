# Pulling upstream into the fork

Practical playbook for merging `upstream/master` into `fork-migrate`. Read this
before touching a pull; extend the history table at the end after every pull.

## State

- Fork base: `1517a4dbca` (upstream `dsh-v0.2.1-alpha.4`). The base is the row
  last added to the history table, also stated in [MIGRATION.md](MIGRATION.md).
  Find it with `git merge-base fork-migrate upstream/master` while the branches
  are still unmerged.
- Last dry run: `fcc0ad4621`, upstream `0.2.2-rc.1`, 2026-11-04. 26 conflicted
  files, 43 conflict hunks; aborted after 2 files resolved (see the table).
- Worktree: `/home/daihu/w/dsh-fork-migrate`, branch `fork-migrate`.
  Never merge in the live checkout `~/__projects__/deepseek-harness`; another
  agent runs and serves the live GUI there. Do not restart the live GUI and do
  not push from this branch.

## Before

```sh
cd /home/daihu/w/dsh-fork-migrate
git status --short            # must be clean outside ignored node_modules/.proof
git fetch upstream
git rev-parse --short upstream/master
git merge-base fork-migrate upstream/master   # expected: the recorded base
```

Dry run first, on a throwaway branch and worktree. It costs a minute and tells
you the conflict set before the real merge:

```sh
tip=$(git rev-parse --short upstream/master)
git worktree add -b pull-check/$tip .pull-check HEAD
cd .pull-check
git merge --no-edit upstream/master
git diff --name-only --diff-filter=U
# inspect, resolve a sample, then either continue or abort:
git merge --abort
cd ..
git worktree remove --force .pull-check
git branch -D pull-check/$tip
```

For the real pull use the same worktree pattern with a branch you keep, e.g.
`pull/upstream-$tip`, or merge directly on `fork-migrate` when the dry run is
small. Commit in small steps: one commit per conflict group, then one for
regenerated output, then one for the history table.

## Keep rule

New work goes in `fork/`. Core edits are allowed only for the 34 file groups
listed in [MIGRATION.md](MIGRATION.md). The founder decided on 2026-11-04 that
those groups stay in core and are re-applied at each pull; they are not open
migration work. Do not add a new core edit without a founder decision, and do
not widen an existing one while resolving a conflict.

The fork diff outside `fork/` must stay exactly those groups:

```sh
git diff --stat upstream/master..HEAD -- . ':(exclude)fork'
```

## Conflict groups

Counts are files/hunks from the 2026-11-04 dry run. "Ours" names the behaviour,
not the file history; use `git log` on the path for detail.

### 1. Session-format anchored decode — 0/0

Ours: optional `anchor` on `SessionFormatCodec.createDecoder` and
`SessionFormatRestoreOptions`, plus `AnchoredSessionFormatRestore`, so a
follower decodes only rows appended after its stored prefix (`8f20097`).
Upstream 0.2.2 changed codec validation only; `session-format/src/types.ts` and
`catalog.ts` merged clean and the anchor is intact. Keep ours, no action.
Prove with the JSONL tail tests and `tsc` for `session-format`.

### 2. session-persistence seam — 1/1

Ours: `lastModifiedAt` on `SessionPersistenceSnapshot`, so a Host can tell a
Session another process is appending to from one that settled
(`d850c7db2f`). Upstream added `location` (backend-resolved artifact path) in
the same snapshot slot. Resolution: take upstream and add `lastModifiedAt`
beside `location`; both are optional and neither replaces the other. The
`sessionAppends`/`sessionStreams` service declarations auto-merged; keep them.
No generated file. Prove: `tsc` for `session-persistence`, then the JSONL tests.
Effort: under 10 minutes.

### 3. JSONL tail and stream — 2/3

Ours: the JSONL provider tails one artifact, decodes rows written by another
process, withholds a torn tail, and publishes live Assistant frames beside the
artifact (`84fe96d610`, `e6ee1c2e4e`, `d850c7db2f`). Upstream rewrote
`src/index.ts` snapshot construction for `location`; it has no append watch or
stream channel. Resolution: keep upstream's `list()` structure and add
`lastModifiedAt: Number(identity.mtimeMs)` beside `location` in both snapshot
builders (list and the single-session stat path). The new tail files
(`appends.ts`, `append-tail.ts`, `tail-reader.ts`, `tail-subscription.ts`,
`stream-channel.ts`, `streams.ts`) are fork-only and auto-added. Resolve the
`README.i18n.yaml` conflict by rebuilding its pairing after editing the README:

```sh
pnpm run verify-translation-pairing --write packages/session/session-persistence-jsonl/README.md
```

Prove with `fork/tests/jsonl-*.spec.ts` and `tsc` for the package. Effort:
about 15 minutes.

### 4. Session-controller activity and follow — 6/9

Ours: `ColdSessionActivity` polls a stored Session this process does not own,
marks it running from its durable writes, and fans appends and live frames to a
follower (`8105afeb08`, `2455f81364`, `2f300d2e12`, `d850c7db2f`). This is the
GUI path that shows and follows an offload job it did not start.
Upstream 0.2.2 split the controller into `history/`, `state/`, and `web/`
controllers (`2f2caf88b3`, `e032fdc2a8`), added its own in-process
`history/assistant-stream.ts` and projection listeners, and made
`ApiSessionList` initialize asynchronously. It still has no cross-process
append/stream subscription. This is the hard group and the reason a pull is not
a 30-minute task.

Resolve by taking upstream and re-applying ours onto the new structure:

```sh
git checkout --theirs -- packages/api/session-controller/src/{index,history,list}.ts
git add packages/api/session-controller/src/{index,history,list}.ts
```

Then, in order:

1. Keep the auto-added `src/activity.ts` (`ColdSessionActivity`).
2. Construct it in `index.ts` after `ctx.sessionQuery` resolves and pass it to
   the new `HistoryController` and `ApiSessionList` (add the parameter to their
   constructors; upstream's `list.ts` now has `initialize()`).
3. Port the append/stream follow buffer from our old `history.ts` into
   `history/controller.ts` and `history/listener.ts`; upstream's
   `history/assistant-stream.ts` covers only the in-process part.
4. Re-apply the status wording widened for a cold running Session.
5. Move the two conflicted tests to the new internals: they now live under
   `fork/tests/session-{durable,streams}.host.spec.ts`; keep the upstream
   test files untouched.

Do not keep our 618-line `history.ts`; upstream's split replaces it. Prove with
the host tests, then the full offload proof below. Effort: about one day.

### 5. Sandbox extraWritableRoots and ssh — 6/13

Ours: `extraWritableRoots` on the execution policy and policy config, bound
with `--bind-try` in bwrap, filtered for landlock, rendered in the policy
prompt, and accepted by the remote helper schema (`68ce0b5052`).
Upstream 0.2.2 added the same capability as `additionalWritableRoots`
(`6b71dcad85`, hardened in `64ece4cac1`), including the SSH schema and a
headless `--add-dir` flag. Take upstream everywhere and delete our field; do not
keep both. Rename the moved tests (`fork/tests/sandbox-{roots,policy}.spec.ts`)
and any deployment profile that still sets `extraWritableRoots`:

```sh
grep -rn extraWritableRoots ~/.dsh/profiles fork
```

Upstream binds granted roots with `--bind` rather than `--bind-try`, so a grant
that no longer exists can fail every command instead of being skipped. Test a
resumed job after deleting its worktree before accepting the pull.
Effort: about one hour including the config rename.

### 6. CLI profile and direct invocation — 0/0

Ours: atomic profile-root creation and repair (`208202b31d`) and the
`pathToFileURL(argv[1])` direct-invocation fallback in `bin.ts`
(`78c362dbf1`). Upstream's profile boot already stages and renames; the merge
kept our extra repair code and the direct-invocation test still mattered.
After the merge, compare `apps/cli/src/profile-boot.ts` with upstream and drop
our repair branch if the staged rename covers a concurrent first start; keep the
`bin.ts` fallback because the offload bridge launches the `.ts` entry through
tsx. Prove: `fork/tests/profile-root.spec.ts`, two concurrent first starts, and
one offload launch. Effort: about 15 minutes.

### 7. Native source checks — 0/0

Ours: compare native constructor source with collapsed whitespace in
`core/tools/src/json-schema.ts`, `extensions/cordis-host-runner/src/guard.ts`,
and `ptc-runtime/ptc-runtime-node/src/json-wire.ts` (`06ad7d7fa0`,
`e29272614a`). Upstream changed only comments in these files; our change
auto-merged. Keep ours. Prove with `fork/tests/util-values.spec.ts` and the
targeted tool/PTC tests under Node 24. Effort: none.

### 8. Generated catalog and docs — 9/16 (catalog 3, docs 13)

Ours: `packages/extensions/tool-cordis/src/api-catalog.ts` was regenerated for
`sandboxPolicy` and the two session services. Upstream regenerated it with its
new services and `additionalWritableRoots`; the same happens to generated docs
(`docs/config-catalog*`, `docs/capability-seams*`,
`docs/event-producer-consumer*`, `docs/subsystems/persistence*`). Never
hand-merge these. Take one side, finish the code, then regenerate:

```sh
pnpm run gen-cordis-catalog
pnpm run gen-cordis-api
pnpm run gen-config-catalog
pnpm run gen-doc-graphs
pnpm run verify-md-links
```

If a fork service disappears during resolution, codegen drops it. Prove with the
matching `verify-*` scripts and a clean `git diff` after regeneration. Effort:
about 10 minutes.

### 9. Guest bundle — 1/1

Ours: merge-commit regeneration only, not a fork decision. Upstream regenerated
the one-line bundle. Take upstream and regenerate instead of editing:

```sh
git checkout --theirs packages/workflow/workflow-ptc/src/guest-source.ts
pnpm exec tsx scripts/gen-workflow-guest.ts
```

Prove with `packages/workflow/workflow-ptc/tests/guest.spec.ts`. Effort: one
minute.

## After

Run in this order; stop at the first failure.

1. Scope check: `git diff --stat upstream/master..HEAD -- . ':(exclude)fork'`
   lists only the 34 groups (and generated files).
2. `pnpm install --frozen-lockfile` if `pnpm-lock.yaml` changed.
3. `pnpm run build:artifacts`, then `pnpm run build:check` (or `pnpm run
   typecheck`). A typecheck that resolves `@deepseek-ai/*` to the live checkout
   means the shared `node_modules` is read-only or stale, not that the merge is
   wrong; use the per-package `tsc -p --noEmit`.
4. `pnpm exec vitest run --config fork/vitest.config.ts` (fork tests). On this
   host `fork/.vitest` must be writable.
5. `pnpm run verify-cordis-catalog && pnpm run verify-config-catalog && pnpm run
   verify-md-links`.
6. Real offload proof, in a scratch `DSH_HOME` on a spare port; never against
   the live GUI:
   - start a job whose first call is silent and long (`sleep 45`);
   - `session/list` reports the session running while the call is the tail;
   - send a GUI prompt (`session/prompt`) and see the steered user message;
   - stop it (`session/cancel`), then resume the same session;
   - prompt again after the stop and get a completed reply;
   - under `workspace-write`, `git commit` in a linked worktree whose `.git`
     lives outside the worktree, using `additionalWritableRoots`.
   Record job ids and session ids here or in the PR description.

## Rollback

Before committing the merge: `git merge --abort` restores the pre-merge branch;
then remove the worktree and branch from the dry-run commands.

After committing: record `git rev-parse HEAD` before the merge, then
`git reset --hard <previous>` on the pull branch, `pnpm install`, and rebuild
`build:artifacts`. The running GUI keeps the old process until its owner
restarts it; do not restart it yourself.

If a pull is abandoned during resolution, keep the branch (do not delete work)
and report the conflicting group; the next attempt starts from the same base.

## Conflict history

| Date | Upstream tip | Version | Files | Hunks | Per group (files/hunks) | Outcome |
|---|---|---|---|---|---|---|
| 2026-11-04 | `fcc0ad4621` | 0.2.2-rc.1 | 26 | 43 | format 0/0, persistence 1/1, jsonl 2/3, controller 6/9, sandbox+ssh 6/13, CLI 0/0, native 0/0, catalog+docs 9/16, guest 1/1 | dry run only; 2/26 files resolved (persistence, jsonl), then aborted; merged tree not built, fork tests not run |
