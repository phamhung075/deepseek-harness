# Fork core-change migration

This table classifies every path under `packages/` and `apps/` that differs from
the fork base `1517a4dbca` (upstream `dsh-v0.2.1-alpha.4`), so an upstream pull
can keep the founder's rule: nothing outside `fork/` differs from upstream.
Verdicts:

- **a** — movable into `fork/` now, as a plugin, overlay, or duplicated
  behaviour with no core edit.
- **b** — upstream already carries it at a newer version; drop it at the next
  pull.
- **c** — tests, docs, or generated output only; moves with its code, and can
  live under `fork/tests` while the code still needs the core seam.
- **d** — cannot be done from `fork/`; the missing hook and the smallest
  upstream patch that would add it are named.

The fork's own plugins (`fork/live-sync`, `fork/live-writer`,
`fork/offload-gui`, `fork/job-control`) are already outside `packages/`, so
they are not in the table.

## Summary

| Verdict | Paths still under `packages/` or `apps/` | State |
|---|---|---|
| a | 0 | no remaining core path is movable without a new upstream hook |
| b | 1 | `workflow-ptc/src/guest-source.ts` is regenerated at the next pull |
| c | 15 | generated catalog follows its code; docs and tests moved to `fork/` |
| d | 35 | core seams listed below; each names its missing hook |

`git diff --name-only 1517a4dbca..HEAD -- packages apps | wc -l` was 60 before
the migration commits.

| Commit | Area | Count after |
|---|---|---|
| `94217c4efe` | table | 60 |
| `e6d52902cf` | value, sandbox root, sandbox policy tests | 59 |
| this commit | follow/stream/tail/profile-start tests | 51 |

The metric lists deleted paths too. Moving a **new** file (absent in the base)
out of `packages/` removes it from the diff; moving a **modified** file leaves a
deletion entry, so its count only falls when the core edit itself is reverted.
That is why the modified core seams below stay: reverting them would break the
offload path, and no hook moves them.

## Table

| Files | What the change does | Why it exists | Verdict |
|---|---|---|---|
| `session/session-format/src/{types,catalog}.ts`, `session-format-v{0..3}-to-v{1..4}/src/codec.ts`, `session-format-catalog/tests/catalog.spec.ts` | Adds an optional decode `anchor` to `SessionFormatCodec.createDecoder` and `SessionFormatRestoreOptions`, and an `AnchoredSessionFormatRestore` that validates appended rows without re-running the artifact-wide restorer. | `8f20097` — a follower must decode only the rows written after the prefix it already has, without the end-seed row that established the prefix cut. | **d** — the JSONL backend constructs codecs and restores inside `session-persistence-jsonl`; no plugin can substitute an anchored decode. Smallest upstream patch: the optional `anchor` parameter and anchored restore path exactly as in this fork. |
| `session/session-persistence/src/{index,appends,streams}.ts`, READMEs | Declares `ctx.sessionAppends` (durable-append watch) and `ctx.sessionStreams` (live Assistant-frame side channel), and adds `lastModifiedAt` to the snapshot. | `d850c7db2f` — a Host that did not run an Agent cannot see its appends or live frames from its own process. | **d** — the service definitions are small, but the JSONL provider implements them and the session-controller consumes them; neither is an extension point. Smallest upstream patch: export both abstract services and add optional `watchAppends`/`watchStreams` to the persistence backend contract. |
| `session/session-persistence-jsonl/src/{index,format,appends,append-tail,tail-reader,tail-subscription,stream-channel,streams}.ts`, READMEs | Tails one stored JSONL artifact, decodes rows written by another process, withholds a torn tail, publishes live frames beside the artifact, and releases every subscription on close/teardown. | `84fe96d610`, `e6ee1c2e4e`, `d850c7db2f` — the offload worker writes the Session; the GUI Host only follows it. | **d** — the provider owns its file layout and the `SessionPersistence` service. Smallest upstream patch: give the backend contract an optional artifact-path + append-observer capability so a fork plugin can tail without editing the provider. |
| `api/session-controller/src/{activity,history,index,list,types}.ts`, `tsconfig.host.json` | Polls a stored Session this process does not own, marks it running from its durable writes, falls back to idle, and fans its appends/live frames to a follower; widens the status event wording. | `8105afeb08`, `2455f81364`, `2f300d2e12`, `d850c7db2f` — the GUI must show and follow an offload job it did not start. | **d** — cold-session liveness and stream fan-out are inside the controller's private `ApiSessionList`/`SessionController` wiring. Smallest upstream patch: a `SessionActivityProvider` service (isRunning + append/stream subscription) that the controller consults, registered by a backend plugin. |
| `sandbox/sandbox/src/{index,roots}.ts`, `sandbox-policy/src/index.ts`, `sandbox-local/src/profiles.ts`, `ssh/ssh/src/schemas.ts` | Adds `extraWritableRoots` to the file policy, includes it in `writableRoots`, binds it with `--bind-try` (bwrap) or filters it (landlock), renders it in the policy prompt, and allows it on the remote helper policy. | `68ce0b5052` — a linked worktree's `.git` directory lives outside the worktree, so `workspace-write` denied `git commit` and other writes the offload jobs need. | **d** — the resolved policy type and both runners are core. No plugin can widen a policy after `sandboxPolicy.resolve()`. Smallest upstream patch: `extraWritableRoots?: string[]` on `SandboxExecutionPolicy` + `writableRoots()` + the local/remote runners (this fork's diff). |
| `apps/cli/src/profile-boot.ts`, `apps/cli/tests/profile-root.spec.ts`, `apps/cli/tests/fixtures/restore-profile-root.ts` | Creates the profile root atomically (temp dir + rename), tolerates a concurrent starter's partial root, and keeps the previous content on failure. | `208202b31d` — offload starts several CLI processes at once; two first starts raced on the same profile root and one failed on a truncated tree. | **d** — profile creation is inside CLI boot; no service or overlay sees it. Smallest upstream patch: the atomic create/repair in `profile-boot.ts` (the tests move to `fork/tests`). |
| `apps/cli/src/bin.ts` | Also treats `import.meta.url === pathToFileURL(argv[1]).href` as a direct invocation, beside `import.meta.main`. | `78c362dbf1` — the offload bridge runs the `.ts` entry through tsx, where `import.meta.main` is absent on older Node. | **d** — the CLI entrypoint is not a plugin host. Smallest upstream patch: the two-line fallback in `bin.ts`. |
| `core/tools/src/json-schema.ts`, `extensions/cordis-host-runner/src/guard.ts`, `ptc-runtime/ptc-runtime-node/src/json-wire.ts`, `util/values/tests/index.spec.ts` | Compares native constructor source with collapsed whitespace, and keeps the PTC check off guest-redefinable methods. | `06ad7d7fa0`, `e29272614a` — engines format `[native code]` differently; the strict comparison rejected valid arrays/objects under the fork's runtime. | **d** — both checks live in core security helpers. Smallest upstream patch: whitespace-normalized comparison in the three places (the values test moves to `fork/tests`). |
| `extensions/tool-cordis/src/api-catalog.ts` | Registers the new `sandboxPolicy` config field and the `sessionAppends`/`sessionStreams` services in the generated `ctx.<key>` catalog. | `68ce0b5052`, `d850c7db2f` — the catalog is generated from the service inventory. | **c** — generated output; regenerates when the core seams above change, and upstream's newer catalog replaces it at the next pull. |
| `workflow/workflow-ptc/src/guest-source.ts` | One regenerated guest bundle line (source URL). | Merge commits only (`725a7e781e` and earlier); no fork decision. | **b** — upstream's newer bundle replaces it at the next pull. |
| Test files under `packages/api/session-controller/tests`, `packages/sandbox/{sandbox,sandbox-policy,sandbox-local}/tests`, `packages/session/session-persistence-jsonl/tests`, `packages/util/values/tests`, `apps/cli/tests` | Cover the cold-session follow, extra writable roots, JSONL append/stream tailing, the engine-independent native-source check, and concurrent profile starts. | Each commit above. | **c** — moved to `fork/tests/` so an upstream pull does not conflict on tests while the code still needs the core seam. |
| `.agents/notes/...`, `docs/...` | Explain handle-based persistence, durable append observation, the live-frame side channel, and the fork plugins. | `d850c7db2f`, `cd63184b7f`, `6ffc543dd0`, `68ce0b5052`. | **c** — fork rationale; canonical copies move under `fork/docs`, and generated projections are regenerated. |
| `scripts/{gen-cordis-catalog,gen-doc-graphs,verify-md-links}.ts`, `scripts/verify-md-links.spec.ts`, `.gitignore` | Teach the generators/checks about the new services and fork docs; ignore the worktree-local `node_modules`. | `d850c7db2f`, `68ce0b5052`, `6ffc543dd0`. | **c** — tooling follows the docs/services above. Revert alongside the docs move when upstream's generators cover the new services. |

## What the running system needs

The live GUI and the offload jobs depend on these behaviours; none may regress
while the seams stay in core:

- **Live job durability and offload** — the worker process writes the Session
  and the GUI follows it: `sessionAppends` + `sessionStreams` in the JSONL
  provider, and the session-controller's cold-session activity. This is the
  "session v4 follow" path.
- **Sandbox git write** — `extraWritableRoots` (commit `68ce0b5052`) so a
  linked worktree can write its `.git` directory under `workspace-write`.
- **Concurrent starts** — the atomic profile root (`208202b31d`) and the direct
  script invocation (`78c362dbf1`), used by the offload bridge to launch
  several jobs at once.
- **Engine-independent source checks** — the whitespace fix keeps tools,
  workflows, and PTC working under the fork's Node/tsx runtime.

## Still in core

Everything marked **d** above. Until upstream offers the named hook, each one
conflicts on pull. The smallest upstream patch for each is the fork diff itself,
kept small on purpose.
