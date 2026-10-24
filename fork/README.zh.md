# fork/

[English](README.md) | 中文

仅用于 fork 的插件。它们不修改任何上游文件，因此合并 `upstream/master` 不会与它们冲突。它们位于上游工作区 glob（`packages/*/*`）之外，因此上游的包门禁、锁文件与注册表都不适用。

## 插件

| 插件 | 作用 |
|---|---|
| [`live-sync`](live-sync/src/index.ts) | 把每个根 Agent 的实时 Assistant 帧发布到 `ctx.sessionStreams`，使另一个 Host 能跟随它的 token 流。除非设置 `publishSubagents: true`，否则跳过 subagent。当没有注册 `sessionStreams` 服务时它不做任何事。 |
| [`job-control`](job-control/src/index.ts) | 从另一个进程操控运行中的根 Agent，并暴露它持有的内容。追加到 `<session>.control.jsonl` 的行（`{"text": "..."}`）会在下一步进入 `agent.steer(...)`；attach 时已存在的行会被忽略。每次回复完成后，它从 `ctx.tokenMeter` 写出 `<session>.usage.json`（`totalTokens`、`surfaceTokens`、`nodes`、`logRevision`）。它的入口是 `.ts` 源码，因此需要源码启动（`pnpm dsh`、tsx）。 |

## 作为 profile 组合包安装

`fork/` 本身就是一个组合包：[`cordis.patch.yml`](cordis.patch.yml) 插入这两个插件，[`package.json`](package.json) 把该 patch 命名为 `dsh.bundle.patch`。每个 profile 安装一次即可，不必在每次启动时传 `--patch`。

在 Web UI 中打开「添加插件」，输入 `link:` 加上本目录。`<repo>` 是本 checkout 的绝对路径：

```text
link:<repo>/fork
```

从 shell 执行同样的操作（`--profile` 属于 `plugin`，不属于 `dsh`）：

```sh
pnpm dsh plugin --profile <name> add link:<repo>/fork
```

两种方式都会在 `~/.dsh/profiles/<name>/` 中运行 `pnpm add`，把依赖记录为 `link:...`，并把 `@phamhung075/dsh-fork` 追加到 `dsh.profile.bundles`。组合包会在该 profile 的每次启动时挂载，因此之后 `pnpm dsh` / `pnpm dsh web` 无需任何 flag 就会加载这两个插件。`pnpm dsh --profile <name> --dump-config` 会把它们列为 `fork-live-sync` 和 `fork-job-control`。

link 安装直接读取本目录，因此 `fork/*/src` 下的改动会在下次启动时生效；只有 `package.json` 和 `cordis.patch.yml` 的改动需要重新安装。入口是 `.ts` 源码，因此该组合包在源码启动下运行：从仓库根目录执行 `pnpm dsh` 和 `pnpm dsh web`。

## 随插件启动

直接运行 `dsh` 不会加载它们。用 `--patch` 添加每个插件的 overlay；该 flag 可以重复。请从仓库根目录运行，因为 overlay 路径相对于它解析。

先构建 client bundle，并在每次 `git merge` 上游之后重新构建：

```sh
pnpm run build:artifacts
```

否则浏览器会显示「加载插件失败」，并且每个 `client-ui-*` 入口都会等待 `sessions`，因为 `packages/*/lib/client*.js` 缺失或陈旧。`make dev-web` 会自行运行这一步。

把 `--patch` 放在 `--no-open`、`--port` 等应用 flag 之前：启动器会把第一个未知 flag 之后的所有内容传给应用，而应用会拒绝 `--patch`。

Web GUI，两个插件（日常使用）：

```sh
pnpm dsh web --patch fork/live-sync/live-sync.overlay.yml --patch fork/job-control/job-control.overlay.yml
```

编辑 client UI 时使用 Web GUI：

```sh
make dev-web ARGS='--patch fork/live-sync/live-sync.overlay.yml --patch fork/job-control/job-control.overlay.yml'
```

无头 ACP 子进程，手动运行：

```sh
pnpm dsh --profile acp --patch fork/live-sync/live-sync.overlay.yml --patch fork/job-control/job-control.overlay.yml
```

- `pnpm dsh web` 从源码（tsx）启动，`job-control` 需要这一点。`make dev-web` 会先运行 `build:artifacts`，并在编辑时重建 client bundle，因此只在进行 client UI 工作时使用它。
- 只跟随 `deepseek-offload` 作业的 GUI 只需要 `live-sync`。`job-control` 额外提供 steering 与用量快照。
- 通常不需要自己启动 ACP 子进程：`deepseek-offload` 的 bridge 会把每个 `fork/<name>/<name>.overlay.yml` 自动加到每个子进程上（`DSH_OFFLOAD_FORK_OVERLAYS=0` 可禁用它）。然后用 `node .agents/deepseek-offload/.agents/skills/deepseek-offload/scripts/session-steer.mjs <jobId> "text"` 操控作业，用 `... <jobId> --usage` 打印它的用量快照。

要在不启动应用的情况下检查组合结果，加上 `--dump-config`，查找 `fork-live-sync` 和 `fork-job-control`。

## 测试与类型检查

```sh
pnpm exec vitest run --config fork/vitest.config.ts
(cd fork/live-sync && pnpm exec tsc -p tsconfig.json 2>&1 | grep '^src/')
(cd fork/job-control && pnpm exec tsc -p tsconfig.json 2>&1 | grep '^src/')
```

`grep` 会隐藏 `vendor/` 下的错误，这些错误出现是因为 fork 的 tsconfig 没有 project reference。只有 `src/` 下的错误才算数。

## 新增插件

1. 创建 `fork/<name>/src/index.ts`，导出 `apply(ctx, config)`（如果接受选项，还要导出 `Config`）。只从 `@deepseek-ai/*` 包使用 `import type`，这样插件不需要 `package.json` 或工作区条目。
2. 用 `ctx.on` 注册监听器，用 `ctx.effect` 清理。不要修改上游文件。
3. 为 `--patch` 路线添加 `fork/<name>/<name>.overlay.yml`，其中 `- insert:` 条目的 `name` 是 `./src/index.ts`（相对于该 overlay 文件解析）。
4. 为组合包路线把同样的条目（`name` 为 `@phamhung075/dsh-fork/<name>`）加到 [`cordis.patch.yml`](cordis.patch.yml)，并在 [`package.json`](package.json) 中加上匹配的 `"./<name>"` 导出。
5. 在 `fork/<name>/tests/` 下添加测试，并在上面的表格中加一行。

## 仍在上游文件中就地修改的部分

jsonl provider 接线、session-controller（`history.ts`、`list.ts`、`index.ts`）、session 格式的 anchored decode 与已发布的 codec，以及 `session-persistence` seam 的导出都尚未抽取。合并时预期在这些位置产生冲突。

合并辅助脚本：`~/.local/bin/dsh-sync-upstream` 会合并 `upstream/master`，在冲突时重新生成生成的文档，并在源码冲突处停止。
