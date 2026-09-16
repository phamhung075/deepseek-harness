# Agent Note: 在会话产物旁发布实时 Assistant frame

Status: implemented

[English](2026-09-16-live-assistant-frame-side-channel.md) | 中文

## 问题

`agent/assistant-stream` 是循环对实时 Assistant 输出的发布：有序的 start、瞬态 chunk frame 与已提交的 end。它刻意是进程本地的，而持久回放改读 `assistant/message` 或 `assistant/attempt` settlement 中嵌入的紧凑带时间 stream，因此已存日志只有在 attempt 提交之后才带有文本。于是，跟随另一个进程所运行会话的 Host 只会收到已提交的记录，记录之间空无一物，因为跟随方的实时帧只来自它自己的进程内 `agent/assistant-stream` 总线，而该总线对另一个进程的 Agent 永不触发。浏览器只能整块绘制每个 settlement，而不是流式绘制。

委派任务让这成为常态。`dsh --profile acp` 在自己的进程中运行，由 `dsh-subagent-acp` 或某个 offload 桥启动，而 Web GUI 跟随该子进程追加的产物。子进程流式输出的 token 除它自己的 ACP 客户端外无人可见；GUI 的转录在每个 settlement 处跳跃。

## 决策

ACP profile 把每个由它持有的会话的实时帧发布到该会话产物旁的纯文本旁路通道，Web 跟随方尾随该通道。持久 settlement 仍是唯一的回放权威；该通道是不带持久性承诺的呈现数据。

**该能力是第二个可选伴随 Service Definition，而不是对 `SessionPersistence` 的修改。** JSONL 提供方的追加观察方已经解析会话的产物路径并持有其目录，因此 `ctx.sessionStreams`（`@deepseek-ai/dsh-session-persistence/streams`）与 `ctx.sessionAppends` 并立，后者以其 `watch(id)` 承载持久事件。`SessionStreams.publish(id)` 打开发布通道，`watch(id)` 尾随它。提供方拥有该文件及其路径；ACP profile 是发布方 Consumer，Session Controller 是尾随方 Consumer，二者都用 `ctx.get('sessionStreams')` 读取该服务，因此未注册它的后端只会让它们沉默，而不会让它们损坏。

**该通道是会话目录内的一个纯文本 JSONL 文件。** 它命名为 `<generation stem>.stream.jsonl` —— 即定位到的产物文件名把其 `.jsonl` 或 `.jsonl.zstd` 后缀替换为 `.stream.jsonl`，例如 `session.v3.stream.jsonl`。没有任何 generation 解析器接受该名称，因此该通道永远不会被当作持久日志读取，枚举 generation 的读取方也会忽略它。纯文本避免了为体积小、频率高且寿命短的帧支付 Zstandard 帧化开销，也让被撕裂的写入方尾部保持为单独一行不完整的行。`sessionDir` 已把该目录记载为「可供未来会话本地产物使用」的目录，这正是该文件的生命周期：它随会话一起被删除。

**一条记录就是一行。** 每行是 `{"at": <下一个持久 seq>, "frame": <AssistantStreamFrame>}`，由循环发出的进程本地帧加上该时刻会话的下一个 seq 序列化而成。存储原始帧与游标 —— 而不是预先构造好的 wire 对象 —— 让转换留在其唯一所有者处：Session Controller 的 `wireAssistantStreamFrame`，Web 路径本就用它给 `start` 帧盖上 `startedAfterSeq`。ACP 包只依赖 Agent runtime 与该持久化接缝，绝不依赖 Web BFF 的 wire 词汇。

**被撕裂的写入方尾部由其所有者修复。** 写入方以追加方式打开该通道，当文件最后一个字节不是换行时，会先截断那条不完整的记录再写入，因此一个在帧中途被杀死的写入方不会留下半条记录、再被下一个写入方的行拼接上去。读取方扣留不完整的尾行，直到补齐它的字节到达；读取方若发现文件变短或被替换，则从字节零重启，而尾随的消费者凭自己的位置吸收该重启。

**跟随方在其快照之前打开该通道。** 选择加入 `assistantStream` 的 follow 会在其开场观察之前建立流订阅，并且只投递在快照发出之后到达的帧，因此快照始终是客户端续接的唯一切面。进程内总线对本 Host 所运行的会话仍是权威的：只有当 `ctx.sessions.get(id)` 找不到进程内会话时才打开该通道，这与事件订阅完全一致；而没有注册该通道的组合不会打开任何东西，也不会挂起。

**发布对 ACP profile 默认开启且可配置。** ACP 插件的 `publishAssistantStream` 字段（默认 `true`）可为不需要该文件的部署关闭它；当持久化提供方不提供通道时，profile 保持沉默。

## 考虑过的替代方案

**只通过 ACP wire 发布这些帧。** 启动子进程的桥会看到实时 chunk，但 Web GUI 跟随的是产物而非任何 ACP 连接，因此桥的客户端改善了，GUI 却仍然冻结。该通道服务于每个已经读取该产物的跟随方，包括从不使用 ACP 的客户端。

**把这些帧持久化为会话事件。** 这不需要旁路文件并复用追加路径，但它会把每个瞬态 chunk 写入追加专用日志。[嵌入式 Assistant stream 决策](2026-09-01-v2-embedded-assistant-streams.zh.md)已确定 stream 在 settlement 时提交一次、实时 chunk 是瞬态的；逐 chunk 的持久事件会让每个日志膨胀，并与此权威相矛盾。

**让 offload 桥发布。** 桥已经接收子进程的 ACP update，因此它可以自己写入该通道，但这只对桥启动的任务有效，并把 GUI 耦合到某一个启动器。产物才是每个写入方唯一共享的界面。

**让 JSONL 提供方自己订阅 `agent/assistant-stream`。** 这会完全去掉 ACP 侧的发布方，但它把 Agent runtime 事件放进存储后端，并让每个 profile 都为它自己的进程内会话写旁路文件。

**把通道存到存储库之外单独的每会话目录。** 这避免了解析产物路径，但文件失去了产物的生命周期，并需要自己的发现、命名与清理规则。

## 后果

Web GUI 会为委派出去的 ACP 会话流式显示文本，而不是在每个 settlement 处跳跃，且客户端、wire 与渲染器都无需改动：该通道承载的记录与进程内总线产生的完全相同。

该通道在构造上就是尽力而为的。在最后一次持久 settlement 之后写入的帧，或写入被撕裂尾部的帧，会丢失且没有修复路径，因为持久日志仍是回放来源，客户端会从它重新同步。帧是呈现数据，绝不是模型的输入，因此没有任何模型可见之物依赖它。

发布方为每个发布会话增加一个有界写入队列与一个文件，该文件随会话目录一起删除。尾随的消费者为每个被跟随的外部会话增加一个监视器与一次安全网轮询，其生命周期与事件观察方的订阅相同。两者都是可配置的（ACP 插件上的 `publishAssistantStream`；JSONL 后端的观察轮询节奏），因此部署可以关闭它们。

## 测试

`packages/session/session-persistence-jsonl/tests/streams.spec.ts` 在同一 root 上挂载一个后端，并锁定：在某个尾随建立之后发布的帧按序被读回；不完整的尾行被扣留直到其最后字节到达；被替换的通道会让读取方重启；写入方在追加之前修复不完整的尾部记录；未知会话被拒绝；sink 与各订阅都在后端拆卸时释放。

`packages/acp/acp/tests/assistant-stream.spec.ts` 锁定：被提示的会话的实时帧到达它自己的通道；第二个会话得不到任何帧；`publishAssistantStream: false` 不写入任何内容。

`packages/api/session-controller/tests/session-streams.host.spec.ts` 锁定：跟随另一个写入方所拥有会话的跟随方会收到该通道的帧，而本 Host 持有的会话保持进程内路径。
