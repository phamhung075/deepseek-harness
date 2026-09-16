# Agent Note: 观察并非本 Host 写入的持久追加

Status: implemented

[English](2026-09-13-durable-append-observation.md) | 中文

## 问题

委派任务运行在另一个进程中。它向 Web GUI 所读的同一个会话产物追加事件 —— `${DSH_HOME}/sessions/<project>/<id>/session.jsonl.zstd` —— 而 GUI 既无法报告它，也无法跟踪它。`ApiSessionList.summarizeCold` 对本 Host 未持有的每个会话硬编码 `running: false`，因此正在运行的任务看起来是空闲的；`SessionHistoryController.follow` 只从进程内的 `session/event` 总线投递实时帧，而该监听器对另一个进程的写入永不触发：被打开的冷会话在写入方仍在工作时冻结在它的开场快照上。

没有任何东西监视会话产物 —— 代码树中仅有的文件监视器观察的是凭据与设置文件 —— 而持久化接缝在原理上也无法表达这种观察。`stat` 返回的是不透明 revision，它只能证明有东西变了，却说不清到达了什么；`api-session/status` 也只由进程内 Agent 的状态发出。

## 决策

`@deepseek-ai/dsh-session-persistence` 声明了第二个、可选的 Service Definition：`SessionAppends`（`ctx.sessionAppends`），其 `watch(id)` 返回一个由调用方持有的订阅，投递在该订阅建立之后变为持久的逻辑事件批次。JSONL 后端在 `ctx.sessionPersistence` 之外提供它；`@deepseek-ai/dsh-api-session-controller` 在列表的运行标记与 follow 中消费它。它的存在取决于部署：无法观察外部追加的后端不注册它，消费者用 `ctx.get('sessionAppends')` 读取，缺失时 Session Controller 会警告一次并指出缺失的服务名。

订阅只报告追加，绝不报告它建立时已经持久的前缀，而这正是让它无缝隙的定序契约：消费者先建立订阅，再读取它要接续的前缀，因此在这两步之间变为持久的事件会经由流到达，而由消费者自己的游标决定它是否已经消费过该事件。被撕裂的记录会一直扣留，直到补齐它的字节到达。丢失位置的后端 —— 修复截断了产物、同一路径下的文件被替换、订阅恰好开在写入方随后重写的帧内部 —— 可能重放消费者已经见过的事件，而不是留下空洞，因此每个消费者都能容忍位于其上或其下的事件。跟随方既有的 `item.seq < expectedSeq` 跳过就是这种容忍；空闲观察方只是重新折叠状态。

写入方也可以在读取方并未丢失位置的情况下把它甩在后面。在一个进程中被中断、又在另一个进程中续跑的回合会追加一批重新发出产物已持有序号的事件，因此文件在增长，而追加记录却向后倒退；这既不会触发重启（没有截断、没有替换），追加的字节也能干净解码。读取方把每一个位于其最后报告序号之上或与之相同的追加序号都视为已经投递：丢弃这些记录、保持自己的位置，并从第一个真正的新序号继续，从而保持 `poll` 输出的连续性，也让仍在被写入的会话的订阅继续存活。只有超出期望序号的追加序号才会终止该流，因为该流从未见过的字节是任何东西都无法吸收的空洞。

JSONL 提供方只解码写入方新追加的内容。它持有产物已消费的字节偏移，通过既有的帧扫描器与解码器解码其后的完整 Zstandard 帧，保留不完整的尾帧；对纯文本产物则保留不完整的尾行；字节无论是否成功解码都会被消费，因此未完成的记录会被重读而不是丢失。对会话目录的 `fs.watch` 及时报告追加，而 `watchPollIntervalMs` 轮询是平台监视器沉默或不可用时的安全网，因此正确性不依赖通知。订阅是被持有的：服务把它们放在集合里，`close()` 释放监视器、定时器、监听器与读取循环，提供方 fiber 的 disposer 关闭每个存活订阅并等待其读取循环，这正是处置测试所观察的内容。

`SessionPersistenceSnapshot` 新增可选的 `lastModifiedAt`，以 Unix 纪元毫秒表示，即 JSONL 后端在它为 `revision` 所做的那次 stat 中早已取得的产物修改时间。它存在的原因是：当本 Host 在任务处于回合中途时启动，尚无可观察的追加，而最近写入方的时新性是判断该运行仍然活跃的唯一廉价证据。

Host 侧的观察者是 Session Controller 中的 `ColdSessionActivity`。它每隔 `coldActivityPollMs` 轮询 `persistence.list()`，收养产物在 `coldActivityIdleMs` 内被修改过的会话（同时最多 `coldActivityMaxSessions` 个），订阅它们的追加，把 `turn/start` 与 `turn/end` 折叠为回合开启标记，并在每次转换时发出 `api-session/status`，因此 GUI 侧边栏无需重新加载即可得知变化。当最后观察到的回合未关闭且写入尚新时，会话即为运行中；观察到 `turn/end` 会立即让它落定，而空闲窗口只是为在回合中途死亡、从未写入该事件的写入方设置的护栏。本 Host 在 `ctx.sessions` 或 `ctx.agents` 中持有的会话永不被观察，而在被观察之后变为活跃的会话会被释放并撤回其运行状态，从而保持进程内路径的权威性。

`follow` 在其开场观察之前建立持久订阅，并把批次并入跟随方的有序缓冲区，因此冷会话在流式接收其追加的同时，快照仍是同一个持久切面。已建立持久订阅的 `follow` 不会提升该会话：提升会把它挂载到本 Host，此后 Session 列表会把一个并未运行的进程内 Agent 视为权威，下一次 follow 也会改用进程内总线——于是打开该行一次就会报告空闲，并在另一进程继续工作期间冻结转录。激活因此留在按需路径上：提示本就会通过 `resolveAgent` 解析 Agent。进程内总线对本 Host 持有的会话仍然是权威的：对这些会话直接跳过持久订阅，而来自两个来源的重复 seq 由跟随方既有的 seq 检查丢弃，而不是引入新的排序规则。

## 考虑过的替代方案

**在 `SessionPersistence` 上添加抽象 `watch`。** 这会让每个提供方都承担义务，而本代码树有八个子类 —— JSONL 后端加七个测试替身 —— 另有五十一处 `ctx.provide('sessionPersistence', …)` 桩，以及两个共享契约套件必须为多数持久化消费者从不调用的能力而扩展。可选的伴随定义把义务限制在能够履行它的提供方，并在唯一需要它的消费者处让缺失变得显式。

**仅凭写入时新性报告运行标记。** 修改时间廉价且不需要回合状态，但它无法及时让会话落定：已完成的任务在整个空闲窗口内仍会报告为运行中，而本次变更的验收规则是关闭的回合即为空闲。回合边界提供了这一点，时新性只用于守护在回合中途死亡的写入方。

**每次追加都重读整个已存日志。** 这可以复用 `observeSession` 且不需要增量解码器，但运行中的会话每秒追加数次，因此每次追加都解码整个日志在会话长度上是平方级的。只解码完整的新增帧则只需付出新增字节的代价。

**监视每个已存会话。** 为整个语料库各配一个监视器就无需轮询 `list()` 来发现活动，但一个存储库有数百个产物，而只有最近被写入的才可能正在运行。有界地收养最近的写入方使监视器数量与并发运行数成正比。

**通过桥接或旁路通道报告外部活动。** offload 桥知道它启动了哪个会话，因此可以自行推送状态。这仅对桥启动的任务有效，并把 GUI 耦合到某一个启动器；只有产物才是每个写入方（包括 GUI 从未见过其启动的写入方）共享的东西。

**让持久 follow 提升它打开的会话。** 提升可以保留本次改动之前冷打开所具有的即时预热激活，但提升会把会话挂载到 Host，从而把权威交给一个并未运行该作业的进程内 Agent：行报告空闲，下一次 follow 改用进程内总线，于是该次运行在写入方持续追加时不再流式传输。对持久 follow 跳过提升，既保持单一权威来源，也让激活留在按需路径上：提示会通过 `resolveAgent` 解析 Agent。

## 后果

Web GUI 会把另一个进程正在运行的会话报告为运行中，在其持久回合关闭或其写入转为空闲时停止报告，并把该会话的追加流入已打开的对话记录，而不是冻结在快照上。本 Host 运行的会话不受影响：它们的列表行与状态推送仍来自 `ctx.agents`，follow 仍使用进程内总线。

仍有两个边界效应。冷观察的日志会用内存中的中断回合收尾事件来配平，因此当会话的回合仍开启时，跟随方会从配平后的游标接续，并跳过位于其上或其下的已存事件 —— 线上序列仍然连续，但被追加运行的最初若干事件可能不会被重新投递。列表在连接时仍是拉取式，因此另一个进程创建的会话会在重新加载后出现在侧边栏，而 workspace-attach 插件早已把它归入其项目工作区。

观察者给 Host 增加了后台工作：每个轮询间隔一次 `persistence.list()`，以及每个被收养会话一个监视器与一个轮询定时器。二者都受配置约束（Session Controller 上的 `coldActivityPollMs`、`coldActivityIdleMs`、`coldActivityMaxSessions`；JSONL 后端上的 `watchPollIntervalMs`）。Web GUI 文案未变 —— 不需要新的面向用户的字符串。

## 测试

`packages/session/session-persistence-jsonl/tests/appends.spec.ts` 在同一根目录上挂载两个后端，第二个代表另一个进程（因为写入所有权是进程内的），并钉住：另一个写入方的追加以其被打包存储行所承载的逻辑事件形式到达、每次持久追加一个批次、纯文本行在补齐它的最后字节到达前被扣留、Zstandard 帧在补齐它的最后字节到达前被扣留、重新发出该流已报告过序号的追加被吸收从而使随后的连续追加仍能到达、跳过某个序号的追加终止该流、订阅在 `close()` 之后停止报告、提供方 fiber 拆除时释放每个订阅、对没有产物的会话予以拒绝，以及跟随方锚定在既有前缀之后。

`packages/api/session-controller/tests/session-durable.host.spec.ts` 组合真实的 Session Controller、Session store、Agent registry 与 JSONL 后端，并钉住：另一个写入方处于回合中途时冷行为运行中、两次 `api-session/status` 转换（运行中，随后在 `turn/end` 到达时落定）、本 Host 持有的会话改为经由进程内路径报告、跟随方按 seq 顺序收到被追加回合的事件，以及本 Host 持有的会话完全不被持久观察，以及被打开的冷会话不会挂载到本 Host，因此第二次 follow 仍能流式接收另一进程的追加。

## 验证

`pnpm run typecheck`（两个编译面）通过，`pnpm exec vitest run packages/session/session-persistence packages/session/session-persistence-jsonl packages/api/session-controller packages/session-query/session-query` 通过，46 个文件 826 项测试，其中包含上面两个套件。本次变更未运行：录制的会话快照（有意未重新录制），以及在 GUI 中实时观察某个委派任务的真实双进程端到端运行。
