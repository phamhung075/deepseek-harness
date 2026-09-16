# Agent Note: Publishing live Assistant frames beside a Session artifact

Status: implemented

English | [中文](2026-09-16-live-assistant-frame-side-channel.zh.md)

## Problem

`agent/assistant-stream` is the loop's publication of live Assistant output: an ordered start, transient chunk frames, and a committed end. It is deliberately process-local, and durable replay reads the compact timed stream embedded in the `assistant/message` or `assistant/attempt` settlement instead, so the stored log carries the text only once the attempt commits. A Host that follows a Session another process runs therefore receives committed rows and nothing between them, because the follower's live frames come only from its own in-process `agent/assistant-stream` bus, which never fires for another process's Agent. The browser draws each settlement whole instead of streaming it.

Delegated work makes that the normal case. `dsh --profile acp` runs in its own process, started by `dsh-subagent-acp` or by an offload bridge, and the Web GUI follows the artifact that child appends. The child streams tokens that reach no one but its own ACP client; the GUI's transcript jumps at each settlement.

## Decision

The ACP profile publishes each owned Session's live frames to a plaintext side channel beside that Session's artifact, and the Web follower tails the channel. The durable settlement stays the only replay authority; the channel is presentation data with no durability promise.

**The capability is a second optional companion Service Definition, not a change to `SessionPersistence`.** The JSONL provider's append observer already resolves a Session's artifact path and owns its directory, so `ctx.sessionStreams` (`@deepseek-ai/dsh-session-persistence/streams`) sits beside `ctx.sessionAppends`, whose `watch(id)` carries the durable events. `SessionStreams.publish(id)` opens the publishing channel and `watch(id)` follows it. The provider owns the file and its path; the ACP profile is the publishing Consumer, the Session Controller is the tailing Consumer, and both read the service with `ctx.get('sessionStreams')`, so a backend that does not register it leaves them silent rather than broken.

**The channel is one plaintext JSONL file inside the Session directory.** It is named `<generation stem>.stream.jsonl` — the located artifact's filename with its `.jsonl` or `.jsonl.zstd` suffix replaced by `.stream.jsonl`, for example `session.v3.stream.jsonl`. No generation parser accepts that name, so the channel can never be read as the durable log, and a reader that lists generations ignores it. Plain text avoids paying Zstandard framing for frames that are small, high-frequency, and short-lived, and keeps a torn writer tail a single incomplete line. `sessionDir` already documents the directory as the home "for future session-local artifacts", which is the file's lifecycle: it is removed with the Session.

**One record is one line.** Each line is `{"at": <next durable seq>, "frame": <AssistantStreamFrame>}`, serialized from the process-local frame the loop emitted plus the Session's next seq at that moment. Storing the raw frame and the cursor — rather than a pre-built wire object — keeps the conversion in its single owner: the Session Controller's `wireAssistantStreamFrame`, which the Web path already uses to stamp a `start` frame's `startedAfterSeq`. The ACP package depends on the Agent runtime and the persistence seam only, never on the Web BFF's wire vocabulary.

**A torn writer tail is repaired by its owner.** The writer opens the channel for append and, when the file's last byte is not a newline, truncates the incomplete record before writing, so a writer killed mid-frame cannot leave a partial record that the next writer's line would be concatenated onto. The reader withholds an incomplete trailing line until the bytes completing it land, and a reader that finds a shorter or replaced file restarts from byte zero, which a tailing consumer absorbs by its own position.

**The follower opens the channel before its snapshot.** A follow that opts into `assistantStream` opens the stream subscription before its opening observation and delivers only frames that arrive after the snapshot is emitted, so the snapshot stays the single cut the client resumes from. The in-process bus stays authoritative for a Session this Host runs: the channel is opened only when `ctx.sessions.get(id)` finds no in-process Session, exactly as the event subscription is, and a composition with no registered channel opens nothing and suspends nothing.

**Publication is default-on for the ACP profile and configurable.** The ACP plugin's `publishAssistantStream` field (default `true`) turns it off for a deployment that does not want the file, and the profile stays silent when the persistence provider offers no channel.

## Alternatives considered

**Publish the frames over the ACP wire only.** The bridge that started the child would see live chunks, but the Web GUI follows the artifact rather than any ACP connection, so the GUI stays frozen while the bridge's client improves. The channel serves every follower that already reads the artifact, including clients that never speak ACP.

**Persist the frames as durable Session events.** It needs no side file and reuses the append path, but it writes every transient chunk into the append-only log. The [embedded Assistant stream decision](2026-09-01-v2-embedded-assistant-streams.md) settled that a stream is committed once at settlement and that live chunks are transient; a per-chunk durable event would inflate every log and contradict that authority.

**Let the offload bridge publish.** The bridge already receives the child's ACP updates, so it could write the channel itself, but that works only for jobs that bridge started and couples the GUI to one launcher. The artifact is the one surface every writer shares.

**Have the JSONL provider subscribe to `agent/assistant-stream` itself.** It would remove the ACP-side publisher entirely, but it puts an Agent-runtime event inside the storage backend and makes every profile write side files for its own in-process Sessions.

**Store the channel in a separate per-session directory outside the store.** It avoids resolving the artifact path, but the file loses the artifact's lifecycle and needs its own discovery, naming, and cleanup rules.

## Consequences

The Web GUI streams text for a delegated ACP Session instead of jumping per settlement, with no client, wire, or renderer change: the channel carries the same record the in-process bus produces.

The channel is best-effort by construction. Frames written after the last durable settlement, or into a torn tail, are lost without a repair path, because the durable log remains the replay source and the client resynchronizes from it. A frame is presentation data, never an input to the model, so nothing model-visible depends on it.

The publisher adds a bounded write queue per publishing Session and one file per Session, removed with the Session directory. The tailing consumer adds one watcher and one safety-net poll per followed foreign Session, bounded by the same subscription lifecycle as the event observer. Both are configuration (`publishAssistantStream` on the ACP plugin; the JSONL backend's watch poll cadence), so a deployment can turn them off.

## Testing

`packages/session/session-persistence-jsonl/tests/streams.spec.ts` mounts a backend over one root and pins: frames published after a tail opened are read back in order, an incomplete trailing line is withheld until its final bytes land, a replaced channel restarts the reader, a writer repairs an incomplete trailing record before appending, an unknown Session is refused, and both the sink and the subscriptions are released on backend teardown.

`packages/acp/acp/tests/assistant-stream.spec.ts` pins that a prompted Session's live frames reach its own channel, that a second Session gets none, and that `publishAssistantStream: false` writes nothing.

`packages/api/session-controller/tests/session-streams.host.spec.ts` pins that a follower of a Session another writer owns receives the channel's frames, while a Session this Host holds keeps the in-process path.
