/**
 * The JSONL backend's `ctx.sessionStreams` provider. It publishes one Session's
 * live Assistant frames into a plaintext side channel beside that Session's
 * artifact, and tails that channel for a Host that did not run the Agent.
 * @module dsh-session-persistence-jsonl/streams
 */

import { Context } from '@deepseek-ai/cordis'
import type { SessionId } from '@deepseek-ai/dsh-session'
import {
  SessionPersistenceNotFoundError,
  SessionStreams,
  type SessionStreamRecord,
  type SessionStreamSink,
  type SessionStreamSubscription,
  type SessionStreamsWatchOptions,
} from '@deepseek-ai/dsh-session-persistence'
import { JsonlStreamSink, StreamTailReader, streamChannelPath } from './stream-channel.ts'
import { closeTailSubscriptions, openTailSubscription, TailSubscriptionHandle } from './tail-subscription.ts'

/** Deployment configuration of the JSONL live-frame side channel. */
export interface JsonlStreamsConfig {
  /**
   * Safety-net poll cadence in milliseconds for frames the platform file
   * watcher does not report. A reported append is observed immediately; this
   * interval bounds the wait when the watcher is silent or unavailable.
   */
  readonly pollIntervalMs: number
  /** Maximum records buffered per publishing Session while a write is in flight. */
  readonly maxPendingRecords: number
  /**
   * Resolve one session's current-generation artifact path.
   * @param id - the stored session to locate.
   * @param signal - optional cancellation for the directory scans.
   * @returns the artifact path, or `undefined` when the session has none.
   */
  readonly resolveLog: (id: SessionId, signal?: AbortSignal) => Promise<string | undefined>
}

/** One owned live-frame subscription, over the shared file-tail subscription. */
class JsonlStreamSubscription extends TailSubscriptionHandle<SessionStreamRecord> implements SessionStreamSubscription {
  /** Batches of live frames, as the stream seam names them. */
  readonly frames = this.batches
}

/**
 * The JSONL backend's live-frame side-channel provider. It registers as
 * `ctx.sessionStreams` beside `ctx.sessionPersistence` and `ctx.sessionAppends`
 * and owns every sink and subscription it hands out.
 */
export class JsonlSessionStreams extends SessionStreams {
  /** Backend label for diagnostics and effects; shadows `Service.name` without changing the service key. */
  override readonly name = 'session-persistence-jsonl.streams'

  private readonly subscriptions = new Set<TailSubscriptionHandle<SessionStreamRecord>>()
  private readonly sinks = new Set<JsonlStreamSink>()

  /**
   * @param ctx - the provider fiber context.
   * @param config - watch cadence, queue bound, and path resolution.
   */
  constructor(ctx: Context, private readonly config: JsonlStreamsConfig) {
    super(ctx)
    ctx.effect(() => () => this.closeAll(), 'session-persistence-jsonl.streams')
  }

  /**
   * Open one Session's publishing channel; see the seam contract. The path is
   * resolved on the first published frame, so a Session that has not
   * materialized yet is not an error.
   * @param id - the Session whose frames this channel publishes.
   * @returns the owned sink.
   */
  publish(id: SessionId): SessionStreamSink {
    const sink = new JsonlStreamSink(
      async () => {
        const artifact = await this.config.resolveLog(id)
        return artifact === undefined ? undefined : streamChannelPath(artifact)
      },
      {
        maxPendingRecords: this.config.maxPendingRecords,
        onError: (error) => {
          this.ctx.logger.warn(`session-persistence-jsonl: live-frame channel for "${id}" stopped: ${String(error)}`)
        },
        onDropped: () => {
          this.ctx.logger.warn(`session-persistence-jsonl: live-frame channel for "${id}" dropped frames past its queue bound`)
        },
        onClose: () => { this.sinks.delete(sink) },
      },
    )
    this.sinks.add(sink)
    return sink
  }

  /**
   * Follow one stored Session's live frames; see the seam contract.
   * @param id - the stored Session to follow.
   * @param options - optional cancellation of the subscription.
   * @returns the owned subscription.
   * @throws {SessionPersistenceNotFoundError} when the Session has no stored artifact.
   */
  async watch(id: SessionId, options?: SessionStreamsWatchOptions): Promise<SessionStreamSubscription> {
    options?.signal?.throwIfAborted()
    const artifact = await this.config.resolveLog(id, options?.signal)
    if (artifact === undefined) throw new SessionPersistenceNotFoundError(id)
    const path = streamChannelPath(artifact)
    const reader = new StreamTailReader(path)
    // A channel that does not exist yet is an empty stream: the writer creates it
    // on its first frame, and the reader then delivers from that start.
    await reader.start(options?.signal)
    return openTailSubscription<SessionStreamRecord, JsonlStreamSubscription>(
      JsonlStreamSubscription,
      this.subscriptions,
      id,
      path,
      reader,
      this.config.pollIntervalMs,
      options?.signal,
    )
  }

  /**
   * Close every live subscription and publishing channel and wait for their work to settle.
   * @returns resolution once no observation is still reading and no sink is still writing.
   */
  async closeAll(): Promise<void> {
    const sinks = [...this.sinks]
    for (const sink of sinks) this.sinks.delete(sink)
    await Promise.allSettled([
      closeTailSubscriptions(this.subscriptions),
      ...sinks.map(sink => sink.close()),
    ])
  }
}

export default JsonlSessionStreams
