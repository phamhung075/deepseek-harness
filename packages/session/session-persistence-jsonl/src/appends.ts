/**
 * The JSONL backend's `ctx.sessionAppends` provider. It reports the events a
 * writer appends to an artifact by decoding only the frames added after the
 * subscription opened, so a session another process is running streams without
 * re-reading a log this process cannot see the writer of.
 * @module dsh-session-persistence-jsonl/appends
 */

import { Context } from '@deepseek-ai/cordis'
import type { SessionEvent, SessionId } from '@deepseek-ai/dsh-session'
import {
  SessionAppends,
  SessionPersistenceNotFoundError,
  type SessionAppendSubscription,
  type SessionAppendsWatchOptions,
} from '@deepseek-ai/dsh-session-persistence'
import { AppendTailReader } from './append-tail.ts'
import type { JsonlCompression } from './format.ts'
import { closeTailSubscriptions, openTailSubscription, TailSubscriptionHandle } from './tail-subscription.ts'

/** Deployment configuration of the JSONL append observer. */
export interface JsonlAppendsConfig {
  /**
   * Safety-net poll cadence in milliseconds for appends the platform file
   * watcher does not report. A reported append is observed immediately; this
   * interval bounds the wait when the watcher is silent or unavailable.
   */
  readonly pollIntervalMs: number
  /** Physical encoding of the artifacts this observer reads. */
  readonly compression: JsonlCompression
  /**
   * Resolve one session's current-generation artifact path.
   * @param id - the stored session to locate.
   * @param signal - optional cancellation for the directory scans.
   * @returns the artifact path, or `undefined` when the session has none.
   */
  readonly resolveLog: (id: SessionId, signal?: AbortSignal) => Promise<string | undefined>
}

/** One owned observation of a session's durable appends, over the shared file-tail subscription. */
class JsonlAppendSubscription extends TailSubscriptionHandle<SessionEvent> implements SessionAppendSubscription {
  /** Batches of durable events, as the append seam names them. */
  readonly events = this.batches
}

/**
 * The JSONL persistence backend's durable-append observer. It registers as
 * `ctx.sessionAppends` beside `ctx.sessionPersistence` and owns every
 * subscription it hands out.
 */
export class JsonlSessionAppends extends SessionAppends {
  /** Backend label for diagnostics and effects; shadows `Service.name` without changing the service key. */
  override readonly name = 'session-persistence-jsonl.appends'

  private readonly subscriptions = new Set<TailSubscriptionHandle<SessionEvent>>()

  /**
   * @param ctx - the provider fiber context.
   * @param config - watch cadence, artifact encoding, and path resolution.
   */
  constructor(ctx: Context, private readonly config: JsonlAppendsConfig) {
    super(ctx)
    ctx.effect(() => () => this.closeAll(), 'session-persistence-jsonl.appends')
  }

  /**
   * Follow one stored session's durable appends; see the seam contract.
   * @param id - the stored session to follow.
   * @param options - optional cancellation of the subscription.
   * @returns the owned subscription.
   * @throws {SessionPersistenceNotFoundError} when the session has no current stored artifact.
   */
  async watch(id: SessionId, options?: SessionAppendsWatchOptions): Promise<SessionAppendSubscription> {
    options?.signal?.throwIfAborted()
    const path = await this.config.resolveLog(id, options?.signal)
    if (path === undefined) throw new SessionPersistenceNotFoundError(id)
    const reader = new AppendTailReader(path, this.config.compression)
    if (!await reader.start(options?.signal)) throw new SessionPersistenceNotFoundError(id)
    return openTailSubscription<SessionEvent, JsonlAppendSubscription>(
      JsonlAppendSubscription,
      this.subscriptions,
      id,
      path,
      reader,
      this.config.pollIntervalMs,
      options?.signal,
    )
  }

  /**
   * Close every live subscription and wait for its read loop to settle.
   * @returns resolution once no observation is still reading.
   */
  async closeAll(): Promise<void> {
    await closeTailSubscriptions(this.subscriptions)
  }
}

export default JsonlSessionAppends
