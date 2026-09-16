/**
 * Durable-append observation Service Definition (`ctx.sessionAppends`). One
 * subscription follows one stored session and streams the logical events a
 * writer makes durable after the subscription resolves, whoever that writer is;
 * `SessionPersistence` alone cannot express it, because a Host that did not
 * write a session never sees its appends.
 * @module @deepseek-ai/dsh-session-persistence/appends
 */

import { Context, Service } from '@deepseek-ai/cordis'
import type { SessionEvent, SessionId } from '@deepseek-ai/dsh-session'

/** Options for {@link SessionAppends.watch}. */
export interface SessionAppendsWatchOptions {
  /** Optional cancellation; aborting ends the stream and releases the subscription. */
  readonly signal?: AbortSignal
}

/** One live subscription to one stored session's durable appends. */
export interface SessionAppendSubscription extends AsyncDisposable {
  /** The stored session this subscription follows. */
  readonly id: SessionId
  /**
   * Batches of logical events made durable after the subscription resolved, in
   * seq order and contiguous within this stream. Each yielded array is owned by
   * the consumer. Reading the subscription starts the observation; the iterable
   * ends when the subscription closes or its signal aborts, and rejects on a
   * corrupt artifact.
   */
  readonly events: AsyncIterable<readonly SessionEvent[]>
  /** Release the subscription and its backend resources; idempotent. */
  close(): void
}

declare module '@deepseek-ai/cordis' {
  interface Context {
    sessionAppends: SessionAppends
  }
}

/**
 * Durable-append observation over stored sessions.
 *
 * A subscription reports appends, never the prefix already durable when it
 * opened: a caller that must continue from a stored read opens the
 * subscription FIRST and reads the prefix afterwards, so events durable
 * between the two steps arrive on the stream and the caller's own cursor
 * decides which of them its reader has already consumed. A torn or incomplete
 * trailing record is withheld until the bytes completing it land, and a
 * backend that loses its place (a repair truncating the artifact, a rewritten
 * file) may replay events the consumer already saw rather than leave a hole;
 * every consumer therefore tolerates an event at or below its cursor.
 *
 * Presence is deployment-shaped: a backend that cannot observe foreign appends
 * does not register this service, and a consumer reads it with
 * `ctx.get('sessionAppends')`, never `ctx.sessionAppends`.
 */
export abstract class SessionAppends extends Service {
  constructor(ctx: Context) {
    super(ctx, 'sessionAppends')
  }

  /**
   * Follow one stored session's durable appends.
   * @param id - the stored session to follow.
   * @param options - optional cancellation of the subscription.
   * @returns a subscription that reports every event made durable after this
   *   call resolved, until it is closed.
   * @throws {SessionPersistenceNotFoundError} when the session has no stored artifact yet.
   */
  abstract watch(id: SessionId, options?: SessionAppendsWatchOptions): Promise<SessionAppendSubscription>
}

export default SessionAppends
