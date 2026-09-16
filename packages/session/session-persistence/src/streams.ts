/**
 * Live Assistant-frame side channel Service Definition (`ctx.sessionStreams`).
 * A Session's live frames are process-local, so a Host that did not run the
 * Agent cannot see them from the durable log until the attempt settles.
 * `SessionPersistence` alone cannot express that observation; this companion
 * definition carries it beside `SessionAppends`, which owns the durable events.
 * @module @deepseek-ai/dsh-session-persistence/streams
 */

import { Context, Service } from '@deepseek-ai/cordis'
import type { SessionId } from '@deepseek-ai/dsh-session'
import type { JsonValue } from '@deepseek-ai/dsh-util-values'

/** One record read from a Session's live-frame side channel. */
export interface SessionStreamRecord {
  /** The publishing Session's next durable seq when the frame was published; `0` before any event. */
  readonly seq: number
  /** The Assistant stream frame, exactly as the publisher serialized it. */
  readonly frame: JsonValue
}

/** One owned publishing channel onto a Session's live-frame side channel. */
export interface SessionStreamSink {
  /**
   * Publish one frame. Publication is ordered and best-effort: a frame beyond
   * the channel's queue bound, or one published after a write failure or
   * {@link SessionStreamSink.close}, is dropped.
   * @param seq - the publishing Session's next durable seq.
   * @param frame - the JSON frame to publish.
   */
  append(seq: number, frame: JsonValue): void
  /**
   * Flush queued frames and release the channel; idempotent.
   * @returns resolution once no write remains in flight.
   */
  close(): Promise<void>
}

/** Options for {@link SessionStreams.watch}. */
export interface SessionStreamsWatchOptions {
  /** Optional cancellation; aborting ends the stream and releases the subscription. */
  readonly signal?: AbortSignal
}

/** One live subscription to one stored Session's live-frame side channel. */
export interface SessionStreamSubscription extends AsyncDisposable {
  /** The stored Session this subscription follows. */
  readonly id: SessionId
  /**
   * Batches of records published after the subscription resolved, in
   * publication order. Each yielded array is owned by the consumer. Reading
   * the subscription starts the observation; the iterable ends when the
   * subscription closes or its signal aborts, and rejects on a corrupt channel.
   */
  readonly frames: AsyncIterable<readonly SessionStreamRecord[]>
  /** Release the subscription and its backend resources; idempotent. */
  close(): void
}

declare module '@deepseek-ai/cordis' {
  interface Context {
    sessionStreams: SessionStreams
  }
}

/**
 * Live Assistant-frame observation over stored Sessions.
 *
 * A subscription reports only frames published after it resolved, so a
 * consumer that must continue from a stored cut opens the subscription FIRST
 * and reads that cut afterwards; a frame published in between arrives on the
 * stream and the consumer's own cursor decides whether it already showed it.
 * An incomplete trailing record is withheld until the bytes completing it
 * land, and a backend that loses its place may replay frames a consumer
 * already saw rather than leave a hole, so every consumer tolerates a record
 * at or below its cursor.
 *
 * Presence is deployment-shaped: a backend that cannot resolve a Session's
 * artifact does not register this service, and a consumer reads it with
 * `ctx.get('sessionStreams')` rather than `ctx.sessionStreams`.
 */
export abstract class SessionStreams extends Service {
  constructor(ctx: Context) {
    super(ctx, 'sessionStreams')
  }

  /**
   * Open the publishing channel for one Session's live frames. The channel is
   * created on the first published frame, so a Session that has not
   * materialized yet is not an error.
   * @param id - the Session whose frames this channel publishes.
   * @returns the owned sink.
   */
  abstract publish(id: SessionId): SessionStreamSink

  /**
   * Follow one stored Session's live frames.
   * @param id - the stored Session to follow.
   * @param options - optional cancellation of the subscription.
   * @returns the owned subscription.
   * @throws {SessionPersistenceNotFoundError} when the Session has no stored artifact yet.
   */
  abstract watch(id: SessionId, options?: SessionStreamsWatchOptions): Promise<SessionStreamSubscription>
}

export default SessionStreams
