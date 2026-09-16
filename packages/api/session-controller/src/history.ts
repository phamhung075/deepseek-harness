/** Cold Session history pagination and live-event source. */

import type { Context } from '@deepseek-ai/cordis'
import { Deque } from '@deepseek-ai/dsh-deque'
import type { AssistantStreamFrame } from '@deepseek-ai/dsh-agent'
import {
  isAppendSurfaceEvent,
  SessionLogOffset,
  SessionSeq,
} from '@deepseek-ai/dsh-session'
import type {
  SessionEvent,
  SessionHeader,
  SessionId,
  SessionLogOffset as SessionLogOffsetType,
  SessionSeqCursor,
} from '@deepseek-ai/dsh-session'
import { SessionQueryError, type SessionObservation } from '@deepseek-ai/dsh-session-query'
import type {
  SessionAppends,
  SessionAppendSubscription,
  SessionStreams,
  SessionStreamSubscription,
} from '@deepseek-ai/dsh-session-persistence'
import type {} from '@deepseek-ai/dsh-subagent'
import { RemoteError } from '@deepseek-ai/dsh-typert-protocol'
import type { JsonValue } from '@deepseek-ai/dsh-util-values'
import type {
  SessionAddress,
  SessionAssistantStreamFrame,
  SessionEventEntry,
  SessionFollowRequest,
  SessionFollowFrame,
  SessionHistoryRecord,
  SessionPage,
  SessionPageRequest,
  SessionProjectionBaseline,
  SessionProjectionValues,
  SessionWireHeader,
  SessionWireEvent,
} from './types.ts'
import { SessionAssistantStreamAccumulator } from './assistant-stream.ts'

const DEFAULT_MAX_MESSAGES = 50
const MESSAGE_TYPES = new Set(['user/message', 'assistant/message'])

/** One unit a follower queues: a durable event or an opted-in assistant-stream frame. */
type FollowBufferItem =
  | { readonly type: 'event'; readonly event: SessionEvent }
  | {
    readonly type: 'assistant-stream'
    readonly frame: SessionAssistantStreamFrame
    readonly ordinal: number
  }

/** Implements cold-safe history operations delegated by the Session Controller. */
export class SessionHistoryController {
  private readonly closeFollowers = new Set<() => void>()
  private readonly assistantStreams = new Map<SessionId, SessionAssistantStreamAccumulator>()

  /**
   * @param ctx - Host context carrying Session query and projection services.
   * @param promote - starts ordinary Session activation after snapshot delivery.
   */
  constructor(
    private readonly ctx: Context,
    private readonly promote: (observation: SessionObservation) => void,
  ) {
    ctx.on('agent/assistant-stream', ({ agent, frame }) => {
      let stream = this.assistantStreams.get(agent.session.id)
      if (stream === undefined) {
        stream = new SessionAssistantStreamAccumulator()
        this.assistantStreams.set(agent.session.id, stream)
      }
      stream.accept(frame, cursorBeforeNext(agent.session.seq))
    }, { global: true })
    ctx.on('agent/disposed', ({ agent }) => {
      this.assistantStreams.delete(agent.session.id)
    }, { global: true })
    ctx.effect(() => () => {
      for (const close of this.closeFollowers) close()
      this.closeFollowers.clear()
    }, 'session-controller.history')
  }

  /**
   * Read one message-aligned history page without activating an Agent.
   * @param request - durable address and backwards-page cursor.
   * @param signal - caller cancellation for persistence reads.
   * @returns a contiguous event page.
   */
  async page(request: SessionPageRequest, signal: AbortSignal): Promise<SessionPage> {
    validatePageRequest(request)
    const throughSeq: SessionSeqCursor = request.throughSeq === -1
      ? -1
      : SessionSeq(request.throughSeq)
    const beforeSeq = request.beforeSeq === undefined
      ? undefined
      : SessionLogOffset(request.beforeSeq)
    using source = await this.sourceFor(request.address, signal, false)
    signal.throwIfAborted()
    const sourceLog = source.events
    const sourceCursor: SessionSeqCursor = sourceLog.at(-1)?.seq ?? -1
    if (throughSeq > sourceCursor) {
      throw new RemoteError(
        'gateway/bad-request',
        `session page through seq ${String(throughSeq)} is past cursor ${String(sourceCursor)}`,
        {},
      )
    }
    /* v8 ignore next -- Session and persistence validation guarantee a dense zero-based event prefix. */
    if (throughSeq >= 0 && sourceLog[throughSeq]?.seq !== throughSeq) {
      throw new RemoteError('gateway/internal', `session log does not contain through seq ${String(throughSeq)}`, {})
    }
    const page = paginate(
      sourceLog,
      beforeSeq,
      request.maxMessages ?? DEFAULT_MAX_MESSAGES,
      throughSeq,
    )
    const records = pageRecords(page.events)
    return {
      records,
      hasMore: page.hasMore,
    }
  }

  /**
   * Follow events appended after an initial cursor on one durable address.
   * @param request - durable address and last committed sequence already held by the caller.
   * @param signal - stream cancellation owned by the Remote carrier.
   * @returns a complete opening snapshot followed by gap-free durable events and opted-in assistant frames.
   */
  async *follow(request: SessionFollowRequest, signal: AbortSignal): AsyncIterable<SessionFollowFrame> {
    validateFollowRequest(request)
    const { address } = request
    const target = addressId(address)
    const buffered = new Deque<FollowBufferItem>()
    let snapshotCursor: SessionSeqCursor | undefined
    let assistantStreamOrdinal = 0
    let wake: (() => void) | undefined
    const notify = (): void => {
      const resume = wake
      wake = undefined
      resume?.()
    }
    const follower = { closed: false }
    const close = (): void => {
      follower.closed = true
      notify()
    }
    this.closeFollowers.add(close)
    const disposeEvent = this.ctx.on('session/event', (session, event) => {
      if (session.id !== target) return
      buffered.pushBack({ type: 'event', event })
      notify()
    }, { global: true })
    const disposeCreated = this.ctx.on('session/created', (session) => {
      if (session.id !== target) return
      // Constructor seed events have no session/event notification. Normally
      // only the end-seed suffix is new; if persistence advanced after the
      // opening observation, replay everything beyond that snapshot cursor.
      // oxlint-disable-next-line typescript/no-deprecated -- Existing Session history read; migration deferred.
      const suffix = session.snapshotEvents(snapshotCursor === undefined
        ? session.firstLiveSeq
        : SessionLogOffset(snapshotCursor + 1))
      for (let index = suffix.length - 1; index >= 0; index -= 1) {
        buffered.pushFront({ type: 'event', event: suffix[index] as SessionEvent })
      }
      notify()
    }, { global: true })
    const disposeAssistantStream = request.assistantStream !== true
      ? undefined
      : this.ctx.on('agent/assistant-stream', ({ agent, frame }) => {
        if (agent.session.id !== target) return
        buffered.pushBack({
          type: 'assistant-stream',
          frame: wireAssistantStreamFrame(frame, cursorBeforeNext(agent.session.seq)),
          ordinal: ++assistantStreamOrdinal,
        })
        notify()
      }, { global: true })
    const onAbort = (): void => { notify() }
    signal.addEventListener('abort', onAbort, { once: true })
    // A session another process is running never reaches the in-process bus, so
    // its appends come from the durable observer. Opening it before the snapshot
    // read closes the window between the two: an event durable in between is
    // delivered here and falls at or below the snapshot cursor, where the seq
    // check below drops it. A composition without the capability opens nothing
    // and suspends nothing, so the snapshot keeps its original timing.
    const appends = this.ctx.get('sessionAppends')
    const durable = appends === undefined || this.ctx.sessions.get(target) !== undefined
      ? undefined
      : await this.openDurableFollow(target, appends, signal)
    const stopDurable = durable === undefined
      ? undefined
      : this.pumpDurable(target, durable, buffered, notify)
    // The side channel is opened here for the same reason as the append
    // subscription: a frame published between this call and the snapshot read
    // arrives on the stream, and the ordinal cut below keeps everything the
    // snapshot already preceded off the wire.
    const streams = this.ctx.get('sessionStreams')
    const durableStream = request.assistantStream !== true
      || streams === undefined
      || this.ctx.sessions.get(target) !== undefined
      ? undefined
      : await this.openDurableStream(target, streams, signal)
    const stopDurableStream = durableStream === undefined
      ? undefined
      : this.pumpDurableStream(target, durableStream, buffered, () => ++assistantStreamOrdinal, notify)
    try {
      using source = await this.sourceFor(address, signal, true)
      const events = source.events
      signal.throwIfAborted()
      const cursor = source.cursor
      snapshotCursor = cursor
      const page = paginate(events, undefined, request.maxMessages ?? DEFAULT_MAX_MESSAGES)
      const assistantStream = request.assistantStream === true
        ? this.assistantStreams.get(target)?.snapshot() ?? { revision: 0 }
        : undefined
      // The accumulator snapshot and this watermark are synchronous. Frames
      // through the cut are represented or superseded by that baseline,
      // including larger revisions from a retired Agent; later revision
      // resets reach Client continuity validation.
      const assistantStreamOrdinalCut = assistantStreamOrdinal
      yield {
        type: 'snapshot',
        header: wireHeader(source.header),
        cursor,
        records: pageRecords(page.events),
        hasMore: page.hasMore,
        projections: source.projections === undefined
          ? { asOfSeq: cursor, values: {} }
          : projectionBlock(source.projections),
        ...assistantStream === undefined ? {} : { assistantStream },
      }
      // A session this Host does not run is followed through its durable appends.
      // Promoting it here would attach it to this process, which makes the Session
      // list treat the in-process Agent as authoritative and makes the next follow
      // take the in-process bus, so the row would report idle and the transcript
      // would freeze while the other process kept working. Activation therefore
      // stays on demand: prompting resolves the Agent through `resolveAgent`.
      if (durable === undefined && address.kind === 'session' && source.source === 'prepared') {
        const promotion = source.retain()
        try {
          this.promote(promotion)
        } catch (error: unknown) {
          promotion[Symbol.dispose]()
          throw error
        }
      }
      // A session another process owns publishes nothing on this process's bus,
      // so its transcript would freeze until the next reload. Its durable
      // appends arrive through the subscription opened above; the loop below
      // drops any seq its own cursor has passed.
      let nextOffset = SessionLogOffset(cursor + 1)
      while (!follower.closed && !signal.aborted) {
        const item = buffered.popFront()
        if (item === undefined) {
          await new Promise<void>((resolve) => { wake = resolve })
          continue
        }
        if (item.type === 'assistant-stream') {
          if (item.ordinal > assistantStreamOrdinalCut) {
            yield { type: 'assistant-stream', frame: item.frame }
          }
          continue
        }
        const expectedSeq = SessionSeq(nextOffset)
        if (item.event.seq < expectedSeq) continue
        if (item.event.seq !== expectedSeq) {
          throw new RemoteError('gateway/internal', `session event stream skipped seq ${String(expectedSeq)}`, {})
        }
        nextOffset = SessionLogOffset(nextOffset + 1)
        yield entryFor(item.event)
      }
    } finally {
      this.closeFollowers.delete(close)
      signal.removeEventListener('abort', onAbort)
      stopDurable?.()
      stopDurableStream?.()
      disposeCreated()
      disposeEvent()
      disposeAssistantStream?.()
    }
  }

  /**
   * Subscribe to the durable appends of a session this Host does not run.
   * @param id - the addressed session.
   * @param appends - the registered durable-append observer.
   * @param signal - the follow's cancellation, which also ends the subscription.
   * @returns the subscription, or `undefined` when the backend refuses this session.
   */
  private async openDurableFollow(
    id: SessionId,
    appends: SessionAppends,
    signal: AbortSignal,
  ): Promise<SessionAppendSubscription | undefined> {
    try {
      return await appends.watch(id, { signal })
    } catch (error: unknown) {
      // The snapshot read below reports a session that has no artifact at all;
      // a backend that refuses this one keeps the in-process source.
      this.ctx.logger.warn(`session-controller: durable follow of "${id}" is unavailable: ${String(error)}`)
      return undefined
    }
  }

  /**
   * Feed one session's durable appends into a follower's buffer.
   * @param id - the followed session.
   * @param subscription - its live durable-append subscription.
   * @param buffered - the follower's ordered buffer.
   * @param notify - wakes the follower loop after a batch lands.
   * @returns the disposer stopping the subscription and its fold.
   */
  private pumpDurable(
    id: SessionId,
    subscription: SessionAppendSubscription,
    buffered: Deque<FollowBufferItem>,
    notify: () => void,
  ): () => void {
    const task = (async () => {
      try {
        for await (const batch of subscription.events) {
          for (const event of batch) buffered.pushBack({ type: 'event', event })
          notify()
        }
      } catch (error: unknown) {
        this.ctx.logger.warn(`session-controller: durable follow of "${id}" failed: ${String(error)}`)
      }
    })()
    return () => { subscription.close(); void task }
  }

  /**
   * Subscribe to the live-frame side channel of a session this Host does not run.
   * @param id - the addressed session.
   * @param streams - the registered live-frame side channel.
   * @param signal - the follow's cancellation, which also ends the subscription.
   * @returns the subscription, or `undefined` when the backend refuses this session.
   */
  private async openDurableStream(
    id: SessionId,
    streams: SessionStreams,
    signal: AbortSignal,
  ): Promise<SessionStreamSubscription | undefined> {
    try {
      return await streams.watch(id, { signal })
    } catch (error: unknown) {
      // A session with no artifact is reported by the snapshot read; a backend
      // that refuses this one keeps the committed rows and drops live frames.
      this.ctx.logger.warn(`session-controller: live-frame follow of "${id}" is unavailable: ${String(error)}`)
      return undefined
    }
  }

  /**
   * Feed one session's live frames into a follower's buffer.
   * @param id - the followed session.
   * @param subscription - its live side-channel subscription.
   * @param buffered - the follower's ordered buffer.
   * @param nextOrdinal - allocates the next assistant-stream ordinal.
   * @param notify - wakes the follower loop after a batch lands.
   * @returns the disposer stopping the subscription and its fold.
   */
  private pumpDurableStream(
    id: SessionId,
    subscription: SessionStreamSubscription,
    buffered: Deque<FollowBufferItem>,
    nextOrdinal: () => number,
    notify: () => void,
  ): () => void {
    const task = (async () => {
      try {
        for await (const batch of subscription.frames) {
          for (const record of batch) {
            const frame = channelAssistantStreamFrame(record.frame)
            if (frame === undefined) {
              this.ctx.logger.warn(`session-controller: live-frame follow of "${id}" skipped an unreadable frame`)
              continue
            }
            buffered.pushBack({
              type: 'assistant-stream',
              frame: wireAssistantStreamFrame(frame, cursorBeforeNext(SessionLogOffset(record.seq))),
              ordinal: nextOrdinal(),
            })
          }
          notify()
        }
      } catch (error: unknown) {
        this.ctx.logger.warn(`session-controller: live-frame follow of "${id}" failed: ${String(error)}`)
      }
    })()
    return () => { subscription.close(); void task }
  }

  private async sourceFor(
    address: SessionAddress,
    signal: AbortSignal,
    withProjections: boolean,
  ): Promise<SessionObservation> {
    const sessionId = addressId(address)
    try {
      const observation = await this.ctx.sessionQuery.observeSession(sessionId, {
        signal,
        projectionMode: withProjections || address.kind === 'subagent' ? 'all' : 'none',
      })
      if (observation.header.cwd === undefined) {
        observation[Symbol.dispose]()
        rejectNotFound(address)
      }
      try {
        validateAddress(
          address,
          observation.header,
          observation.inheritedEventCount,
          observation.projections,
        )
      } catch (error: unknown) {
        observation[Symbol.dispose]()
        throw error
      }
      return observation
    } catch (error: unknown) {
      if (error instanceof SessionQueryError
        && error.code === 'SESSION_QUERY_SESSION_NOT_FOUND') rejectNotFound(address)
      throw error
    }
  }

}

function cursorBeforeNext(nextSeq: SessionLogOffsetType): SessionSeqCursor {
  return nextSeq === 0 ? -1 : SessionSeq(nextSeq - 1)
}

function wireAssistantStreamFrame(
  frame: AssistantStreamFrame,
  durableCursor: SessionSeqCursor,
): SessionAssistantStreamFrame {
  if (frame.type === 'start') return { ...frame, startedAfterSeq: durableCursor }
  if (frame.type === 'end') return frame
  return {
    ...frame,
    chunk: frame.chunk as JsonValue,
  }
}

/**
 * Decode one side-channel frame at the file boundary. The channel carries the
 * loop's frame vocabulary, but bytes written by another process are not
 * trusted blindly: only the discriminant is checked here, because the client
 * validates chunk internals and an unknown frame is presentation data that
 * must not reach the wire.
 * @param value - one frame value read from the side channel.
 * @returns the frame, or `undefined` when it is not a stream frame.
 */
function channelAssistantStreamFrame(value: JsonValue): AssistantStreamFrame | undefined {
  if (typeof value !== 'object' || value === null) return undefined
  const type = (value as { type?: unknown }).type
  if (type !== 'start' && type !== 'chunk' && type !== 'end') return undefined
  return value as unknown as AssistantStreamFrame
}

function projectionBlock(
  snapshot: NonNullable<SessionObservation['projections']>,
): SessionProjectionBaseline {
  return {
    asOfSeq: snapshot.asOfSeq,
    // Projection definitions validate whole JSON values before snapshot publication.
    values: snapshot.values as SessionProjectionValues,
  }
}

function validatePageRequest(request: SessionPageRequest): void {
  if (!Number.isSafeInteger(request.throughSeq)
    || request.throughSeq < -1
    || Object.is(request.throughSeq, -0)) {
    throw new RemoteError('gateway/bad-request', 'throughSeq must be an integer greater than or equal to -1', {})
  }
  if (request.beforeSeq !== undefined
    && (!Number.isSafeInteger(request.beforeSeq)
      || request.beforeSeq < 0
      || Object.is(request.beforeSeq, -0))) {
    throw new RemoteError('gateway/bad-request', 'beforeSeq must be a non-negative safe integer', {})
  }
  if (request.maxMessages !== undefined
    && (!Number.isSafeInteger(request.maxMessages) || request.maxMessages <= 0)) {
    throw new RemoteError('gateway/bad-request', 'maxMessages must be a positive safe integer', {})
  }
}

function validateFollowRequest(request: SessionFollowRequest): void {
  if (request.maxMessages !== undefined
    && (!Number.isSafeInteger(request.maxMessages) || request.maxMessages <= 0)) {
    throw new RemoteError('gateway/bad-request', 'maxMessages must be a positive safe integer', {})
  }
}

function addressId(address: SessionAddress): SessionId {
  return address.kind === 'session' ? address.sessionId : address.childSessionId
}

function validateAddress(
  address: SessionAddress,
  header: SessionHeader,
  inheritedEventCount: SessionLogOffsetType,
  projections: SessionObservation['projections'],
): void {
  if (address.kind === 'session') {
    if (header.origin === 'subagent') {
      throw new RemoteError('session/agent-busy', 'subagent Sessions require their durable parent address', {
        reason: 'use subagent delivery for this child session',
      })
    }
    return
  }
  if (header.origin !== 'subagent' || header.parentSession !== address.parentSessionId) {
    throw new RemoteError('subagent/unauthorized', 'subagent does not belong to the supplied parent', {
      childSessionId: address.childSessionId,
    })
  }
  const identity = projections?.values.subagent
  if (identity === null) {
    throw new RemoteError('subagent/catalog-diagnostic', 'subagent descriptor is corrupt', {
      parentSessionId: address.parentSessionId,
      childSessionId: address.childSessionId,
      reason: 'corrupt',
    })
  }
  if (identity === undefined || identity.seq < inheritedEventCount) {
    throw new RemoteError('subagent/catalog-diagnostic', 'subagent descriptor is unavailable', {
      parentSessionId: address.parentSessionId,
      childSessionId: address.childSessionId,
      reason: 'unsupported',
    })
  }
  if (identity.mode !== address.mode) {
    throw new RemoteError('subagent/unauthorized', 'subagent mode does not match the supplied address', {
      childSessionId: address.childSessionId,
    })
  }
}

function rejectNotFound(address: SessionAddress): never {
  if (address.kind === 'session') {
    throw new RemoteError('session/not-found', `session "${address.sessionId}" not found`, { sessionId: address.sessionId })
  }
  throw new RemoteError('subagent/not-found', 'subagent is unavailable', {
    parentSessionId: address.parentSessionId,
    childSessionId: address.childSessionId,
  })
}

function paginate(
  events: readonly SessionEvent[],
  beforeSeq: SessionLogOffsetType | undefined,
  maxMessages: number,
  throughSeq: SessionSeqCursor = events.at(-1)?.seq ?? -1,
): { readonly events: SessionEvent[]; readonly hasMore: boolean } {
  const end = SessionLogOffset(Math.min(throughSeq + 1, beforeSeq ?? throughSeq + 1))
  let count = 0
  let cut = SessionLogOffset(0)
  for (let index = end - 1; index >= 0; index--) {
    const event = events[index] as SessionEvent
    if (!MESSAGE_TYPES.has(event.type) || !isAppendSurfaceEvent(event)) continue
    count++
    const sources = event.sourceEventSeqs
    let groupStart = event.seq
    if (sources !== undefined) {
      for (const source of sources) {
        if (source < groupStart) groupStart = source
      }
    }
    if (count >= maxMessages) {
      cut = SessionLogOffset(groupStart)
      break
    }
  }
  return { events: events.slice(cut, end), hasMore: cut > 0 }
}

/** Translate current logical Session metadata to the browser wire. */
function wireHeader(header: SessionHeader): SessionWireHeader {
  return { ...header }
}

function entryFor(event: SessionEvent): SessionEventEntry {
  return {
    type: 'event',
    // Session.append validates and freezes event data as JSON before publication.
    event: event as unknown as SessionWireEvent,
  }
}

/** Encode one bounded logical page without changing its pagination cut. */
function pageRecords(events: readonly SessionEvent[]): SessionHistoryRecord[] {
  return events.map(entryFor)
}
