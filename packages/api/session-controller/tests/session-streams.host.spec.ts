/**
 * A follower of a Session another process runs receives that Session's live
 * Assistant frames from the side channel beside the artifact, while a Session
 * this Host holds keeps the in-process bus.
 *
 * The "other process" is a second persistence instance over the same root: a
 * backend's write ownership is in-process, so two instances write one artifact
 * exactly as two processes do.
 */

import { afterEach, describe, expect, it } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import { freezeMessage, MessageId } from '@deepseek-ai/dsh-llm'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { SESSION_FORMAT_VERSION, SessionSeq, SessionId } from '@deepseek-ai/dsh-session'
import type { SessionEvent, SessionHeader } from '@deepseek-ai/dsh-session'
import SessionStore from '@deepseek-ai/dsh-session'
import AgentRegistry from '@deepseek-ai/dsh-agent'
import JsonlSessionPersistence from '@deepseek-ai/dsh-session-persistence-jsonl'
import type { SessionFollowFrame } from '../src/types.ts'
import { createSessionTestRemote, type TestSessionRemote } from './test-remote.ts'

const POLL_MS = 20
const IDLE_MS = 30_000
const dirs: string[] = []

interface Mounted {
  readonly ctx: Context
  readonly dispose: () => Promise<void>
}

interface Host extends Mounted {
  readonly remote: TestSessionRemote
}

function header(id: string, cwd: string): SessionHeader {
  return { version: SESSION_FORMAT_VERSION, id: SessionId(id), createdAt: 1000, isSeeded: false, cwd }
}

async function freshRoot(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), 'dsh-cold-streams-'))
  dirs.push(dir)
  return dir
}

/** Mount the observing Host: Session store, Agent registry, persistence, and the Session Controller. */
async function mountHost(root: string): Promise<Host> {
  const ctx = new Context()
  await ctx.plugin(SessionStore)
  await ctx.plugin(AgentRegistry)
  await ctx.plugin(JsonlSessionPersistence, { root, compression: 'zstd', watchPollIntervalMs: POLL_MS })
  const remote = createSessionTestRemote(ctx, {
    defaultModelSelection: () => ({ provider: 'mock', model: 'mock' }),
    cwd: root,
    coldActivityPollMs: POLL_MS,
    coldActivityIdleMs: IDLE_MS,
    coldActivityMaxSessions: 4,
  })
  return { ctx, remote, dispose: async () => { await ctx.fiber.dispose() } }
}

/** Mount another process's view of the same store: persistence only. */
async function mountWriter(root: string): Promise<Mounted> {
  const ctx = new Context()
  await ctx.plugin(JsonlSessionPersistence, { root, compression: 'zstd', watchPollIntervalMs: POLL_MS })
  return { ctx, dispose: async () => { await ctx.fiber.dispose() } }
}

function userMessage(seq: number): SessionEvent {
  return {
    type: 'user/message',
    seq: SessionSeq(seq),
    time: 1000 + seq,
    data: freezeMessage({
      id: MessageId('stream-user'),
      role: 'user',
      content: [{ type: 'text', text: 'run the job' }],
      source: { kind: 'user' },
    }),
    surfaceOp: 'append',
  }
}

/** Persist one whole log through the writer's own handle. */
async function seed(mounted: Mounted, id: string, cwd: string, events: readonly SessionEvent[]): Promise<void> {
  const handle = await mounted.ctx.sessionPersistence.create(header(id, cwd))
  try {
    await handle.append(events)
  } finally {
    await handle.close()
  }
}

async function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => { reject(new Error(`nothing arrived within ${String(ms)}ms`)) }, ms)
      }),
    ])
  } finally {
    if (timer !== undefined) clearTimeout(timer)
  }
}

function delay(ms: number): Promise<void> {
  return new Promise<void>((resolve) => { setTimeout(resolve, ms) })
}

afterEach(async () => {
  for (const dir of dirs.splice(0)) await rm(dir, { recursive: true, force: true })
})

describe('cold session live-frame follow', () => {
  it('delivers side-channel frames to a follower of a session this Host does not run', async () => {
    const root = await freshRoot()
    const writer = await mountWriter(root)
    await seed(writer, 'streamed', root, [userMessage(0)])
    const host = await mountHost(root)
    const abort = new AbortController()
    try {
      const iterator = host.remote.follow({
        address: { kind: 'session', sessionId: SessionId('streamed') },
        assistantStream: true,
      }, abort.signal)[Symbol.asyncIterator]()
      const opening = await withTimeout(iterator.next(), 8_000)
      expect((opening.value as SessionFollowFrame).type).toBe('snapshot')

      const sink = writer.ctx.sessionStreams.publish(SessionId('streamed'))
      sink.append(1, { type: 'start', attemptId: 'a', revision: 1, turn: 1, step: 1 })
      sink.append(2, {
        type: 'chunk',
        attemptId: 'a',
        revision: 1,
        index: 0,
        time: 1,
        chunk: { type: 'text-delta', index: 0, text: 'hi' },
      })
      sink.append(3, { type: 'end', attemptId: 'a', revision: 1, index: 1, outcome: { kind: 'abandoned' } })

      const streamed: Extract<SessionFollowFrame, { type: 'assistant-stream' }>[] = []
      while (streamed.length < 3) {
        const next = await withTimeout(iterator.next(), 8_000)
        if (next.done === true) break
        if (next.value.type === 'assistant-stream') streamed.push(next.value)
      }
      expect(streamed.map(entry => entry.frame.type)).toEqual(['start', 'chunk', 'end'])
      expect(streamed[0]?.frame).toMatchObject({ type: 'start', startedAfterSeq: 0 })
    } finally {
      abort.abort()
      await host.dispose()
      await writer.dispose()
    }
  })

  it('leaves a session this Host holds to the in-process bus', async () => {
    const root = await freshRoot()
    const writer = await mountWriter(root)
    await seed(writer, 'owned', root, [userMessage(0)])
    const host = await mountHost(root)
    const abort = new AbortController()
    try {
      // The Host holds this Session, so its live frames come from the bus and
      // the side channel is never opened, even though the other writer fills it.
      host.ctx.sessions.create(SessionId('owned'), { meta: { cwd: root } })
      const iterator = host.remote.follow({
        address: { kind: 'session', sessionId: SessionId('owned') },
        assistantStream: true,
      }, abort.signal)[Symbol.asyncIterator]()
      expect((await withTimeout(iterator.next(), 8_000)).value).toMatchObject({ type: 'snapshot' })

      const sink = writer.ctx.sessionStreams.publish(SessionId('owned'))
      sink.append(1, { type: 'start', attemptId: 'a', revision: 1, turn: 1, step: 1 })
      await sink.close()

      const quiet = Symbol('quiet')
      expect(await Promise.race([
        iterator.next(),
        delay(400).then(() => quiet),
      ])).toBe(quiet)
    } finally {
      abort.abort()
      await host.dispose()
      await writer.dispose()
    }
  })
})
