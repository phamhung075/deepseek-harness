/**
 * Cold-session durable activity and follow: a session another process is
 * running reports as running while its writes land, stops when its turn closes,
 * and streams its appends to a follower instead of freezing at the snapshot,
 * including when that row is opened a second time.
 *
 * The "other process" is a second persistence instance over the same root: a
 * backend's write ownership is in-process, so two instances append to one
 * artifact exactly as two processes do.
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
import type { SessionEventEntry, SessionFollowFrame, SessionSummary } from '../src/types.ts'
import { createSessionTestRemote, type TestSessionRemote } from './test-remote.ts'
import { installSessionReadTestServices } from './test-remote.ts'
import { SessionHistoryController } from '../src/history.ts'

const POLL_MS = 20
const IDLE_MS = 30_000
const dirs: string[] = []

/** One mounted process's view of the shared store. */
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
  const dir = await mkdtemp(join(tmpdir(), 'dsh-cold-activity-'))
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

/** Persist one whole log through the writer's own handle. */
async function seed(mounted: Mounted, id: string, cwd: string, events: readonly SessionEvent[]): Promise<void> {
  const handle = await mounted.ctx.sessionPersistence.create(header(id, cwd))
  try {
    await handle.append(events)
  } finally {
    await handle.close()
  }
}

/** Append one contiguous batch from the writer's view. */
async function append(mounted: Mounted, id: string, events: readonly SessionEvent[]): Promise<void> {
  const handle = await mounted.ctx.sessionPersistence.open(SessionId(id), 'write')
  try {
    await handle.append(events)
  } finally {
    await handle.close()
  }
}

function turnStart(seq: number, turn = 1): SessionEvent {
  return { type: 'turn/start', seq: SessionSeq(seq), time: 1000 + seq, data: { turn } }
}

function stepStart(seq: number): SessionEvent {
  return { type: 'step/start', seq: SessionSeq(seq), time: 1000 + seq, data: { turn: 1, step: 1 } }
}

function stepEnd(seq: number): SessionEvent {
  return { type: 'step/end', seq: SessionSeq(seq), time: 1000 + seq, data: { turn: 1, step: 1 } }
}

function turnEnd(seq: number): SessionEvent {
  return {
    type: 'turn/end',
    seq: SessionSeq(seq),
    time: 1000 + seq,
    data: { turn: 1, reason: { kind: 'completed' } },
  }
}

function userMessage(seq: number): SessionEvent {
  return {
    type: 'user/message',
    seq: SessionSeq(seq),
    time: 1000 + seq,
    data: freezeMessage({
      id: MessageId('durable-user'),
      role: 'user',
      content: [{ type: 'text', text: 'run the job' }],
      source: { kind: 'user' },
    }),
    surfaceOp: 'append',
  }
}

/** The running flag one delivered Session list row carries. */
async function runningOf(remote: TestSessionRemote, id: string): Promise<boolean | undefined> {
  const response = await remote.list({})
  if (!response.ok) throw new Error(`session list failed: ${response.error.message}`)
  const row: SessionSummary | undefined = response.value.items.find(item => item.sessionId === id)
  return row?.running
}

/** Retry one assertion until it holds, so the spec does not race the observer's cadence. */
async function until(check: () => Promise<void>, timeoutMs = 8_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  for (;;) {
    try {
      await check()
      return
    } catch (error: unknown) {
      if (Date.now() >= deadline) throw error
      await delay(20)
    }
  }
}

function delay(ms: number): Promise<void> {
  return new Promise<void>((resolve) => { setTimeout(resolve, ms) })
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

afterEach(async () => {
  for (const dir of dirs.splice(0)) await rm(dir, { recursive: true, force: true })
})

describe('cold session durable activity', () => {
  it('reports a session another process is running, then stops when its turn closes', async () => {
    const root = await freshRoot()
    const writer = await mountWriter(root)
    // The other process is mid-turn: turn and step are open, nothing closed them.
    await seed(writer, 'live-job', root, [turnStart(0), userMessage(1), stepStart(2)])

    const host = await mountHost(root)
    const statuses: Array<{ readonly id: string; readonly running: boolean }> = []
    host.ctx.on('api-session/status', (id, running) => { statuses.push({ id: String(id), running }) })
    try {
      await until(async () => {
        expect(await runningOf(host.remote, 'live-job')).toBe(true)
      })

      await append(writer, 'live-job', [stepEnd(3), turnEnd(4)])

      await until(async () => {
        expect(await runningOf(host.remote, 'live-job')).toBe(false)
      })
      expect(statuses.filter(entry => entry.id === 'live-job'))
        .toEqual([{ id: 'live-job', running: true }, { id: 'live-job', running: false }])
    } finally {
      await host.dispose()
      await writer.dispose()
    }
  })

  it('leaves a session this Host holds to its in-process status', { timeout: 20_000 }, async () => {
    const root = await freshRoot()
    const writer = await mountWriter(root)
    await seed(writer, 'hosted-job', root, [turnStart(0), userMessage(1), stepStart(2)])
    const host = await mountHost(root)
    try {
      expect(host.ctx.get('sessionAppends')).toBeDefined()
      // While this Host holds no Session, durable activity alone marks the row.
      await until(async () => {
        expect(await runningOf(host.remote, 'hosted-job')).toBe(true)
      })
      // Once it holds the Session, the in-process path is authoritative: no
      // Agent is running, so the recent durable writes no longer mark it.
      host.ctx.sessions.create(SessionId('hosted-job'), { meta: header('hosted-job', root), seed: [] })
      await until(async () => {
        expect(await runningOf(host.remote, 'hosted-job')).toBe(false)
      })
    } finally {
      await host.dispose()
      await writer.dispose()
    }
  })

  it('streams the appends of a session this Host does not run', async () => {
    const root = await freshRoot()
    const writer = await mountWriter(root)
    // A closed turn, so the snapshot cursor is exactly the stored log end.
    await seed(writer, 'streamed', root, [turnStart(0), userMessage(1), stepStart(2), stepEnd(3), turnEnd(4)])

    const host = await mountHost(root)
    const controller = new AbortController()
    const frames: AsyncIterator<SessionFollowFrame> = host.remote
      .follow({ address: { kind: 'session', sessionId: SessionId('streamed') } }, controller.signal)
      [Symbol.asyncIterator]()
    try {
      const opening = await withTimeout(frames.next(), 5_000)
      expect(opening.done).toBe(false)
      const snapshot = opening.value as Extract<SessionFollowFrame, { type: 'snapshot' }>
      expect(snapshot.type).toBe('snapshot')
      expect(snapshot.cursor).toBe(4)

      // The other process starts the next turn while the follower is open.
      await append(writer, 'streamed', [turnStart(5, 2), stepStart(6), stepEnd(7)])

      const seen: number[] = []
      while (seen.length < 3) {
        const next = await withTimeout(frames.next(), 5_000)
        expect(next.done).toBe(false)
        seen.push((next.value as SessionEventEntry).event.seq)
      }
      expect(seen).toEqual([5, 6, 7])

      controller.abort()
      await withTimeout(frames.next(), 5_000)
    } finally {
      controller.abort()
      await host.dispose()
      await writer.dispose()
    }
  })

  it('keeps streaming for a session this Host does not run after it is opened', { timeout: 20_000 }, async () => {
    const root = await freshRoot()
    const writer = await mountWriter(root)
    // A closed turn, so the opening snapshot's cursor is exactly the stored log end.
    await seed(writer, 'watched', root, [
      turnStart(0), userMessage(1), stepStart(2), stepEnd(3), turnEnd(4),
    ])

    const host = await mountHost(root)
    /** Open one follow stream over the cold session, as opening the row does. */
    const openFollow = (signal: AbortSignal) => host.remote
      .follow({ address: { kind: 'session', sessionId: SessionId('watched') } }, signal)
      [Symbol.asyncIterator]()
    const first = new AbortController()
    const firstFrames = openFollow(first.signal)
    try {
      expect((await withTimeout(firstFrames.next(), 5_000)).value)
        .toMatchObject({ type: 'snapshot' })
      // Opening it must not attach the Session to this Host, which is what would
      // hand authority to an in-process Agent that is not running the job.
      expect(host.ctx.sessions.get(SessionId('watched'))).toBeUndefined()

      first.abort()
      await withTimeout(firstFrames.next(), 5_000)

      // A second open still streams a turn the other process starts afterwards.
      const second = new AbortController()
      const secondFrames = openFollow(second.signal)
      try {
        expect((await withTimeout(secondFrames.next(), 5_000)).value)
          .toMatchObject({ type: 'snapshot' })
        await append(writer, 'watched', [turnStart(5, 2), stepStart(6), stepEnd(7)])
        const seen: number[] = []
        while (seen.length < 3) {
          const next = await withTimeout(secondFrames.next(), 5_000)
          expect(next.done).toBe(false)
          seen.push((next.value as SessionEventEntry).event.seq)
        }
        expect(seen).toEqual([5, 6, 7])
        // The durable observation survived the open too, so the row reports the run.
        await until(async () => {
          expect(await runningOf(host.remote, 'watched')).toBe(true)
        })
      } finally {
        second.abort()
        await withTimeout(secondFrames.next(), 5_000)
      }
    } finally {
      first.abort()
      await host.dispose()
      await writer.dispose()
    }
  })

  it('does not promote a cold session it follows durably', async () => {
    const root = await freshRoot()
    const writer = await mountWriter(root)
    await seed(writer, 'unpromoted', root, [turnStart(0), userMessage(1), stepEnd(2), turnEnd(3)])

    const ctx = new Context()
    await ctx.plugin(SessionStore)
    await ctx.plugin(AgentRegistry)
    await ctx.plugin(JsonlSessionPersistence, { root, compression: 'zstd', watchPollIntervalMs: POLL_MS })
    installSessionReadTestServices(ctx)
    const promoted: string[] = []
    // The controller is composed directly so the promotion decision is observable:
    // activating the Agent in this composition fails, so an attached Session is not.
    const controller = new SessionHistoryController(ctx, (observation) => {
      promoted.push(String(observation.header.id))
      observation[Symbol.dispose]()
    })
    const abort = new AbortController()
    const frames = controller
      .follow({ address: { kind: 'session', sessionId: SessionId('unpromoted') } }, abort.signal)
      [Symbol.asyncIterator]()
    try {
      expect((await withTimeout(frames.next(), 5_000)).value).toMatchObject({ type: 'snapshot' })
      // Promotion happens after the snapshot frame, so resume the follower and give a
      // promotion that must not happen a chance to be observed.
      const resumed = frames.next()
      const deadline = Date.now() + 500
      while (promoted.length === 0 && Date.now() < deadline) await delay(20)
      expect(promoted).toEqual([])
      abort.abort()
      await withTimeout(resumed, 5_000).catch(() => undefined)
    } finally {
      abort.abort()
      await withTimeout(frames.next(), 5_000).catch(() => undefined)
      await ctx.fiber.dispose()
      await writer.dispose()
    }
  })

  it('leaves follow to the in-process bus for a session this Host holds', async () => {
    const root = await freshRoot()
    const writer = await mountWriter(root)
    await seed(writer, 'owned', root, [turnStart(0), userMessage(1)])
    const host = await mountHost(root)
    try {
      host.ctx.sessions.create(SessionId('owned'), { meta: header('owned', root), seed: [] })
      const controller = new AbortController()
      const frames: AsyncIterator<SessionFollowFrame> = host.remote
        .follow({ address: { kind: 'session', sessionId: SessionId('owned') } }, controller.signal)
        [Symbol.asyncIterator]()
      const opening = await withTimeout(frames.next(), 5_000)
      expect((opening.value as Extract<SessionFollowFrame, { type: 'snapshot' }>).type).toBe('snapshot')

      // The durable observer steps aside: another process's appends do not
      // reach a follower of a Session this Host owns.
      await append(writer, 'owned', [stepStart(2), stepEnd(3), turnEnd(4)])
      await expect(withTimeout(frames.next(), 400)).rejects.toThrow('nothing arrived')

      controller.abort()
      await withTimeout(frames.next(), 5_000)
    } finally {
      await host.dispose()
      await writer.dispose()
    }
  })
})
