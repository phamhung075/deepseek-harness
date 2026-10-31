/**
 * The live-writer correction: a cold observation of a Session whose writer still
 * holds its kernel lock is presented as stored, and a free lock keeps the
 * crash-recovery transcript the Host produced.
 */

import { Context, Service } from '@deepseek-ai/cordis'
import { SESSION_FORMAT_VERSION, SessionId, SessionSeq } from '@deepseek-ai/dsh-session'
import type { SessionEvent, SessionHeader } from '@deepseek-ai/dsh-session'
import type { SessionObservation } from '@deepseek-ai/dsh-session-query'
import { spawn } from 'node:child_process'
import type { ChildProcess } from 'node:child_process'
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import * as LiveWriter from '../src/index.ts'

const roots: string[] = []
const children: ChildProcess[] = []

afterEach(async () => {
  for (const child of children.splice(0)) {
    if (child.exitCode === null) child.kill('SIGKILL')
  }
  for (const root of roots.splice(0)) await rm(root, { recursive: true, force: true })
})

function header(id: string, cwd = '/work'): SessionHeader {
  return { version: SESSION_FORMAT_VERSION, id: SessionId(id), createdAt: 1, isSeeded: false, cwd }
}

/** A log the writer left mid-turn: the turn and its step are still open. */
function openLog(): SessionEvent[] {
  return [
    { type: 'turn/start', seq: SessionSeq(0), time: 1000, data: { turn: 1 } },
    { type: 'step/start', seq: SessionSeq(1), time: 1001, data: { turn: 1, step: 1 } },
  ]
}

/** The exact tail `interruptedTurnClosers` appends, stamped with the last stored time. */
function closers(time: number, from: number): SessionEvent[] {
  return [
    {
      type: 'tool/result',
      seq: SessionSeq(from),
      time,
      data: {
        turn: 1,
        step: 1,
        message: {
          id: `interrupted-tool-result-${String(from)}`,
          role: 'tool',
          toolCallId: 'call',
          isError: true,
          content: [{ type: 'text', text: 'the call was interrupted' }],
        },
        error: { name: 'ToolOutcomeUnknownError', code: 'TOOL_OUTCOME_UNKNOWN' },
      } as never,
    },
    { type: 'step/end', seq: SessionSeq(from + 1), time, data: { turn: 1, step: 1 } },
    { type: 'turn/end', seq: SessionSeq(from + 2), time, data: { turn: 1, reason: { kind: 'interrupted' } } },
  ]
}

function balancedLog(): SessionEvent[] {
  const stored = openLog()
  return [...stored, ...closers(1001, 2)]
}

async function tempRoot(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'dsh-fork-live-writer-'))
  roots.push(root)
  return root
}

/** Create the Session directory a stored Session would have. */
async function seedSession(root: string, id: string): Promise<string> {
  const directory = join(root, '--work--', id)
  await mkdir(directory, { recursive: true })
  const lock = join(directory, 'session.lock')
  await writeFile(lock, '')
  return lock
}

/** Hold an exclusive lock through the system `flock` utility, as a writer would. */
async function holdLock(lock: string): Promise<void> {
  const child = spawn('flock', ['-x', lock, 'sleep', '30'], { stdio: 'ignore' })
  children.push(child)
  await new Promise(resolve => setTimeout(resolve, 150))
}

function observation(events: readonly SessionEvent[], meta: SessionHeader): SessionObservation {
  let disposed = false
  return {
    source: 'prepared',
    header: meta,
    inheritedEventCount: 0 as never,
    events,
    cursor: (events.at(-1)?.seq ?? -1) as never,
    retain: () => observation(events, meta),
    [Symbol.dispose]: () => { disposed = true; void disposed },
  }
}

/** Stand-in for the Host's `sessionQuery`: one canned observation. */
async function fakeQuery(ctx: Context, produced: SessionObservation): Promise<{ calls: number }> {
  const state = { calls: 0 }
  class FakeQuery extends Service {
    constructor(context: Context) { super(context, 'sessionQuery') }
    observeSession(): Promise<SessionObservation> {
      state.calls += 1
      return Promise.resolve(produced)
    }
  }
  await ctx.plugin(FakeQuery)
  return state
}

describe('storedPrefix', () => {
  it('strips a synthetic recovery tail from an open turn', () => {
    const log = balancedLog()
    expect(LiveWriter.storedPrefix(log)?.map(event => event.type)).toEqual(['turn/start', 'step/start'])
  })

  it('keeps a log whose last turn genuinely completed', () => {
    const log: SessionEvent[] = [
      { type: 'turn/start', seq: SessionSeq(0), time: 1, data: { turn: 1 } },
      { type: 'turn/end', seq: SessionSeq(1), time: 2, data: { turn: 1, reason: { kind: 'completed' } } },
    ]
    expect(LiveWriter.storedPrefix(log)).toBeUndefined()
  })

  it('keeps a failed turn end, which is not crash recovery', () => {
    const log: SessionEvent[] = [
      { type: 'turn/start', seq: SessionSeq(0), time: 1, data: { turn: 1 } },
      {
        type: 'turn/end',
        seq: SessionSeq(1),
        time: 2,
        data: { turn: 1, reason: { kind: 'error', error: { message: 'boom' } } },
      },
    ]
    expect(LiveWriter.storedPrefix(log)).toBeUndefined()
  })

  it('keeps a recovery-shaped tail whose prefix closed its turn', () => {
    const log: SessionEvent[] = [
      { type: 'turn/start', seq: SessionSeq(0), time: 1000, data: { turn: 1 } },
      { type: 'turn/end', seq: SessionSeq(1), time: 1001, data: { turn: 1, reason: { kind: 'completed' } } },
      ...closers(1001, 2),
    ]
    expect(LiveWriter.storedPrefix(log)).toBeUndefined()
  })
})

describe('writerHolds', () => {
  it.skipIf(process.platform !== 'linux')('observes a held kernel lock and its release', async () => {
    const root = await tempRoot()
    const lock = await seedSession(root, 'held-session')
    expect(await LiveWriter.writerHolds(root, header('held-session'))).toBe(false)
    await holdLock(lock)
    expect(await LiveWriter.writerHolds(root, header('held-session'))).toBe(true)
    for (const child of children.splice(0)) child.kill('SIGKILL')
    await new Promise(resolve => setTimeout(resolve, 150))
    expect(await LiveWriter.writerHolds(root, header('held-session'))).toBe(false)
  })

  it('reports a Session with no stored directory as free', async () => {
    const root = await tempRoot()
    await mkdir(root, { recursive: true })
    expect(await LiveWriter.writerHolds(root, header('absent'))).toBe(false)
  })
})

describe('observeSession wrapper', () => {
  it.skipIf(process.platform !== 'linux')('presents the stored prefix while the writer holds the Session', async () => {
    const root = await tempRoot()
    const meta = header('running')
    const lock = await seedSession(root, meta.id)
    await holdLock(lock)
    const ctx = new Context()
    const query = await fakeQuery(ctx, observation(balancedLog(), meta))
    await ctx.plugin(LiveWriter, { sessionsRoot: root })

    using observed = await ctx.sessionQuery.observeSession(meta.id)

    expect(query.calls).toBe(1)
    expect(observed.events.map(event => event.type)).toEqual(['turn/start', 'step/start'])
    expect(observed.cursor).toBe(1)
    await ctx.fiber.dispose()
  })

  it('leaves the crash-recovery transcript alone once the lock is free', async () => {
    const root = await tempRoot()
    const meta = header('finished')
    await seedSession(root, meta.id)
    const ctx = new Context()
    const balanced = balancedLog()
    await fakeQuery(ctx, observation(balanced, meta))
    await ctx.plugin(LiveWriter, { sessionsRoot: root })

    using observed = await ctx.sessionQuery.observeSession(meta.id)

    expect(observed.events).toBe(balanced)
    expect(observed.cursor).toBe(4)
    await ctx.fiber.dispose()
  })

  it.skipIf(process.platform !== 'linux')('restores the Host behaviour when the plugin unloads', async () => {
    const root = await tempRoot()
    const meta = header('restored')
    const lock = await seedSession(root, meta.id)
    await holdLock(lock)
    const ctx = new Context()
    const balanced = balancedLog()
    await fakeQuery(ctx, observation(balanced, meta))
    const fiber = await ctx.plugin(LiveWriter, { sessionsRoot: root })

    using corrected = await ctx.sessionQuery.observeSession(meta.id)
    expect(corrected.cursor).toBe(1)

    await fiber.dispose()
    await new Promise(resolve => setTimeout(resolve, 10))
    using plain = await ctx.sessionQuery.observeSession(meta.id)
    expect(plain.events).toBe(balanced)
    expect(plain.cursor).toBe(4)
    await ctx.fiber.dispose()
  })
})
