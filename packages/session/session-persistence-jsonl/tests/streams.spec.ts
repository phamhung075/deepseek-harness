/**
 * The live-frame side channel of the JSONL backend: frames published beside a
 * Session artifact are read back by a tail opened before them, an incomplete
 * trailing record is withheld until its final bytes land, a replaced channel
 * restarts the reader, and every sink is released on backend teardown.
 */

import { afterEach, describe, expect, it } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import { appendFile, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { SESSION_FORMAT_VERSION, SessionSeq, SessionId } from '@deepseek-ai/dsh-session'
import type { SessionEvent, SessionHeader } from '@deepseek-ai/dsh-session'
import { SessionPersistenceNotFoundError } from '@deepseek-ai/dsh-session-persistence'
import type { SessionStreamRecord, SessionStreamSubscription } from '@deepseek-ai/dsh-session-persistence'
import JsonlSessionPersistence from '@deepseek-ai/dsh-session-persistence-jsonl'
import { logPath } from '../src/format.ts'
import { streamChannelPath } from '../src/stream-channel.ts'

const CWD = '/proj'
const POLL_MS = 20
const dirs: string[] = []

function header(id: string): SessionHeader {
  return { version: SESSION_FORMAT_VERSION, id: SessionId(id), createdAt: 1000, isSeeded: false, cwd: CWD }
}

function turnStart(seq: number): SessionEvent {
  return { type: 'turn/start', seq: SessionSeq(seq), time: 1000 + seq, data: { turn: 1 } }
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
        timer = setTimeout(() => { reject(new Error(`no frame was observed within ${String(ms)}ms`)) }, ms)
      }),
    ])
  } finally {
    if (timer !== undefined) clearTimeout(timer)
  }
}

interface Mounted {
  readonly ctx: Context
  readonly dispose: () => Promise<void>
}

/** Mount one backend instance over a shared root. */
async function mount(root: string): Promise<Mounted> {
  const ctx = new Context()
  const fiber = await ctx.plugin(JsonlSessionPersistence, { root, compression: 'none', watchPollIntervalMs: POLL_MS })
  return { ctx, dispose: async () => { await fiber.dispose() } }
}

/** Materialize one Session artifact so the side channel has a directory. */
async function seed(mounted: Mounted, id: string): Promise<void> {
  const handle = await mounted.ctx.sessionPersistence.create(header(id))
  try {
    await handle.append([turnStart(0)])
  } finally {
    await handle.close()
  }
}

/** The side-channel path beside one Session's plaintext artifact. */
function channelPath(root: string, id: string): string {
  return streamChannelPath(logPath(root, CWD, SessionId(id), 'none'))
}

/** Read a subscription's batches without letting a missing one hang the spec. */
function reader(subscription: SessionStreamSubscription): {
  next: (timeoutMs?: number) => Promise<readonly SessionStreamRecord[]>
  quiet: (windowMs?: number) => Promise<void>
  ended: (timeoutMs?: number) => Promise<void>
} {
  const iterator = subscription.frames[Symbol.asyncIterator]()
  let pending: Promise<IteratorResult<readonly SessionStreamRecord[]>> | undefined
  const start = (): Promise<IteratorResult<readonly SessionStreamRecord[]>> => {
    pending ??= iterator.next()
    return pending
  }
  return {
    next: async (timeoutMs = 5_000) => {
      const result = await withTimeout(start(), timeoutMs)
      pending = undefined
      if (result.done === true) throw new Error('the subscription stream ended')
      return result.value
    },
    quiet: async (windowMs = 250) => {
      const outcome = await Promise.race([
        start().then(() => 'frame' as const),
        delay(windowMs).then(() => 'quiet' as const),
      ])
      expect(outcome).toBe('quiet')
    },
    ended: async (timeoutMs = 5_000) => {
      const result = await withTimeout(start(), timeoutMs)
      expect(result.done).toBe(true)
    },
  }
}

async function freshRoot(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), 'dsh-jsonl-streams-'))
  dirs.push(dir)
  return dir
}

afterEach(async () => {
  for (const dir of dirs.splice(0)) await rm(dir, { recursive: true, force: true })
})

describe('streamChannelPath', () => {
  it('names the side channel so no generation parser accepts it', () => {
    expect(streamChannelPath('/root/proj/id/session.v3.jsonl.zstd'))
      .toBe('/root/proj/id/session.v3.stream.jsonl')
    expect(streamChannelPath('/root/proj/id/session.jsonl'))
      .toBe('/root/proj/id/session.stream.jsonl')
  })

  it('refuses a path that is not a JSONL artifact', () => {
    expect(() => streamChannelPath('/root/proj/id/notes.txt')).toThrow('does not carry a JSONL suffix')
  })
})

describe('JsonlSessionStreams', () => {
  it('delivers frames published after the tail opened, in order', async () => {
    const root = await freshRoot()
    const mounted = await mount(root)
    try {
      await seed(mounted, 'watched')
      const id = SessionId('watched')
      // Anchor a frame the tail must not deliver, so the spec proves it anchors
      // at the channel's current end rather than replaying its prefix.
      const prefix = mounted.ctx.sessionStreams.publish(id)
      prefix.append(0, { type: 'start' })
      await prefix.close()

      const subscription = await mounted.ctx.sessionStreams.watch(id)
      const frames = reader(subscription)
      const sink = mounted.ctx.sessionStreams.publish(id)
      sink.append(1, { type: 'chunk', index: 0 })
      sink.append(2, { type: 'chunk', index: 1 })

      expect(await frames.next()).toEqual([
        { seq: 1, frame: { type: 'chunk', index: 0 } },
        { seq: 2, frame: { type: 'chunk', index: 1 } },
      ])
    } finally {
      await mounted.dispose()
    }
  })

  it('withholds an incomplete trailing record until its final bytes land', async () => {
    const root = await freshRoot()
    const mounted = await mount(root)
    try {
      await seed(mounted, 'partial')
      const path = channelPath(root, 'partial')
      const frames = reader(await mounted.ctx.sessionStreams.watch(SessionId('partial')))

      await appendFile(path, '{"seq":1,"frame":{"type":"chunk"}}')
      await frames.quiet()
      await appendFile(path, '\n')
      expect(await frames.next()).toEqual([{ seq: 1, frame: { type: 'chunk' } }])
    } finally {
      await mounted.dispose()
    }
  })

  it('restarts the reader when the channel is replaced by a shorter one', async () => {
    const root = await freshRoot()
    const mounted = await mount(root)
    try {
      await seed(mounted, 'replaced')
      const path = channelPath(root, 'replaced')
      const frames = reader(await mounted.ctx.sessionStreams.watch(SessionId('replaced')))
      const sink = mounted.ctx.sessionStreams.publish(SessionId('replaced'))
      sink.append(1, { type: 'chunk', index: 0 })
      expect((await frames.next())[0]?.seq).toBe(1)

      await writeFile(path, '{"seq":9,"frame":{"type":"end"}}\n')
      expect(await frames.next()).toEqual([{ seq: 9, frame: { type: 'end' } }])
    } finally {
      await mounted.dispose()
    }
  })

  it('repairs an incomplete trailing record before the next writer appends', async () => {
    const root = await freshRoot()
    const mounted = await mount(root)
    try {
      await seed(mounted, 'repaired')
      const path = channelPath(root, 'repaired')
      await appendFile(path, '{"seq":0,"frame":{"type":"start"')
      const sink = mounted.ctx.sessionStreams.publish(SessionId('repaired'))
      sink.append(1, { type: 'end' })
      await sink.close()

      const lines = (await readFile(path, 'utf8')).split('\n').filter(line => line.length > 0)
      expect(lines.map(line => (JSON.parse(line) as { seq: number }).seq)).toEqual([1])
    } finally {
      await mounted.dispose()
    }
  })

  it('refuses a session with no stored artifact', async () => {
    const root = await freshRoot()
    const mounted = await mount(root)
    try {
      await expect(mounted.ctx.sessionStreams.watch(SessionId('absent')))
        .rejects.toBeInstanceOf(SessionPersistenceNotFoundError)
    } finally {
      await mounted.dispose()
    }
  })

  it('flushes a publishing sink and ends subscriptions on backend teardown', async () => {
    const root = await freshRoot()
    const mounted = await mount(root)
    await seed(mounted, 'teardown')
    const id = SessionId('teardown')
    const path = channelPath(root, 'teardown')
    const frames = reader(await mounted.ctx.sessionStreams.watch(id))
    const sink = mounted.ctx.sessionStreams.publish(id)
    sink.append(1, { type: 'chunk', index: 0 })
    await frames.next()

    await mounted.dispose()
    await frames.ended()
    expect(await readFile(path, 'utf8')).toContain('"seq":1')
  })
})
