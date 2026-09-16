/**
 * Durable-append observation of the JSONL backend: another writer's appends
 * become logical events, a record that is not yet complete is withheld, and
 * every observation is released on close and on backend teardown.
 */

import { afterEach, describe, expect, it } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import { appendFile, mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { SESSION_FORMAT_VERSION, SessionLogOffset, SessionSeq, SessionId } from '@deepseek-ai/dsh-session'
import type { SessionEvent, SessionHeader } from '@deepseek-ai/dsh-session'
import type { SessionAppendSubscription } from '@deepseek-ai/dsh-session-persistence'
import JsonlSessionPersistence from '@deepseek-ai/dsh-session-persistence-jsonl'
import { eventLines, logPath } from '../src/format.ts'
import { compressZstdFrame } from '../src/zstd.ts'

const CWD = '/proj'
const POLL_MS = 20
const dirs: string[] = []

function header(id: string): SessionHeader {
  return { version: SESSION_FORMAT_VERSION, id: SessionId(id), createdAt: 1000, isSeeded: false, cwd: CWD }
}

function turnStart(seq: number): SessionEvent {
  return { type: 'turn/start', seq: SessionSeq(seq), time: 1000 + seq, data: { turn: 1 } }
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

interface Mounted {
  readonly ctx: Context
  readonly dispose: () => Promise<void>
}

/** Mount one backend instance over a shared root; each instance is a separate process's view. */
async function mount(root: string, compression: 'zstd' | 'none'): Promise<Mounted> {
  const ctx = new Context()
  const fiber = await ctx.plugin(JsonlSessionPersistence, { root, compression, watchPollIntervalMs: POLL_MS })
  return { ctx, dispose: async () => { await fiber.dispose() } }
}

/** Persist one whole log through the writer's own handle. */
async function seed(mounted: Mounted, id: string, events: readonly SessionEvent[]): Promise<void> {
  const handle = await mounted.ctx.sessionPersistence.create(header(id))
  try {
    await handle.append(events)
  } finally {
    await handle.close()
  }
}

/** Append one batch from the other process's view. */
async function append(mounted: Mounted, id: string, events: readonly SessionEvent[]): Promise<void> {
  const handle = await mounted.ctx.sessionPersistence.open(SessionId(id), 'write')
  try {
    await handle.append(events)
  } finally {
    await handle.close()
  }
}

/** Read the subscription's batches without letting a missing one hang the spec. */
function reader(subscription: SessionAppendSubscription): {
  next: (timeoutMs?: number) => Promise<readonly SessionEvent[]>
  quiet: (windowMs?: number) => Promise<void>
} {
  const iterator = subscription.events[Symbol.asyncIterator]()
  let pending: Promise<IteratorResult<readonly SessionEvent[]>> | undefined
  const start = (): Promise<IteratorResult<readonly SessionEvent[]>> => {
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
        start().then(() => 'event' as const),
        delay(windowMs).then(() => 'quiet' as const),
      ])
      expect(outcome).toBe('quiet')
    },
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
        timer = setTimeout(() => { reject(new Error(`no append was observed within ${String(ms)}ms`)) }, ms)
      }),
    ])
  } finally {
    if (timer !== undefined) clearTimeout(timer)
  }
}

async function freshRoot(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), 'dsh-jsonl-appends-'))
  dirs.push(dir)
  return dir
}

afterEach(async () => {
  for (const dir of dirs.splice(0)) await rm(dir, { recursive: true, force: true })
})

describe('JsonlSessionAppends', () => {
  it('reports another writer\'s appends as logical events, one batch per durable append', async () => {
    const root = await freshRoot()
    const observer = await mount(root, 'zstd')
    const writer = await mount(root, 'zstd')
    await seed(observer, 'watched', [turnStart(0)])
    const subscription = await observer.ctx.sessionAppends.watch(SessionId('watched'))
    const batches = reader(subscription)

    // One append carrying several events must arrive as those logical events,
    // not as the storage rows that carried them.
    await append(writer, 'watched', [stepStart(1), stepEnd(2)])
    const first = await batches.next()
    expect(first.map(event => event.type)).toEqual(['step/start', 'step/end'])
    expect(first.map(event => event.seq)).toEqual([1, 2])

    await append(writer, 'watched', [turnEnd(3)])
    expect((await batches.next()).map(event => event.type)).toEqual(['turn/end'])

    subscription.close()
    await observer.dispose()
    await writer.dispose()
  })

  it('withholds a plaintext record until the bytes completing it land', async () => {
    const root = await freshRoot()
    const observer = await mount(root, 'none')
    const writer = await mount(root, 'none')
    await seed(observer, 'torn-line', [turnStart(0)])
    const subscription = await observer.ctx.sessionAppends.watch(SessionId('torn-line'))
    const batches = reader(subscription)

    const line = Buffer.from(eventLines([turnEnd(1)]) + '\n')
    const path = logPath(root, CWD, SessionId('torn-line'), 'none')
    await appendFile(path, line.subarray(0, 12))
    await batches.quiet()

    await appendFile(path, line.subarray(12))
    expect((await batches.next()).map(event => event.type)).toEqual(['turn/end'])
    expect(subscription.id).toBe('torn-line')

    subscription.close()
    await observer.dispose()
    await writer.dispose()
  })

  it('withholds a Zstandard frame until its final bytes land', async () => {
    const root = await freshRoot()
    const observer = await mount(root, 'zstd')
    const writer = await mount(root, 'zstd')
    await seed(observer, 'torn-frame', [turnStart(0)])
    const subscription = await observer.ctx.sessionAppends.watch(SessionId('torn-frame'))
    const batches = reader(subscription)

    const frame = await compressZstdFrame(eventLines([turnEnd(1)]) + '\n')
    const path = logPath(root, CWD, SessionId('torn-frame'), 'zstd')
    await appendFile(path, frame.subarray(0, frame.length - 4))
    await batches.quiet()

    await appendFile(path, frame.subarray(frame.length - 4))
    expect((await batches.next()).map(event => event.type)).toEqual(['turn/end'])

    subscription.close()
    await observer.dispose()
    await writer.dispose()
  })

  it('stops reporting once the subscription closes', async () => {
    const root = await freshRoot()
    const observer = await mount(root, 'zstd')
    const writer = await mount(root, 'zstd')
    await seed(observer, 'closed', [turnStart(0)])
    const subscription = await observer.ctx.sessionAppends.watch(SessionId('closed'))
    const batches = reader(subscription)

    await append(writer, 'closed', [stepStart(1)])
    expect((await batches.next()).map(event => event.seq)).toEqual([1])

    subscription.close()
    await append(writer, 'closed', [stepEnd(2)])
    await expect(batches.next()).rejects.toThrow('the subscription stream ended')

    await observer.dispose()
    await writer.dispose()
  })

  it('releases every subscription when the backend fiber unloads', async () => {
    const root = await freshRoot()
    const observer = await mount(root, 'zstd')
    await seed(observer, 'unmounted', [turnStart(0)])
    const subscription = await observer.ctx.sessionAppends.watch(SessionId('unmounted'))
    const batches = reader(subscription)

    await observer.dispose()

    expect(observer.ctx.get('sessionAppends')).toBeUndefined()
    await expect(batches.next()).rejects.toThrow('the subscription stream ended')
    expect(subscription.id).toBe('unmounted')
  })

  it('refuses a session with no stored artifact', async () => {
    const root = await freshRoot()
    const observer = await mount(root, 'zstd')
    await expect(observer.ctx.sessionAppends.watch(SessionId('absent')))
      .rejects.toThrow(/absent/)
    await observer.dispose()
  })

  it('follows a stored prefix that is not empty', async () => {
    const root = await freshRoot()
    const observer = await mount(root, 'zstd')
    const writer = await mount(root, 'zstd')
    await seed(observer, 'mid-log', [turnStart(0), stepStart(1)])
    const subscription = await observer.ctx.sessionAppends.watch(SessionId('mid-log'))
    const batches = reader(subscription)

    // The subscription anchors at the durable end: only later events arrive,
    // and their seqs continue the stored prefix.
    await append(writer, 'mid-log', [turnEnd(2)])
    const batch = await batches.next()
    expect(batch.map(event => event.seq)).toEqual([2])
    expect(SessionLogOffset(batch.length)).toBe(1)

    subscription.close()
    await observer.dispose()
    await writer.dispose()
  })

  it('absorbs an append that re-emits seqs it already reported', async () => {
    const root = await freshRoot()
    const observer = await mount(root, 'zstd')
    const writer = await mount(root, 'zstd')
    await seed(observer, 'rewound', [turnStart(0)])
    const subscription = await observer.ctx.sessionAppends.watch(SessionId('rewound'))
    const batches = reader(subscription)
    const path = logPath(root, CWD, SessionId('rewound'), 'zstd')

    await append(writer, 'rewound', [stepStart(1)])
    expect((await batches.next()).map(event => event.seq)).toEqual([1])

    // A writer that resumed this session from a shorter prefix re-emits seqs the
    // artifact already holds, so the append arrives behind the stream's
    // position: the reader keeps that position and reports what follows.
    await appendFile(path, await compressZstdFrame(eventLines([stepStart(1), stepEnd(2)]) + '\n'))
    expect((await batches.next()).map(event => event.seq)).toEqual([2])

    // The absorbed rewrite must not move the stream's position: the next append is still contiguous.
    await appendFile(path, await compressZstdFrame(eventLines([turnEnd(3)]) + '\n'))
    expect((await batches.next()).map(event => event.type)).toEqual(['turn/end'])

    subscription.close()
    await observer.dispose()
    await writer.dispose()
  })

  it('ends the subscription on an append that skips seqs it never reported', async () => {
    const root = await freshRoot()
    const observer = await mount(root, 'zstd')
    const writer = await mount(root, 'zstd')
    await seed(observer, 'gapped', [turnStart(0)])
    const subscription = await observer.ctx.sessionAppends.watch(SessionId('gapped'))
    const batches = reader(subscription)
    const path = logPath(root, CWD, SessionId('gapped'), 'zstd')

    await append(writer, 'gapped', [stepStart(1)])
    expect((await batches.next()).map(event => event.seq)).toEqual([1])

    // Bytes this stream never saw cannot be absorbed: the stream reports the hole.
    await appendFile(path, await compressZstdFrame(eventLines([stepStart(5)]) + '\n'))
    await expect(batches.next()).rejects.toThrow(/skipped seq 2/)

    await observer.dispose()
    await writer.dispose()
  })
})
