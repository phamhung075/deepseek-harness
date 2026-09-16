import { appendFile, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { AppendTailReader } from '../src/append-tail.ts'
import { eventLine, toHeaderLine } from '../src/format.ts'
import { meta, oneTurnLog } from '../../session-persistence/tests/contract.ts'

const line = (value: string): string => `${value}\n`

describe('AppendTailReader', () => {
  let dir: string
  let path: string
  const events = oneTurnLog()

  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), 'dsh-append-tail-'))
    path = join(dir, 'session.jsonl')
    await writeFile(path, line(JSON.stringify(toHeaderLine(meta('append-tail')))))
  })

  afterEach(async () => { await rm(dir, { recursive: true, force: true }) })

  it('reports only rows appended after it anchored', async () => {
    await appendFile(path, line(eventLine(events[0] as never)))
    const reader = new AppendTailReader(path, 'none')
    expect(await reader.start()).toBe(true)
    // The reader anchored at the durable end, so the row already stored is not its business.
    expect(await reader.poll()).toEqual([])
    await appendFile(path, line(eventLine(events[1] as never)))
    const reported = await reader.poll()
    expect(reported.map(event => event.seq)).toEqual([1])
  })

  it('retains an incomplete trailing row until the bytes completing it land', async () => {
    const reader = new AppendTailReader(path, 'none')
    await reader.start()
    const row = line(eventLine(events[0] as never))
    await appendFile(path, row.slice(0, row.length - 4))
    expect(await reader.poll()).toEqual([])
    await appendFile(path, row.slice(row.length - 4))
    const reported = await reader.poll()
    expect(reported.map(event => event.type)).toEqual(['turn/start'])
  })

  it('absorbs a row that re-emits a seq it already reported', async () => {
    const reader = new AppendTailReader(path, 'none')
    await reader.start()
    await appendFile(path, line(eventLine(events[0] as never)))
    expect((await reader.poll()).map(event => event.seq)).toEqual([0])
    await appendFile(path, line(eventLine(events[0] as never)))
    expect(await reader.poll()).toEqual([])
  })

  it('ends the stream on a row that skips a seq it never reported', async () => {
    const reader = new AppendTailReader(path, 'none')
    await reader.start()
    await appendFile(path, line(eventLine(events[0] as never)))
    await reader.poll()
    await appendFile(path, line(eventLine(events[3] as never)))
    await expect(reader.poll()).rejects.toThrow(/skipped seq 1/)
  })

  it('reports an absent artifact instead of throwing', async () => {
    const reader = new AppendTailReader(join(dir, 'missing.jsonl'), 'none')
    expect(await reader.start()).toBe(false)
  })

  it('refuses a header-less artifact', async () => {
    await writeFile(path, 'not a session log')
    const reader = new AppendTailReader(path, 'none')
    await expect(reader.start()).rejects.toThrow(/header-less/)
  })
})
