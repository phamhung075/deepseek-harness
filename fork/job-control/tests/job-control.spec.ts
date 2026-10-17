import { Context, Service } from '@deepseek-ai/cordis'
import type { Agent } from '@deepseek-ai/dsh-agent'
import { appendFile, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it, vi } from 'vitest'
import * as JobControl from '../src/index.ts'

let dirs: string[] = []
afterEach(async () => {
  for (const dir of dirs) await rm(dir, { recursive: true, force: true })
  dirs = []
})

async function setup() {
  const dir = await mkdtemp(join(tmpdir(), 'fork-job-control-'))
  dirs.push(dir)
  const artifact = join(dir, 's1.jsonl')
  const ctx = new Context()
  class FakePersistence extends Service {
    constructor(c: Context) { super(c, 'sessionPersistence') }
    async resolveCurrentLog(): Promise<string> { return artifact }
  }
  class FakeMeter extends Service {
    constructor(c: Context) { super(c, 'tokenMeter') }
    measure() { return { logRevision: 3, totalTokens: 42, surfaceTokens: 40, nodes: [{}, {}] } }
  }
  await ctx.plugin(FakePersistence)
  await ctx.plugin(FakeMeter)
  await ctx.plugin(JobControl, { pollIntervalMs: 20 })
  const steered: unknown[] = []
  const agent = { session: { id: 's1', seq: 0, header: {} }, steer: (m: unknown) => steered.push(m) } as unknown as Agent
  return { ctx, dir, artifact, agent, steered }
}

describe('siblingPath / parseControlLines', () => {
  it('derives sibling paths and rejects other suffixes', () => {
    expect(JobControl.siblingPath('/a/b.jsonl', '.control.jsonl')).toBe('/a/b.control.jsonl')
    expect(JobControl.siblingPath('/a/b.jsonl.zstd', '.usage.json')).toBe('/a/b.usage.json')
    expect(JobControl.siblingPath('/a/b.txt', '.x')).toBeUndefined()
  })
  it('skips malformed and empty records', () => {
    expect(JobControl.parseControlLines('{"text":"a"}\n\nnot json\n{"text":""}\n{"id":1}\n{"text":"b"}')).toEqual(['a', 'b'])
  })
})

describe('fork job-control', () => {
  it('steers on lines appended after the Agent is observed and ignores older ones', async () => {
    const { ctx, artifact, agent, steered } = await setup()
    const control = join(artifact, '..', 's1.control.jsonl')
    await writeFile(control, '{"text":"old"}\n')
    await (ctx.emit as (n: string, p: unknown) => void).call(ctx, 'agent/created', { agent, source: 'fresh' })
    await new Promise(resolve => setTimeout(resolve, 60))
    await appendFile(control, '{"text":"new one"}\n{"text":"par')
    await vi.waitFor(() => { expect(steered).toHaveLength(1) })
    const message = steered[0] as { content: { text: string }[]; role: string }
    expect(message.role).toBe('user')
    expect(message.content[0]?.text).toBe('new one')
    await appendFile(control, 'tial"}\n')
    await vi.waitFor(() => { expect(steered).toHaveLength(2) })
    ctx.emit('agent/disposed', { agent })
  })

  it('writes a usage snapshot after an end frame', async () => {
    const { ctx, artifact, agent } = await setup()
    await (ctx.emit as (n: string, p: unknown) => void).call(ctx, 'agent/created', { agent, source: 'fresh' })
    ctx.emit('agent/assistant-stream', { agent, frame: { type: 'end' } } as never)
    const usage = join(artifact, '..', 's1.usage.json')
    await vi.waitFor(async () => {
      const snapshot = JSON.parse(await readFile(usage, 'utf8')) as { totalTokens: number; nodes: number }
      expect(snapshot).toMatchObject({ totalTokens: 42, nodes: 2 })
    })
  })
})

describe('default configuration', () => {
  it('applies without a config object', async () => {
    const ctx = new Context()
    await expect(ctx.plugin(JobControl)).resolves.toBeDefined()
  })
})
