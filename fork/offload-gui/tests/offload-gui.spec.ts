import { Context, Service } from '@deepseek-ai/cordis'
import type { SessionId } from '@deepseek-ai/dsh-session'
import { createServer, type Server } from 'node:net'
import { mkdtemp, rm, writeFile, unlink } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import * as OffloadGui from '../src/index.ts'

const sid = (id: string): SessionId => id as SessionId

interface PromptRequest {
  readonly requestId: string
  readonly sessionId: SessionId
  readonly mode: 'queue' | 'steer'
  readonly content: readonly { readonly type: string; readonly text?: string }[]
}

class FakeController extends Service {
  readonly rows = [{ sessionId: sid('s1'), running: false }, { sessionId: sid('s2'), running: false }]
  readonly prompts: PromptRequest[] = []
  readonly cancels: { sessionId: SessionId }[] = []
  constructor(ctx: Context) { super(ctx, 'sessionController') }
  async list(): Promise<{ items: typeof this.rows }> { return { items: this.rows } }
  async prompt(request: PromptRequest): Promise<{ accepted: true }> {
    this.prompts.push(request)
    return { accepted: true }
  }
  async cancel(request: { sessionId: SessionId }): Promise<{ accepted: true }> {
    this.cancels.push(request)
    return { accepted: true }
  }
}

const dirs: string[] = []
const servers: Server[] = []
afterEach(async () => {
  await Promise.all(servers.splice(0).map(server => new Promise<void>((resolve) => { server.close(() => { resolve() }) })))
  await Promise.all(dirs.splice(0).map(dir => rm(dir, { recursive: true, force: true })))
})

async function registry(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), 'fork-offload-gui-'))
  dirs.push(dir)
  return dir
}

async function workerFile(dir: string, jobId: string, sessionId: string, socketPath: string): Promise<string> {
  const path = join(dir, `${jobId}.json`)
  await writeFile(path, JSON.stringify({
    v: 1,
    jobId,
    pid: process.pid,
    pidStart: OffloadGui.procStartTime(process.pid),
    socketPath,
    sessionId,
  }))
  return path
}

async function socket(dir: string): Promise<{ path: string; received: string[] }> {
  const received: string[] = []
  const path = join(dir, 'worker.sock')
  const server = createServer((connection) => {
    connection.setEncoding('utf8')
    connection.on('data', (chunk: string) => {
      received.push(chunk.trim())
      connection.end('{"ok":true}\n')
    })
  })
  servers.push(server)
  await new Promise<void>((resolve) => { server.listen(path, resolve) })
  return { path, received }
}

async function setup(config: Partial<OffloadGui.Config> = {}) {
  const dir = await registry()
  const ctx = new Context()
  await ctx.plugin(FakeController)
  await ctx.plugin(OffloadGui, { workerRegistry: dir, pollIntervalMs: 20, socketTimeoutMs: 500, ...config })
  const view = ctx.get('sessionController') as FakeController
  return { ctx, dir, view }
}

describe('worker records', () => {
  it('parses a live record and rejects a stale pid or start time', async () => {
    const dir = await registry()
    const socketPath = join(dir, 's.sock')
    await workerFile(dir, 'job-live', 's1', socketPath)
    await writeFile(join(dir, 'dead.json'), JSON.stringify({
      v: 1, jobId: 'job-dead', pid: 99_999_999, socketPath, sessionId: 's2',
    }))
    await writeFile(join(dir, 'reused.json'), JSON.stringify({
      v: 1, jobId: 'job-reused', pid: process.pid, pidStart: '0', socketPath, sessionId: 's3',
    }))
    await writeFile(join(dir, 'broken.json'), '{"jobId":')
    const live = await OffloadGui.readWorkers(dir)
    expect([...live.keys()]).toEqual([sid('s1')])
    expect(OffloadGui.isWorkerAlive({ jobId: 'j', pid: process.pid, socketPath, sessionId: sid('s') })).toBe(true)
  })

  it('applies live and stopped rows over what the core computed', () => {
    const worker: OffloadGui.Worker = { jobId: 'j', pid: 1, socketPath: '/x', sessionId: sid('s1') }
    const rows = OffloadGui.patchRows(
      [{ sessionId: sid('s1'), running: false }, { sessionId: sid('s2'), running: true }],
      new Map([[sid('s1'), worker]]),
      new Set([sid('s2')]),
    )
    expect(rows.map(row => row.running)).toEqual([true, false])
  })

  it('takes only one non-empty text body', () => {
    const request = (content: PromptRequest['content']): PromptRequest =>
      ({ requestId: 'r', sessionId: sid('s'), mode: 'queue', content })
    expect(OffloadGui.promptText(request([{ type: 'text', text: ' hi ' }]))).toBe('hi')
    expect(OffloadGui.promptText(request([{ type: 'image' }]))).toBeUndefined()
    expect(OffloadGui.promptText(request([{ type: 'text', text: '  ' }]))).toBeUndefined()
  })
})

describe('fork offload-gui', () => {
  it('reports a live worker running through the service view and list rows', async () => {
    const { ctx, dir, view } = await setup()
    const statuses: { id: string; running: boolean }[] = []
    ctx.on('api-session/status', (id: SessionId, running: boolean) => { statuses.push({ id, running }) })
    const control = await socket(dir)
    await workerFile(dir, 'job-1', 's1', control.path)
    await new Promise((resolve) => setTimeout(resolve, 80))
    expect(statuses.some(entry => entry.id === 's1' && entry.running)).toBe(true)
    expect((await view.list({})).items[0]?.running).toBe(true)
  })

  it('routes prompt and cancel over the socket, and falls through when the worker is gone', async () => {
    const { dir, view } = await setup()
    const control = await socket(dir)
    const file = await workerFile(dir, 'job-1', 's1', control.path)
    await new Promise((resolve) => setTimeout(resolve, 60))

    await expect(view.prompt({
      requestId: 'req-1', sessionId: sid('s1'), mode: 'queue', content: [{ type: 'text', text: 'go on' }],
    })).resolves.toEqual({ accepted: true })
    await expect(view.cancel({ sessionId: sid('s1') })).resolves.toEqual({ accepted: true })
    expect(control.received).toEqual(['{"message":"go on","rpcId":"req-1"}', '{"cancel":true}'])
    expect(view.prompts).toEqual([])

    await unlink(file)
    await new Promise((resolve) => setTimeout(resolve, 80))
    await view.prompt({ requestId: 'req-2', sessionId: sid('s1'), mode: 'queue', content: [{ type: 'text', text: 'later' }] })
    expect(view.prompts).toHaveLength(1)
  })
})
