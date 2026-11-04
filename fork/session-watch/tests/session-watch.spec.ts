/**
 * The stored-Session watcher: a Session another process stores after the
 * dashboard's list belongs in the list without a reload, announced with the
 * same summary `session/list` would return.
 */

import { Context, Service } from '@deepseek-ai/cordis'
import type {} from '@deepseek-ai/dsh-api-session-controller/types'
import type { SessionListValue, SessionSummary } from '@deepseek-ai/dsh-api-session-controller/types'
import type {} from '@deepseek-ai/dsh-session-query'
import type { SessionRecord } from '@deepseek-ai/dsh-session-query'
import { SESSION_FORMAT_VERSION, SessionId } from '@deepseek-ai/dsh-session'
import type { SessionHeader } from '@deepseek-ai/dsh-session'
import { describe, expect, it, vi } from 'vitest'
import * as SessionWatch from '../src/index.ts'

const POLL_MS = 15

interface Corpus {
  readonly records: SessionRecord[]
  readonly summaries: Map<string, SessionSummary>
  queryCalls: number
  listCalls: number
}

function header(id: string, cwd?: string, createdAt = 1): SessionHeader {
  return {
    version: SESSION_FORMAT_VERSION,
    id: SessionId(id),
    createdAt,
    isSeeded: false,
    ...(cwd === undefined ? {} : { cwd }),
  }
}

function record(id: string): SessionRecord {
  return { header: header(id, '/work'), live: false, persisted: true, formatStatus: 'current' }
}

function summary(id: string, overrides: Partial<SessionSummary> = {}): SessionSummary {
  return {
    sessionId: SessionId(id),
    updatedAt: 1,
    agentAvailable: false,
    running: false,
    blank: false,
    cwd: '/work',
    ...overrides,
  }
}

function corpus(): Corpus {
  return { records: [], summaries: new Map(), queryCalls: 0, listCalls: 0 }
}

async function setup(initial?: (corpus: Corpus) => void, holdFirstScan = false) {
  const data = corpus()
  initial?.(data)
  let release = (): void => {}
  const gate = holdFirstScan ? new Promise<void>((resolve) => { release = resolve }) : undefined
  const ctx = new Context()
  class FakeQuery extends Service {
    constructor(c: Context) { super(c, 'sessionQuery') }
    async listSessions(): Promise<SessionRecord[]> {
      data.queryCalls++
      if (gate !== undefined && data.queryCalls === 1) await gate
      return data.records.map(item => structuredClone(item))
    }
  }
  class FakeController extends Service {
    constructor(c: Context) { super(c, 'sessionController') }
    async list(): Promise<SessionListValue> {
      data.listCalls++
      return { items: data.records.map(item => data.summaries.get(item.header.id)).filter(item => item !== undefined) }
    }
  }
  await ctx.plugin(FakeQuery)
  await ctx.plugin(FakeController)
  const added: SessionSummary[] = []
  ctx.on('api-session/added', item => { added.push(item) })
  await ctx.plugin(SessionWatch, { pollIntervalMs: POLL_MS })
  return { ctx, data, added, release }
}

/** Wait for the watcher's first scan to establish its baseline. */
async function baselined(data: Corpus): Promise<void> {
  await vi.waitFor(() => { expect(data.queryCalls).toBeGreaterThan(0) })
  // A baseline never summarizes: it only records what the dashboard already lists.
  expect(data.listCalls).toBe(0)
}

describe('fork session-watch', () => {
  it('announces a Session another process stores after the baseline', async () => {
    const { data, added } = await setup()
    await baselined(data)
    data.records.push(record('job-1'))
    data.summaries.set('job-1', summary('job-1', { running: true }))
    await vi.waitFor(() => { expect(added).toHaveLength(1) })
    expect(added[0]).toMatchObject({ sessionId: 'job-1', running: true, agentAvailable: false })
  })

  it('does not announce the corpus that existed before it started', async () => {
    const { data, added } = await setup((store) => {
      store.records.push(record('old-job'))
      store.summaries.set('old-job', summary('old-job'))
    })
    await baselined(data)
    await new Promise(resolve => setTimeout(resolve, POLL_MS * 3))
    expect(added).toEqual([])
  })

  it('announces a Session created while the first scan was in flight', async () => {
    const { data, added, release } = await setup(undefined, true)
    data.records.push({ ...record('boot-job'), header: header('boot-job', '/work', Date.now()) })
    data.summaries.set('boot-job', summary('boot-job'))
    release()
    await vi.waitFor(() => { expect(added.map(item => item.sessionId)).toEqual(['boot-job']) })
  })

  it('announces a stored Session once across later scans', async () => {
    const { data, added } = await setup()
    await baselined(data)
    data.records.push(record('job-2'))
    data.summaries.set('job-2', summary('job-2'))
    await vi.waitFor(() => { expect(added).toHaveLength(1) })
    await new Promise(resolve => setTimeout(resolve, POLL_MS * 4))
    expect(added).toHaveLength(1)
  })

  it('leaves a Session this Host runs to the controller', async () => {
    const { data, added } = await setup()
    await baselined(data)
    data.records.push(record('host-job'))
    data.summaries.set('host-job', summary('host-job', { agentAvailable: true }))
    await vi.waitFor(() => { expect(data.listCalls).toBe(1) })
    expect(added).toEqual([])
  })

  it('ignores a stored record the list would not show', async () => {
    const { data, added } = await setup()
    await baselined(data)
    data.records.push({ ...record('no-cwd'), header: header('no-cwd') })
    data.summaries.set('no-cwd', summary('no-cwd', { cwd: undefined }))
    await new Promise(resolve => setTimeout(resolve, POLL_MS * 3))
    expect(data.listCalls).toBe(0)
    expect(added).toEqual([])
  })

  it('stops scanning when the plugin unloads', async () => {
    const { ctx, data } = await setup()
    await baselined(data)
    await ctx.fiber.dispose()
    const calls = data.queryCalls
    await new Promise(resolve => setTimeout(resolve, POLL_MS * 3))
    expect(data.queryCalls).toBe(calls)
  })
})
