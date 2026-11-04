/**
 * Fork-only plugin: makes an offload job behave like a hand-created Session in
 * the Web GUI without editing any upstream file.
 *
 * A job worker publishes `$DSH_HOME/offload-workers/<jobId>.json`; this plugin
 * reads it, reports those Sessions running, patches the list rows the core
 * computes from write recency, and routes a prompt or a cancel to the worker's
 * socket instead of resuming a second writer. The prompt carries the browser's
 * request id to the worker so the durable user message can echo it back, which
 * retires the composer's optimistic echo without a client change.
 * @module @phamhung075/dsh-fork/offload-gui
 */

import type { Context } from '@deepseek-ai/cordis'
import type {} from '@deepseek-ai/dsh-api-session-controller/types'
import type { SessionId } from '@deepseek-ai/dsh-session'
import { readFileSync } from 'node:fs'
import { readdir, readFile } from 'node:fs/promises'
import { connect } from 'node:net'
import { homedir } from 'node:os'
import { join } from 'node:path'

export const name = 'fork-offload-gui'

/** Plugin configuration. */
export interface Config {
  /** Worker registry directory; defaults to `$DSH_HOME/offload-workers`. */
  workerRegistry?: string
  /** Cadence in milliseconds at which worker liveness is re-read; defaults to 1000. */
  pollIntervalMs?: number
  /** How long to wait for a worker's answer to one prompt or cancel; defaults to 15000. */
  socketTimeoutMs?: number
}

/** One live job worker's published identity and control socket. */
export interface Worker {
  readonly jobId: string
  readonly pid: number
  readonly pidStart?: string
  readonly socketPath: string
  readonly sessionId: SessionId
}

/** One row of the Session list this plugin rewrites. */
interface SessionRow {
  readonly sessionId: SessionId
  readonly running: boolean
  readonly [field: string]: unknown
}

/** The list value the core controller returns. */
interface SessionListValue {
  readonly items: readonly SessionRow[]
}

/** One text part of a prompt request. */
interface PromptPart {
  readonly type: string
  readonly text?: string
}

/** The prompt request the controller receives from the browser. */
interface PromptRequest {
  readonly requestId: string
  readonly sessionId: SessionId
  readonly mode: 'queue' | 'steer'
  readonly content: readonly PromptPart[]
}

/** The controller methods this plugin wraps. */
interface ControllerLike {
  list(request: unknown, signal?: AbortSignal): Promise<SessionListValue>
  prompt(request: PromptRequest, signal?: AbortSignal): Promise<{ readonly accepted: true }>
  cancel(request: { readonly sessionId: SessionId }): Promise<{ readonly accepted: true }>
}

/** Reply a worker sends on one socket connection. */
interface WorkerReply {
  readonly ok: boolean
  readonly error?: string
}

/** Kernel start time of one pid, in clock ticks since boot; undefined off Linux. */
export function procStartTime(pid: number): string | undefined {
  try {
    const fields = readFileSync(`/proc/${pid}/stat`, 'utf8')
    const after = fields.slice(fields.lastIndexOf(')') + 2).trim().split(' ')
    // after[0] is field 3 (state), so field 22 (starttime) is after[19].
    return after[19]
  } catch {
    // `/proc` is absent, or the process exited between the liveness check and this read.
    return undefined
  }
}

/** Whether a published worker still runs; a rebooted or reused pid does not count. */
export function isWorkerAlive(worker: Worker): boolean {
  if (!Number.isInteger(worker.pid) || worker.pid <= 0) return false
  try {
    process.kill(worker.pid, 0)
  } catch (error: unknown) {
    if ((error as NodeJS.ErrnoException).code !== 'EPERM') return false
  }
  if (worker.pidStart === undefined) return true
  const start = procStartTime(worker.pid)
  return start === undefined || start === worker.pidStart
}

/** Validate one parsed registry file this process did not write. */
export function parseWorker(value: unknown): Worker | undefined {
  if (typeof value !== 'object' || value === null) return undefined
  const record = value as Partial<Worker>
  if (typeof record.jobId !== 'string' || record.jobId === '') return undefined
  if (!Number.isInteger(record.pid) || (record.pid ?? 0) <= 0) return undefined
  if (typeof record.socketPath !== 'string' || record.socketPath === '') return undefined
  if (typeof record.sessionId !== 'string' || record.sessionId === '') return undefined
  return {
    jobId: record.jobId,
    pid: record.pid as number,
    ...(typeof record.pidStart === 'string' ? { pidStart: record.pidStart } : {}),
    socketPath: record.socketPath,
    sessionId: record.sessionId as SessionId,
  }
}

/**
 * Resolve the worker registry directory.
 * @param dshHome - explicit home, defaulting to `DSH_HOME` and then `~/.dsh`.
 * @returns the absolute registry path.
 */
export function workerRegistryPath(dshHome = process.env.DSH_HOME ?? join(homedir(), '.dsh')): string {
  return join(dshHome, 'offload-workers')
}

/**
 * Read every alive worker advertisement, keyed by the Session each owns.
 * @param directory - the worker registry directory.
 * @returns one live worker per Session.
 */
export async function readWorkers(directory: string): Promise<Map<SessionId, Worker>> {
  let names: string[]
  try {
    names = (await readdir(directory)).filter(name => name.endsWith('.json')).sort()
  } catch {
    // No job has run against this DSH_HOME yet.
    return new Map()
  }
  const live = new Map<SessionId, Worker>()
  for (const name of names) {
    let text: string
    try {
      text = await readFile(join(directory, name), 'utf8')
    } catch {
      // The worker settled between the directory read and this open.
      continue
    }
    let parsed: unknown
    try {
      parsed = JSON.parse(text)
    } catch {
      // A truncated file from a crash is stale.
      continue
    }
    const worker = parseWorker(parsed)
    if (worker === undefined || !isWorkerAlive(worker)) continue
    if (!live.has(worker.sessionId)) live.set(worker.sessionId, worker)
  }
  return live
}

/**
 * Rewrite the running bit of rows this plugin owns.
 * @param rows - rows the core list returned.
 * @param live - Sessions with a live worker.
 * @param stopped - Sessions whose seen worker is gone.
 * @returns rows with this plugin's running bit applied.
 */
export function patchRows(
  rows: readonly SessionRow[],
  live: ReadonlyMap<SessionId, Worker>,
  stopped: ReadonlySet<SessionId>,
): SessionRow[] {
  return rows.map((row) => {
    if (live.has(row.sessionId)) return row.running ? row : { ...row, running: true }
    if (stopped.has(row.sessionId)) return row.running ? { ...row, running: false } : row
    return row
  })
}

/** Extract the single text body a running job can accept. */
export function promptText(request: PromptRequest): string | undefined {
  if (request.content.some(part => part.type !== 'text')) return undefined
  const text = request.content
    .map(part => part.text ?? '')
    .join('')
    .trim()
  return text === '' ? undefined : text
}

/**
 * Send one JSON line to a worker socket and read its reply.
 * @param socketPath - the worker's published control socket.
 * @param payload - `{message, rpcId}` to steer, or `{cancel:true}` to stop.
 * @param timeoutMs - how long to wait for the worker's answer.
 * @returns the worker's reply.
 */
export async function requestWorker(
  socketPath: string,
  payload: { message: string; rpcId?: string } | { cancel: true },
  timeoutMs: number,
): Promise<WorkerReply> {
  return await new Promise<WorkerReply>((resolve, reject) => {
    const socket = connect(socketPath)
    let buffer = ''
    let settled = false
    const finish = (error: Error | undefined, reply?: WorkerReply): void => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      socket.destroy()
      if (error !== undefined) reject(error)
      else resolve(reply ?? { ok: false, error: 'job worker sent no reply' })
    }
    const timer = setTimeout(() => {
      finish(new Error(`timed out after ${timeoutMs}ms waiting for the job worker`))
    }, timeoutMs)
    socket.setEncoding('utf8')
    socket.on('connect', () => { socket.write(`${JSON.stringify(payload)}\n`) })
    socket.on('data', (chunk: string) => {
      buffer += chunk
      const end = buffer.indexOf('\n')
      if (end < 0) return
      try {
        const parsed = JSON.parse(buffer.slice(0, end)) as Partial<WorkerReply>
        finish(undefined, {
          ok: parsed.ok === true,
          ...(typeof parsed.error === 'string' ? { error: parsed.error } : {}),
        })
      } catch (error: unknown) {
        finish(new Error(`job worker sent an unreadable reply: ${error instanceof Error ? error.message : String(error)}`))
      }
    })
    socket.on('end', () => { finish(new Error('job worker closed the connection without answering')) })
    socket.on('error', (error: Error) => { finish(error) })
  })
}

/**
 * Register the list, prompt, cancel, and status hooks.
 * @param ctx - plugin context carrying the session controller.
 * @param config - registry, cadence, and socket timeout.
 */
function install(ctx: Context, config: Config): void {
  const controller = ctx.get('sessionController') as ControllerLike | undefined
  if (controller === undefined) return
  const registry = config.workerRegistry ?? workerRegistryPath()
  const timeoutMs = config.socketTimeoutMs ?? 15_000
  let live = new Map<SessionId, Worker>()
  let stopped = new Set<SessionId>()
  let refreshing: Promise<void> | undefined
  let stoppedPolling = false

  const emitStatus = (id: SessionId, running: boolean): void => {
    ctx.emit('api-session/status', id, running)
  }

  const refresh = async (): Promise<void> => {
    const next = await readWorkers(registry)
    if (stoppedPolling) return
    const previous = live
    // Publish the new map before the stops, so the status listener sees these
    // Sessions as unowned and does not re-assert them running.
    live = next
    for (const id of next.keys()) {
      if (!previous.has(id)) emitStatus(id, true)
    }
    for (const id of previous.keys()) {
      if (!next.has(id)) {
        stopped.add(id)
        emitStatus(id, false)
      }
    }
  }

  const poll = async (): Promise<void> => {
    if (refreshing !== undefined) return
    const task = refresh()
    refreshing = task
    try {
      await task
    } finally {
      if (refreshing === task) refreshing = undefined
    }
  }

  // Core cold activity derives running from durable writes only. While a worker
  // is alive its `false` is wrong; once a seen worker is gone its `true` is wrong.
  ctx.on('api-session/status', (id: SessionId, running: boolean) => {
    // Re-assert after the original dispatch, so the corrected value is the one
    // the Client applies last instead of racing the core event.
    if (!running && live.has(id)) {
      queueMicrotask(() => { if (live.has(id)) emitStatus(id, true) })
    } else if (running && !live.has(id) && stopped.has(id)) {
      queueMicrotask(() => { if (!live.has(id) && stopped.has(id)) emitStatus(id, false) })
    }
  })

  const originalList = controller.list
  const list = async (request: unknown, signal?: AbortSignal): Promise<SessionListValue> => {
    await poll()
    const value = await originalList.call(controller, request, signal)
    return { ...value, items: patchRows(value.items, live, stopped) }
  }
  controller.list = list

  const originalPrompt = controller.prompt
  const prompt = async (request: PromptRequest, signal?: AbortSignal): Promise<{ accepted: true }> => {
    const worker = live.get(request.sessionId)
    if (worker === undefined) return await originalPrompt.call(controller, request, signal)
    const text = promptText(request)
    if (text === undefined) {
      throw new Error('a running job accepts text messages only; stop it or wait for it to settle before sending an attachment')
    }
    const reply = await requestWorker(worker.socketPath, { message: text, rpcId: request.requestId }, timeoutMs)
    if (!reply.ok) throw new Error(`the job worker rejected the message: ${reply.error ?? 'unknown error'}`)
    return { accepted: true }
  }
  controller.prompt = prompt

  const originalCancel = controller.cancel
  const cancel = async (request: { sessionId: SessionId }): Promise<{ accepted: true }> => {
    const worker = live.get(request.sessionId)
    if (worker === undefined) return await originalCancel.call(controller, request)
    const reply = await requestWorker(worker.socketPath, { cancel: true }, timeoutMs)
    if (!reply.ok) throw new Error(`the job worker rejected the cancel: ${reply.error ?? 'unknown error'}`)
    return { accepted: true }
  }
  controller.cancel = cancel

  ctx.effect(() => {
    const timer = setInterval(() => { void poll() }, config.pollIntervalMs ?? 1_000)
    timer.unref()
    void poll()
    return () => {
      stoppedPolling = true
      clearInterval(timer)
      if (Object.getOwnPropertyDescriptor(controller, 'list')?.value === list) Reflect.deleteProperty(controller, 'list')
      if (Object.getOwnPropertyDescriptor(controller, 'prompt')?.value === prompt) Reflect.deleteProperty(controller, 'prompt')
      if (Object.getOwnPropertyDescriptor(controller, 'cancel')?.value === cancel) Reflect.deleteProperty(controller, 'cancel')
    }
  }, 'fork-offload-gui')
}

/**
 * Wait for the Session controller the Web Host provides. The ACP child loads
 * the same fork bundle but has no such service, so this plugin must stay
 * inactive there instead of holding the whole entry pending.
 * @param ctx - plugin context.
 * @param config - registry, cadence, and socket timeout.
 */
export function apply(ctx: Context, config: Config = {}): void {
  ctx.inject(['sessionController'], (scoped) => { install(scoped, config) })
}
