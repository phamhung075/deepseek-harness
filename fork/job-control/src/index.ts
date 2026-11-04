/**
 * Fork-only plugin: lets another process steer a running root Agent and read
 * what it currently holds, without waiting for the turn to finish.
 *
 * - Control inbox `<artifact>.control.jsonl`: each appended line
 *   `{"id": string, "text": string}` is submitted with `agent.steer(...)` at the
 *   Agent's next step boundary. Lines present when the Agent is first observed
 *   are ignored, so a resumed Session never replays old commands.
 * - Usage snapshot `<artifact>.usage.json`: rewritten after each completed
 *   Assistant attempt with `ctx.tokenMeter`'s measurement of the live Session.
 *
 * Both files sit beside the Session artifact resolved through the jsonl
 * provider's `resolveCurrentLog`. The plugin edits no upstream file.
 * @module @phamhung075/dsh-fork-job-control
 */

import type { Context } from '@deepseek-ai/cordis'
import type { Agent } from '@deepseek-ai/dsh-agent'
import type { MessageSource } from '@deepseek-ai/dsh-llm'
import { open, rename, writeFile } from 'node:fs/promises'

/** Plugin configuration. */
export interface Config {
  /** Cadence in milliseconds at which control inboxes are re-read; defaults to 500. */
  pollIntervalMs?: number
}

/** Persistence service members this plugin needs; present on the jsonl provider. */
interface LogResolver {
  resolveCurrentLog(id: string, signal?: AbortSignal): Promise<string | undefined>
}

interface Watched {
  /** Byte offset of the first unread control byte; `undefined` until the inbox is first observed. */
  offset: number | undefined
  /** Incomplete trailing line carried to the next read. */
  partial: string
  busy: boolean
}

const DEFAULT_POLL_MS = 500

/**
 * Derive a sibling path from a JSONL Session artifact path.
 * @param artifact - absolute artifact path ending in `.jsonl` or `.jsonl.zstd`.
 * @param extension - sibling suffix such as `.control.jsonl`.
 * @returns the sibling path, or `undefined` when the artifact has no JSONL suffix.
 */
export function siblingPath(artifact: string, extension: string): string | undefined {
  for (const suffix of ['.jsonl.zstd', '.jsonl']) {
    if (artifact.endsWith(suffix)) return `${artifact.slice(0, -suffix.length)}${extension}`
  }
  return undefined
}

/** One parsed control line: the text to steer and the browser identity when the GUI sent it. */
export interface ControlRecord {
  /** Message body submitted to the Agent. */
  readonly text: string
  /** Browser request id, echoed onto the durable user message so its optimistic echo retires. */
  readonly rpcId?: string
}

/**
 * Parse complete control lines.
 * @param text - complete lines separated by newlines.
 * @returns every well-formed record, in order; malformed lines are skipped.
 */
export function parseControlRecords(text: string): ControlRecord[] {
  const out: ControlRecord[] = []
  for (const line of text.split('\n')) {
    if (line.trim() === '') continue
    try {
      const record = JSON.parse(line) as { text?: unknown; rpcId?: unknown }
      if (typeof record.text === 'string' && record.text !== '') {
        out.push({
          text: record.text,
          ...(typeof record.rpcId === 'string' && record.rpcId !== '' ? { rpcId: record.rpcId } : {}),
        })
      }
    } catch {
      // A line a sender wrote badly cannot be steered; the next line is independent.
    }
  }
  return out
}

/**
 * Parse complete control lines into their text only.
 * @param text - complete lines separated by newlines.
 * @returns the non-empty `text` of every well-formed record; malformed lines are skipped.
 */
export function parseControlLines(text: string): string[] {
  return parseControlRecords(text).map(record => record.text)
}

/**
 * Register the control inbox and usage snapshot.
 * @param ctx - plugin context; without a jsonl provider the plugin is silent.
 * @param config - plugin configuration.
 */
export function apply(ctx: Context, config: Config = {}): void {
  const watched = new Map<Agent, Watched>()

  const artifactOf = async (agent: Agent): Promise<string | undefined> => {
    const persistence = ctx.get('sessionPersistence') as unknown as Partial<LogResolver> | undefined
    if (typeof persistence?.resolveCurrentLog !== 'function') return undefined
    try {
      return await persistence.resolveCurrentLog(agent.session.id)
    } catch (error: unknown) {
      ctx.logger.warn(`job-control: artifact lookup failed: ${String(error)}`)
      return undefined
    }
  }

  const pollOne = async (agent: Agent, state: Watched): Promise<void> => {
    const artifact = await artifactOf(agent)
    const path = artifact === undefined ? undefined : siblingPath(artifact, '.control.jsonl')
    if (path === undefined) return
    let handle
    try {
      handle = await open(path, 'r')
    } catch (error: unknown) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
        state.offset ??= 0
        return
      }
      throw error
    }
    try {
      const { size } = await handle.stat()
      if (state.offset === undefined) {
        state.offset = size
        return
      }
      if (size <= state.offset) return
      const buffer = Buffer.alloc(size - state.offset)
      const { bytesRead } = await handle.read(buffer, 0, buffer.length, state.offset)
      state.offset += bytesRead
      const text = state.partial + buffer.subarray(0, bytesRead).toString('utf8')
      const end = text.lastIndexOf('\n')
      state.partial = text.slice(end + 1)
      if (end < 0) return
      const { createUserMessage } = await import('@deepseek-ai/dsh-llm')
      for (const record of parseControlRecords(text.slice(0, end))) {
        const source = (record.rpcId === undefined
          ? { kind: 'user' }
          : { kind: 'user', rpcId: record.rpcId }) as MessageSource
        agent.steer(createUserMessage({ content: [{ type: 'text', text: record.text }], source }))
      }
    } finally {
      await handle.close()
    }
  }

  const poll = (): void => {
    for (const [agent, state] of watched) {
      if (state.busy) continue
      state.busy = true
      pollOne(agent, state)
        .catch((error: unknown) => { ctx.logger.warn(`job-control: poll failed: ${String(error)}`) })
        .finally(() => { state.busy = false })
    }
  }

  ctx.on('agent/created', ({ agent }) => {
    if (agent.session.header.origin === 'subagent') return
    watched.set(agent, { offset: undefined, partial: '', busy: false })
    poll()
  })

  ctx.on('agent/disposed', ({ agent }) => { watched.delete(agent) })

  ctx.on('agent/assistant-stream', ({ agent, frame }) => {
    if (frame.type !== 'end' || !watched.has(agent)) return
    const meter = ctx.get('tokenMeter')
    if (meter === undefined) return
    void (async () => {
      const artifact = await artifactOf(agent)
      const path = artifact === undefined ? undefined : siblingPath(artifact, '.usage.json')
      if (path === undefined) return
      const measurement = meter.measure(agent.session)
      const snapshot = {
        updatedAt: Date.now(),
        logRevision: Number(measurement.logRevision),
        totalTokens: measurement.totalTokens,
        surfaceTokens: measurement.surfaceTokens,
        nodes: measurement.nodes.length,
      }
      await writeFile(`${path}.tmp`, JSON.stringify(snapshot))
      await rename(`${path}.tmp`, path)
    })().catch((error: unknown) => { ctx.logger.warn(`job-control: usage write failed: ${String(error)}`) })
  })

  ctx.effect(() => {
    const timer = setInterval(poll, config.pollIntervalMs ?? DEFAULT_POLL_MS)
    timer.unref()
    return () => { clearInterval(timer) }
  })
}
