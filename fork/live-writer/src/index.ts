/**
 * Fork-only plugin: present a Session another process is still writing as it is
 * stored, instead of the crash-recovery transcript a cold read balances it with.
 *
 * A Host that follows a Session it does not run reads it cold, and a cold read
 * cannot tell a live writer from a dead one: it appends synthetic
 * `interruptedTurnClosers` to any log whose last turn is open. For a running
 * Session that fabricates a stop ("The DSH process exited…"), and the invented
 * seqs also publish a snapshot cursor past appends the writer has not made yet,
 * so the follower drops the turn's real end.
 *
 * This plugin wraps the resolved `sessionQuery` service. When an observation
 * ends in a synthetic interrupted-turn tail and the writer's own kernel lock is
 * still held, it exposes the stored prefix and its exact cursor instead. It
 * imports nothing but Node builtins and Cordis types, so it runs under the
 * source launch from a linked bundle; it edits no upstream file and restores the
 * original method when it unloads.
 * @module @phamhung075/dsh-fork/live-writer
 */

import type { Context } from '@deepseek-ai/cordis'
import type { SessionEvent, SessionHeader, SessionId, SessionSeqCursor } from '@deepseek-ai/dsh-session'
import type { SessionObservation, SessionObservationOptions } from '@deepseek-ai/dsh-session-query'
import { readFile, readdir, stat } from 'node:fs/promises'
import type { BigIntStats } from 'node:fs'
import { homedir } from 'node:os'
import { join } from 'node:path'

export const name = 'fork-live-writer'

/** The wrapped service must be resolved before its method can be replaced. */
export const inject = ['sessionQuery']

/** Plugin configuration. */
export interface Config {
  /**
   * Absolute `sessions` root of the mounted JSONL backend. Omit to use the
   * profile convention, `$DSH_HOME/sessions` (`~/.dsh/sessions` by default).
   */
  sessionsRoot?: string
}

/** Recovery code the Harness writes for a call whose result was never recorded. */
const TOOL_OUTCOME_UNKNOWN = 'TOOL_OUTCOME_UNKNOWN'

/** Session directory names are memoized: a stored Session never moves. */
const directoryCache = new Map<string, string | undefined>()

/** Upper bound of the memo above, so a long-lived Host cannot grow it forever. */
const DIRECTORY_CACHE_LIMIT = 512

/**
 * Register the live-writer correction.
 * @param ctx - plugin context carrying the resolved `sessionQuery` service.
 * @param config - optional sessions-root override.
 */
export function apply(ctx: Context, config: Config = {}): void {
  const sessionsRoot = resolveSessionsRoot(config.sessionsRoot)
  const query = ctx.sessionQuery
  const original = query.observeSession.bind(query)
  const patched = async (
    sessionId: SessionId,
    options: SessionObservationOptions = {},
  ): Promise<SessionObservation> => presentStoredTurn(sessionsRoot, await original(sessionId, options))
  query.observeSession = patched
  ctx.effect(() => () => {
    // The service is reached through a tracing proxy whose reads wrap methods,
    // so the wrapper installed above is identified by the owned property
    // descriptor. Dropping it exposes the prototype method again, and a wrapper
    // another plugin installed later stays untouched.
    if (Object.getOwnPropertyDescriptor(query, 'observeSession')?.value === patched) {
      Reflect.deleteProperty(query, 'observeSession')
    }
  }, 'fork-live-writer.observe')
}

/**
 * Replace a fabricated interruption with the stored prefix while a writer holds
 * the Session.
 * @param sessionsRoot - the JSONL backend's sessions root.
 * @param observation - the observation the Host's cold read produced.
 * @returns the corrected observation, or the same one when nothing applies.
 */
async function presentStoredTurn(
  sessionsRoot: string,
  observation: SessionObservation,
): Promise<SessionObservation> {
  if (observation.source !== 'prepared') return observation
  const stored = storedPrefix(observation.events)
  if (stored === undefined) return observation
  if (await writerHolds(sessionsRoot, observation.header) !== true) return observation
  return correctedLease(observation, stored, stored.at(-1)?.seq ?? -1)
}

/**
 * Read the stored prefix out of a balanced log without the upstream closer
 * generator: the synthetic tail is the run of same-timestamp recovery events
 * that closes a turn the prefix leaves open, and the prefix must leave one open
 * for those closers to exist at all.
 * @param events - the balanced events a cold read returned.
 * @returns the stored prefix, or `undefined` when the tail is not that shape.
 */
export function storedPrefix(events: readonly SessionEvent[]): readonly SessionEvent[] | undefined {
  const last = events.at(-1)
  if (last === undefined || last.type !== 'turn/end') return undefined
  // A turn that failed carries an `error` reason instead.
  if (last.data.reason.kind !== 'interrupted') return undefined
  let cut = events.length - 1
  while (cut > 0) {
    const previous = events[cut - 1]
    if (previous === undefined || previous.time !== last.time) break
    if (!isRecoveryEvent(previous)) break
    cut -= 1
  }
  if (cut === 0) return undefined
  const prefix = events.slice(0, cut)
  return openTurn(prefix) ? prefix : undefined
}

/**
 * Whether one event belongs to the synthetic recovery tail.
 * @param event - the candidate event.
 * @returns true for a recovery tool result or a step end.
 */
function isRecoveryEvent(event: SessionEvent): boolean {
  if (event.type === 'step/end') return true
  if (event.type !== 'tool/result') return false
  return event.data.error?.code === TOOL_OUTCOME_UNKNOWN
}

/**
 * Whether the log leaves a turn open, which is what makes closers necessary.
 * @param events - the candidate stored prefix.
 * @returns true when the last turn boundary is a start.
 */
function openTurn(events: readonly SessionEvent[]): boolean {
  let open = false
  for (const event of events) {
    if (event.type === 'turn/start') open = true
    else if (event.type === 'turn/end') open = false
  }
  return open
}

/**
 * Report whether a writer currently holds the Session's kernel lock.
 *
 * The JSONL backend holds an exclusive `flock(2)` on `<session dir>/session.lock`
 * for a write handle's whole life, and Linux publishes every such lock in
 * `/proc/locks`; that answers exactly, without taking the lock and without a
 * spawn. Only Linux is observable here: other platforms answer `undefined` and
 * the Host keeps its own presentation.
 * @param sessionsRoot - the JSONL backend's sessions root.
 * @param header - the stored Session header, which locates its directory.
 * @returns `true` while a writer holds the lock, `false` when it is free, and
 *   `undefined` when the lock cannot be examined.
 */
export async function writerHolds(
  sessionsRoot: string,
  header: SessionHeader,
): Promise<boolean | undefined> {
  if (process.platform !== 'linux') return undefined
  const directory = await sessionDirectory(sessionsRoot, header.id)
  if (directory === undefined) return false
  let identity: BigIntStats
  try {
    identity = await stat(join(directory, 'session.lock'), { bigint: true })
  } catch (error: unknown) {
    return errorCode(error) === 'ENOENT' ? false : undefined
  }
  let table: string
  try {
    table = await readFile('/proc/locks', 'utf8')
  } catch {
    // A distribution or container may hide the lock table; hidden liveness is
    // unknown, not a free lock.
    return undefined
  }
  const device = `${hex8(deviceMajor(identity.dev))}:${hex8(deviceMinor(identity.dev))}`
  const inode = String(identity.ino)
  return table.split('\n').some((line) => {
    const fields = line.trim().split(/\s+/)
    // <id>: FLOCK ADVISORY WRITE <pid> <major>:<minor>:<inode> <start> <end>
    if (fields[1] !== 'FLOCK' || fields[3] !== 'WRITE') return false
    const parts = fields[5]?.split(':')
    return parts !== undefined
      && parts.length === 3
      && `${parts[0]}:${parts[1]}` === device
      && parts[2] === inode
  })
}

/** Format one device component the way `/proc/locks` prints it. */
function hex8(value: number): string {
  return value.toString(16).padStart(2, '0')
}

/** glibc `gnu_dev_major` for a `stat` device number. */
function deviceMajor(dev: bigint): number {
  return Number(((dev >> 8n) & 0xfffn) | ((dev >> 32n) & ~0xfffn))
}

/** glibc `gnu_dev_minor` for a `stat` device number. */
function deviceMinor(dev: bigint): number {
  return Number((dev & 0xffn) | ((dev >> 12n) & ~0xffn))
}

/**
 * Locate one Session directory beneath the sessions root.
 *
 * The root groups Sessions by project directory, whose name this plugin does
 * not need: it looks for the Session's own segment under each project. Session
 * ids reach the filesystem through the backend's escape of anything outside
 * `[A-Za-z0-9._-]`, which a UUID passes through unchanged.
 * @param sessionsRoot - the JSONL backend's sessions root.
 * @param id - the Session to locate.
 * @returns the Session directory, or `undefined` when it is not stored.
 */
async function sessionDirectory(sessionsRoot: string, id: SessionId): Promise<string | undefined> {
  const key = `${sessionsRoot}\u0000${id}`
  if (directoryCache.has(key)) return directoryCache.get(key)
  const segment = encodeSegment(id)
  let found: string | undefined
  try {
    for (const entry of await readdir(sessionsRoot, { withFileTypes: true })) {
      if (!entry.isDirectory()) continue
      const candidate = join(sessionsRoot, entry.name, segment)
      try {
        if ((await stat(candidate)).isDirectory()) {
          found = candidate
          break
        }
      } catch {
        // Another project simply has no such Session.
      }
    }
  } catch {
    found = undefined
  }
  if (directoryCache.size >= DIRECTORY_CACHE_LIMIT) directoryCache.clear()
  directoryCache.set(key, found)
  return found
}

/** Escape one path segment the way the JSONL backend names Session directories. */
function encodeSegment(raw: string): string {
  if (raw === '.') return '~002E'
  if (raw === '..') return '~002E~002E'
  let out = ''
  for (const ch of raw) {
    const safe = ch !== '~' && /^[A-Za-z0-9._-]$/.test(ch)
    out += safe ? ch : `~${ch.charCodeAt(0).toString(16).toUpperCase().padStart(4, '0')}`
  }
  return out
}

/** Resolve the sessions root: an explicit override, then the profile convention. */
function resolveSessionsRoot(configured?: string): string {
  if (configured !== undefined) {
    if (!isAbsolutePath(configured)) {
      throw new Error(`fork-live-writer: sessionsRoot must be an absolute path, got "${configured}"`)
    }
    return configured
  }
  const home = process.env.DSH_HOME?.trim()
  return join(home !== undefined && home !== '' ? home : join(homedir(), '.dsh'), 'sessions')
}

/** Whether a configured root is absolute. */
function isAbsolutePath(path: string): boolean {
  return path.startsWith('/') || /^[A-Za-z]:[\\/]/.test(path)
}

/** Read one Node error code. */
function errorCode(error: unknown): string | undefined {
  const code = (error as NodeJS.ErrnoException | null)?.code
  return typeof code === 'string' ? code : undefined
}

/** Re-publish one observation with the stored prefix and its exact cursor. */
function correctedLease(
  underlying: SessionObservation,
  events: readonly SessionEvent[],
  cursor: SessionSeqCursor,
): SessionObservation {
  const lease = (source: SessionObservation): SessionObservation => {
    let disposed = false
    return {
      source: source.source,
      header: source.header,
      inheritedEventCount: source.inheritedEventCount,
      events,
      cursor,
      ...source.revision === undefined ? {} : { revision: source.revision },
      ...source.projections === undefined ? {} : {
        projections: { asOfSeq: cursor, values: source.projections.values },
      },
      retain: () => {
        if (disposed) throw new Error('fork-live-writer: observation is disposed')
        return lease(source.retain())
      },
      [Symbol.dispose]: () => {
        if (disposed) return
        disposed = true
        source[Symbol.dispose]()
      },
    }
  }
  return lease(underlying)
}
