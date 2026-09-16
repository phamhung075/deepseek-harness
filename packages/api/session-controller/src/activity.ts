/** Durable activity of stored sessions this Host does not run. */

import type { Context } from '@deepseek-ai/cordis'
import type { SessionId } from '@deepseek-ai/dsh-session'
import type { SessionAppends, SessionAppendSubscription } from '@deepseek-ai/dsh-session-persistence'

/** Default cadence at which stored sessions are re-examined for recent durable writes. */
export const DEFAULT_COLD_ACTIVITY_POLL_MS = 5_000

/** Default window after the last durable write in which a cold session keeps reporting running. */
export const DEFAULT_COLD_ACTIVITY_IDLE_MS = 60_000

/** Default maximum number of cold sessions observed at once. */
export const DEFAULT_COLD_ACTIVITY_MAX_SESSIONS = 16

/** Resolved policy of the durable cold-session observer. */
export interface ColdActivityPolicy {
  /** Cadence in milliseconds at which the stored corpus is re-examined. */
  readonly pollIntervalMs: number
  /**
   * Window in milliseconds after a session's last durable write during which it
   * still reports running. It bounds how long a writer that died mid-turn keeps
   * its session looking alive.
   */
  readonly idleMs: number
  /** Maximum cold sessions observed at once; sessions beyond it wait for a free slot. */
  readonly maxSessions: number
}

/**
 * The durable-activity view Session list rows consult for stored sessions.
 * Consumers depend on this predicate, not on how activity is observed.
 */
export interface ColdActivityView {
  /**
   * Whether one stored session with no in-process owner is currently running.
   * @param id - the session to report on.
   * @returns true while its last durable turn is open and its writes are recent.
   */
  isRunning(id: SessionId): boolean
}

/** One cold session this Host observes durable appends for. */
interface ObservedSession {
  /** Time of the last durable write this Host observed, or adopted from the stored artifact. */
  lastDurableAt: number
  /**
   * Whether the last durable turn boundary this Host observed opened a turn;
   * `undefined` while no boundary was observed, which reports running for as
   * long as the session keeps writing.
   */
  openTurn: boolean | undefined
  /** Whether the last emitted status said running, so a transition is pushed once. */
  reported: boolean
  readonly subscription: SessionAppendSubscription
  /** The fold of this subscription's appends; awaited at teardown for quiescence. */
  pump: Promise<void>
}

/**
 * Reports durable activity for stored sessions another process is running, so
 * the Session list can mark them running and stop when their turn closes.
 *
 * The in-process path stays authoritative: a session this Host holds in
 * `ctx.sessions` or `ctx.agents` is never observed here, and one that becomes
 * live after being observed is dropped. The capability is deployment-shaped —
 * without `ctx.sessionAppends` nothing can be observed, which this class warns
 * about once rather than reporting every cold session as idle in silence.
 */
export class ColdSessionActivity implements ColdActivityView {
  private readonly observed = new Map<SessionId, ObservedSession>()
  private timer: ReturnType<typeof setInterval> | undefined
  private polling: Promise<void> | undefined
  private stopped = false
  private warnedAbsent = false

  /**
   * @param ctx - Host context carrying persistence, Session, and Agent registries.
   * @param policy - resolved observation cadence, idle window, and concurrency bound.
   */
  constructor(
    private readonly ctx: Context,
    private readonly policy: ColdActivityPolicy,
  ) {
    ctx.effect(() => () => this.stop(), 'session-controller.cold-activity')
    this.timer = setInterval(() => { void this.poll() }, policy.pollIntervalMs)
    this.timer.unref()
    void this.poll()
  }

  /**
   * Whether one stored session with no in-process owner is currently running.
   * @param id - the session to report on.
   * @returns true while its last durable turn is open and its writes are recent.
   */
  isRunning(id: SessionId): boolean {
    const entry = this.observed.get(id)
    if (entry === undefined || entry.openTurn === false) return false
    return Date.now() - entry.lastDurableAt <= this.policy.idleMs
  }

  /** Re-examine the stored corpus once, unless a pass is already running. */
  private async poll(): Promise<void> {
    if (this.stopped || this.polling !== undefined) return
    const task = this.runPoll()
    this.polling = task
    try {
      await task
    } finally {
      if (this.polling === task) this.polling = undefined
    }
  }

  /** Adopt every recently written cold session, then release settled and hosted ones. */
  private async runPoll(): Promise<void> {
    const appends = this.ctx.get('sessionAppends')
    const persistence = this.ctx.get('sessionPersistence')
    if (appends === undefined || persistence === undefined) {
      this.warnAbsent(appends === undefined ? 'ctx.sessionAppends' : 'ctx.sessionPersistence')
      return
    }
    let snapshots
    try {
      snapshots = await persistence.list()
    } catch (error: unknown) {
      this.ctx.logger.warn(`session-controller: cold activity listing failed: ${messageOf(error)}`)
      return
    }
    const now = Date.now()
    for (const snapshot of snapshots) {
      if (this.stopped) return
      const id = snapshot.header.id
      if (this.hosted(id)) {
        this.release(id)
        continue
      }
      if (this.observed.has(id)) continue
      const lastModifiedAt = snapshot.lastModifiedAt
      if (lastModifiedAt === undefined || now - lastModifiedAt > this.policy.idleMs) continue
      if (this.observed.size >= this.policy.maxSessions) continue
      await this.adopt(id, lastModifiedAt, appends)
    }
    for (const [id, entry] of this.observed) {
      if (now - entry.lastDurableAt > this.policy.idleMs) this.release(id)
    }
  }

  /** Whether this Host already runs the session, which keeps its own status authoritative. */
  private hosted(id: SessionId): boolean {
    return this.ctx.sessions.get(id) !== undefined || this.ctx.get('agents')?.get(id) !== undefined
  }

  /**
   * Start observing one cold session's durable appends.
   * @param id - the stored session to observe.
   * @param lastModifiedAt - the artifact modification time adopted as its first activity.
   * @param appends - the mounted durable-append capability.
   */
  private async adopt(
    id: SessionId,
    lastModifiedAt: number,
    appends: SessionAppends,
  ): Promise<void> {
    let subscription: SessionAppendSubscription
    try {
      subscription = await appends.watch(id)
    } catch (error: unknown) {
      this.ctx.logger.warn(`session-controller: cold activity observation of "${id}" failed: ${messageOf(error)}`)
      return
    }
    if (this.stopped || this.hosted(id)) {
      subscription.close()
      return
    }
    const entry: ObservedSession = {
      lastDurableAt: lastModifiedAt,
      openTurn: undefined,
      reported: false,
      subscription,
      pump: Promise.resolve(),
    }
    this.observed.set(id, entry)
    entry.pump = this.pump(id, subscription)
    this.report(id)
  }

  /**
   * Fold one observed session's appends into its durable turn state.
   * @param id - the observed session.
   * @param subscription - its live durable-append subscription.
   */
  private async pump(id: SessionId, subscription: SessionAppendSubscription): Promise<void> {
    try {
      for await (const batch of subscription.events) {
        const entry = this.observed.get(id)
        if (entry === undefined) return
        entry.lastDurableAt = Date.now()
        for (const event of batch) {
          if (event.type === 'turn/start') entry.openTurn = true
          else if (event.type === 'turn/end') entry.openTurn = false
        }
        this.report(id)
      }
    } catch (error: unknown) {
      this.ctx.logger.warn(`session-controller: cold activity observation of "${id}" failed: ${messageOf(error)}`)
    }
  }

  /**
   * Push one observed session's status when it changed.
   * @param id - the observed session.
   */
  private report(id: SessionId): void {
    const entry = this.observed.get(id)
    if (entry === undefined) return
    const running = this.isRunning(id)
    if (entry.reported === running) return
    entry.reported = running
    this.ctx.emit('api-session/status', id, running)
  }

  /**
   * Stop observing one session and retract a running status it reported.
   * @param id - the session to release.
   */
  private release(id: SessionId): void {
    const entry = this.observed.get(id)
    if (entry === undefined) return
    this.observed.delete(id)
    entry.subscription.close()
    if (entry.reported) this.ctx.emit('api-session/status', id, false)
  }

  /** Warn once that this deployment cannot observe durable activity. */
  private warnAbsent(missing: string): void {
    if (this.warnedAbsent) return
    this.warnedAbsent = true
    this.ctx.logger.warn(
      `session-controller: cold sessions cannot report durable activity because ${missing} is not mounted`,
    )
  }

  /** Stop observing, releasing every subscription and waiting for its pump to settle. */
  private async stop(): Promise<void> {
    if (this.stopped) return
    this.stopped = true
    if (this.timer !== undefined) clearInterval(this.timer)
    this.timer = undefined
    const entries = [...this.observed.values()]
    this.observed.clear()
    for (const entry of entries) entry.subscription.close()
    await Promise.allSettled(entries.map(entry => entry.pump))
    await this.polling
  }
}

/** One message for a failure whose only consumer is a log line. */
function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}
