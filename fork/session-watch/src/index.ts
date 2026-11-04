/**
 * Fork-only plugin: announces stored Sessions another process creates, so a
 * dashboard that listed the corpus once adds their rows without a reload.
 *
 * The Session controller announces only Sessions its own Host creates; a
 * Session written by another process (a deepseek-offload child) reaches the
 * list only through `session/list`. This plugin re-reads the stored corpus on
 * a cadence and, for each identity it has not seen, publishes the exact
 * `api-session/added` summary that endpoint would return.
 * @module @phamhung075/dsh-fork-session-watch
 */

import type { Context } from '@deepseek-ai/cordis'
import type {} from '@deepseek-ai/dsh-api-session-controller'
import type {} from '@deepseek-ai/dsh-api-session-controller/types'
import type {} from '@deepseek-ai/dsh-session-query'
import type { SessionId } from '@deepseek-ai/dsh-session'

export const name = 'fork-session-watch'

/** Both services must resolve before the corpus can be read and summarized. */
export const inject = ['sessionController', 'sessionQuery']

/** Plugin configuration. */
export interface Config {
  /** Cadence in milliseconds at which the stored corpus is re-read; defaults to 5000. */
  pollIntervalMs?: number
}

const DEFAULT_POLL_MS = 5_000

/**
 * Register the stored-Session watcher.
 * @param ctx - plugin context carrying the Session query and controller services.
 * @param config - plugin configuration.
 */
export function apply(ctx: Context, config: Config = {}): void {
  const pollMs = config.pollIntervalMs ?? DEFAULT_POLL_MS
  const startedAt = Date.now()
  const cancellation = new AbortController()
  /** Identities the first scan observed, or later scans announced or skipped. */
  let known: Set<SessionId> | undefined
  let inflight: Promise<void> | undefined
  let stopped = false
  let warned = false

  const scan = async (): Promise<void> => {
    const records = await ctx.sessionQuery.listSessions(cancellation.signal)
    if (stopped) return
    const visible = records.filter(record => record.header.cwd !== undefined)
    let observed = known
    if (observed === undefined) {
      // A Session that existed before this plugin started belongs to the first
      // list a dashboard requests. One created since may land in this same
      // scan while that first list is already in flight, so it stays fresh and
      // is announced like a later arrival.
      observed = new Set(visible
        .filter(record => record.header.createdAt < startedAt)
        .map(record => record.header.id))
      known = observed
    }
    const fresh = visible
      .map(record => record.header.id)
      .filter(id => !observed.has(id))
    if (fresh.length === 0) return
    // The controller owns the list row's projections and running flag, so ask
    // it for the exact summary a dashboard reload would receive.
    const listed = await ctx.sessionController.list({}, cancellation.signal)
    if (stopped) return
    const summaries = new Map(listed.items.map(item => [item.sessionId, item]))
    for (const id of fresh) {
      observed.add(id)
      const summary = summaries.get(id)
      // A Session this Host already runs was announced by the controller; a
      // corpus record the list omits has no row to add.
      if (summary === undefined || summary.agentAvailable) continue
      ctx.emit('api-session/added', summary)
    }
  }

  const poll = (): void => {
    if (stopped || inflight !== undefined) return
    const task = scan().then(() => { warned = false }, (error: unknown) => {
      // A persistent store failure would otherwise repeat this warning every
      // cadence; the next success re-arms it.
      if (warned) return
      warned = true
      ctx.logger.warn(`session-watch: stored Session scan failed: ${messageOf(error)}`)
    })
    inflight = task
    void task.finally(() => {
      if (inflight === task) inflight = undefined
    })
  }

  // Scan immediately: the first scan also fixes which Sessions predate this
  // plugin, so a Session stored during boot is not lost behind the first list
  // a dashboard requests.
  poll()
  const timer = setInterval(poll, pollMs)
  timer.unref()
  ctx.effect(() => () => {
    stopped = true
    clearInterval(timer)
    cancellation.abort()
    return inflight
  }, 'fork-session-watch.poll')
}

/**
 * Render one failure for a log line whose only consumer is the operator.
 * @param error - the thrown value.
 * @returns its message when it is an Error, or its string form.
 */
function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}
