/**
 * Fork-only plugin: publishes each root Agent's live Assistant frames to the
 * `ctx.sessionStreams` side channel beside its Session artifact, so a Host that
 * did not run the Agent can follow its tokens. It listens to the upstream
 * `agent/assistant-stream` and `agent/disposed` events and edits no upstream file.
 * @module @phamhung075/dsh-fork-live-sync
 */

import type { Context } from '@deepseek-ai/cordis'
import type { Agent } from '@deepseek-ai/dsh-agent'
import type { SessionStreamSink } from '@deepseek-ai/dsh-session-persistence'
import type { JsonValue } from '@deepseek-ai/dsh-util-values'

export const name = 'fork-live-sync'

/** Plugin configuration. */
export interface Config {
  /** Also publish frames of subagents; by default only root Agents publish. */
  publishSubagents?: boolean
}

/**
 * Register the frame publisher.
 * @param ctx - plugin context; `sessionStreams` is optional, and without it the plugin is silent.
 * @param config - plugin configuration.
 */
export function apply(ctx: Context, config: Config = {}): void {
  const sinks = new Map<Agent, SessionStreamSink>()

  ctx.on('agent/assistant-stream', ({ agent, frame }) => {
    const streams = ctx.get('sessionStreams')
    if (streams === undefined) return
    if (config.publishSubagents !== true && agent.session.header.origin === 'subagent') return
    let sink = sinks.get(agent)
    if (sink === undefined) {
      sink = streams.publish(agent.session.id)
      sinks.set(agent, sink)
    }
    // The loop's frame vocabulary is JSON, but `JsonValue`'s object member is an
    // index signature no typed interface satisfies; the channel serializes the value.
    sink.append(agent.session.seq, frame as unknown as JsonValue)
  })

  ctx.on('agent/disposed', ({ agent }) => {
    const sink = sinks.get(agent)
    if (sink === undefined) return
    sinks.delete(agent)
    sink.close().catch((error: unknown) => {
      ctx.logger.warn(`live-frame channel close failed: ${String(error)}`)
    })
  })

  ctx.effect(() => async () => {
    const open = [...sinks.values()]
    sinks.clear()
    await Promise.allSettled(open.map(sink => sink.close()))
  })
}
