import { Context, Service } from '@deepseek-ai/cordis'
import type { Agent } from '@deepseek-ai/dsh-agent'
import type { SessionStreamSink } from '@deepseek-ai/dsh-session-persistence'
import { describe, expect, it } from 'vitest'
import * as LiveSync from '../src/index.ts'

interface Published { readonly id: string; readonly seq: number; readonly frame: unknown }

function fakeAgent(id: string, seq: number, origin?: 'subagent'): Agent {
  return { session: { id, seq, header: { origin } } } as unknown as Agent
}

async function setup(config: LiveSync.Config = {}, withStreams = true) {
  const ctx = new Context()
  const published: Published[] = []
  const closed: string[] = []
  if (withStreams) {
    class FakeStreams extends Service {
      constructor(c: Context) { super(c, 'sessionStreams') }
      publish(id: string): SessionStreamSink {
        return {
          append: (seq, frame) => { published.push({ id, seq, frame }) },
          close: async () => { closed.push(id) },
        }
      }
    }
    await ctx.plugin(FakeStreams)
  }
  await ctx.plugin(LiveSync, config)
  return { ctx, published, closed }
}

describe('fork live-sync', () => {
  it('publishes a root Agent frame with the Session seq and closes on dispose', async () => {
    const { ctx, published, closed } = await setup()
    const agent = fakeAgent('s1', 7)
    ctx.emit('agent/assistant-stream', { agent, frame: { type: 'start' } } as never)
    ctx.emit('agent/assistant-stream', { agent, frame: { type: 'end' } } as never)
    ctx.emit('agent/disposed', { agent })
    await new Promise(resolve => setImmediate(resolve))
    expect(published.map(p => [p.id, p.seq, p.frame])).toEqual([['s1', 7, { type: 'start' }], ['s1', 7, { type: 'end' }]])
    expect(closed).toEqual(['s1'])
  })

  it('skips subagents unless configured', async () => {
    const quiet = await setup()
    quiet.ctx.emit('agent/assistant-stream', { agent: fakeAgent('c', 0, 'subagent'), frame: {} } as never)
    expect(quiet.published).toEqual([])
    const loud = await setup({ publishSubagents: true })
    loud.ctx.emit('agent/assistant-stream', { agent: fakeAgent('c', 0, 'subagent'), frame: {} } as never)
    expect(loud.published).toHaveLength(1)
  })

  it('is silent without a sessionStreams service', async () => {
    const { ctx, published } = await setup({}, false)
    ctx.emit('agent/assistant-stream', { agent: fakeAgent('s', 0), frame: {} } as never)
    expect(published).toEqual([])
  })
})
