/**
 * The ACP profile publishes each owned Session's live Assistant frames to the
 * side channel beside that Session's artifact, so a Host that did not run the
 * Agent can follow its tokens. Publication is per Session and can be disabled.
 */

import { PROTOCOL_VERSION } from '@agentclientprotocol/sdk'
import { mkdtemp, readdir, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { makeBridgeHarness, textResponse, type BridgeHarness } from './harness.ts'

let harness: BridgeHarness | undefined
let roots: string[] = []

afterEach(async () => {
  await harness?.dispose()
  harness = undefined
  for (const root of roots) await rm(root, { recursive: true, force: true })
  roots = []
})

async function freshRoot(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'dsh-acp-stream-'))
  roots.push(root)
  return root
}

/** Every live-frame side channel below one persistence root. */
async function findChannels(root: string): Promise<string[]> {
  const found: string[] = []
  const walk = async (dir: string): Promise<void> => {
    let entries
    try {
      entries = await readdir(dir, { withFileTypes: true })
    } catch {
      return
    }
    for (const entry of entries) {
      const path = join(dir, entry.name)
      if (entry.isDirectory()) await walk(path)
      else if (entry.name.endsWith('.stream.jsonl')) found.push(path)
    }
  }
  await walk(root)
  return found
}

interface StoredFrame {
  readonly seq: number
  readonly frame: { readonly type?: string }
}

async function readFrames(path: string): Promise<StoredFrame[]> {
  const text = await readFile(path, 'utf8')
  return text.split('\n').filter(line => line.length > 0).map(line => JSON.parse(line) as StoredFrame)
}

describe('ACP live-frame publication', () => {
  it('publishes a prompted session\'s frames beside its artifact', async () => {
    const root = await freshRoot()
    harness = await makeBridgeHarness({ script: [textResponse('hello')], persistenceRoot: root })
    await harness.client.initialize({ protocolVersion: PROTOCOL_VERSION, clientCapabilities: {} })
    const sessionId = (await harness.client.newSession({ cwd: process.cwd(), mcpServers: [] })).sessionId

    await harness.client.prompt({ sessionId, prompt: [{ type: 'text', text: 'go' }] })

    let channels: string[] = []
    await vi.waitFor(async () => {
      channels = await findChannels(root)
      expect(channels).toHaveLength(1)
    })
    const channel = channels[0] as string
    expect(channel.endsWith('.stream.jsonl')).toBe(true)
    await vi.waitFor(async () => {
      const frames = await readFrames(channel)
      expect(frames.at(-1)?.frame.type).toBe('end')
    })
    const frames = await readFrames(channel)
    expect(frames[0]?.frame.type).toBe('start')
    expect(frames.some(entry => entry.frame.type === 'chunk')).toBe(true)
    expect(frames.every(entry => Number.isSafeInteger(entry.seq) && entry.seq >= 0)).toBe(true)
  })

  it('publishes nothing when the profile disables the channel', async () => {
    const root = await freshRoot()
    harness = await makeBridgeHarness({
      script: [textResponse('quiet')],
      persistenceRoot: root,
      config: { publishAssistantStream: false },
    })
    await harness.client.initialize({ protocolVersion: PROTOCOL_VERSION, clientCapabilities: {} })
    const sessionId = (await harness.client.newSession({ cwd: process.cwd(), mcpServers: [] })).sessionId

    await harness.client.prompt({ sessionId, prompt: [{ type: 'text', text: 'go' }] })

    expect(await findChannels(root)).toEqual([])
  })

  it('gives each session its own channel', async () => {
    const root = await freshRoot()
    harness = await makeBridgeHarness({ script: [textResponse('one')], persistenceRoot: root })
    await harness.client.initialize({ protocolVersion: PROTOCOL_VERSION, clientCapabilities: {} })
    const first = (await harness.client.newSession({ cwd: process.cwd(), mcpServers: [] })).sessionId
    await harness.client.newSession({ cwd: process.cwd(), mcpServers: [] })

    await harness.client.prompt({ sessionId: first, prompt: [{ type: 'text', text: 'go' }] })

    let channels: string[] = []
    await vi.waitFor(async () => {
      channels = await findChannels(root)
      expect(channels).toHaveLength(1)
    })
    expect(channels[0]?.includes(first)).toBe(true)
  })
})
