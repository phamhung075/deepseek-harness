/**
 * Live Assistant-frame side channel beside one Session artifact: the plaintext
 * JSONL writer a publishing process appends frames to, and the incremental tail
 * reader another process follows. Plain text keeps a torn writer tail a single
 * incomplete line and avoids Zstandard framing for records that are small and
 * short-lived; the durable Session log stays the only replay authority.
 * @module dsh-session-persistence-jsonl/stream-channel
 */

import { open, stat, truncate } from 'node:fs/promises'
import type { FileHandle } from 'node:fs/promises'
import type { SessionStreamRecord } from '@deepseek-ai/dsh-session-persistence'
import { FileTailReader, isAbsent, splitCompleteLines } from './tail-reader.ts'

const NEWLINE = 0x0A
/** Bytes of the existing tail scanned to find the last complete record boundary. */
const TAIL_SCAN_BYTES = 64 * 1024

/** Deployment policy of one publishing side channel. */
export interface JsonlStreamPolicy {
  /**
   * Maximum records buffered while a write is in flight. A frame arriving at
   * the bound is dropped rather than growing memory without limit; the channel
   * is presentation data and the durable settlement remains the replay source.
   */
  readonly maxPendingRecords: number
  /** Diagnostic sink for a background write failure; the channel stops publishing after one. */
  readonly onError?: (error: unknown) => void
  /** Diagnostic sink called once with the number of records dropped at the queue bound. */
  readonly onDropped?: (count: number) => void
  /** Called once when the sink closes, so its owner can drop its reference. */
  readonly onClose?: () => void
}

/** One message for a failure whose only consumer is a log line. */
function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

/**
 * Derive one artifact's side-channel path: the artifact filename with its
 * `.jsonl` or `.jsonl.zstd` suffix replaced by `.stream.jsonl`. No generation
 * parser accepts that name, so a side file can never be read as the durable log.
 * @param artifactPath - absolute path of a session's generation artifact.
 * @returns the side-channel path beside it.
 * @throws when the artifact path does not carry a JSONL suffix.
 */
export function streamChannelPath(artifactPath: string): string {
  for (const suffix of ['.jsonl.zstd', '.jsonl'] as const) {
    if (artifactPath.endsWith(suffix)) {
      return `${artifactPath.slice(0, -suffix.length)}.stream.jsonl`
    }
  }
  throw new Error(`session artifact path does not carry a JSONL suffix: "${artifactPath}"`)
}

/**
 * Truncate an incomplete trailing record left by a writer killed mid-frame, so
 * the next writer never concatenates a new record onto a partial line. A tail
 * with no newline at all is not a complete record and is discarded.
 * @param path - the side channel to repair.
 */
async function repairStreamTail(path: string): Promise<void> {
  let size: number
  try {
    size = (await stat(path)).size
  } catch (error: unknown) {
    if (isAbsent(error)) return
    throw error
  }
  if (size === 0) return
  const length = Math.min(size, TAIL_SCAN_BYTES)
  const buffer = Buffer.alloc(length)
  const handle = await open(path, 'r')
  let read: number
  try {
    read = (await handle.read(buffer, 0, length, size - length)).bytesRead
  } finally {
    await handle.close()
  }
  const newline = buffer.subarray(0, read).lastIndexOf(NEWLINE)
  const complete = newline === -1 ? size - length : size - length + newline + 1
  if (complete < size) await truncate(path, complete)
}

/**
 * One owned publishing channel onto a Session's side file. Writes are ordered
 * and coalesced behind a single in-flight flush; `close` awaits the last flush
 * and releases the file handle.
 */
export class JsonlStreamSink {
  private path: string | undefined
  private handle: FileHandle | undefined
  private pending: string[] = []
  private flushing: Promise<void> | undefined
  private closed = false
  private failure: Error | undefined
  private dropped = 0

  /**
   * @param resolvePath - resolves the side-channel path, or `undefined` while the Session has no artifact yet.
   * @param policy - queue bound and background-failure diagnostics.
   */
  constructor(
    private readonly resolvePath: () => Promise<string | undefined>,
    private readonly policy: JsonlStreamPolicy,
  ) {}

  /**
   * Queue one frame for publication. A frame arriving at the queue bound, or
   * after a write failure or {@link close}, is dropped.
   * @param seq - the Session's next durable seq.
   * @param frame - the process-local frame to serialize.
   */
  append(seq: number, frame: unknown): void {
    if (this.closed || this.failure !== undefined) return
    if (this.pending.length >= this.policy.maxPendingRecords) {
      this.dropped += 1
      if (this.dropped === 1) this.policy.onDropped?.(1)
      return
    }
    this.pending.push(`${JSON.stringify({ seq, frame })}\n`)
    void this.flush()
  }

  /**
   * Flush queued frames and release the file handle; idempotent.
   * @returns resolution once no write remains in flight.
   */
  async close(): Promise<void> {
    if (this.closed) {
      await this.flushing
      return
    }
    this.closed = true
    await this.flushing
    const handle = this.handle
    this.handle = undefined
    await handle?.close()
    this.policy.onClose?.()
  }

  /** Start one flush unless another is already running. */
  private flush(): Promise<void> {
    if (this.flushing !== undefined) return this.flushing
    this.flushing = this.drain().finally(() => { this.flushing = undefined })
    return this.flushing
  }

  /** Write queued frames in order until none remain, recording the first failure. */
  private async drain(): Promise<void> {
    try {
      while (this.pending.length > 0) {
        const handle = await this.open()
        // The artifact may not exist yet; those frames are dropped, and a later
        // frame retries once the Session has materialized.
        if (handle === undefined) {
          this.pending = []
          return
        }
        const batch = this.pending
        this.pending = []
        await handle.writeFile(batch.join(''))
      }
    } catch (error: unknown) {
      this.failure = error instanceof Error ? error : new Error(messageOf(error))
      this.pending = []
      this.policy.onError?.(this.failure)
    }
  }

  /** Open the append handle once, repairing an incomplete trailing record first. */
  private async open(): Promise<FileHandle | undefined> {
    if (this.handle !== undefined) return this.handle
    this.path ??= await this.resolvePath()
    const path = this.path
    if (path === undefined) return undefined
    await repairStreamTail(path)
    this.handle = await open(path, 'a')
    return this.handle
  }
}

/**
 * Anchored incremental tail of one plaintext side channel. It holds the byte
 * offset it has consumed, parses only complete records appended past it, and
 * retains an incomplete trailing record until the bytes completing it land.
 */
export class StreamTailReader extends FileTailReader<SessionStreamRecord> {
  /** Appended bytes that do not yet form a complete line. */
  private carry: Buffer = Buffer.alloc(0)

  /** The side channel carries no header record to anchor on. */
  protected anchor(): Promise<void> {
    return Promise.resolve()
  }

  /** Drop the incomplete trailing record a restart invalidates. */
  protected onRestart(): void {
    this.carry = Buffer.alloc(0)
  }

  /**
   * Parse the complete records in one appended chunk, retaining its partial trailing line.
   * @param chunk - bytes appended since the previous read.
   * @param startedAtFileHead - whether this chunk starts at byte zero of the channel.
   * @returns the records the complete lines carry.
   */
  protected decode(chunk: Buffer, startedAtFileHead: boolean): readonly SessionStreamRecord[] {
    const buffer = this.carry.length === 0 ? chunk : Buffer.concat([this.carry, chunk])
    const { lines, carry } = splitCompleteLines(buffer)
    this.carry = carry
    const records: SessionStreamRecord[] = []
    let firstLine = true
    for (const line of lines) {
      if (line.length > 0) {
        try {
          records.push(parseStoredFrame(line))
        } catch (error: unknown) {
          if (firstLine && !startedAtFileHead && !this.realigned) {
            this.restart()
            return []
          }
          throw error
        }
      }
      firstLine = false
    }
    return records
  }
}

/**
 * Parse one complete stored line into its record.
 * @param line - one complete line without its trailing newline.
 * @returns the parsed record.
 * @throws when the line is not a JSON object carrying a non-negative integer seq and an object frame.
 */
function parseStoredFrame(line: Buffer): SessionStreamRecord {
  let parsed: unknown
  try {
    parsed = JSON.parse(line.toString('utf8'))
  } catch {
    throw new Error('corrupt live-frame channel: stored line is not valid JSON')
  }
  if (typeof parsed !== 'object' || parsed === null) {
    throw new Error('corrupt live-frame channel: stored line is not a record object')
  }
  const seq = (parsed as { seq?: unknown }).seq
  const frame = (parsed as { frame?: unknown }).frame
  if (typeof seq !== 'number' || !Number.isSafeInteger(seq) || seq < 0) {
    throw new Error('corrupt live-frame channel: stored record has no non-negative integer seq')
  }
  if (typeof frame !== 'object' || frame === null) {
    throw new Error('corrupt live-frame channel: stored record has no frame object')
  }
  // JSON.parse output is a JSON value by construction, which is exactly what the publisher serialized.
  return { seq, frame: frame as SessionStreamRecord['frame'] }
}
