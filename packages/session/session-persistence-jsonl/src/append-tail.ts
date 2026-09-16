/**
 * Incremental reader over one append-only JSONL artifact, for following a
 * session another process is writing. It anchors at the artifact's durable end
 * and decodes only the bytes appended past it, so a follower never re-reads a
 * log this process cannot see the writer of.
 *
 * @module dsh-session-persistence-jsonl/append-tail
 */

import type { SessionEvent } from '@deepseek-ai/dsh-session'
import { open, stat } from 'node:fs/promises'
import { AppendedRowDecoder, type JsonlCompression } from './format.ts'
import { createZstdFrameDecoder, scanZstdFrames } from './zstd.ts'

const ZSTD_MAGIC = 0xFD2FB528
const NEWLINE = 0x0A
/** Header records are one bounded line; a longer first line is not a session log. */
const HEADER_READ_BYTES = 64 * 1024

/** Whether a filesystem error means absence. */
function isAbsent(error: unknown): boolean {
  return (error as NodeJS.ErrnoException | null)?.code === 'ENOENT'
}

/**
 * Follows one artifact's durable appends. The reader holds the next seq it
 * expects, so a writer that resumes an interrupted turn and re-emits durable
 * seqs is absorbed, while a seq ahead of that cursor — bytes this reader never
 * saw — ends the stream instead of leaving a hole.
 */
export class AppendTailReader {
  /** Bytes of the artifact this reader has consumed, whether or not they decoded. */
  private consumed = 0
  /** Appended bytes that do not yet form a complete Zstandard frame. */
  private frameCarry = Buffer.alloc(0)
  /** Appended bytes that do not yet form a complete plaintext line. */
  private lineCarry = Buffer.alloc(0)
  /** Artifact identity, so a replaced file restarts the reader instead of splicing two logs. */
  private identity: { readonly dev: bigint; readonly ino: bigint } | undefined
  /** Whether one alignment repair already ran; a second unreadable boundary is corruption. */
  private realigned = false
  /** Next seq this stream expects, or `undefined` before its first event. */
  private nextSeq: number | undefined
  private decoder: AppendedRowDecoder | undefined
  private headerRecord: Buffer | undefined

  /**
   * @param path - the artifact this reader follows.
   * @param compression - the artifact's physical encoding.
   */
  constructor(
    private readonly path: string,
    private readonly compression: JsonlCompression,
  ) {}

  /**
   * Anchor the reader at the artifact's current durable end.
   * @param signal - optional cancellation for the header read.
   * @returns false when the artifact does not exist.
   */
  async start(signal?: AbortSignal): Promise<boolean> {
    signal?.throwIfAborted()
    let info
    try {
      info = await stat(this.path, { bigint: true })
    } catch (error: unknown) {
      if (isAbsent(error)) return false
      throw error
    }
    const header = await this.readHeader(Math.min(Number(info.size), HEADER_READ_BYTES), signal)
    this.headerRecord = header
    this.decoder = new AppendedRowDecoder(header)
    this.identity = { dev: info.dev, ino: info.ino }
    this.consumed = Number(info.size)
    return true
  }

  /**
   * Read every byte appended since the previous read and decode it.
   * @returns complete logical events, in seq order and contiguous within this stream.
   */
  async poll(): Promise<readonly SessionEvent[]> {
    let info
    try {
      info = await stat(this.path, { bigint: true })
    } catch (error: unknown) {
      // A missing artifact yields nothing rather than ending the stream: a
      // rewrite publishes a new file under the same path.
      if (isAbsent(error)) return []
      throw error
    }
    const size = Number(info.size)
    if (this.identity === undefined
      || this.identity.dev !== info.dev
      || this.identity.ino !== info.ino
      || size < this.consumed) {
      // A repair truncated the artifact, or another writer replaced it. Only a
      // restart keeps the stream honest: replaying from byte 0 may repeat events
      // a consumer already saw, which its own cursor absorbs.
      this.restart()
      this.identity = { dev: info.dev, ino: info.ino }
    }
    if (size <= this.consumed) return []
    const length = size - this.consumed
    const chunk = Buffer.alloc(length)
    const handle = await open(this.path, 'r')
    try {
      await handle.read(chunk, 0, length, this.consumed)
    } finally {
      await handle.close()
    }
    const startedAtFileHead = this.consumed === 0
    this.consumed = size
    return this.decode(chunk, startedAtFileHead)
  }

  /** Read the artifact's header record, the one row that must precede appended rows. */
  private async readHeader(length: number, signal?: AbortSignal): Promise<Buffer> {
    const buffer = Buffer.alloc(length)
    const handle = await open(this.path, 'r')
    try {
      await handle.read(buffer, 0, length, 0)
    } finally {
      await handle.close()
    }
    signal?.throwIfAborted()
    const newline = buffer.indexOf(NEWLINE)
    if (newline === -1) throw new Error('empty or header-less session log')
    return Buffer.from(buffer.subarray(0, newline + 1))
  }

  /** Discard the reader's position so the next poll reads the artifact from its start. */
  private restart(): void {
    this.consumed = 0
    this.frameCarry = Buffer.alloc(0)
    this.lineCarry = Buffer.alloc(0)
    this.realigned = true
    this.nextSeq = undefined
    // A replayed prefix re-delivers the header row, which only a fresh decoder
    // reads as a header.
    if (this.headerRecord !== undefined) this.decoder = new AppendedRowDecoder(this.headerRecord)
  }

  /**
   * Decode one appended chunk.
   * @param chunk - bytes appended since the previous read.
   * @param startedAtFileHead - whether this chunk starts at byte 0 of the artifact.
   * @returns the events the chunk completed.
   */
  private decode(chunk: Buffer, startedAtFileHead: boolean): readonly SessionEvent[] {
    return this.compression === 'zstd'
      ? this.decodeFrames(chunk)
      : this.decodeLines(this.join(this.lineCarry, chunk), startedAtFileHead, true)
  }

  /**
   * Decode the complete Zstandard frames in one appended chunk.
   * @param chunk - bytes appended since the previous read.
   * @returns the events the complete frames carry.
   */
  private decodeFrames(chunk: Buffer): readonly SessionEvent[] {
    const buffer = this.join(this.frameCarry, chunk)
    if (buffer.length >= 4 && buffer.readUInt32LE(0) !== ZSTD_MAGIC) {
      // The subscription opened inside a frame the writer later truncated and
      // rewrote. Realigning costs one full replay; guessing would misparse.
      if (!this.realigned) {
        this.restart()
        return []
      }
      throw new Error('appended bytes do not begin a Zstandard frame')
    }
    const { frames } = scanZstdFrames(buffer)
    if (frames.length === 0) {
      this.frameCarry = Buffer.from(buffer)
      return []
    }
    const decoder = createZstdFrameDecoder()
    const events: SessionEvent[] = []
    try {
      for (const plaintext of decoder.decode(buffer, frames)) {
        events.push(...this.decodeLines(plaintext, false, false))
      }
    } finally {
      decoder.close()
    }
    const committed = (frames.at(-1) as { end: number }).end
    this.frameCarry = Buffer.from(buffer.subarray(committed))
    return this.collect(events)
  }

  /**
   * Decode the complete lines in one plaintext chunk, retaining its last partial line.
   * @param buffer - plaintext lines to decode.
   * @param startedAtFileHead - whether the buffer starts at byte 0 of the artifact.
   * @param retainTail - whether an incomplete trailing line is this reader's carry (plaintext artifacts) or corruption (a complete frame).
   * @returns the events the complete lines carry.
   */
  private decodeLines(buffer: Buffer, startedAtFileHead: boolean, retainTail: boolean): readonly SessionEvent[] {
    const events: SessionEvent[] = []
    let start = 0
    let firstLine = true
    for (;;) {
      const newline = buffer.indexOf(NEWLINE, start)
      if (newline === -1) break
      const line = buffer.subarray(start, newline)
      start = newline + 1
      if (line.length > 0) {
        try {
          events.push(...this.rowDecoder().decode(line))
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
    const tail = buffer.subarray(start)
    if (retainTail) {
      this.lineCarry = Buffer.from(tail)
    } else if (tail.length > 0) {
      throw new Error('a complete frame ends inside a stored record')
    }
    return retainTail ? this.collect(events) : events
  }

  /** The decoder built from the header this reader anchored on. */
  private rowDecoder(): AppendedRowDecoder {
    /* v8 ignore next -- start() builds the decoder before any poll decodes a row. */
    if (this.decoder === undefined) throw new Error('append reader used before start')
    return this.decoder
  }

  /**
   * Validate one decoded batch against this stream's position and advance it.
   * @param events - events decoded from appended bytes, in log order.
   * @returns the events at or after this stream's next seq, in log order.
   */
  private collect(events: readonly SessionEvent[]): readonly SessionEvent[] {
    const reported: SessionEvent[] = []
    for (const event of events) {
      if (this.nextSeq !== undefined) {
        if (event.seq < this.nextSeq) continue
        if (event.seq !== this.nextSeq) {
          throw new Error(`durable append stream skipped seq ${String(this.nextSeq)} (got ${String(event.seq)})`)
        }
      }
      this.nextSeq = event.seq + 1
      reported.push(event)
    }
    return reported
  }

  /** Concatenate a retained prefix with new bytes, copying only when both are present. */
  private join(carry: Buffer, chunk: Buffer): Buffer {
    return carry.length === 0 ? chunk : Buffer.concat([carry, chunk])
  }
}
