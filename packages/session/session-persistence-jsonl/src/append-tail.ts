/**
 * Incremental reader over one append-only JSONL artifact, for following a
 * session another process is writing. It anchors at the artifact's durable end
 * and decodes only the bytes appended past it, so a follower never re-reads a
 * log this process cannot see the writer of.
 *
 * @module dsh-session-persistence-jsonl/append-tail
 */

import type { SessionEvent } from '@deepseek-ai/dsh-session'
import { open } from 'node:fs/promises'
import { AppendedRowDecoder, type JsonlCompression } from './format.ts'
import { FileTailReader, splitCompleteLines } from './tail-reader.ts'
import { createZstdFrameDecoder, decompressZstdFrame, scanZstdFrames } from './zstd.ts'

const ZSTD_MAGIC = 0xFD2FB528
/** Header records are one bounded line; a longer first line is not a session log. */
const HEADER_READ_BYTES = 64 * 1024

/**
 * Follows one artifact's durable appends. The reader holds the next seq it
 * expects, so a writer that resumes an interrupted turn and re-emits durable
 * seqs is absorbed, while a seq ahead of that cursor — bytes this reader never
 * saw — ends the stream instead of leaving a hole.
 */
export class AppendTailReader extends FileTailReader<SessionEvent> {
  /** Appended bytes that do not yet form a complete Zstandard frame. */
  private frameCarry: Buffer = Buffer.alloc(0)
  /** Appended bytes that do not yet form a complete plaintext line. */
  private lineCarry: Buffer = Buffer.alloc(0)
  /** Next seq this stream expects, or `undefined` before its first event. */
  private nextSeq: number | undefined
  private decoder: AppendedRowDecoder | undefined
  private headerRecord: Buffer | undefined

  /**
   * @param path - the artifact this reader follows.
   * @param compression - the artifact's physical encoding.
   */
  constructor(path: string, private readonly compression: JsonlCompression) {
    super(path)
  }

  /**
   * Read the artifact's header record, the one row that must precede appended rows.
   * @param size - current artifact size in bytes.
   * @param signal - optional cancellation for the header read.
   */
  protected async anchor(size: number, signal?: AbortSignal): Promise<void> {
    const header = await this.readHeader(Math.min(size, HEADER_READ_BYTES), signal)
    this.headerRecord = header
    this.decoder = new AppendedRowDecoder(header)
  }

  /** Reset the carries, position, and decoder a replayed prefix invalidates. */
  protected onRestart(): void {
    this.frameCarry = Buffer.alloc(0)
    this.lineCarry = Buffer.alloc(0)
    this.nextSeq = undefined
    // A replayed prefix re-delivers the header row, which only a fresh decoder
    // reads as a header.
    if (this.headerRecord !== undefined) this.decoder = new AppendedRowDecoder(this.headerRecord)
  }

  /**
   * Read the artifact's header record, the one row that must precede appended
   * rows. A Zstandard artifact stores it inside the first compressed frame; a
   * plaintext artifact stores it as its first line.
   */
  private async readHeader(length: number, signal?: AbortSignal): Promise<Buffer> {
    const buffer = Buffer.alloc(length)
    const handle = await open(this.path, 'r')
    try {
      await handle.read(buffer, 0, length, 0)
    } finally {
      await handle.close()
    }
    signal?.throwIfAborted()
    const plaintext = this.compression === 'zstd'
      ? await this.firstFramePlaintext(buffer)
      : buffer
    const newline = plaintext.indexOf(0x0A)
    if (newline === -1) throw new Error('empty or header-less session log')
    return Buffer.from(plaintext.subarray(0, newline + 1))
  }

  /**
   * Decompress the first complete Zstandard frame in a bounded artifact prefix.
   * @param buffer - bytes read from the artifact's start.
   * @returns the first frame's plaintext.
   */
  private async firstFramePlaintext(buffer: Buffer): Promise<Buffer> {
    const first = scanZstdFrames(buffer, 1).frames[0]
    if (first === undefined) throw new Error('empty or header-less session log')
    return decompressZstdFrame(buffer.subarray(first.start, first.end))
  }

  /**
   * Decode one appended chunk.
   * @param chunk - bytes appended since the previous read.
   * @param startedAtFileHead - whether this chunk starts at byte 0 of the artifact.
   * @returns the events the chunk completed.
   */
  protected decode(chunk: Buffer, startedAtFileHead: boolean): readonly SessionEvent[] {
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
    const { lines, carry } = splitCompleteLines(buffer)
    const events: SessionEvent[] = []
    let firstLine = true
    for (const line of lines) {
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
    if (retainTail) {
      this.lineCarry = carry
    } else if (carry.length > 0) {
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
