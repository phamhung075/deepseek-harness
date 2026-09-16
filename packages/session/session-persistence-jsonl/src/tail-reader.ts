/**
 * Shared skeleton of an incremental reader over one append-only file: it anchors
 * at the file's current durable end, holds the byte offset it has consumed,
 * reads only bytes appended past it, and restarts from byte zero when the file
 * is truncated or replaced. A subclass decodes the bytes it is handed and resets
 * its own state on a restart.
 * @module dsh-session-persistence-jsonl/tail-reader
 */

import { open, stat } from 'node:fs/promises'

const NEWLINE = 0x0A

/**
 * Whether a filesystem error means absence.
 * @param error - the caught failure value.
 * @returns true for the platform's "no such file" error.
 */
export function isAbsent(error: unknown): boolean {
  return (error as NodeJS.ErrnoException | null)?.code === 'ENOENT'
}

/**
 * Split complete newline-terminated records out of one buffer.
 * @param buffer - the bytes to split.
 * @returns the complete lines without their newlines, and the incomplete trailing bytes.
 */
export function splitCompleteLines(buffer: Buffer): {
  readonly lines: readonly Buffer[]
  readonly carry: Buffer
} {
  const lines: Buffer[] = []
  let start = 0
  for (;;) {
    const newline = buffer.indexOf(NEWLINE, start)
    if (newline === -1) break
    lines.push(buffer.subarray(start, newline))
    start = newline + 1
  }
  return { lines, carry: Buffer.from(buffer.subarray(start)) }
}

/**
 * Follows one append-only file. The reader holds the byte offset it has
 * consumed, so a poll decodes only what a writer appended past it.
 */
export abstract class FileTailReader<T> {
  /** Bytes of the file this reader has consumed, whether or not they decoded. */
  private consumed = 0
  /** File identity, so a replaced file restarts the reader instead of splicing two files. */
  private identity: { readonly dev: bigint; readonly ino: bigint } | undefined
  /** Whether one alignment repair already ran; a second unreadable boundary is corruption. */
  protected realigned = false

  /**
   * @param path - the file this reader follows.
   */
  constructor(protected readonly path: string) {}

  /**
   * Anchor the reader at the file's current durable end.
   * @param signal - optional cancellation for the anchor read.
   * @returns false when the file does not exist.
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
    await this.anchor(Number(info.size), signal)
    this.identity = { dev: info.dev, ino: info.ino }
    this.consumed = Number(info.size)
    return true
  }

  /**
   * Read every byte appended since the previous read and decode it.
   * @returns the records the appended bytes completed.
   */
  async poll(): Promise<readonly T[]> {
    let info
    try {
      info = await stat(this.path, { bigint: true })
    } catch (error: unknown) {
      // A missing file yields nothing rather than ending the stream: a writer
      // publishes a new file under the same path.
      if (isAbsent(error)) return []
      throw error
    }
    const size = Number(info.size)
    if (this.identity === undefined
      || this.identity.dev !== info.dev
      || this.identity.ino !== info.ino
      || size < this.consumed) {
      // A repair truncated the file, or another writer replaced it. Only a
      // restart keeps the stream honest: replaying from byte zero may repeat
      // records a consumer already saw, which its own cursor absorbs.
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

  /** Discard the reader's position so the next poll reads the file from its start. */
  protected restart(): void {
    this.consumed = 0
    this.realigned = true
    this.onRestart()
  }

  /**
   * Read whatever this reader anchors on before it holds a position.
   * @param size - current file size in bytes.
   * @param signal - optional cancellation.
   */
  protected abstract anchor(size: number, signal?: AbortSignal): Promise<void>

  /**
   * Decode one appended chunk.
   * @param chunk - bytes appended since the previous read.
   * @param startedAtFileHead - whether this chunk starts at byte zero of the file.
   * @returns the records the chunk completed.
   */
  protected abstract decode(chunk: Buffer, startedAtFileHead: boolean): readonly T[]

  /** Reset subclass state after a restart. */
  protected abstract onRestart(): void
}
