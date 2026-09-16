/**
 * One owned subscription that follows one append-only file through a tail
 * reader: it drives the reader on watcher notifications and a safety-net poll,
 * accumulates the batches the reader completes, and releases every resource it
 * holds on close. Both the durable-append observer and the live-frame side
 * channel subscribe through this class.
 * @module dsh-session-persistence-jsonl/tail-subscription
 */

import { watch, type FSWatcher } from 'node:fs'
import { dirname } from 'node:path'
import type { SessionId } from '@deepseek-ai/dsh-session'

/** Incremental reader a {@link TailSubscription} drives. */
export interface TailReader<T> {
  /**
   * Anchor at the file's current durable end.
   * @param signal - optional cancellation.
   * @returns false when the file does not exist.
   */
  start(signal?: AbortSignal): Promise<boolean>
  /**
   * Read and decode every byte appended since the previous read.
   * @returns the records the appended bytes completed.
   */
  poll(): Promise<readonly T[]>
}

/** Deployment cadence and ownership callbacks of one tail subscription. */
export interface TailSubscriptionSpec<T> {
  /** Safety-net poll cadence for appends the platform file watcher does not report. */
  readonly pollIntervalMs: number
  /** Optional cancellation that also ends the subscription. */
  readonly signal?: AbortSignal
  /** Called once when this subscription releases itself. */
  readonly release: (subscription: TailSubscription<T>) => void
}

/** One message for a failure whose only consumer is a log line. */
function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

/** One owned observation of one append-only file. */
export class TailSubscription<T> {
  /** Reported batches awaiting the consumer, as one accumulated array. */
  private readonly pending: T[] = []
  private readonly controller = new AbortController()
  private readonly task: Promise<void>
  private watcher: FSWatcher | undefined
  private poller: NodeJS.Timeout | undefined
  private waiter: (() => void) | undefined
  private changeWaiter: (() => void) | undefined
  private failure: Error | undefined
  private finished = false
  private released = false
  private readonly onAbort: () => void

  /**
   * @param path - the file this subscription follows, whose directory is watched.
   * @param reader - the anchored incremental reader.
   * @param spec - watch cadence, cancellation, and the service's registry callback.
   */
  constructor(
    path: string,
    private readonly reader: TailReader<T>,
    private readonly spec: TailSubscriptionSpec<T>,
  ) {
    this.items = { [Symbol.asyncIterator]: () => this.drain() }
    this.onAbort = () => { this.close() }
    spec.signal?.addEventListener('abort', this.onAbort, { once: true })
    this.poller = setInterval(() => { this.notifyChange() }, spec.pollIntervalMs)
    // A pending observation must not keep a shutting-down process alive.
    this.poller.unref()
    try {
      this.watcher = watch(dirname(path), { signal: this.controller.signal, persistent: false }, () => {
        this.notifyChange()
      })
    } catch {
      // The poll interval alone keeps the subscription correct on a platform
      // whose watcher is unavailable (for example an exhausted inotify budget).
      this.watcher = undefined
    }
    this.task = this.observe()
  }

  /** Batches the reader completed, in publication order; ends when the subscription stops. */
  readonly items: AsyncIterable<readonly T[]>

  /**
   * Release the subscription; idempotent.
   */
  close(): void {
    if (this.released) return
    this.released = true
    this.spec.signal?.removeEventListener('abort', this.onAbort)
    if (this.poller !== undefined) clearInterval(this.poller)
    this.poller = undefined
    this.controller.abort()
    this.watcher?.close()
    this.watcher = undefined
    this.spec.release(this)
    this.notifyChange()
    this.notifyItem()
  }

  /**
   * Wait until this subscription stops reading.
   * @returns settlement of the read loop.
   */
  settled(): Promise<void> {
    return this.task
  }

  /** Release this subscription on `await using`. */
  [Symbol.asyncDispose](): Promise<void> {
    this.close()
    return Promise.resolve()
  }

  /** Read appended bytes until the subscription closes, publishing what it decodes. */
  private async observe(): Promise<void> {
    try {
      while (!this.released) {
        const items = await this.reader.poll()
        if (items.length > 0) {
          this.pending.push(...items)
          this.notifyItem()
        }
        await this.waitChange()
      }
    } catch (error: unknown) {
      this.failure = error instanceof Error ? error : new Error(messageOf(error))
    } finally {
      this.finished = true
      this.notifyItem()
    }
  }

  /** Serve accumulated records, then end with the read loop's outcome. */
  private async *drain(): AsyncGenerator<readonly T[]> {
    try {
      for (;;) {
        if (this.pending.length > 0) {
          yield this.pending.splice(0, this.pending.length)
          continue
        }
        if (this.failure !== undefined) throw this.failure
        if (this.finished || this.released) return
        await this.waitItem()
      }
    } finally {
      this.close()
    }
  }

  /** Resolve when the read loop has something to publish, or has stopped. */
  private waitItem(): Promise<void> {
    if (this.pending.length > 0 || this.finished || this.released || this.failure !== undefined) {
      return Promise.resolve()
    }
    return new Promise<void>((resolve) => { this.waiter = resolve })
  }

  /** Release the consumer's wait. */
  private notifyItem(): void {
    const waiter = this.waiter
    this.waiter = undefined
    waiter?.()
  }

  /** Resolve when the file may have changed, or the subscription stops. */
  private waitChange(): Promise<void> {
    if (this.released) return Promise.resolve()
    return new Promise<void>((resolve) => { this.changeWaiter = resolve })
  }

  /** Release the reader loop's wait, called by the watcher, the poll timer, and close. */
  private notifyChange(): void {
    const waiter = this.changeWaiter
    this.changeWaiter = undefined
    waiter?.()
  }
}

/**
 * One owned tail subscription shaped for a seam's subscription interface. A
 * subclass exposes {@link TailSubscriptionHandle.batches} under the name its
 * service definition uses.
 */
export class TailSubscriptionHandle<T> {
  /** Batches the reader completed, in publication order. */
  readonly batches: AsyncIterable<readonly T[]>
  private readonly inner: TailSubscription<T>

  /**
   * @param id - the followed stored session.
   * @param path - the file the reader follows.
   * @param reader - the anchored incremental reader.
   * @param spec - watch cadence and cancellation.
   * @param release - the service's registry callback, called once on release.
   */
  constructor(
    readonly id: SessionId,
    path: string,
    reader: TailReader<T>,
    spec: Omit<TailSubscriptionSpec<T>, 'release'>,
    release: (handle: TailSubscriptionHandle<T>) => void,
  ) {
    this.inner = new TailSubscription(path, reader, { ...spec, release: () => { release(this) } })
    this.batches = this.inner.items
  }

  /** Release the subscription; idempotent. */
  close(): void {
    this.inner.close()
  }

  /** Wait until this subscription stops reading. */
  settled(): Promise<void> {
    return this.inner.settled()
  }

  /** Release this subscription on `await using`. */
  [Symbol.asyncDispose](): Promise<void> {
    return this.inner[Symbol.asyncDispose]()
  }
}

/**
 * Close every live tail subscription and wait for each read loop to settle.
 * @param registry - the owning service's subscription registry.
 * @returns resolution once no observation is still reading.
 */
export async function closeTailSubscriptions<T>(registry: Set<TailSubscriptionHandle<T>>): Promise<void> {
  const live = [...registry]
  for (const handle of live) handle.close()
  await Promise.allSettled(live.map(handle => handle.settled()))
}

/** Constructor of one seam-shaped tail subscription. */
export type TailSubscriptionFactory<T, H extends TailSubscriptionHandle<T>> = new (
  id: SessionId,
  path: string,
  reader: TailReader<T>,
  spec: Omit<TailSubscriptionSpec<T>, 'release'>,
  release: (handle: TailSubscriptionHandle<T>) => void,
) => H

/**
 * Open one seam-shaped tail subscription and register it with its service.
 * @param factory - the seam-shaped subscription class to construct.
 * @param registry - the owning service's subscription registry.
 * @param id - the followed stored session.
 * @param path - the file the reader follows.
 * @param reader - the anchored incremental reader.
 * @param pollIntervalMs - safety-net poll cadence.
 * @param signal - optional cancellation that also ends the subscription.
 * @returns the registered subscription.
 */
export function openTailSubscription<T, H extends TailSubscriptionHandle<T>>(
  factory: TailSubscriptionFactory<T, H>,
  registry: Set<TailSubscriptionHandle<T>>,
  id: SessionId,
  path: string,
  reader: TailReader<T>,
  pollIntervalMs: number,
  signal: AbortSignal | undefined,
): H {
  const handle = new factory(id, path, reader, {
    pollIntervalMs,
    ...signal === undefined ? {} : { signal },
  }, (released) => { registry.delete(released) })
  registry.add(handle)
  return handle
}
