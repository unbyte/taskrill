import type { TaskContext, TaskFailure } from './types'

/**
 * A unit of executable work handed to the {@link SchedulerImpl}. `run()` never
 * rejects — it captures handler success and failure internally and resolves
 * once the task's bookkeeping is complete. `cancel()` terminalizes a task that
 * never started (dropped from the queue on abort).
 */
export interface Job {
  run(): Promise<void>
  cancel(): void
}

/**
 * The scheduler surface a unit depends on: somewhere to enqueue its jobs and
 * report failures, plus the abort state and shared context it needs while
 * running. A unit is coupled to this narrow view, never to {@link SchedulerImpl}.
 */
export interface Scheduler {
  /** True once the observed signal has aborted. */
  readonly aborted: boolean
  /** The shared context handed to every handler (carries the signal). */
  readonly context: TaskContext
  enqueue(job: Job): void
  reportError(failure: TaskFailure): void
}

/** A signal that never aborts, used for the context when the scheduler has none. */
const NEVER_ABORT: AbortSignal = new AbortController().signal

/**
 * A queue-driven, FIFO, stack-safe dispatcher enforcing a single global
 * concurrency limit, and the owner of the abort lifecycle. `enqueue()` never
 * invokes a job inline — dispatch is always deferred to a microtask, so a
 * handler can never run within the `submit()` that scheduled it.
 *
 * When the observed signal aborts, every tracked unit is force-sealed before
 * the queue drains, and every not-yet-started task terminalizes as cancelled.
 */
export class SchedulerImpl implements Scheduler {
  readonly context: TaskContext

  private readonly concurrency: number
  private readonly signal?: AbortSignal
  private readonly onError?: (failure: TaskFailure) => void
  private readonly units = new Set<{ forceSeal(): void }>()
  private readonly queue: Job[] = []
  private head = 0
  private running = 0
  private pumpScheduled = false

  constructor(concurrency: number, signal?: AbortSignal, onError?: (failure: TaskFailure) => void) {
    if (concurrency !== Number.POSITIVE_INFINITY && !(Number.isInteger(concurrency) && concurrency >= 1)) {
      throw new RangeError(`concurrency must be an integer >= 1 or Infinity, got ${concurrency}`)
    }
    this.concurrency = concurrency
    this.signal = signal
    this.onError = onError
    this.context = { signal: signal ?? NEVER_ABORT }

    if (signal && !signal.aborted) {
      signal.addEventListener('abort', () => this.onAbort(), { once: true })
    }
  }

  get aborted() {
    return this.signal?.aborted ?? false
  }

  /** Registers a unit to be force-sealed if the signal aborts. */
  track(unit: { forceSeal(): void }): void {
    this.units.add(unit)
  }

  enqueue(job: Job) {
    this.queue.push(job)
    this.schedulePump()
  }

  reportError(failure: TaskFailure) {
    if (!this.onError) return
    try {
      this.onError(failure)
    } catch (error) {
      // If onError itself throws, re-throw on a fresh microtask so the bug
      // surfaces loudly without disturbing scheduler bookkeeping.
      queueMicrotask(() => {
        throw error
      })
    }
  }

  private onAbort() {
    // Force-seal every tracked unit before draining, so a pending -> 0
    // transition during teardown settles the unit rather than firing onIdle.
    for (const unit of this.units) unit.forceSeal()
    // Every not-yet-started task terminalizes as cancelled.
    this.cancelAll()
  }

  /** Drops every not-yet-started job, terminalizing each as cancelled. */
  private cancelAll() {
    if (this.head >= this.queue.length) return
    const dropped = this.queue.slice(this.head)
    this.queue.length = 0
    this.head = 0
    for (const job of dropped) job.cancel()
  }

  private schedulePump() {
    if (this.pumpScheduled) return
    this.pumpScheduled = true
    queueMicrotask(() => {
      this.pumpScheduled = false
      this.pump()
    })
  }

  private pump() {
    while (this.running < this.concurrency && this.head < this.queue.length) {
      const job = this.queue[this.head]
      // Release the reference so a large drained backlog can be collected.
      this.queue[this.head] = undefined as unknown as Job
      this.head++
      this.dispatch(job)
    }
    this.compact()
  }

  private compact() {
    if (this.head === 0) return
    if (this.head >= this.queue.length) {
      this.queue.length = 0
      this.head = 0
    } else if (this.head > 1024) {
      this.queue.splice(0, this.head)
      this.head = 0
    }
  }

  private dispatch(job: Job) {
    this.running++
    job.run().finally(() => {
      this.running--
      this.schedulePump()
    })
  }
}
