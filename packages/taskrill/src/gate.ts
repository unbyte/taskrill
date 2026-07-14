import type { Job, Scheduler } from './scheduler'
import type { TaskFailure } from './types'

/**
 * A {@link Scheduler} decorator that gives one group its own concurrency limit
 * on top of a shared scheduler. A group created with its own `concurrency`
 * enqueues through a Gate: it admits at most `limit` of that group's jobs to the
 * underlying scheduler at once, so the group never occupies more than `limit` of
 * the Runtime's slots and cannot starve other units. Abort state, shared
 * context, and error reporting pass straight through, so a unit sees the same
 * {@link Scheduler} contract with or without one.
 *
 * On abort a Gate cancels the jobs it is still holding back, so every submitted
 * task reaches a terminal state even if it never reached the underlying
 * scheduler.
 */
export class Gate implements Scheduler {
  private readonly buffer: Job[] = []
  private head = 0
  private running = 0

  constructor(
    private readonly inner: Scheduler,
    private readonly limit: number,
  ) {}

  get aborted() {
    return this.inner.aborted
  }

  get context() {
    return this.inner.context
  }

  reportError(failure: TaskFailure) {
    this.inner.reportError(failure)
  }

  enqueue(job: Job) {
    this.buffer.push(job)
    this.pump()
  }

  /** Abort teardown: cancels the jobs still held back so the group can settle. */
  forceSeal() {
    if (this.head >= this.buffer.length) return
    const dropped = this.buffer.slice(this.head)
    this.buffer.length = 0
    this.head = 0
    for (const job of dropped) job.cancel()
  }

  private pump() {
    while (this.running < this.limit && this.head < this.buffer.length) {
      const job = this.buffer[this.head]
      // Release the reference so a drained backlog can be collected.
      this.buffer[this.head] = undefined as unknown as Job
      this.head++
      this.running++
      this.inner.enqueue(this.wrap(job))
    }
    this.compact()
  }

  private compact() {
    if (this.head === 0) return
    if (this.head >= this.buffer.length) {
      this.buffer.length = 0
      this.head = 0
    } else if (this.head > 1024) {
      this.buffer.splice(0, this.head)
      this.head = 0
    }
  }

  /** Wraps a job so finishing frees a slot and admits the next held-back one. */
  private wrap(job: Job): Job {
    return {
      run: () =>
        job.run().finally(() => {
          this.running--
          if (!this.inner.aborted) this.pump()
        }),
      // A job cancelled without running keeps its slot; that only happens on
      // abort, where nothing more is admitted anyway.
      cancel: () => job.cancel(),
    }
  }
}
