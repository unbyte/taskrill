import { FifoQueue } from './fifo'
import type { Job, JobQueue, Scheduler } from './scheduler'
import type { Unsubscribe } from './types'

/** Per-node limiter layered in front of the shared runtime scheduler. */
export class Gate implements JobQueue {
  private readonly buffer = new FifoQueue<Job>()
  private running = 0
  private unregisterAbort?: Unsubscribe

  constructor(
    private readonly inner: Scheduler,
    private readonly limit: number,
  ) {}

  enqueue(job: Job) {
    if (this.inner.aborted) {
      job.cancel()
      return
    }

    this.buffer.push(job)
    this.trackWhileBuffered()
    this.pump()
  }

  abort() {
    const jobs = this.buffer.drain()
    this.stopTracking()
    for (const job of jobs) job.cancel()
  }

  private pump() {
    while (!this.inner.aborted && this.running < this.limit) {
      const job = this.buffer.shift()
      if (!job) break

      this.running++
      this.inner.enqueue(this.wrap(job))
    }

    if (this.buffer.size === 0) this.stopTracking()
  }

  private wrap(job: Job): Job {
    let finished = false
    const finish = () => {
      if (finished) return
      finished = true
      this.running--
      if (!this.inner.aborted) this.pump()
    }

    return {
      run: async () => {
        try {
          await job.run()
        } finally {
          finish()
        }
      },
      cancel: () => {
        try {
          job.cancel()
        } finally {
          finish()
        }
      },
    }
  }

  private trackWhileBuffered() {
    this.unregisterAbort ??= this.inner.registerAbortable(this)
  }

  private stopTracking() {
    this.unregisterAbort?.()
    this.unregisterAbort = undefined
  }
}
