import { describe, expect, it } from 'vitest'
import { Gate } from './gate'
import type { Job, Scheduler } from './scheduler'
import type { TaskContext, TaskFailure } from './types'

/**
 * A scheduler that captures enqueued jobs instead of dispatching them, so a test
 * can drive each released job to its terminal state by hand and observe exactly
 * how many the Gate has let through.
 */
class FakeScheduler implements Scheduler {
  aborted = false
  readonly context: TaskContext = { signal: new AbortController().signal }
  readonly jobs: Job[] = []
  readonly errors: TaskFailure[] = []

  enqueue(job: Job) {
    this.jobs.push(job)
  }

  reportError(failure: TaskFailure) {
    this.errors.push(failure)
  }
}

/** A job whose `run()` blocks until the returned `settle` is called. */
const blockingJob = (onCancel: () => void = () => {}) => {
  let settle!: () => void
  const done = new Promise<void>((resolve) => {
    settle = resolve
  })
  const job: Job = { run: () => done, cancel: onCancel }
  return { job, settle }
}

describe('Gate', () => {
  it('releases at most `limit` jobs into the inner scheduler at once', () => {
    const inner = new FakeScheduler()
    const gate = new Gate(inner, 2)

    const blockers = Array.from({ length: 5 }, () => blockingJob())
    for (const { job } of blockers) gate.enqueue(job)

    expect(inner.jobs).toHaveLength(2) // three held back
  })

  it('releases the next held-back job in FIFO order as running ones complete', async () => {
    const inner = new FakeScheduler()
    const gate = new Gate(inner, 1)

    const order: number[] = []
    const settlers: Array<() => void> = []
    for (let i = 0; i < 4; i++) {
      let settle!: () => void
      gate.enqueue({
        run: () => {
          order.push(i)
          return new Promise<void>((resolve) => {
            settle = resolve
          })
        },
        cancel: () => {},
      })
      // The settler is assigned when the job actually runs, so drive released
      // jobs one at a time.
      settlers.push(() => settle())
    }

    expect(inner.jobs).toHaveLength(1)
    // Start each released job, settle it, then await its completion — which
    // should release the next held-back job.
    for (let i = 0; i < 4; i++) {
      const done = inner.jobs[i].run()
      settlers[i]()
      await done
    }

    expect(order).toEqual([0, 1, 2, 3])
    expect(inner.jobs).toHaveLength(4) // all eventually released, one at a time
  })

  it('does not release a new job while every slot is occupied', async () => {
    const inner = new FakeScheduler()
    const gate = new Gate(inner, 2)

    const a = blockingJob()
    const b = blockingJob()
    const c = blockingJob()
    gate.enqueue(a.job)
    gate.enqueue(b.job)
    gate.enqueue(c.job)

    const runA = inner.jobs[0].run()
    inner.jobs[1].run()
    expect(inner.jobs).toHaveLength(2)

    a.settle()
    await runA
    expect(inner.jobs).toHaveLength(3) // freeing one slot released c
  })

  it('delegates abort state, context, and error reporting to the inner scheduler', () => {
    const inner = new FakeScheduler()
    const gate = new Gate(inner, 1)

    expect(gate.aborted).toBe(false)
    inner.aborted = true
    expect(gate.aborted).toBe(true)
    expect(gate.context).toBe(inner.context)

    const failure = { error: new Error('x'), unit: {} as never, input: 1 }
    gate.reportError(failure)
    expect(inner.errors).toEqual([failure])
  })

  it('cancels every held-back job on forceSeal and leaves released ones alone', () => {
    const inner = new FakeScheduler()
    const gate = new Gate(inner, 1)

    const cancelled: number[] = []
    for (let i = 0; i < 4; i++) gate.enqueue(blockingJob(() => cancelled.push(i)).job)

    expect(inner.jobs).toHaveLength(1) // one released, three held back
    gate.forceSeal()

    expect(cancelled).toEqual([1, 2, 3]) // only the held-back jobs are cancelled
  })

  it('does not release more work after an abort', async () => {
    const inner = new FakeScheduler()
    const gate = new Gate(inner, 1)

    const first = blockingJob()
    gate.enqueue(first.job)
    gate.enqueue(blockingJob().job)
    expect(inner.jobs).toHaveLength(1)

    const run = inner.jobs[0].run()
    inner.aborted = true
    first.settle()
    await run

    expect(inner.jobs).toHaveLength(1) // the held-back job was not released
  })
})
