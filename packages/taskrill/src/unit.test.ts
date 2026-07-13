import { afterEach, describe, expect, it } from 'vitest'
import { SealedUnitError } from './errors'
import type { Job, Scheduler } from './scheduler'
import type { TaskContext, TaskFailure } from './types'
import { GroupImpl, SingleImpl } from './unit'

/**
 * A scheduler that captures enqueued jobs instead of dispatching them, so a
 * test can drive each task to its terminal state by hand.
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

const flush = () => new Promise((resolve) => setTimeout(resolve, 0))

describe('SingleImpl', () => {
  it('runs the handler for its one submission and settles all-succeeded', async () => {
    const scheduler = new FakeScheduler()
    const seen: number[] = []
    const single = new SingleImpl<number>(scheduler, 1, undefined, async (x) => void seen.push(x))

    single.submit(5)
    expect(scheduler.jobs).toHaveLength(1)
    await scheduler.jobs[0].run()

    expect(seen).toEqual([5])
    await expect(single.done).resolves.toEqual({ submitted: 1, succeeded: 1, failed: 0, cancelled: 0, ok: true })
  })

  it('derives a default name from kind and id', () => {
    const scheduler = new FakeScheduler()
    expect(new SingleImpl(scheduler, 3, undefined, async () => {}).name).toBe('single#3')
    expect(new SingleImpl(scheduler, 3, 'E', async () => {}).name).toBe('E')
  })

  it('throws SealedUnitError on a second submission', () => {
    const scheduler = new FakeScheduler()
    const single = new SingleImpl<number>(scheduler, 1, undefined, async () => {})
    single.submit(1)
    expect(() => single.submit(2)).toThrow(SealedUnitError)
  })

  it('skip() settles the single empty and enqueues nothing', async () => {
    const scheduler = new FakeScheduler()
    const single = new SingleImpl<void>(scheduler, 1, undefined, async () => {})
    single.skip()
    expect(scheduler.jobs).toHaveLength(0)
    await expect(single.done).resolves.toEqual({ submitted: 0, succeeded: 0, failed: 0, cancelled: 0, ok: true })
  })

  it('treats submit() and skip() as mutually exclusive seals', () => {
    const scheduler = new FakeScheduler()

    const skipped = new SingleImpl<void>(scheduler, 1, undefined, async () => {})
    skipped.skip()
    expect(() => skipped.skip()).not.toThrow() // idempotent
    expect(() => skipped.submit()).toThrow(SealedUnitError)

    const submitted = new SingleImpl<void>(scheduler, 2, undefined, async () => {})
    submitted.submit()
    expect(() => submitted.skip()).toThrow(SealedUnitError)
  })

  it('drops submit() and skip() as silent no-ops after abort', async () => {
    const scheduler = new FakeScheduler()
    scheduler.aborted = true
    const single = new SingleImpl<void>(scheduler, 1, undefined, async () => {})

    expect(() => single.submit()).not.toThrow()
    expect(() => single.skip()).not.toThrow()
    expect(scheduler.jobs).toHaveLength(0)

    let resolved = false
    single.done.then(() => {
      resolved = true
    })
    await flush()
    expect(resolved).toBe(false) // an inert unit's done never resolves
  })

  it('reports a failing task through onError and settles not ok', async () => {
    const scheduler = new FakeScheduler()
    const boom = new Error('boom')
    const single = new SingleImpl<void>(scheduler, 1, undefined, async () => {
      throw boom
    })

    single.submit()
    await scheduler.jobs[0].run()

    await expect(single.done).resolves.toEqual({ submitted: 1, succeeded: 0, failed: 1, cancelled: 0, ok: false })
    expect(scheduler.errors).toHaveLength(1)
    expect(scheduler.errors[0]).toMatchObject({ error: boom, unit: single, input: undefined })
  })

  it('captures a synchronous throw exactly like an async rejection', async () => {
    const scheduler = new FakeScheduler()
    const single = new SingleImpl<void>(scheduler, 1, undefined, () => {
      throw new Error('sync boom')
    })
    single.submit()
    await scheduler.jobs[0].run()
    await expect(single.done).resolves.toMatchObject({ failed: 1, ok: false })
    expect(scheduler.errors).toHaveLength(1)
  })

  it('counts a cancelled task without reporting onError', async () => {
    const scheduler = new FakeScheduler()
    const single = new SingleImpl<void>(scheduler, 1, undefined, async () => {})
    single.submit()

    single.forceSeal()
    scheduler.jobs[0].cancel()

    await expect(single.done).resolves.toEqual({ submitted: 1, succeeded: 0, failed: 0, cancelled: 1, ok: false })
    expect(scheduler.errors).toHaveLength(0)
  })
})

describe('GroupImpl', () => {
  it('settles after seal once every task is terminal', async () => {
    const scheduler = new FakeScheduler()
    const seen: number[] = []
    const group = new GroupImpl<number>(scheduler, 1, undefined, async (x) => void seen.push(x))

    group.submit(1)
    group.submit(2)
    group.submit(3)
    expect(scheduler.jobs).toHaveLength(3)

    group.seal()
    await Promise.all(scheduler.jobs.map((job) => job.run()))

    expect([...seen].sort()).toEqual([1, 2, 3])
    await expect(group.done).resolves.toEqual({ submitted: 3, succeeded: 3, failed: 0, cancelled: 0, ok: true })
  })

  it('settles an empty sealed group immediately', async () => {
    const scheduler = new FakeScheduler()
    const group = new GroupImpl<void>(scheduler, 1, undefined, async () => {})
    group.seal()
    await expect(group.done).resolves.toEqual({ submitted: 0, succeeded: 0, failed: 0, cancelled: 0, ok: true })
  })

  it('is idempotent on seal() and throws on submit() after seal', () => {
    const scheduler = new FakeScheduler()
    const group = new GroupImpl<void>(scheduler, 1, undefined, async () => {})
    group.seal()
    expect(() => group.seal()).not.toThrow()
    expect(() => group.submit()).toThrow(SealedUnitError)
  })

  it('fires onIdle on an open pending -> 0 transition so a self-fed group seals itself', async () => {
    const scheduler = new FakeScheduler()
    let idle = 0
    const group = new GroupImpl<number>(
      scheduler,
      1,
      undefined,
      async () => {},
      (unit) => {
        idle++
        unit.seal()
      },
    )

    group.submit(1)
    await scheduler.jobs[0].run()

    expect(idle).toBe(1)
    await expect(group.done).resolves.toMatchObject({ submitted: 1, ok: true })
  })

  it('fires onIdle multi-shot while open and never once sealed', async () => {
    const scheduler = new FakeScheduler()
    let idle = 0
    const group = new GroupImpl<number>(
      scheduler,
      1,
      undefined,
      async () => {},
      () => void idle++,
    )

    group.submit(1)
    await scheduler.jobs[0].run()
    expect(idle).toBe(1)

    group.submit(2)
    await scheduler.jobs[1].run()
    expect(idle).toBe(2)

    group.seal() // settlement, not an idle transition
    await expect(group.done).resolves.toMatchObject({ submitted: 2 })
    expect(idle).toBe(2)
  })

  it('never fires onIdle for an empty group', async () => {
    const scheduler = new FakeScheduler()
    let idle = 0
    const group = new GroupImpl(
      scheduler,
      1,
      undefined,
      async () => {},
      () => void idle++,
    )
    group.seal()
    await group.done
    expect(idle).toBe(0)
  })

  it('settles rather than firing onIdle when force-sealed before draining', async () => {
    const scheduler = new FakeScheduler()
    let idle = 0
    const group = new GroupImpl<number>(
      scheduler,
      1,
      undefined,
      async () => {},
      () => void idle++,
    )

    group.submit(1)
    group.forceSeal()
    await scheduler.jobs[0].run()

    expect(idle).toBe(0)
    await expect(group.done).resolves.toMatchObject({ submitted: 1, ok: true })
  })
})

describe('onIdle error handling', () => {
  const realQueueMicrotask = globalThis.queueMicrotask

  afterEach(() => {
    globalThis.queueMicrotask = realQueueMicrotask
  })

  it('re-throws a thrown onIdle on a fresh microtask without disturbing settlement', async () => {
    const scheduler = new FakeScheduler()
    const boom = new Error('idle boom')
    const deferred: Array<() => void> = []
    globalThis.queueMicrotask = (cb: () => void) => void deferred.push(cb)

    const group = new GroupImpl<number>(
      scheduler,
      1,
      undefined,
      async () => {},
      () => {
        throw boom
      },
    )
    group.submit(1)
    await scheduler.jobs[0].run() // onIdle throws here, caught and deferred

    expect(deferred).toHaveLength(1)
    expect(() => deferred[0]()).toThrow(boom)

    group.seal()
    await expect(group.done).resolves.toMatchObject({ submitted: 1 })
  })
})
