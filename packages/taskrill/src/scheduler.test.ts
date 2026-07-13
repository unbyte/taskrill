import { describe, expect, it } from 'vitest'
import { type Job, SchedulerImpl } from './scheduler'
import type { Unit } from './types'

/** Resolves after the entire pending microtask cascade has drained. */
const flush = () => new Promise((resolve) => setTimeout(resolve, 0))

const noopJob = (run: () => Promise<void> | void, cancel: () => void = () => {}): Job => ({
  run: () => Promise.resolve(run()),
  cancel,
})

describe('SchedulerImpl', () => {
  it('never invokes a job inline during enqueue', () => {
    const s = new SchedulerImpl(1)
    let ran = false
    s.enqueue(
      noopJob(() => {
        ran = true
      }),
    )
    expect(ran).toBe(false)
  })

  it('dispatches in FIFO order', async () => {
    const s = new SchedulerImpl(1)
    const order: number[] = []
    for (let i = 0; i < 6; i++) s.enqueue(noopJob(() => void order.push(i)))
    await flush()
    expect(order).toEqual([0, 1, 2, 3, 4, 5])
  })

  it('never exceeds the configured concurrency', async () => {
    const concurrency = 3
    const total = 12
    const s = new SchedulerImpl(concurrency)
    let active = 0
    let peak = 0
    let completed = 0
    const release: Array<() => void> = []

    for (let i = 0; i < total; i++) {
      s.enqueue({
        run: () => {
          active++
          peak = Math.max(peak, active)
          return new Promise<void>((resolve) => {
            release.push(() => {
              active--
              completed++
              resolve()
            })
          })
        },
        cancel: () => {},
      })
    }

    await flush()
    expect(active).toBe(concurrency)

    for (let i = 0; i < total; i++) {
      expect(release.length).toBeGreaterThan(0)
      release.shift()?.()
      await flush()
    }

    expect(completed).toBe(total)
    expect(peak).toBe(concurrency)
  })

  it('runs everything under Infinity concurrency', async () => {
    const s = new SchedulerImpl(Number.POSITIVE_INFINITY)
    let active = 0
    let peak = 0
    let completed = 0
    const release: Array<() => void> = []
    const total = 50

    for (let i = 0; i < total; i++) {
      s.enqueue({
        run: () => {
          active++
          peak = Math.max(peak, active)
          return new Promise<void>((resolve) =>
            release.push(() => {
              active--
              completed++
              resolve()
            }),
          )
        },
        cancel: () => {},
      })
    }

    await flush()
    expect(peak).toBe(total) // every job started at once
    for (const r of release) r()
    await flush()
    expect(completed).toBe(total)
  })

  it('drops queued jobs without running them when the signal aborts', async () => {
    const ac = new AbortController()
    const s = new SchedulerImpl(1, ac.signal)
    const ran: number[] = []
    const cancelled: number[] = []
    let releaseFirst!: () => void
    const firstDone = new Promise<void>((resolve) => {
      releaseFirst = resolve
    })

    s.enqueue({
      run: () => {
        ran.push(0)
        return firstDone
      },
      cancel: () => cancelled.push(0),
    })
    for (let i = 1; i < 4; i++) {
      s.enqueue({
        run: async () => void ran.push(i),
        cancel: () => cancelled.push(i),
      })
    }

    await flush()
    expect(ran).toEqual([0]) // only the first fits the single slot

    ac.abort()
    releaseFirst()
    await flush()

    expect(ran).toEqual([0]) // queued jobs never ran
    expect(cancelled).toEqual([1, 2, 3])
  })

  it('remains stack-safe under a large backlog', async () => {
    const s = new SchedulerImpl(Number.POSITIVE_INFINITY)
    let count = 0
    const n = 100_000
    for (let i = 0; i < n; i++) s.enqueue(noopJob(() => void count++))
    await flush()
    expect(count).toBe(n)
  })

  it('re-throws a thrown onError on a fresh microtask without corrupting bookkeeping', () => {
    const boom = new Error('observer boom')
    const s = new SchedulerImpl(1, undefined, () => {
      throw boom
    })
    const deferred: Array<() => void> = []
    const real = globalThis.queueMicrotask
    globalThis.queueMicrotask = (cb: () => void) => void deferred.push(cb)
    try {
      s.reportError({ error: new Error('task'), unit: {} as unknown as Unit, input: undefined })
      expect(deferred).toHaveLength(1)
      expect(() => deferred[0]()).toThrow(boom)
    } finally {
      globalThis.queueMicrotask = real
    }
  })
})
