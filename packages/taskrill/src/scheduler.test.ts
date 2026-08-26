import { describe, expect, it } from 'vitest'
import { Emitter } from './emitter'
import { type Job, Scheduler } from './scheduler'
import type { RuntimeEventMap } from './types'

const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 0))

const accept = (scheduler: Scheduler, job: Job, published: number[] = []) => {
  return scheduler.accept(scheduler, (id) => ({
    job,
    publish: () => published.push(id),
  }))
}

const immediateJob = (run: () => void = () => {}): Job => ({
  run: async () => run(),
  cancel: () => {},
})

describe('Scheduler', () => {
  it('validates global concurrency', () => {
    const events = new Emitter<RuntimeEventMap>()
    expect(() => new Scheduler(0, undefined, events)).toThrow(RangeError)
    expect(() => new Scheduler(-1, undefined, events)).toThrow(RangeError)
    expect(() => new Scheduler(1.5, undefined, events)).toThrow(RangeError)
    expect(() => new Scheduler(Number.NaN, undefined, events)).toThrow(RangeError)
    expect(() => new Scheduler(1, undefined, events)).not.toThrow()
    expect(() => new Scheduler(Number.POSITIVE_INFINITY, undefined, events)).not.toThrow()
  })

  it('publishes task IDs synchronously but dispatches jobs later in FIFO order', async () => {
    const scheduler = new Scheduler(1, undefined, new Emitter())
    const published: number[] = []
    const ran: number[] = []

    const first = accept(
      scheduler,
      immediateJob(() => ran.push(1)),
      published,
    )
    const second = accept(
      scheduler,
      immediateJob(() => ran.push(2)),
      published,
    )

    expect([first, second]).toEqual([1, 2])
    expect(published).toEqual([1, 2])
    expect(ran).toEqual([])

    await flush()
    expect(ran).toEqual([1, 2])
  })

  it('enforces the global concurrency limit', async () => {
    const scheduler = new Scheduler(3, undefined, new Emitter())
    const releases: Array<() => void> = []
    let active = 0
    let peak = 0

    for (let index = 0; index < 9; index++) {
      accept(scheduler, {
        run: () => {
          active++
          peak = Math.max(peak, active)
          return new Promise<void>((resolve) => {
            releases.push(() => {
              active--
              resolve()
            })
          })
        },
        cancel: () => {},
      })
    }

    await flush()
    expect(active).toBe(3)

    while (releases.length > 0) {
      releases.shift()?.()
      await flush()
    }

    expect(peak).toBe(3)
  })

  it('emits idle only after an accepted workload drains', async () => {
    const events = new Emitter<RuntimeEventMap>()
    const scheduler = new Scheduler(1, undefined, events)
    const idle: string[] = []
    events.on('idle', () => idle.push('idle'))

    await flush()
    expect(idle).toEqual([])

    accept(scheduler, immediateJob())
    await flush()
    expect(idle).toEqual(['idle'])
  })

  it('gracefully closes after accepted work drains', async () => {
    const scheduler = new Scheduler(1, undefined, new Emitter())
    const ran: number[] = []

    expect(
      accept(
        scheduler,
        immediateJob(() => ran.push(1)),
      ),
    ).toBe(1)
    expect(
      accept(
        scheduler,
        immediateJob(() => ran.push(2)),
      ),
    ).toBe(2)
    scheduler.close()

    expect(scheduler.accepting).toBe(false)
    expect(
      accept(
        scheduler,
        immediateJob(() => ran.push(3)),
      ),
    ).toBeUndefined()

    await scheduler.closed
    expect(ran).toEqual([1, 2])
  })

  it('cancels queued jobs and suppresses idle after abort', async () => {
    const abortController = new AbortController()
    const events = new Emitter<RuntimeEventMap>()
    const scheduler = new Scheduler(1, abortController.signal, events)
    const cancelled: number[] = []
    const idle: string[] = []
    let release!: () => void
    const running = new Promise<void>((resolve) => {
      release = resolve
    })

    events.on('idle', () => idle.push('idle'))
    accept(scheduler, { run: () => running, cancel: () => cancelled.push(0) })
    for (let id = 1; id < 4; id++) {
      accept(scheduler, { run: async () => {}, cancel: () => cancelled.push(id) })
    }

    await flush()
    abortController.abort()
    release()
    await flush()

    expect(cancelled).toEqual([1, 2, 3])
    expect(idle).toEqual([])
  })

  it('does not create a task and starts closed when constructed with an aborted signal', async () => {
    const abortController = new AbortController()
    abortController.abort()
    const scheduler = new Scheduler(1, abortController.signal, new Emitter())
    let created = false

    const taskId = scheduler.accept(scheduler, () => {
      created = true
      return { job: immediateJob(), publish: () => {} }
    })

    expect(taskId).toBeUndefined()
    expect(created).toBe(false)
    await expect(scheduler.closed).resolves.toBeUndefined()
  })
})
