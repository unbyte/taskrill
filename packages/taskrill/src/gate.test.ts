import { describe, expect, it } from 'vitest'
import { Emitter } from './emitter'
import { Gate } from './gate'
import { type Job, Scheduler } from './scheduler'

const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 0))

describe('Gate', () => {
  it('limits one node without reducing the runtime limit for other nodes', async () => {
    const scheduler = new Scheduler(4, undefined, new Emitter())
    const gate = new Gate(scheduler, 1)
    const limitedReleases: Array<() => void> = []
    const freeReleases: Array<() => void> = []
    let limitedActive = 0
    let freeActive = 0

    const blockingJob = (limited: boolean): Job => ({
      run: () => {
        if (limited) limitedActive++
        else freeActive++
        return new Promise<void>((resolve) => {
          const release = () => {
            if (limited) limitedActive--
            else freeActive--
            resolve()
          }
          ;(limited ? limitedReleases : freeReleases).push(release)
        })
      },
      cancel: () => {},
    })

    for (let index = 0; index < 4; index++) {
      scheduler.accept(gate, () => ({ job: blockingJob(true), publish: () => {} }))
      scheduler.accept(scheduler, () => ({ job: blockingJob(false), publish: () => {} }))
    }

    await flush()
    expect(limitedActive).toBe(1)
    expect(freeActive).toBe(3)

    let released = 0
    while (released < 8) {
      const releases = [...limitedReleases.splice(0), ...freeReleases.splice(0)]
      released += releases.length
      for (const release of releases) release()
      await flush()
    }

    expect(limitedActive).toBe(0)
    expect(freeActive).toBe(0)
  })

  it('cancels tasks held behind its limit when the runtime aborts', async () => {
    const abortController = new AbortController()
    const scheduler = new Scheduler(5, abortController.signal, new Emitter())
    const gate = new Gate(scheduler, 2)
    const cancelled: number[] = []
    const started: number[] = []
    let release!: () => void
    const running = new Promise<void>((resolve) => {
      release = resolve
    })

    for (let id = 0; id < 6; id++) {
      scheduler.accept(gate, () => ({
        job: {
          run: () => {
            started.push(id)
            return running
          },
          cancel: () => cancelled.push(id),
        },
        publish: () => {},
      }))
    }

    await flush()
    abortController.abort()
    release()
    await flush()

    expect(started).toEqual([0, 1])
    expect(cancelled).toEqual([2, 3, 4, 5])
  })

  it('cancels a job immediately when it is enqueued after abort', async () => {
    const abortController = new AbortController()
    const scheduler = new Scheduler(1, abortController.signal, new Emitter())
    const gate = new Gate(scheduler, 1)
    let ran = false
    let cancelled = false

    abortController.abort()
    await scheduler.closed
    gate.enqueue({
      run: async () => {
        ran = true
      },
      cancel: () => {
        cancelled = true
      },
    })

    expect(ran).toBe(false)
    expect(cancelled).toBe(true)
  })

  it('cancels jobs forwarded to the scheduler as well as jobs still buffered', async () => {
    const abortController = new AbortController()
    const scheduler = new Scheduler(1, abortController.signal, new Emitter())
    const gate = new Gate(scheduler, 1)
    const cancelled: number[] = []
    let release!: () => void
    const blocker = new Promise<void>((resolve) => {
      release = resolve
    })

    scheduler.accept(scheduler, () => ({
      job: { run: () => blocker, cancel: () => {} },
      publish: () => {},
    }))
    for (const id of [1, 2]) {
      scheduler.accept(gate, () => ({
        job: { run: async () => {}, cancel: () => cancelled.push(id) },
        publish: () => {},
      }))
    }

    await flush()
    abortController.abort()
    release()
    await scheduler.closed

    expect(cancelled).toEqual([2, 1])
  })
})
