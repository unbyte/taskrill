import { describe, expect, it } from 'vitest'
import { Runtime } from '../src'
import { deferred, flush, runToIdle } from './e2e-helpers'

describe('e2e: abort and runtime closure', () => {
  it('closes when external code aborts after the workload has become idle', async () => {
    const abortController = new AbortController()
    const runtime = new Runtime({ concurrency: 2, signal: abortController.signal })
    const completed: string[] = []
    const Task = runtime.node<string>(async (input) => void completed.push(input))

    await runToIdle(runtime, () => {
      Task.submit('first')
      Task.submit('second')
    })
    expect(completed).toEqual(['first', 'second'])

    abortController.abort()
    await runtime.closed

    expect(Task.submit('rejected')).toBeUndefined()
  })

  it('allows closure to be awaited after an interrupted handler has already terminated', async () => {
    const abortController = new AbortController()
    const runtime = new Runtime({ concurrency: 1, signal: abortController.signal })
    const started = deferred()
    const failures: unknown[] = []
    const Task = runtime.node(async (_input, { signal }) => {
      started.resolve()
      await new Promise<void>((_resolve, reject) => {
        signal.addEventListener('abort', () => reject(signal.reason), { once: true })
      })
    })
    runtime.on('task:failure', ({ error }) => failures.push(error))

    expect(Task.submit()).toBe(1)
    await started.promise
    abortController.abort('external interruption')
    await flush()

    await expect(runtime.closed).resolves.toBeUndefined()
    expect(failures).toEqual(['external interruption'])
  })

  it('waits for running tasks while cancelling globally queued tasks', async () => {
    const abortController = new AbortController()
    const runtime = new Runtime({ concurrency: 2, signal: abortController.signal })
    const releases = [deferred(), deferred()]
    const bothStarted = deferred()
    const started: number[] = []
    const completed: number[] = []
    const cancelled: number[] = []
    let closed = false
    const Task = runtime.node<number>(async (input) => {
      started.push(input)
      if (started.length === 2) bothStarted.resolve()
      await releases[input].promise
    })
    Task.on('task:complete', ({ input }) => completed.push(input))
    Task.on('task:cancel', ({ input }) => cancelled.push(input))
    void runtime.closed.then(() => {
      closed = true
    })

    expect([Task.submit(0), Task.submit(1), Task.submit(2), Task.submit(3)]).toEqual([1, 2, 3, 4])
    await bothStarted.promise
    abortController.abort()

    expect(Task.submit(4)).toBeUndefined()
    expect(cancelled).toEqual([2, 3])
    await flush()
    expect(closed).toBe(false)

    releases[0].resolve()
    await flush()
    expect(closed).toBe(false)

    releases[1].resolve()
    await runtime.closed
    expect(started).toEqual([0, 1])
    expect(completed).toEqual([0, 1])
    expect(closed).toBe(true)
  })

  it('accounts for tasks buffered behind a node concurrency limit', async () => {
    const abortController = new AbortController()
    const runtime = new Runtime({ concurrency: 4, signal: abortController.signal })
    const started = deferred()
    const release = deferred()
    const completed: number[] = []
    const cancelled: number[] = []
    const Limited = runtime.node<number>(
      async (input) => {
        started.resolve()
        await release.promise
        completed.push(input)
      },
      { concurrency: 1 },
    )
    Limited.on('task:cancel', ({ input }) => cancelled.push(input))

    for (let input = 0; input < 4; input++) Limited.submit(input)
    await started.promise
    abortController.abort()

    expect(cancelled).toEqual([1, 2, 3])
    release.resolve()
    await runtime.closed
    expect(completed).toEqual([0])
  })

  it('finishes the current failure transition before abort cancellation and closure', async () => {
    const abortController = new AbortController()
    const runtime = new Runtime({ concurrency: 1, signal: abortController.signal })
    const order: string[] = []
    const Task = runtime.node<number>(async (input) => {
      if (input === 0) throw new Error('stop')
    })
    Task.on('task:failure', ({ input }) => {
      order.push(`node:failure:${input}`)
      abortController.abort()
    })
    runtime.on('task:failure', ({ input }) => order.push(`runtime:failure:${input}`))
    Task.on('task:cancel', ({ input }) => order.push(`node:cancel:${input}`))
    void runtime.closed.then(() => order.push('closed'))

    Task.submit(0)
    Task.submit(1)
    await runtime.closed

    expect(order).toEqual(['node:failure:0', 'runtime:failure:0', 'node:cancel:1', 'closed'])
  })
})
