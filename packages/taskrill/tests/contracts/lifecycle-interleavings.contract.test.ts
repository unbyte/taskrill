import { describe, expect, it } from 'vitest'
import { Runtime } from '../../src'
import { deferred, flush, runToIdle } from '../e2e-helpers'

const expectClosesSoon = async (runtime: Runtime) => {
  const observed = await Promise.race([
    runtime.closed.then(() => 'closed' as const),
    flush().then(() => 'timeout' as const),
  ])
  expect(observed).toBe('closed')
}

describe('contract: unusual lifecycle interleavings', () => {
  it('closes when an empty active runtime is aborted', async () => {
    const abortController = new AbortController()
    const runtime = new Runtime({ concurrency: 1, signal: abortController.signal })

    abortController.abort()

    await expectClosesSoon(runtime)
  })

  it('closes when aborted after several separate idle periods', async () => {
    const abortController = new AbortController()
    const runtime = new Runtime({ concurrency: 1, signal: abortController.signal })
    const completed: number[] = []
    const Task = runtime.node<number>(async (input) => void completed.push(input))

    for (let input = 1; input <= 3; input++) {
      await runToIdle(runtime, () => Task.submit(input))
    }
    abortController.abort()

    await expectClosesSoon(runtime)
    expect(completed).toEqual([1, 2, 3])
  })

  it('closes when an idle listener aborts the runtime', async () => {
    const abortController = new AbortController()
    const runtime = new Runtime({ concurrency: 1, signal: abortController.signal })
    const order: string[] = []
    const Task = runtime.node(async () => {})
    Task.on('task:complete', () => order.push('complete'))
    runtime.on('idle', () => {
      order.push('idle')
      abortController.abort()
    })
    void runtime.closed.then(() => order.push('closed'))

    Task.submit()
    await runtime.closed

    expect(order).toEqual(['complete', 'idle', 'closed'])
  })

  it('allows abort to strengthen a graceful close while accepted work remains', async () => {
    const abortController = new AbortController()
    const runtime = new Runtime({ concurrency: 1, signal: abortController.signal })
    const started = deferred()
    const release = deferred()
    const completed: number[] = []
    const cancelled: number[] = []
    const Task = runtime.node<number>(async (input) => {
      if (input === 0) {
        started.resolve()
        await release.promise
      }
      completed.push(input)
    })
    Task.on('task:cancel', ({ input }) => cancelled.push(input))

    Task.submit(0)
    Task.submit(1)
    await started.promise
    runtime.close()
    abortController.abort()
    release.resolve()
    await runtime.closed

    expect(completed).toEqual([0])
    expect(cancelled).toEqual([1])
  })

  it('treats abort from a start listener as a running task failure', async () => {
    const abortController = new AbortController()
    const runtime = new Runtime({ concurrency: 1, signal: abortController.signal })
    const order: string[] = []
    const Task = runtime.node(async (_input, { signal }) => {
      order.push(`handler:aborted:${signal.aborted}`)
      signal.throwIfAborted()
    })
    Task.on('task:start', () => {
      order.push('start')
      abortController.abort('start interruption')
    })
    Task.on('task:failure', ({ error }) => order.push(`failure:${String(error)}`))
    Task.on('task:cancel', () => order.push('cancel'))

    Task.submit()
    await runtime.closed

    expect(order).toEqual(['start', 'handler:aborted:true', 'failure:start interruption'])
  })

  it('cancels a child submitted immediately before abort in a completion listener', async () => {
    const abortController = new AbortController()
    const runtime = new Runtime({ concurrency: 1, signal: abortController.signal })
    const order: string[] = []
    let childTaskId: number | undefined
    const Child = runtime.node(async () => void order.push('child handler'))
    const Parent = runtime.node(async () => {})
    Child.on('task:submit', () => order.push('child submit'))
    Child.on('task:cancel', () => order.push('child cancel'))
    Parent.on('task:complete', () => {
      order.push('parent complete')
      childTaskId = Child.submit()
      abortController.abort()
    })
    void runtime.closed.then(() => order.push('closed'))

    Parent.submit()
    await runtime.closed

    expect(childTaskId).toBeTypeOf('number')
    expect(order).toEqual(['parent complete', 'child submit', 'child cancel', 'closed'])
  })

  it('supports closure observers registered before, during, and after abort', async () => {
    const abortController = new AbortController()
    const runtime = new Runtime({ concurrency: 1, signal: abortController.signal })
    const observed: string[] = []
    const first = runtime.closed.then(() => observed.push('before'))

    abortController.abort()
    const second = runtime.closed.then(() => observed.push('during'))
    await Promise.all([first, second])
    await runtime.closed
    observed.push('after')

    expect(observed).toEqual(['before', 'during', 'after'])
  })

  it('cancels buffers from multiple node concurrency gates before closing', async () => {
    const abortController = new AbortController()
    const runtime = new Runtime({ concurrency: 4, signal: abortController.signal })
    const bothStarted = deferred()
    const releaseA = deferred()
    const releaseB = deferred()
    const completed: string[] = []
    const cancelled: string[] = []
    let running = 0
    const markStarted = () => {
      running++
      if (running === 2) bothStarted.resolve()
    }
    const A = runtime.node<number>(
      async (input) => {
        markStarted()
        await releaseA.promise
        completed.push(`A:${input}`)
      },
      { concurrency: 1 },
    )
    const B = runtime.node<number>(
      async (input) => {
        markStarted()
        await releaseB.promise
        completed.push(`B:${input}`)
      },
      { concurrency: 1 },
    )
    A.on('task:cancel', ({ input }) => cancelled.push(`A:${input}`))
    B.on('task:cancel', ({ input }) => cancelled.push(`B:${input}`))

    A.submit(0)
    A.submit(1)
    B.submit(0)
    B.submit(1)
    await bothStarted.promise
    abortController.abort()
    releaseA.resolve()
    releaseB.resolve()
    await runtime.closed

    expect(completed.sort()).toEqual(['A:0', 'B:0'])
    expect(cancelled.sort()).toEqual(['A:1', 'B:1'])
  })
})
