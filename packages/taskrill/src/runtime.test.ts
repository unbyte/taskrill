import { describe, expect, it } from 'vitest'
import { Runtime } from './runtime'

const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 0))

const runToIdle = (runtime: Runtime, start: () => void) =>
  new Promise<void>((resolve) => {
    const unsubscribe = runtime.on('idle', () => {
      unsubscribe()
      resolve()
    })
    start()
  })

describe('Runtime nodes', () => {
  it('assigns monotonic node IDs and generated names', () => {
    const runtime = new Runtime({ concurrency: 1 })
    const first = runtime.node(async () => {})
    const second = runtime.node<number>(async () => {}, { name: 'Second' })

    expect(first).toMatchObject({ id: 1, name: 'node#1' })
    expect(second).toMatchObject({ id: 2, name: 'Second' })
    expect(first).not.toHaveProperty('done')
    expect(first).not.toHaveProperty('seal')
  })

  it('validates node concurrency without consuming a node ID', () => {
    const runtime = new Runtime({ concurrency: 4 })
    expect(() => runtime.node(async () => {}, { concurrency: 0 })).toThrow(RangeError)
    expect(() => runtime.node(async () => {}, { concurrency: -1 })).toThrow(RangeError)
    expect(() => runtime.node(async () => {}, { concurrency: 1.5 })).toThrow(RangeError)
    expect(() => runtime.node(async () => {}, { concurrency: Number.NaN })).toThrow(RangeError)
    expect(runtime.node(async () => {}).id).toBe(1)
  })

  it('accepts repeated submissions to the same reusable node', async () => {
    const runtime = new Runtime({ concurrency: 2 })
    const seen: number[] = []
    const node = runtime.node<number>(async (input) => void seen.push(input))

    await runToIdle(runtime, () => {
      node.submit(1)
      node.submit(2)
      node.submit(3)
    })

    expect(seen).toEqual([1, 2, 3])
  })

  it('passes the runtime signal or a never-aborting placeholder to handlers', async () => {
    const abortController = new AbortController()
    const withSignal = new Runtime({ concurrency: 1, signal: abortController.signal })
    const withoutSignal = new Runtime({ concurrency: 1 })
    let observedSignal: AbortSignal | undefined
    let placeholder: AbortSignal | undefined

    await Promise.all([
      runToIdle(withSignal, () =>
        withSignal
          .node((_input, context) => {
            observedSignal = context.signal
          })
          .submit(),
      ),
      runToIdle(withoutSignal, () =>
        withoutSignal
          .node((_input, context) => {
            placeholder = context.signal
          })
          .submit(),
      ),
    ])

    expect(observedSignal).toBe(abortController.signal)
    expect(placeholder).toBeInstanceOf(AbortSignal)
    expect(placeholder?.aborted).toBe(false)
  })
})

describe('Task lifecycle events', () => {
  it('emits submit synchronously and defers start and handler execution', async () => {
    const runtime = new Runtime({ concurrency: 1 })
    const order: string[] = []
    const node = runtime.node<number>(async () => void order.push('handler'))
    node.on('task:submit', () => order.push('submit'))
    node.on('task:start', () => order.push('start'))
    node.on('task:complete', () => order.push('complete'))

    const idle = runToIdle(runtime, () => node.submit(7))
    expect(order).toEqual(['submit'])

    await idle
    expect(order).toEqual(['submit', 'start', 'handler', 'complete'])
  })

  it('carries stable IDs, node identity, and typed input through a task lifecycle', async () => {
    const runtime = new Runtime({ concurrency: 2 })
    const events: Array<{ event: string; id: number; input: string }> = []
    const node = runtime.node<string>(async () => {})

    for (const event of ['task:submit', 'task:start', 'task:complete'] as const) {
      node.on(event, (task) => {
        expect(task.node).toBe(node)
        events.push({ event, id: task.id, input: task.input })
      })
    }

    await runToIdle(runtime, () => {
      node.submit('first')
      node.submit('second')
    })

    expect(events).toEqual([
      { event: 'task:submit', id: 1, input: 'first' },
      { event: 'task:submit', id: 2, input: 'second' },
      { event: 'task:start', id: 1, input: 'first' },
      { event: 'task:start', id: 2, input: 'second' },
      { event: 'task:complete', id: 1, input: 'first' },
      { event: 'task:complete', id: 2, input: 'second' },
    ])
  })

  it('reports synchronous and asynchronous handler failures exactly once', async () => {
    const runtime = new Runtime({ concurrency: 2 })
    const nodeFailures: unknown[] = []
    const runtimeFailures: unknown[] = []
    const node = runtime.node<number>((input) => {
      if (input === 1) throw new Error('sync')
      return Promise.reject(new Error('async'))
    })
    node.on('task:failure', ({ error }) => nodeFailures.push(error))
    runtime.on('task:failure', ({ error }) => runtimeFailures.push(error))

    await runToIdle(runtime, () => {
      node.submit(1)
      node.submit(2)
    })

    expect(nodeFailures.map((error) => (error as Error).message)).toEqual(['sync', 'async'])
    expect(runtimeFailures).toEqual(nodeFailures)
  })

  it('emits node failure before runtime failure', async () => {
    const runtime = new Runtime({ concurrency: 1 })
    const order: string[] = []
    const node = runtime.node(async () => {
      throw new Error('boom')
    })
    node.on('task:failure', () => order.push('node'))
    runtime.on('task:failure', () => order.push('runtime'))

    await runToIdle(runtime, () => node.submit())
    expect(order).toEqual(['node', 'runtime'])
  })
})

describe('Runtime idle', () => {
  it('does not fire for an initially empty runtime', async () => {
    const runtime = new Runtime({ concurrency: 1 })
    let idle = 0
    runtime.on('idle', () => idle++)

    await flush()
    expect(idle).toBe(0)
  })

  it('is multi-shot and includes work submitted by an idle listener', async () => {
    const runtime = new Runtime({ concurrency: 1 })
    const inputs: number[] = []
    const node = runtime.node<number>(async (input) => void inputs.push(input))
    let idle = 0
    runtime.on('idle', () => {
      idle++
      if (idle === 1) node.submit(2)
    })

    node.submit(1)
    await flush()

    expect(inputs).toEqual([1, 2])
    expect(idle).toBe(2)
  })

  it('waits for listener-submitted causal work before becoming idle', async () => {
    const runtime = new Runtime({ concurrency: 1 })
    const order: string[] = []
    const downstream = runtime.node(async () => void order.push('downstream'))
    const upstream = runtime.node(async () => void order.push('upstream'))
    upstream.on('task:complete', () => downstream.submit())
    runtime.on('idle', () => order.push('idle'))

    upstream.submit()
    await flush()

    expect(order).toEqual(['upstream', 'downstream', 'idle'])
  })
})

describe('Runtime abort', () => {
  it('preserves submit-before-cancel ordering when a submit listener aborts', async () => {
    const abortController = new AbortController()
    const runtime = new Runtime({ concurrency: 1, signal: abortController.signal })
    const order: string[] = []
    const node = runtime.node(async () => void order.push('handler'))

    node.on('task:submit', () => {
      order.push('submit:first')
      abortController.abort()
    })
    node.on('task:submit', () => order.push('submit:second'))
    node.on('task:cancel', () => order.push('cancel'))

    node.submit()
    expect(order).toEqual(['submit:first', 'submit:second', 'cancel'])

    await flush()
    expect(order).not.toContain('handler')
  })

  it('accepts nothing when the runtime starts aborted', () => {
    const abortController = new AbortController()
    abortController.abort()
    const runtime = new Runtime({ concurrency: 1, signal: abortController.signal })
    const events: string[] = []
    const node = runtime.node(async () => void events.push('handler'))
    node.on('task:submit', () => events.push('submit'))

    node.submit()
    expect(events).toEqual([])
  })

  it('cancels tasks that never started while a running handler settles normally', async () => {
    const abortController = new AbortController()
    const runtime = new Runtime({ concurrency: 1, signal: abortController.signal })
    const started: number[] = []
    const cancelled: number[] = []
    const completed: number[] = []
    let release!: () => void
    const running = new Promise<void>((resolve) => {
      release = resolve
    })
    const node = runtime.node<number>(async (input) => {
      started.push(input)
      await running
    })
    node.on('task:cancel', ({ input }) => cancelled.push(input))
    node.on('task:complete', ({ input }) => completed.push(input))

    node.submit(0)
    node.submit(1)
    node.submit(2)
    await flush()
    abortController.abort()
    release()
    await flush()

    expect(started).toEqual([0])
    expect(cancelled).toEqual([1, 2])
    expect(completed).toEqual([0])
  })

  it('still reports a running failure after abort and never emits idle', async () => {
    const abortController = new AbortController()
    const runtime = new Runtime({ concurrency: 1, signal: abortController.signal })
    const failures: unknown[] = []
    let reject!: (error: unknown) => void
    const running = new Promise<void>((_resolve, rejectPromise) => {
      reject = rejectPromise
    })
    const node = runtime.node(async () => running)
    runtime.on('task:failure', ({ error }) => failures.push(error))
    runtime.on('idle', () => failures.push('idle'))

    node.submit()
    await flush()
    abortController.abort()
    const boom = new Error('late boom')
    reject(boom)
    await flush()

    expect(failures).toEqual([boom])
  })
})
