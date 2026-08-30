import { describe, expect, it } from 'vitest'
import { Runtime } from '../../src'
import { deferred } from '../e2e-helpers'

describe('contract: application-controlled runtime closure', () => {
  it('stays reusable across idle periods until application state chooses to close', async () => {
    const runtime = new Runtime({ concurrency: 2 })
    const firstIdle = deferred()
    const completed: string[] = []
    let closed = false
    const Task = runtime.node<string>(async (input) => void completed.push(input))
    runtime.on('idle', () => {
      if (completed.length === 2) runtime.close()
      else firstIdle.resolve()
    })
    void runtime.closed.then(() => {
      closed = true
    })

    expect(Task.submit('first phase')).toBe(1)
    await firstIdle.promise
    expect(closed).toBe(false)

    expect(Task.submit('second phase')).toBe(2)
    await runtime.closed
    expect(completed).toEqual(['first phase', 'second phase'])
    expect(closed).toBe(true)
  })

  it('drains accepted work without allowing new causal submissions after close', async () => {
    const runtime = new Runtime({ concurrency: 1 })
    const completed: number[] = []
    const rejectedChildren: Array<number | undefined> = []
    const Child = runtime.node<number>(async () => {})
    const Parent = runtime.node<number>(async (input) => void completed.push(input))
    Parent.on('task:complete', ({ input }) => rejectedChildren.push(Child.submit(input)))

    expect([Parent.submit(1), Parent.submit(2), Parent.submit(3)]).toEqual([1, 2, 3])
    runtime.close()
    await runtime.closed

    expect(completed).toEqual([1, 2, 3])
    expect(rejectedChildren).toEqual([undefined, undefined, undefined])
    expect(() => runtime.node(async () => {})).toThrow()
  })
})
