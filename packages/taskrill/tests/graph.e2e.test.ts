import { describe, expect, it } from 'vitest'
import { Runtime } from '../src'

const runToIdle = (runtime: Runtime, start: () => void) =>
  new Promise<void>((resolve) => {
    const unsubscribe = runtime.on('idle', () => {
      unsubscribe()
      resolve()
    })
    start()
  })

describe('e2e: application-owned graph progression', () => {
  it('implements correlated fan-in without a runtime join primitive', async () => {
    interface Input {
      operationId: string
    }

    const runtime = new Runtime({ concurrency: 4 })
    const state = new Map<string, { left: boolean; right: boolean; merged: boolean }>()
    const merged: string[] = []

    const Merge = runtime.node<Input>(async ({ operationId }) => void merged.push(operationId))
    const Left = runtime.node<Input>(async () => {})
    const Right = runtime.node<Input>(async () => {})

    const advance = (operationId: string) => {
      const operation = state.get(operationId)
      if (!operation || operation.merged || !operation.left || !operation.right) return
      operation.merged = true
      Merge.submit({ operationId })
    }

    Left.on('task:complete', ({ input }) => {
      const operation = state.get(input.operationId)
      if (operation) operation.left = true
      advance(input.operationId)
    })
    Right.on('task:complete', ({ input }) => {
      const operation = state.get(input.operationId)
      if (operation) operation.right = true
      advance(input.operationId)
    })

    await runToIdle(runtime, () => {
      for (const operationId of ['one', 'two']) {
        state.set(operationId, { left: false, right: false, merged: false })
        Left.submit({ operationId })
        Right.submit({ operationId })
      }
    })

    expect(merged.sort()).toEqual(['one', 'two'])
  })

  it('supports conditional cycles between reusable nodes', async () => {
    interface Input {
      operationId: string
      round: number
    }

    const runtime = new Runtime({ concurrency: 2 })
    const order: string[] = []
    const A = runtime.node<Input>(async ({ round }) => void order.push(`A:${round}`))
    const B = runtime.node<Input>(async ({ round }) => void order.push(`B:${round}`))

    A.on('task:complete', ({ input }) => B.submit(input))
    B.on('task:complete', ({ input }) => {
      if (input.round < 3) A.submit({ ...input, round: input.round + 1 })
    })

    await runToIdle(runtime, () => A.submit({ operationId: 'cycle', round: 1 }))

    expect(order).toEqual(['A:1', 'B:1', 'A:2', 'B:2', 'A:3', 'B:3'])
  })

  it('tracks recursively discovered fan-out through runtime idle', async () => {
    const runtime = new Runtime({ concurrency: 8 })
    const visited: string[] = []
    const childrenOf = (key: string) => (key.length >= 4 ? [] : [`${key}0`, `${key}1`])
    const Crawl = runtime.node<string>(async (key) => {
      visited.push(key)
      for (const child of childrenOf(key)) Crawl.submit(child)
    })

    await runToIdle(runtime, () => Crawl.submit('r'))

    expect(new Set(visited).size).toBe(15)
  })
})
