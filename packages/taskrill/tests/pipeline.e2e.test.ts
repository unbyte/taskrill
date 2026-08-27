import { describe, expect, it } from 'vitest'
import { Runtime } from '../src'

describe('e2e: pipelines and diamonds', () => {
  it('runs several correlated inputs through a linear pipeline', async () => {
    interface Input {
      operationId: string
    }

    const runtime = new Runtime({ concurrency: 3 })
    const stages = new Map<string, string[]>()
    const inputs = ['one', 'two', 'three'].map((operationId) => ({ operationId }))
    const record = (operationId: string, stage: string) => {
      const seen = stages.get(operationId) ?? []
      seen.push(stage)
      stages.set(operationId, seen)
    }
    const Store = runtime.node<Input>(async ({ operationId }) => record(operationId, 'store'))
    const Validate = runtime.node<Input>(async ({ operationId }) => record(operationId, 'validate'))
    const Parse = runtime.node<Input>(async ({ operationId }) => record(operationId, 'parse'))
    Parse.on('task:complete', ({ input }) => Validate.submit(input))
    Validate.on('task:complete', ({ input }) => Store.submit(input))
    runtime.on('idle', () => {
      if ([...stages.values()].every((seen) => seen.at(-1) === 'store')) runtime.close()
    })

    for (const input of inputs) Parse.submit(input)
    await runtime.closed

    expect(Object.fromEntries(stages)).toEqual({
      one: ['parse', 'validate', 'store'],
      two: ['parse', 'validate', 'store'],
      three: ['parse', 'validate', 'store'],
    })
  })

  it('combines fan-out and application-owned fan-in in a diamond graph', async () => {
    interface Input {
      operationId: string
    }
    interface State {
      left: boolean
      right: boolean
      merged: boolean
    }

    const runtime = new Runtime({ concurrency: 4 })
    const states = new Map<string, State>()
    const merged: string[] = []
    const Merge = runtime.node<Input>(async ({ operationId }) => void merged.push(operationId))
    const Left = runtime.node<Input>(async () => {})
    const Right = runtime.node<Input>(async () => {})
    const Root = runtime.node<Input>(async () => {})
    const advance = (operationId: string) => {
      const state = states.get(operationId)
      if (!state?.left || !state.right || state.merged) return
      state.merged = true
      Merge.submit({ operationId })
    }
    Root.on('task:complete', ({ input }) => {
      Left.submit(input)
      Right.submit(input)
    })
    Left.on('task:complete', ({ input }) => {
      const state = states.get(input.operationId)
      if (state) state.left = true
      advance(input.operationId)
    })
    Right.on('task:complete', ({ input }) => {
      const state = states.get(input.operationId)
      if (state) state.right = true
      advance(input.operationId)
    })
    runtime.on('idle', () => {
      if (merged.length === states.size) runtime.close()
    })

    for (const operationId of ['alpha', 'beta', 'gamma']) {
      states.set(operationId, { left: false, right: false, merged: false })
      Root.submit({ operationId })
    }
    await runtime.closed

    expect(merged.sort()).toEqual(['alpha', 'beta', 'gamma'])
    expect([...states.values()].every(({ merged: done }) => done)).toBe(true)
  })
})
