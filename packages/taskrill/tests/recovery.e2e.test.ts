import { describe, expect, it } from 'vitest'
import { Runtime } from '../src'

describe('e2e: failure recovery', () => {
  it('retries failed executions through application state before continuing downstream', async () => {
    interface Input {
      operationId: string
      attempt: number
    }

    const runtime = new Runtime({ concurrency: 3 })
    const attempts = new Map<string, number[]>()
    const finalized: string[] = []
    const failures: unknown[] = []
    const Finalize = runtime.node<Input>(
      async ({ operationId }) => void finalized.push(operationId),
    )
    const Work = runtime.node<Input>(async ({ operationId, attempt }) => {
      const seen = attempts.get(operationId) ?? []
      seen.push(attempt)
      attempts.set(operationId, seen)
      if (attempt < 2) throw new Error(`${operationId}:${attempt}`)
    })
    Work.on('task:failure', ({ input }) => {
      Work.submit({ ...input, attempt: input.attempt + 1 })
    })
    Work.on('task:complete', ({ input }) => Finalize.submit(input))
    runtime.on('task:failure', ({ error }) => failures.push(error))
    runtime.on('idle', () => {
      if (finalized.length === 2) runtime.close()
    })

    Work.submit({ operationId: 'a', attempt: 0 })
    Work.submit({ operationId: 'b', attempt: 0 })
    await runtime.closed

    expect(Object.fromEntries(attempts)).toEqual({ a: [0, 1, 2], b: [0, 1, 2] })
    expect(finalized.sort()).toEqual(['a', 'b'])
    expect(failures).toHaveLength(4)
  })

  it('uses a compensation branch as one input to a later fan-in', async () => {
    interface Input {
      operationId: string
    }

    const runtime = new Runtime({ concurrency: 3 })
    const state = { refunded: false, notified: false, audited: false }
    const Audit = runtime.node<Input>(async () => {
      state.audited = true
    })
    const Refund = runtime.node<Input>(async () => {})
    const Notify = runtime.node<Input>(async () => {})
    const Charge = runtime.node<Input>(async () => {
      throw new Error('charge declined')
    })
    const advance = (input: Input) => {
      if (!state.refunded || !state.notified || state.audited) return
      Audit.submit(input)
    }
    Charge.on('task:failure', ({ input }) => Refund.submit(input))
    Refund.on('task:complete', ({ input }) => {
      state.refunded = true
      advance(input)
    })
    Notify.on('task:complete', ({ input }) => {
      state.notified = true
      advance(input)
    })
    runtime.on('idle', () => {
      if (state.audited) runtime.close()
    })

    const input = { operationId: 'purchase-1' }
    Charge.submit(input)
    Notify.submit(input)
    await runtime.closed

    expect(state).toEqual({ refunded: true, notified: true, audited: true })
  })
})
