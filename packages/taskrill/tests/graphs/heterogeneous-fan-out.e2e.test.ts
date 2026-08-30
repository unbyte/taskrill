import { describe, expect, it } from 'vitest'
import { Runtime } from '../../src'

interface OrderInput {
  orderId: string
}

interface PaymentInput extends OrderInput {
  attempt: number
}

interface OrderState {
  inventory: boolean
  payment: boolean
  delivery: boolean
  audit: boolean
  publishTaskId?: number
}

describe('e2e: heterogeneous one-to-many graph', () => {
  it('fans out into unequal branch pipelines and publishes after application-owned fan-in', async () => {
    const runtime = new Runtime({ concurrency: 6 })
    const states = new Map<string, OrderState>()
    const paymentAttempts = new Map<string, number[]>()
    const published: string[] = []
    const failures: string[] = []
    const Publish = runtime.node<OrderInput>(async ({ orderId }) => void published.push(orderId), {
      name: 'Publish',
    })
    const ConfirmInventory = runtime.node<OrderInput>(async () => {}, {
      name: 'ConfirmInventory',
    })
    const ReserveInventory = runtime.node<OrderInput>(async () => {}, {
      name: 'ReserveInventory',
    })
    const Receipt = runtime.node<OrderInput>(async () => {}, { name: 'Receipt' })
    const Charge = runtime.node<PaymentInput>(
      async ({ orderId, attempt }) => {
        const attempts = paymentAttempts.get(orderId) ?? []
        attempts.push(attempt)
        paymentAttempts.set(orderId, attempts)
        if (attempt === 0) throw new Error(`retry payment for ${orderId}`)
      },
      { name: 'Charge' },
    )
    const BookDelivery = runtime.node<OrderInput>(async () => {}, { name: 'BookDelivery' })
    const QuoteDelivery = runtime.node<OrderInput>(async () => {}, { name: 'QuoteDelivery' })
    const PlanDelivery = runtime.node<OrderInput>(async () => {}, { name: 'PlanDelivery' })
    const Audit = runtime.node<OrderInput>(async () => {}, { name: 'Audit' })
    const Root = runtime.node<OrderInput>(async () => {}, { name: 'Root' })
    const advance = (orderId: string) => {
      const state = states.get(orderId)
      if (!state || state.publishTaskId !== undefined) return
      if (!state.inventory || !state.payment || !state.delivery || !state.audit) return

      state.publishTaskId = Publish.submit({ orderId })
    }

    Root.on('task:complete', ({ input }) => {
      ReserveInventory.submit(input)
      Charge.submit({ ...input, attempt: 0 })
      PlanDelivery.submit(input)
      Audit.submit(input)
    })
    ReserveInventory.on('task:complete', ({ input }) => ConfirmInventory.submit(input))
    ConfirmInventory.on('task:complete', ({ input }) => {
      const state = states.get(input.orderId)
      if (state) state.inventory = true
      advance(input.orderId)
    })
    Charge.on('task:failure', ({ input }) =>
      Charge.submit({ ...input, attempt: input.attempt + 1 }),
    )
    Charge.on('task:complete', ({ input }) => Receipt.submit(input))
    Receipt.on('task:complete', ({ input }) => {
      const state = states.get(input.orderId)
      if (state) state.payment = true
      advance(input.orderId)
    })
    PlanDelivery.on('task:complete', ({ input }) => QuoteDelivery.submit(input))
    QuoteDelivery.on('task:complete', ({ input }) => BookDelivery.submit(input))
    BookDelivery.on('task:complete', ({ input }) => {
      const state = states.get(input.orderId)
      if (state) state.delivery = true
      advance(input.orderId)
    })
    Audit.on('task:complete', ({ input }) => {
      const state = states.get(input.orderId)
      if (state) state.audit = true
      advance(input.orderId)
    })
    runtime.on('task:failure', ({ error }) => failures.push((error as Error).message))
    runtime.on('idle', () => {
      if (published.length === states.size) runtime.close()
    })

    for (const orderId of ['order-a', 'order-b']) {
      states.set(orderId, {
        inventory: false,
        payment: false,
        delivery: false,
        audit: false,
      })
      Root.submit({ orderId })
    }
    await runtime.closed

    expect(published.sort()).toEqual(['order-a', 'order-b'])
    expect(Object.fromEntries(paymentAttempts)).toEqual({
      'order-a': [0, 1],
      'order-b': [0, 1],
    })
    expect(failures.sort()).toEqual(['retry payment for order-a', 'retry payment for order-b'])
    expect(
      [...states.values()].every(
        ({ inventory, payment, delivery, audit, publishTaskId }) =>
          inventory && payment && delivery && audit && publishTaskId !== undefined,
      ),
    ).toBe(true)
  })
})
