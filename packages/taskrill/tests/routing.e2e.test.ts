import { describe, expect, it } from 'vitest'
import { Runtime } from '../src'

describe('e2e: conditional and dynamic routing', () => {
  it('routes each input through exactly one conditional branch', async () => {
    interface Input {
      recipient: string
      channel: 'email' | 'sms'
    }

    const runtime = new Runtime({ concurrency: 4 })
    const delivered: string[] = []
    const Email = runtime.node<Input>(
      async ({ recipient }) => void delivered.push(`email:${recipient}`),
    )
    const Sms = runtime.node<Input>(
      async ({ recipient }) => void delivered.push(`sms:${recipient}`),
    )
    const Route = runtime.node<Input>(async () => {})
    Route.on('task:complete', ({ input }) => {
      if (input.channel === 'email') Email.submit(input)
      else Sms.submit(input)
    })
    runtime.on('idle', () => {
      if (delivered.length === 4) runtime.close()
    })

    Route.submit({ recipient: 'a', channel: 'email' })
    Route.submit({ recipient: 'b', channel: 'sms' })
    Route.submit({ recipient: 'c', channel: 'sms' })
    Route.submit({ recipient: 'd', channel: 'email' })
    await runtime.closed

    expect(delivered.sort()).toEqual(['email:a', 'email:d', 'sms:b', 'sms:c'])
  })

  it('creates task nodes dynamically while processing graph events', async () => {
    const runtime = new Runtime({ concurrency: 3 })
    const children: Array<{ name: string; nodeId: number; taskId: number | undefined }> = []
    const completed: string[] = []
    const Root = runtime.node<string>(async () => {})
    Root.on('task:complete', ({ input }) => {
      const Child = runtime.node(async () => void completed.push(input), { name: `Child:${input}` })
      children.push({ name: Child.name, nodeId: Child.id, taskId: Child.submit() })
    })
    runtime.on('idle', () => {
      if (completed.length === 3) runtime.close()
    })

    Root.submit('a')
    Root.submit('b')
    Root.submit('c')
    await runtime.closed

    expect(completed.sort()).toEqual(['a', 'b', 'c'])
    expect(children).toEqual([
      { name: 'Child:a', nodeId: 2, taskId: 4 },
      { name: 'Child:b', nodeId: 3, taskId: 5 },
      { name: 'Child:c', nodeId: 4, taskId: 6 },
    ])
  })

  it('supports a bounded feedback loop through one reusable node', async () => {
    interface Input {
      attempt: number
    }

    const runtime = new Runtime({ concurrency: 1 })
    const attempts: number[] = []
    const Poll = runtime.node<Input>(async ({ attempt }) => void attempts.push(attempt))
    Poll.on('task:complete', ({ input }) => {
      if (input.attempt < 4) Poll.submit({ attempt: input.attempt + 1 })
    })
    runtime.on('idle', () => {
      if (attempts.at(-1) === 4) runtime.close()
    })

    expect(Poll.submit({ attempt: 1 })).toBe(1)
    await runtime.closed

    expect(attempts).toEqual([1, 2, 3, 4])
  })
})
