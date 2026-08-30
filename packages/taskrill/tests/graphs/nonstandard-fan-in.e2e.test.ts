import { describe, expect, it } from 'vitest'
import { Runtime } from '../../src'
import { deferred, flush } from '../e2e-helpers'

describe('e2e: application-defined fan-in policies', () => {
  it('continues after any three of five branches complete', async () => {
    interface BranchInput {
      operationId: string
      branch: number
    }

    interface QuorumInput {
      operationId: string
      branches: number[]
    }

    const runtime = new Runtime({ concurrency: 5 })
    const gates = Array.from({ length: 5 }, deferred)
    const allStarted = deferred()
    const started: number[] = []
    const completed: number[] = []
    const quorumSnapshots: number[][] = []
    let quorumSubmitted = false
    const getGate = (branch: number) => {
      const gate = gates[branch]
      if (!gate) throw new Error(`Unknown branch ${branch}`)
      return gate
    }
    const Quorum = runtime.node<QuorumInput>(async ({ branches }) => {
      quorumSnapshots.push(branches)
    })
    const Worker = runtime.node<BranchInput>(async ({ branch }) => {
      started.push(branch)
      if (started.length === 5) allStarted.resolve()
      await getGate(branch).promise
    })
    const Root = runtime.node<{ operationId: string }>(async () => {})

    Root.on('task:complete', ({ input }) => {
      for (let branch = 0; branch < 5; branch++) Worker.submit({ ...input, branch })
    })
    Worker.on('task:complete', ({ input }) => {
      completed.push(input.branch)
      if (completed.length < 3 || quorumSubmitted) return

      quorumSubmitted = true
      Quorum.submit({ operationId: input.operationId, branches: [...completed] })
    })
    runtime.on('idle', () => {
      if (completed.length === 5 && quorumSnapshots.length === 1) runtime.close()
    })

    Root.submit({ operationId: 'quorum' })
    await allStarted.promise
    getGate(4).resolve()
    await flush()
    getGate(1).resolve()
    await flush()
    getGate(3).resolve()
    await flush()

    expect(quorumSnapshots).toEqual([[4, 1, 3]])

    getGate(0).resolve()
    getGate(2).resolve()
    await runtime.closed

    expect(started.sort()).toEqual([0, 1, 2, 3, 4])
    expect(completed).toEqual([4, 1, 3, 0, 2])
    expect(quorumSnapshots).toHaveLength(1)
  })

  it('selects the first successful branch without cancelling the slower branch', async () => {
    type Source = 'primary' | 'secondary'

    const runtime = new Runtime({ concurrency: 2 })
    const primaryGate = deferred()
    const secondaryGate = deferred()
    const bothStarted = deferred()
    const started: Source[] = []
    const completed: Source[] = []
    const consumed: Source[] = []
    let winnerSubmitted = false
    const Winner = runtime.node<Source>(async (source) => void consumed.push(source))
    const Primary = runtime.node(async () => {
      started.push('primary')
      if (started.length === 2) bothStarted.resolve()
      await primaryGate.promise
    })
    const Secondary = runtime.node(async () => {
      started.push('secondary')
      if (started.length === 2) bothStarted.resolve()
      await secondaryGate.promise
    })
    const choose = (source: Source) => {
      completed.push(source)
      if (winnerSubmitted) return

      winnerSubmitted = true
      Winner.submit(source)
    }

    Primary.on('task:complete', () => choose('primary'))
    Secondary.on('task:complete', () => choose('secondary'))
    runtime.on('idle', () => {
      if (completed.length === 2 && consumed.length === 1) runtime.close()
    })

    Primary.submit(undefined)
    Secondary.submit(undefined)
    await bothStarted.promise
    secondaryGate.resolve()
    await flush()

    expect(consumed).toEqual(['secondary'])

    primaryGate.resolve()
    await runtime.closed

    expect(started.sort()).toEqual(['primary', 'secondary'])
    expect(completed).toEqual(['secondary', 'primary'])
    expect(consumed).toEqual(['secondary'])
  })

  it('keeps interleaved fan-in generations correlated by operation id', async () => {
    type Side = 'left' | 'right'

    interface Input {
      operationId: string
    }

    interface State {
      left: boolean
      right: boolean
      merged: boolean
    }

    const runtime = new Runtime({ concurrency: 4 })
    const operations = ['one', 'two']
    const states = new Map<string, State>(
      operations.map((operationId) => [operationId, { left: false, right: false, merged: false }]),
    )
    const gates = new Map<string, ReturnType<typeof deferred>>(
      operations.flatMap((operationId) => [
        [`${operationId}:left`, deferred()] as const,
        [`${operationId}:right`, deferred()] as const,
      ]),
    )
    const allStarted = deferred()
    const started: string[] = []
    const merged: string[] = []
    const getGate = (key: string) => {
      const gate = gates.get(key)
      if (!gate) throw new Error(`Unknown branch ${key}`)
      return gate
    }
    const getState = (operationId: string) => {
      const state = states.get(operationId)
      if (!state) throw new Error(`Unknown operation ${operationId}`)
      return state
    }
    const Merge = runtime.node<Input>(async ({ operationId }) => void merged.push(operationId))
    const runBranch =
      (side: Side) =>
      async ({ operationId }: Input) => {
        const key = `${operationId}:${side}`
        started.push(key)
        if (started.length === 4) allStarted.resolve()
        await getGate(key).promise
      }
    const Left = runtime.node<Input>(runBranch('left'))
    const Right = runtime.node<Input>(runBranch('right'))
    const Root = runtime.node<Input>(async () => {})
    const advance = (input: Input) => {
      const state = getState(input.operationId)
      if (!state.left || !state.right || state.merged) return

      state.merged = true
      Merge.submit(input)
    }
    const complete = (side: Side, input: Input) => {
      const state = getState(input.operationId)
      state[side] = true
      advance(input)
    }

    Root.on('task:complete', ({ input }) => {
      Left.submit(input)
      Right.submit(input)
    })
    Left.on('task:complete', ({ input }) => complete('left', input))
    Right.on('task:complete', ({ input }) => complete('right', input))
    runtime.on('idle', () => {
      if (merged.length === operations.length) runtime.close()
    })

    for (const operationId of operations) Root.submit({ operationId })
    await allStarted.promise
    getGate('one:left').resolve()
    getGate('two:right').resolve()
    await flush()

    expect(merged).toEqual([])

    getGate('one:right').resolve()
    getGate('two:left').resolve()
    await runtime.closed

    expect(started.sort()).toEqual(['one:left', 'one:right', 'two:left', 'two:right'])
    expect(merged.sort()).toEqual(['one', 'two'])
    expect([...states.values()]).toEqual([
      { left: true, right: true, merged: true },
      { left: true, right: true, merged: true },
    ])
  })
})
