import { describe, expect, it } from 'vitest'
import { Runtime } from '../../src'

interface RoundInput {
  operationId: string
  round: number
}

interface RoundState {
  left: boolean
  right: boolean
  joined: boolean
}

describe('e2e: cyclic graph topologies', () => {
  it('re-enters a diamond through its fan-in edge for several bounded rounds', async () => {
    const runtime = new Runtime({ concurrency: 3 })
    const rounds = new Map<number, RoundState>()
    const hubs: number[] = []
    const joins: number[] = []
    const finalized: string[] = []
    const Final = runtime.node<RoundInput>(async ({ operationId }) => {
      finalized.push(operationId)
    })
    const Join = runtime.node<RoundInput>(async ({ round }) => {
      joins.push(round)
    })
    const Left = runtime.node<RoundInput>(async () => {})
    const Right = runtime.node<RoundInput>(async () => {})
    const Hub = runtime.node<RoundInput>(async ({ round }) => {
      hubs.push(round)
    })
    const startRound = (input: RoundInput) => {
      rounds.set(input.round, { left: false, right: false, joined: false })
      Hub.submit(input)
    }
    const getRound = (round: number) => {
      const state = rounds.get(round)
      if (!state) throw new Error(`Round ${round} has not started`)
      return state
    }
    const advance = (input: RoundInput) => {
      const state = getRound(input.round)
      if (!state.left || !state.right || state.joined) return

      state.joined = true
      Join.submit(input)
    }

    Hub.on('task:complete', ({ input }) => {
      Left.submit(input)
      Right.submit(input)
    })
    Left.on('task:complete', ({ input }) => {
      getRound(input.round).left = true
      advance(input)
    })
    Right.on('task:complete', ({ input }) => {
      getRound(input.round).right = true
      advance(input)
    })
    Join.on('task:complete', ({ input }) => {
      if (input.round < 3) startRound({ ...input, round: input.round + 1 })
      else Final.submit(input)
    })
    runtime.on('idle', () => {
      if (finalized.length === 1) runtime.close()
    })

    startRound({ operationId: 'looping-diamond', round: 1 })
    await runtime.closed

    expect(hubs).toEqual([1, 2, 3])
    expect(joins).toEqual([1, 2, 3])
    expect([...rounds.values()]).toEqual([
      { left: true, right: true, joined: true },
      { left: true, right: true, joined: true },
      { left: true, right: true, joined: true },
    ])
    expect(finalized).toEqual(['looping-diamond'])
  })

  it('alternates through both lobes of a figure-eight cycle', async () => {
    interface Input {
      step: number
    }

    const runtime = new Runtime({ concurrency: 2 })
    const trace: string[] = []
    const Final = runtime.node<Input>(async () => void trace.push('final'))
    const LeftB = runtime.node<Input>(async ({ step }) => void trace.push(`left-b:${step}`))
    const LeftA = runtime.node<Input>(async ({ step }) => void trace.push(`left-a:${step}`))
    const RightB = runtime.node<Input>(async ({ step }) => void trace.push(`right-b:${step}`))
    const RightA = runtime.node<Input>(async ({ step }) => void trace.push(`right-a:${step}`))
    const Pivot = runtime.node<Input>(async ({ step }) => void trace.push(`pivot:${step}`))

    Pivot.on('task:complete', ({ input }) => {
      if (input.step === 4) Final.submit(input)
      else if (input.step % 2 === 0) LeftA.submit(input)
      else RightA.submit(input)
    })
    LeftA.on('task:complete', ({ input }) => LeftB.submit(input))
    LeftB.on('task:complete', ({ input }) => Pivot.submit({ step: input.step + 1 }))
    RightA.on('task:complete', ({ input }) => RightB.submit(input))
    RightB.on('task:complete', ({ input }) => Pivot.submit({ step: input.step + 1 }))
    runtime.on('idle', () => {
      if (trace.at(-1) === 'final') runtime.close()
    })

    Pivot.submit({ step: 0 })
    await runtime.closed

    expect(trace).toEqual([
      'pivot:0',
      'left-a:0',
      'left-b:0',
      'pivot:1',
      'right-a:1',
      'right-b:1',
      'pivot:2',
      'left-a:2',
      'left-b:2',
      'pivot:3',
      'right-a:3',
      'right-b:3',
      'pivot:4',
      'final',
    ])
  })

  it('traverses cyclic data once by deduplicating visits in application state', async () => {
    type Key = 'a' | 'b' | 'c' | 'd' | 'e'

    const edges: Record<Key, Key[]> = {
      a: ['b', 'c'],
      b: ['c', 'd'],
      c: ['a', 'd'],
      d: ['b', 'e'],
      e: [],
    }
    const runtime = new Runtime({ concurrency: 3 })
    const scheduled = new Set<Key>()
    const visited: Key[] = []
    const Visit = runtime.node<Key>(async (key) => void visited.push(key))
    const schedule = (key: Key) => {
      if (scheduled.has(key)) return

      scheduled.add(key)
      Visit.submit(key)
    }

    Visit.on('task:complete', ({ input }) => {
      for (const neighbor of edges[input]) schedule(neighbor)
    })
    runtime.on('idle', () => {
      if (visited.length === Object.keys(edges).length) runtime.close()
    })

    schedule('a')
    await runtime.closed

    expect(visited).toHaveLength(5)
    expect(visited.sort()).toEqual(['a', 'b', 'c', 'd', 'e'])
    expect(scheduled).toEqual(new Set(['a', 'b', 'c', 'd', 'e']))
  })

  it('aborts an active ring and closes after rejecting its next back edge', async () => {
    interface Input {
      lap: number
    }

    const abortController = new AbortController()
    const runtime = new Runtime({ concurrency: 1, signal: abortController.signal })
    const trace: string[] = []
    const rejectedBackEdges: Array<number | undefined> = []
    const A = runtime.node<Input>(async ({ lap }) => void trace.push(`a:${lap}`))
    const B = runtime.node<Input>(async ({ lap }) => void trace.push(`b:${lap}`))

    A.on('task:complete', ({ input }) => B.submit(input))
    B.on('task:complete', ({ input }) => {
      if (input.lap < 4) {
        A.submit({ lap: input.lap + 1 })
        return
      }

      abortController.abort('stop ring')
      rejectedBackEdges.push(A.submit({ lap: input.lap + 1 }))
    })

    A.submit({ lap: 1 })
    await runtime.closed

    expect(trace).toEqual(['a:1', 'b:1', 'a:2', 'b:2', 'a:3', 'b:3', 'a:4', 'b:4'])
    expect(rejectedBackEdges).toEqual([undefined])
  })

  it('uses a repair node as a back edge until a failing node can continue', async () => {
    interface Input {
      operationId: string
      attempt: number
    }

    const runtime = new Runtime({ concurrency: 2 })
    const executions: number[] = []
    const repairs: number[] = []
    const failures: string[] = []
    const verified: string[] = []
    const Verify = runtime.node<Input>(async ({ operationId }) => {
      verified.push(operationId)
    })
    const Repair = runtime.node<Input>(async ({ attempt }) => {
      repairs.push(attempt)
    })
    const Execute = runtime.node<Input>(async ({ operationId, attempt }) => {
      executions.push(attempt)
      if (attempt < 2) throw new Error(`${operationId}:${attempt}`)
    })

    Execute.on('task:failure', ({ input }) => Repair.submit(input))
    Execute.on('task:complete', ({ input }) => Verify.submit(input))
    Repair.on('task:complete', ({ input }) => {
      Execute.submit({ ...input, attempt: input.attempt + 1 })
    })
    runtime.on('task:failure', ({ error }) => {
      failures.push(error instanceof Error ? error.message : String(error))
    })
    runtime.on('idle', () => {
      if (verified.length === 1) runtime.close()
    })

    Execute.submit({ operationId: 'repair-loop', attempt: 0 })
    await runtime.closed

    expect(executions).toEqual([0, 1, 2])
    expect(repairs).toEqual([0, 1])
    expect(failures).toEqual(['repair-loop:0', 'repair-loop:1'])
    expect(verified).toEqual(['repair-loop'])
  })
})
