import { describe, expect, it } from 'vitest'
import type { Handler, TaskNode } from '../src'
import { Runtime } from '../src'
import { deferred } from './e2e-helpers'

type Stage = 'first' | 'second' | 'third'

interface BranchInput {
  operationId: string
  branch: number
}

interface MergeInput {
  operationId: string
}

interface GraphOptions {
  first?: Handler<BranchInput>
  second?: Handler<BranchInput>
  third?: Handler<BranchInput>
  merge?: Handler<MergeInput>
  firstConcurrency?: number
  secondConcurrency?: number
  thirdConcurrency?: number
  onMergeSubmitted?: (taskId: number | undefined) => void
}

interface GraphState {
  readonly history: Map<number, Stage[]>
  readonly started: Record<Stage, number[]>
  readonly cancelled: Record<Stage, number[]>
  readonly failures: Array<{ stage: Stage; branch: number; error: unknown }>
  readonly thirdCompleted: number[]
  mergeAttempts: number
  mergeTaskId?: number
  mergeRuns: number
  mergeCancelled: boolean
}

const createThreeStageFanIn = (
  runtime: Runtime,
  branchCount: number,
  options: GraphOptions = {},
) => {
  const operationId = 'operation'
  const state: GraphState = {
    history: new Map(),
    started: { first: [], second: [], third: [] },
    cancelled: { first: [], second: [], third: [] },
    failures: [],
    thirdCompleted: [],
    mergeAttempts: 0,
    mergeTaskId: undefined,
    mergeRuns: 0,
    mergeCancelled: false,
  }
  const runStage =
    (stage: Stage, handler?: Handler<BranchInput>): Handler<BranchInput> =>
    async (input, context) => {
      state.started[stage].push(input.branch)
      const history = state.history.get(input.branch) ?? []
      history.push(stage)
      state.history.set(input.branch, history)
      await handler?.(input, context)
    }
  const observeStage = (stage: Stage, node: TaskNode<BranchInput>) => {
    node.on('task:cancel', ({ input }) => state.cancelled[stage].push(input.branch))
    node.on('task:failure', ({ input, error }) => {
      state.failures.push({ stage, branch: input.branch, error })
    })
  }

  const Merge = runtime.node<MergeInput>(
    async (input, context) => {
      state.mergeRuns++
      await options.merge?.(input, context)
    },
    { name: 'Merge' },
  )
  const Third = runtime.node(runStage('third', options.third), {
    name: 'Third',
    concurrency: options.thirdConcurrency,
  })
  const Second = runtime.node(runStage('second', options.second), {
    name: 'Second',
    concurrency: options.secondConcurrency,
  })
  const First = runtime.node(runStage('first', options.first), {
    name: 'First',
    concurrency: options.firstConcurrency,
  })
  const Root = runtime.node<MergeInput>(async () => {}, { name: 'Root' })

  observeStage('first', First)
  observeStage('second', Second)
  observeStage('third', Third)
  Merge.on('task:cancel', () => {
    state.mergeCancelled = true
  })
  Root.on('task:complete', ({ input }) => {
    for (let branch = 0; branch < branchCount; branch++) First.submit({ ...input, branch })
  })
  First.on('task:complete', ({ input }) => Second.submit(input))
  Second.on('task:complete', ({ input }) => Third.submit(input))
  Third.on('task:complete', ({ input }) => {
    state.thirdCompleted.push(input.branch)
    if (state.thirdCompleted.length !== branchCount || state.mergeAttempts !== 0) return

    state.mergeAttempts++
    state.mergeTaskId = Merge.submit({ operationId: input.operationId })
    options.onMergeSubmitted?.(state.mergeTaskId)
  })

  return {
    state,
    start: () => Root.submit({ operationId }),
  }
}

describe('e2e: three serial tasks per fan-out branch followed by fan-in', () => {
  it('runs every branch serially and merges only after all third tasks complete', async () => {
    const runtime = new Runtime({ concurrency: 4 })
    let thirdTasksObservedByMerge: number[] = []
    let graph!: ReturnType<typeof createThreeStageFanIn>
    graph = createThreeStageFanIn(runtime, 5, {
      merge: async () => {
        thirdTasksObservedByMerge = [...graph.state.thirdCompleted]
      },
    })
    runtime.on('idle', () => {
      if (graph.state.mergeRuns === 1) runtime.close()
    })

    expect(graph.start()).toBe(1)
    await runtime.closed

    for (let branch = 0; branch < 5; branch++) {
      expect(graph.state.history.get(branch)).toEqual(['first', 'second', 'third'])
    }
    expect(thirdTasksObservedByMerge.sort()).toEqual([0, 1, 2, 3, 4])
    expect(graph.state.mergeAttempts).toBe(1)
    expect(graph.state.mergeRuns).toBe(1)
  })

  it('aborts running second tasks, cancels buffered branches, and never reaches fan-in', async () => {
    const abortController = new AbortController()
    const runtime = new Runtime({ concurrency: 5, signal: abortController.signal })
    const twoSecondTasksStarted = deferred()
    let secondTasksStarted = 0
    const graph = createThreeStageFanIn(runtime, 5, {
      secondConcurrency: 2,
      second: async (_input, { signal }) => {
        await new Promise<void>((_resolve, reject) => {
          signal.addEventListener('abort', () => reject(signal.reason), { once: true })
          secondTasksStarted++
          if (secondTasksStarted === 2) twoSecondTasksStarted.resolve()
        })
      },
    })

    graph.start()
    await twoSecondTasksStarted.promise
    abortController.abort('stop branches')
    await runtime.closed

    expect(graph.state.started.first.sort()).toEqual([0, 1, 2, 3, 4])
    expect(graph.state.started.second.sort()).toEqual([0, 1])
    expect(graph.state.cancelled.second.sort()).toEqual([2, 3, 4])
    expect(graph.state.failures.map(({ stage }) => stage)).toEqual(['second', 'second'])
    expect(graph.state.started.third).toEqual([])
    expect(graph.state.mergeAttempts).toBe(0)
    expect(graph.state.mergeRuns).toBe(0)
  })

  it('lets running third tasks finish after abort but rejects the resulting merge submission', async () => {
    const abortController = new AbortController()
    const runtime = new Runtime({ concurrency: 3, signal: abortController.signal })
    const allThirdTasksStarted = deferred()
    const releaseThirdTasks = deferred()
    let thirdTasksStarted = 0
    const graph = createThreeStageFanIn(runtime, 3, {
      third: async () => {
        thirdTasksStarted++
        if (thirdTasksStarted === 3) allThirdTasksStarted.resolve()
        await releaseThirdTasks.promise
      },
    })

    graph.start()
    await allThirdTasksStarted.promise
    abortController.abort()
    releaseThirdTasks.resolve()
    await runtime.closed

    expect(graph.state.thirdCompleted.sort()).toEqual([0, 1, 2])
    expect(graph.state.mergeAttempts).toBe(1)
    expect(graph.state.mergeTaskId).toBeUndefined()
    expect(graph.state.mergeRuns).toBe(0)
  })

  it('cancels an accepted merge when abort occurs before its handler starts', async () => {
    const abortController = new AbortController()
    const runtime = new Runtime({ concurrency: 4, signal: abortController.signal })
    const graph = createThreeStageFanIn(runtime, 4, {
      onMergeSubmitted: (taskId) => {
        if (taskId !== undefined) abortController.abort()
      },
    })

    graph.start()
    await runtime.closed

    expect(graph.state.thirdCompleted.sort()).toEqual([0, 1, 2, 3])
    expect(graph.state.mergeAttempts).toBe(1)
    expect(graph.state.mergeTaskId).toBeTypeOf('number')
    expect(graph.state.mergeCancelled).toBe(true)
    expect(graph.state.mergeRuns).toBe(0)
  })
})
