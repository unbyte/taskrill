import { describe, expect, it } from 'vitest'
import { join, Runtime } from '../src'

describe('e2e: incremental fan-out pipeline (B -> C -> D -> E)', () => {
  it('flows tasks forward immediately while sealing propagates on settlement', async () => {
    const order: string[] = []
    const ac = new AbortController()
    const runtime = new Runtime({ concurrency: 4, signal: ac.signal })

    type CJob = { id: string }
    type DJob = { parentId: string; id: string }

    const produced: DJob[] = []

    const E = runtime.single(async () => {
      order.push('E')
    })

    const D = runtime.group<DJob>(async (job) => {
      produced.push(job)
      order.push(`D:${job.parentId}/${job.id}`)
    })

    const C = runtime.group<CJob>(async (job) => {
      // Each C incrementally submits two D tasks, awaited implicitly because
      // submit() completes synchronously (Handler Contract rule 1).
      for (let i = 0; i < 2; i++) D.submit({ parentId: job.id, id: String(i) })
    })

    const B = runtime.single(async () => {
      try {
        for (const id of ['a', 'b', 'c']) C.submit({ id })
      } finally {
        C.seal()
      }
    })

    C.done.then(() => D.seal())
    D.done.then((s) => (s.ok ? E.submit() : E.skip()))

    B.submit()

    const [bs, cs, ds, es] = await Promise.all([B.done, C.done, D.done, E.done])
    if (ac.signal.aborted) throw ac.signal.reason

    expect(bs).toMatchObject({ submitted: 1, succeeded: 1, ok: true })
    expect(cs).toMatchObject({ submitted: 3, succeeded: 3, ok: true })
    expect(ds).toMatchObject({ submitted: 6, succeeded: 6, ok: true })
    expect(es).toMatchObject({ submitted: 1, succeeded: 1, ok: true })
    expect(produced).toHaveLength(6)
    // E is terminal: it may only run after every D has settled.
    expect(order[order.length - 1]).toBe('E')
    expect(order.filter((s) => s.startsWith('D:'))).toHaveLength(6)
  })

  it('skips the terminal single when an upstream task fails', async () => {
    const runtime = new Runtime({ concurrency: 2 })
    const ran: string[] = []

    const E = runtime.single(async () => {
      ran.push('E')
    })
    const D = runtime.group<number>(async (n) => {
      if (n === 1) throw new Error('boom')
    })

    D.done.then((s) => (s.ok ? E.submit() : E.skip()))

    D.submit(0)
    D.submit(1)
    D.submit(2)
    D.seal()

    const [ds, es] = await Promise.all([D.done, E.done])
    expect(ds).toMatchObject({ submitted: 3, succeeded: 2, failed: 1, ok: false })
    expect(es).toEqual({ submitted: 0, succeeded: 0, failed: 0, cancelled: 0, ok: true })
    expect(ran).toEqual([]) // E was skipped, never ran
  })
})

describe('e2e: fan-in via join', () => {
  it('seals a downstream unit only after every upstream has settled', async () => {
    const runtime = new Runtime({ concurrency: 4 })
    const dInputs: number[] = []

    const D = runtime.group<number>(async (n) => void dInputs.push(n))
    const C1 = runtime.group<number>(async (n) => D.submit(n))
    const C2 = runtime.group<number>(async (n) => D.submit(n * 10))
    const E = runtime.single(async () => {})

    // D grows from two upstreams; seal it only once both have settled.
    join(C1, C2).then(() => D.seal())
    D.done.then((s) => (s.ok ? E.submit() : E.skip()))

    C1.submit(1)
    C1.submit(2)
    C1.seal()
    C2.submit(3)
    C2.seal()

    const upstream = await join(C1, C2)
    const [ds, es] = await Promise.all([D.done, E.done])

    expect(upstream).toMatchObject({ submitted: 3, succeeded: 3, ok: true })
    expect(ds).toMatchObject({ submitted: 3, succeeded: 3, ok: true })
    expect(es).toMatchObject({ submitted: 1, succeeded: 1, ok: true })
    expect([...dInputs].sort((a, b) => a - b)).toEqual([1, 2, 30])
  })
})

describe('e2e: recursive fan-out via onIdle', () => {
  it('drives a self-fed group to completion and seals itself on the true idle', async () => {
    const runtime = new Runtime({ concurrency: 8 })
    const visited: string[] = []

    // A balanced binary tree of depth 4: node key length encodes its depth.
    const childrenOf = (key: string): string[] => (key.length >= 4 ? [] : [`${key}0`, `${key}1`])

    const Crawl = runtime.group<string>(
      async (key) => {
        visited.push(key)
        for (const child of childrenOf(key)) Crawl.submit(child)
      },
      { onIdle: (unit) => unit.seal() },
    )

    Crawl.submit('r')
    const settlement = await Crawl.done

    // 1 + 2 + 4 + 8 nodes across four levels.
    expect(settlement).toMatchObject({ submitted: 15, succeeded: 15, ok: true })
    expect(new Set(visited).size).toBe(15)
  })
})

describe('e2e: abort as a liveness backstop', () => {
  it('recovers a never-sealed pipeline once the signal aborts', async () => {
    const ac = new AbortController()
    const runtime = new Runtime({ concurrency: 4, signal: ac.signal })

    const G = runtime.group<number>(async (_input, ctx) => {
      // Cooperative handler: it settles promptly when the signal aborts.
      await new Promise<void>((resolve) => {
        if (ctx.signal.aborted) return resolve()
        ctx.signal.addEventListener('abort', () => resolve(), { once: true })
      })
    })

    for (let i = 0; i < 4; i++) G.submit(i)
    // G is never sealed by wiring; the abort force-seals it.
    queueMicrotask(() => ac.abort())

    const settlement = await G.done
    expect(ac.signal.aborted).toBe(true)
    // All four started before the abort, so each honors the signal and succeeds.
    expect(settlement).toEqual({ submitted: 4, succeeded: 4, failed: 0, cancelled: 0, ok: true })
  })
})

describe('e2e: a mid-pipeline failure never strands the tail', () => {
  it('keeps every downstream done live when a task throws (a -> b -> c -> d)', async () => {
    const errors: unknown[] = []
    const runtime = new Runtime({ concurrency: 1, onError: ({ error }) => errors.push(error) })
    const ran: string[] = []

    const A = runtime.single(async () => void ran.push('a'))
    const B = runtime.single(async () => {
      ran.push('b')
      throw new Error('b boom')
    })
    const C = runtime.single(async () => void ran.push('c'))
    const D = runtime.single(async () => void ran.push('d'))

    // Advance a stage only when its upstream actually ran and succeeded. A
    // failed *or* skipped upstream (succeeded === 0) skips its successor, so the
    // failure cascades and the whole tail still seals rather than deadlocking.
    // (`s.ok` would not do: a skipped unit is vacuously ok and would run its
    // successor.)
    A.done.then((s) => (s.succeeded > 0 ? B.submit() : B.skip()))
    B.done.then((s) => (s.succeeded > 0 ? C.submit() : C.skip()))
    C.done.then((s) => (s.succeeded > 0 ? D.submit() : D.skip()))

    A.submit()

    // The point of the test: d.done resolves even though b threw upstream.
    const ds = await D.done
    expect(ds).toEqual({ submitted: 0, succeeded: 0, failed: 0, cancelled: 0, ok: true })
    expect(ran).toEqual(['a', 'b']) // c and d were skipped, never ran

    // Nothing is left pending, and b's throw surfaced as a counted failure
    // (reported once) rather than a rejection or a hang.
    const [as, bs, cs] = await Promise.all([A.done, B.done, C.done])
    expect(as).toMatchObject({ submitted: 1, succeeded: 1, ok: true })
    expect(bs).toMatchObject({ submitted: 1, failed: 1, ok: false })
    expect(cs).toMatchObject({ submitted: 0, ok: true }) // skipped
    expect(errors).toHaveLength(1)
    expect((errors[0] as Error).message).toBe('b boom')
  })
})
