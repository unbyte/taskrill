import { describe, expect, it } from 'vitest'
import { SealedUnitError } from './errors'
import { join } from './join'
import { Runtime } from './runtime'

const flush = () => new Promise((resolve) => setTimeout(resolve, 0))

describe('Runtime construction', () => {
  it('validates concurrency at construction', () => {
    expect(() => new Runtime({ concurrency: 0 })).toThrow(RangeError)
    expect(() => new Runtime({ concurrency: -1 })).toThrow(RangeError)
    expect(() => new Runtime({ concurrency: 1.5 })).toThrow(RangeError)
    expect(() => new Runtime({ concurrency: Number.NaN })).toThrow(RangeError)
    expect(() => new Runtime({ concurrency: 1 })).not.toThrow()
    expect(() => new Runtime({ concurrency: Number.POSITIVE_INFINITY })).not.toThrow()
  })

  it('assigns monotonic ids and default kind#id names', () => {
    const runtime = new Runtime({ concurrency: 1 })
    const a = runtime.single(async () => {})
    const b = runtime.group(async () => {})
    const c = runtime.group(async () => {}, { name: 'D' })
    expect([a.id, b.id, c.id]).toEqual([1, 2, 3])
    expect(a.name).toBe('single#1')
    expect(b.name).toBe('group#2')
    expect(c.name).toBe('D')
  })
})

describe('Runtime scheduling', () => {
  it('never runs a handler inline during submit', () => {
    const runtime = new Runtime({ concurrency: 1 })
    let ran = false
    const single = runtime.single(() => {
      ran = true
    })
    single.submit()
    expect(ran).toBe(false)
  })

  it('caps concurrent handlers across the whole runtime', async () => {
    const runtime = new Runtime({ concurrency: 3 })
    let active = 0
    let peak = 0
    const release: Array<() => void> = []
    const group = runtime.group<number>(async () => {
      active++
      peak = Math.max(peak, active)
      await new Promise<void>((resolve) =>
        release.push(() => {
          active--
          resolve()
        }),
      )
    })

    for (let i = 0; i < 9; i++) group.submit(i)
    group.seal()

    await flush()
    expect(active).toBe(3)

    for (let i = 0; i < 9; i++) {
      release.shift()?.()
      await flush()
    }

    await expect(group.done).resolves.toEqual({ submitted: 9, succeeded: 9, failed: 0, cancelled: 0, ok: true })
    expect(peak).toBe(3)
  })

  it('resolves done asynchronously even when a unit settles synchronously', async () => {
    const runtime = new Runtime({ concurrency: 1 })
    const group = runtime.group(async () => {})
    let resolved = false
    group.done.then(() => {
      resolved = true
    })
    group.seal()
    expect(resolved).toBe(false)
    await group.done
    expect(resolved).toBe(true)
  })
})

describe('Runtime signal', () => {
  it('passes the runtime signal to every handler', async () => {
    const ac = new AbortController()
    const runtime = new Runtime({ concurrency: 1, signal: ac.signal })
    let got: AbortSignal | undefined
    const single = runtime.single((_input, ctx) => {
      got = ctx.signal
    })
    single.submit()
    await single.done
    expect(got).toBe(ac.signal)
  })

  it('provides a never-aborting placeholder when no signal is given', async () => {
    const runtime = new Runtime({ concurrency: 1 })
    let got: AbortSignal | undefined
    const single = runtime.single((_input, ctx) => {
      got = ctx.signal
    })
    single.submit()
    await single.done
    expect(got).toBeInstanceOf(AbortSignal)
    expect(got?.aborted).toBe(false)
  })
})

describe('Runtime errors', () => {
  it('reports each failed task exactly once with unit and input', async () => {
    const failures: Array<{ error: unknown; name: string; input: unknown }> = []
    const runtime = new Runtime({
      concurrency: 2,
      onError: ({ error, unit, input }) => failures.push({ error, name: unit.name, input }),
    })
    const boom = new Error('boom')
    const group = runtime.group<number>(
      async (x) => {
        if (x === 2) throw boom
      },
      { name: 'G' },
    )
    group.submit(1)
    group.submit(2)
    group.submit(3)
    group.seal()

    const settlement = await group.done
    expect(settlement).toEqual({ submitted: 3, succeeded: 2, failed: 1, cancelled: 0, ok: false })
    expect(failures).toEqual([{ error: boom, name: 'G', input: 2 }])
  })

  it('never rejects done even when a handler throws', async () => {
    const runtime = new Runtime({ concurrency: 1 })
    const single = runtime.single(async () => {
      throw new Error('boom')
    })
    single.submit()
    await expect(single.done).resolves.toMatchObject({ failed: 1, ok: false })
  })
})

describe('Runtime cancellation', () => {
  it('cancels queued tasks and lets running tasks finish naturally on abort', async () => {
    const ac = new AbortController()
    const runtime = new Runtime({ concurrency: 2, signal: ac.signal })
    let release!: () => void
    const gate = new Promise<void>((resolve) => {
      release = resolve
    })
    const started: number[] = []
    const group = runtime.group<number>(async (x) => {
      started.push(x)
      await gate
    })

    for (let i = 0; i < 5; i++) group.submit(i)
    await flush()
    expect(started).toHaveLength(2)

    ac.abort()
    release()

    await expect(group.done).resolves.toEqual({ submitted: 5, succeeded: 2, failed: 0, cancelled: 3, ok: false })
    expect(started).toHaveLength(2)
  })

  it('still fires onError for a running task that rejects after abort', async () => {
    const ac = new AbortController()
    const errors: unknown[] = []
    const runtime = new Runtime({ concurrency: 1, signal: ac.signal, onError: ({ error }) => errors.push(error) })
    let rejectGate!: (reason: unknown) => void
    const gate = new Promise<void>((_resolve, reject) => {
      rejectGate = reject
    })
    const group = runtime.group<number>(async () => {
      await gate
    })
    group.submit(1)
    group.submit(2)
    await flush()

    ac.abort()
    const boom = new Error('late boom')
    rejectGate(boom)

    await expect(group.done).resolves.toEqual({ submitted: 2, succeeded: 0, failed: 1, cancelled: 1, ok: false })
    expect(errors).toEqual([boom])
  })

  it('force-seals unsealed units on abort so their done resolves', async () => {
    const ac = new AbortController()
    const runtime = new Runtime({ concurrency: 1, signal: ac.signal })
    const group = runtime.group<number>(async () => {}) // never sealed by wiring
    ac.abort()
    await expect(group.done).resolves.toEqual({ submitted: 0, succeeded: 0, failed: 0, cancelled: 0, ok: true })
  })

  it('drops submit() after abort but throws after an application seal', () => {
    const ac = new AbortController()
    const aborted = new Runtime({ concurrency: 1, signal: ac.signal })
    const g1 = aborted.group<number>(async () => {})
    ac.abort()
    expect(() => g1.submit(1)).not.toThrow()

    const plain = new Runtime({ concurrency: 1 })
    const g2 = plain.group<number>(async () => {})
    g2.seal()
    expect(() => g2.submit(1)).toThrow(SealedUnitError)
  })

  it('supports fail-fast by aborting from onError', async () => {
    const ac = new AbortController()
    const seen: number[] = []
    const runtime = new Runtime({
      concurrency: 1,
      signal: ac.signal,
      onError: ({ error }) => ac.abort(error),
    })
    const group = runtime.group<number>(async (x) => {
      seen.push(x)
      if (x === 0) throw new Error('boom')
    })
    group.submit(0)
    group.submit(1)
    group.submit(2)

    const settlement = await group.done
    expect(seen).toEqual([0])
    expect(settlement).toMatchObject({ submitted: 3, failed: 1, cancelled: 2, ok: false })
    expect(ac.signal.reason).toBeInstanceOf(Error)
  })
})

describe('Runtime group concurrency', () => {
  /** A group handler that stays active until its pushed release is called. */
  const blockingGroup = (runtime: Runtime, concurrency?: number) => {
    let active = 0
    let peak = 0
    const release: Array<() => void> = []
    const group = runtime.group<number>(
      async () => {
        active++
        peak = Math.max(peak, active)
        await new Promise<void>((resolve) =>
          release.push(() => {
            active--
            resolve()
          }),
        )
      },
      concurrency === undefined ? undefined : { concurrency },
    )
    return {
      group,
      release,
      get active() {
        return active
      },
      get peak() {
        return peak
      },
    }
  }

  it('caps one group below the runtime concurrency', async () => {
    const runtime = new Runtime({ concurrency: 5 })
    const g = blockingGroup(runtime, 2)

    for (let i = 0; i < 6; i++) g.group.submit(i)
    g.group.seal()

    await flush()
    expect(g.active).toBe(2) // held to the group's own limit, not the runtime's 5

    for (let i = 0; i < 6; i++) {
      g.release.shift()?.()
      await flush()
    }

    await expect(g.group.done).resolves.toEqual({ submitted: 6, succeeded: 6, failed: 0, cancelled: 0, ok: true })
    expect(g.peak).toBe(2)
  })

  it('limits a group even when the runtime concurrency is unbounded', async () => {
    const runtime = new Runtime({ concurrency: Number.POSITIVE_INFINITY })
    const g = blockingGroup(runtime, 3)

    for (let i = 0; i < 10; i++) g.group.submit(i)
    g.group.seal()

    await flush()
    expect(g.active).toBe(3)

    for (let i = 0; i < 10; i++) {
      g.release.shift()?.()
      await flush()
    }

    await expect(g.group.done).resolves.toMatchObject({ submitted: 10, succeeded: 10, ok: true })
    expect(g.peak).toBe(3)
  })

  it('clips a group concurrency at or above the runtime cap to no extra limit', async () => {
    const runtime = new Runtime({ concurrency: 2 })
    const g = blockingGroup(runtime, 5) // >= runtime: the global cap already dominates

    for (let i = 0; i < 6; i++) g.group.submit(i)
    g.group.seal()

    await flush()
    expect(g.active).toBe(2)

    for (let i = 0; i < 6; i++) {
      g.release.shift()?.()
      await flush()
    }

    await expect(g.group.done).resolves.toMatchObject({ submitted: 6, succeeded: 6, ok: true })
    expect(g.peak).toBe(2)
  })

  it('gives each group an independent limit that shares the runtime cap', async () => {
    const runtime = new Runtime({ concurrency: 4 })
    const a = blockingGroup(runtime, 1)
    const b = blockingGroup(runtime, 1)

    for (let i = 0; i < 3; i++) a.group.submit(i)
    for (let i = 0; i < 3; i++) b.group.submit(i)
    a.group.seal()
    b.group.seal()

    await flush()
    expect(a.active).toBe(1) // each group runs one at a time...
    expect(b.active).toBe(1) // ...but the two run in parallel under the runtime's 4

    for (let i = 0; i < 3; i++) {
      a.release.shift()?.()
      b.release.shift()?.()
      await flush()
    }

    await expect(join(a.group, b.group)).resolves.toMatchObject({ submitted: 6, succeeded: 6, ok: true })
    expect(a.peak).toBe(1)
    expect(b.peak).toBe(1)
  })

  it('validates group concurrency at creation', () => {
    const runtime = new Runtime({ concurrency: 4 })
    expect(() => runtime.group(async () => {}, { concurrency: 0 })).toThrow(RangeError)
    expect(() => runtime.group(async () => {}, { concurrency: -1 })).toThrow(RangeError)
    expect(() => runtime.group(async () => {}, { concurrency: 1.5 })).toThrow(RangeError)
    expect(() => runtime.group(async () => {}, { concurrency: Number.NaN })).toThrow(RangeError)
    expect(() => runtime.group(async () => {}, { concurrency: 2 })).not.toThrow()
    expect(() => runtime.group(async () => {}, { concurrency: Number.POSITIVE_INFINITY })).not.toThrow()
  })

  it('cancels a limited group’s held-back tasks on abort and settles', async () => {
    const ac = new AbortController()
    const runtime = new Runtime({ concurrency: 5, signal: ac.signal })
    let release!: () => void
    const held = new Promise<void>((resolve) => {
      release = resolve
    })
    const started: number[] = []
    const group = runtime.group<number>(
      async (x) => {
        started.push(x)
        await held
      },
      { concurrency: 2 },
    )

    for (let i = 0; i < 6; i++) group.submit(i)
    await flush()
    expect(started).toHaveLength(2) // only two started under the group's limit

    ac.abort()
    release()

    await expect(group.done).resolves.toEqual({ submitted: 6, succeeded: 2, failed: 0, cancelled: 4, ok: false })
    expect(started).toHaveLength(2) // held-back tasks never started
  })
})
