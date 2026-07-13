import { describe, expect, it } from 'vitest'
import { SealedUnitError } from './errors'
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
