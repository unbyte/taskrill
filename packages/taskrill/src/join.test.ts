import { describe, expect, it } from 'vitest'
import { join } from './join'
import { Runtime } from './runtime'

const flush = () => new Promise((resolve) => setTimeout(resolve, 0))

describe('join', () => {
  it('sums counts and is ok only when every unit is ok', async () => {
    const runtime = new Runtime({ concurrency: 4 })
    const a = runtime.group<number>(async () => {})
    const b = runtime.group<number>(async (n) => {
      if (n === 0) throw new Error('boom')
    })

    a.submit(1)
    a.submit(2)
    a.seal()
    b.submit(0)
    b.submit(1)
    b.seal()

    await expect(join(a, b)).resolves.toEqual({ submitted: 4, succeeded: 3, failed: 1, cancelled: 0, ok: false })
  })

  it('is ok when every joined unit fully succeeds', async () => {
    const runtime = new Runtime({ concurrency: 2 })
    const a = runtime.single(async () => {})
    const b = runtime.group<number>(async () => {})

    a.submit()
    b.submit(1)
    b.seal()

    await expect(join(a, b)).resolves.toEqual({ submitted: 2, succeeded: 2, failed: 0, cancelled: 0, ok: true })
  })

  it('resolves vacuously ok for no units', async () => {
    await expect(join()).resolves.toEqual({ submitted: 0, succeeded: 0, failed: 0, cancelled: 0, ok: true })
  })

  it('resolves only after every joined unit has settled', async () => {
    const runtime = new Runtime({ concurrency: 2 })
    let release!: () => void
    const gate = new Promise<void>((resolve) => {
      release = resolve
    })
    const a = runtime.single(async () => {})
    const b = runtime.single(async () => {
      await gate
    })

    a.submit()
    b.submit()

    let settled = false
    const joined = join(a, b).then((s) => {
      settled = true
      return s
    })

    await flush()
    expect(settled).toBe(false) // b is still running

    release()
    await expect(joined).resolves.toMatchObject({ submitted: 2, ok: true })
    expect(settled).toBe(true)
  })

  it('never rejects even when joined units contain failures', async () => {
    const runtime = new Runtime({ concurrency: 1 })
    const a = runtime.single(async () => {
      throw new Error('boom')
    })
    a.submit()
    await expect(join(a)).resolves.toMatchObject({ failed: 1, ok: false })
  })

  it('accepts a spread array of units', async () => {
    const runtime = new Runtime({ concurrency: 4 })
    const units = [0, 1, 2].map((n) => {
      const g = runtime.group<number>(async () => {})
      g.submit(n)
      g.seal()
      return g
    })
    await expect(join(...units)).resolves.toMatchObject({ submitted: 3, succeeded: 3, ok: true })
  })
})
