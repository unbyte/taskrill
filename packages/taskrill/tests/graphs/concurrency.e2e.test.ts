import { describe, expect, it } from 'vitest'
import { Runtime } from '../../src'

const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 0))

const probe = () => {
  let active = 0
  let peak = 0
  const waiters: Array<() => void> = []
  return {
    handler: async () => {
      active++
      peak = Math.max(peak, active)
      await new Promise<void>((resolve) => waiters.push(resolve))
      active--
    },
    release: () => {
      for (const resolve of waiters.splice(0)) resolve()
    },
    get active() {
      return active
    },
    get peak() {
      return peak
    },
  }
}

describe('e2e: global and per-node concurrency', () => {
  it('keeps a throttled node within its lane while other nodes use the runtime', async () => {
    const runtime = new Runtime({ concurrency: 6 })
    const limited = probe()
    const free = probe()
    const Limited = runtime.node<number>(limited.handler, { concurrency: 2 })
    const Free = runtime.node<number>(free.handler)

    for (let input = 0; input < 6; input++) {
      Limited.submit(input)
      Free.submit(input)
    }

    await flush()
    expect(limited.active).toBe(2)
    expect(free.active).toBe(4)

    while (limited.active > 0 || free.active > 0) {
      limited.release()
      free.release()
      await flush()
    }

    expect(limited.peak).toBe(2)
    expect(free.peak).toBe(4)
  })
})
