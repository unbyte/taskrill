import { describe, expect, it } from 'vitest'
import { join, Runtime } from '../src'

/** Lets every microtask scheduled so far run to a fixed point (dispatch is microtask-driven). */
const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 0))

/**
 * A handler that blocks until released, so several of its tasks stay live at
 * once and a test can read how many the Runtime is running concurrently.
 * `active`/`max` are the current and peak concurrency, `waiting` the live
 * handlers, and `release()` frees everyone currently blocked.
 */
function probe() {
  let active = 0
  let max = 0
  const waiters: Array<() => void> = []
  return {
    handler: async () => {
      active++
      max = Math.max(max, active)
      await new Promise<void>((resolve) => waiters.push(resolve))
      active--
    },
    release: () => {
      for (const resolve of waiters.splice(0)) resolve()
    },
    get active() {
      return active
    },
    get max() {
      return max
    },
    get waiting() {
      return waiters.length
    },
  }
}

/** Releases live handlers and lets freed slots admit the backlog until nothing is left waiting. */
const drain = async (p: ReturnType<typeof probe>) => {
  do {
    p.release()
    await flush()
  } while (p.waiting > 0)
}

describe('e2e: a rate-limited stage inside a pipeline', () => {
  it('confines a throttled stage to its lane while the rest of the pipeline runs at full width', async () => {
    // Mirrors the README's image pipeline: every file is both resized and
    // thumbnailed, and a Report runs once both stages settle — but the thumbnail
    // service is rate-limited to two at a time.
    const runtime = new Runtime({ concurrency: 6 })
    const files = ['a', 'b', 'c', 'd', 'e', 'f']

    const resize = probe() // free to spread across the Runtime's slots
    const thumb = probe() // capped by its gate to a narrow lane
    const Resize = runtime.group<string>(resize.handler)
    const Thumbnail = runtime.group<string>(thumb.handler, { concurrency: 2 })

    const reportRan: string[] = []
    const Report = runtime.single(async () => void reportRan.push('report'))

    // Seal graph: Report runs only once both image stages have settled.
    join(Resize, Thumbnail).then((s) => (s.ok ? Report.submit() : Report.skip()))

    for (const file of files) {
      Resize.submit(file)
      Thumbnail.submit(file)
    }
    Resize.seal()
    Thumbnail.seal()

    await flush()
    // Both stages compete for the six slots, but Thumbnail's gate holds it to two
    // while Resize spreads across the remaining four — the throttle confines one
    // stage without stalling the pipeline.
    expect(thumb.active).toBe(2)
    expect(resize.active).toBe(4)
    expect(reportRan).toEqual([]) // the terminal is still waiting on both upstreams

    await drain(resize)
    await drain(thumb)

    const [rs, ts, reportSettle] = await Promise.all([Resize.done, Thumbnail.done, Report.done])
    expect(rs).toMatchObject({ submitted: 6, succeeded: 6, ok: true })
    expect(ts).toMatchObject({ submitted: 6, succeeded: 6, ok: true })
    expect(thumb.max).toBe(2) // the cap bound the whole run...
    expect(resize.max).toBe(4) // ...while Resize kept using the slots it left free
    expect(reportSettle).toMatchObject({ submitted: 1, succeeded: 1, ok: true })
    expect(reportRan).toEqual(['report']) // and Report ran, only after both settled
  })

  it('settles a throttled stage and its tail on abort instead of hanging on the backlog', async () => {
    const ac = new AbortController()
    const runtime = new Runtime({ concurrency: 4, signal: ac.signal })

    // A rate-limited stage: two handlers run, the other four sit in the gate's
    // backlog — never enqueued to the scheduler, so only the gate can cancel them.
    const started: number[] = []
    const Limited = runtime.group<number>(
      async (n, { signal }) => {
        started.push(n)
        await new Promise<void>((resolve) => {
          if (signal.aborted) return resolve()
          signal.addEventListener('abort', () => resolve(), { once: true })
        })
      },
      { concurrency: 2 },
    )

    // A downstream terminal, wired to run once Limited settles.
    const ran: string[] = []
    const Report = runtime.single(async () => void ran.push('report'))
    Limited.done.then((s) => (s.succeeded > 0 ? Report.submit() : Report.skip()))

    for (let i = 0; i < 6; i++) Limited.submit(i)
    // Limited is never sealed by wiring. The abort must force-seal it *and* cancel
    // the four jobs the gate still holds; otherwise Limited.done — and the tail
    // behind it — would hang.
    queueMicrotask(() => ac.abort())

    const [ls, rs] = await Promise.all([Limited.done, Report.done])

    expect(ac.signal.aborted).toBe(true)
    // The two admitted tasks honor the signal and succeed; the gate's four held-back
    // jobs — which never reached the scheduler queue, so only the gate can cancel —
    // are cancelled, so Limited settles rather than hanging.
    expect(ls).toEqual({ submitted: 6, succeeded: 2, failed: 0, cancelled: 4, ok: false })
    expect(started).toEqual([0, 1]) // only the two admitted tasks ever ran
    // The abort force-seals every unit, so the downstream terminal settles empty and
    // its done resolves too — the whole tail drains instead of stranding. Report never
    // runs: it is force-sealed, and its wiring's submit() no-ops after the abort.
    expect(rs).toEqual({ submitted: 0, succeeded: 0, failed: 0, cancelled: 0, ok: true })
    expect(ran).toEqual([])
  })
})
