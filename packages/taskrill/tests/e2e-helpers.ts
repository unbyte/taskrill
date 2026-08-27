import type { Runtime } from '../src'

export const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 0))

export const deferred = () => {
  let resolve!: () => void
  const promise = new Promise<void>((resolvePromise) => {
    resolve = resolvePromise
  })
  return { promise, resolve }
}

export const runToIdle = (runtime: Runtime, start: () => void) =>
  new Promise<void>((resolve) => {
    const unsubscribe = runtime.on('idle', () => {
      unsubscribe()
      resolve()
    })
    start()
  })
