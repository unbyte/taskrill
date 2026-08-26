import { afterEach, describe, expect, it } from 'vitest'
import { Emitter } from './emitter'

interface Events {
  value: number
}

describe('Emitter', () => {
  const realQueueMicrotask = globalThis.queueMicrotask

  afterEach(() => {
    globalThis.queueMicrotask = realQueueMicrotask
  })

  it('delivers synchronously in registration order', () => {
    const emitter = new Emitter<Events>()
    const seen: number[] = []

    emitter.on('value', (value) => seen.push(value))
    emitter.on('value', (value) => seen.push(value * 10))
    emitter.emit('value', 2)

    expect(seen).toEqual([2, 20])
  })

  it('uses a stable snapshot when listeners mutate subscriptions', () => {
    const emitter = new Emitter<Events>()
    const seen: string[] = []
    let added = false
    let unsubscribeSecond!: () => void

    emitter.on('value', () => {
      seen.push('first')
      unsubscribeSecond()
      if (!added) {
        added = true
        emitter.on('value', () => seen.push('third'))
      }
    })
    unsubscribeSecond = emitter.on('value', () => seen.push('second'))

    emitter.emit('value', 1)
    emitter.emit('value', 2)

    expect(seen).toEqual(['first', 'second', 'first', 'third'])
  })

  it('returns independent idempotent unsubscribe functions', () => {
    const emitter = new Emitter<Events>()
    const seen: number[] = []
    const listener = (value: number) => seen.push(value)
    const unsubscribeFirst = emitter.on('value', listener)
    emitter.on('value', listener)

    unsubscribeFirst()
    unsubscribeFirst()
    emitter.emit('value', 3)

    expect(seen).toEqual([3])
  })

  it('continues after a listener throws and rethrows on a fresh microtask', () => {
    const emitter = new Emitter<Events>()
    const deferred: Array<() => void> = []
    const seen: string[] = []
    const boom = new Error('listener boom')
    globalThis.queueMicrotask = (callback) => void deferred.push(callback)

    emitter.on('value', () => {
      seen.push('throwing')
      throw boom
    })
    emitter.on('value', () => seen.push('later'))

    emitter.emit('value', 1)

    expect(seen).toEqual(['throwing', 'later'])
    expect(deferred).toHaveLength(1)
    expect(() => deferred[0]()).toThrow(boom)
  })
})
