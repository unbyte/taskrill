import type { Unsubscribe } from './types'

type StoredListener = (event: never) => void

/** Synchronous, snapshot-based event delivery with per-listener error isolation. */
export class Emitter<Events extends object> {
  private readonly listeners = new Map<keyof Events, Set<StoredListener>>()

  on<K extends keyof Events>(event: K, listener: (value: Events[K]) => void): Unsubscribe {
    let listeners = this.listeners.get(event)
    if (!listeners) {
      listeners = new Set()
      this.listeners.set(event, listeners)
    }

    const stored = ((value: Events[K]) => listener(value)) as StoredListener
    listeners.add(stored)

    return () => {
      listeners.delete(stored)
      if (listeners.size === 0 && this.listeners.get(event) === listeners)
        this.listeners.delete(event)
    }
  }

  emit<K extends keyof Events>(event: K, value: Events[K]) {
    const listeners = this.listeners.get(event)
    if (!listeners) return

    for (const listener of [...listeners]) {
      try {
        const invoke = listener as (value: Events[K]) => void
        invoke(value)
      } catch (error) {
        queueMicrotask(() => {
          throw error
        })
      }
    }
  }
}
