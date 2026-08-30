import { describe, expect, it } from 'vitest'
import { FifoQueue } from './fifo'

describe('FifoQueue', () => {
  it('preserves order and reports its remaining size', () => {
    const queue = new FifoQueue<{ id: number }>()
    queue.push({ id: 1 })
    queue.push({ id: 2 })
    queue.push({ id: 3 })

    expect(queue.shift()?.id).toBe(1)
    expect(queue.size).toBe(2)
    expect(queue.drain().map(({ id }) => id)).toEqual([2, 3])
    expect(queue.size).toBe(0)
    expect(queue.shift()).toBeUndefined()
  })

  it('stays FIFO after compacting a large consumed prefix', () => {
    const queue = new FifoQueue<{ id: number }>()
    for (let id = 0; id < 2_050; id++) queue.push({ id })

    for (let id = 0; id < 1_500; id++) expect(queue.shift()?.id).toBe(id)

    expect(queue.drain().map(({ id }) => id)).toEqual(
      Array.from({ length: 550 }, (_, index) => index + 1_500),
    )
  })
})
