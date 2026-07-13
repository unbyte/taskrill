import { describe, expect, it } from 'vitest'
import { SealedUnitError } from './errors'
import type { Unit } from './types'

describe('SealedUnitError', () => {
  const unit = { id: 7, name: 'group#7', done: Promise.resolve() } as unknown as Unit

  it('is an Error with a stable name', () => {
    const err = new SealedUnitError(unit)
    expect(err).toBeInstanceOf(Error)
    expect(err).toBeInstanceOf(SealedUnitError)
    expect(err.name).toBe('SealedUnitError')
  })

  it('carries the offending unit and names it in the message', () => {
    const err = new SealedUnitError(unit)
    expect(err.unit).toBe(unit)
    expect(err.message).toContain('group#7')
  })
})
