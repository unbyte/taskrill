import type { Unit } from './types'

/**
 * Thrown synchronously by `submit()` on a unit the caller has already sealed —
 * a group after `seal()`, or a single after `submit()` or `skip()`.
 *
 * This is distinct from `submit()` after an *abort*, which is a silent no-op.
 * The most common way to reach it is a handler that detaches downstream work
 * and races the downstream `seal()` (Handler Contract rule 1).
 */
export class SealedUnitError extends Error {
  readonly unit: Unit

  constructor(unit: Unit) {
    super(
      `Cannot submit to sealed unit ${unit.name}. ` +
        `A unit is sealed by seal()/submit()/skip(); the usual cause is a handler ` +
        `that submits from detached work which outlives the handler and races the seal — ` +
        `await everything that submits downstream before the handler resolves.`,
    )
    this.name = 'SealedUnitError'
    this.unit = unit
  }
}
