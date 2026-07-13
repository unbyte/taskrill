import type { Settlement, Unit } from './types'

/**
 * Awaits every unit's `done` and reduces them to one aggregate {@link Settlement}:
 * counts are summed and `ok` is `true` only when every joined unit is `ok`. With
 * no units it resolves to a vacuously-ok empty settlement.
 *
 * Because each unit's `done` never rejects, `join` never rejects either — so it
 * preserves the deadlock-free wiring invariant. Use it to seal a downstream unit
 * once several upstreams have all settled:
 *
 * ```ts
 * join(C1, C2).then(() => D.seal())
 * join(D1, D2, D3).then((s) => (s.ok ? E.submit() : E.skip()))
 * ```
 *
 * Handler Contract rule 1 guarantees every upstream submission to the downstream
 * unit has completed by the time each upstream `done` resolves, so a seal wired
 * through `join` can never strand a late submission.
 */
export async function join(...units: Unit[]): Promise<Settlement> {
  const settlements = await Promise.all(units.map((unit) => unit.done))

  let submitted = 0
  let succeeded = 0
  let failed = 0
  let cancelled = 0
  for (const settlement of settlements) {
    submitted += settlement.submitted
    succeeded += settlement.succeeded
    failed += settlement.failed
    cancelled += settlement.cancelled
  }

  return { submitted, succeeded, failed, cancelled, ok: failed === 0 && cancelled === 0 }
}
