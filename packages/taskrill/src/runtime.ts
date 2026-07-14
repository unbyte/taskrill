import { Gate } from './gate'
import { assertValidConcurrency, SchedulerImpl } from './scheduler'
import type { Group, GroupOptions, Handler, RuntimeOptions, Single, UnitOptions } from './types'
import { GroupImpl, SingleImpl } from './unit'

/**
 * The public entry point: one in-process scheduling environment that owns task
 * execution, global concurrency control, and abort behavior, and hands out the
 * units that submit and observe work.
 *
 * It has no "finished" state of its own — completion is a property of individual
 * units, observed through their `done`.
 */
export class Runtime {
  private readonly scheduler: SchedulerImpl
  private readonly concurrency: number
  private nextId = 1

  constructor(options: RuntimeOptions) {
    const { concurrency, signal, onError } = options
    this.concurrency = concurrency
    this.scheduler = new SchedulerImpl(concurrency, signal, onError)
  }

  /**
   * Creates a {@link Single} bound to this Runtime — a unit that accepts exactly
   * one task submission and seals on it. Submitting the returned single starts
   * the pipeline; no separate `start()` is needed.
   */
  single<I = void>(handler: Handler<I>, options?: UnitOptions): Single<I> {
    const unit = new SingleImpl<I>(this.scheduler, this.nextId++, options?.name, handler)
    this.scheduler.track(unit)
    return unit
  }

  /**
   * Creates a {@link Group} bound to this Runtime — a unit that accepts many
   * task submissions until an explicit `seal()`. Pass `onIdle` for the self-fed
   * recursive fan-out pattern, where the callback seals the group; pass
   * `concurrency` to cap how many of this group's handlers run at once.
   */
  group<I = void>(handler: Handler<I>, options?: GroupOptions<I>): Group<I> {
    const gate = this.gateFor(options?.concurrency)
    const unit = new GroupImpl<I>(gate ?? this.scheduler, this.nextId++, options?.name, handler, options?.onIdle)
    // Track the unit before its gate so an abort seals the unit first, letting
    // the gate's backlog cancellations settle it instead of firing onIdle.
    this.scheduler.track(unit)
    if (gate) this.scheduler.track(gate)
    return unit
  }

  /**
   * The per-group limiter a group enqueues through, or `undefined` when it needs
   * none — either no `concurrency` was given, or it is at/above the Runtime's own
   * concurrency, where the Runtime's limit already dominates.
   */
  private gateFor(concurrency: number | undefined): Gate | undefined {
    if (concurrency === undefined) return undefined
    assertValidConcurrency(concurrency, 'group concurrency')
    if (concurrency >= this.concurrency) return undefined
    return new Gate(this.scheduler, concurrency)
  }
}
