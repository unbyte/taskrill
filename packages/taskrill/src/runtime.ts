import { SchedulerImpl } from './scheduler'
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
  private nextId = 1

  constructor(options: RuntimeOptions) {
    const { concurrency, signal, onError } = options
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
   * recursive fan-out pattern, where the callback seals the group.
   */
  group<I = void>(handler: Handler<I>, options?: GroupOptions<I>): Group<I> {
    const unit = new GroupImpl<I>(this.scheduler, this.nextId++, options?.name, handler, options?.onIdle)
    this.scheduler.track(unit)
    return unit
  }
}
